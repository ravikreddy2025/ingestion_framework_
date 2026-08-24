"""The operational control table: layer 4 of the five-layer configuration.

One row per `source_key` in `{ops_catalog}.{control_schema}.ingest_control`, editable by
the support team with no deploy. This module turns that row into the plain override dict
framework/config.py merges over the YAML layers, and does nothing else - it never applies
an override itself, so there is exactly one place where precedence between layers lives.

THREE RULES, EACH LOAD-BEARING
------------------------------
1. A MISSING ROW IS NOT AN ERROR. It means "no overrides", so a newly onboarded source
   runs the moment its YAML merges - nobody has to remember to INSERT a row first.
2. DUPLICATE ROWS ARE an error naming the source_key. Two rows means two answers to "is
   this source enabled?", and silently taking either one is how an emergency stop gets
   ignored.
3. EVERY OVERRIDE IS VALIDATED against that source's SOURCE_SPEC, producing the same
   unknown-key error a YAML typo produces. Without this a control column would be a hole in
   the middle of validation everything else is careful about.

ONE SHARED TABLE, PREFIXED COLUMNS (docs/build_log/DECISIONS.md D-01)
-----------------------------------------------------------------------
Every source type's row lives in the SAME physical table, so a column meaning the exact
same thing for every type stays unprefixed and framework-owned (`enabled`,
`replay_rerun_id`) - see `_FRAMEWORK_SETTING_COLUMNS` below - while a column specific to
ONE source type is named `<source_type>_<setting>` and declared on that type's own
`SOURCE_SPEC.control_columns` (column name -> setting name). This module reads whichever
columns the CALLER'S spec declares and never hardcodes a source type's name - `runner.py`
is the one place in framework/ allowed to know those.

There is deliberately no free-form JSON escape hatch for a source-specific setting any
more (the former `source_overrides` column is gone): a setting either has a dedicated
column, or it is not operationally overridable from this table. `replay_controls` stays
JSON and stays unprefixed - it carries structured, incident-scoped replay parameters whose
SHAPE differs per source type, which is a different job from a standing override.

A COLUMN SET FOR THE WRONG SOURCE TYPE IS AN ERROR, NOT A SILENT IGNORE
-------------------------------------------------------------------------
Because the table is shared, a column that belongs to some OTHER source type can be
populated on this row by mistake - a copy-pasted template, or a typo in the prefix. That is
caught by `other_control_columns`, a column -> owning-source-type map built once in
`runner.py` from every known `SOURCE_SPEC.control_columns` and passed in here; a column in
that map but not in THIS spec's own `control_columns` is rejected rather than silently
ignored, naming both the column and the mismatch.

STRUCTURAL FIELDS ARE IGNORED, NOT REJECTED
-------------------------------------------
Exactly as the YAML layers behave (framework/config.py `apply_overrides`). Partitioning,
merge keys and target names describe what is already on disk; an operational lever must not
be able to move them, and a support engineer who tries gets a log line rather than a failed
run at 3am.

WHAT THIS MODULE READS
----------------------
`_FRAMEWORK_SETTING_COLUMNS` below, the calling source's own `spec.control_columns`, and
one JSON column (`replay_controls`). Everything else on the table - notes, attribution,
another source type's columns with nothing set, anything a future admin adds - is ignored
on purpose.

NO PYSPARK IMPORT: the session arrives as an argument.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Mapping

from .config import ConfigError, validate_override_keys
from .contracts import SourceSpec

LOG = logging.getLogger(__name__)

# Control-table columns that mean the exact same thing for EVERY source type, so they stay
# unprefixed and the framework - not any one SOURCE_SPEC - owns the mapping. Everything
# type-specific comes from the calling spec's own `control_columns` instead.
_FRAMEWORK_SETTING_COLUMNS = {
    "enabled": "enabled",
    "replay_rerun_id": "rerun_id",
}

# JSON object columns, merged over the named columns. Just the one now that
# `source_overrides` is gone - see the module docstring.
_JSON_COLUMNS = ("replay_controls",)

# source_key is interpolated into the WHERE clause. It is a deployed YAML filename stem,
# never free-form input, and this refuses anything that is not.
_SAFE_KEY = re.compile(r"^[A-Za-z0-9_.-]+$")


def read_control(
    spark: Any,
    control_table: str,
    source_key: str,
    spec: SourceSpec,
    other_control_columns: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """The layer-4 override dict for one source. Empty when there is nothing to override.

    `other_control_columns` is column name -> owning source_type for every OTHER known
    source type's control columns, so a column belonging to a different type can be caught
    rather than silently ignored. `runner.py` builds and passes it; a direct caller (a test,
    a notebook) that omits it simply loses that one check.

    Returns plain data. Applying it - including deciding that a structural key is ignored -
    is framework/config.py's job.
    """
    row = _control_row(spark, control_table, source_key)
    if row is None:
        return {}
    _check_declared_source_type(row, control_table, source_key, spec)
    _check_no_foreign_control_columns(row, source_key, spec, other_control_columns or {})

    setting_columns = {**_FRAMEWORK_SETTING_COLUMNS, **spec.control_columns}
    overrides = {setting: row[column] for column, setting in setting_columns.items() if row.get(column) is not None}
    for column in _JSON_COLUMNS:
        overrides.update(_parse_json_column(row.get(column), control_table, source_key, column))

    validate_override_keys(spec, overrides, f"{control_table} row for source_key '{source_key}'")
    if overrides:
        LOG.info("Control table overrides for '%s': %s", source_key, sorted(overrides))
    return overrides


def _control_row(spark: Any, control_table: str, source_key: str) -> dict[str, Any] | None:
    """The single row for this source, or None. Two rows is an error, no table is not.

    A fresh environment where the support table has not been provisioned yet still runs on
    its YAML: the control table exists to CHANGE behaviour, and its absence means nobody
    has changed any.
    """
    if not spark.catalog.tableExists(control_table):
        LOG.warning(
            "Control table %s does not exist; running on YAML configuration only. "
            "Provision it with sql/01_operational_config.sql.",
            control_table,
        )
        return None
    rows = spark.table(control_table).where(f"source_key = '{_safe(source_key)}'").limit(2).collect()
    if not rows:
        return None
    if len(rows) > 1:
        raise ConfigError(
            f"{control_table} contains {len(rows)}+ rows for source_key '{source_key}'. "
            "The control table must hold exactly one row per source - deduplicate it before "
            "rerunning, and check which row is the intended one rather than guessing."
        )
    return dict(rows[0].asDict())


def _check_declared_source_type(row: Mapping[str, Any], control_table: str, source_key: str, spec: SourceSpec) -> None:
    """The row's own source_type must agree with the source it is overriding.

    The column exists so support can query the table by feed type. Letting it disagree with
    reality would make every such query quietly wrong, so it is checked rather than
    decorative.
    """
    declared = row.get("source_type")
    if declared and str(declared) != spec.source_type:
        raise ConfigError(
            f"{control_table} row for source_key '{source_key}' declares source_type "
            f"'{declared}', but that source is a '{spec.source_type}'. Fix the row - the "
            "overrides on it were written for a different kind of source."
        )


def _check_no_foreign_control_columns(
    row: Mapping[str, Any], source_key: str, spec: SourceSpec, other_control_columns: Mapping[str, str]
) -> None:
    """A prefixed column that belongs to a DIFFERENT source type is an error, not a silent
    ignore (docs/build_log/DECISIONS.md D-01 point 4).

    A column this spec itself declares is exempt even if some other type happens to declare
    the identical name; everything else in `other_control_columns` is, by construction, a
    column this row's own source type has no business setting.
    """
    for column, owner_type in other_control_columns.items():
        if column in spec.control_columns or row.get(column) is None:
            continue
        raise ConfigError(
            f"'{column}' is set for source_key '{source_key}', whose source_type is "
            f"'{spec.source_type}' - '{column}' belongs to source_type '{owner_type}'. Fix "
            "the row, or clear the column if it was set by mistake."
        )


def _parse_json_column(raw: Any, control_table: str, source_key: str, column: str) -> dict[str, Any]:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"{control_table}.{column} for source_key '{source_key}' is not valid JSON: {exc}. "
            'It must be a JSON object, e.g. {"batch_limit": 100000}.'
        ) from exc
    if not isinstance(parsed, dict):
        raise ConfigError(
            f"{control_table}.{column} for source_key '{source_key}' parsed as "
            f"{type(parsed).__name__}, not a JSON object of setting -> value."
        )
    return parsed


def _safe(source_key: str) -> str:
    if not _SAFE_KEY.match(source_key):
        raise ConfigError(f"source_key '{source_key}' contains characters that are not allowed in a config key")
    return source_key
