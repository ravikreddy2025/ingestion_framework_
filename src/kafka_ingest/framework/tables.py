"""Target names and table creation. The only module that issues CREATE TABLE.

Two jobs, and nothing else belongs here:

  NAMING   which table does this source write for this layer? The answer is always a
           configured pattern, never a name built in code. `render()` fills a pattern's
           {tokens} from values the caller supplies; `targets()` reads the resolved names
           back out of the configuration by the one convention below.

  CREATION `ensure_table()` - CREATE TABLE IF NOT EXISTS with the configured
           TBLPROPERTIES and exactly one physical layout clause.

THE ONE NAMING CONVENTION
-------------------------
A source's target table for layer L is the setting `<L>_table`. `landing` -> landing_table,
`curated` -> curated_table, `quarantine` -> quarantine_table. The layers themselves come
from the source's SOURCE_SPEC, so this module resolves targets for any source type without
knowing one exists - and framework/config.py accepts those keys for the same reason.

WHY NAMES ARE VALIDATED AT CONFIG LOAD
--------------------------------------
`validate_name()` runs during run start-up, before anything connects, because the failure
it catches is a name that is perfectly legal in the system being read and illegal in Unity
Catalog - a table with a `$` or a `#` in it, or a two-part name. Discovering that at write
time means discovering it after the read has already cost an hour.

WHAT IS NOT HERE
----------------
GRANTs and CREATE SCHEMA. Both need privileges the ingestion service principal should not
have - a job that can grant is a job that can grant itself more - and both name environment
-specific principals that have no business in source-agnostic code. They live in sql/01 and
sql/02, which a platform admin runs when provisioning an environment.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Mapping

from .config import ConfigError

LOG = logging.getLogger(__name__)

# Unity Catalog needs catalog.schema.table. A two-part name resolves against whatever the
# session default happens to be, which is how a dev run writes into prod.
_NAME_PARTS = 3

# An unquoted Unity Catalog identifier. Anything outside this needs backticks, and a name
# that needs backticks is a name nobody will type correctly in a support query.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Same shape as framework/config.py's, deliberately: a pattern is configuration, and a
# reader should not have to learn two placeholder syntaxes.
_TOKEN = re.compile(r"\{([a-z_][a-z0-9_]*)\}")

# Used only when a configuration supplies no table_properties at all. The real defaults
# live in conf/defaults.yaml so they are reviewable and overridable per source.
_FALLBACK_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


# --------------------------------------------------------------------------------------
# Naming
# --------------------------------------------------------------------------------------


def render(pattern: str, tokens: Mapping[str, Any], where: str) -> str:
    """Fill a target-name pattern from values the caller derived.

    This is what makes a name like `{catalog}.landing.{source_schema}_{source_table}`
    possible without the framework knowing what a source_schema is: the source computes
    its own tokens and hands them over, and the pattern stays in
    conf/defaults/<source_type>.yaml where it is reviewable.

    An unresolved token is an error naming the pattern and the token, for the same reason
    an unresolved {placeholder} is one in config.py - a table literally called
    `{catalog}.landing.{source_table}` fails much later and much less clearly.
    """

    def _replace(match: re.Match) -> str:
        name = match.group(1)
        if name not in tokens:
            raise ConfigError(
                f"{where}: target pattern '{pattern}' uses {{{name}}}, which the source did "
                f"not supply. Available: {sorted(tokens)}."
            )
        return str(tokens[name])

    return _TOKEN.sub(_replace, pattern)


def validate_name(name: Any, where: str) -> str:
    """Three parts, each a legal unquoted Unity Catalog identifier. Anything else raises."""
    if not isinstance(name, str) or not name.strip():
        raise ConfigError(f"{where}: expected a table name, got {name!r}")
    parts = name.split(".")
    if len(parts) != _NAME_PARTS:
        raise ConfigError(
            f"{where}: '{name}' is not a three-part Unity Catalog name "
            "(catalog.schema.table). A shorter name resolves against whatever the session "
            "default happens to be."
        )
    for part in parts:
        if not _IDENTIFIER.match(part):
            raise ConfigError(
                f"{where}: '{name}' contains the part '{part}', which is not a legal "
                "unquoted Unity Catalog identifier. Legal in the source system is not the "
                "same as legal here - map it to an underscored name in the target pattern."
            )
    return name


def targets(cfg: Any) -> dict[str, str]:
    """Layer -> validated table name, for every layer this source type has.

    `cfg` is a framework/config.py ResolvedConfig. The layers come from its spec, so this
    returns one entry per layer with no knowledge of which source type is running.
    """
    resolved: dict[str, str] = {}
    for layer in cfg.layers:
        setting = f"{layer}_table"
        resolved[layer] = validate_name(cfg.get(setting), f"source '{cfg.source_key}' setting '{setting}'")
    return resolved


# The framework's own tables. audit and state are REQUIRED of every configuration: a run
# with nowhere to record what it did, or nowhere to keep its run sequence, is a run whose
# re-run behaviour nobody can reason about. control_table is optional - a fresh environment
# with no support table yet simply has no operational overrides.
REQUIRED_FRAMEWORK_TABLES = ("audit_table", "state_table")
OPTIONAL_FRAMEWORK_TABLES = ("control_table",)


def validate_targets(cfg: Any) -> dict[str, str]:
    """Every table name this run will touch, validated before anything connects.

    Called by the runner immediately after configuration resolves. Returns the names purely
    so a caller can log them; the value is the validation.
    """
    names = targets(cfg)
    for setting in REQUIRED_FRAMEWORK_TABLES:
        names[setting] = validate_name(cfg.get(setting), f"source '{cfg.source_key}' setting '{setting}'")
    for setting in OPTIONAL_FRAMEWORK_TABLES:
        value = cfg.get(setting)
        if value is not None:
            names[setting] = validate_name(value, f"source '{cfg.source_key}' setting '{setting}'")
    return names


# --------------------------------------------------------------------------------------
# Creation
# --------------------------------------------------------------------------------------


def table_exists(spark: Any, name: str) -> bool:
    return spark.catalog.tableExists(name)


def ensure_table(
    spark: Any,
    name: str,
    columns: str,
    comment: str,
    properties: Mapping[str, Any] | None = None,
    partition_by: list[str] | None = None,
    cluster_by: list[str] | None = None,
) -> None:
    """CREATE TABLE IF NOT EXISTS. Idempotent, and a metadata no-op once the table exists.

    `columns` is a DDL column list. Creating tables explicitly rather than letting the
    first write infer them is what makes the physical layout part of the code: an
    implicitly created table gets no TBLPROPERTIES, so the table people actually query
    ends up the only one without auto-compaction.
    """
    validate_name(name, f"table '{name}'")
    layout = _layout_clause(name, partition_by, cluster_by)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {name} (
        {columns}
        )
        USING DELTA
        {layout}
        COMMENT '{_quotable(comment, "comment")}'
        TBLPROPERTIES ({properties_clause(properties)})
        """
    )


def _layout_clause(name: str, partition_by: list[str] | None, cluster_by: list[str] | None) -> str:
    """PARTITIONED BY or CLUSTER BY, never both - Delta accepts one or the other.

    Rejected here rather than left to Delta because the error a cluster returns for this
    names neither the table nor the setting that caused it.
    """
    if partition_by and cluster_by:
        raise ConfigError(
            f"table '{name}' asks for both PARTITIONED BY {partition_by} and CLUSTER BY "
            f"{cluster_by}. Delta accepts one or the other - choose."
        )
    if partition_by:
        return f"PARTITIONED BY ({', '.join(partition_by)})"
    if cluster_by:
        return f"CLUSTER BY ({', '.join(cluster_by)})"
    return ""


def properties_clause(properties: Mapping[str, Any] | None) -> str:
    """Render TBLPROPERTIES from configuration.

    Keys and values are quoted as SQL string literals. A single quote in either would break
    the statement, so it is rejected rather than escaped - a Delta property name containing
    a quote is a typo, not a use case.
    """
    effective = dict(properties) if properties else dict(_FALLBACK_TABLE_PROPERTIES)
    return ", ".join(
        f"'{_quotable(key, 'table_properties key')}' = '{_quotable(value, 'table_properties value')}'"
        for key, value in effective.items()
    )


def _quotable(value: Any, what: str) -> str:
    text = str(value)
    if "'" in text:
        raise ConfigError(f"{what} {text!r} contains a single quote, which cannot be embedded in the generated DDL.")
    return text
