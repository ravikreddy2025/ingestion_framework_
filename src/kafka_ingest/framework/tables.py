"""Target names and table creation. The only module that issues CREATE TABLE.

Two jobs, and nothing else belongs here:

  NAMING   which table does this source write for this layer? The answer is always a
           configured pattern, never a name built in code. `target()` renders one and
           validates the result; `targets()` does it for every layer the source type has;
           `render()` is the substitution underneath both.

  CREATION `ensure_table()` - CREATE TABLE IF NOT EXISTS with the configured
           TBLPROPERTIES and exactly one physical layout clause.

THE ONE NAMING CONVENTION
-------------------------
A source's target table for layer L is the setting `<L>_table`. `landing` -> landing_table,
`curated` -> curated_table, `quarantine` -> quarantine_table. The layers themselves come
from the source's SOURCE_SPEC, so this module resolves targets for any source type without
knowing one exists - and framework/config.py accepts those keys for the same reason.

WHY NAMES ARE VALIDATED BEFORE ANYTHING CONNECTS
------------------------------------------------
`validate_name()` runs at run start-up because the failure it catches is a name that is
perfectly legal in the system being read and illegal in Unity Catalog - a table with a `$`
or a `#` in it, or a two-part name. Discovering that at write time means discovering it
after the read has already cost an hour.

There are two moments, not one, and the difference is who knows the name:

  the runner        `validate_targets(cfg)`, for every name configuration fully resolved.
  the source        `target(cfg, layer, tokens)`, at the top of run() and still before any
                    read, for a name whose pattern carries a token only the source can
                    fill - a Kafka topic turned into an identifier, a database SCHEMA and
                    TABLE. The token survived configuration load because that source type
                    DECLARED it in SOURCE_SPEC.target_tokens; anything undeclared already
                    failed in config.py.

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


def is_deferred(name: Any) -> bool:
    """Does this configured name still carry a token only the source can fill?

    A `{token}` can only survive configuration load if the source type DECLARED it in
    SOURCE_SPEC.target_tokens - config.py fails on any other one - so a brace here is
    never a typo that slipped through. It means "the source has not rendered this yet".
    """
    return isinstance(name, str) and bool(_TOKEN.search(name))


def target(cfg: Any, layer: str, tokens: Mapping[str, Any] | None = None) -> str:
    """The final table name for one layer: render the pattern, then validate the result.

    THIS is what a source calls, once, at the top of run() - before it reads anything - for
    a target whose name it has to complete itself. `tokens` are the values it derived:
    {"topic_table": "vector_patient_events_v1"}, {"source_table": "employees"}. A source
    whose pattern has no tokens can call it with none and get the same validation.
    """
    setting = f"{layer}_table"
    where = f"source '{cfg.source_key}' setting '{setting}'"
    pattern = cfg.get(setting)
    if pattern is None:
        raise ConfigError(f"{where}: not set. Every layer a source type declares needs one.")
    return validate_name(render(pattern, tokens or {}, where), where)


def targets(cfg: Any, tokens: Mapping[str, Any] | None = None) -> dict[str, str]:
    """Layer -> validated table name, for every layer this source type has.

    `cfg` is a framework/config.py ResolvedConfig. The layers come from its spec, so this
    returns one entry per layer with no knowledge of which source type is running.
    """
    return {layer: target(cfg, layer, tokens) for layer in cfg.layers}


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

    A layer whose pattern still carries a source-supplied token is SKIPPED here and
    validated by `target()` when the source renders it, at the top of run() and still
    before any read. That is the honest boundary: the framework cannot check a name it
    cannot yet know, and pretending otherwise would mean either validating a string with a
    brace in it or making the source hand its tokens to the runner - which would put a
    source's own vocabulary into framework/.
    """
    names = {}
    for layer in cfg.layers:
        pattern = cfg.get(f"{layer}_table")
        if is_deferred(pattern):
            LOG.info(
                "Target for layer '%s' is '%s'; the source renders and validates it at the start of its run.",
                layer,
                pattern,
            )
            continue
        names[layer] = target(cfg, layer)
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


def effective_properties(properties: Mapping[str, Any] | None) -> dict[str, Any]:
    """What a table actually gets: the configured map, or the fallback when none is set.

    Exposed separately from `properties_clause` so a caller that needs to ADD one more
    property on top of whatever configuration would otherwise produce - framework/state.py
    does, for deletion vectors - starts from the same answer `properties_clause` renders,
    rather than re-deciding the fallback itself.
    """
    return dict(properties) if properties else dict(_FALLBACK_TABLE_PROPERTIES)


def properties_clause(properties: Mapping[str, Any] | None) -> str:
    """Render TBLPROPERTIES from configuration.

    Keys and values are quoted as SQL string literals. A single quote in either would break
    the statement, so it is rejected rather than escaped - a Delta property name containing
    a quote is a typo, not a use case.
    """
    effective = effective_properties(properties)
    return ", ".join(
        f"'{_quotable(key, 'table_properties key')}' = '{_quotable(value, 'table_properties value')}'"
        for key, value in effective.items()
    )


def _quotable(value: Any, what: str) -> str:
    text = str(value)
    if "'" in text:
        raise ConfigError(f"{what} {text!r} contains a single quote, which cannot be embedded in the generated DDL.")
    return text
