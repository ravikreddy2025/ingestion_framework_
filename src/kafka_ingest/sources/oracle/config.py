"""The resolved settings this source actually runs on. NO PySpark import.

framework/config.py hands every source a `ResolvedConfig`: identity, the declared layers,
a read-only settings mapping and the registers. This module turns that generic mapping
into ONE frozen dataclass with names, types and validation - so the rest of the package
reads `cfg.source_table` rather than `ctx.cfg.get("source_table")`, and a typo is an
AttributeError rather than a None that reaches a database.

The framework validates KEYS against SOURCE_SPEC; this module validates VALUES - the
enumerations, the cross-field rules, and the two rules that must fail at CONFIG LOAD
rather than at read time:

  * every identifier that will be pasted into SQL is checked against a conservative
    pattern. `filter_criteria` is a SQL fragment by design, so it gets the strictest
    check in the file - see `_check_filter_criteria`.
  * an Oracle name that is legal in Oracle and illegal in Unity Catalog fails here.
    `CLAIMS.CLAIM_HEADER` becomes `{catalog}.oracle_claims.claim_header`; a name with a
    `$` or a `#` in it has no legal target and must be caught before an hour-long read.

ORACLE FOLDS UNQUOTED IDENTIFIERS TO UPPER CASE, and Unity Catalog folds to lower. Both
truths live in `target_tokens()`, which is the one function that crosses between them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from ...framework.config import ConfigError
from .spec import COLUMNS, DYNAMIC_DATE_FILTER, FILTER_COLUMN, FILTER_CRITERIA, SQL_QUERY

# How much of the table this run extracts.
#
#   full    everything, every run. The right shape for a small reference table.
#   cursor  rows whose cursor column moved since the last committed watermark. Needs
#           durable state, which is why framework/state.py exists.
#   filter  a fixed predicate, evaluated fresh every run. No state, no watermark - the
#           right shape for "all open claims", where the ROW SET changes but the
#           boundary does not.
MODE_FULL = "full"
MODE_CURSOR = "cursor"
MODE_FILTER = "filter"
VALID_INCREMENTAL_MODES = (MODE_FULL, MODE_CURSOR, MODE_FILTER)

# What the cursor column holds. This decides how a watermark is rendered back into SQL,
# and getting it wrong is a comparison against the wrong type rather than an error.
CURSOR_TIMESTAMP = "timestamp"
CURSOR_NUMBER = "number"
VALID_CURSOR_TYPES = (CURSOR_TIMESTAMP, CURSOR_NUMBER)

# An Oracle identifier we are willing to paste into generated SQL unquoted. Oracle also
# allows `$` and `#` in unquoted names, and both are legal here because they are legal
# THERE - what they are not is legal in a Unity Catalog identifier, which is why
# `target_tokens()` checks the derived target name separately and rejects them.
#
# Quoted, case-sensitive Oracle identifiers are deliberately NOT supported: they would
# have to survive into a target name, and "Claim Header" has no unquoted UC equivalent.
_ORACLE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")

# A Unity Catalog identifier that needs no backtick quoting. framework/tables.py applies
# the same rule to the assembled three-part name; this catches the part we derive.
_UC_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ISO-8601 durations, restricted to the two units a source-side retention window is ever
# expressed in. `P7D` = seven days, `PT12H` = twelve hours. Anything else is rejected
# rather than approximated - a month is not 30 days to everyone.
_DURATION = re.compile(r"^P(?:(?P<days>\d{1,4})D|T(?P<hours>\d{1,5})H)$")

# A SQL fragment support could not have written: statement separators, comment
# introducers, and the keywords that would turn a WHERE clause into something else.
# Checked as whole words so a column called `UPDATED_BY` or `CREATE_DT` is unaffected.
_FRAGMENT_FORBIDDEN_TEXT = (";", "--", "/*", "*/")
_FRAGMENT_FORBIDDEN_WORDS = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "COMMIT",
    "ROLLBACK",
    "EXECUTE",
    "EXEC",
    "BEGIN",
    "DECLARE",
    "UNION",
    "INTO",
)

# Statements `sql_query` may begin with. A common table expression is still one statement
# and still a read, so `WITH` is allowed; everything else is not.
_QUERY_OPENERS = ("SELECT", "WITH")


@dataclass(frozen=True)
class DateWindow:
    """The `dynamic_date_filter` block: a column, and how far back to look from now.

    Kept as an amount plus a unit rather than a rendered SQL fragment so that the SQL
    stays in one file (query.py) and the parsing stays in one file (this one).
    """

    column: str
    amount: int
    unit: str  # DAY | HOUR - the two Oracle INTERVAL units _DURATION admits


@dataclass(frozen=True)
class OracleConfig:
    """One table, one environment, one execution. Frozen: nothing mutates it mid-run."""

    source_key: str
    environment: str
    domain: str

    jdbc_ref: str
    jdbc_profile: Mapping[str, Any]

    source_schema: str
    source_table: str
    landing_table: str

    sql_query: str | None
    columns: tuple[str, ...]
    filter_column: str | None
    filter_criteria: str | None
    dynamic_date_filter: DateWindow | None

    incremental_mode: str
    cursor_column: str | None
    cursor_type: str | None
    merge_keys: tuple[str, ...]

    partition_column: str | None
    num_partitions: int
    fetch_size: int

    table_properties: Mapping[str, Any]

    # -- derived ---------------------------------------------------------------------

    @property
    def source_ref(self) -> str:
        """What the audit table records as this run's source-side identifier.

        Upper case, because that is the name Oracle actually holds - a support engineer
        pastes this straight into a session against the source database.
        """
        return f"{self.source_schema.upper()}.{self.source_table.upper()}"

    @property
    def is_cursor(self) -> bool:
        return self.incremental_mode == MODE_CURSOR

    @property
    def merge_on_write(self) -> bool:
        """Merge keys change CORRECTNESS here, not just performance.

        With them, a cursor run re-reads its own boundary (`>=`) and de-duplicates on
        write, so rows sharing the last watermark value cannot be lost. Without them the
        predicate has to exclude the boundary (`>`), and any row committed in Oracle with
        exactly that cursor value after the previous run read it is never seen again.
        See docs/CONFIGURATION.md and sub-step 4c.
        """
        return bool(self.merge_keys)


def build(cfg: Any, tables: Any) -> OracleConfig:
    """Turn a framework ResolvedConfig into a validated OracleConfig.

    `tables` is framework/tables.py, carried on the RunContext. It renders the landing
    pattern - which still holds {source_schema} and {source_table}, because only this
    source can supply them - and validates the resulting name before anything connects.
    """
    source_schema = _identifier(_required_text(cfg, "source_schema"), "source_schema", cfg.source_key)
    source_table = _identifier(_required_text(cfg, "source_table"), "source_table", cfg.source_key)
    jdbc_ref = _required_text(cfg, "jdbc_ref")
    incremental_mode = str(cfg.get("incremental_mode") or "").strip().lower()
    # Asked here rather than in _validate because the answer is "was the key SET", and by
    # the time the dataclass exists `merge_keys: []` and no merge_keys at all look alike.
    _require_a_merge_key_decision(cfg, incremental_mode)

    resolved = OracleConfig(
        source_key=cfg.source_key,
        environment=cfg.environment,
        domain=str(cfg.get("domain") or ""),
        jdbc_ref=jdbc_ref,
        # Resolved, not just named: `profile()` raises and lists the valid names, so a
        # typo in jdbc_ref fails at config load rather than as a connection error.
        # sub-step 4b turns this mapping into a URL and a credential lookup.
        jdbc_profile=dict(cfg.profile("jdbc", jdbc_ref)),
        source_schema=source_schema,
        source_table=source_table,
        landing_table=tables.target(cfg, "landing", target_tokens(source_schema, source_table, cfg.source_key)),
        sql_query=_text(cfg.get(SQL_QUERY)),
        columns=_identifiers(cfg, COLUMNS),
        filter_column=_optional_identifier(cfg, FILTER_COLUMN),
        filter_criteria=_text(cfg.get(FILTER_CRITERIA)),
        dynamic_date_filter=_date_window(cfg),
        incremental_mode=incremental_mode,
        cursor_column=_optional_identifier(cfg, "cursor_column"),
        cursor_type=_lower(cfg.get("cursor_type")),
        merge_keys=_identifiers(cfg, "merge_keys"),
        partition_column=_optional_identifier(cfg, "partition_column"),
        num_partitions=_positive_int(cfg.get("num_partitions"), "num_partitions", cfg.source_key),
        fetch_size=_positive_int(cfg.get("fetch_size"), "fetch_size", cfg.source_key),
        table_properties=cfg.get("table_properties") or {},
    )
    _validate(resolved)
    return resolved


def target_tokens(source_schema: str, source_table: str, source_key: str) -> dict[str, str]:
    """The two tokens the landing pattern in conf/defaults/oracle.yaml carries.

    THE CASE RULE, in one place. Oracle folds unquoted identifiers to upper case, so
    `CLAIMS.CLAIM_HEADER` is the real name over there; Unity Catalog folds to lower, so
    `oracle_claims.claim_header` is the real name over here. Configuration may be written
    in either case and resolves to the same target either way.

    An Oracle name containing `$` or `#` is legal in Oracle and illegal unquoted in Unity
    Catalog. It is rejected HERE, at config load, with the name and the reason - not left
    for framework/tables.py to reject as "not a legal identifier" after the read.
    """
    tokens = {"source_schema": source_schema.lower(), "source_table": source_table.lower()}
    for token, value in tokens.items():
        if not _UC_IDENTIFIER.match(value):
            raise ConfigError(
                f"source '{source_key}': {token} '{value}' is legal in Oracle but not as an "
                "unquoted Unity Catalog identifier, so no target name can be derived from it. "
                "Set `landing_table:` explicitly in the source file."
            )
    return tokens


# --------------------------------------------------------------------------------------
# Value-level rules the spec cannot express
# --------------------------------------------------------------------------------------


def _validate(cfg: OracleConfig) -> None:
    where = f"source '{cfg.source_key}'"
    if cfg.incremental_mode not in VALID_INCREMENTAL_MODES:
        raise ConfigError(
            f"{where}: incremental_mode '{cfg.incremental_mode}' not in {sorted(VALID_INCREMENTAL_MODES)}."
        )
    _check_mode_requirements(cfg, where)
    _check_filter_pair(cfg, where)
    _check_filter_criteria(cfg, where)
    _check_sql_query(cfg, where)
    _check_read_parallelism(cfg, where)


def _check_mode_requirements(cfg: OracleConfig, where: str) -> None:
    if cfg.incremental_mode == MODE_CURSOR:
        if not (cfg.cursor_column and cfg.cursor_type):
            raise ConfigError(
                f"{where}: incremental_mode 'cursor' requires cursor_column and cursor_type - "
                "without them there is nothing to compare the stored watermark against."
            )
        if cfg.cursor_type not in VALID_CURSOR_TYPES:
            raise ConfigError(f"{where}: cursor_type '{cfg.cursor_type}' not in {sorted(VALID_CURSOR_TYPES)}.")
    elif cfg.incremental_mode == MODE_FILTER and not (cfg.filter_column and cfg.filter_criteria):
        raise ConfigError(
            f"{where}: incremental_mode 'filter' requires filter_column and filter_criteria - "
            "they ARE the increment in this mode, and without them the run would extract the "
            "whole table while claiming to be incremental."
        )


def _require_a_merge_key_decision(cfg: Any, incremental_mode: str) -> None:
    """`merge_keys` absent is not the same as `merge_keys: []`, and that is the point.

    A cursor extract without merge keys appends, and a non-unique cursor - a
    second-granularity timestamp is the usual one - can then LOSE the rows sharing the
    boundary value. That is an acceptable trade for a table with no stable key, and it is
    never an acceptable accident, so this refuses to infer it from silence: the source
    file must say `merge_keys: []` to take it. CORE section 10 records the recommendation
    as "require unless explicitly waived"; this is what the waiver looks like.
    """
    if incremental_mode != MODE_CURSOR or cfg.get("merge_keys") is not None:
        return
    raise ConfigError(
        f"source '{cfg.source_key}': incremental_mode 'cursor' needs `merge_keys:` to be a "
        "decision, not a default. Set the table's stable key - the run then re-reads its own "
        "boundary and de-duplicates on write - or set `merge_keys: []` to accept that rows "
        "sharing the last watermark value can be lost. See the MUST-READ row in "
        "docs/CONFIGURATION.md."
    )


def _check_filter_pair(cfg: OracleConfig, where: str) -> None:
    """Neither half of the static filter means anything alone."""
    if bool(cfg.filter_column) != bool(cfg.filter_criteria):
        raise ConfigError(
            f"{where}: filter_column and filter_criteria are one setting in two parts - set "
            f"both or neither (got filter_column={cfg.filter_column!r}, "
            f"filter_criteria={cfg.filter_criteria!r})."
        )


def _check_filter_criteria(cfg: OracleConfig, where: str) -> None:
    """`filter_criteria` is a SQL fragment, and this is the only place that is true.

    It reaches the source database's parser verbatim, so it is validated against a
    conservative allowlist and it is STRUCTURAL - sources/oracle/spec.py deliberately
    leaves it out of `operational_keys`, so a control-table override of it is ignored and
    support cannot reach Oracle's parser through a table they can UPDATE.
    """
    if cfg.filter_criteria is None:
        return
    _check_sql_fragment(cfg.filter_criteria, f"{where}: filter_criteria")


def _check_sql_fragment(fragment: str, where: str) -> None:
    upper = fragment.upper()
    for text in _FRAGMENT_FORBIDDEN_TEXT:
        if text in fragment:
            raise ConfigError(
                f"{where}: '{fragment}' contains '{text}'. It is pasted into the extraction "
                "query verbatim, so statement separators and comment markers are refused."
            )
    for word in _FRAGMENT_FORBIDDEN_WORDS:
        if re.search(rf"\b{word}\b", upper):
            raise ConfigError(
                f"{where}: '{fragment}' contains the keyword '{word}'. A predicate needs none "
                "of them; anything that does belongs in `sql_query`, which is reviewed in a PR."
            )


def _check_sql_query(cfg: OracleConfig, where: str) -> None:
    """One statement, and it must be a read.

    The same forbidden-word list as a filter fragment would reject the query's own
    `SELECT`, so the check here is shape rather than vocabulary: what it starts with, and
    that it contains no statement separator or comment marker to hide a second statement
    behind.
    """
    if cfg.sql_query is None:
        return
    query = cfg.sql_query.strip()
    if not query.upper().startswith(_QUERY_OPENERS):
        raise ConfigError(
            f"{where}: sql_query must be a single SELECT (or a WITH ... SELECT), got "
            f"'{query[:60]}...'. This framework extracts; it does not modify the source."
        )
    for text in _FRAGMENT_FORBIDDEN_TEXT:
        # A trailing semicolon is still a statement separator once the query is wrapped in
        # `SELECT * FROM ( ... )`, so it is refused rather than stripped.
        if text in query:
            raise ConfigError(
                f"{where}: sql_query contains '{text}'. It is wrapped in an inline view, so a "
                "statement separator or comment marker either breaks the read or hides a "
                "second statement inside it."
            )


def _check_read_parallelism(cfg: OracleConfig, where: str) -> None:
    """`numPartitions` without a `partitionColumn` does not split anything.

    Spark needs a column, a lower bound and an upper bound to generate the per-partition
    WHERE clauses; given only a partition count it issues ONE query on ONE executor. The
    read is then single-threaded regardless of cluster size - Oracle's equivalent of
    Kafka's minPartitions, and it fails as slowness rather than as an error, which is
    exactly why it is checked here.
    """
    if cfg.num_partitions > 1 and not cfg.partition_column:
        raise ConfigError(
            f"{where}: num_partitions={cfg.num_partitions} needs a partition_column. Without "
            "one Spark cannot split the read and issues a single query on a single executor, "
            "however large the cluster is. Name a numeric, date or timestamp column - those "
            "are the only types partitionColumn accepts - or set num_partitions: 1 to accept "
            "a serial read."
        )


# --------------------------------------------------------------------------------------
# Readers
# --------------------------------------------------------------------------------------


def _date_window(cfg: Any) -> DateWindow | None:
    raw = cfg.get(DYNAMIC_DATE_FILTER)
    if raw is None:
        return None
    where = f"source '{cfg.source_key}': {DYNAMIC_DATE_FILTER}"
    if not isinstance(raw, Mapping):
        raise ConfigError(f"{where} must be a mapping with `column:` and `window:`, got {type(raw).__name__}.")
    unknown = sorted(set(raw) - {"column", "window"})
    if unknown:
        raise ConfigError(f"{where} has unknown key(s) {unknown}. It takes `column:` and `window:` and nothing else.")
    column = _identifier(str(raw.get("column") or "").strip(), f"{DYNAMIC_DATE_FILTER}.column", cfg.source_key)
    window = str(raw.get("window") or "").strip().upper()
    match = _DURATION.match(window)
    if not match:
        raise ConfigError(
            f"{where}: window '{window}' is not a supported ISO-8601 duration. Use P<n>D for "
            "days or PT<n>H for hours - months and years are refused because their length "
            "depends on when you ask."
        )
    if match.group("days"):
        return DateWindow(column=column, amount=int(match.group("days")), unit="DAY")
    return DateWindow(column=column, amount=int(match.group("hours")), unit="HOUR")


def _identifier(value: str, key: str, source_key: str) -> str:
    if not _ORACLE_IDENTIFIER.match(value):
        raise ConfigError(
            f"source '{source_key}': '{key}' is '{value}', which is not an Oracle identifier "
            "this framework will paste into generated SQL. Letters, digits, underscore, `$` "
            "and `#`, starting with a letter, up to 128 characters. Quoted, case-sensitive "
            "names are not supported."
        )
    return value


def _optional_identifier(cfg: Any, key: str) -> str | None:
    value = _text(cfg.get(key))
    return None if value is None else _identifier(value, key, cfg.source_key)


def _identifiers(cfg: Any, key: str) -> tuple[str, ...]:
    value = cfg.get(key)
    if value is None:
        return ()
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        raise ConfigError(f"source '{cfg.source_key}': '{key}' must be a list of column names, got {value!r}.")
    return tuple(_identifier(str(item).strip(), key, cfg.source_key) for item in value)


def _required_text(cfg: Any, key: str) -> str:
    value = cfg.get(key)
    if not value or not str(value).strip():
        raise ConfigError(
            f"source '{cfg.source_key}': '{key}' is required and is empty. Set it in the "
            "source file, or in conf/defaults/oracle.yaml if every Oracle source shares it."
        )
    return str(value).strip()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _lower(value: Any) -> str | None:
    text = _text(value)
    return None if text is None else text.lower()


def _positive_int(value: Any, key: str, source_key: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"source '{source_key}': '{key}' must be an integer, got {value!r}") from exc
    if number <= 0:
        raise ConfigError(
            f"source '{source_key}': '{key}' must be greater than zero, got {number}. Unset or "
            "zero is not 'the driver default' - for fetch_size the driver default is TEN ROWS "
            "per round trip. See conf/defaults/oracle.yaml."
        )
    return number
