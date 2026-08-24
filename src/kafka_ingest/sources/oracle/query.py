"""The extraction query, as a string. No Spark, no database, no side effects.

ONE FUNCTION, ONE SHAPE. `build_query()` assembles the same four parts in the same order
every time:

    1. the base       `sql_query` if set, else SELECT <columns or *> FROM <schema>.<table>
    2. the static filter        filter_column + filter_criteria
    3. the dynamic date filter  dynamic_date_filter
    4. the incremental predicate, for `incremental_mode: cursor` only

This is deliberately not a SQL builder and must not become one: there is no expression
tree, no operator vocabulary, no composition. Every input it will ever take is already
validated by sources/oracle/config.py, and everything it can produce is enumerated by
tests/test_oracle_query.py.

WHY THE QUERY IS RECORDED IN THE AUDIT ROW
------------------------------------------
"What did this run actually ask Oracle for" is the first question of every Oracle
incident, and once a dynamic window and a watermark are involved it is not reconstructable
from the configuration afterwards - the configuration says `P7D`, not which seven days.
The caller (sources/oracle/run.py) puts the return value of this function into
`source_detail`.

WHY LITERALS AND NOT BIND VARIABLES
-----------------------------------
The watermark bounds are rendered as SQL literals, not `:last_watermark` binds. Spark's
JDBC source takes a query as a parenthesised subquery in `dbtable` and offers no way to
bind parameters to it, so a bind variable here would arrive at Oracle as text. That makes
the rendering the safety boundary, which is why `_literal()` accepts only a number or an
ISO-8601 timestamp and refuses everything else - including any watermark this framework
did not itself write into `ingest_state`.
"""

from __future__ import annotations

import re

from ...framework.config import ConfigError
from .config import CURSOR_NUMBER, CURSOR_TIMESTAMP, MODE_CURSOR, OracleConfig

# A watermark this module is willing to render into SQL. Both are deliberately narrower
# than what Oracle would accept: the value comes back from `ingest_state` as text, and the
# only text this framework ever writes there is one of these two shapes.
_NUMBER = re.compile(r"^-?\d{1,38}(\.\d{1,38})?$")
_TIMESTAMP = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})[ T](?P<time>\d{2}:\d{2}:\d{2})(?P<fraction>\.\d{1,9})?$")

# Oracle format models for the two timestamp shapes. `FF` matches whatever fractional
# precision the literal carries, so one model covers .1 through .123456789.
_TS_FORMAT = "YYYY-MM-DD HH24:MI:SS"
_TS_FORMAT_FRACTIONAL = "YYYY-MM-DD HH24:MI:SS.FF"


def build_query(
    cfg: OracleConfig,
    last_watermark: str | None = None,
    run_high_water: str | None = None,
) -> str:
    """The SQL this run sends to Oracle.

    `last_watermark` is what `ingest_state` holds for this source - None on the first ever
    cursor run, which reads everything up to the high-water mark. `run_high_water` is the
    upper bound captured at the start of THIS run, and for a cursor extract it is
    mandatory: see `_cursor_predicates`.

    Both are ignored by the other two modes, and passing one anyway is refused rather than
    dropped - a caller that hands a watermark to a `full` extract has misunderstood
    something, and silently extracting the whole table would hide it.
    """
    if cfg.incremental_mode != MODE_CURSOR and (last_watermark or run_high_water):
        raise ConfigError(
            f"source '{cfg.source_key}': a watermark was supplied for incremental_mode "
            f"'{cfg.incremental_mode}', which has no cursor. Only 'cursor' reads or advances one."
        )

    predicates = [
        *_static_filter(cfg),
        *_dynamic_date_filter(cfg),
        *_cursor_predicates(cfg, last_watermark, run_high_water),
    ]
    base = _base(cfg, bool(predicates))
    if not predicates:
        return base
    return f"{base} WHERE {' AND '.join(predicates)}"


def _base(cfg: OracleConfig, has_predicates: bool) -> str:
    """The FROM half. A hand-written query becomes an inline view so predicates can attach.

    A `sql_query` with nothing to append is returned untouched: what the audit row then
    shows is character-for-character what the source file asked for, which is what the
    person reading it during an incident is comparing against.
    """
    if cfg.sql_query:
        return f"SELECT * FROM ({cfg.sql_query}) src" if has_predicates else cfg.sql_query
    columns = ", ".join(cfg.columns) if cfg.columns else "*"
    return f"SELECT {columns} FROM {cfg.source_schema}.{cfg.source_table}"


def _static_filter(cfg: OracleConfig) -> list[str]:
    """`filter_column: STATUS` + `filter_criteria: "IN ('A','P')"` -> `STATUS IN ('A','P')`.

    The two are one setting in two parts and config.py refuses either alone, so this needs
    no half-set case.
    """
    if not cfg.filter_criteria:
        return []
    return [f"{cfg.filter_column} {cfg.filter_criteria}"]


def _dynamic_date_filter(cfg: OracleConfig) -> list[str]:
    """A rolling window, evaluated by ORACLE and not by the driver.

    `SYSTIMESTAMP` is the source database's clock, in the source database's time zone, and
    that is the intent: the window is about how far back the SOURCE retains changes.
    Computing it here instead would compare Databricks' clock against Oracle's data - see
    the verification backlog before assuming the two agree.
    """
    window = cfg.dynamic_date_filter
    if window is None:
        return []
    return [f"{window.column} >= SYSTIMESTAMP - INTERVAL '{window.amount}' {window.unit}"]


def _cursor_predicates(cfg: OracleConfig, last_watermark: str | None, run_high_water: str | None) -> list[str]:
    """THE CLOSED INTERVAL. This is the correctness core of the Oracle source.

        cursor > :last_watermark AND cursor <= :run_high_water

    The upper bound is not optional and is not a nicety. With an open upper bound, rows
    committed in Oracle WHILE the extract is running may or may not be read depending on
    when each JDBC partition happens to reach them - and the watermark afterwards advances
    past all of them regardless. It passes every test written against a static table and
    loses rows against a live one, which is why `run_high_water` is a required argument
    here rather than a defaulted one.

    THE LOWER BOUND'S OPERATOR IS DECIDED BY `merge_keys` (see 4c and OracleConfig), and
    by whether this is a REPLAY. A replay's start bound is a window boundary a human typed,
    so it is always inclusive: excluding it would silently drop the very rows the operator
    named, and a replay is a supervised action where re-reading one boundary value costs
    nothing.

      merge keys set     `>=`  re-reads the boundary, and the MERGE de-duplicates it.
                               Tie-safe on a non-unique cursor. The recommended default.
      merge keys waived  `>`   never re-reads, appends, and LOSES any row that arrived in
                               Oracle with exactly the boundary value after the previous
                               run passed it. Deliberate, documented, and asserted by a
                               test so it is a known property rather than a surprise.
    """
    if cfg.incremental_mode != MODE_CURSOR:
        return []
    if not run_high_water:
        raise ConfigError(
            f"source '{cfg.source_key}': a cursor extract needs an upper bound captured at the "
            "start of the run. Without one the read has no end, and rows committed during it "
            "are silently skipped by the watermark that follows."
        )
    column = cfg.cursor_column
    predicates = [f"{column} <= {_literal(cfg, run_high_water)}"]
    if last_watermark:
        operator = ">=" if (cfg.merge_on_write or cfg.is_replay) else ">"
        predicates.insert(0, f"{column} {operator} {_literal(cfg, last_watermark)}")
    return predicates


def bounds_query(cfg: OracleConfig, query: str) -> str:
    """MIN and MAX of the partition column, over the query this run is about to extract.

    Over the QUERY and not over the table, deliberately: bounds taken from the whole table
    would slice a filtered extract into partitions that are mostly empty, and the last one
    would do all the work. The column names are fixed here because reader.py reads them
    back by name - a positional read would break the day someone adds a third aggregate.
    """
    column = cfg.partition_column
    if not column:
        raise ConfigError(
            f"source '{cfg.source_key}': partition bounds were asked for, but no "
            "partition_column is configured. A serial read needs no bounds."
        )
    return f"SELECT MIN({column}) AS lower_bound, MAX({column}) AS upper_bound FROM ({query}) b"


def _literal(cfg: OracleConfig, value: str) -> str:
    """A watermark, rendered as an Oracle literal. The one place text becomes SQL.

    Refuses anything that is not a plain number or an ISO-8601 timestamp. A watermark
    reaches here from `ingest_state`, which only this framework writes - but "only this
    framework writes it" is a claim about a table a support engineer can UPDATE, so the
    check is here rather than assumed.
    """
    text = str(value).strip()
    if cfg.cursor_type == CURSOR_NUMBER:
        if not _NUMBER.match(text):
            raise ConfigError(
                f"source '{cfg.source_key}': cursor_type is 'number' but the watermark '{text}' "
                "is not one. It is rendered straight into the extraction query, so it is "
                "refused rather than quoted."
            )
        return text
    match = _TIMESTAMP.match(text)
    if cfg.cursor_type != CURSOR_TIMESTAMP or not match:
        raise ConfigError(
            f"source '{cfg.source_key}': watermark '{text}' is not an ISO-8601 timestamp "
            "(YYYY-MM-DD HH:MM:SS[.ffffff]). Check ingest_state for this source - a hand-edited "
            "watermark is the usual cause."
        )
    fraction = match.group("fraction") or ""
    model = _TS_FORMAT_FRACTIONAL if fraction else _TS_FORMAT
    return f"TO_TIMESTAMP('{match.group('date')} {match.group('time')}{fraction}', '{model}')"
