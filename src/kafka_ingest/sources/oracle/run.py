"""The oracle source's one function. `run(ctx)` is the WHOLE contract, with SOURCE_SPEC.

There is no `read()`, `parse()`, `write()` or `validate()` here and none may be added -
see framework/contracts.py.

THE ORDER IS THE DESIGN, AND IT IS NOT NEGOTIABLE
-------------------------------------------------
    1. capture the high-water mark      before the extract, so the interval has an end
    2. read the closed interval         > last_watermark AND <= high_water
    3. write landing, and let it commit
    4. ONLY THEN advance the watermark

A crash anywhere before step 4 leaves the stored watermark where it was, so the next run
re-extracts the same interval. That is the safe direction: the re-extract is absorbed by
the MERGE key (or, for a source that waived merge keys, produces duplicate rows a support
engineer can see and delete). Advancing first would mean a crash silently skipped a window,
which nothing downstream could detect.

A REPLAY NEVER WRITES `ingest_state`. It re-extracts an interval a human named, while the
scheduled run keeps its own position - so the two cannot interfere, and a replay cannot
strand production state at a bound somebody typed for one incident.

A FULL RUN NEVER TOUCHES THE WATERMARK EITHER. Support can switch a source to a full load
without a deploy (docs/build_log/DECISIONS.md D-09); the full run has no interval, so it has
nothing trustworthy to advance to, and clearing the watermark would make the switch BACK to
delta re-read the entire table.

WHAT THIS RUN CANNOT PROMISE
----------------------------
A row committed in Oracle AFTER the high-water mark is captured, but carrying a cursor value
BELOW it, is never seen: the interval that would have contained it has already been read and
the watermark has moved past. That is inherent to a cursor over a wall-clock column - the
fix is a change-tracking mechanism (SCN, flashback, CDC), not a bigger interval - and it is
recorded as a known limitation in docs/DESIGN.md rather than papered over.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from ...framework.contracts import RunContext, RunResult
from ...framework.security import SecretResolver
from ...framework.state import STATE_WATERMARK, TYPE_STRING
from . import config as oracle_config
from . import landing, reader, tables, types
from .query import build_query, high_water_query

LAYER_LANDING = "landing"


def run(ctx: RunContext, secrets: Any = None) -> RunResult:
    """Resolve, capture the bound, read, write, then advance. In that order.

    `secrets` is an optional injection point and not part of the contract: a run builds its
    own SecretResolver, and a test supplies a stand-in rather than a workspace.
    """
    cfg = oracle_config.build(ctx.cfg, ctx.run_type, ctx.tables)
    resolver = secrets or SecretResolver()

    ctx.audit.source_ref = cfg.source_ref
    if cfg.replay.rerun_id:
        ctx.audit.rerun_id = cfg.replay.rerun_id
    _log_resolved(ctx, cfg)

    # 1. THE UPPER BOUND, captured before anything is extracted.
    last_watermark, high_water = _interval(ctx, cfg, resolver)
    if cfg.is_cursor and high_water is None:
        # Nothing the extract can see has a cursor value at all - an empty table, or a
        # filter that currently matches nothing. There is no interval, so there is nothing
        # to read and nothing to advance to.
        ctx.log.info("oracle_nothing_to_extract", reason="no cursor values visible to this extract")
        return _result(cfg, rows=0, written={}, query=None, start=last_watermark, end=None)

    query = build_query(cfg, last_watermark, high_water)
    ctx.log.info("oracle_extract_query", query=query, position_start=last_watermark, position_end=high_water)

    # 2. THE READ.
    bounds = reader.partition_bounds(ctx.spark, cfg, resolver, query)
    frame = reader.read(ctx.spark, cfg, resolver, query, bounds)
    types.refuse_unmapped(frame.schema, cfg.source_key)
    _refuse_schema_drift(ctx, cfg, frame.schema)
    tables.ensure_landing_table(ctx, cfg, frame.schema)

    # 3. THE WRITE. Returns once the write has committed, or raises.
    rows = _write(ctx, cfg, frame)

    # 4. AND ONLY NOW the watermark. Nothing between this line and the write may raise
    #    without the run failing - which is exactly why framework/state.py's writes are
    #    mandatory and must not be caught here.
    _advance_watermark(ctx, cfg, high_water)

    return _result(cfg, rows=rows, written={LAYER_LANDING: rows}, query=query, start=last_watermark, end=high_water)


# --------------------------------------------------------------------------------------
# The interval
# --------------------------------------------------------------------------------------


def _interval(ctx: RunContext, cfg: Any, secrets: Any) -> tuple[str | None, str | None]:
    """(last_watermark, high_water) for this run. Both None for a non-cursor extract.

    A replay's bounds come from the operator, not from state - it never reads the stored
    watermark and never writes one. An unbounded replay ("from there to now") still gets a
    real upper bound, captured the same way a scheduled run's is.
    """
    if not cfg.is_cursor:
        return (None, None)
    if cfg.is_replay:
        return (cfg.replay.cursor_start, cfg.replay.cursor_end or _capture_high_water(ctx, cfg, secrets))
    stored = ctx.state.read_state(cfg.source_key, STATE_WATERMARK)
    if stored is None:
        ctx.log.info("oracle_first_cursor_run", detail="no stored watermark; reading everything up to the high water")
    return (stored, _capture_high_water(ctx, cfg, secrets))


def _capture_high_water(ctx: RunContext, cfg: Any, secrets: Any) -> str | None:
    """`SELECT MAX(cursor)` over what this extract can see, as text.

    Taken as a VALUE THAT EXISTS in the data rather than from a clock: a clock reading is
    ahead of every committed row by definition, so it would move the watermark past rows
    that were still in flight. This is not a complete answer either - see the module
    docstring - but it never claims to have read past the last row it saw.
    """
    row = reader.read_scalar_row(ctx.spark, cfg, secrets, high_water_query(cfg))
    value = row["high_water"] if row else None
    return None if value is None else _watermark_text(value)


def _watermark_text(value: Any) -> str:
    """One value from the database, as the text `ingest_state` stores.

    The shapes query.py's `_literal()` will accept, and no others - so a value this
    function cannot render is caught here, before the write, rather than on the next run
    when the watermark it wrote turns out to be unusable.
    """
    if isinstance(value, datetime):
        micro = value.microsecond
        return value.strftime("%Y-%m-%d %H:%M:%S.%f") if micro else value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        # An Oracle DATE that reached Spark as a plain date - VB-03's other outcome. Midnight
        # is the only honest reading of it, and it is what a comparison would use anyway.
        return f"{value.isoformat()} 00:00:00"
    return str(value)


def _advance_watermark(ctx: RunContext, cfg: Any, high_water: str | None) -> None:
    """Step 4, and the three cases where it must NOT happen.

    Every skip is logged. A watermark that silently did not move looks identical to one
    that did until the next run reads the same interval again.
    """
    if cfg.is_replay:
        ctx.log.info("oracle_watermark_not_advanced", reason="replay", rerun_id=cfg.replay.rerun_id)
        return
    if high_water is None:
        # TWO CASES, ONE GUARD. A non-cursor run has no interval at all - `_interval()`
        # returns (None, None) for it - and a cursor run whose probe saw no rows has nothing
        # to advance TO. Neither may move the watermark, and for a full run that is D-09's
        # rule: clearing it would make the switch back to delta re-read the whole table.
        ctx.log.info(
            "oracle_watermark_not_advanced",
            reason="not a cursor extract" if not cfg.is_cursor else "no high-water mark was captured",
            mode=cfg.incremental_mode,
        )
        return
    ctx.state.write_state(cfg.source_key, STATE_WATERMARK, high_water, TYPE_STRING, ctx.run_id)
    ctx.log.info("oracle_watermark_advanced", watermark=high_water)


# --------------------------------------------------------------------------------------
# The write
# --------------------------------------------------------------------------------------


def _write(ctx: RunContext, cfg: Any, frame: Any) -> int:
    """Count and write from ONE evaluation of the frame, then return the row count.

    Cached deliberately: `count()` and the write are two actions, and without a cache the
    JDBC extract would run TWICE - two full reads of the source table, and two chances for
    the two of them to disagree about what Oracle held.
    """
    projected = landing.project(frame, cfg, ctx.run_sequence, ctx.run_id)
    cached = _persist(projected)
    try:
        rows = cached.count()
        if rows:
            _write_landing(ctx, cfg, cached)
        else:
            ctx.log.info("oracle_empty_interval", detail="the interval matched no rows; nothing written")
        return rows
    finally:
        # On the failure path too: a run that dies after caching a large extract would
        # otherwise leave it pinned until the executor is recycled.
        cached.unpersist()


def _persist(frame: Any) -> Any:
    """MEMORY_AND_DISK, imported inside the function so this module stays importable with
    no PySpark - which is what lets the whole lifecycle be tested against stand-ins."""
    from pyspark.storagelevel import StorageLevel

    return frame.persist(StorageLevel.MEMORY_AND_DISK)


def _write_landing(ctx: RunContext, cfg: Any, frame: Any) -> None:
    """MERGE when the source has a key, append when it has deliberately waived one.

    THE PARTITION PREDICATE IS `true`, and that is deliberate rather than lazy. Landing is
    partitioned by `ingest_date` - the date a row was WRITTEN - so a re-extracted row
    carries today's while the row it should match carries the day it first arrived. Any
    bound derived from this frame would therefore match NOTHING and insert duplicates,
    which is worse than the full scan it would save. `framework/writers.merge()` requires
    the predicate as an argument precisely so that passing `true` is a decision somebody
    wrote down; this is that decision. See the same call in sources/kafka/run.py.
    """
    if not cfg.merge_on:
        # Delta's idempotent-write markers make a RETRY of this exact run a no-op. They do
        # not make a re-extracted interval one - nothing can, without a key - which is what
        # the startup warning above is about.
        ctx.writers.append(
            frame,
            cfg.landing_table,
            txn_app_id=cfg.txn_app_id,
            txn_version=ctx.run_sequence,
            partition_by=list(cfg.landing_partition_by),
        )
        return
    if not ctx.tables.table_exists(ctx.spark, cfg.landing_table):
        # Nothing to merge against on the very first run. The table was created moments ago
        # and is empty, so an append and a merge would produce the same rows.
        ctx.writers.append(
            frame,
            cfg.landing_table,
            txn_app_id=cfg.txn_app_id,
            txn_version=ctx.run_sequence,
            partition_by=list(cfg.landing_partition_by),
        )
        return
    ctx.writers.merge(
        ctx.spark,
        frame,
        cfg.landing_table,
        cfg.merge_on,
        "true",
        update_matched=cfg.update_matched_rows,
    )


def _refuse_schema_drift(ctx: RunContext, cfg: Any, schema: Any) -> None:
    """Compare what the extract returned against what the landing table already holds.

    Skipped on the first run, when there is no table to compare with. A NEW column is
    additive and allowed; a changed type or a column that stopped being returned stops the
    run - see sources/oracle/types.py for why that is a failure and not a warning.
    """
    if not ctx.tables.table_exists(ctx.spark, cfg.landing_table):
        return
    existing = types.describe(ctx.spark.table(cfg.landing_table).schema)
    incoming = types.describe(schema)
    # The metadata columns are this framework's, not the source's, and they are present in
    # the target and absent from the extract by construction.
    for column in landing.METADATA_COLUMNS:
        existing.pop(column, None)
    types.assert_no_drift(existing, incoming, cfg.source_key, cfg.landing_table)


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------


def _log_resolved(ctx: RunContext, cfg: Any) -> None:
    """One line saying what this run is about to do, plus the two standing warnings.

    Both warnings describe a configuration that is legal, deliberate and lossy in a way
    that is invisible in a row count - which is the only kind of thing worth a WARN on every
    single run.
    """
    ctx.log.info(
        "oracle_run_resolved",
        source_ref=cfg.source_ref,
        landing=cfg.landing_table,
        jdbc=cfg.jdbc_ref,
        mode=cfg.incremental_mode,
        merge_on=",".join(cfg.merge_on) or "-",
        num_partitions=cfg.num_partitions,
        txn_app_id=cfg.txn_app_id,
    )
    if cfg.is_cursor and not cfg.merge_keys:
        ctx.log.warning(
            "oracle_boundary_rows_can_be_lost",
            detail=(
                "merge_keys is empty, so this source appends and its cursor predicate excludes "
                "the last watermark. A row committed in Oracle with exactly that cursor value, "
                "after the previous run passed it, will never be extracted. Set merge_keys to "
                "make the boundary safe."
            ),
            cursor_column=cfg.cursor_column,
        )
    if cfg.num_partitions <= 1:
        ctx.log.warning(
            "oracle_serial_read",
            detail=(
                "num_partitions is 1, so this extract runs on ONE executor whatever the cluster "
                "size. Set partition_column and num_partitions once the source team has "
                "confirmed a column to split on."
            ),
        )


def _result(cfg: Any, rows: int, written: dict, query: str | None, start: Any, end: Any) -> RunResult:
    """What the runner audits.

    `source_detail` carries THE QUERY, and that is the point of it: "what did this run
    actually ask Oracle for" is the first question of every Oracle incident, and once a
    dynamic window and a watermark are involved it is not reconstructable from the
    configuration afterwards.

    `pending_work` is None rather than 0: knowing how much the source still holds would
    take another round trip, and a confident zero from a source that never checked is the
    claim a permanently-lagging feed makes falsely.
    """
    return RunResult(
        rows_read=rows,
        rows_written=dict(written),
        rows_quarantined=0,
        position_start=start,
        position_end=end,
        source_detail=json.dumps(
            {
                "source_ref": cfg.source_ref,
                "jdbc_ref": cfg.jdbc_ref,
                "incremental_mode": cfg.incremental_mode,
                "cursor_column": cfg.cursor_column,
                "merge_on": list(cfg.merge_on),
                "num_partitions": cfg.num_partitions,
                "fetch_size": cfg.fetch_size,
                "query": query,
                "txn_app_id": cfg.txn_app_id,
            },
            sort_keys=True,
        ),
        pending_work=None,
    )
