"""sources/oracle/run.py - the watermark lifecycle, and the order that makes it correct.

    capture the high water -> read the closed interval -> write -> THEN advance

Every test here is about that order or about a case where a step must NOT happen. A source
is handed a RunContext and nothing else, so the way to test one without a database is to
hand it a context of stand-ins and assert on what it did with them: which query it built,
which write it called, and - the one that matters most - whether `ingest_state` moved.

NOTHING HERE OPENS A CONNECTION. The reads are recorded, not performed.
"""

from __future__ import annotations

import importlib
import json

import pytest

from conftest import (
    FakeJdbcSpark,
    FakeSchema,
    FakeSecrets,
    LoadedFrame,
    RecordingState,
    make_oracle_ctx,
    write_oracle_source,
)
from kafka_ingest.framework.config import ConfigError

# importlib, not `from ... import run`: the package's __init__ re-exports the run FUNCTION
# under that name (it is the source's public surface), so a plain import would bind the
# function rather than the module - see tests/test_kafka_run.py, which does the same.
oracle_run = importlib.import_module("kafka_ingest.sources.oracle.run")

SOURCE_SCHEMA = FakeSchema({"CLAIM_ID": "decimal(38,0)", "LAST_UPDATE_DT": "timestamp", "STATUS": "string"})
WATERMARK = "2026-08-01 00:00:00"
HIGH_WATER = "2026-08-24 06:30:00"


def _cursor_source(config_root, **settings):
    write_oracle_source(
        config_root,
        incremental_mode="cursor",
        cursor_column="LAST_UPDATE_DT",
        cursor_type="timestamp",
        merge_keys=settings.pop("merge_keys", ["CLAIM_ID"]),
        **settings,
    )


def _spark(high_water=HIGH_WATER, rows=3, schema=SOURCE_SCHEMA, **kwargs):
    """A probe frame carrying the high-water mark, then the extract frame."""
    frames = []
    if high_water is not _NO_PROBE:
        frames.append(LoadedFrame([{"high_water": high_water}]))
    frames.append(LoadedFrame([{"CLAIM_ID": n} for n in range(rows)], schema=schema))
    return FakeJdbcSpark(frames=frames, **kwargs)


_NO_PROBE = object()


def _run(ctx, secrets=None):
    return oracle_run.run(ctx, secrets or FakeSecrets())


# --------------------------------------------------------------------------------------
# The order
# --------------------------------------------------------------------------------------


def test_a_cursor_run_reads_a_closed_interval_and_then_advances(oracle_config_root):
    """The whole lifecycle in one test: the stored watermark becomes the lower bound, the
    probe supplies the upper one, and the watermark ends where the probe said."""
    _cursor_source(oracle_config_root)
    state = RecordingState({("demo_oracle", "watermark"): WATERMARK})
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), state=state)

    result = _run(ctx)

    extract = ctx.spark.options_for(1)["dbtable"]
    assert "LAST_UPDATE_DT >= TO_TIMESTAMP('2026-08-01 00:00:00'" in extract
    assert "LAST_UPDATE_DT <= TO_TIMESTAMP('2026-08-24 06:30:00'" in extract
    assert state.watermark == HIGH_WATER
    assert (result.position_start, result.position_end) == (WATERMARK, HIGH_WATER)
    assert result.rows_written == {"landing": 3}


def test_the_watermark_is_written_after_the_write_and_never_before(oracle_config_root):
    """THE ORDERING RULE, asserted directly. A state write that landed first would mean a
    crash during the write silently skipped the interval it claimed to have read."""
    _cursor_source(oracle_config_root)
    order = []
    state = RecordingState(on_write=lambda: order.append("state"))
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), state=state)
    original_append = ctx.writers.append

    def recording_append(*args, **kwargs):
        order.append("write")
        return original_append(*args, **kwargs)

    ctx.writers.append = recording_append
    _run(ctx)

    assert order == ["write", "state"]


def test_a_failed_write_leaves_the_watermark_where_it_was(oracle_config_root):
    """A crash between read and advance re-extracts the same interval on the next run,
    which the MERGE key absorbs. The opposite - advancing first - loses the window."""
    _cursor_source(oracle_config_root)
    state = RecordingState({("demo_oracle", "watermark"): WATERMARK})
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), state=state)

    def explode(*args, **kwargs):
        raise RuntimeError("the cluster died mid-write")

    ctx.writers.append = explode
    with pytest.raises(RuntimeError):
        _run(ctx)

    assert state.writes == []
    assert state.values[("demo_oracle", "watermark")] == WATERMARK


def test_the_first_cursor_run_has_no_lower_bound_but_still_has_an_upper_one(oracle_config_root):
    """No stored watermark yet. The upper bound still applies, which is what makes the
    watermark this run writes trustworthy."""
    _cursor_source(oracle_config_root)
    state = RecordingState()
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), state=state)

    result = _run(ctx)

    extract = ctx.spark.options_for(1)["dbtable"]
    assert extract.count("LAST_UPDATE_DT") == 1
    assert "<=" in extract
    assert result.position_start is None
    assert state.watermark == HIGH_WATER


# --------------------------------------------------------------------------------------
# When the watermark must NOT move
# --------------------------------------------------------------------------------------


def test_a_replay_never_writes_state(oracle_config_root):
    """THE INVARIANT. A replay re-extracts an interval a human named while the scheduled
    run keeps its own position - so the two cannot interfere, and an incident cannot strand
    production state at a bound somebody typed once."""
    _cursor_source(oracle_config_root)
    state = RecordingState({("demo_oracle", "watermark"): WATERMARK})
    ctx = make_oracle_ctx(
        oracle_config_root,
        run_type="oracle_replay",
        spark=_spark(high_water=_NO_PROBE),
        state=state,
        rerun_id="INC-1042",
        replay_cursor_start="2026-07-01 00:00:00",
        replay_cursor_end="2026-07-02 00:00:00",
    )

    result = _run(ctx)

    assert state.writes == []
    assert state.values[("demo_oracle", "watermark")] == WATERMARK
    extract = ctx.spark.options_for(0)["dbtable"]
    assert "TO_TIMESTAMP('2026-07-01 00:00:00'" in extract
    assert "TO_TIMESTAMP('2026-07-02 00:00:00'" in extract
    assert result.position_end == "2026-07-02 00:00:00"


def test_a_replay_reads_no_stored_watermark_at_all(oracle_config_root):
    """Not merely ignored - never read. A replay's bounds come from the operator."""
    _cursor_source(oracle_config_root)
    state = RecordingState({("demo_oracle", "watermark"): WATERMARK})
    ctx = make_oracle_ctx(
        oracle_config_root,
        run_type="oracle_replay",
        spark=_spark(high_water=_NO_PROBE),
        state=state,
        rerun_id="INC-1042",
        replay_cursor_start="2026-07-01 00:00:00",
        replay_cursor_end="2026-07-02 00:00:00",
    )
    _run(ctx)
    assert state.reads == []


def test_an_unbounded_replay_still_gets_a_real_upper_bound(oracle_config_root):
    """'From there to now' is a legitimate replay, and it is still a CLOSED interval."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(
        oracle_config_root,
        run_type="oracle_replay",
        spark=_spark(),
        rerun_id="INC-1042",
        replay_cursor_start="2026-07-01 00:00:00",
    )
    result = _run(ctx)
    assert result.position_end == HIGH_WATER
    assert "LAST_UPDATE_DT <= TO_TIMESTAMP" in ctx.spark.options_for(1)["dbtable"]


def test_a_full_run_does_not_touch_the_watermark(oracle_config_root):
    """docs/build_log/DECISIONS.md D-09: support can switch a delta source to a full load
    with no deploy. The full run has no interval, so it has nothing trustworthy to advance
    to - and CLEARING the watermark would make the switch back re-read the whole table."""
    _cursor_source(oracle_config_root)
    state = RecordingState({("demo_oracle", "watermark"): WATERMARK})
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(high_water=_NO_PROBE), state=state, incremental_mode="full")

    result = _run(ctx)

    assert state.writes == []
    assert state.values[("demo_oracle", "watermark")] == WATERMARK
    assert "WHERE" not in ctx.spark.options_for(0)["dbtable"]
    assert result.position_end is None
    # And it SAYS so, naming the mode: a watermark that silently did not move looks
    # identical to one that did until the next run reads the same interval again.
    assert ctx.log.fields("oracle_watermark_not_advanced") == {"reason": "not a cursor extract", "mode": "full"}


def test_an_extract_with_no_cursor_values_reads_nothing_and_advances_nothing(oracle_config_root):
    """An empty table, or a filter matching nothing. There is no interval, so there is
    nothing to read - and nothing to advance to."""
    _cursor_source(oracle_config_root)
    state = RecordingState()
    spark = FakeJdbcSpark(frames=[LoadedFrame([{"high_water": None}])])
    ctx = make_oracle_ctx(oracle_config_root, spark=spark, state=state)

    result = _run(ctx)

    assert len(spark.reads) == 1  # the probe only
    assert state.writes == []
    assert ctx.writers.appends == [] and ctx.writers.merges == []
    assert (result.rows_read, result.rows_written) == (0, {})


# --------------------------------------------------------------------------------------
# The write
# --------------------------------------------------------------------------------------


def test_a_source_with_merge_keys_merges_on_the_key_and_the_cursor(oracle_config_root):
    """The cursor is IN the merge key, so `(CLAIM_ID, LAST_UPDATE_DT)` identifies a
    VERSION of a claim rather than the claim. That is what keeps landing a retained mirror
    instead of a current-state table - and what makes a full/delta switch idempotent."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), existing_tables=("cat_dev.oracle_claims.claim_header",))

    _run(ctx)

    merge = ctx.writers.merge_into("cat_dev.oracle_claims.claim_header")
    assert merge["keys"] == ("CLAIM_ID", "LAST_UPDATE_DT")
    assert merge["update_matched"] is False


def test_the_merge_key_does_not_change_when_the_mode_does(oracle_config_root):
    """THE REASON THE CURSOR IS IN THE KEY. A full run keyed on the business key alone
    would match every historical version of a claim with one source row and overwrite all
    of them. Keying on the cursor as well means each version matches itself."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(
        oracle_config_root,
        spark=_spark(high_water=_NO_PROBE),
        existing_tables=("cat_dev.oracle_claims.claim_header",),
        incremental_mode="full",
    )

    _run(ctx)

    assert ctx.writers.merge_into("cat_dev.oracle_claims.claim_header")["keys"] == ("CLAIM_ID", "LAST_UPDATE_DT")


def test_a_source_with_no_cursor_updates_matched_rows(oracle_config_root):
    """With no cursor the key identifies the ROW, so a match means the source row CHANGED -
    and the mirror goes stale unless it is rewritten."""
    write_oracle_source(oracle_config_root, merge_keys=["CLAIM_ID"])
    ctx = make_oracle_ctx(
        oracle_config_root,
        spark=_spark(high_water=_NO_PROBE),
        existing_tables=("cat_dev.oracle_claims.claim_header",),
    )

    _run(ctx)

    merge = ctx.writers.merge_into("cat_dev.oracle_claims.claim_header")
    assert merge["keys"] == ("CLAIM_ID",)
    assert merge["update_matched"] is True


def test_the_landing_merge_declares_that_no_partition_bound_is_derivable(oracle_config_root):
    """`true` is framework/writers.merge()'s declared escape hatch, and this is the case it
    exists for: landing is partitioned by the date a row was WRITTEN, so a re-extracted row
    carries today's while its target twin carries the day it first arrived. Any bound from
    this frame would match NOTHING and insert duplicates - worse than the full scan."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), existing_tables=("cat_dev.oracle_claims.claim_header",))
    _run(ctx)
    assert ctx.writers.merge_into("cat_dev.oracle_claims.claim_header")["partition_predicate"] == "true"


def test_a_source_that_waived_merge_keys_appends_with_idempotency_markers(oracle_config_root):
    """Delta's markers make a RETRY of this exact run a no-op. They do not make a
    re-extracted interval one - nothing can, without a key."""
    _cursor_source(oracle_config_root, merge_keys=[])
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(), run_sequence=7)

    _run(ctx)

    append = ctx.writers.append_into("cat_dev.oracle_claims.claim_header")
    assert append["txn_app_id"] == "ingest::oracle::demo_oracle"
    assert append["txn_version"] == 7
    assert append["partition_by"] == ["ingest_date"]
    assert ctx.writers.merges == []


def test_the_waived_boundary_risk_is_warned_about_on_every_run(oracle_config_root):
    """It is legal, deliberate and lossy in a way no row count shows - which is the only
    kind of thing worth a WARN on every single run."""
    _cursor_source(oracle_config_root, merge_keys=[])
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())
    _run(ctx)
    assert "oracle_boundary_rows_can_be_lost" in ctx.log.events("WARNING")


def test_a_serial_read_is_warned_about(oracle_config_root):
    """num_partitions=1 means one executor whatever the cluster size, and it fails as
    slowness rather than as an error."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())
    _run(ctx)
    assert "oracle_serial_read" in ctx.log.events("WARNING")


def test_the_first_run_appends_because_there_is_nothing_to_merge_against(oracle_config_root):
    """The table was created moments earlier and is empty, so a merge and an append produce
    the same rows - and the append carries the idempotency markers a merge cannot."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())
    _run(ctx)
    assert ctx.writers.merges == []
    assert ctx.writers.append_into("cat_dev.oracle_claims.claim_header")["txn_version"] == 1


def test_the_extract_is_evaluated_once_and_released_afterwards(oracle_config_root):
    """Without the cache, `count()` and the write are two actions over a JDBC source - two
    full reads of the source table, and two chances to disagree about what Oracle held."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())
    _run(ctx)
    frame = ctx.spark.reads[1].load_result
    assert (frame.persisted, frame.unpersisted) == (1, 1)


def test_the_cache_is_released_on_the_failure_path_too(oracle_config_root):
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())

    def explode(*args, **kwargs):
        raise RuntimeError("write failed")

    ctx.writers.append = explode
    with pytest.raises(RuntimeError):
        _run(ctx)
    assert ctx.spark.reads[1].load_result.unpersisted == 1


# --------------------------------------------------------------------------------------
# The table, the schema, and what stops a run before it writes
# --------------------------------------------------------------------------------------


def test_the_landing_table_is_created_from_the_resolved_schema_before_the_write(oracle_config_root):
    """Explicitly, so it gets the platform's TBLPROPERTIES and its partitioning. An
    implicitly created table would be the only one people query without auto-compaction."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())

    _run(ctx)

    created = ctx.tables.created[0]
    assert created["name"] == "cat_dev.oracle_claims.claim_header"
    assert created["partition_by"] == ["ingest_date"]
    assert "CLAIM_ID decimal(38,0)" in created["columns"]
    assert "run_id" in created["columns"]


def test_a_column_the_driver_could_not_map_stops_the_run_before_any_write(oracle_config_root):
    _cursor_source(oracle_config_root)
    schema = FakeSchema({"CLAIM_ID": "decimal(38,0)", "NOTES": "void"})
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(schema=schema))

    with pytest.raises(ConfigError, match="NOTES"):
        _run(ctx)
    assert ctx.writers.appends == [] and ctx.tables.created == []


def test_a_type_change_against_the_existing_table_stops_the_run(oracle_config_root):
    """Invisible to a row count, and it breaks the consumer rather than the pipeline."""
    _cursor_source(oracle_config_root)
    table = "cat_dev.oracle_claims.claim_header"
    spark = _spark()
    spark._frames_by_table = {table: LoadedFrame(schema=FakeSchema({"CLAIM_ID": "string"}))}
    ctx = make_oracle_ctx(oracle_config_root, spark=spark, existing_tables=(table,))

    with pytest.raises(ConfigError, match="changed type"):
        _run(ctx)
    assert ctx.writers.merges == [] and ctx.writers.appends == []


def test_the_frameworks_own_columns_are_not_mistaken_for_drift(oracle_config_root):
    """The landing table carries seven provenance columns the extract never returns.
    Comparing them would make every run after the first one fail."""
    _cursor_source(oracle_config_root)
    table = "cat_dev.oracle_claims.claim_header"
    spark = _spark()
    spark._frames_by_table = {
        table: LoadedFrame(
            schema=FakeSchema(
                {
                    "CLAIM_ID": "decimal(38,0)",
                    "LAST_UPDATE_DT": "timestamp",
                    "STATUS": "string",
                    "source_key": "string",
                    "ingest_ts": "timestamp",
                    "ingest_date": "date",
                    "ingested_via": "string",
                    "replay_run_id": "string",
                    "txn_version": "bigint",
                    "run_id": "string",
                }
            )
        )
    }
    ctx = make_oracle_ctx(oracle_config_root, spark=spark, existing_tables=(table,))

    _run(ctx)

    assert ctx.writers.merge_into(table)["keys"] == ("CLAIM_ID", "LAST_UPDATE_DT")


def test_a_source_column_that_would_shadow_a_provenance_column_is_refused(oracle_config_root):
    """An Oracle column called `run_id` would collide with this run's own provenance, and
    the error Delta gives for that names neither the source table nor the projection."""
    _cursor_source(oracle_config_root)
    schema = FakeSchema({"CLAIM_ID": "decimal(38,0)", "LAST_UPDATE_DT": "timestamp", "run_id": "string"})
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark(schema=schema))

    with pytest.raises(ConfigError, match="run_id"):
        _run(ctx)


# --------------------------------------------------------------------------------------
# What the run reports
# --------------------------------------------------------------------------------------


def test_the_audit_row_carries_the_query_this_run_actually_ran(oracle_config_root):
    """The first question of every Oracle incident, and not reconstructable from the
    configuration once a dynamic window and a watermark are involved."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())

    detail = json.loads(_run(ctx).source_detail)

    assert "LAST_UPDATE_DT <= TO_TIMESTAMP('2026-08-24 06:30:00'" in detail["query"]
    assert detail["source_ref"] == "CLAIMS.CLAIM_HEADER"
    assert detail["merge_on"] == ["CLAIM_ID", "LAST_UPDATE_DT"]


def test_the_audit_row_names_the_oracle_object_and_the_replay(oracle_config_root):
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(
        oracle_config_root,
        run_type="oracle_replay",
        spark=_spark(high_water=_NO_PROBE),
        rerun_id="INC-1042",
        replay_cursor_start="2026-07-01 00:00:00",
        replay_cursor_end="2026-07-02 00:00:00",
    )
    _run(ctx)
    assert ctx.audit.source_ref == "CLAIMS.CLAIM_HEADER"
    assert ctx.audit.rerun_id == "INC-1042"


def test_pending_work_is_null_because_this_source_cannot_know_it(oracle_config_root):
    """A zero here reads as 'fully caught up', which is exactly the claim a lagging feed
    makes falsely. Learning the truth would take another round trip to Oracle."""
    _cursor_source(oracle_config_root)
    ctx = make_oracle_ctx(oracle_config_root, spark=_spark())
    assert _run(ctx).pending_work is None
