"""sources/kafka/run.py - the guard, the run shapes, and the writes.

Nothing here needs Spark. A source is handed a RunContext and nothing else, so the way to
test one without a cluster is to hand it a context of stand-ins and assert on what it did
with them - which is also exactly the part that has to be right. Whether Delta honours a
txnAppId is Delta's problem; whether this code SUPPLIES one is ours.

THE THREE THINGS THIS FILE EXISTS FOR
  * the checkpoint-reset guard, including the reuse refusal - the state that would report
    success and ingest nothing
  * append vs MERGE per layer, and the partition predicate each MERGE carries
  * that a failure is audited against the layer it happened in, and the cache is released
"""

from __future__ import annotations

import importlib

import pytest

from conftest import FakeSpark, RecordingDataFrame, make_kafka_ctx
from kafka_ingest.sources.kafka.config import RUN_TYPE_CURATED_REPLAY, RUN_TYPE_KAFKA_REPLAY
from kafka_ingest.sources.kafka.landing import RECORD_KEYS

# importlib, not `from ... import run`: the package's __init__ re-exports the run FUNCTION
# under that name (it is the source's public surface), so a plain import would bind the
# function, and monkeypatching module-level names on a function does not work.
kafka_run = importlib.import_module("kafka_ingest.sources.kafka.run")

OFFSETS = '{"demo.events.v1": {"0": 100, "1": 250}}'
AUDIT_TABLE = "cat_dev.audit.ingest_audit"


# --------------------------------------------------------------------------------------
# The startup guard
# --------------------------------------------------------------------------------------


@pytest.fixture
def checkpoint(tmp_path, monkeypatch):
    """Control whether the guard believes the checkpoint exists.

    Patches the probe rather than creating directories: the real one is an os.stat() on a
    Volume path, and what matters is which of its three answers the guard acts on.
    """

    def _set(exists=True, error=None):
        def probe(_path):
            if error is not None:
                raise error
            return exists

        monkeypatch.setattr(kafka_run, "_checkpoint_offsets_exist", probe)

    return _set


def _ctx(config_root, existing_tables=(), landing_rows=(), audit_rows=(), **kwargs):
    spark = FakeSpark(
        existing_tables=existing_tables,
        rows_by_table={
            "cat_dev.landing.demo_events_v1": list(landing_rows),
            AUDIT_TABLE: list(audit_rows),
        },
    )
    return make_kafka_ctx(config_root, spark=spark, existing_tables=existing_tables, **kwargs)


def _guard(ctx):
    from kafka_ingest.sources.kafka import config as kafka_config

    cfg = kafka_config.build(ctx.cfg, ctx.run_type, ctx.tables)
    kafka_run._guard_against_checkpoint_reset(ctx, cfg)


def test_the_guard_allows_a_run_whose_checkpoint_is_intact(config_root, checkpoint):
    checkpoint(exists=True)
    _guard(_ctx(config_root, existing_tables=("cat_dev.landing.demo_events_v1",), landing_rows=[{"topic": "x"}]))


def test_the_guard_allows_a_genuine_first_run(config_root, checkpoint):
    """No checkpoint AND no landing rows for this topic is what a first run looks like."""
    checkpoint(exists=False)
    _guard(_ctx(config_root, existing_tables=("cat_dev.landing.demo_events_v1",), landing_rows=[]))


def test_the_guard_allows_a_run_when_the_landing_table_does_not_exist_yet(config_root, checkpoint):
    checkpoint(exists=False)
    _guard(_ctx(config_root))


def test_the_guard_refuses_when_the_checkpoint_vanished_but_data_exists(config_root, checkpoint):
    """The state that would report success and ingest nothing: batch ids restart at 0 and
    Delta skips every write as a duplicate."""
    checkpoint(exists=False)
    ctx = _ctx(config_root, existing_tables=("cat_dev.landing.demo_events_v1",), landing_rows=[{"topic": "x"}])
    with pytest.raises(RuntimeError) as exc:
        _guard(ctx)
    message = str(exc.value)
    assert "REFUSING TO RUN" in message
    assert "skip every write" in message.lower() or "SKIP every write" in message
    # It must name the alternative, or the next move is to delete landing rows to get past it.
    assert "replay" in message and "kafka_checkpoint_reset_id" in message


def test_the_guard_never_blocks_a_replay(config_root, checkpoint):
    """A replay has its own checkpoint and its own app id by construction, so neither
    collision is possible."""
    checkpoint(exists=False)
    _guard(
        _ctx(
            config_root,
            existing_tables=("cat_dev.landing.demo_events_v1",),
            landing_rows=[{"topic": "x"}],
            run_type=RUN_TYPE_KAFKA_REPLAY,
            rerun_id="INC1",
            replay_starting_offsets=OFFSETS,
        )
    )


def test_an_unreadable_checkpoint_volume_is_not_treated_as_a_missing_checkpoint(config_root, monkeypatch, tmp_path):
    """os.path.exists() would swallow this and return False, making an unreachable Volume
    indistinguishable from a deleted checkpoint - and the guard turns "absent" into a hard
    refusal, so a false absent blocks a healthy stream."""
    import os

    def deny(_path):
        raise PermissionError("no access to the Volume from this compute profile")

    monkeypatch.setattr(os, "stat", deny)
    with pytest.raises(RuntimeError, match="NOT the same as the checkpoint being missing"):
        kafka_run._checkpoint_offsets_exist(str(tmp_path))


# --------------------------------------------------------------------------------------
# checkpoint_reset_id: single-use
# --------------------------------------------------------------------------------------


def test_a_fresh_reset_id_lets_the_guard_through_and_says_so_loudly(config_root, checkpoint):
    checkpoint(exists=False)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1", AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[],
        checkpoint_reset_id="INC-1042",
    )
    _guard(ctx)
    assert "kafka_checkpoint_reset_engaged" in ctx.log.events("WARNING")
    fields = ctx.log.fields("kafka_checkpoint_reset_engaged")
    assert fields["checkpoint_reset_id"] == "INC-1042"
    # The warning has to say NOT to clear it: reverting would restore the old identity.
    assert "clear" in fields["note"]


def test_a_reused_reset_id_is_refused(config_root, checkpoint):
    """THE THIRD SILENT-DATA-LOSS STATE, and the nastiest of the three.

    The guard is bypassed, batch ids restart at 0, and the app id is UNCHANGED because it
    is derived from the same reset id - so Delta still holds high versions against it and
    skips every write. The run reports success and ingests nothing.
    """
    checkpoint(exists=False)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1", AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
        checkpoint_reset_id="INC-1042",
    )
    with pytest.raises(RuntimeError) as exc:
        _guard(ctx)
    message = str(exc.value)
    assert "INC-1042" in message, "the refusal must name the spent id"
    assert "single-use" in message
    assert "SKIPPED AS A DUPLICATE" in message
    assert "UNUSED" in message, "the refusal must say what to do instead"


def test_a_stale_reset_id_is_inert_once_the_checkpoint_exists_again(config_root, checkpoint):
    """Case 2 of the three. The field stays set forever - clearing it would revert the app
    id to the identity the reset forked away from - so the guard must not trip on it, and
    must not warn about it either once there is nothing to bypass."""
    checkpoint(exists=True)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1", AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
        checkpoint_reset_id="INC-1042",
    )
    _guard(ctx)
    assert ctx.log.events("WARNING") == []


def test_the_reuse_check_excludes_this_run_s_own_audit_rows(config_root, checkpoint):
    """Otherwise the check would depend on being called before the reset id reaches the
    audit writer - true today, and exactly the kind of ordering that breaks silently.

    Asserted on the predicate rather than on rows, because the exclusion IS the predicate:
    a stand-in that filtered would only prove the stand-in filters.
    """
    checkpoint(exists=False)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1", AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
        checkpoint_reset_id="INC-1042",
    )
    with pytest.raises(RuntimeError):
        _guard(ctx)
    predicate = ctx.spark.table(AUDIT_TABLE).conditions[0]
    assert "run_id <> 'demo_topic-primary-test'" in predicate
    assert "run_type = 'primary'" in predicate
    assert "rerun_id = 'INC-1042'" in predicate


def test_the_reuse_check_treats_a_missing_audit_table_as_never_used(config_root, checkpoint):
    """The audit table is best-effort evidence, which is normally a reason not to depend on
    it. The failure direction here is the safe one: a missing row reads as "unused", so a
    genuine first use proceeds under a forked identity. Refusing a legitimate reset would
    be the more expensive mistake, and this cannot make it."""
    checkpoint(exists=False)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1",),
        landing_rows=[{"topic": "x"}],
        checkpoint_reset_id="INC-1042",
    )
    _guard(ctx)


def test_the_reset_id_is_recorded_on_the_audit_row_so_the_reuse_check_can_see_it(config_root, checkpoint, monkeypatch):
    """rerun_id, not a new column: run_type disambiguates. A primary run with a non-NULL
    rerun_id is a reset, by construction - which is what Q6d in sql/03 queries."""
    checkpoint(exists=False)
    ctx = _ctx(
        config_root,
        existing_tables=("cat_dev.landing.demo_events_v1", AUDIT_TABLE),
        landing_rows=[],
        checkpoint_reset_id="INC-1042",
    )
    monkeypatch.setattr(kafka_run._Session, "prepare", lambda self: None)
    monkeypatch.setattr(kafka_run._Session, "run_streaming", lambda self: None)
    kafka_run.run(ctx, secrets=object())
    assert ctx.audit.rerun_id == "INC-1042"


# --------------------------------------------------------------------------------------
# The writes: append vs MERGE, and what each MERGE is bounded by
# --------------------------------------------------------------------------------------


class _BoundedFrame(RecordingDataFrame):
    """A frame whose event_date min/max are known, so the merge predicate is assertable."""

    def __init__(self, low="2026-08-01", high="2026-08-03", rows=(1,)):
        super().__init__(rows)
        self._bounds = {"lo": low, "hi": high}

    def selectExpr(self, *_expressions):  # noqa: N802 - mirrors the Spark API
        return self

    def collect(self):
        return [self._bounds]


def _session(config_root, run_type="primary", existing_tables=(), **kwargs):
    ctx = _ctx(config_root, existing_tables=existing_tables, run_type=run_type, **kwargs)
    from kafka_ingest.sources.kafka import config as kafka_config

    cfg = kafka_config.build(ctx.cfg, run_type, ctx.tables)
    return kafka_run._Session(ctx=ctx, cfg=cfg, state=kafka_run._RunState(cfg=cfg), secrets=None)


def test_a_primary_landing_write_appends_with_the_idempotency_markers(config_root):
    """This is what makes a retried microbatch a no-op instead of a duplicate."""
    session = _session(config_root)
    session._write_landing(RecordingDataFrame([1]), 7)
    append = session.ctx.writers.append_into("cat_dev.landing.demo_events_v1")
    assert append["txn_version"] == 7
    assert append["txn_app_id"] == "kafka_ingest::demo_topic::primary::primary"
    assert session.ctx.writers.merges == []


def test_a_landing_replay_merges_insert_if_absent_and_never_rewrites_the_arrival_record(config_root):
    """The original landing row records what arrived on the primary stream. Rewriting its
    provenance columns with replay metadata would destroy that record."""
    session = _session(
        config_root,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        existing_tables=("cat_dev.landing.demo_events_v1",),
        rerun_id="INC1",
        replay_starting_offsets=OFFSETS,
    )
    session._write_landing(RecordingDataFrame([1]), -1)
    merge = session.ctx.writers.merge_into("cat_dev.landing.demo_events_v1")
    assert merge["keys"] == RECORD_KEYS
    assert merge.get("update_matched", False) is False
    assert session.ctx.writers.appends == []


def test_the_landing_merge_declares_that_no_partition_bound_is_derivable(config_root):
    """Landing is partitioned by ingest_date - the date a row was WRITTEN - so a replayed
    record carries today's while its target twin carries the day it originally arrived.
    Bounding on the source frame would match nothing and INSERT DUPLICATES, which is worse
    than the full scan it saves. `true` is the framework's declared escape hatch for
    exactly this, and it is used here rather than a wrong bound."""
    session = _session(
        config_root,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        existing_tables=("cat_dev.landing.demo_events_v1",),
        rerun_id="INC1",
        replay_starting_offsets=OFFSETS,
    )
    session._write_landing(RecordingDataFrame([1]), -1)
    assert session.ctx.writers.merge_into("cat_dev.landing.demo_events_v1")["partition_predicate"] == "true"


def test_a_landing_replay_into_a_missing_table_falls_back_to_append(config_root):
    """Unusual but legal - rebuilding a dropped table from the broker. Nothing to merge."""
    session = _session(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", replay_starting_offsets=OFFSETS)
    session._write_landing(RecordingDataFrame([1]), -1)
    assert session.ctx.writers.merges == []
    assert session.ctx.writers.append_into("cat_dev.landing.demo_events_v1")["table"]


def test_a_primary_curated_write_appends_partitioned_with_markers_and_no_event_date_bound(config_root):
    """The append path has no MERGE and therefore no predicate at all - asserted here so
    the bound below cannot be mistaken for something that applies everywhere."""
    session = _session(config_root)
    session._write_curated(RecordingDataFrame([1]), 7)
    append = session.ctx.writers.append_into("cat_dev.curated.demo_events_v1")
    assert append["partition_by"] == ["event_date"]
    assert append["merge_schema"] is True
    assert append["txn_version"] == 7
    assert session.ctx.writers.merges == []


def test_a_curated_replay_merge_is_bounded_to_the_days_it_is_replaying(config_root):
    """WITHOUT THIS, ONE REPLAYED DAY REWRITES EVERY PARTITION IT MIGHT MATCH. The merge
    key says nothing about event_date, so Delta has no way to prune - and on a table with
    three years of daily partitions a two-hour replay rewrites three years."""
    session = _session(
        config_root,
        run_type=RUN_TYPE_CURATED_REPLAY,
        existing_tables=("cat_dev.curated.demo_events_v1",),
        rerun_id="FIX1",
        replay_landing_filter="ingest_date = '2026-08-01'",
    )
    session._write_curated(_BoundedFrame("2026-08-01", "2026-08-03"), -1)
    merge = session.ctx.writers.merge_into("cat_dev.curated.demo_events_v1")
    assert merge["partition_predicate"] == "t.event_date BETWEEN '2026-08-01' AND '2026-08-03'"
    assert merge["keys"] == RECORD_KEYS


def test_a_curated_replay_replaces_the_bad_parse_and_allows_the_schema_to_widen(config_root):
    """The opposite of landing, and for the opposite reason: the whole point of a curated
    replay is to REPLACE a bad parse - and it is the operation run right after a schema
    change, so without evolution it fails on the schema it was run to apply."""
    session = _session(
        config_root,
        run_type=RUN_TYPE_CURATED_REPLAY,
        existing_tables=("cat_dev.curated.demo_events_v1",),
        rerun_id="FIX1",
        replay_landing_filter="ingest_date = '2026-08-01'",
    )
    session._write_curated(_BoundedFrame(), -1)
    merge = session.ctx.writers.merge_into("cat_dev.curated.demo_events_v1")
    assert merge["update_matched"] is True
    assert merge["schema_evolution"] is True


def test_a_batch_with_no_usable_event_date_refuses_rather_than_formatting_none_into_sql(config_root):
    """A predicate reading `BETWEEN 'None' AND 'None'` is a MERGE that silently matches
    nothing - every row inserted, every existing row left as it was."""
    session = _session(
        config_root,
        run_type=RUN_TYPE_CURATED_REPLAY,
        existing_tables=("cat_dev.curated.demo_events_v1",),
        rerun_id="FIX1",
        replay_landing_filter="ingest_date = '2026-08-01'",
    )
    with pytest.raises(ValueError, match="no usable event_date"):
        session._write_curated(_BoundedFrame(None, None), -1)
    assert session.ctx.writers.merges == []


def test_quarantine_always_appends_under_its_own_app_id(config_root):
    """A record can legitimately be quarantined twice - once by the primary run, once by a
    replay that still lacked the schema - and both attempts are evidence."""
    session = _session(config_root)
    session._write_quarantine(RecordingDataFrame([1]), 7)
    append = session.ctx.writers.append_into("cat_dev.landing.demo_events_v1_quarantine")
    assert append["txn_app_id"].endswith("::quarantine")
    assert append["txn_app_id"] != session.cfg.txn_app_id


# --------------------------------------------------------------------------------------
# The microbatch body
# --------------------------------------------------------------------------------------


class _Batch(RecordingDataFrame):
    """A batch frame that records persist/unpersist, so the cache lifecycle is assertable."""

    def __init__(self, rows=(1,), fail_on_count=False):
        super().__init__(rows)
        self.persisted = 0
        self.unpersisted = 0
        self.fail_on_count = fail_on_count

    def persist(self, _level=None):
        self.persisted += 1
        return self

    def unpersist(self):
        self.unpersisted += 1
        return self

    def count(self):
        if self.fail_on_count:
            raise RuntimeError("the batch could not be counted")
        return len(self._rows)


@pytest.fixture
def batch_session(config_root, monkeypatch):
    """A session whose landing projection and curated layer are replaced by stand-ins.

    The point of these tests is the ORDER and the FAILURE HANDLING of the microbatch body,
    not what the projection produces - which is tests/test_kafka_landing.py's job and needs
    Spark.
    """
    session = _session(config_root)
    frames = {}

    def project(_raw, _cfg, _txn, _run_id):
        frames["landing"] = _Batch(rows=frames.get("rows", (1, 2, 3)))
        return frames["landing"]

    monkeypatch.setattr(kafka_run.landing, "project", project)
    return session, frames


def test_a_healthy_microbatch_audits_both_layers_in_order(batch_session, monkeypatch):
    session, _ = batch_session
    monkeypatch.setattr(kafka_run._Session, "_curated_layer", lambda self, df, txn: None)
    session.process_microbatch(RecordingDataFrame(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "COMPLETED"]
    assert session.state.rows_written["landing"] == 3
    assert session.state.rows_read == 3


def test_an_empty_batch_is_recorded_as_no_data_and_writes_nothing(batch_session, monkeypatch):
    """Trigger.AvailableNow emits a final empty batch on every run, so this is the normal
    tail of a healthy run and must not look like a failure."""
    session, frames = batch_session
    frames["rows"] = ()
    monkeypatch.setattr(kafka_run._Session, "_curated_layer", lambda self, df, txn: None)
    session.process_microbatch(RecordingDataFrame(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "NO_DATA"]
    assert session.ctx.writers.appends == []


def test_a_curated_failure_is_audited_against_curated_and_re_raised(batch_session, monkeypatch):
    """Re-raising is what stops Structured Streaming committing the batch. The FAILED row
    names the layer that was in progress, which is what makes "which layer was it on?" a
    lookup rather than a guess."""
    session, _ = batch_session

    def explode(self, df, txn):
        raise RuntimeError("the registry is unreachable")

    monkeypatch.setattr(kafka_run._Session, "_curated_layer", explode)
    with pytest.raises(RuntimeError, match="registry is unreachable"):
        session.process_microbatch(RecordingDataFrame(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "COMPLETED"]
    assert session.ctx.audit.statuses("curated") == ["FAILED"]
    failure = next(row for row in session.ctx.audit.rows if row["status"] == "FAILED")
    assert failure["error_class"] == "RuntimeError"


def test_the_cache_is_released_on_the_failure_path_too(batch_session, monkeypatch):
    """The poison-batch path is the one that runs over and over, and it is exactly where a
    leaked cache accumulates - so the unpersist is in a `finally`, not on the success path."""
    session, frames = batch_session

    def explode(self, df, txn):
        raise RuntimeError("boom")

    monkeypatch.setattr(kafka_run._Session, "_curated_layer", explode)
    with pytest.raises(RuntimeError):
        session.process_microbatch(RecordingDataFrame(), 4)
    assert frames["landing"].persisted == 1
    assert frames["landing"].unpersisted == 1


def test_a_landing_failure_is_audited_against_landing(batch_session, monkeypatch):
    session, frames = batch_session

    def project(_raw, _cfg, _txn, _run_id):
        frames["landing"] = _Batch(fail_on_count=True)
        return frames["landing"]

    monkeypatch.setattr(kafka_run.landing, "project", project)
    with pytest.raises(RuntimeError, match="could not be counted"):
        session.process_microbatch(RecordingDataFrame(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "FAILED"]
    assert session.ctx.audit.statuses("curated") == []
