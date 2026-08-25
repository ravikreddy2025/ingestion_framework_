"""framework/checkpoint.py - the checkpoint-reset guard shared by every checkpoint-based
source.

Nothing here needs Spark or a real source config: the guard's whole contract is what it
does with a RunContext of stand-ins plus a handful of primitive values (checkpoint path,
landing table, reset id, control column) - which is also exactly the part that has to be
right. This used to be two near-identical copies, one under sources/kafka/run.py and one
under sources/file/run.py; this file is the single behaviour matrix that replaces both.
tests/test_kafka_run.py and tests/test_file_run.py each keep a couple of their own tests
proving they WIRE their own config into this correctly - which control column, which
landing table, which checkpoint path - not re-proving the guard's own logic.
"""

from __future__ import annotations

import importlib

import pytest

from conftest import FakeSpark, RecordingAudit, RecordingLog, RecordingTables
from kafka_ingest.framework.contracts import RunContext
from kafka_ingest.sources import file as file_source
from kafka_ingest.sources import kafka

checkpoint = importlib.import_module("kafka_ingest.framework.checkpoint")

AUDIT_TABLE = "ops_dev.audit.ingest_audit"
LANDING_TABLE = "cat_dev.landing.demo_events"
CHECKPOINT_PATH = "/Volumes/demo/checkpoints/demo_source/primary"
CONTROL_COLUMN = "kafka_checkpoint_reset_id"


class _FakeResolvedConfig:
    """Stands in for framework/config.py's ResolvedConfig - the guard only ever reads
    `.source_key` and `.get("audit_table")` off it."""

    def __init__(self, source_key="demo_source", audit_table=AUDIT_TABLE):
        self.source_key = source_key
        self._audit_table = audit_table

    def get(self, key, default=None):
        return self._audit_table if key == "audit_table" else default


@pytest.fixture
def checkpoint_probe(monkeypatch):
    """Control whether the guard believes the checkpoint exists.

    Patches the probe rather than creating directories: the real one is an os.stat() on a
    Volume path, and what matters is which of its three answers the guard acts on.
    """

    def _set(exists=True, error=None):
        def probe(_path):
            if error is not None:
                raise error
            return exists

        monkeypatch.setattr(checkpoint, "_checkpoint_offsets_exist", probe)

    return _set


def _ctx(
    existing_tables=(),
    landing_rows=(),
    audit_rows=(),
    run_id="demo_source-primary-test",
    source_key="demo_source",
):
    spark = FakeSpark(
        existing_tables=existing_tables,
        rows_by_table={LANDING_TABLE: list(landing_rows), AUDIT_TABLE: list(audit_rows)},
    )
    return RunContext(
        cfg=_FakeResolvedConfig(source_key=source_key),
        spark=spark,
        audit=RecordingAudit(),
        state=None,
        writers=None,
        tables=RecordingTables(existing_tables),
        log=RecordingLog(),
        run_id=run_id,
        run_type="primary",
        run_sequence=1,
    )


def _guard(ctx, **overrides):
    kwargs = dict(
        checkpoint_path=CHECKPOINT_PATH,
        landing_table=LANDING_TABLE,
        checkpoint_reset_id=None,
        is_replay=False,
        control_column=CONTROL_COLUMN,
    )
    kwargs.update(overrides)
    checkpoint.guard_against_checkpoint_reset(ctx, **kwargs)


# --------------------------------------------------------------------------------------
# control_column_for - the reverse lookup that keeps this module from naming a source type
# --------------------------------------------------------------------------------------


def test_control_column_for_reverse_looks_up_kafkas_reset_column():
    assert checkpoint.control_column_for(kafka.SOURCE_SPEC, "checkpoint_reset_id") == "kafka_checkpoint_reset_id"


def test_control_column_for_reverse_looks_up_files_reset_column():
    assert checkpoint.control_column_for(file_source.SOURCE_SPEC, "checkpoint_reset_id") == "file_checkpoint_reset_id"


# --------------------------------------------------------------------------------------
# The startup guard
# --------------------------------------------------------------------------------------


def test_the_guard_allows_a_run_whose_checkpoint_is_intact(checkpoint_probe):
    checkpoint_probe(exists=True)
    _guard(_ctx(existing_tables=(LANDING_TABLE,), landing_rows=[{"topic": "x"}]))


def test_the_guard_allows_a_genuine_first_run(checkpoint_probe):
    """No checkpoint AND no landing rows yet is what a first run looks like."""
    checkpoint_probe(exists=False)
    _guard(_ctx(existing_tables=(LANDING_TABLE,), landing_rows=[]))


def test_the_guard_allows_a_run_when_the_landing_table_does_not_exist_yet(checkpoint_probe):
    checkpoint_probe(exists=False)
    _guard(_ctx())


def test_the_guard_refuses_when_the_checkpoint_vanished_but_data_exists(checkpoint_probe):
    """The state that would report success and ingest nothing: batch ids restart at 0 and
    Delta skips every write as a duplicate."""
    checkpoint_probe(exists=False)
    ctx = _ctx(existing_tables=(LANDING_TABLE,), landing_rows=[{"topic": "x"}])
    with pytest.raises(RuntimeError) as exc:
        _guard(ctx)
    message = str(exc.value)
    assert "REFUSING TO RUN" in message
    assert "SKIP every write" in message
    # It must name the alternative, or the next move is to delete landing rows to get past it.
    assert CONTROL_COLUMN in message


def test_the_guard_never_blocks_a_replay(checkpoint_probe):
    """A replay has its own checkpoint and its own app id by construction, so neither
    collision is possible - regardless of what the checkpoint probe or landing table say."""
    checkpoint_probe(exists=False)
    _guard(
        _ctx(existing_tables=(LANDING_TABLE,), landing_rows=[{"topic": "x"}]),
        is_replay=True,
    )


def test_an_unreadable_checkpoint_volume_is_not_treated_as_a_missing_checkpoint(monkeypatch, tmp_path):
    """os.path.exists() would swallow this and return False, making an unreachable Volume
    indistinguishable from a deleted checkpoint - and the guard turns "absent" into a hard
    refusal, so a false absent blocks a healthy stream."""
    import os

    def deny(_path):
        raise PermissionError("no access to the Volume from this compute profile")

    monkeypatch.setattr(os, "stat", deny)
    with pytest.raises(RuntimeError, match="NOT the same as the checkpoint being missing"):
        checkpoint._checkpoint_offsets_exist(str(tmp_path))


# --------------------------------------------------------------------------------------
# checkpoint_reset_id: single-use
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("control_column", ["kafka_checkpoint_reset_id", "file_checkpoint_reset_id"])
def test_a_fresh_reset_id_lets_the_guard_through_and_says_so_loudly(checkpoint_probe, control_column):
    checkpoint_probe(exists=False)
    ctx = _ctx(existing_tables=(LANDING_TABLE, AUDIT_TABLE), landing_rows=[{"topic": "x"}], audit_rows=[])
    _guard(ctx, checkpoint_reset_id="INC-1042", control_column=control_column)
    assert "checkpoint_reset_engaged" in ctx.log.events("WARNING")
    fields = ctx.log.fields("checkpoint_reset_engaged")
    assert fields["checkpoint_reset_id"] == "INC-1042"
    # The warning has to say NOT to clear it: reverting would restore the old identity.
    assert "clear" in fields["note"]


@pytest.mark.parametrize("control_column", ["kafka_checkpoint_reset_id", "file_checkpoint_reset_id"])
def test_a_reused_reset_id_is_refused(checkpoint_probe, control_column):
    """THE THIRD SILENT-DATA-LOSS STATE, and the nastiest of the three.

    The guard is bypassed, batch ids restart at 0, and the app id is UNCHANGED because it
    is derived from the same reset id - so Delta still holds high versions against it and
    skips every write. The run reports success and ingests nothing.
    """
    checkpoint_probe(exists=False)
    ctx = _ctx(
        existing_tables=(LANDING_TABLE, AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
    )
    with pytest.raises(RuntimeError) as exc:
        _guard(ctx, checkpoint_reset_id="INC-1042", control_column=control_column)
    message = str(exc.value)
    assert "INC-1042" in message, "the refusal must name the spent id"
    assert "single-use" in message
    assert "SKIPPED AS A DUPLICATE" in message
    assert "UNUSED" in message, "the refusal must say what to do instead"
    assert control_column in message


def test_a_stale_reset_id_is_inert_once_the_checkpoint_exists_again(checkpoint_probe):
    """Case 2 of the three. The field stays set forever - clearing it would revert the app
    id to the identity the reset forked away from - so the guard must not trip on it, and
    must not warn about it either once there is nothing to bypass."""
    checkpoint_probe(exists=True)
    ctx = _ctx(
        existing_tables=(LANDING_TABLE, AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
    )
    _guard(ctx, checkpoint_reset_id="INC-1042")
    assert ctx.log.events("WARNING") == []


def test_the_reuse_check_excludes_this_run_s_own_audit_rows(checkpoint_probe):
    """Otherwise the check would depend on being called before the reset id reaches the
    audit writer - true today, and exactly the kind of ordering that breaks silently.

    Asserted on the predicate rather than on rows, because the exclusion IS the predicate:
    a stand-in that filtered would only prove the stand-in filters.
    """
    checkpoint_probe(exists=False)
    ctx = _ctx(
        existing_tables=(LANDING_TABLE, AUDIT_TABLE),
        landing_rows=[{"topic": "x"}],
        audit_rows=[{"rerun_id": "INC-1042", "run_type": "primary", "run_id": "an-older-run"}],
        run_id="demo_source-primary-test",
    )
    with pytest.raises(RuntimeError):
        _guard(ctx, checkpoint_reset_id="INC-1042")
    predicate = ctx.spark.table(AUDIT_TABLE).conditions[0]
    assert "run_id <> 'demo_source-primary-test'" in predicate
    assert "run_type = 'primary'" in predicate
    assert "rerun_id = 'INC-1042'" in predicate


def test_the_reuse_check_treats_a_missing_audit_table_as_never_used(checkpoint_probe):
    """The audit table is best-effort evidence, which is normally a reason not to depend on
    it. The failure direction here is the safe one: a missing row reads as "unused", so a
    genuine first use proceeds under a forked identity. Refusing a legitimate reset would
    be the more expensive mistake, and this cannot make it."""
    checkpoint_probe(exists=False)
    ctx = _ctx(existing_tables=(LANDING_TABLE,), landing_rows=[{"topic": "x"}])
    _guard(ctx, checkpoint_reset_id="INC-1042")
