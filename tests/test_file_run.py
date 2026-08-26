"""sources/file/run.py - the checkpoint-reset guard wiring, and the microbatch body.

The checkpoint-reset guard itself is a shared framework function now
(framework/checkpoint.py, tested in tests/test_framework_checkpoint.py); what is left here
is proof that this source wires its own fields into it correctly, mirroring
tests/test_kafka_run.py's wiring tests of the same shape.
"""

from __future__ import annotations

import importlib

import pytest

from conftest import FakeSchema, FakeSpark, RecordingDataFrame, make_file_ctx, write_file_source

file_run = importlib.import_module("kafka_ingest.sources.file.run")

AUDIT_TABLE = "ops_dev.audit.ingest_audit"
LANDING_TABLE = "cat_dev.files_claims.claims_inbound"


# --------------------------------------------------------------------------------------
# The startup guard - the full behaviour matrix lives in tests/test_framework_checkpoint.py
# now that the guard itself is a shared framework function. What is left here is WIRING:
# does this source's run() feed ITS OWN checkpoint path, landing table, reset id and
# control column into that shared guard correctly.
# --------------------------------------------------------------------------------------


@pytest.fixture
def checkpoint(monkeypatch):
    """Control whether the guard believes the checkpoint exists.

    Patches the shared probe in framework/checkpoint.py - same technique as
    tests/test_kafka_run.py's fixture of the same name.
    """
    from kafka_ingest.framework import checkpoint as checkpoint_guard

    def _set(exists=True, error=None):
        def probe(_path):
            if error is not None:
                raise error
            return exists

        monkeypatch.setattr(checkpoint_guard, "_checkpoint_offsets_exist", probe)

    return _set


def _ctx(config_root, existing_tables=(), landing_rows=(), audit_rows=(), **kwargs):
    spark = FakeSpark(
        existing_tables=existing_tables,
        rows_by_table={LANDING_TABLE: list(landing_rows), AUDIT_TABLE: list(audit_rows)},
    )
    return make_file_ctx(config_root, spark=spark, existing_tables=existing_tables, **kwargs)


def test_the_guard_refuses_when_the_checkpoint_vanished_but_data_exists(file_config_root, checkpoint):
    """The refusal must name THIS source's own control column, not some other source's -
    proof that this source's cfg.landing_table and control column reach the shared guard."""
    checkpoint(exists=False)
    ctx = _ctx(file_config_root, existing_tables=(LANDING_TABLE,), landing_rows=[{"claim_id": "1"}])
    with pytest.raises(RuntimeError) as exc:
        file_run.run(ctx, secrets=object())
    message = str(exc.value)
    assert "REFUSING TO RUN" in message
    assert "file_checkpoint_reset_id" in message


def test_the_reset_id_is_recorded_on_the_audit_row(file_config_root, checkpoint, monkeypatch):
    """rerun_id, not a new column: run_type disambiguates. A primary run with a non-NULL
    rerun_id is a reset, by construction - the same convention Kafka's uses."""
    checkpoint(exists=False)
    ctx = _ctx(
        file_config_root,
        existing_tables=(LANDING_TABLE, AUDIT_TABLE),
        landing_rows=[],
        checkpoint_reset_id="INC-1042",
    )
    monkeypatch.setattr(file_run._Session, "run_streaming", lambda self: None)
    file_run.run(ctx, secrets=object())
    assert ctx.audit.rerun_id == "INC-1042"


# --------------------------------------------------------------------------------------
# Unity Catalog Volume source paths apply no storage credentials (docs/build_log/
# DECISIONS.md D-13). `build_stream_reader` is stopped with a sentinel right after the
# storage-option branch, the same distance a real streaming query is out of reach of these
# stand-ins - see the module docstring for why `run_streaming` itself is monkeypatched away
# everywhere else in this file.
# --------------------------------------------------------------------------------------


class _StreamingStoppedError(Exception):
    """Raised by a stubbed `build_stream_reader` so a test can inspect what happened
    before it, without needing a real Structured Streaming query object."""


def test_a_volume_source_applies_no_storage_options(file_config_root, monkeypatch):
    write_file_source(file_config_root, storage_ref=None, source_path="/Volumes/cat_dev/files_claims/landing/")
    calls = []
    monkeypatch.setattr(file_run.security, "build_storage_options", lambda *a, **k: calls.append("storage_options"))
    monkeypatch.setattr(file_run, "apply_session_options", lambda *a, **k: calls.append("apply_session_options"))
    monkeypatch.setattr(file_run, "build_stream_reader", lambda *a, **k: (_ for _ in ()).throw(_StreamingStoppedError))

    ctx = _ctx(file_config_root)
    with pytest.raises(_StreamingStoppedError):
        file_run.run(ctx, secrets=object())
    assert calls == [], "a Volume-governed source must apply no storage credentials at all"


def test_a_storage_ref_source_still_applies_its_options(file_config_root, monkeypatch):
    """The other branch, proven alongside the Volume one so a future edit cannot make both
    paths silently skip session options - see conftest's default file source, which uses
    storage_ref."""
    calls = []

    def _fake_apply_session_options(*_a, **_k):
        calls.append("apply_session_options")
        return lambda: None

    monkeypatch.setattr(file_run.security, "build_storage_options", lambda *a, **k: calls.append("storage_options"))
    monkeypatch.setattr(file_run, "apply_session_options", _fake_apply_session_options)
    monkeypatch.setattr(file_run, "build_stream_reader", lambda *a, **k: (_ for _ in ()).throw(_StreamingStoppedError))

    ctx = _ctx(file_config_root)
    with pytest.raises(_StreamingStoppedError):
        file_run.run(ctx, secrets=object())
    assert calls == ["storage_options", "apply_session_options"]


# --------------------------------------------------------------------------------------
# The microbatch body
# --------------------------------------------------------------------------------------


class _Batch(RecordingDataFrame):
    """A batch frame that records persist/unpersist, so the cache lifecycle is assertable."""

    def __init__(self, rows=(1,), fail_on_count=False, rescued=0):
        super().__init__(rows)
        self.persisted = 0
        self.unpersisted = 0
        self.fail_on_count = fail_on_count
        self.rescued = rescued

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


class _RawBatch(RecordingDataFrame):
    """Stands in for the raw batch_df `process_microbatch` receives. Only `.schema` is
    real: `landing.project` is monkeypatched away in these tests (it is the one call that
    needs real Spark - see `batch_session`'s docstring), so nothing else about this frame
    is read before `tables.ensure_landing_table(ctx, cfg, batch_df.schema)` is called.
    """

    def __init__(self):
        super().__init__()
        self.schema = FakeSchema({})


def _raw_batch():
    return _RawBatch()


def _session(config_root, **kwargs):
    from kafka_ingest.sources.file import config as file_config_module

    ctx = make_file_ctx(config_root, spark=FakeSpark(existing_tables=(LANDING_TABLE,)), **kwargs)
    cfg = file_config_module.build(ctx.cfg, ctx.run_type, ctx.tables)
    state = file_run._RunState(cfg=cfg)
    return file_run._Session(ctx=ctx, cfg=cfg, state=state, secrets=None)


@pytest.fixture
def batch_session(file_config_root, monkeypatch):
    """A session whose landing projection is replaced by a stand-in, exactly
    tests/test_kafka_run.py's `batch_session` fixture - the point is the ORDER and the
    FAILURE HANDLING of the microbatch body, not what the projection produces, which needs
    real Spark and is untested in the fast suite here (matching the Kafka precedent)."""
    session = _session(file_config_root)
    frames = {}

    def project(_raw, _cfg, _txn, _run_id):
        frames["landing"] = _Batch(rows=frames.get("rows", (1, 2, 3)), rescued=frames.get("rescued", 0))
        return frames["landing"]

    def rescued_count(df):
        return getattr(df, "rescued", 0)

    monkeypatch.setattr(file_run.landing, "project", project)
    monkeypatch.setattr(file_run.landing, "rescued_count", rescued_count)
    monkeypatch.setattr(file_run.tables, "ensure_landing_table", lambda ctx, cfg, schema: None)
    return session, frames


def test_a_healthy_microbatch_audits_landing_and_writes(batch_session):
    session, _ = batch_session
    session.process_microbatch(_raw_batch(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "COMPLETED"]
    assert session.state.rows_written["landing"] == 3
    assert session.state.rows_read == 3
    append = session.ctx.writers.append_into(session.cfg.landing_table)
    assert append["txn_version"] == 4
    assert append["txn_app_id"] == session.cfg.txn_app_id


def test_an_empty_batch_is_recorded_as_no_data_and_writes_nothing(batch_session):
    session, frames = batch_session
    frames["rows"] = ()
    session.process_microbatch(_raw_batch(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "NO_DATA"]
    assert session.ctx.writers.appends == []


def test_rescued_rows_under_failfast_refuse_the_batch(batch_session):
    """FAILFAST is the platform default - a batch holding rows that did not fit the
    configured schema must not land silently."""
    session, frames = batch_session
    frames["rescued"] = 2
    with pytest.raises(RuntimeError, match="did not fit the configured schema"):
        session.process_microbatch(_raw_batch(), 4)
    assert session.ctx.writers.appends == []
    assert session.ctx.audit.statuses("landing") == ["STARTED", "FAILED"]


def test_rescued_rows_under_quarantine_land_and_report_the_count(file_config_root, monkeypatch):
    from conftest import write_file_source

    write_file_source(file_config_root, failure_mode="QUARANTINE")
    session = _session(file_config_root)
    frames = {}

    def project(_raw, _cfg, _txn, _run_id):
        frames["landing"] = _Batch(rows=(1, 2, 3), rescued=1)
        return frames["landing"]

    monkeypatch.setattr(file_run.landing, "project", project)
    monkeypatch.setattr(file_run.landing, "rescued_count", lambda df: getattr(df, "rescued", 0))
    monkeypatch.setattr(file_run.tables, "ensure_landing_table", lambda ctx, cfg, schema: None)

    session.process_microbatch(_raw_batch(), 4)
    assert session.ctx.writers.appends != []
    assert session.state.rows_quarantined == 1
    completed = next(row for row in session.ctx.audit.rows if row["status"] == "COMPLETED")
    assert completed["quarantined_count"] == 1


def test_the_cache_is_released_on_the_failure_path_too(batch_session):
    """The poison-batch path is the one that runs over and over, and it is exactly where a
    leaked cache accumulates - so the unpersist is in a `finally`."""
    session, frames = batch_session
    frames["rows"] = ()
    frames["rescued"] = 0
    session.process_microbatch(_raw_batch(), 4)
    assert frames["landing"].persisted == 1
    assert frames["landing"].unpersisted == 1


def test_a_landing_failure_is_audited_against_landing(batch_session, monkeypatch):
    session, frames = batch_session

    def project(_raw, _cfg, _txn, _run_id):
        frames["landing"] = _Batch(fail_on_count=True)
        return frames["landing"]

    monkeypatch.setattr(file_run.landing, "project", project)
    with pytest.raises(RuntimeError, match="could not be counted"):
        session.process_microbatch(_raw_batch(), 4)
    assert session.ctx.audit.statuses("landing") == ["STARTED", "FAILED"]
    assert frames["landing"].unpersisted == 1
