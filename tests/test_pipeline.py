"""Run-shape dispatch, transaction identity, and the checkpoint-reset guard.

These are the parts of the framework that decide *whether* data is written and *under whose
identity*. None of them needs Spark: dispatch is a branch over TopicConfig, the transaction
id is a pure derivation, and the guard's inputs are a filesystem probe plus a row count.

WHY THE TRANSACTION ID ASSERTIONS PIN THE EXACT STRING
------------------------------------------------------
Delta dedups a retried batch on (txnAppId, txnVersion), and that history already exists in
deployed environments. Changing how `_make_txn_app_id` composes its value would not fail
loudly - it would silently start a fresh dedup lineage, so the first replayed batch after
the change would be written twice. Pinning the literal shape makes that a deliberate test
edit rather than an invisible regression.
"""

from __future__ import annotations

import os

import pytest

from conftest import FakeSpark
from kafka_ingest import pipeline
from kafka_ingest.config import (
    RUN_TYPE_CURATED_REPLAY,
    RUN_TYPE_KAFKA_REPLAY,
    resolve_topic_config,
)

CONTROL_TABLE = "ops.ingestion.control"


def _cfg(config_root, run_type="primary", enabled=True, **overrides):
    cfg = resolve_topic_config(
        FakeSpark(), config_root, "demo_topic", CONTROL_TABLE, "prod",
        run_type=run_type, overrides=overrides,
    )
    if not enabled:
        # `enabled` is an operational field, but going through the control table here would
        # test the config layer rather than the dispatch. Set it directly.
        from dataclasses import replace
        cfg = replace(cfg, enabled=False)
    return cfg


@pytest.fixture
def calls(monkeypatch):
    """Replace every run shape with a recorder, so run() is tested as pure dispatch."""
    recorded = []

    def recorder(name):
        def _record(*_args, **_kwargs):
            recorded.append(name)
        return _record

    for name in ("run_streaming", "run_bounded_replay", "run_curated_replay", "run_disabled"):
        monkeypatch.setattr(pipeline, name, recorder(name))
    return recorded


# --------------------------------------------------------------------------------------
# run() dispatch - four paths, and they must not be confusable
# --------------------------------------------------------------------------------------


def test_primary_run_goes_to_the_streaming_shape(config_root, calls):
    pipeline.run(FakeSpark(), _cfg(config_root), secrets=None)
    assert calls == ["run_streaming"]


def test_disabled_topic_is_skipped_rather_than_run(config_root, calls):
    """Disabling is the emergency stop. It must win over every other consideration."""
    pipeline.run(FakeSpark(), _cfg(config_root, enabled=False), secrets=None)
    assert calls == ["run_disabled"]


def test_disabling_stops_a_replay_too(config_root, calls):
    """A disabled topic must not be reachable through the replay entrypoints either -
    otherwise the emergency stop has a hole in it."""
    cfg = _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, enabled=False,
               rerun_id="INC1", starting_timestamp="0")
    pipeline.run(FakeSpark(), cfg, secrets=None)
    assert calls == ["run_disabled"]


def test_curated_replay_never_touches_the_kafka_shapes(config_root, calls):
    cfg = _cfg(config_root, run_type=RUN_TYPE_CURATED_REPLAY,
               rerun_id="FIX1", landing_filter="ingest_date = '2026-08-01'")
    pipeline.run(FakeSpark(), cfg, secrets=None)
    assert calls == ["run_curated_replay"]


def test_bounded_kafka_replay_uses_the_batch_shape(config_root, calls):
    """An explicit end offset/timestamp means a batch read - the streaming Kafka source has
    no ending-offset option, so routing this to the streaming shape would silently run to
    latest instead of stopping where the operator asked."""
    cfg = _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
               starting_timestamp="2026-08-11T09:00:00Z", ending_timestamp="2026-08-11T15:00:00Z")
    assert cfg.run.is_bounded
    pipeline.run(FakeSpark(), cfg, secrets=None)
    assert calls == ["run_bounded_replay"]


def test_unbounded_kafka_replay_uses_the_streaming_shape(config_root, calls):
    cfg = _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
               starting_timestamp="2026-08-11T09:00:00Z")
    assert not cfg.run.is_bounded
    pipeline.run(FakeSpark(), cfg, secrets=None)
    assert calls == ["run_streaming"]


# --------------------------------------------------------------------------------------
# Transaction identity - what Delta dedups retried batches against
# --------------------------------------------------------------------------------------


def test_txn_app_id_is_stable_across_calls(config_root):
    """Stability across restarts is the whole mechanism. A value derived from anything
    per-run (a uuid, a timestamp) would silently disable duplicate suppression."""
    cfg = _cfg(config_root)
    assert pipeline._make_txn_app_id(cfg) == pipeline._make_txn_app_id(cfg)


def test_txn_app_id_has_the_expected_shape(config_root):
    """Pinned deliberately - see the module docstring."""
    assert pipeline._make_txn_app_id(_cfg(config_root)) == "kafka_ingest::demo_topic::primary::primary"


def test_replay_and_primary_never_share_a_txn_app_id(config_root):
    """If they shared one, a replay's batch 0 would be skipped as a duplicate of the primary
    stream's batch 0 - the replay would report success and write nothing."""
    primary = pipeline._make_txn_app_id(_cfg(config_root))
    replay = pipeline._make_txn_app_id(
        _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", starting_timestamp="0")
    )
    assert primary != replay
    assert "INC1" in replay


def test_checkpoint_reset_id_forks_the_primary_txn_app_id(config_root):
    """The whole point of the override: a reset primary must not inherit the old lineage's
    already-committed versions, or the 'safe' restart would silently skip writes exactly
    like the guard exists to prevent."""
    original = pipeline._make_txn_app_id(_cfg(config_root))
    reset = pipeline._make_txn_app_id(_cfg(config_root, checkpoint_reset_id="INC12345"))
    assert original != reset
    assert "INC12345" in reset


def test_two_replays_have_distinct_txn_app_ids(config_root):
    def app_id(rerun_id):
        return pipeline._make_txn_app_id(
            _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY,
                 rerun_id=rerun_id, starting_timestamp="0")
        )

    assert app_id("INC1") != app_id("INC2")


def test_run_id_identifies_the_topic_and_run_type(config_root):
    run_id = pipeline._make_run_id(_cfg(config_root, job_run_id="12345"))
    assert run_id == "demo_topic-primary-12345"


def test_run_id_is_unique_when_no_job_run_id_is_supplied(config_root):
    """Interactive runs have no Databricks job run id; two of them must still be tellable
    apart in the audit table."""
    cfg = _cfg(config_root)
    assert pipeline._make_run_id(cfg) != pipeline._make_run_id(cfg)


# --------------------------------------------------------------------------------------
# The checkpoint-reset guard - the one check that prevents silent data loss
# --------------------------------------------------------------------------------------


@pytest.fixture
def checkpoint(monkeypatch):
    """Control what the guard's filesystem probe finds.

    checkpoint_root is validated to be a /Volumes/... path, which exists on no test machine,
    so the probe is stubbed rather than the config being bent out of shape. Returns a
    callable that marks one config's checkpoint as present.
    """
    present: set[str] = set()

    def fake_stat(path, *_args, **_kwargs):
        if str(path) not in present:
            raise FileNotFoundError(2, "No such file or directory", str(path))
        return None

    monkeypatch.setattr(pipeline.os, "stat", fake_stat)

    def create(cfg):
        present.add(os.path.join(cfg.checkpoint_path, "offsets"))

    return create


def test_guard_allows_a_run_whose_checkpoint_is_intact(config_root, checkpoint):
    cfg = _cfg(config_root)
    checkpoint(cfg)
    spark = FakeSpark(control_rows=[{"topic": "demo.events.v1"}],
                      existing_tables={cfg.landing_table})
    pipeline.guard_against_checkpoint_reset(spark, cfg)   # must not raise


def test_guard_allows_a_genuine_first_run(config_root, checkpoint):
    """No checkpoint and no landing rows is what a brand new topic looks like. Blocking it
    would be as damaging as missing the case the guard exists for."""
    cfg = _cfg(config_root)
    spark = FakeSpark(control_rows=[], existing_tables={cfg.landing_table})
    pipeline.guard_against_checkpoint_reset(spark, cfg)   # must not raise


def test_guard_allows_a_run_when_the_landing_table_does_not_exist_yet(config_root, checkpoint):
    cfg = _cfg(config_root)
    pipeline.guard_against_checkpoint_reset(FakeSpark(), cfg)   # must not raise


def test_guard_refuses_when_the_checkpoint_vanished_but_data_exists(config_root, checkpoint):
    """The case the guard exists for: batch ids would restart at 0 and Delta would skip
    every write as a duplicate, so the job would report success and ingest nothing."""
    cfg = _cfg(config_root)
    spark = FakeSpark(control_rows=[{"topic": "demo.events.v1"}],
                      existing_tables={cfg.landing_table})
    with pytest.raises(RuntimeError, match="REFUSING TO RUN"):
        pipeline.guard_against_checkpoint_reset(spark, cfg)


def test_checkpoint_reset_override_lets_the_guard_through(config_root, checkpoint, caplog):
    """Exactly the scenario the guard exists to refuse - checkpoint gone, landing already has
    rows - but with support's explicit, logged override in place."""
    cfg = _cfg(config_root, checkpoint_reset_id="INC12345")
    spark = FakeSpark(control_rows=[{"topic": "demo.events.v1"}],
                      existing_tables={cfg.landing_table})
    with caplog.at_level("WARNING"):
        pipeline.guard_against_checkpoint_reset(spark, cfg)   # must not raise
    assert "INC12345" in caplog.text
    assert cfg.topic_key in caplog.text


def test_guard_never_blocks_a_replay(config_root, checkpoint):
    """Replays have their own checkpoint and their own txnAppId, so they cannot collide -
    and a replay is exactly what an operator is told to run after a checkpoint is lost."""
    cfg = _cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY,
               rerun_id="INC1", starting_timestamp="0")
    spark = FakeSpark(control_rows=[{"topic": "demo.events.v1"}],
                      existing_tables={cfg.landing_table})
    pipeline.guard_against_checkpoint_reset(spark, cfg)   # must not raise


def test_an_unreadable_checkpoint_volume_is_not_treated_as_a_missing_checkpoint(
    config_root, monkeypatch
):
    """A driver that cannot reach the Volume must not be mistaken for a deleted checkpoint.

    os.path.exists() would have swallowed the error and returned False, which would refuse
    to start a perfectly healthy stream. The two cases get different, explicit outcomes.
    """
    def boom(_path, *_args, **_kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(pipeline.os, "stat", boom)
    cfg = _cfg(config_root)
    with pytest.raises(RuntimeError, match="NOT the same as the checkpoint being missing"):
        pipeline.guard_against_checkpoint_reset(FakeSpark(), cfg)


# --------------------------------------------------------------------------------------
# Table properties - one template in conf/defaults.yaml, overridable per topic
# --------------------------------------------------------------------------------------


def test_table_properties_come_from_config(config_root):
    from kafka_ingest.tables import _properties_clause

    cfg = _cfg(config_root)
    clause = _properties_clause(cfg)
    assert "'delta.autoOptimize.optimizeWrite' = 'true'" in clause
    assert "'delta.autoOptimize.autoCompact' = 'true'" in clause


def test_a_topic_can_override_the_table_properties(config_root):
    """The escape hatch for one feed that genuinely needs different Delta properties."""
    from dataclasses import replace

    from kafka_ingest.tables import _properties_clause

    cfg = replace(_cfg(config_root), table_properties={"delta.appendOnly": "true"})
    assert _properties_clause(cfg) == "'delta.appendOnly' = 'true'"


def test_a_quote_in_a_table_property_is_refused(config_root):
    """It would terminate the TBLPROPERTIES string early and produce a confusing SQL error."""
    from dataclasses import replace

    from kafka_ingest.tables import _properties_clause

    cfg = replace(_cfg(config_root), table_properties={"delta.x": "it's bad"})
    with pytest.raises(ValueError, match="single quote"):
        _properties_clause(cfg)


def test_properties_fall_back_when_config_supplies_none(config_root):
    """A config predating the template still gets auto-compaction rather than nothing."""
    from dataclasses import replace

    from kafka_ingest.tables import _properties_clause

    cfg = replace(_cfg(config_root), table_properties={})
    assert "autoOptimize" in _properties_clause(cfg)


# --------------------------------------------------------------------------------------
# Curated is created by this framework, never left to Spark
# --------------------------------------------------------------------------------------


def test_curated_table_is_created_explicitly_from_the_reader_schema(config_root, monkeypatch):
    """The whole point: curated must not be conjured by the first saveAsTable.

    pipeline.ensure_curated derives the schema on the driver and issues explicit DDL, so the
    table exists with a shape this framework chose - and with the same TBLPROPERTIES every
    other table gets.
    """
    from kafka_ingest import curated_writer, pipeline, tables

    created = {}

    def fake_schema(_spark, _cfg, reader_schema):
        created["reader_schema"] = reader_schema
        return "STUB_SCHEMA"

    def fake_create(_spark, cfg, schema):
        created["table"] = cfg.curated_table
        created["schema"] = schema

    monkeypatch.setattr(pipeline, "curated_schema", fake_schema)
    monkeypatch.setattr(tables, "ensure_curated_table", fake_create)

    cfg = _cfg(config_root)
    ctx = pipeline.PipelineContext(
        spark=FakeSpark(), cfg=cfg, client=None, audit=None,
        run_id="run-1", txn_app_id="txn", reader_schema=(5512, "{}"),
    )
    pipeline.ensure_curated(FakeSpark(), cfg, ctx)

    assert created["table"] == cfg.curated_table
    assert created["schema"] == "STUB_SCHEMA"
    # It must use the reader schema resolved for THIS run, not re-resolve its own.
    assert created["reader_schema"] == (5512, "{}")
    assert curated_writer.curated_schema is not fake_schema  # only pipeline was patched
