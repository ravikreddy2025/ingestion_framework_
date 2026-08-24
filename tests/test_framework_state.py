"""Durable state: the watermark and the run sequence.

THE POINT OF THIS MODULE IS THE ASYMMETRY WITH AUDIT. Audit writes must never raise; state
writes must. Every test below that asserts a failure propagates is guarding a real failure
mode: a watermark that silently failed to advance re-reads a window, and one that silently
advanced skips one - and neither is visible until someone counts rows.

Needs no Spark. framework/state.py imports no PySpark at all: the session arrives as an
argument and delta.tables is imported inside the one function that needs it, which is what
lets the recording stand-ins in conftest test the whole path.
"""

from __future__ import annotations

import pytest

from conftest import FakeSpark
from kafka_ingest.framework import state as state_module
from kafka_ingest.framework.config import ConfigError, resolve_config
from kafka_ingest.framework.state import STATE_RUN_SEQUENCE, STATE_WATERMARK, StateStore

STATE_TABLE = "ops_prod.ingestion.ingest_state"


def _store(rows=None, spark=None):
    session = spark or FakeSpark(rows_by_table={STATE_TABLE: rows or []})
    return StateStore(session, STATE_TABLE, run_id="run-1"), session


# --------------------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------------------


def test_nothing_written_yet_reads_as_none():
    """A first run sees None, and callers must read that as "start from the beginning" -
    never as a failure."""
    store, _ = _store([])
    assert store.read_state("demo_source", STATE_WATERMARK) is None


def test_a_stored_value_comes_back_as_text():
    store, _ = _store([{"state_value": "2026-08-11T09:00:00Z"}])
    assert store.read_state("demo_source", STATE_WATERMARK) == "2026-08-11T09:00:00Z"


def test_duplicate_rows_for_one_key_are_an_error():
    """Two rows means two answers to "where did we get to", and picking either silently is
    how a window gets re-read or skipped."""
    store, _ = _store([{"state_value": "1"}, {"state_value": "2"}])
    with pytest.raises(ConfigError, match="one row per key"):
        store.read_state("demo_source", STATE_WATERMARK)


def test_a_read_failure_propagates():
    """State is not best-effort. A read that cannot answer must stop the run, not default."""

    class Broken(FakeSpark):
        def table(self, name):
            raise RuntimeError("metastore unavailable")

    store, _ = _store(spark=Broken())
    with pytest.raises(RuntimeError, match="metastore unavailable"):
        store.read_state("demo_source", STATE_WATERMARK)


# --------------------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------------------


def test_a_write_upserts_on_the_two_key_columns(fake_delta):
    store, spark = _store([])
    store.write_state("demo_source", STATE_WATERMARK, "2026-08-11", "string", "run-1")

    merge = fake_delta.last.merge_op
    assert fake_delta.last.name == STATE_TABLE
    assert merge.condition == "t.source_key = s.source_key AND t.state_key = s.state_key"
    assert merge.clauses == ["whenMatchedUpdateAll", "whenNotMatchedInsertAll"]
    assert merge.executed
    values, _schema = spark.created_frames[0]
    assert values[0][:4] == ("demo_source", STATE_WATERMARK, "2026-08-11", "string")
    assert values[0][5] == "run-1"


def test_a_failed_write_raises(fake_delta, monkeypatch):
    """THE rule of this module, and the deliberate opposite of framework/audit.py. Someone
    will try to make the two consistent; this is what stops them."""
    from conftest import RecordingMerge

    def boom(self):
        raise RuntimeError("delta write failed")

    monkeypatch.setattr(RecordingMerge, "execute", boom)
    store, _ = _store([])
    with pytest.raises(RuntimeError, match="delta write failed"):
        store.write_state("demo_source", STATE_WATERMARK, "2026-08-11", "string", "run-1")


def test_a_state_key_that_did_not_come_from_configuration_is_rejected(fake_delta):
    """Both key columns are interpolated into SQL. They come from a filename stem and a
    fixed vocabulary, never from data, and this refuses anything that is not."""
    store, _ = _store([])
    with pytest.raises(ConfigError, match="not allowed"):
        store.read_state("demo'; DROP TABLE x --", STATE_WATERMARK)


# --------------------------------------------------------------------------------------
# The run sequence
# --------------------------------------------------------------------------------------


def test_the_first_run_sequence_is_one(fake_delta):
    store, _ = _store([])
    assert store.next_run_sequence("demo_source") == 1


def test_the_run_sequence_increments_from_what_is_stored(fake_delta):
    store, _ = _store([{"state_value": "41"}])
    assert store.next_run_sequence("demo_source") == 42


def test_the_run_sequence_is_written_before_it_is_returned(fake_delta, monkeypatch):
    """A run that dies mid-way must BURN its number rather than let the next run reuse it -
    reusing one means two runs writing under the same Delta txnVersion, where the second is
    silently dropped as a duplicate."""
    from conftest import RecordingMerge

    def boom(self):
        raise RuntimeError("delta write failed")

    monkeypatch.setattr(RecordingMerge, "execute", boom)
    store, _ = _store([{"state_value": "41"}])
    with pytest.raises(RuntimeError):
        store.next_run_sequence("demo_source")


def test_the_run_sequence_is_stored_as_an_int_typed_value(fake_delta):
    store, spark = _store([])
    store.next_run_sequence("demo_source")
    values, _schema = spark.created_frames[0]
    assert values[0][1] == STATE_RUN_SEQUENCE
    assert values[0][2] == "1"
    assert values[0][3] == "int"


def test_the_run_id_that_allocated_the_sequence_is_recorded(fake_delta):
    """ "Which run moved this?" is the first question of every late-data investigation."""
    store, spark = _store([])
    store.next_run_sequence("demo_source")
    assert spark.created_frames[0][0][0][5] == "run-1"


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


def test_a_source_with_no_state_table_configured_is_told_so_plainly():
    """Better than an AttributeError on a None table name three frames deeper."""
    store = StateStore(FakeSpark(), None, run_id="run-1")
    with pytest.raises(ConfigError, match="no `state_table` is configured"):
        store.read_state("demo_source", STATE_WATERMARK)


def test_ensure_creates_a_table_partitioned_by_source_key_with_deletion_vectors(demo_config_root, demo_spec):
    """docs/build_log/DECISIONS.md D-04: partitioning isolates concurrent MERGEs from
    different sources at the file level, and deletion vectors make the small, frequent,
    single-row MERGE updates this table takes cheaper than rewriting a file per update."""
    cfg = resolve_config(demo_config_root, "demo_source", "prod", demo_spec)
    spark = FakeSpark()
    state_module.ensure_state_table(spark, cfg, STATE_TABLE)
    statement = spark.sql_statements[0]
    assert f"CREATE TABLE IF NOT EXISTS {STATE_TABLE}" in statement
    assert "PARTITIONED BY (source_key)" in statement
    assert "'delta.enableDeletionVectors' = 'true'" in statement


def test_ensure_keeps_the_configured_properties_alongside_deletion_vectors(demo_config_root, demo_spec):
    """Deletion vectors must be ADDED to whatever table_properties configuration already
    carries, not replace it - losing autoOptimize on this table was not the point."""
    cfg = resolve_config(demo_config_root, "demo_source", "prod", demo_spec)
    spark = FakeSpark()
    state_module.ensure_state_table(spark, cfg, STATE_TABLE)
    statement = spark.sql_statements[0]
    assert "'delta.autoOptimize.optimizeWrite' = 'true'" in statement
