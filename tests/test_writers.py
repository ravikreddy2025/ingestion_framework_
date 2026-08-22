"""What the writers actually hand to Delta.

Every other test in this suite stops at the DataFrame. These start where those stop, and
they cover the two decisions that determine whether data is correct on a re-run:

  * the Delta idempotent-write markers (txnAppId / txnVersion), which is what makes a
    retried microbatch a no-op instead of a duplicate
  * append versus MERGE, which is what stops a replay duplicating everything it overlaps

Both are plain option and method calls, so a recording stand-in (conftest.RecordingWriter /
RecordingDeltaTable) tests them exactly - no Delta, no JVM, no cluster. The stand-in does
not simulate merge semantics on purpose: these assert which branch was taken and what it was
given, not what Delta would do with it.

NOTE ON THE FILE ITSELF
-----------------------
This lives in its own module rather than joining test_curated_writer.py because that module
requires a live SparkSession and the spark-avro connector for every test in it. These need
neither and belong in the fast suite that runs on every pull request.
"""

from __future__ import annotations

import pytest

from conftest import FakeSpark, RecordingDataFrame
from kafka_ingest.audit import LAYER_LANDING, STATUS_COMPLETED, AuditWriter
from kafka_ingest.config import RUN_TYPE_CURATED_REPLAY, RUN_TYPE_KAFKA_REPLAY, resolve_topic_config
from kafka_ingest.curated_writer import write_curated, write_quarantine
from kafka_ingest.landing_writer import RECORD_KEYS, write_landing

CONTROL_TABLE = "ops.ingestion.control"
TXN_APP_ID = "kafka_ingest::demo_topic::primary::primary"

# The merge key both layers use. Spelled out here rather than derived, so a change to
# RECORD_KEYS has to be acknowledged in a test rather than silently following along.
EXPECTED_MERGE_CONDITION = (
    "t.topic = s.topic AND t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset"
)
# Both layers are one table per topic and merge on the same record key, so the condition is
# identical. Aliased rather than reused directly so that a future divergence has somewhere
# obvious to live.
EXPECTED_LANDING_MERGE_CONDITION = EXPECTED_MERGE_CONDITION


def _cfg(config_root, run_type="primary", **overrides):
    return resolve_topic_config(
        FakeSpark(), config_root, "demo_topic", CONTROL_TABLE, "prod",
        run_type=run_type, overrides=overrides,
    )


def _replay_cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY):
    if run_type == RUN_TYPE_CURATED_REPLAY:
        return _cfg(config_root, run_type=run_type, rerun_id="FIX1",
                    landing_filter="ingest_date = '2026-08-01'")
    return _cfg(config_root, run_type=run_type, rerun_id="INC1", starting_timestamp="0")


def test_record_keys_are_what_the_merge_condition_is_built_from():
    assert RECORD_KEYS == ("topic", "kafka_partition", "kafka_offset")


# --------------------------------------------------------------------------------------
# Landing - append with idempotency markers on the primary stream
# --------------------------------------------------------------------------------------


def test_primary_landing_append_carries_the_idempotency_markers(config_root):
    """This is the mechanism the whole no-duplicates story rests on. Without these two
    options a retried batch appends its rows a second time."""
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_landing(FakeSpark(), df, cfg, batch_id=7, txn_app_id=TXN_APP_ID)

    assert df.write.format_used == "delta"
    assert df.write.mode_used == "append"
    assert df.write.options["txnAppId"] == TXN_APP_ID
    assert df.write.options["txnVersion"] == 7
    assert df.write.saved_as == cfg.landing_table


def test_non_streaming_batch_id_gets_no_idempotency_markers(config_root):
    """A bounded batch replay uses batch id -1. txnVersion must be a real microbatch id, so
    the markers are skipped and idempotency comes from the MERGE key instead."""
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_landing(FakeSpark(), df, cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert df.write.mode_used == "append"
    assert "txnAppId" not in df.write.options
    assert "txnVersion" not in df.write.options


def test_landing_append_without_a_txn_app_id_sets_no_markers(config_root):
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_landing(FakeSpark(), df, cfg, batch_id=0, txn_app_id=None)
    assert "txnAppId" not in df.write.options


def test_batch_id_zero_still_gets_markers(config_root):
    """Guards the boundary: batch 0 is a real microbatch, and `if batch_id` rather than
    `if batch_id >= 0` would silently skip deduplication for the first batch of every run."""
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_landing(FakeSpark(), df, cfg, batch_id=0, txn_app_id=TXN_APP_ID)
    assert df.write.options["txnVersion"] == 0


# --------------------------------------------------------------------------------------
# Landing - MERGE on replay, insert-if-absent only
# --------------------------------------------------------------------------------------


def test_landing_replay_merges_instead_of_appending(config_root, fake_delta):
    """A replay overlaps existing data by definition, so appending would duplicate it."""
    cfg = _replay_cfg(config_root)
    df = RecordingDataFrame()
    spark = FakeSpark(existing_tables={cfg.landing_table})

    write_landing(spark, df, cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    table = fake_delta.last
    assert table is not None, "replay must go through DeltaTable.merge, not a plain append"
    assert table.name == cfg.landing_table
    assert table.merge_op.condition == EXPECTED_LANDING_MERGE_CONDITION
    assert table.merge_op.executed
    assert df.write.saved_as is None, "must not also append"


def test_landing_replay_never_overwrites_the_original_arrival_record(config_root, fake_delta):
    """Insert-if-absent ONLY. The original landing row records what arrived on the primary
    stream; rewriting its provenance columns with replay metadata would destroy that.
    Curated deliberately does the opposite - see below."""
    cfg = _replay_cfg(config_root)
    spark = FakeSpark(existing_tables={cfg.landing_table})

    write_landing(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert fake_delta.last.merge_op.clauses == ["whenNotMatchedInsertAll"]


def test_landing_replay_into_a_missing_table_falls_back_to_append(config_root, fake_delta):
    """Rebuilding a dropped landing table from the broker is unusual but legal - there is
    simply nothing to merge against."""
    cfg = _replay_cfg(config_root)
    df = RecordingDataFrame()

    write_landing(FakeSpark(existing_tables=set()), df, cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert fake_delta.last is None
    assert df.write.mode_used == "append"
    assert df.write.saved_as == cfg.landing_table


# --------------------------------------------------------------------------------------
# Curated - append on primary, upsert on replay
# --------------------------------------------------------------------------------------


def test_primary_curated_append_partitions_and_carries_markers(config_root):
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_curated(FakeSpark(), df, cfg, batch_id=3, txn_app_id=TXN_APP_ID)

    assert df.write.mode_used == "append"
    # partitionBy is what CREATES the partitioning on the first write - curated cannot be
    # pre-created with DDL because its payload struct is schema-derived.
    assert df.write.partition_by == cfg.curated_partition_by == ["event_date"]
    # mergeSchema lets an additive Avro change land without a manual ALTER TABLE.
    assert df.write.options["mergeSchema"] == "true"
    assert df.write.options["txnAppId"] == TXN_APP_ID
    assert df.write.options["txnVersion"] == 3
    assert df.write.saved_as == cfg.curated_table


def test_curated_replay_upserts_because_a_bad_parse_must_be_replaced(config_root, fake_delta):
    """The whole point of a curated replay is to REPLACE a bad parse with a good one, so
    unlike landing it updates matched rows as well as inserting missing ones."""
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    spark = FakeSpark(existing_tables={cfg.curated_table})

    write_curated(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    merge = fake_delta.last.merge_op
    assert merge.condition == EXPECTED_MERGE_CONDITION
    assert merge.clauses == ["withSchemaEvolution", "whenMatchedUpdateAll", "whenNotMatchedInsertAll"]
    assert merge.executed


# --------------------------------------------------------------------------------------
# Curated replay must tolerate a widened schema
#
# The append path sets mergeSchema=true, so an additive Avro change widens curated on the
# primary stream. MERGE does not honour that option - and a curated replay is precisely what
# gets run after a schema change. Without evolution the replay fails on the schema it was
# run to apply.
# --------------------------------------------------------------------------------------


def test_curated_replay_allows_the_schema_to_widen(config_root, fake_delta):
    """Uses the operation-scoped builder method when the runtime has it (DBR 15.4 LTS+)."""
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    spark = FakeSpark(existing_tables={cfg.curated_table})

    write_curated(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert "withSchemaEvolution" in fake_delta.last.merge_op.clauses
    # The operation-scoped API needs no session flag at all.
    assert spark.conf.history == []


def test_older_runtimes_fall_back_to_the_session_flag_and_restore_it(
    config_root, fake_delta, monkeypatch
):
    """On a runtime without withSchemaEvolution() the legacy flag is the only mechanism.

    It must be scoped to this one merge: leaving it on would silently change how every other
    write in the session handles an unexpected column.
    """
    from conftest import RecordingMerge
    from kafka_ingest.curated_writer import _AUTO_MERGE_CONF

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    spark = FakeSpark(existing_tables={cfg.curated_table})

    write_curated(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert fake_delta.last.merge_op.executed
    assert spark.conf.history == [("set", _AUTO_MERGE_CONF, "true"), ("unset", _AUTO_MERGE_CONF, None)]
    assert _AUTO_MERGE_CONF not in spark.conf.values


def test_the_session_flag_is_restored_not_cleared_when_it_was_already_set(
    config_root, fake_delta, monkeypatch
):
    """Another job on a shared session may have set it deliberately; clearing it would be a
    surprising side effect of running a replay."""
    from conftest import RecordingMerge
    from kafka_ingest.curated_writer import _AUTO_MERGE_CONF

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    spark = FakeSpark(existing_tables={cfg.curated_table}, conf={_AUTO_MERGE_CONF: "false"})

    write_curated(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert spark.conf.values[_AUTO_MERGE_CONF] == "false"


def test_the_session_flag_is_restored_even_if_the_merge_fails(config_root, fake_delta, monkeypatch):
    """A failed replay must not leave schema auto-merge switched on for the whole session."""
    from conftest import RecordingMerge
    from kafka_ingest.curated_writer import _AUTO_MERGE_CONF

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")

    def boom(self):
        raise RuntimeError("merge failed")

    monkeypatch.setattr(RecordingMerge, "execute", boom)
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    spark = FakeSpark(existing_tables={cfg.curated_table})

    with pytest.raises(RuntimeError, match="merge failed"):
        write_curated(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert _AUTO_MERGE_CONF not in spark.conf.values


def test_landing_replay_does_not_ask_for_schema_evolution(config_root, fake_delta):
    """Landing's schema is fixed DDL and never evolves. Asking for evolution there would
    invite an unexpected column to silently widen the system of record."""
    cfg = _replay_cfg(config_root)
    spark = FakeSpark(existing_tables={cfg.landing_table})

    write_landing(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    assert "withSchemaEvolution" not in fake_delta.last.merge_op.clauses
    assert spark.conf.history == []


def test_curated_replay_sets_no_idempotency_markers(config_root, fake_delta):
    """A replay's idempotency comes from the MERGE key. Reusing the primary stream's
    txnVersion sequence here would make a replay's batch collide with a real one."""
    cfg = _replay_cfg(config_root, RUN_TYPE_CURATED_REPLAY)
    df = RecordingDataFrame()
    write_curated(FakeSpark(existing_tables=set()), df, cfg, batch_id=2, txn_app_id=TXN_APP_ID)
    assert "txnAppId" not in df.write.options


# --------------------------------------------------------------------------------------
# Quarantine
# --------------------------------------------------------------------------------------


def test_quarantine_always_appends_under_its_own_app_id(config_root):
    """A record can legitimately be quarantined twice - once by primary, once by a replay
    that still lacked the schema - and both attempts are evidence worth keeping. The
    distinct app id keeps DESCRIBE HISTORY readable."""
    cfg = _cfg(config_root)
    df = RecordingDataFrame()
    write_quarantine(df, cfg, batch_id=5, txn_app_id=TXN_APP_ID)

    assert df.write.mode_used == "append"
    assert df.write.options["txnAppId"] == f"{TXN_APP_ID}::quarantine"
    assert df.write.options["txnVersion"] == 5
    assert df.write.saved_as == cfg.quarantine_table


def test_quarantine_app_id_never_collides_with_the_landing_one(config_root):
    """Delta tracks (appId, version) per table, so sharing would be harmless - but these
    land in different tables and a shared id makes the history much harder to read."""
    cfg = _cfg(config_root)
    landing, quarantine = RecordingDataFrame(), RecordingDataFrame()
    write_landing(FakeSpark(), landing, cfg, batch_id=1, txn_app_id=TXN_APP_ID)
    write_quarantine(quarantine, cfg, batch_id=1, txn_app_id=TXN_APP_ID)
    assert landing.write.options["txnAppId"] != quarantine.write.options["txnAppId"]


# --------------------------------------------------------------------------------------
# Audit - must never be the reason a good batch fails
# --------------------------------------------------------------------------------------


class _AuditSpark(FakeSpark):
    def __init__(self, fail=False):
        super().__init__()
        self._fail = fail
        self.frame = RecordingDataFrame()

    def createDataFrame(self, data, schema=None):  # noqa: N802 - mirrors the Spark API
        if self._fail:
            raise RuntimeError("simulated Delta failure")
        self.rows = list(data)
        return self.frame


def test_audit_row_is_appended_to_the_audit_table(config_root):
    cfg = _cfg(config_root)
    spark = _AuditSpark()
    AuditWriter(spark, cfg, "run-1").emit(LAYER_LANDING, STATUS_COMPLETED, 4, record_count=10)

    assert spark.frame.write.mode_used == "append"
    assert spark.frame.write.saved_as == cfg.audit_table


def test_a_failed_audit_write_never_fails_the_batch(config_root):
    """Auditing must never be the reason a good batch fails. If this ever raises, a Delta
    hiccup on the audit table takes the ingestion down with it."""
    cfg = _cfg(config_root)
    AuditWriter(_AuditSpark(fail=True), cfg, "run-1").emit(LAYER_LANDING, STATUS_COMPLETED, 4)


def test_audit_write_failure_is_logged_loudly(config_root, caplog):
    """Swallowed is not the same as hidden - the failure has to reach the driver log."""
    cfg = _cfg(config_root)
    with caplog.at_level("ERROR"):
        AuditWriter(_AuditSpark(fail=True), cfg, "run-1").emit(LAYER_LANDING, STATUS_COMPLETED, 4)
    assert "Failed to write audit row" in caplog.text


@pytest.mark.parametrize("layer_status", [(LAYER_LANDING, STATUS_COMPLETED)])
def test_audit_values_are_positional_in_schema_order(config_root, layer_status):
    """createDataFrame with an explicit StructType does not reorder dict keys, so the row is
    built as a positional tuple. A missing key would put a value in the wrong column."""
    from kafka_ingest.audit import AUDIT_SCHEMA

    cfg = _cfg(config_root)
    spark = _AuditSpark()
    AuditWriter(spark, cfg, "run-1").emit(*layer_status, 4, record_count=10)

    values = spark.rows[0]
    assert len(values) == len(AUDIT_SCHEMA.fields)
    assert values[AUDIT_SCHEMA.fieldNames().index("record_count")] == 10
    assert values[AUDIT_SCHEMA.fieldNames().index("audit_id")] == "run-1::4::landing::COMPLETED"


def test_landing_merge_does_not_pin_a_topic_literal(config_root, fake_delta):
    """Landing is one table per topic, so a literal `t.topic = '...'` would match every row
    and prune nothing.

    It was there when landing was one shared table partitioned by topic, where it bought
    static partition elimination. Now `topic` is not a partition column at all - pruning
    comes from ingest_date - so the literal would be noise in the condition and a misleading
    hint that the table holds more than one feed.
    """
    cfg = _replay_cfg(config_root)
    spark = FakeSpark(existing_tables={cfg.landing_table})

    write_landing(spark, RecordingDataFrame(), cfg, batch_id=-1, txn_app_id=TXN_APP_ID)

    condition = fake_delta.last.merge_op.condition
    assert f"t.topic = '{cfg.topic}'" not in condition
    assert condition == EXPECTED_MERGE_CONDITION
