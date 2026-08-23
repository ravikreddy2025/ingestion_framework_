"""The shared audit table: its three definitions, and its one absolute rule.

THREE DEFINITIONS THAT MUST AGREE
---------------------------------
    AuditWriter.build_row()  the dict a row is built from
    AUDIT_SCHEMA             the StructType the DataFrame is created with
    AUDIT_DDL_COLUMNS        the columns the table is created with
    sql/02_layer_tables.sql  the columns an environment is provisioned with

Any two of them drifting apart surfaces as a Delta schema error on the first append, on a
cluster, during an incident - which is the worst possible moment to discover it. All four
are compared here instead.

ONE ABSOLUTE RULE
-----------------
Audit writes never raise. A test drives a deliberately failing underlying write and asserts
the failure is logged and swallowed, because the day this stops being true is the day a
Delta hiccup on the audit table takes ingestion down with it.

Needs no Spark: the writer builds a positional tuple and hands it to createDataFrame, so a
recording stand-in tests exactly what reaches the table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="framework/audit.py declares a StructType")

from conftest import FakeSpark, RecordingDataFrame, ddl_column_names, sql_table_columns
from kafka_ingest.framework import audit as audit_module
from kafka_ingest.framework.audit import (
    AUDIT_DDL_COLUMNS,
    AUDIT_SCHEMA,
    LAYER_RUN,
    STATUS_COMPLETED,
    STATUS_FAILED,
    AuditWriter,
)
from kafka_ingest.framework.config import resolve_config

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "02_layer_tables.sql"
AUDIT_TABLE = "cat_prod.audit.ingest_audit"


@pytest.fixture
def cfg(demo_config_root, demo_spec):
    return resolve_config(demo_config_root, "demo_source", "prod", demo_spec)


@pytest.fixture
def audit(cfg):
    # spark is unused by build_row; emit() is exercised against a stand-in below.
    return AuditWriter(None, cfg, AUDIT_TABLE, "run-1", "primary", job_run_id="4417")


class _AuditSpark(FakeSpark):
    """A session whose createDataFrame can be told to fail, as Delta occasionally will."""

    def __init__(self, fail=False):
        super().__init__()
        self._fail = fail
        self.frame = RecordingDataFrame()

    def createDataFrame(self, data, schema=None):  # noqa: N802 - mirrors the Spark API
        if self._fail:
            raise RuntimeError("simulated Delta failure")
        self.rows = list(data)
        self.schema_used = schema
        return self.frame


# --------------------------------------------------------------------------------------
# The three-way agreement
# --------------------------------------------------------------------------------------


def test_the_row_supplies_every_column_the_schema_declares(audit):
    """emit() builds a positional tuple in AUDIT_SCHEMA order - a missing key is a KeyError
    at write time, i.e. a lost audit row during an incident."""
    row = audit.build_row(LAYER_RUN, STATUS_COMPLETED)
    assert set(row) == {field.name for field in AUDIT_SCHEMA.fields}


def test_the_schema_and_the_ddl_agree_column_for_column():
    assert ddl_column_names(AUDIT_DDL_COLUMNS) == [f.name for f in AUDIT_SCHEMA.fields]


def test_the_provisioning_sql_agrees_with_both():
    """sql/02 provisions the table ahead of the first run. A column present in one and not
    the other fails on the first append, on a cluster."""
    sql = SQL_PATH.read_text(encoding="utf-8")
    assert sql_table_columns(sql, "audit.ingest_audit") == ddl_column_names(AUDIT_DDL_COLUMNS)


def test_the_position_columns_say_on_themselves_that_they_mean_three_things():
    """A support engineer reads the column comment before they read any document, and
    position_start holds a Kafka offsets JSON, a cursor value or a file boundary."""
    assert "THREE MEANINGS" in AUDIT_DDL_COLUMNS
    assert "JSON STRING, not a map" in AUDIT_DDL_COLUMNS


# --------------------------------------------------------------------------------------
# What a row carries
# --------------------------------------------------------------------------------------


def test_a_row_identifies_the_source_generically(audit, cfg):
    """source_type / source_key / source_ref are what make ONE audit table serve every
    source type - no per-type column, no per-type table."""
    row = audit.build_row("landing", STATUS_COMPLETED)
    assert row["source_type"] == "demo"
    assert row["source_key"] == "demo_source"
    assert row["domain"] == "demo"
    assert row["run_type"] == "primary"
    assert row["job_run_id"] == "4417"


def test_source_ref_is_whatever_the_source_set_on_the_writer(audit):
    """Only the source knows its own source-side identifier, and the framework must not
    learn which config key it lives under. So the source sets it; the framework carries it."""
    assert audit.build_row("landing", STATUS_COMPLETED)["source_ref"] is None
    audit.source_ref = "HR.EMPLOYEES"
    assert audit.build_row("landing", STATUS_COMPLETED)["source_ref"] == "HR.EMPLOYEES"


def test_the_audit_id_is_unique_per_layer_and_status(audit):
    """Several rows per run share a run_id, so the id must include layer and status."""
    ids = {
        audit.build_row(layer, status, 3)["audit_id"]
        for layer in (LAYER_RUN, "landing")
        for status in ("STARTED", "COMPLETED")
    }
    assert len(ids) == 4
    assert "run-1::3::landing::STARTED" in ids


def test_a_replay_row_carries_its_rerun_id(demo_config_root, demo_spec):
    """rerun_id is framework-owned and operational-only: a replay id checked into Git would
    re-apply on every future deploy."""
    cfg = resolve_config(demo_config_root, "demo_source", "prod", demo_spec, control={"rerun_id": "INC42"})
    row = AuditWriter(None, cfg, AUDIT_TABLE, "run-9", "demo_replay").build_row("landing", STATUS_COMPLETED)
    assert row["run_type"] == "demo_replay"
    assert row["rerun_id"] == "INC42"


def test_the_read_boundary_is_recorded_as_text(audit):
    row = audit.build_row(LAYER_RUN, STATUS_COMPLETED, position_start="100", position_end="107")
    assert (row["position_start"], row["position_end"]) == ("100", "107")


def test_source_detail_is_serialised_to_json_when_it_is_not_already_text(audit):
    """The column is a JSON STRING, not a MAP: a new source type must never force an ALTER
    TABLE on the one table every source shares."""
    assert (
        audit.build_row(LAYER_RUN, STATUS_COMPLETED, source_detail={"schema_ids": [5513]})["source_detail"]
        == '{"schema_ids": [5513]}'
    )
    assert audit.build_row(LAYER_RUN, STATUS_COMPLETED, source_detail='{"a": 1}')["source_detail"] == '{"a": 1}'
    assert audit.build_row(LAYER_RUN, STATUS_COMPLETED)["source_detail"] is None


def test_a_failed_row_carries_error_detail_and_truncates_it(audit):
    row = audit.build_row(LAYER_RUN, STATUS_FAILED, error_class="ValueError", error_message="x" * 9000)
    assert row["error_class"] == "ValueError"
    assert len(row["error_message"]) == 4000


def test_a_batch_id_defaults_to_minus_one(audit):
    """A bounded read outside a microbatch has no batch identity, and recording 0 would be
    indistinguishable from the first microbatch of a streaming run."""
    assert audit.build_row(LAYER_RUN, STATUS_COMPLETED)["batch_id"] == -1


# --------------------------------------------------------------------------------------
# Writing - and never raising
# --------------------------------------------------------------------------------------


def test_a_row_is_appended_to_the_audit_table(cfg):
    spark = _AuditSpark()
    AuditWriter(spark, cfg, AUDIT_TABLE, "run-1", "primary").emit("landing", STATUS_COMPLETED, record_count=10)
    assert spark.frame.write.mode_used == "append"
    assert spark.frame.write.format_used == "delta"
    assert spark.frame.write.saved_as == AUDIT_TABLE


def test_the_values_are_positional_in_schema_order(cfg):
    """createDataFrame with an explicit StructType does not reorder dict keys, so the row is
    built as a positional tuple. A missing key would put a value in the wrong column."""
    spark = _AuditSpark()
    AuditWriter(spark, cfg, AUDIT_TABLE, "run-1", "primary").emit("landing", STATUS_COMPLETED, 4, record_count=10)
    values = spark.rows[0]
    assert spark.schema_used is AUDIT_SCHEMA
    assert len(values) == len(AUDIT_SCHEMA.fields)
    assert values[AUDIT_SCHEMA.fieldNames().index("record_count")] == 10
    assert values[AUDIT_SCHEMA.fieldNames().index("audit_id")] == "run-1::4::landing::COMPLETED"


def test_a_failed_audit_write_never_fails_the_run(cfg):
    """THE rule of this module. If this ever raises, a Delta hiccup on the audit table takes
    the ingestion down with it - which is exactly backwards."""
    AuditWriter(_AuditSpark(fail=True), cfg, AUDIT_TABLE, "run-1", "primary").emit("landing", STATUS_COMPLETED)


def test_a_failed_audit_write_is_logged_loudly(cfg, caplog):
    """Swallowed is not the same as hidden - the failure has to reach the driver log."""
    with caplog.at_level("ERROR"):
        AuditWriter(_AuditSpark(fail=True), cfg, AUDIT_TABLE, "run-1", "primary").emit("landing", STATUS_COMPLETED)
    assert "Failed to write audit row" in caplog.text


def test_a_row_built_from_a_broken_detail_still_does_not_raise(cfg):
    """The row is BUILT inside the same guard as the write. A value that cannot be
    serialised must not escape as an exception either."""

    class Unserialisable:
        pass

    AuditWriter(_AuditSpark(), cfg, AUDIT_TABLE, "run-1", "primary").emit(
        "landing", STATUS_COMPLETED, source_detail=Unserialisable()
    )


def test_no_session_says_so_rather_than_failing_silently(cfg, caplog):
    """Only reachable from a caller that supplied no SparkSession - the disabled
    short-circuit. Stated explicitly so a real audit failure is not hidden among
    AttributeErrors."""
    with caplog.at_level("WARNING"):
        AuditWriter(None, cfg, AUDIT_TABLE, "run-1", "primary").emit(LAYER_RUN, "SKIPPED")
    assert "audit row not written" in caplog.text


def test_ensure_creates_the_table_partitioned_by_audit_date(cfg):
    spark = FakeSpark()
    audit_module.ensure_audit_table(spark, cfg, AUDIT_TABLE)
    statement = spark.sql_statements[0]
    assert f"CREATE TABLE IF NOT EXISTS {AUDIT_TABLE}" in statement
    assert "PARTITIONED BY (audit_date)" in statement
