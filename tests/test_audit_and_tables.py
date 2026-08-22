"""Audit row construction and DDL/schema alignment.

Three kinds of drift are cheap to introduce and expensive to discover in production:
  * AuditWriter.build_row() stops emitting a key that AUDIT_SCHEMA declares
  * AUDIT_SCHEMA and the CREATE TABLE DDL diverge
  * a writer's projection stops matching its table DDL
Each is asserted here rather than left to a comment saying "keep these in step".

`ddl_column_names` also serves test_curated_writer.py - it is the shared way to read a
column list out of a DDL string.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="audit/tables import pyspark")

from conftest import FakeSpark
from kafka_ingest.audit import (
    AUDIT_SCHEMA,
    LAYER_CURATED,
    LAYER_LANDING,
    STATUS_COMPLETED,
    STATUS_FAILED,
    AuditWriter,
)
from kafka_ingest.config import resolve_topic_config
from kafka_ingest.tables import (
    AUDIT_DDL_COLUMNS,
    CURATED_FIXED_COLUMNS,
    LANDING_DDL_COLUMNS,
    QUARANTINE_DDL_COLUMNS,
)


def ddl_column_names(ddl: str) -> list:
    """Pull column names out of a DDL column list, ignoring types, comments and nesting."""
    names, depth = [], 0
    for raw_line in ddl.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Skip continuation lines inside a nested type such as ARRAY<STRUCT<...>>.
        if depth == 0:
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+\S", line)
            if match:
                names.append(match.group(1))
        depth += line.count("<") - line.count(">")
    return names


@pytest.fixture
def cfg(config_root):
    return resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")


@pytest.fixture
def audit(cfg):
    # spark is unused by build_row; emit() is exercised in the round-trip test below.
    return AuditWriter(spark=None, cfg=cfg, run_id="run-1")


# --------------------------------------------------------------------------------------
# Audit rows
# --------------------------------------------------------------------------------------


def test_audit_row_supplies_every_declared_field(audit):
    """emit() builds a positional tuple in AUDIT_SCHEMA order - a missing key would be a
    KeyError at write time, i.e. a lost audit row during an incident."""
    row = audit.build_row(LAYER_LANDING, STATUS_COMPLETED, 7)
    assert set(row) == {f.name for f in AUDIT_SCHEMA.fields}


def test_audit_schema_matches_the_ddl():
    assert ddl_column_names(AUDIT_DDL_COLUMNS) == [f.name for f in AUDIT_SCHEMA.fields]


def test_audit_id_is_unique_per_layer_and_status(audit):
    """Six rows per batch share a batch_id, so the id must include layer and status."""
    ids = {
        audit.build_row(layer, status, 3)["audit_id"]
        for layer in (LAYER_LANDING, LAYER_CURATED)
        for status in ("STARTED", "COMPLETED")
    }
    assert len(ids) == 4
    assert "run-1::3::landing::STARTED" in ids


def test_audit_row_carries_replay_provenance(config_root):
    replay_cfg = resolve_topic_config(
        FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
        run_type="kafka_replay",
        overrides={"rerun_id": "INC42", "starting_timestamp": "2026-08-11T09:00:00Z"},
    )
    row = AuditWriter(None, replay_cfg, "run-9").build_row(LAYER_CURATED, STATUS_COMPLETED, 3)
    assert row["run_type"] == "kafka_replay"
    assert row["rerun_id"] == "INC42"
    # The checkpoint path is recorded so triage can tell which lineage produced the batch.
    assert row["checkpoint_path"].endswith("/replay/INC42")


def test_failed_row_carries_error_detail_and_is_truncated(audit):
    row = audit.build_row(LAYER_CURATED, STATUS_FAILED, 2,
                          error_class="ValueError", error_message="x" * 9000)
    assert row["status"] == STATUS_FAILED
    assert row["error_class"] == "ValueError"
    assert len(row["error_message"]) == 4000


def test_writer_schema_ids_survive_as_ints(audit):
    row = audit.build_row(LAYER_CURATED, STATUS_COMPLETED, 1, writer_schema_ids=[101, 202])
    assert row["writer_schema_ids"] == [101, 202]


def test_empty_schema_id_list_is_null_not_an_empty_array(audit):
    """Distinguishes 'no schemas seen' from 'we did not look'."""
    assert audit.build_row(LAYER_LANDING, STATUS_COMPLETED, 1)["writer_schema_ids"] is None


# --------------------------------------------------------------------------------------
# DDL validity - needs a SparkSession to parse, but touches no filesystem
# --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("kafka-ingest-ddl-tests").config("spark.ui.enabled", "false").getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no local Spark available ({type(exc).__name__}: {exc})")
    yield session
    session.stop()


@pytest.mark.spark
@pytest.mark.parametrize("name, ddl", [
    ("landing", LANDING_DDL_COLUMNS),
    ("curated_fixed", CURATED_FIXED_COLUMNS),
    ("quarantine", QUARANTINE_DDL_COLUMNS),
    ("audit", AUDIT_DDL_COLUMNS),
])
def test_ddl_parses(spark, name, ddl):
    """Catches a typo in a type or a stray comma without needing a catalog or a warehouse."""
    from pyspark.sql.types import StructType

    assert len(StructType.fromDDL(ddl).fields) == len(ddl_column_names(ddl)), name


@pytest.mark.spark
def test_partition_columns_exist_in_their_tables(spark, cfg):
    """A partition column that is not in the schema fails at CREATE TABLE, on a cluster."""
    landing_cols = ddl_column_names(LANDING_DDL_COLUMNS)
    for column in cfg.landing_partition_by:
        assert column in landing_cols, f"landing partition column '{column}' is not a landing column"
    curated_cols = ddl_column_names(CURATED_FIXED_COLUMNS)
    for column in cfg.curated_partition_by:
        assert column in curated_cols, f"curated partition column '{column}' is not a curated column"


@pytest.mark.spark
def test_landing_and_curated_share_their_kafka_and_cloudevent_columns(spark):
    """A curated row must always be traceable back to its landing row, and an operator
    moving between layers should not have to relearn the column names."""
    landing = set(ddl_column_names(LANDING_DDL_COLUMNS))
    curated = set(ddl_column_names(CURATED_FIXED_COLUMNS))
    shared = {"topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_key",
              "kafka_headers", "ce_id", "ce_source", "ce_type", "ce_time", "run_id"}
    assert shared <= landing and shared <= curated


@pytest.mark.spark
def test_audit_row_round_trips_through_the_declared_schema(spark, cfg):
    """Proves the positional tuple AuditWriter builds actually satisfies AUDIT_SCHEMA."""
    row = AuditWriter(spark, cfg, "run-1").build_row(
        LAYER_CURATED, STATUS_COMPLETED, 4, record_count=10, writer_schema_ids=[101])
    values = tuple(row[f.name] for f in AUDIT_SCHEMA.fields)
    materialised = spark.createDataFrame([values], schema=AUDIT_SCHEMA).collect()[0]
    assert materialised["audit_id"] == "run-1::4::curated::COMPLETED"
    assert materialised["record_count"] == 10
    assert materialised["writer_schema_ids"] == [101]


# --------------------------------------------------------------------------------------
# sql/02_layer_tables.sql vs tables.py
#
# That file's own header says "KEEP THIS FILE AND tables.py IN STEP", and explains that a
# mismatch surfaces as a Delta schema error on the first append. Everything else in this
# suite that says "keep these in step" has a test behind it; this did not.
#
# Column NAMES in ORDER only. Types and comments drift harmlessly between a provisioning
# script and the code that would otherwise create the table, but a column that exists in one
# and not the other - or in a different position - does not.
# --------------------------------------------------------------------------------------

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "02_layer_tables.sql"


def sql_table_columns(sql: str, table_suffix: str) -> list:
    """Column names, in order, from the CREATE TABLE whose name ends with `table_suffix`."""
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+\S*" + re.escape(table_suffix) + r"\s*\((.*?)\n\)",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(sql)
    assert match, f"no CREATE TABLE ending in '{table_suffix}' found in {SQL_PATH.name}"
    return ddl_column_names(match.group(1))


@pytest.fixture(scope="module")
def layer_sql() -> str:
    assert SQL_PATH.is_file(), f"{SQL_PATH} is missing"
    return SQL_PATH.read_text(encoding="utf-8")


@pytest.mark.parametrize("table_suffix, ddl_constant, label", [
    ("landing.{topic_table}", LANDING_DDL_COLUMNS, "landing"),
    ("audit.stream_audit", AUDIT_DDL_COLUMNS, "audit"),
    ("_quarantine", QUARANTINE_DDL_COLUMNS, "quarantine"),
], ids=["landing", "audit", "quarantine"])
def test_provisioning_sql_matches_the_python_ddl(layer_sql, table_suffix, ddl_constant, label):
    """A column present in one and not the other fails on the first append, on a cluster."""
    assert sql_table_columns(layer_sql, table_suffix) == ddl_column_names(ddl_constant), (
        f"{label}: sql/02_layer_tables.sql and tables.py disagree on columns or their order"
    )
