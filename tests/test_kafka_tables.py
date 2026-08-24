"""sources/kafka/tables.py - the three target tables' columns, and who creates them.

Three definitions of each layer table exist and must agree: the DDL constant that creates
it, the projection that writes into it, and the CREATE TABLE in sql/ that provisions it
ahead of the first run. This file holds two of the three pairs together; the third - the
projection - needs Spark and lives in tests/test_kafka_curated.py.

A mismatch surfaces as a Delta schema error on the FIRST APPEND, on a cluster, which is
both the latest and the least informative moment to find out.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import ddl_column_names, make_kafka_cfg, sql_table_columns
from kafka_ingest.sources.kafka import tables as kafka_tables
from kafka_ingest.sources.kafka.tables import (
    CURATED_FIXED_COLUMNS,
    LANDING_DDL_COLUMNS,
    QUARANTINE_DDL_COLUMNS,
)

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "02_layer_tables.sql"


@pytest.fixture(scope="module")
def layer_sql() -> str:
    assert SQL_PATH.is_file(), f"{SQL_PATH} is missing"
    return SQL_PATH.read_text(encoding="utf-8")


# --------------------------------------------------------------------------------------
# What the tables are made of
# --------------------------------------------------------------------------------------


def test_landing_and_curated_share_their_kafka_and_cloudevent_columns():
    """A curated row must always be traceable back to its landing row on
    (topic, kafka_partition, kafka_offset) - which is also the MERGE key that makes a replay
    idempotent - and an operator moving between layers should not have to relearn names."""
    landing = set(ddl_column_names(LANDING_DDL_COLUMNS))
    curated = set(ddl_column_names(CURATED_FIXED_COLUMNS))
    shared = {
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "kafka_headers",
        "ce_id",
        "ce_source",
        "ce_type",
        "ce_time",
        "run_id",
    }
    assert shared <= landing and shared <= curated


def test_quarantine_retains_the_raw_bytes_and_says_why_the_record_is_there():
    """Quarantine is a diagnostic index into landing, not a separate copy of truth: the raw
    bytes are what let a curated replay recover the record once the cause is fixed."""
    columns = ddl_column_names(QUARANTINE_DDL_COLUMNS)
    assert "value" in columns
    assert "quarantine_reason" in columns and "quarantine_detail" in columns


def test_the_partition_columns_exist_in_the_tables_they_partition(config_root):
    """A partition column that is not in the schema fails at CREATE TABLE, on a cluster."""
    cfg = make_kafka_cfg(config_root)
    landing_columns = ddl_column_names(LANDING_DDL_COLUMNS)
    for column in cfg.landing_partition_by:
        assert column in landing_columns, f"landing partition column '{column}' is not a landing column"
    curated_columns = ddl_column_names(CURATED_FIXED_COLUMNS)
    for column in cfg.curated_partition_by:
        assert column in curated_columns, f"curated partition column '{column}' is not a curated column"


def test_the_malformed_reason_vocabulary_is_the_one_the_column_documents():
    """The three reasons are named in the landing column's own COMMENT, because that is
    where a support engineer reads them - and a fourth reason added in code with no comment
    change would be a value nobody can interpret."""
    from kafka_ingest.sources.kafka import wire

    for reason in wire.MALFORMED_REASONS:
        assert reason in LANDING_DDL_COLUMNS, f"{reason} is emitted but not documented on the column"
        assert reason in wire.MALFORMED_DETAIL, f"{reason} has no explanatory detail"


# --------------------------------------------------------------------------------------
# Creation goes through the framework, so every table gets the same treatment
# --------------------------------------------------------------------------------------


def test_every_table_this_source_creates_goes_through_the_framework(config_root, kafka_ctx):
    """One call site for TBLPROPERTIES, name validation and the one-layout-clause rule.

    A table created implicitly by its first write gets no TBLPROPERTIES at all, which is how
    curated - the table people actually query - once ended up the only one without
    auto-compaction.
    """
    cfg = make_kafka_cfg(config_root)
    kafka_tables.ensure_landing(kafka_ctx, cfg)
    kafka_tables.ensure_quarantine(kafka_ctx, cfg)

    created = {entry["name"]: entry for entry in kafka_ctx.tables.created}
    assert created[cfg.landing_table]["partition_by"] == ["ingest_date"]
    assert created[cfg.quarantine_table]["partition_by"] == ["ingest_date"]


def test_curated_is_created_from_the_resolved_reader_schema_not_from_the_first_batch(config_root, kafka_ctx):
    """Curated's payload struct follows the Avro reader schema, so it cannot be a static
    constant - but it IS known on the driver before any row is read, and creating it
    explicitly is what stops Spark inventing a shape from whatever arrived first."""

    class _Schema:
        def toDDL(self):  # noqa: N802 - mirrors the Spark API
            return "topic STRING, payload STRUCT<event_id: BIGINT>"

    cfg = make_kafka_cfg(config_root)
    kafka_tables.ensure_curated(kafka_ctx, cfg, _Schema())
    created = {entry["name"]: entry for entry in kafka_ctx.tables.created}
    assert created[cfg.curated_table]["partition_by"] == ["event_date"]
    assert "payload STRUCT" in created[cfg.curated_table]["columns"]


# --------------------------------------------------------------------------------------
# sql/02_layer_tables.sql vs the Python DDL
#
# That file's own header says "KEEP THIS FILE AND THE PYTHON DDL IN STEP", and explains that
# a mismatch surfaces as a Delta schema error on the first append. Everything else in this
# repository that says "keep these in step" has a test behind it; this is that test.
#
# Column NAMES in ORDER only. Types and comments drift harmlessly between a provisioning
# script and the code that would otherwise create the table; a column that exists in one and
# not the other - or in a different position - does not.
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table_suffix, ddl_constant, label",
    [
        ("landing.{topic_table}", LANDING_DDL_COLUMNS, "landing"),
        ("_quarantine", QUARANTINE_DDL_COLUMNS, "quarantine"),
    ],
    ids=["landing", "quarantine"],
)
def test_provisioning_sql_matches_the_python_ddl(layer_sql, table_suffix, ddl_constant, label):
    """A column present in one and not the other fails on the first append, on a cluster."""
    assert sql_table_columns(layer_sql, table_suffix) == ddl_column_names(ddl_constant), (
        f"{label}: sql/02_layer_tables.sql and sources/kafka/tables.py disagree on columns or their order"
    )


def test_curated_is_deliberately_absent_from_the_provisioning_sql(layer_sql):
    """A static copy of curated's columns would go stale the first time a schema was
    registered, which is worse than not having one."""
    assert "CREATE TABLE IF NOT EXISTS {catalog}.curated." not in layer_sql


# --------------------------------------------------------------------------------------
# DDL validity - needs a SparkSession to parse, but touches no filesystem
# --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("kafka-ingest-ddl-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no local Spark available ({type(exc).__name__}: {exc})")
    yield session
    session.stop()


@pytest.mark.spark
@pytest.mark.parametrize(
    "name, ddl",
    [
        ("landing", LANDING_DDL_COLUMNS),
        ("curated_fixed", CURATED_FIXED_COLUMNS),
        ("quarantine", QUARANTINE_DDL_COLUMNS),
    ],
)
def test_ddl_parses(spark, name, ddl):
    """Catches a typo in a type or a stray comma without needing a catalog or a warehouse."""
    from pyspark.sql.types import StructType

    assert len(StructType.fromDDL(ddl).fields) == len(ddl_column_names(ddl)), name
