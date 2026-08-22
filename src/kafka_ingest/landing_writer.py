"""Landing projection and write.

Landing is ONE Delta table per topic, partitioned by ingest_date and named from the Kafka
topic (see config.table_name_for()). It stores the Kafka value byte-for-byte - including the
5-byte Confluent header - so any future correction to parsing or decoding logic can be
replayed against it without going back to the broker, and therefore without depending on
Kafka retention.

Two things are lifted out of the record here, and neither touches the payload:
  * the writer schema id, parsed from the wire header (framing metadata, not business data)
  * the CloudEvents context attributes, read from Kafka headers

Both are metadata *about* the message. The payload itself is not interpreted at all in
this layer.

Write semantics differ by run type, deliberately:

  primary   append with Delta idempotent-write markers (txnAppId + txnVersion). A retried
            microbatch after a mid-batch failure is dropped by Delta itself, which is what
            makes the chained landing -> curated foreachBatch exactly-once.

  replay    MERGE keyed on (topic, kafka_partition, kafka_offset), insert-if-absent only.
            A replay overlaps existing data by definition, so append would duplicate.
            Matched rows are left untouched: the original landing row records what
            arrived on the primary stream, and rewriting its provenance columns with
            replay metadata would destroy that record. See docs/DESIGN.md.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType

from .config import TopicConfig
from .schema_resolver import add_wire_format_columns

LOG = logging.getLogger(__name__)

# Unique identity of a Kafka record. Used as the MERGE key by both landing and curated,
# which is what lets a replay be idempotent without a surrogate key or a hash.
RECORD_KEYS = ("topic", "kafka_partition", "kafka_offset")

_HEADERS_TYPE = ArrayType(
    StructType([StructField("key", StringType()), StructField("value", BinaryType())])
)

# CloudEvents v1.0 context attributes, Kafka binary content mode: each attribute travels
# as a `ce_<name>` header. Header names are matched case-insensitively because brokers and
# client libraries disagree about casing.
CE_ATTRIBUTES = ("ce_id", "ce_source", "ce_type", "ce_subject",
                 "ce_time", "ce_specversion", "ce_dataschema")


def _first_header(name: str, headers_col: str = "headers") -> Column:
    """First value of a header, or NULL when absent.

    Uses aggregate(transform(filter(...))) rather than an array index on purpose: Kafka
    permits duplicate and missing headers, and indexing an empty array throws under ANSI
    mode. This form returns NULL for "no such header" on every Spark version and under
    either ANSI setting.
    """
    return F.expr(
        f"aggregate("
        f"  transform(filter({headers_col}, h -> lower(h.key) = '{name}'),"
        f"            h -> decode(h.value, 'UTF-8')),"
        f"  CAST(NULL AS STRING),"
        f"  (acc, x) -> coalesce(acc, x))"
    )


def add_cloudevent_columns(df: DataFrame, headers_col: str = "headers") -> DataFrame:
    """Promote CloudEvents context attributes from Kafka headers to typed columns.

    Attributes beyond the standard eight remain available in the retained headers array.
    A topic that does not use CloudEvents simply gets NULLs - this is not an error, and a
    NULL ce_id is the signal that a producer is not emitting CloudEvents.
    """
    for attribute in CE_ATTRIBUTES:
        df = df.withColumn(attribute, _first_header(attribute, headers_col))
    # The CloudEvents Kafka binding maps `datacontenttype` onto the standard content-type
    # header. Some producers send ce_datacontenttype anyway, so accept both.
    return df.withColumn(
        "ce_datacontenttype",
        F.coalesce(_first_header("content-type", headers_col),
                   _first_header("ce_datacontenttype", headers_col)),
    )


def project_landing(raw_df: DataFrame, cfg: TopicConfig, batch_id: int, run_id: str) -> DataFrame:
    """Shape the raw Kafka source DataFrame into the landing table schema.

    Column order matches tables.LANDING_DDL_COLUMNS exactly; a test asserts this, because
    a drift would surface as a confusing Delta schema error on the first append.
    """
    df = add_wire_format_columns(raw_df, value_col="value")

    if "headers" not in df.columns:
        # includeHeaders=false for this topic. Keep the column so landing has one stable
        # schema across topics and across a later flip of that setting; CloudEvent columns
        # then resolve to NULL.
        df = df.withColumn("headers", F.lit(None).cast(_HEADERS_TYPE))

    df = add_cloudevent_columns(df)

    return df.select(
        F.col("topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("bigint").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").cast("int").alias("kafka_timestamp_type"),
        F.col("key").alias("kafka_key"),
        # Best effort only: decode() substitutes U+FFFD for invalid byte sequences rather
        # than failing, so a binary key yields mojibake here, never a dropped row. The
        # authoritative key is always the BINARY column next to it.
        F.when(F.col("key").isNotNull(), F.decode(F.col("key"), "UTF-8")).alias("kafka_key_string"),
        F.col("headers").cast(_HEADERS_TYPE).alias("kafka_headers"),
        F.col("value"),
        F.col("writer_schema_id"),
        F.col("wire_format_valid"),
        F.col("payload_bytes"),
        F.col("ce_id"), F.col("ce_source"), F.col("ce_type"), F.col("ce_subject"),
        F.col("ce_time"), F.col("ce_specversion"), F.col("ce_dataschema"),
        F.col("ce_datacontenttype"),
        F.current_timestamp().alias("ingest_ts"),
        F.current_date().alias("ingest_date"),
        F.lit(cfg.ingested_via).alias("ingested_via"),
        F.lit(cfg.run.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
        F.lit(run_id).alias("run_id"),
    )


def write_landing(
    spark: SparkSession,
    landing_df: DataFrame,
    cfg: TopicConfig,
    batch_id: int,
    txn_app_id: Optional[str],
) -> None:
    """Persist the projected batch. Chooses append-idempotent vs MERGE by run type."""
    if cfg.run.is_replay:
        _merge_landing(spark, landing_df, cfg)
    else:
        _append_landing(landing_df, cfg, batch_id, txn_app_id)


def _append_landing(df: DataFrame, cfg: TopicConfig, batch_id: int, txn_app_id: Optional[str]) -> None:
    writer = df.write.format("delta").mode("append")
    if txn_app_id is not None and batch_id >= 0:
        # Delta records (txnAppId, txnVersion) in the table's transaction log and skips a
        # write whose version it has already seen. This is what makes a foreachBatch retry
        # safe: landing committed, curated failed, batch retried -> the landing append is
        # a no-op, curated is written, and the pair ends consistent.
        writer = writer.option("txnAppId", txn_app_id).option("txnVersion", batch_id)
    writer.saveAsTable(cfg.landing_table)


def _merge_landing(spark: SparkSession, df: DataFrame, cfg: TopicConfig) -> None:
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(cfg.landing_table):
        # A replay into a landing table that does not exist yet is unusual but legal
        # (e.g. rebuilding a dropped table from the broker). Nothing to merge against.
        df.write.format("delta").mode("append").saveAsTable(cfg.landing_table)
        return

    # Just the record key. An earlier version pinned `t.topic = '<literal>'` to force
    # partition elimination, which mattered when landing was one shared table partitioned by
    # topic. It is now one table per topic partitioned by ingest_date, so that literal
    # matches every row and prunes nothing - it was carrying its own weight and no more.
    # Pruning now comes from ingest_date, which Delta infers from the source frame.
    condition = " AND ".join(f"t.{k} = s.{k}" for k in RECORD_KEYS)
    # No source-side dedup: (topic, kafka_partition, kafka_offset) is unique by
    # construction within a single Kafka read, so MERGE cannot hit the
    # multiple-source-rows-per-target error.
    (
        DeltaTable.forName(spark, cfg.landing_table)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    LOG.info("Landing MERGE complete for topic '%s' rerun_id=%s", cfg.topic, cfg.run.rerun_id)
