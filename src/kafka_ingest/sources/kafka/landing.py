"""The landing projection: raw Kafka bytes plus metadata ABOUT them, nothing interpreted.

Landing stores the Kafka value byte-for-byte - INCLUDING the 5-byte Confluent header - so
any future correction to parsing or decoding can be replayed against it without going back
to the broker, and therefore without depending on Kafka retention.

Exactly two things are lifted out of the record here, and neither touches the payload:

  * the writer schema id, parsed from the wire header (framing metadata, not business data)
  * the CloudEvents context attributes, read from Kafka headers

Both are metadata ABOUT the message. The payload itself is not interpreted in this layer at
all - that is curated.py's job, and keeping the two apart is what makes a curated replay
possible years later.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType

from .config import KafkaConfig
from .wire import add_wire_format_columns

# The unique identity of a Kafka record, and the MERGE key for every layer that has one.
# This is what lets a replay be idempotent without a surrogate key or a hash - and it is
# why rows must stay 1:1 with Kafka records and are never exploded.
RECORD_KEYS = ("topic", "kafka_partition", "kafka_offset")

_HEADERS_TYPE = ArrayType(StructType([StructField("key", StringType()), StructField("value", BinaryType())]))

# CloudEvents v1.0 context attributes, Kafka binary content mode: each attribute travels as
# a `ce_<name>` header. Header names are matched case-INSENSITIVELY, because brokers and
# client libraries disagree about casing and a topic is not going to change producer.
CE_ATTRIBUTES = ("ce_id", "ce_source", "ce_type", "ce_subject", "ce_time", "ce_specversion", "ce_dataschema")


def _first_header(name: str, headers_col: str = "headers") -> Column:
    """The first value of a header, or NULL when it is absent.

    aggregate(transform(filter(...))) rather than an array index, on purpose: Kafka permits
    DUPLICATE and MISSING header keys, and indexing an empty array THROWS under ANSI mode.
    This form returns NULL for "no such header" on every Spark version and under either ANSI
    setting, and takes the first of a duplicated key rather than erroring.
    """
    return F.expr(
        f"aggregate("
        f"  transform(filter({headers_col}, h -> lower(h.key) = '{name}'),"
        f"            h -> decode(h.value, 'UTF-8')),"
        f"  CAST(NULL AS STRING),"
        f"  (acc, x) -> coalesce(acc, x))"
    )


def add_cloudevent_columns(df: DataFrame, headers_col: str = "headers") -> DataFrame:
    """Promote the eight CloudEvents context attributes from headers to typed columns.

    Attributes beyond the standard eight stay available in the retained headers array. A
    topic that does not use CloudEvents simply gets NULLs - that is NOT an error, and a
    NULL ce_id is the signal that a producer is not emitting CloudEvents.
    """
    for attribute in CE_ATTRIBUTES:
        df = df.withColumn(attribute, _first_header(attribute, headers_col))
    # The CloudEvents Kafka binding maps `datacontenttype` onto the standard content-type
    # header. Some producers send ce_datacontenttype anyway, so accept both.
    return df.withColumn(
        "ce_datacontenttype",
        F.coalesce(_first_header("content-type", headers_col), _first_header("ce_datacontenttype", headers_col)),
    )


def project(raw_df: DataFrame, cfg: KafkaConfig, txn_version: int, run_id: str) -> DataFrame:
    """Shape the raw Kafka source DataFrame into the landing table's schema.

    Column order matches tables.LANDING_DDL_COLUMNS exactly; a test asserts it, because a
    drift surfaces as a confusing Delta schema error on the first append and nowhere
    earlier.
    """
    df = add_wire_format_columns(raw_df, value_col="value")

    if "headers" not in df.columns:
        # Only reachable from a frame that was not read with includeHeaders - a bounded
        # batch read in a notebook, say. Keep the column so landing has one stable schema;
        # the CloudEvent columns then resolve to NULL rather than failing analysis.
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
        F.col("malformed_reason"),
        F.col("payload_bytes"),
        F.col("ce_id"),
        F.col("ce_source"),
        F.col("ce_type"),
        F.col("ce_subject"),
        F.col("ce_time"),
        F.col("ce_specversion"),
        F.col("ce_dataschema"),
        F.col("ce_datacontenttype"),
        F.current_timestamp().alias("ingest_ts"),
        F.current_date().alias("ingest_date"),
        F.lit(cfg.ingested_via).alias("ingested_via"),
        F.lit(cfg.replay.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(txn_version).cast("bigint").alias("txn_version"),
        F.lit(run_id).alias("run_id"),
    )
