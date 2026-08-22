"""Parse landing bytes into curated rows, and write them.

This module owns one coherent responsibility: bytes -> parsed rows -> curated table
(with failures split off to quarantine). Splitting the parse from its write would produce
two modules that are only ever called together, in order, by one caller.

SHAPE OF A CURATED ROW
----------------------
    Kafka columns  +  CloudEvent columns  +  lineage columns  +  payload STRUCT

The payload stays NESTED in a single `payload` column rather than being flattened into
top-level columns. Three consequences, all deliberate:

  * Every curated table has the same recognisable outer shape regardless of topic, so an
    operator moving between topics does not relearn the columns each time.
  * No collision is possible between business fields and framework columns, which is why
    the lineage columns here are plainly named (`topic`, `ingest_ts`) rather than
    underscore-prefixed.
  * Nested structures survive as structs, arrays and maps - queryable with
    `payload.patient.id`, and readable via `SELECT to_json(payload)`.

Rows are 1:1 with Kafka records. Arrays are NOT exploded: doing so would break the
(topic, kafka_partition, kafka_offset) merge key that makes replay idempotent. Fan-out
belongs downstream in dm.

THE CENTRAL IDEA
----------------
Every record carries its own writer schema id in the wire header. A microbatch may legally
contain several - a producer rolling out a new schema version does not switch all
partitions at once, and FORWARD_TRANSITIVE compatibility explicitly permits the mix. So
the batch is *grouped by writer_schema_id*, each group is decoded with its own writer
schema, and the groups are unioned back together.

The number of groups is bounded by the number of schema versions a topic has ever had
(typically 1-3 per batch), so this is a handful of narrow scans, not a per-record
operation. All registry I/O happens on the driver; only literal schema JSON strings cross
to the executors, which keeps the decode fully vectorised.

READER SCHEMA - AND WHY IT IS MANDATORY HERE
--------------------------------------------
`from_avro(payload, jsonFormatSchema, options)` maps onto Avro's two-schema resolution:
  jsonFormatSchema   -> the WRITER (actual) schema: how the bytes were encoded
  options.avroSchema -> the READER (expected) schema: the shape you want out

The reader schema fixes the type of the `payload` struct. Because curated stores payload
as ONE struct column, two writer versions decoded without a common reader schema would
produce two incompatible struct types that cannot be unioned into one table. That is why
`reader_schema_mode: writer` does not exist. Avro resolution fills reader-only fields from
their defaults and drops writer-only fields, so a mixed-version batch lands cleanly.

Because that positional/option mapping is easy to get backwards and has moved between
Spark versions, `assert_from_avro_semantics()` proves it at runtime, once per run, with a
5-byte synthetic record.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import ArrayType, MapType, StructField, StructType
from pyspark.sql.window import Window

from . import tables
from .config import ON_ERROR_QUARANTINE, READER_PINNED, TopicConfig
from .landing_writer import RECORD_KEYS
from .schema_resolver import (
    SchemaRegistryClient,
    SchemaResolutionError,
    avro_payload_col,
    distinct_schema_ids,
)

LOG = logging.getLogger(__name__)

PAYLOAD_COL = "payload"
_DECODED = "_decoded_tmp"

REASON_MALFORMED = "malformed_wire_format"
REASON_SCHEMA_LOOKUP = "schema_resolution_failed"
REASON_DECODE = "avro_decode_failed"

# Columns copied straight through from the landing projection into curated.
#
# Kept as two named groups rather than one tuple that gets sliced positionally: the
# quarantine projection interleaves other columns between them, and a slice index would
# silently mis-split the moment anyone added a Kafka column.
_KAFKA_PASSTHROUGH = (
    "topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_timestamp_type",
    "kafka_key", "kafka_key_string", "kafka_headers",
)
_CE_PASSTHROUGH = (
    "ce_id", "ce_source", "ce_type", "ce_subject", "ce_time", "ce_specversion",
    "ce_dataschema", "ce_datacontenttype",
)
_PASSTHROUGH = _KAFKA_PASSTHROUGH + _CE_PASSTHROUGH

_SEMANTICS_CHECKED = False


def event_date_col():
    """Curated's partition key: the date the EVENT happened, not the date we ingested it.

    Prefers the CloudEvents time, falling back to the Kafka record timestamp when the
    producer does not emit CloudEvents or the value is unusable.

    The regex guard is load-bearing. ce_time is stored verbatim as a STRING, so it may be
    NULL or malformed; to_date() on a malformed string throws under ANSI mode and would
    fail the whole batch. CASE WHEN evaluates its branches per row, so to_date only ever
    sees a value that already starts with YYYY-MM-DD. RFC3339 guarantees that prefix.
    """
    return F.coalesce(
        F.when(
            F.col("ce_time").rlike(r"^\d{4}-\d{2}-\d{2}"),
            F.to_date(F.substring(F.col("ce_time"), 1, 10), "yyyy-MM-dd"),
        ),
        F.to_date(F.col("kafka_timestamp")),
    )


@dataclass
class ParseResult:
    """What the parser produces for one microbatch.

    Both DataFrames may be None: an empty batch produces neither, and a batch with no
    failures produces no quarantine frame. Callers must handle None rather than assuming
    an empty DataFrame.
    """

    curated_df: Optional[DataFrame] = None
    quarantine_df: Optional[DataFrame] = None
    writer_schema_ids: List[int] = field(default_factory=list)
    reader_schema_id: Optional[int] = None
    unresolvable_schema_ids: Dict[int, str] = field(default_factory=dict)


# --------------------------------------------------------------------------------------
# Runtime self-check of from_avro's writer/reader argument mapping
# --------------------------------------------------------------------------------------

_SELFCHECK_WRITER = json.dumps({
    "type": "record", "name": "FromAvroSelfCheck", "namespace": "kafka_ingest.selfcheck",
    "fields": [{"name": "a", "type": "int"}],
})
_SELFCHECK_READER = json.dumps({
    "type": "record", "name": "FromAvroSelfCheck", "namespace": "kafka_ingest.selfcheck",
    "fields": [
        {"name": "a", "type": "int"},
        {"name": "b", "type": "string", "default": "resolved-by-default"},
    ],
})
# Avro binary for {"a": 2}: int is zigzag varint, zigzag(2) = 4 -> single byte 0x04.
_SELFCHECK_PAYLOAD = bytes([4])


def assert_from_avro_semantics(spark: SparkSession) -> None:
    """Prove that jsonFormatSchema is the writer and options.avroSchema is the reader.

    Decodes one synthetic record written with a 1-field schema, read with a 2-field schema
    whose extra field has a default. Correct schema resolution yields a=2 AND b filled from
    the default. If the arguments were interpreted the other way round the decode would
    either fail or return the wrong column set - either way, this raises before a single
    production byte is touched.
    """
    global _SEMANTICS_CHECKED
    if _SEMANTICS_CHECKED:
        return
    probe = spark.createDataFrame([(_SELFCHECK_PAYLOAD,)], "payload BINARY")
    try:
        decoded = probe.select(
            from_avro(F.col("payload"), _SELFCHECK_WRITER, {"avroSchema": _SELFCHECK_READER}).alias("d")
        ).select("d.a", "d.b").collect()
    except Exception as exc:
        raise RuntimeError(
            "from_avro reader/writer schema self-check failed to execute. This runtime does "
            "not support supplying a reader schema via the 'avroSchema' option (DBR 13.3 LTS / "
            f"Spark 3.4+ required). Underlying error: {exc}"
        ) from exc

    if not decoded or decoded[0]["a"] != 2 or decoded[0]["b"] != "resolved-by-default":
        raise RuntimeError(
            "from_avro reader/writer schema self-check returned unexpected results "
            f"({decoded!r}). This runtime maps the positional schema argument differently "
            "than this framework expects; decoding production data would be unsafe."
        )
    _SEMANTICS_CHECKED = True
    LOG.info("from_avro writer/reader schema semantics verified on this runtime.")


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


def resolve_reader_schema(cfg: TopicConfig, client: SchemaRegistryClient) -> tuple:
    """Return (reader_schema_id, reader_schema_json). Never None - see module docstring.

    Resolved once per run, never per batch: a registration landing mid-run would otherwise
    change the payload struct's shape partway through a sequence of microbatches.
    """
    if cfg.reader_schema_mode == READER_PINNED:
        return cfg.reader_schema_id, client.get_schema_by_id(cfg.reader_schema_id)
    return client.get_latest(cfg.subject)


def parse_batch(
    spark: SparkSession,
    landing_df: DataFrame,
    cfg: TopicConfig,
    client: SchemaRegistryClient,
    batch_id: int,
    run_id: str,
    reader_schema: Optional[tuple] = None,
) -> ParseResult:
    """Parse a projected-landing DataFrame into curated rows plus quarantine rows.

    `landing_df` is the same in-memory microbatch already written to landing - it is not
    re-read from Kafka or from the landing table.
    """
    reader_id, reader_json = reader_schema or resolve_reader_schema(cfg, client)
    assert_from_avro_semantics(spark)

    quarantine_mode = cfg.on_deser_error == ON_ERROR_QUARANTINE
    decode_options = {
        "mode": "PERMISSIVE" if quarantine_mode else "FAILFAST",
        "avroSchema": reader_json,
    }

    result = ParseResult(reader_schema_id=reader_id)
    schema_ids = distinct_schema_ids(landing_df)
    if not schema_ids:
        # Empty microbatch. AvailableNow emits one at the tail of every run.
        return result

    curated_frames: List[DataFrame] = []
    quarantine_frames: List[DataFrame] = []

    # --- records with no usable wire-format header -----------------------------------
    if None in schema_ids:
        malformed = landing_df.where(F.col("writer_schema_id").isNull())
        detail = "value was NULL, shorter than 5 bytes, or did not start with the 0x00 magic byte"
        if not quarantine_mode:
            # FAILFAST: surface the offsets so the operator can look at the exact records.
            sample = malformed.select("topic", "kafka_partition", "kafka_offset").limit(5).collect()
            raise SchemaResolutionError(
                f"topic '{cfg.topic}' batch {batch_id}: records without a valid Confluent "
                f"wire-format header ({detail}). First offsets: "
                f"{[(r['topic'], r['kafka_partition'], r['kafka_offset']) for r in sample]}. "
                "Set on_deser_error='quarantine' for this topic if these are expected."
            )
        quarantine_frames.append(_to_quarantine(malformed, cfg, batch_id, run_id, REASON_MALFORMED, detail))
        schema_ids = [sid for sid in schema_ids if sid is not None]

    # --- one narrow pass per distinct writer schema id --------------------------------
    for schema_id in sorted(schema_ids):
        group = landing_df.where(F.col("writer_schema_id") == F.lit(schema_id))
        try:
            writer_json = client.get_schema_by_id(schema_id)
        except SchemaResolutionError as exc:
            if not quarantine_mode:
                raise
            # The whole group is undecodable - the registry cannot tell us how these bytes
            # were written. Raw bytes go to quarantine and can be recovered by a curated
            # replay once the schema is registered.
            result.unresolvable_schema_ids[schema_id] = str(exc)
            quarantine_frames.append(
                _to_quarantine(group, cfg, batch_id, run_id, REASON_SCHEMA_LOOKUP, str(exc)[:2000])
            )
            continue

        result.writer_schema_ids.append(schema_id)
        decoded = group.withColumn(
            _DECODED, from_avro(avro_payload_col("value"), writer_json, decode_options)
        )

        if quarantine_mode:
            # PERMISSIVE turns an unparseable record into a NULL struct. Split on that.
            failed = decoded.where(F.col(_DECODED).isNull())
            quarantine_frames.append(
                _to_quarantine(failed, cfg, batch_id, run_id, REASON_DECODE,
                               f"payload did not parse against writer schema id {schema_id}")
            )
            decoded = decoded.where(F.col(_DECODED).isNotNull())

        curated_frames.append(_to_curated(decoded, cfg, batch_id, run_id, reader_id))

    if curated_frames:
        curated_df = curated_frames[0]
        for frame in curated_frames[1:]:
            # Every frame shares one reader schema, so the payload struct type is identical
            # across writer versions and this is a plain union with no column reconciliation.
            curated_df = curated_df.unionByName(frame)
        result.curated_df = _apply_dedup(curated_df, cfg)

    if quarantine_frames:
        quarantine_df = quarantine_frames[0]
        for frame in quarantine_frames[1:]:
            quarantine_df = quarantine_df.unionByName(frame)
        result.quarantine_df = quarantine_df

    return result


def _to_curated(decoded: DataFrame, cfg: TopicConfig, batch_id: int, run_id: str,
                reader_id: Optional[int]) -> DataFrame:
    """Kafka + CloudEvent + lineage columns, then the nested payload last.

    Payload goes last so that `SELECT *` shows the identifying columns before the blob.
    Order matches tables.CURATED_FIXED_COLUMNS + ['payload']; a test asserts it.
    """
    return decoded.select(
        *[F.col(c) for c in _PASSTHROUGH],
        event_date_col().alias("event_date"),
        F.col("writer_schema_id"),
        F.lit(reader_id).cast("int").alias("reader_schema_id"),
        F.col("ingest_ts"),
        F.col("ingest_date"),
        F.lit(cfg.ingested_via).alias("ingested_via"),
        F.lit(cfg.run.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
        F.lit(run_id).alias("run_id"),
        F.col(_DECODED).alias(PAYLOAD_COL),
    )


def _apply_dedup(curated_df: DataFrame, cfg: TopicConfig) -> DataFrame:
    """Collapse duplicates on the configured business key, WITHIN this batch only.

    Keys may be nested paths into the payload, e.g. `payload.claim_id` - that is the normal
    case now that the payload is a struct.

    Cross-batch deduplication is not attempted here: on the primary stream Kafka offsets
    are already unique, and on a replay the MERGE key handles overlap. Doing it here as
    well would need streaming state and would make each batch depend on the last, which is
    exactly what makes a replay non-reproducible.
    """
    if not cfg.curated_dedup_keys:
        return curated_df
    for key in list(cfg.curated_dedup_keys) + [cfg.curated_dedup_order_by]:
        try:
            curated_df.selectExpr(key)
        except Exception as exc:
            raise ValueError(
                f"topic '{cfg.topic_key}': dedup expression '{key}' does not resolve against the "
                f"curated row. Business keys live inside the payload struct, so they are normally "
                f"written as 'payload.<field>'. Available top-level columns: "
                f"{sorted(curated_df.columns)}. Underlying error: {exc}"
            ) from exc

    window = Window.partitionBy(*[F.expr(k) for k in cfg.curated_dedup_keys]).orderBy(
        F.expr(cfg.curated_dedup_order_by).desc(), F.col("kafka_offset").desc()
    )
    return (
        curated_df.withColumn("_dedup_rank", F.row_number().over(window))
        .where(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def _to_quarantine(df: DataFrame, cfg: TopicConfig, batch_id: int, run_id: str,
                   reason: str, detail: str) -> DataFrame:
    """Project failing records into the quarantine schema, raw bytes retained.

    Keeping `value` means a quarantined record is fully recoverable: register the missing
    schema, then run the curated replay entrypoint over the landing rows at the same
    offsets. Quarantine is a diagnostic index into landing, not a separate copy of truth.
    """
    return df.select(
        *[F.col(c) for c in _KAFKA_PASSTHROUGH],
        F.col("value"),
        F.col("writer_schema_id"),
        F.lit(reason).alias("quarantine_reason"),
        F.lit(detail[:2000]).alias("quarantine_detail"),
        F.current_timestamp().alias("quarantined_ts"),
        *[F.col(c) for c in _CE_PASSTHROUGH],
        F.col("ingest_ts"),
        F.col("ingest_date"),
        F.lit(cfg.ingested_via).alias("ingested_via"),
        F.lit(cfg.run.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
        F.lit(run_id).alias("run_id"),
    )


# --------------------------------------------------------------------------------------
# Curated table schema, computed before any data is read
# --------------------------------------------------------------------------------------


def _as_all_nullable(data_type):
    """Recursively mark every field nullable.

    Avro records declare non-optional fields, and from_avro faithfully reports them as
    NOT NULL. Baking that into the CREATE TABLE would be a bad trade: the payload comes from
    an external producer, and a single record that violates it fails the whole batch on
    write rather than being routed to quarantine like every other bad record.

    The Avro decode still enforces the contract at parse time - this only stops the TABLE
    carrying a constraint that turns a data-quality problem into an outage.
    """
    if isinstance(data_type, StructType):
        return StructType([
            StructField(f.name, _as_all_nullable(f.dataType), True, f.metadata)
            for f in data_type.fields
        ])
    if isinstance(data_type, ArrayType):
        return ArrayType(_as_all_nullable(data_type.elementType), True)
    if isinstance(data_type, MapType):
        return MapType(data_type.keyType, _as_all_nullable(data_type.valueType), True)
    return data_type


def curated_schema(spark: SparkSession, cfg: TopicConfig, reader_schema: tuple) -> StructType:
    """The exact schema curated rows will have, derived with ZERO rows read.

    Runs the real projection - the same _to_curated() the writer uses - over an empty landing
    frame. That is the point: the created table matches what the writer produces by
    construction, rather than by two definitions being kept in step by hand.

    Everything happens on the driver against an empty DataFrame, so this costs one Catalyst
    analysis and touches no data, no Kafka and no executors.
    """
    reader_id, reader_json = reader_schema
    empty_landing = spark.createDataFrame([], StructType.fromDDL(tables.LANDING_DDL_COLUMNS))
    decoded = empty_landing.withColumn(
        _DECODED, from_avro(avro_payload_col("value"), reader_json, {"avroSchema": reader_json})
    )
    return _as_all_nullable(_to_curated(decoded, cfg, 0, "", reader_id).schema)


# --------------------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------------------


_AUTO_MERGE_CONF = "spark.databricks.delta.schema.autoMerge.enabled"


def _merge_curated(spark: SparkSession, curated_df: DataFrame, cfg: TopicConfig) -> None:
    """Upsert a replayed batch into curated, allowing the schema to widen.

    WHY SCHEMA EVOLUTION IS NEEDED HERE AND NOWHERE ELSE
    ----------------------------------------------------
    The append path below sets mergeSchema=true, so an additive reader-schema change (a new
    optional Avro field) widens curated automatically on the primary stream. MERGE does not
    honour that option, and a curated replay is the very thing an operator runs *after* a
    schema change - re-parsing older rows with the corrected schema. Without evolution here,
    that replay fails on the schema it was run to apply.

    Landing deliberately does NOT get this: its schema is fixed DDL and never evolves.

    TWO MECHANISMS, BECAUSE THE RUNTIME FLOOR IS NOT PINNED
    -------------------------------------------------------
    withSchemaEvolution() is the documented, operation-scoped way, but it needs DBR 15.4 LTS
    or later (and serverless environment version 2+; on version 1 it raises AttributeError).
    Older runtimes have only the session flag, which Databricks describes as legacy. So:
    use the builder method when this runtime has it, otherwise set the flag for the duration
    of this one operation and put it back. Either way the blast radius is one merge.
    """
    from delta.tables import DeltaTable

    condition = " AND ".join(f"t.{k} = s.{k}" for k in RECORD_KEYS)
    merge = (
        DeltaTable.forName(spark, cfg.curated_table)
        .alias("t")
        .merge(curated_df.alias("s"), condition)
    )

    if hasattr(merge, "withSchemaEvolution"):
        merge.withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        return

    LOG.info("This runtime has no withSchemaEvolution(); scoping %s to this merge instead.",
             _AUTO_MERGE_CONF)
    previous = spark.conf.get(_AUTO_MERGE_CONF, None)
    spark.conf.set(_AUTO_MERGE_CONF, "true")
    try:
        merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    finally:
        # Restore rather than unset-always: another job on a shared session may have set it
        # deliberately, and silently clearing it would be a surprising side effect.
        if previous is None:
            spark.conf.unset(_AUTO_MERGE_CONF)
        else:
            spark.conf.set(_AUTO_MERGE_CONF, previous)


def write_curated(spark: SparkSession, curated_df: DataFrame, cfg: TopicConfig,
                  batch_id: int, txn_app_id: Optional[str]) -> None:
    """Append (primary) or upsert (replay) into the topic's curated table.

    Replay uses whenMatchedUpdateAll, unlike landing: the entire point of a curated replay
    is to REPLACE a bad parse with a good one. Landing deliberately does the opposite,
    preserving the original arrival record.
    """
    if cfg.run.is_replay and spark.catalog.tableExists(cfg.curated_table):
        _merge_curated(spark, curated_df, cfg)
        return

    writer = curated_df.write.format("delta").mode("append")
    # mergeSchema lets an additive Avro change (a new optional field in the reader schema)
    # land without a manual ALTER TABLE. Additive only - Delta still rejects type changes.
    writer = writer.option("mergeSchema", "true")
    # The table is normally created up front by pipeline.ensure_curated(), so this is not
    # what establishes the partitioning any more. Kept because it is free, and because it
    # keeps the layout explicit at the call site - and it still covers the one path that
    # reaches a write without going through a run shape (a direct call in a notebook).
    writer = writer.partitionBy(*cfg.curated_partition_by)
    if txn_app_id is not None and batch_id >= 0 and not cfg.run.is_replay:
        writer = writer.option("txnAppId", txn_app_id).option("txnVersion", batch_id)
    writer.saveAsTable(cfg.curated_table)


def write_quarantine(quarantine_df: DataFrame, cfg: TopicConfig, batch_id: int,
                     txn_app_id: Optional[str]) -> None:
    """Append quarantined records. Always append - a record can legitimately be quarantined
    twice (once by primary, once by a replay that still lacked the schema), and both
    attempts are evidence worth keeping."""
    writer = quarantine_df.write.format("delta").mode("append")
    if txn_app_id is not None and batch_id >= 0 and not cfg.run.is_replay:
        # A distinct appId suffix: Delta tracks (appId, version) per table, and reusing the
        # landing appId here would be correct but harder to read in DESCRIBE HISTORY.
        writer = writer.option("txnAppId", f"{txn_app_id}::quarantine").option("txnVersion", batch_id)
    writer.saveAsTable(cfg.quarantine_table)
