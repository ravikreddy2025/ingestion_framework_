"""Landing bytes -> parsed curated rows, with everything unparseable split off.

SHAPE OF A CURATED ROW
----------------------
    Kafka columns  +  CloudEvent columns  +  lineage columns  +  payload STRUCT

The payload stays NESTED in a single `payload` column rather than being flattened into
top-level columns. Three consequences, all deliberate:

  * every curated table has the same recognisable outer shape regardless of topic, so an
    operator moving between topics does not relearn the columns each time;
  * no collision is possible between business fields and framework columns, which is why
    the lineage columns are plainly named (`topic`, `ingest_ts`) rather than prefixed;
  * nested structures survive as structs, arrays and maps - queryable with
    `payload.patient.id`, readable via `SELECT to_json(payload)`.

ROWS ARE 1:1 WITH KAFKA RECORDS. Arrays are NOT exploded: doing so would break the
(topic, kafka_partition, kafka_offset) merge key that makes a replay idempotent. Fan-out
belongs downstream.

THE CENTRAL IDEA
----------------
Every record carries its OWN writer schema id in the wire header. A microbatch may legally
contain several - a producer rolling out a new version does not switch all partitions at
once, and FORWARD_TRANSITIVE compatibility explicitly permits the mix. So the batch is
GROUPED BY writer_schema_id, each group is decoded with its own writer schema, and the
groups are unioned back together.

The number of groups is bounded by the number of schema versions the topic has ever had
(typically 1-3 per batch), so this is a handful of narrow scans, not a per-record
operation. All registry I/O happens on the DRIVER; only literal schema JSON strings cross
to the executors, which keeps the decode fully vectorised.

READER SCHEMA - AND WHY IT IS MANDATORY
---------------------------------------
`from_avro(payload, jsonFormatSchema, options)` maps onto Avro's two-schema resolution:

    jsonFormatSchema   -> the WRITER (actual) schema: how the bytes were encoded
    options.avroSchema -> the READER (expected) schema: the shape you want out

The reader schema fixes the type of the `payload` struct. Because curated stores payload as
ONE struct column, two writer versions decoded without a common reader schema would produce
two incompatible struct types that cannot be unioned into one table. Avro resolution fills
reader-only fields from their defaults and drops writer-only fields, so a mixed-version
batch lands cleanly.

Because that positional/option mapping is easy to get backwards and has moved between Spark
versions, `assert_from_avro_semantics()` PROVES it at runtime, once per run, with a
synthetic record - rather than a comment asserting it is true. VB-10.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import ArrayType, MapType, StructField, StructType
from pyspark.sql.window import Window

from . import tables
from .config import READER_PINNED, KafkaConfig
from .registry import SchemaResolutionError
from .wire import MALFORMED_DETAIL, avro_payload_col, distinct_schema_ids

LOG = logging.getLogger(__name__)

PAYLOAD_COL = "payload"
_DECODED = "_decoded_tmp"

REASON_SCHEMA_LOOKUP = "schema_resolution_failed"
REASON_DECODE = "avro_decode_failed"

# Columns copied straight through from the landing projection into curated.
#
# Two named groups rather than one tuple that gets sliced positionally: the quarantine
# projection interleaves other columns between them, and a slice index would silently
# mis-split the moment anyone added a Kafka column.
_KAFKA_PASSTHROUGH = (
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "kafka_key",
    "kafka_key_string",
    "kafka_headers",
)
_CE_PASSTHROUGH = (
    "ce_id",
    "ce_source",
    "ce_type",
    "ce_subject",
    "ce_time",
    "ce_specversion",
    "ce_dataschema",
    "ce_datacontenttype",
)
_PASSTHROUGH = _KAFKA_PASSTHROUGH + _CE_PASSTHROUGH

_SEMANTICS_CHECKED = False


def event_date_col():
    """Curated's partition key: the date the EVENT happened, not the date we ingested it.

    Prefers the CloudEvents time, falling back to the Kafka record timestamp when the
    producer emits no CloudEvents or the value is unusable.

    The regex guard is load-bearing. ce_time is stored verbatim as a STRING, so it may be
    NULL or malformed; to_date() on a malformed string THROWS under ANSI mode and would
    fail the whole batch. CASE WHEN evaluates its branches per row, so to_date only ever
    sees a value that already starts with YYYY-MM-DD, which RFC3339 guarantees.
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
    failures produces no quarantine frame. Callers must handle None rather than assuming an
    empty DataFrame - an empty frame would cost a job to discover was never populated.
    """

    curated_df: DataFrame | None = None
    quarantine_df: DataFrame | None = None
    writer_schema_ids: list = field(default_factory=list)
    reader_schema_id: int | None = None
    unresolvable_schema_ids: dict = field(default_factory=dict)


# --------------------------------------------------------------------------------------
# Runtime self-check of from_avro's writer/reader argument mapping
# --------------------------------------------------------------------------------------

_SELFCHECK_WRITER = json.dumps(
    {
        "type": "record",
        "name": "FromAvroSelfCheck",
        "namespace": "kafka_ingest.selfcheck",
        "fields": [{"name": "a", "type": "int"}],
    }
)
_SELFCHECK_READER = json.dumps(
    {
        "type": "record",
        "name": "FromAvroSelfCheck",
        "namespace": "kafka_ingest.selfcheck",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string", "default": "resolved-by-default"},
        ],
    }
)
# Avro binary for {"a": 2}: an int is a zigzag varint, zigzag(2) = 4 -> the single byte 0x04.
_SELFCHECK_PAYLOAD = bytes([4])


def assert_from_avro_semantics(spark: Any) -> None:
    """Prove that jsonFormatSchema is the WRITER and options.avroSchema is the READER.

    Decodes one synthetic record written with a 1-field schema, read with a 2-field schema
    whose extra field has a default. Correct schema resolution yields a=2 AND b filled from
    the default. If the arguments were interpreted the other way round the decode would
    either fail or return the wrong column set - either way this raises before a single
    production byte is touched.
    """
    global _SEMANTICS_CHECKED
    if _SEMANTICS_CHECKED:
        return
    probe = spark.createDataFrame([(_SELFCHECK_PAYLOAD,)], "payload BINARY")
    try:
        decoded = (
            probe.select(from_avro(F.col("payload"), _SELFCHECK_WRITER, {"avroSchema": _SELFCHECK_READER}).alias("d"))
            .select("d.a", "d.b")
            .collect()
        )
    except Exception as exc:
        raise RuntimeError(
            "from_avro reader/writer schema self-check failed to execute. This runtime does not "
            "support supplying a reader schema via the 'avroSchema' option. Underlying error: "
            f"{exc}"
        ) from exc

    if not decoded or decoded[0]["a"] != 2 or decoded[0]["b"] != "resolved-by-default":
        raise RuntimeError(
            "from_avro reader/writer schema self-check returned unexpected results "
            f"({decoded!r}). This runtime maps the positional schema argument differently than "
            "this framework expects; decoding production data would be unsafe."
        )
    _SEMANTICS_CHECKED = True
    LOG.info("from_avro writer/reader schema semantics verified on this runtime.")


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


def resolve_reader_schema(cfg: KafkaConfig, client: Any) -> tuple:
    """Return (reader_schema_id, reader_schema_json). Never None - see the module docstring.

    Resolved ONCE PER RUN, never per batch: a registration landing mid-run would otherwise
    change the payload struct's shape partway through a sequence of microbatches.
    """
    if cfg.reader_schema_mode == READER_PINNED:
        return cfg.reader_schema_id, client.get_schema_by_id(cfg.reader_schema_id)
    return client.get_latest(cfg.subject)


def parse_batch(
    spark: Any,
    landing_df: DataFrame,
    cfg: KafkaConfig,
    client: Any,
    txn_version: int,
    run_id: str,
    reader_schema: tuple | None = None,
) -> ParseResult:
    """Parse a projected-landing DataFrame into curated rows plus quarantine rows.

    `landing_df` is the SAME in-memory microbatch already written to landing - it is not
    re-read from Kafka and not re-read from the landing table.
    """
    reader_id, reader_json = reader_schema or resolve_reader_schema(cfg, client)
    assert_from_avro_semantics(spark)

    quarantine_mode = cfg.quarantine_on_error
    decode_options = {"mode": "PERMISSIVE" if quarantine_mode else "FAILFAST", "avroSchema": reader_json}

    result = ParseResult(reader_schema_id=reader_id)
    schema_ids = distinct_schema_ids(landing_df)
    if not schema_ids:
        # An empty microbatch. AvailableNow emits one at the tail of every run.
        return result

    curated_frames: list = []
    quarantine_frames: list = []

    # --- records with no usable wire-format header -----------------------------------
    # Split by REASON, not lumped together: a tombstone, a truncated payload and a
    # non-Confluent framing are three different producing-team conversations, and one
    # shared "malformed" label sends the first responder to the wrong one.
    if None in schema_ids:
        malformed = landing_df.where(F.col("writer_schema_id").isNull())
        if not quarantine_mode:
            _raise_failfast(malformed, cfg, txn_version)
        for reason, detail in MALFORMED_DETAIL.items():
            group = malformed.where(F.col("malformed_reason") == F.lit(reason))
            quarantine_frames.append(_to_quarantine(group, cfg, txn_version, run_id, reason, detail))
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
            # were written. Raw bytes go to quarantine and are recoverable by a curated
            # replay once the schema is registered.
            result.unresolvable_schema_ids[schema_id] = str(exc)
            quarantine_frames.append(
                _to_quarantine(group, cfg, txn_version, run_id, REASON_SCHEMA_LOOKUP, str(exc)[:2000])
            )
            continue

        result.writer_schema_ids.append(schema_id)
        decoded = group.withColumn(_DECODED, from_avro(avro_payload_col("value"), writer_json, decode_options))

        if quarantine_mode:
            # PERMISSIVE turns an unparseable record into a NULL struct. Split on that.
            failed = decoded.where(F.col(_DECODED).isNull())
            quarantine_frames.append(
                _to_quarantine(
                    failed,
                    cfg,
                    txn_version,
                    run_id,
                    REASON_DECODE,
                    f"payload did not parse against writer schema id {schema_id}",
                )
            )
            decoded = decoded.where(F.col(_DECODED).isNotNull())

        curated_frames.append(_to_curated(decoded, cfg, txn_version, run_id, reader_id))

    result.curated_df = _apply_dedup(_union(curated_frames), cfg) if curated_frames else None
    result.quarantine_df = _union(quarantine_frames)
    return result


def _union(frames: list) -> DataFrame | None:
    """Union frames that share a schema by construction, or None when there are none.

    Every curated frame shares ONE reader schema, so the payload struct type is identical
    across writer versions and this is a plain union with no column reconciliation.
    """
    if not frames:
        return None
    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame)
    return unioned


def _raise_failfast(malformed: DataFrame, cfg: KafkaConfig, txn_version: int) -> None:
    """FAILFAST on a wire-format failure: name the reasons AND the offsets.

    The offsets are what an operator needs to look at the actual records, and the reasons
    are what tells them which producing team to call. Collecting five rows on the driver is
    affordable precisely because this path ends the batch.
    """
    sample = malformed.select("topic", "kafka_partition", "kafka_offset", "malformed_reason").limit(5).collect()
    reasons = sorted({row["malformed_reason"] for row in sample})
    offsets = [(row["topic"], row["kafka_partition"], row["kafka_offset"]) for row in sample]
    raise SchemaResolutionError(
        f"topic '{cfg.topic}' batch {txn_version}: records without a valid Confluent wire-format "
        f"header. Reasons seen: {reasons}. First offsets: {offsets}. If these are expected on "
        "this topic, set kafka_failure_mode='QUARANTINE' in the control table to drain the "
        "stream - the records stay recoverable from landing."
    )


def _to_curated(
    decoded: DataFrame, cfg: KafkaConfig, txn_version: int, run_id: str, reader_id: int | None
) -> DataFrame:
    """Kafka + CloudEvent + lineage columns, then the nested payload LAST.

    Payload goes last so `SELECT *` shows the identifying columns before the blob. The
    order matches tables.CURATED_FIXED_COLUMNS + ['payload']; a test asserts it.
    """
    return decoded.select(
        *[F.col(c) for c in _PASSTHROUGH],
        event_date_col().alias("event_date"),
        F.col("writer_schema_id"),
        F.lit(reader_id).cast("int").alias("reader_schema_id"),
        F.col("ingest_ts"),
        F.col("ingest_date"),
        F.lit(cfg.ingested_via).alias("ingested_via"),
        F.lit(cfg.replay.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(txn_version).cast("bigint").alias("txn_version"),
        F.lit(run_id).alias("run_id"),
        F.col(_DECODED).alias(PAYLOAD_COL),
    )


def _apply_dedup(curated_df: DataFrame, cfg: KafkaConfig) -> DataFrame:
    """Collapse duplicates on the configured business key, WITHIN THIS BATCH ONLY.

    Keys may be nested paths into the payload, e.g. `payload.claim_id` - the normal case
    now that the payload is a struct.

    Cross-batch deduplication is deliberately not attempted: on the primary stream Kafka
    offsets are already unique, and on a replay the MERGE key handles the overlap. Doing it
    here as well would need streaming state and would make each batch depend on the last,
    which is exactly what makes a replay non-reproducible.
    """
    if not cfg.curated_dedup_keys:
        return curated_df
    for key in list(cfg.curated_dedup_keys) + [cfg.curated_dedup_order_by]:
        try:
            curated_df.selectExpr(key)
        except Exception as exc:
            raise ValueError(
                f"source '{cfg.source_key}': dedup expression '{key}' does not resolve against the "
                "curated row. Business keys live inside the payload struct, so they are normally "
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


def _to_quarantine(
    df: DataFrame, cfg: KafkaConfig, txn_version: int, run_id: str, reason: str, detail: str
) -> DataFrame:
    """Project failing records into the quarantine schema, RAW BYTES RETAINED.

    Keeping `value` means a quarantined record is fully recoverable: register the missing
    schema, then run the curated replay over the landing rows at the same offsets.
    Quarantine is a diagnostic index into landing, not a separate copy of truth.
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
        F.lit(cfg.replay.rerun_id).cast("string").alias("replay_run_id"),
        F.lit(txn_version).cast("bigint").alias("txn_version"),
        F.lit(run_id).alias("run_id"),
    )


# --------------------------------------------------------------------------------------
# The curated table's schema, computed before any data is read
# --------------------------------------------------------------------------------------


def _as_all_nullable(data_type):
    """Recursively mark every field nullable.

    Avro records declare non-optional fields and from_avro faithfully reports them as NOT
    NULL. Baking that into the CREATE TABLE would be a bad trade: the payload comes from an
    external producer, and a single record violating it would fail the whole batch ON WRITE
    rather than being routed to quarantine like every other bad record.

    The Avro decode still enforces the contract at parse time. This only stops the TABLE
    carrying a constraint that turns a data-quality problem into an outage.
    """
    if isinstance(data_type, StructType):
        return StructType(
            [StructField(f.name, _as_all_nullable(f.dataType), True, f.metadata) for f in data_type.fields]
        )
    if isinstance(data_type, ArrayType):
        return ArrayType(_as_all_nullable(data_type.elementType), True)
    if isinstance(data_type, MapType):
        return MapType(data_type.keyType, _as_all_nullable(data_type.valueType), True)
    return data_type


def curated_schema(spark: Any, cfg: KafkaConfig, reader_schema: tuple) -> StructType:
    """The exact schema curated rows will have, derived with ZERO rows read.

    Runs the real projection - the same _to_curated() the writer uses - over an empty
    landing frame. That is the point: the created table matches what the writer produces by
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
