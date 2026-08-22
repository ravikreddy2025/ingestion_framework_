"""Target table DDL and physical layout.

    landing     ONE Delta table per topic, PARTITIONED BY (ingest_date)
    curated     ONE Delta table per topic, PARTITIONED BY (event_date)
    quarantine  Per topic. Records that could not be parsed, raw bytes retained.
    audit       ONE shared table. One row per (batch, layer, status) transition.

Landing, quarantine and audit have fixed, known schemas and are created here with explicit
DDL, so the physical layout is part of the code rather than an accident of whatever the
first write produced.

Curated is different only in WHERE its column list comes from. Its `payload` column is a
STRUCT whose shape follows the Avro reader schema, so it cannot be written as a static
constant - but the schema is known on the driver once the reader schema has been resolved,
before a single row is read. curated_writer.curated_schema() computes it, and this module
creates the table from it with the same explicit DDL as everything else.

That matters for three reasons:
  * the table gets the same TBLPROPERTIES as every other table. When Spark created it
    implicitly on first write it got none, so curated - the table people actually query -
    was the only one without auto-compaction.
  * the shape is decided by this framework rather than inferred from whatever the first
    batch happened to contain.
  * it exists, empty and queryable, from the first run - before any data arrives.

Additive schema growth afterwards is still handled by mergeSchema on append and by
withSchemaEvolution on the replay MERGE. Explicit creation fixes the STARTING shape; those
handle the CHANGING shape. See curated_writer.py.

WHY PARTITIONING AND NOT LIQUID CLUSTERING
------------------------------------------
Delta lets a table use PARTITIONED BY or CLUSTER BY, never both.

  landing   (ingest_date). The table is one-per-topic, so `topic` is constant inside it
            and would only add a single-value partition directory. `ingest_date` bounds
            partition growth over years of daily runs and makes retention a partition drop
            rather than a rewrite.
  curated   (event_date). Also one-per-topic. Queries filter on when the event happened,
            not on when we ingested it.

Late-arriving events write into older event_date partitions. That is expected and correct.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from pyspark.sql import SparkSession

from .config import TopicConfig

LOG = logging.getLogger(__name__)

# Fallback used only when a config supplies no table_properties at all. The real defaults
# live in conf/defaults.yaml so they are reviewable and overridable per topic.
_FALLBACK_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _properties_clause(cfg: TopicConfig) -> str:
    """Render TBLPROPERTIES from config.

    Keys and values are quoted as SQL string literals. A single quote in either would break
    the statement, so it is rejected rather than escaped - a Delta property name containing
    a quote is a typo, not a use case.
    """
    properties = cfg.table_properties or _FALLBACK_TABLE_PROPERTIES
    for key, value in properties.items():
        if "'" in str(key) or "'" in str(value):
            raise ValueError(
                f"table_properties entry {key!r}={value!r} contains a single quote, which "
                "cannot be embedded in a TBLPROPERTIES clause."
            )
    return ", ".join(f"'{key}' = '{value}'" for key, value in properties.items())

# --------------------------------------------------------------------------------------
# Column blocks shared between landing and curated.
#
# Both layers carry the SAME Kafka and CloudEvent columns, so a curated row can always be
# traced back to its landing row on (topic, kafka_partition, kafka_offset), and an operator
# moving between layers does not have to relearn the column names.
# --------------------------------------------------------------------------------------

# `kafka_partition` / `kafka_offset` rather than bare `partition` / `offset`: both are SQL
# keywords, and `partition` reads ambiguously next to a PARTITIONED BY clause.
KAFKA_COLUMNS = """
    topic                 STRING    COMMENT 'Kafka topic name, verbatim. Constant within a table; kept for lineage',
    kafka_partition       INT       COMMENT 'Kafka partition',
    kafka_offset          BIGINT    COMMENT 'Kafka offset - unique with (topic, kafka_partition)',
    kafka_timestamp       TIMESTAMP COMMENT 'Broker or producer timestamp, per topic config',
    kafka_timestamp_type  INT       COMMENT '0=CreateTime (producer), 1=LogAppendTime (broker)',
    kafka_key             BINARY    COMMENT 'Raw Kafka key bytes, verbatim',
    kafka_key_string      STRING    COMMENT 'Best-effort UTF-8 rendering of the key',
    kafka_headers         ARRAY<STRUCT<key: STRING, value: BINARY>> COMMENT 'All Kafka headers, verbatim'
"""

# CloudEvents v1.0, Kafka protocol binding, BINARY content mode: context attributes travel
# as `ce_*` headers and the event data is the Kafka message value - which is exactly the
# Avro payload this framework decodes.
#
# ce_time is stored as STRING verbatim. RFC3339 UTC timestamps sort correctly as strings,
# and parsing here would either silently NULL a malformed value or throw under ANSI mode -
# neither is a decision an ingestion layer should make. Cast downstream with to_timestamp(),
# or use the derived event_date column on curated.
#
# Attributes beyond these eight remain available in kafka_headers. A ce_extensions MAP was
# deliberately NOT added: Kafka permits duplicate header keys and map construction from
# them errors on duplicates.
CLOUDEVENT_COLUMNS = """
    ce_id                 STRING COMMENT 'CloudEvents id - unique per event from the producer',
    ce_source             STRING COMMENT 'CloudEvents source - the producing context URI',
    ce_type               STRING COMMENT 'CloudEvents type, e.g. com.acme.claim.updated',
    ce_subject            STRING COMMENT 'CloudEvents subject - the entity within the source',
    ce_time               STRING COMMENT 'CloudEvents time, RFC3339 verbatim. Cast with to_timestamp().',
    ce_specversion        STRING COMMENT 'CloudEvents spec version, normally 1.0',
    ce_dataschema         STRING COMMENT 'CloudEvents dataschema URI',
    ce_datacontenttype    STRING COMMENT 'From the content-type header, falling back to ce_datacontenttype'
"""

INGESTION_COLUMNS = """
    ingest_ts             TIMESTAMP COMMENT 'When this framework wrote the row',
    ingest_date           DATE      COMMENT 'Date form of ingest_ts - landing partition key',
    ingested_via          STRING    COMMENT 'primary | kafka_replay | curated_replay',
    replay_run_id         STRING    COMMENT 'rerun_id when ingested_via is a replay, else NULL',
    batch_id              BIGINT    COMMENT 'Structured Streaming microbatch id, or -1 for batch runs',
    run_id                STRING    COMMENT 'Correlates with audit_table.run_id'
"""

LANDING_DDL_COLUMNS = f"""{KAFKA_COLUMNS},
    value                 BINARY  COMMENT 'Raw Kafka value bytes, verbatim, INCLUDING the 5-byte Confluent header',
    writer_schema_id      INT     COMMENT 'Schema id from wire-format bytes 1-4; NULL when not Confluent-framed',
    wire_format_valid     BOOLEAN COMMENT 'FALSE when the magic byte is missing or the record is under 5 bytes',
    payload_bytes         INT     COMMENT 'Length of the value column in bytes',
{CLOUDEVENT_COLUMNS},
{INGESTION_COLUMNS}
"""

# Curated's non-payload columns, in order. `payload` is appended by the projection. This constant
# documents the non-payload columns, in order, and a test asserts the projection matches.
CURATED_FIXED_COLUMNS = f"""{KAFKA_COLUMNS},
{CLOUDEVENT_COLUMNS},
    event_date            DATE COMMENT 'Curated partition key: date of ce_time, falling back to kafka_timestamp',
    writer_schema_id      INT  COMMENT 'Schema the record was DECODED with, from its own wire header',
    reader_schema_id      INT  COMMENT 'Schema that fixed the shape of the payload struct',
{INGESTION_COLUMNS}
"""

QUARANTINE_DDL_COLUMNS = f"""{KAFKA_COLUMNS},
    value                 BINARY COMMENT 'Raw bytes, so a fixed parser can recover this row later',
    writer_schema_id      INT    COMMENT 'NULL when the wire format itself was unreadable',
    quarantine_reason     STRING COMMENT 'malformed_wire_format | schema_resolution_failed | avro_decode_failed',
    quarantine_detail     STRING COMMENT 'Registry error text or decode diagnostics, truncated',
    quarantined_ts        TIMESTAMP,
{CLOUDEVENT_COLUMNS},
{INGESTION_COLUMNS}
"""

# One row per (batch, layer, status). A healthy batch writes six: landing STARTED/COMPLETED,
# curated STARTED/COMPLETED, and two stream rows from the listener. That is what makes
# "which layer was it on when it died?" answerable.
AUDIT_DDL_COLUMNS = """
    audit_id              STRING    COMMENT 'run_id::batch_id::layer::status - unique per row',
    run_id                STRING    COMMENT 'One value per job execution; also stamped on data rows',
    batch_id              BIGINT    COMMENT 'Microbatch id, or -1 for bounded batch runs',
    topic_key             STRING,
    topic                 STRING,
    domain                STRING,
    layer                 STRING    COMMENT 'stream | landing | curated',
    status                STRING    COMMENT 'STARTED | COMPLETED | FAILED | SKIPPED | NO_DATA',
    record_count          BIGINT    COMMENT 'Rows PRESENTED to the write - see audit.py caveat',
    quarantined_count     BIGINT,
    event_ts              TIMESTAMP COMMENT 'When this transition was recorded',
    duration_ms           BIGINT    COMMENT 'Time in this layer, or whole-batch time for layer=stream',
    run_type              STRING    COMMENT 'primary | kafka_replay | curated_replay',
    rerun_id              STRING,
    job_run_id            STRING,
    starting_offsets      STRING    COMMENT 'Per-partition JSON at batch start (layer=stream)',
    ending_offsets        STRING    COMMENT 'Per-partition JSON at batch end (layer=stream)',
    writer_schema_ids     ARRAY<INT> COMMENT 'Distinct writer schema ids seen in the batch',
    reader_schema_id      INT,
    checkpoint_path       STRING    COMMENT 'Which checkpoint lineage produced this batch',
    error_class           STRING,
    error_message         STRING,
    spark_progress_json   STRING    COMMENT 'Raw StreamingQueryProgress JSON (layer=stream)',
    audit_date            DATE
"""


def table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def _create(spark: SparkSession, cfg: TopicConfig, name: str, columns: str, comment: str,
            partition_by: Optional[List[str]] = None) -> None:
    """CREATE TABLE IF NOT EXISTS. Idempotent, and a metadata no-op once the table exists."""
    clause = f"PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {name} (
        {columns}
        )
        USING DELTA
        {clause}
        COMMENT '{comment}'
        TBLPROPERTIES ({_properties_clause(cfg)})
        """
    )


def ensure_landing_table(spark: SparkSession, cfg: TopicConfig) -> None:
    """One table per topic, named from the Kafka topic - see config.table_name_for()."""
    _create(spark, cfg, cfg.landing_table, LANDING_DDL_COLUMNS,
            "Raw Kafka wire-format bytes for one topic, append-only. System of record for what arrived.",
            partition_by=cfg.landing_partition_by)


def ensure_quarantine_table(spark: SparkSession, cfg: TopicConfig) -> None:
    _create(spark, cfg, cfg.quarantine_table, QUARANTINE_DDL_COLUMNS,
            "Records that could not be parsed into curated, with raw bytes retained.",
            partition_by=["ingest_date"])


def ensure_audit_table(spark: SparkSession, cfg: TopicConfig) -> None:
    _create(spark, cfg, cfg.audit_table, AUDIT_DDL_COLUMNS,
            "Per-batch, per-layer streaming status. First stop for incident triage.",
            partition_by=["audit_date"])


def ensure_curated_table(spark: SparkSession, cfg: TopicConfig, schema) -> None:
    """Create curated from a schema computed on the driver, before any row is read.

    `schema` comes from curated_writer.curated_schema(), which runs the real projection over
    an empty frame - so the created table matches what the writer produces by construction,
    not by two definitions being kept in step by hand.

    StructType.toDDL() renders nested structs, arrays and maps correctly, which is what makes
    a static constant unnecessary here.
    """
    _create(spark, cfg, cfg.curated_table, schema.toDDL(),
            "Parsed events for one topic. Payload kept nested in a single struct column.",
            partition_by=cfg.curated_partition_by)


def ensure_all(spark: SparkSession, cfg: TopicConfig) -> None:
    """Called once per run, before any write. Cheap no-ops when everything already exists.

    Curated is absent here because it needs the resolved reader schema, which is not
    available this early. pipeline.py creates it immediately after building the run context
    via ensure_curated_table() - it is not left to Spark.
    """
    ensure_audit_table(spark, cfg)
    ensure_landing_table(spark, cfg)
    if cfg.on_deser_error == "quarantine":
        ensure_quarantine_table(spark, cfg)
