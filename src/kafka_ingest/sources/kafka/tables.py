"""This source type's three target tables: their column lists and their creation.

    landing     ONE Delta table per topic, PARTITIONED BY (ingest_date)
    curated     ONE Delta table per topic, PARTITIONED BY (event_date)
    quarantine  Per topic, PARTITIONED BY (ingest_date). Raw bytes retained.

Landing and quarantine have fixed, known schemas and are created here from explicit DDL,
so the physical layout is part of the code rather than an accident of whatever the first
write happened to produce.

Curated differs only in WHERE its column list comes from. Its `payload` column is a STRUCT
whose shape follows the Avro reader schema, so it cannot be a static constant - but the
schema is known on the driver once the reader schema resolves, before a single row is read.
curated.curated_schema() computes it and this module creates the table from it, with the
same explicit DDL as everything else. That matters for three reasons: the table gets the
same TBLPROPERTIES as every other table (created implicitly, curated - the table people
actually query - would be the only one without auto-compaction); the shape is decided here
rather than inferred from whatever the first batch contained; and it exists, empty and
queryable, from the first run.

LANDING AND CURATED SHARE THEIR KAFKA AND CLOUDEVENT COLUMNS
------------------------------------------------------------
Deliberately, so a curated row always traces back to its landing row on
(topic, kafka_partition, kafka_offset) - which is also the MERGE key that makes a replay
idempotent - and so an operator moving between layers does not relearn the column names.

`kafka_partition` / `kafka_offset` rather than bare `partition` / `offset`: both are SQL
keywords, and `partition` reads ambiguously next to a PARTITIONED BY clause.

WHY PARTITIONING AND NOT LIQUID CLUSTERING
------------------------------------------
Delta allows PARTITIONED BY or CLUSTER BY, never both. Both layers are one table per topic,
so `topic` is constant inside each and worthless as a key. landing gets ingest_date, which
bounds partition growth over years of daily runs and makes retention a partition drop;
curated gets event_date, because queries filter on when the event happened. Late-arriving
events write into older event_date partitions, which is expected and correct.
"""

from __future__ import annotations

from typing import Any

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
# Avro payload this source decodes.
#
# ce_time is stored as STRING VERBATIM. RFC3339 UTC timestamps sort correctly as strings,
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
    txn_version           BIGINT    COMMENT 'Delta txnVersion for this write: a microbatch id, or -1',
    run_id                STRING    COMMENT 'Correlates with the audit table run_id'
"""

LANDING_DDL_COLUMNS = f"""{KAFKA_COLUMNS},
    value                 BINARY  COMMENT 'Raw Kafka value bytes, verbatim, INCLUDING the 5-byte Confluent header',
    writer_schema_id      INT     COMMENT 'Schema id from wire-format bytes 1-4; NULL when not Confluent-framed',
    wire_format_valid     BOOLEAN COMMENT 'FALSE when the value is NULL, under 5 bytes, or not 0x00-framed',
    malformed_reason      STRING  COMMENT 'NULL_VALUE_TOMBSTONE | TRUNCATED_PAYLOAD | BAD_MAGIC_BYTE; NULL when valid',
    payload_bytes         INT     COMMENT 'Length of the value column in bytes',
{CLOUDEVENT_COLUMNS},
{INGESTION_COLUMNS}
"""

# Curated's non-payload columns, in order. `payload` is appended by the projection, last,
# so that SELECT * shows the identifying columns before the blob. A test asserts the
# projection matches this list exactly.
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
    quarantine_reason     STRING COMMENT 'A wire-format reason (see landing.malformed_reason), or a decode failure',
    quarantine_detail     STRING COMMENT 'Registry error text or decode diagnostics, truncated',
    quarantined_ts        TIMESTAMP,
{CLOUDEVENT_COLUMNS},
{INGESTION_COLUMNS}
"""

_LANDING_COMMENT = "Raw Kafka wire-format bytes for one topic, append-only. System of record for what arrived."
_CURATED_COMMENT = "Parsed events for one topic. Payload kept nested in a single struct column."
_QUARANTINE_COMMENT = "Records that could not be parsed into curated, with raw bytes retained."


def ensure_landing(ctx: Any, cfg: Any) -> None:
    _create(ctx, cfg, cfg.landing_table, LANDING_DDL_COLUMNS, _LANDING_COMMENT, list(cfg.landing_partition_by))


def ensure_quarantine(ctx: Any, cfg: Any) -> None:
    _create(ctx, cfg, cfg.quarantine_table, QUARANTINE_DDL_COLUMNS, _QUARANTINE_COMMENT, ["ingest_date"])


def ensure_curated(ctx: Any, cfg: Any, schema: Any) -> None:
    """Create curated from a schema computed on the driver, before any row is read.

    `schema` comes from curated.curated_schema(), which runs the REAL projection over an
    empty frame - so the created table matches what the writer produces by construction,
    rather than by two definitions being kept in step by hand. StructType.toDDL() renders
    nested structs, arrays and maps correctly, which is what makes a static constant
    unnecessary here.
    """
    _create(ctx, cfg, cfg.curated_table, schema.toDDL(), _CURATED_COMMENT, list(cfg.curated_partition_by))


def _create(ctx: Any, cfg: Any, name: str, columns: str, comment: str, partition_by: list) -> None:
    """One call into framework/tables.py, so every table this source creates gets the same
    TBLPROPERTIES, the same name validation and the same one-layout-clause rule."""
    ctx.tables.ensure_table(
        ctx.spark,
        name,
        columns,
        comment,
        properties=cfg.table_properties,
        partition_by=partition_by,
    )
