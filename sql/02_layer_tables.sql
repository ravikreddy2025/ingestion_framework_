-- =====================================================================================
-- LANDING / CURATED / AUDIT schemas and tables.
--
-- THIS FILE IS OPTIONAL. Onboarding a topic requires NO manual DDL.
--
-- The framework creates every table it owns on first run:
--   landing / quarantine          from the constants in sources/kafka/tables.py
--   audit                         from framework/audit.py
--   curated                       from a schema derived on the driver from the Avro reader
--                                 schema, before any row is read - see
--                                 sources/kafka/curated.py curated_schema()
-- All of them use CREATE TABLE IF NOT EXISTS, so this is a metadata no-op after run one.
--
-- This file exists for two other reasons: so an environment can be provisioned and GRANTed
-- ahead of the first run, and so the landing schema is reviewable in a PR rather than only
-- discoverable by reading Python. Curated is deliberately not here - its payload struct is
-- schema-derived, so a static copy would go stale.
--
-- KEEP THIS FILE AND THE PYTHON DDL IN STEP. A mismatch surfaces immediately as a Delta
-- schema error on the first append, not as silently NULL columns. Two tests compare the
-- column lists in this file against the constants that would otherwise create the tables,
-- so drift fails in CI rather than on a cluster:
--   audit                  tests/test_framework_audit.py (framework/audit.py)
--   landing / quarantine   tests/test_kafka_tables.py (sources/kafka/tables.py)
--
-- CURATED IS NOT HERE ON PURPOSE. Its `payload` column is a STRUCT whose shape comes from
-- the Avro reader schema, so a static copy here would go stale the first time a schema is
-- registered. The framework creates it with explicit DDL at run time instead, from a schema
-- derived off the reader schema - see sources/kafka/curated.py curated_schema() and
-- sources/kafka/tables.py ensure_curated().
-- =====================================================================================
-- TEMPLATE - NOT READY TO RUN AS-IS. See the note in sql/01_operational_config.sql:
-- {catalog}, {ops_catalog} and {audit_schema} are rendered from conf/environments/<env>.yaml
-- by notebooks/00_validate_config.

CREATE SCHEMA IF NOT EXISTS {catalog}.landing
  COMMENT 'Raw Kafka wire bytes. One table per topic, partitioned by ingest_date.';
CREATE SCHEMA IF NOT EXISTS {catalog}.curated
  COMMENT 'Parsed events, one table per topic, payload kept nested.';
-- The audit table lives in the OPS catalog, not here (docs/build_log/DECISIONS.md D-06) -
-- it is operational metadata about every source type, not this topic's data. This file
-- also creates its own schema for it (rather than relying on sql/01 having run first) so
-- it stays runnable on its own, exactly like the landing/curated schemas above.
CREATE SCHEMA IF NOT EXISTS {ops_catalog}.{audit_schema}
  COMMENT 'Per-run, per-layer ingestion status for every source. Separate from business data by design.';

-- -------------------------------------------------------------------------------------
-- LANDING - ONE TABLE PER TOPIC.
--
-- {topic_table} is the Kafka topic name with dots and hyphens replaced by underscores,
-- matching conf/defaults.yaml. Repeat this block once per topic, or just let the framework
-- create them - tables.py issues the same DDL on first run. This file exists so an
-- environment can be provisioned and GRANTed ahead of that.
--
-- Partitioned by ingest_date alone: the table holds one topic, so `topic` is constant
-- inside it and would only add a single-value partition directory. ingest_date bounds
-- partition growth and makes retention a partition drop.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.landing.{topic_table} (
  topic                 STRING    COMMENT 'Kafka topic name, verbatim. Constant within this table',
  kafka_partition       INT,
  kafka_offset          BIGINT    COMMENT 'Unique with (topic, kafka_partition) - the replay MERGE key',
  kafka_timestamp       TIMESTAMP,
  kafka_timestamp_type  INT       COMMENT '0=CreateTime (producer), 1=LogAppendTime (broker)',
  kafka_key             BINARY,
  kafka_key_string      STRING    COMMENT 'Best-effort UTF-8 rendering of the key',
  kafka_headers         ARRAY<STRUCT<key: STRING, value: BINARY>>,
  value                 BINARY    COMMENT 'Raw Kafka value, verbatim, INCLUDING the 5-byte Confluent header',
  writer_schema_id      INT       COMMENT 'Parsed from wire bytes 1-4; NULL when not Confluent-framed',
  wire_format_valid     BOOLEAN   COMMENT 'FALSE when the value is NULL, under 5 bytes, or not 0x00-framed',
  malformed_reason      STRING    COMMENT 'NULL_VALUE_TOMBSTONE | TRUNCATED_PAYLOAD | BAD_MAGIC_BYTE; NULL when valid',
  payload_bytes         INT,
  ce_id                 STRING    COMMENT 'CloudEvents attributes, read from ce_* Kafka headers',
  ce_source             STRING,
  ce_type               STRING,
  ce_subject            STRING,
  ce_time               STRING    COMMENT 'RFC3339 verbatim. Cast with to_timestamp().',
  ce_specversion        STRING,
  ce_dataschema         STRING,
  ce_datacontenttype    STRING,
  ingest_ts             TIMESTAMP,
  ingest_date           DATE      COMMENT 'Partition key',
  ingested_via          STRING    COMMENT 'primary | kafka_replay | curated_replay',
  replay_run_id         STRING,
  txn_version           BIGINT    COMMENT 'The Delta txnVersion this row was written under, or -1',
  run_id                STRING    COMMENT 'Correlates with audit.ingest_audit.run_id'
)
USING DELTA
PARTITIONED BY (ingest_date)
COMMENT 'Raw Kafka wire-format bytes for one topic. System of record for what arrived.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- -------------------------------------------------------------------------------------
-- AUDIT - ONE table, shared by EVERY source of every type.
-- One row per (run, layer, status) transition.
--
-- THREE COLUMNS MEAN THREE THINGS. position_start / position_end hold a Kafka offsets
-- JSON, a database cursor value or a file boundary, depending on source_type - which is
-- why they are STRING and why it is said on the column itself. source_detail is a JSON
-- STRING and not a MAP, so a new source type never forces an ALTER TABLE here.
--
-- txn_version (docs/build_log/DECISIONS.md D-03) carries the Delta txnVersion for
-- whatever produced the row - a microbatch id, a batch source's run_sequence, or -1.
--
-- Keep this in step with framework/audit.py: AUDIT_DDL_COLUMNS (the table),
-- AUDIT_SCHEMA (the DataFrame) and this block are compared column-for-column by
-- tests/test_framework_audit.py.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {ops_catalog}.{audit_schema}.ingest_audit (
  audit_id              STRING    COMMENT 'run_id::txn_version::layer::status - unique per row',
  run_id                STRING    COMMENT 'One value per job execution; also stamped on data rows',
  txn_version           BIGINT    COMMENT 'The Delta txnVersion this row corresponds to: a streaming microbatch id, a batch source run_sequence, or -1',
  source_type           STRING    COMMENT 'Which source implementation ran',
  source_key            STRING    COMMENT 'Matches conf/sources/<source_key>.yaml and ingest_control.source_key',
  source_ref            STRING    COMMENT 'Source-side identifier: topic name, SCHEMA.TABLE, or path glob',
  domain                STRING    COMMENT 'Owning team',
  layer                 STRING    COMMENT 'run, plus whichever layers the source type has',
  status                STRING    COMMENT 'STARTED | COMPLETED | FAILED | SKIPPED | NO_DATA',
  record_count          BIGINT    COMMENT 'Rows PRESENTED to the write - see the caveat in framework/audit.py',
  quarantined_count     BIGINT,
  event_ts              TIMESTAMP COMMENT 'When this transition was recorded',
  duration_ms           BIGINT    COMMENT 'Time spent in this layer',
  run_type              STRING    COMMENT 'primary, or a source-specific replay type',
  rerun_id              STRING    COMMENT 'A replay id; on a primary run, the reset id that forked its write identity',
  job_run_id            STRING    COMMENT 'The Databricks Workflows run id, when there is one',
  position_start        STRING    COMMENT 'THREE MEANINGS by source_type: Kafka offsets, a cursor, a file boundary',
  position_end          STRING    COMMENT 'Upper read boundary. Same three meanings as position_start',
  source_detail         STRING    COMMENT 'JSON STRING, not a map - a new source type forces no ALTER TABLE',
  pending_work          BIGINT    COMMENT 'Work still outstanding when the run ended. NULL means the source cannot cheaply know',
  error_class           STRING,
  error_message         STRING    COMMENT 'Truncated to 4000 characters',
  audit_date            DATE      COMMENT 'Partition key: date of event_ts'
)
USING DELTA
PARTITIONED BY (audit_date)
COMMENT 'Per-run, per-layer ingestion status for every source. First stop for incident triage.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- -------------------------------------------------------------------------------------
-- QUARANTINE - per topic, only used when kafka_failure_mode = QUARANTINE.
-- Template; repeat per topic with the name from the topic YAML.
-- Grants follow the data's sensitivity: a quarantined record still holds the payload.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.landing.rcm_claim_status_quarantine (
  topic                 STRING,
  kafka_partition       INT,
  kafka_offset          BIGINT,
  kafka_timestamp       TIMESTAMP,
  kafka_timestamp_type  INT,
  kafka_key             BINARY,
  kafka_key_string      STRING,
  kafka_headers         ARRAY<STRUCT<key: STRING, value: BINARY>>,
  value                 BINARY    COMMENT 'Raw bytes retained so a curated replay can recover this row',
  writer_schema_id      INT       COMMENT 'NULL when the wire format itself was unreadable',
  quarantine_reason     STRING    COMMENT 'A wire-format reason (see landing.malformed_reason), schema_resolution_failed, or avro_decode_failed',
  quarantine_detail     STRING,
  quarantined_ts        TIMESTAMP,
  ce_id                 STRING,
  ce_source             STRING,
  ce_type               STRING,
  ce_subject            STRING,
  ce_time               STRING,
  ce_specversion        STRING,
  ce_dataschema         STRING,
  ce_datacontenttype    STRING,
  ingest_ts             TIMESTAMP,
  ingest_date           DATE,
  ingested_via          STRING,
  replay_run_id         STRING,
  txn_version           BIGINT    COMMENT 'The Delta txnVersion this row was written under, or -1',
  run_id                STRING
)
USING DELTA
PARTITIONED BY (ingest_date)
COMMENT 'Records that could not be parsed into curated, with raw bytes retained.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =====================================================================================
-- GRANTS ARE TERRAFORM-OWNED, NOT ISSUED HERE (docs/build_log/DECISIONS.md D-02).
-- See the "Unity Catalog privileges" table in docs/ARCHITECTURE_OVERVIEW.md for the exact
-- privilege list a platform admin provisions for the ingestion service principal and the
-- support group, on both the data catalog (landing/curated, below) and the ops catalog
-- (audit, above).
-- =====================================================================================
