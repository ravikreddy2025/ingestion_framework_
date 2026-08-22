-- =====================================================================================
-- LANDING / CURATED / AUDIT schemas and tables.
--
-- THIS FILE IS OPTIONAL. Onboarding a topic requires NO manual DDL.
--
-- The framework creates every table it owns on first run (src/kafka_ingest/tables.py):
--   landing / quarantine / audit  from the constants in that module
--   curated                       from a schema derived on the driver from the Avro reader
--                                 schema, before any row is read - see
--                                 curated_writer.curated_schema()
-- All of them use CREATE TABLE IF NOT EXISTS, so this is a metadata no-op after run one.
--
-- This file exists for two other reasons: so an environment can be provisioned and GRANTed
-- ahead of the first run, and so the landing schema is reviewable in a PR rather than only
-- discoverable by reading Python. Curated is deliberately not here - its payload struct is
-- schema-derived, so a static copy would go stale.
--
-- KEEP THIS FILE AND tables.py IN STEP. A mismatch surfaces immediately as a Delta schema
-- error on the first append, not as silently NULL columns.
-- tests/test_audit_and_tables.py::test_provisioning_sql_matches_the_python_ddl compares the
-- column lists in this file against the DDL constants in tables.py, so drift fails in CI
-- rather than on a cluster.
--
-- CURATED IS NOT HERE ON PURPOSE. Its `payload` column is a STRUCT whose shape comes from
-- the Avro reader schema, so a static copy here would go stale the first time a schema is
-- registered. The framework creates it with explicit DDL at run time instead, from a schema
-- derived off the reader schema - see curated_writer.curated_schema() and
-- pipeline.ensure_curated().
-- =====================================================================================
-- TEMPLATE - NOT READY TO RUN AS-IS. See the note in sql/01_operational_config.sql:
-- {catalog} is rendered from conf/environments/<env>.yaml by notebooks/00_validate_config.

CREATE SCHEMA IF NOT EXISTS {catalog}.landing
  COMMENT 'Raw Kafka wire bytes. One table per topic, partitioned by ingest_date.';
CREATE SCHEMA IF NOT EXISTS {catalog}.curated
  COMMENT 'Parsed events, one table per topic, payload kept nested.';
CREATE SCHEMA IF NOT EXISTS {catalog}.audit
  COMMENT 'Per-batch, per-layer streaming status. Separate from business data by design.';

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
  wire_format_valid     BOOLEAN,
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
  batch_id              BIGINT,
  run_id                STRING    COMMENT 'Correlates with audit.stream_audit.run_id'
)
USING DELTA
PARTITIONED BY (ingest_date)
COMMENT 'Raw Kafka wire-format bytes for one topic. System of record for what arrived.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- -------------------------------------------------------------------------------------
-- AUDIT - ONE table. One row per (batch, layer, status) transition.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.audit.stream_audit (
  audit_id              STRING    COMMENT 'run_id::batch_id::layer::status',
  run_id                STRING,
  batch_id              BIGINT    COMMENT '-1 for bounded batch runs',
  topic_key             STRING,
  topic                 STRING,
  domain                STRING,
  layer                 STRING    COMMENT 'stream | landing | curated',
  status                STRING    COMMENT 'STARTED | COMPLETED | FAILED | SKIPPED | NO_DATA',
  record_count          BIGINT    COMMENT 'Rows PRESENTED to the write - see the caveat in audit.py',
  quarantined_count     BIGINT,
  event_ts              TIMESTAMP,
  duration_ms           BIGINT,
  run_type              STRING,
  rerun_id              STRING,
  job_run_id            STRING,
  starting_offsets      STRING    COMMENT 'Per-partition JSON at batch start (layer=stream)',
  ending_offsets        STRING    COMMENT 'Per-partition JSON at batch end (layer=stream)',
  writer_schema_ids     ARRAY<INT>,
  reader_schema_id      INT,
  checkpoint_path       STRING    COMMENT 'Which checkpoint lineage produced this batch',
  error_class           STRING,
  error_message         STRING,
  spark_progress_json   STRING,
  audit_date            DATE
)
USING DELTA
PARTITIONED BY (audit_date)
COMMENT 'Per-batch, per-layer ingestion status. First stop for incident triage.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- -------------------------------------------------------------------------------------
-- QUARANTINE - per topic, only used when on_deser_error = quarantine.
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
  quarantine_reason     STRING    COMMENT 'malformed_wire_format | schema_resolution_failed | avro_decode_failed',
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
  batch_id              BIGINT,
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
-- GRANTS
-- =====================================================================================
GRANT SELECT ON TABLE {catalog}.audit.stream_audit TO `ingestion-support`;
GRANT SELECT ON TABLE {catalog}.landing.{topic_table} TO `ingestion-support`;
GRANT SELECT, MODIFY ON TABLE {catalog}.audit.stream_audit TO `sp-kafka-ingestion`;
GRANT SELECT, MODIFY ON TABLE {catalog}.landing.{topic_table} TO `sp-kafka-ingestion`;
