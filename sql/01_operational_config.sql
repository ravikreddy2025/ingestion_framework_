-- =====================================================================================
-- OPERATIONAL CONFIG - Tier 2. Support-team editable, no deploy cycle.
--
-- Everything here answers "how should this topic behave RIGHT NOW". Structural facts
-- (which cluster, which registry, which tables, how tables are PARTITIONED) live in Git
-- YAML and are deliberately NOT overridable from this table: a typo in a catalog name or
-- a partition key at 3am should require a PR.
--
-- Exactly one row per topic_key. The job tolerates a MISSING row (meaning "no overrides")
-- but refuses to run on duplicates.
--
-- Attribution: updated_by / updated_at are set by the support UPDATE templates in
-- sql/03_support_queries.sql. Delta's own DESCRIBE HISTORY is the backstop.
-- =====================================================================================
-- TEMPLATE - NOT READY TO RUN AS-IS.
-- {ops_catalog} and {catalog} are placeholders, using the same convention as conf/. Render
-- them for a target environment with notebooks/00_validate_config (section "Render the
-- provisioning SQL"), which substitutes them from conf/environments/<env>.yaml so this file
-- cannot drift from what the code reads. Do NOT hand-edit a copy per environment.

CREATE SCHEMA IF NOT EXISTS {ops_catalog}.ingestion
  COMMENT 'Operational control for the Kafka ingestion framework';

CREATE TABLE IF NOT EXISTS {ops_catalog}.ingestion.ingestion_topic_control (
  topic_key                    STRING  NOT NULL COMMENT 'Matches conf/topics/<topic_key>.yaml',

  -- ---- behaviour toggles ----------------------------------------------------------
  enabled                      BOOLEAN COMMENT 'FALSE = emergency stop. Job runs, consumes nothing, writes a SKIPPED audit row.',
  trigger                      STRING  COMMENT 'availableNow (default) | once | processingTime=<interval>',
  max_offsets_per_trigger      BIGINT  COMMENT 'Caps microbatch size. Lower it to get a huge backlog through in survivable chunks.',
  on_deser_error               STRING  COMMENT 'fail (default) | quarantine. THE lever for unblocking a stuck stream.',
  fail_on_data_loss            BOOLEAN COMMENT 'FALSE only with explicit domain sign-off that gaps are acceptable.',
  reader_schema_mode           STRING  COMMENT 'registry_latest | pinned_id',
  reader_schema_id             INT     COMMENT 'Required when reader_schema_mode = pinned_id',

  -- ---- checkpoint-reset override (incident use only) -------------------------------
  -- Bypasses guard_against_checkpoint_reset AND forks the primary run's Delta txnAppId,
  -- so the restart cannot collide with versions already committed under the old identity.
  -- Set this ONLY after confirming the primary checkpoint is genuinely gone (see
  -- RUNBOOK_SUPPORT.md 5.4a) - never to "unblock" a stream some other way. Put the
  -- incident number here, e.g. 'INC12345'. Do NOT blank it back out once used: reverting
  -- to NULL reverts the txnAppId to the original lineage, which still carries the OLD
  -- watermark and would silently skip every write again.
  checkpoint_reset_id          STRING  COMMENT 'Set ONLY per RUNBOOK_SUPPORT.md 5.4a. Never clear after use.',

  -- ---- replay controls -------------------------------------------------------------
  -- Populated to park a replay intent durably. The replay JOB PARAMETERS win over these,
  -- so an urgent one-off needs no UPDATE first. Clear these once the replay is done.
  rerun_id                     STRING  COMMENT 'REQUIRED for any replay. Isolates the checkpoint AND the Delta txnAppId.',
  rerun_starting_offsets       STRING  COMMENT 'Spark startingOffsets JSON: {"topic":{"0":45231}}. -2=earliest, -1=latest.',
  rerun_starting_timestamp     STRING  COMMENT 'ISO-8601 or epoch millis. Mutually exclusive with rerun_starting_offsets.',
  rerun_ending_offsets         STRING  COMMENT 'Optional. Present => bounded batch replay instead of a streaming replay.',
  rerun_ending_timestamp       STRING  COMMENT 'Optional. Same, by time.',
  curated_replay_landing_filter STRING COMMENT 'SQL predicate over LANDING for a curated-only replay, e.g. writer_schema_id = 5513',

  -- ---- attribution -----------------------------------------------------------------
  change_reason                STRING  COMMENT 'Why this row was last changed. Incident number belongs here.',
  updated_by                   STRING,
  updated_at                   TIMESTAMP,

  CONSTRAINT topic_key_present CHECK (topic_key IS NOT NULL),
  CONSTRAINT on_deser_error_valid CHECK (on_deser_error IS NULL OR on_deser_error IN ('fail', 'quarantine')),
  -- 'writer' was removed: curated stores the payload as ONE struct column, and two writer
  -- schema versions produce two incompatible struct types that cannot share a table.
  CONSTRAINT reader_schema_mode_valid CHECK (
    reader_schema_mode IS NULL OR reader_schema_mode IN ('registry_latest', 'pinned_id')
  ),
  -- Catches the most damaging replay mistake: asking for both a start offset and a start
  -- timestamp, where the silently-ignored one is not the one you expected.
  CONSTRAINT rerun_start_exclusive CHECK (
    rerun_starting_offsets IS NULL OR rerun_starting_timestamp IS NULL
  ),
  CONSTRAINT rerun_end_exclusive CHECK (
    rerun_ending_offsets IS NULL OR rerun_ending_timestamp IS NULL
  )
)
USING DELTA
COMMENT 'Operational overrides and replay controls, one row per topic. Support-team editable.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  -- Row-level history for "who changed what, when" without a shadow audit table.
  'delta.enableChangeDataFeed'       = 'true'
);

-- =====================================================================================
-- GRANTS - the support team edits, the job's service principal only reads.
-- Splitting these is what stops an ingestion job being able to disable itself.
-- =====================================================================================

GRANT USE CATALOG ON CATALOG {ops_catalog} TO `ingestion-support`;
GRANT USE SCHEMA  ON SCHEMA  {ops_catalog}.ingestion TO `ingestion-support`;
GRANT SELECT, MODIFY ON TABLE {ops_catalog}.ingestion.ingestion_topic_control TO `ingestion-support`;

GRANT USE CATALOG ON CATALOG {ops_catalog} TO `sp-kafka-ingestion`;
GRANT USE SCHEMA  ON SCHEMA  {ops_catalog}.ingestion TO `sp-kafka-ingestion`;
GRANT SELECT ON TABLE {ops_catalog}.ingestion.ingestion_topic_control TO `sp-kafka-ingestion`;

-- =====================================================================================
-- Seed rows. A topic with no row runs on its YAML defaults, so this is optional - but an
-- explicit row gives support somewhere obvious to look.
-- =====================================================================================

INSERT INTO {ops_catalog}.ingestion.ingestion_topic_control
  (topic_key, enabled, change_reason, updated_by, updated_at)
SELECT * FROM (
  VALUES
    ('vector_patient_events',  true, 'initial onboarding', current_user(), current_timestamp()),
    ('rcm_claim_status',       true, 'initial onboarding', current_user(), current_timestamp()),
    ('antifraud_txn_alerts',   true, 'initial onboarding', current_user(), current_timestamp())
) AS seed(topic_key, enabled, change_reason, updated_by, updated_at)
WHERE NOT EXISTS (
  SELECT 1 FROM {ops_catalog}.ingestion.ingestion_topic_control c WHERE c.topic_key = seed.topic_key
);
