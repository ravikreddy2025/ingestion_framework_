-- =====================================================================================
-- OPERATIONAL TABLES - the framework's own two tables in the OPS catalog.
--
--   ingest_control   Tier 2 configuration. Support-team editable, no deploy cycle.
--                    Everything here answers "how should this source behave RIGHT NOW".
--   ingest_state     Durable run state: watermarks and run sequences. Written by the
--                    ingestion job ONLY. Support reads it; support does not edit it.
--
-- Structural facts (which cluster, which connection, which tables, how tables are
-- PARTITIONED) live in Git YAML and are deliberately NOT overridable from ingest_control:
-- a typo in a catalog name or a partition key at 3am should require a PR. The job IGNORES
-- a structural key it finds here rather than failing on it - see framework/control.py.
--
-- Exactly one row per source_key in each table. The job tolerates a MISSING control row
-- (meaning "no overrides") but refuses to run on duplicates.
--
-- Attribution: updated_by / updated_at are set by the support UPDATE templates in
-- sql/03_support_queries.sql. Delta's own DESCRIBE HISTORY is the backstop.
-- =====================================================================================
-- TEMPLATE - NOT READY TO RUN AS-IS.
-- {ops_catalog} is a placeholder, using the same convention as conf/. Render it for a
-- target environment with notebooks/00_validate_config (section "Render the provisioning
-- SQL"), which substitutes it from conf/environments/<env>.yaml so this file cannot drift
-- from what the code reads. Do NOT hand-edit a copy per environment.

CREATE SCHEMA IF NOT EXISTS {ops_catalog}.ingestion
  COMMENT 'Operational control and durable state for the multi-source ingestion framework';

-- =====================================================================================
-- CONTROL - layer 4 of the five-layer configuration. One row per source_key.
--
-- The named columns below are the levers EVERY source type is expected to share. Anything
-- specific to one source type goes in source_overrides as JSON, and is validated against
-- that source's SOURCE_SPEC exactly like a YAML key - an unknown key here fails the run
-- with the same message a YAML typo produces, rather than being silently ignored.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS {ops_catalog}.ingestion.ingest_control (
  source_key                   STRING  NOT NULL COMMENT 'Matches conf/sources/<source_key>.yaml. Unique across ALL source types.',
  source_type                  STRING  COMMENT 'kafka | oracle | file. Checked against the source''s own declaration.',

  -- ---- behaviour toggles ----------------------------------------------------------
  enabled                      BOOLEAN COMMENT 'FALSE = emergency stop. The job runs, reads nothing, writes a SKIPPED audit row.',
  failure_mode                 STRING  COMMENT 'FAILFAST | QUARANTINE. THE lever for unblocking a source stuck on bad records.',
  batch_limit                  BIGINT  COMMENT 'Caps how much one run reads: max offsets per trigger, fetch cap, or max files per trigger.',

  -- ---- checkpoint-reset override (incident use only) -------------------------------
  -- Checkpoint-based sources only. Bypasses the source''s checkpoint-reset guard AND forks
  -- the Delta txnAppId, so a restart cannot collide with versions already committed under
  -- the old identity. Set this ONLY after confirming the checkpoint is genuinely gone.
  -- Put the incident number here, e.g. 'INC12345'. Do NOT blank it back out once used:
  -- reverting to NULL reverts the txnAppId to the original lineage, which still carries the
  -- OLD watermark and would silently skip every write again.
  checkpoint_reset_id          STRING  COMMENT 'Incident use only. Never clear after use.',

  -- ---- replay controls -------------------------------------------------------------
  -- Populated to park a replay intent durably. The replay JOB PARAMETERS win over these,
  -- so an urgent one-off needs no UPDATE first. Clear these once the replay is done.
  replay_rerun_id              STRING  COMMENT 'REQUIRED for any replay. Isolates the checkpoint AND the Delta txnAppId.',
  replay_controls              STRING  COMMENT 'JSON object of source-specific replay settings, e.g. {"starting_offsets": "{...}"}',

  -- ---- everything else -------------------------------------------------------------
  source_overrides             STRING  COMMENT 'JSON object of source-specific operational overrides. Validated against that source type''s spec.',

  -- ---- attribution -----------------------------------------------------------------
  notes                        STRING  COMMENT 'Why this row was last changed. Incident number belongs here.',
  updated_by                   STRING,
  updated_at                   TIMESTAMP,

  CONSTRAINT source_key_present CHECK (source_key IS NOT NULL),
  CONSTRAINT failure_mode_valid CHECK (
    failure_mode IS NULL OR failure_mode IN ('FAILFAST', 'QUARANTINE')
  )
)
USING DELTA
COMMENT 'Operational overrides and replay controls, one row per source. Support-team editable.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  -- Row-level history for "who changed what, when" without a shadow audit table.
  'delta.enableChangeDataFeed'       = 'true'
);

-- =====================================================================================
-- STATE - durable run state. KEY/VALUE ON PURPOSE: a new source concept is a new row, not
-- an ALTER TABLE on a table every environment shares.
--
--   watermark      the last committed cursor for a source with no checkpoint. Advanced
--                  ONLY after the write it covers has committed.
--   run_sequence   a monotonically increasing integer per source_key, used as the Delta
--                  txnVersion by sources with no Spark microbatch id.
--
-- NEVER derive either from the audit table. Audit writes are best-effort by design and
-- must never raise; state writes are mandatory and must. That asymmetry is why these are
-- two tables. See framework/state.py.
--
-- The job creates this table itself if it is missing, so provisioning it here is belt and
-- braces - but doing so is what lets the GRANTs below be in place before the first run.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS {ops_catalog}.ingestion.ingest_state (
  source_key       STRING    NOT NULL COMMENT 'Matches conf/sources/<source_key>.yaml',
  state_key        STRING    NOT NULL COMMENT 'watermark | run_sequence',
  state_value      STRING    COMMENT 'The value, as text. value_type says how to read it.',
  value_type       STRING    COMMENT 'int | string | whatever the writing source recorded',
  updated_at       TIMESTAMP,
  updated_by_run   STRING    COMMENT 'run_id of the run that last wrote this row',

  CONSTRAINT state_key_present CHECK (source_key IS NOT NULL AND state_key IS NOT NULL)
)
USING DELTA
COMMENT 'Durable ingestion state: watermarks and run sequences. Written by the ingestion job only.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  -- "When did this watermark move, and which run moved it?" is the first question of every
  -- late-data investigation, and CDF answers it without a shadow table.
  'delta.enableChangeDataFeed'       = 'true'
);

-- =====================================================================================
-- GRANTS
--
-- CONTROL: the support team edits, the job's service principal only reads. Splitting these
-- is what stops an ingestion job being able to disable itself.
-- STATE: the reverse. The job writes; support reads. A hand-edited watermark is a silent
-- data-loss incident, so support gets SELECT and nothing more.
-- =====================================================================================

GRANT USE CATALOG ON CATALOG {ops_catalog} TO `ingestion-support`;
GRANT USE SCHEMA  ON SCHEMA  {ops_catalog}.ingestion TO `ingestion-support`;
GRANT SELECT, MODIFY ON TABLE {ops_catalog}.ingestion.ingest_control TO `ingestion-support`;
GRANT SELECT ON TABLE {ops_catalog}.ingestion.ingest_state TO `ingestion-support`;

GRANT USE CATALOG ON CATALOG {ops_catalog} TO `sp-kafka-ingestion`;
GRANT USE SCHEMA  ON SCHEMA  {ops_catalog}.ingestion TO `sp-kafka-ingestion`;
GRANT SELECT ON TABLE {ops_catalog}.ingestion.ingest_control TO `sp-kafka-ingestion`;
GRANT SELECT, MODIFY ON TABLE {ops_catalog}.ingestion.ingest_state TO `sp-kafka-ingestion`;

-- The job creates ingest_state (and the audit table) on first run if provisioning has not
-- run. That needs CREATE TABLE on the schema; drop this grant if you would rather the job
-- fail loudly on an unprovisioned environment. See VB-16.
GRANT CREATE TABLE ON SCHEMA {ops_catalog}.ingestion TO `sp-kafka-ingestion`;

-- =====================================================================================
-- Seed rows. A source with no control row runs on its YAML defaults, so this is optional -
-- but an explicit row gives support somewhere obvious to look.
-- =====================================================================================

INSERT INTO {ops_catalog}.ingestion.ingest_control
  (source_key, source_type, enabled, notes, updated_by, updated_at)
SELECT * FROM (
  VALUES
    ('vector_patient_events', 'kafka', true, 'initial onboarding', current_user(), current_timestamp()),
    ('rcm_claim_status',      'kafka', true, 'initial onboarding', current_user(), current_timestamp()),
    ('antifraud_txn_alerts',  'kafka', true, 'initial onboarding', current_user(), current_timestamp())
) AS seed(source_key, source_type, enabled, notes, updated_by, updated_at)
WHERE NOT EXISTS (
  SELECT 1 FROM {ops_catalog}.ingestion.ingest_control c WHERE c.source_key = seed.source_key
);
