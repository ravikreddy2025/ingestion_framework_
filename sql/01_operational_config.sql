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
-- {ops_catalog}, {control_schema} and {logs_schema} are placeholders, using the same
-- convention as conf/. Render this for a target environment with notebooks/00_validate_config
-- (section "Render the provisioning SQL"), which substitutes them from
-- conf/environments/<env>.yaml so this file cannot drift from what the code reads. Do NOT
-- hand-edit a copy per environment.

CREATE SCHEMA IF NOT EXISTS {ops_catalog}.{control_schema}
  COMMENT 'Operational control and durable state for the multi-source ingestion framework';

-- Reserved for structured logging - nothing is written here yet (D-06). Created now, empty,
-- so the shape is visible in a PR and Terraform can grant on it ahead of actually needing to.
CREATE SCHEMA IF NOT EXISTS {ops_catalog}.{logs_schema}
  COMMENT 'Reserved for structured logging. Nothing is written here yet.';

-- =====================================================================================
-- CONTROL - layer 4 of the five-layer configuration. One row per source_key.
--
-- FRAMEWORK-OWNED COLUMNS mean the exact same thing for every source type: source_key,
-- source_type, enabled, replay_rerun_id, replay_controls, notes, updated_by, updated_at.
--
-- SOURCE-TYPE-OWNED COLUMNS are named `<source_type>_<setting>` (docs/build_log/
-- DECISIONS.md D-01) because this ONE table is shared by every source type, and a bare
-- `failure_mode` or `batch_limit` would mean a different mechanism depending on which row
-- you were looking at. Each source type's own SOURCE_SPEC.control_columns declares which
-- of these columns it owns; setting one for a row of the WRONG source type is a run-time
-- error, not a silent no-op - see framework/control.py.
--
-- Adding a source type's columns is a deploy (new package, job template, conf files)
-- already, so it may also need an ALTER TABLE ADD COLUMNS here. That is honest, not a
-- regression: there is no second, untyped mechanism (the former source_overrides JSON) for
-- a source-specific setting to hide in instead.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS {ops_catalog}.{control_schema}.ingest_control (
  source_key                   STRING  NOT NULL COMMENT 'Matches conf/sources/<source_key>.yaml. Unique across ALL source types.',
  source_type                  STRING  COMMENT 'kafka | oracle | file. Checked against the source''s own declaration.',

  -- ---- behaviour toggles, framework-owned ------------------------------------------
  enabled                      BOOLEAN COMMENT 'FALSE = emergency stop. The job runs, reads nothing, writes a SKIPPED audit row.',

  -- ---- kafka-only levers --------------------------------------------------------------
  -- See sources/kafka/spec.py SOURCE_SPEC.control_columns for which setting each column maps to.
  kafka_failure_mode            STRING  COMMENT 'FAILFAST | QUARANTINE. Kafka''s lever for unblocking a source stuck on bad records.',
  kafka_max_offsets_per_trigger BIGINT  COMMENT 'Caps how many offsets one microbatch reads.',
  -- Bypasses Kafka's checkpoint-reset guard AND forks the Delta txnAppId, so a restart
  -- cannot collide with versions already committed under the old identity. Set this ONLY
  -- after confirming the checkpoint is genuinely gone. Put the incident number here, e.g.
  -- 'INC12345'. Do NOT blank it back out once used: reverting to NULL reverts the
  -- txnAppId to the original lineage, which still carries the OLD watermark and would
  -- silently skip every write again.
  kafka_checkpoint_reset_id     STRING  COMMENT 'Incident use only. Never clear after use.',

  -- ---- oracle-only levers -------------------------------------------------------------
  -- See sources/oracle/spec.py SOURCE_SPEC.control_columns for which setting each maps to.
  -- The first two change only how hard the extract leans on the source database, never
  -- which rows it returns - that is what makes them safe to turn without a PR. The third
  -- is the deliberate exception and carries its own note below. Everything that decides
  -- what the increment MEANS (source_schema, source_table, filter_criteria, merge_keys,
  -- cursor_column, cursor_type) stays structural and an override of it is ignored.
  oracle_fetch_size             INT     COMMENT 'JDBC rows per round trip. The driver''s own default is TEN. Lower it when rows are wide.',
  oracle_num_partitions         INT     COMMENT 'Parallel JDBC connections. Needs a partition_column in the source file; 1 means a serial read.',
  -- THE FULL-VS-DELTA SWITCH (docs/build_log/DECISIONS.md D-09). The one lever that
  -- changes WHICH ROWS an Oracle run extracts, and it is operational because it is a
  -- recovery action: a delta load that has been skipping rows is repaired by one full
  -- load, and waiting for a PR to merge is the wrong shape of answer at 3am.
  --
  -- SWITCHING TO 'full' DUPLICATES ROWS on any source whose merge_keys are waived
  -- (`merge_keys: []`), because that source appends. Where merge_keys are set the MERGE
  -- absorbs the re-read and the switch is free. Check which you have before setting this.
  --
  -- A full run does NOT advance or clear the watermark, so switching back to 'cursor'
  -- resumes from the last genuine delta boundary.
  oracle_incremental_mode       STRING  COMMENT 'full | cursor | filter. Recovery lever. Duplicates rows where merge_keys are waived.',

  -- ---- replay controls, framework-owned ------------------------------------------------
  -- Populated to park a replay intent durably. The replay JOB PARAMETERS win over these,
  -- so an urgent one-off needs no UPDATE first. Clear these once the replay is done.
  replay_rerun_id              STRING  COMMENT 'REQUIRED for any replay. Isolates the checkpoint AND the Delta txnAppId.',
  replay_controls              STRING  COMMENT 'JSON object of source-specific replay settings, e.g. {"starting_offsets": "{...}"}',

  -- ---- attribution, framework-owned -----------------------------------------------------
  notes                        STRING  COMMENT 'Why this row was last changed. Incident number belongs here.',
  updated_by                   STRING,
  updated_at                   TIMESTAMP,

  CONSTRAINT source_key_present CHECK (source_key IS NOT NULL),
  CONSTRAINT kafka_failure_mode_valid CHECK (
    kafka_failure_mode IS NULL OR kafka_failure_mode IN ('FAILFAST', 'QUARANTINE')
  ),
  -- Zero is not "the default" for either: sources/oracle/config.py refuses both, and the
  -- constraint refuses them here so the UPDATE fails at the keyboard rather than the run
  -- failing at 3am.
  CONSTRAINT oracle_tuning_positive CHECK (
    (oracle_fetch_size     IS NULL OR oracle_fetch_size     > 0) AND
    (oracle_num_partitions IS NULL OR oracle_num_partitions > 0)
  ),
  CONSTRAINT oracle_incremental_mode_valid CHECK (
    oracle_incremental_mode IS NULL OR oracle_incremental_mode IN ('full', 'cursor', 'filter')
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
-- PARTITIONED BY (source_key), with deletion vectors enabled (docs/build_log/
-- DECISIONS.md D-04). A run sequence is allocated on every run of every source, so with
-- many sources on the same schedule this table takes concurrent MERGEs from different
-- sources; Delta detects conflicts at file granularity, so partitioning by source_key puts
-- each source's rows in disjoint files and stops those MERGEs conflicting with each other.
-- Not CLUSTER BY - Delta allows one or the other, never both, and partitioning is what
-- buys the conflict isolation here. See VB-15 for what is still unverified about this.
--
-- The job creates this table itself if it is missing, so provisioning it here is belt and
-- braces, kept in step with framework/state.py.ensure_state_table by
-- tests/test_shipped_sql.py.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS {ops_catalog}.{control_schema}.ingest_state (
  source_key       STRING    NOT NULL COMMENT 'Matches conf/sources/<source_key>.yaml',
  state_key        STRING    NOT NULL COMMENT 'watermark | run_sequence',
  state_value      STRING    COMMENT 'The value, as text. value_type says how to read it.',
  value_type       STRING    COMMENT 'int | string | whatever the writing source recorded',
  updated_at       TIMESTAMP,
  updated_by_run   STRING    COMMENT 'run_id of the run that last wrote this row',

  CONSTRAINT state_key_present CHECK (source_key IS NOT NULL AND state_key IS NOT NULL)
)
USING DELTA
PARTITIONED BY (source_key)
COMMENT 'Durable ingestion state: watermarks and run sequences. Written by the ingestion job only.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  -- "When did this watermark move, and which run moved it?" is the first question of every
  -- late-data investigation, and CDF answers it without a shadow table.
  'delta.enableChangeDataFeed'       = 'true',
  -- Cheap single-row MERGE updates instead of rewriting a whole file per update - the shape
  -- every write to this table takes. See VB-15.
  'delta.enableDeletionVectors'      = 'true'
);

-- =====================================================================================
-- GRANTS ARE TERRAFORM-OWNED, NOT ISSUED HERE (docs/build_log/DECISIONS.md D-02).
--
-- Neither this file nor the framework GRANTs anything: user groups and service-principal
-- names are environment-specific and belong in Terraform, outside this repository. The
-- exact privileges the ingestion service principal and the support group need - CONTROL:
-- support edits, the job only reads, so an ingestion job can never disable itself. STATE:
-- the reverse, the job writes and support gets SELECT only, because a hand-edited
-- watermark is a silent data-loss incident - are recorded as a specification for whoever
-- writes the Terraform in the "Unity Catalog privileges" table of
-- docs/RUNBOOK_CLIENT_IT.md. See VB-16.
-- =====================================================================================

-- =====================================================================================
-- Seed rows. A source with no control row runs on its YAML defaults, so this is optional -
-- but an explicit row gives support somewhere obvious to look.
-- =====================================================================================

INSERT INTO {ops_catalog}.{control_schema}.ingest_control
  (source_key, source_type, enabled, notes, updated_by, updated_at)
SELECT * FROM (
  VALUES
    ('vector_patient_events', 'kafka', true, 'initial onboarding', current_user(), current_timestamp()),
    ('rcm_claim_status',      'kafka', true, 'initial onboarding', current_user(), current_timestamp()),
    ('antifraud_txn_alerts',  'kafka', true, 'initial onboarding', current_user(), current_timestamp())
) AS seed(source_key, source_type, enabled, notes, updated_by, updated_at)
WHERE NOT EXISTS (
  SELECT 1 FROM {ops_catalog}.{control_schema}.ingest_control c WHERE c.source_key = seed.source_key
);
