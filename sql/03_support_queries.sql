-- =====================================================================================
-- SUPPORT RUNBOOK QUERIES
--
-- Triage order is deliberate: read the audit table BEFORE touching state or a checkpoint,
-- and BEFORE considering a replay. Most "the feed is broken" tickets are answered by
-- Q1-Q4 without changing anything.
--
-- THREE TABLES, THREE JOBS. Know which one you are in before you edit anything:
--
--   {catalog}.audit.ingest_audit              EVIDENCE. What every run did, per layer.
--                                             Read-only for support. Best-effort by
--                                             design (the job never fails over an audit
--                                             write), so a MISSING row is weak evidence -
--                                             never treat it as proof a run did not read.
--   {ops_catalog}.ingestion.ingest_control    CONFIGURATION. What support may change at
--                                             runtime, no deploy. Edit this.
--   {ops_catalog}.ingestion.ingest_state      TRUTH. Where each source actually got to.
--                                             Written by the job only, and support has
--                                             SELECT and nothing more - a hand-edited
--                                             watermark is a silent data-loss incident.
--                                             If you believe one is wrong, say so in the
--                                             incident and get an engineer.
--
-- ONE TABLE, EVERY SOURCE TYPE. The audit table serves Kafka, database and file sources
-- alike, so three of its columns mean different things depending on `source_type`:
--   source_ref       the topic name / SCHEMA.TABLE / path glob
--   position_start   the read boundary - Kafka offsets JSON, a cursor value, a file mark
--   position_end     the upper boundary, same three meanings
--   source_detail    a JSON STRING of whatever that source type wanted recorded
-- Read `source_type` first, then read those four accordingly. Queries that dig into
-- source_detail are marked SOURCE-SPECIFIC and only make sense for the type named on them.
--
-- The failure scenarios these map to are documented in docs/DESIGN.md.
-- =====================================================================================
-- TEMPLATE - {catalog} and {ops_catalog} are rendered from conf/environments/<env>.yaml
-- by notebooks/00_validate_config. Running these against the wrong environment is exactly
-- what the placeholders exist to prevent.

-- -------------------------------------------------------------------------------------
-- Q1. Did every source run, and how did it end?
--     One row per source per layer. This is the morning dashboard query, and it covers
--     every source type at once - which is the point of one shared audit table.
-- -------------------------------------------------------------------------------------
SELECT
  source_type,
  source_key,
  layer,
  max(event_ts)                                          AS last_seen,
  sum(record_count)  FILTER (WHERE status = 'COMPLETED') AS records,
  sum(quarantined_count)                                 AS quarantined,
  count_if(status = 'FAILED')                            AS failures,
  count_if(status = 'SKIPPED')                           AS skipped,
  max(CASE WHEN status = 'FAILED' THEN error_message END) AS last_error
FROM {catalog}.audit.ingest_audit
WHERE audit_date >= current_date() - INTERVAL 1 DAY
GROUP BY source_type, source_key, layer
ORDER BY failures DESC, source_type, source_key, layer;

-- -------------------------------------------------------------------------------------
-- Q1b. Which sources have NOT run? The query above can only show what did.
--     A source that is disabled, or whose schedule is broken, produces no rows at all -
--     and no rows looks identical to "not looked at yet" on a dashboard.
-- -------------------------------------------------------------------------------------
SELECT c.source_key, c.source_type, c.enabled, a.last_seen,
       datediff(current_timestamp(), a.last_seen) AS days_since_last_run
FROM {ops_catalog}.ingestion.ingest_control c
LEFT JOIN (
  SELECT source_key, max(event_ts) AS last_seen
  FROM {catalog}.audit.ingest_audit
  GROUP BY source_key
) a ON a.source_key = c.source_key
WHERE a.last_seen IS NULL OR a.last_seen < current_timestamp() - INTERVAL 1 DAY
ORDER BY c.enabled DESC, a.last_seen NULLS FIRST;

-- -------------------------------------------------------------------------------------
-- Q2. WHICH LAYER did it die on?
--     This is the query the per-layer audit rows exist for. Grouped by run_id rather than
--     batch_id, because batch_id is a streaming microbatch id for one source type and a
--     run_sequence for another - run_id means the same thing for all of them.
-- -------------------------------------------------------------------------------------
SELECT
  run_id,
  min(event_ts)                                              AS started,
  max(CASE WHEN layer = 'run'     THEN status END)           AS run_status,
  max(CASE WHEN layer = 'landing' THEN status END)           AS landing_status,
  max(CASE WHEN layer = 'landing' THEN record_count END)     AS landing_rows,
  max(CASE WHEN layer = 'curated' THEN status END)           AS curated_status,
  max(CASE WHEN layer = 'curated' THEN record_count END)     AS curated_rows,
  max(quarantined_count)                                     AS quarantined,
  max(CASE WHEN layer = 'run' THEN position_start END)       AS read_from,
  max(CASE WHEN layer = 'run' THEN position_end END)         AS read_to,
  max(error_class)                                           AS error_class,
  left(max(error_message), 400)                              AS error_head
FROM {catalog}.audit.ingest_audit
WHERE source_key = 'rcm_claim_status'
  AND audit_date >= current_date() - INTERVAL 7 DAYS
GROUP BY run_id
ORDER BY started DESC
LIMIT 50;

-- A run with a 'run/STARTED' row and no 'run/COMPLETED' or 'run/FAILED' row died between
-- them, hard enough that even the failure row did not get written. Look at the driver log
-- for that run_id next; the audit table has told you everything it can.
SELECT source_key, run_id, min(event_ts) AS started
FROM {catalog}.audit.ingest_audit
WHERE audit_date >= current_date() - INTERVAL 3 DAYS
GROUP BY source_key, run_id
HAVING count_if(layer = 'run' AND status = 'STARTED') > 0
   AND count_if(layer = 'run' AND status IN ('COMPLETED', 'FAILED')) = 0
ORDER BY started DESC;

-- -------------------------------------------------------------------------------------
-- Q3. Is this stuck? (the same failure repeating across runs)
--     If so it will never clear on its own - see Q6.
-- -------------------------------------------------------------------------------------
SELECT source_type, source_key, layer, error_class,
       count(*)               AS failed_rows,
       count(DISTINCT run_id) AS distinct_runs,
       min(event_ts)          AS first_failure,
       max(event_ts)          AS latest_failure,
       left(max(error_message), 500) AS error_head
FROM {catalog}.audit.ingest_audit
WHERE status = 'FAILED' AND audit_date >= current_date() - INTERVAL 14 DAYS
GROUP BY source_type, source_key, layer, error_class
HAVING count(DISTINCT run_id) > 1
ORDER BY failed_rows DESC;

-- -------------------------------------------------------------------------------------
-- Q4. WHERE HAS EACH SOURCE ACTUALLY GOT TO? The state table, not the audit table.
--
--     For a source with no checkpoint this IS the resume point: the next run reads from
--     here. A watermark that has not moved while runs keep completing is the signature of
--     a source that is reading and writing nothing.
--
--     run_sequence is the Delta txnVersion for sources with no microbatch id. It should
--     go up by one per run. If it is not moving, idempotent writes are not protecting
--     anything - stop and get an engineer.
-- -------------------------------------------------------------------------------------
SELECT source_key, state_key, state_value, value_type, updated_at, updated_by_run,
       datediff(current_timestamp(), updated_at) AS days_since_moved
FROM {ops_catalog}.ingestion.ingest_state
ORDER BY source_key, state_key;

-- Did the watermark move on the last run that claimed to complete? Compare the two.
SELECT s.source_key,
       s.state_value                       AS watermark,
       s.updated_by_run                    AS moved_by_run,
       a.latest_completed_run,
       s.updated_by_run = a.latest_completed_run AS moved_on_the_last_run
FROM {ops_catalog}.ingestion.ingest_state s
LEFT JOIN (
  SELECT source_key, max_by(run_id, event_ts) AS latest_completed_run
  FROM {catalog}.audit.ingest_audit
  WHERE layer = 'run' AND status = 'COMPLETED'
  GROUP BY source_key
) a ON a.source_key = s.source_key
WHERE s.state_key = 'watermark'
ORDER BY moved_on_the_last_run, s.source_key;

-- -------------------------------------------------------------------------------------
-- Q5. SOURCE-SPECIFIC (kafka). Schema drift, and records that could not be parsed.
--     Kafka records its writer schema ids in source_detail, which is a JSON string - one
--     column that costs no ALTER TABLE when a fourth source type wants to record
--     something else entirely.
-- -------------------------------------------------------------------------------------
SELECT audit_date,
       get_json_object(source_detail, '$.writer_schema_ids') AS writer_schema_ids,
       count(*) AS runs
FROM {catalog}.audit.ingest_audit
WHERE source_key = 'rcm_claim_status' AND layer = 'curated'
  AND source_detail IS NOT NULL
  AND audit_date >= current_date() - INTERVAL 30 DAYS
GROUP BY audit_date, writer_schema_ids
ORDER BY audit_date DESC;

-- The quarantine table itself. Its columns are Kafka's, because a quarantined record
-- keeps the raw bytes of whatever arrived - see sql/02_layer_tables.sql.
SELECT quarantine_reason, writer_schema_id, count(*) AS records,
       min(quarantined_ts) AS first_seen, max(quarantined_ts) AS last_seen,
       min(kafka_offset) AS min_offset, max(kafka_offset) AS max_offset
FROM {catalog}.landing.rcm_claim_status_quarantine
WHERE ingest_date >= current_date() - INTERVAL 7 DAYS
GROUP BY quarantine_reason, writer_schema_id
ORDER BY records DESC;

-- =====================================================================================
-- FIXES - all of these are configuration. None needs a deploy.
--
-- Two rules for every UPDATE below:
--   * put the incident number in `notes`. Delta's DESCRIBE HISTORY says WHAT changed;
--     only you can say why.
--   * a MISSING row is not an error - a newly onboarded source runs on its YAML - but it
--     also means an UPDATE affects nothing. Q7c finds sources with no row; seed one from
--     the bottom of sql/01_operational_config.sql first.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Q6. UNBLOCK A SOURCE STUCK ON BAD RECORDS.
--     Prefer fixing the cause - registering the missing schema, correcting the upstream
--     file. Use this when that cannot happen quickly: bad records go to quarantine, the
--     run completes, the backlog drains. Recover them afterwards with a replay (Q8), then
--     set this back to FAILFAST.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET failure_mode = 'QUARANTINE',
    notes        = 'INC12345 - unblock stuck source, schema 5513 not yet registered',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6b. Slow a source down without stopping it. batch_limit caps how much one run reads -
--     max offsets per trigger, fetch cap or max files per trigger, depending on the type.
--     The lever for getting a huge backlog through in survivable chunks.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET batch_limit = 50000,
    notes       = 'INC12345 - drain a 3-day backlog in smaller batches',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6c. Anything the named columns above do not cover is a JSON object in
--     source_overrides, and it is VALIDATED against that source type's spec on the next
--     run - an unknown key fails the run with the same error a YAML typo produces, rather
--     than being silently ignored. So a typo here is loud, but it is loud at 3am. Check
--     the key against docs/CONFIGURATION.md before you commit it.
--
--     Structural settings - partitioning, merge keys, target names - are IGNORED here by
--     design, not rejected: they describe what is already on disk and change by PR only.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET source_overrides = '{"trigger": "availableNow"}',
    notes            = 'INC12345 - pin the trigger for one night',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q7. EMERGENCY STOP. Takes effect on the next scheduled run. No deploy, no PR.
--     It stops replays too - disabling is absolute, which is the point.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET enabled = false,
    notes   = 'INC12345 - upstream producer emitting corrupt payloads',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q7b. BULK ON/OFF. At scale an incident usually means "everything from this system", not
--     one source_key. An explicit list is the safest form: what changed is exactly what
--     you typed.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET enabled = false,
    notes   = 'INC12345 - planned maintenance on the rcm systems',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- By source TYPE - "pause every file load while the storage account is migrated".
UPDATE {ops_catalog}.ingestion.ingest_control
SET enabled = false,
    notes   = 'INC12345 - ADLS migration window',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_type = 'file';

-- By domain. `domain` is a structural, PR-reviewed value from each source's config, so it
-- is stable and safe to filter on - but it is deliberately NOT a column on this table
-- (support does not own it). Read it from the audit rows, which already carry it.
UPDATE {ops_catalog}.ingestion.ingest_control c
SET enabled = false,
    notes   = 'INC12345 - rcm domain paused for maintenance window',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE c.source_key IN (
  SELECT DISTINCT source_key FROM {catalog}.audit.ingest_audit WHERE domain = 'rcm'
);

-- Re-enable the same set once the incident is closed. Keep the WHERE identical to the
-- disable statement, so the "on" list matches the "off" list exactly.
UPDATE {ops_catalog}.ingestion.ingest_control
SET enabled = true,
    notes   = 'INC12345 - maintenance complete, resuming',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- -------------------------------------------------------------------------------------
-- Q7c. Which sources are OFF right now, and for how long? The dashboard query for "did
--     someone leave a source disabled and forget", which is the main risk of Q7/Q7b.
--     The second half finds sources with NO control row, which the UPDATEs cannot touch.
-- -------------------------------------------------------------------------------------
SELECT source_key, source_type, enabled, failure_mode, notes, updated_by, updated_at,
       datediff(current_timestamp(), updated_at) AS days_since_change
FROM {ops_catalog}.ingestion.ingest_control
WHERE enabled = false
ORDER BY updated_at;

-- Sources that have run but have no control row. Not an error - they run on their YAML -
-- but an UPDATE above would silently affect none of them.
SELECT DISTINCT a.source_key, a.source_type
FROM {catalog}.audit.ingest_audit a
LEFT ANTI JOIN {ops_catalog}.ingestion.ingest_control c ON c.source_key = a.source_key
ORDER BY a.source_key;

-- -------------------------------------------------------------------------------------
-- Q8. Park a replay intent durably. Optional: the replay job's own parameters win, so
--     this is only for a replay that must survive being re-triggered.
--
--     replay_rerun_id is REQUIRED for any replay - it is what isolates the replay's
--     checkpoint and its Delta txnAppId from the primary run's. replay_controls is a JSON
--     object of source-specific bounds, validated on the next run exactly like
--     source_overrides, and it WINS over source_overrides for the same key.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingest_control
SET replay_rerun_id = 'INC12345',
    replay_controls = '{"starting_offsets": "{\"rcm.claim.status.v2\":{\"0\":45231}}"}',
    notes           = 'INC12345 - replay the 6h consumer gap',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- Clear it once done, so the next incident starts from a clean row. Leaving a replay
-- parked means the next scheduled run is a replay.
UPDATE {ops_catalog}.ingestion.ingest_control
SET replay_rerun_id = NULL,
    replay_controls = NULL,
    notes           = 'INC12345 - replay complete, controls cleared',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- =====================================================================================
-- VERIFICATION
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Q9. Did the replay run, and what did it cover? Replay runs are tagged in the audit
--     table by run_type and rerun_id, for every source type.
-- -------------------------------------------------------------------------------------
SELECT run_id, run_type, rerun_id, layer, status, record_count,
       position_start, position_end, event_ts
FROM {catalog}.audit.ingest_audit
WHERE source_key = 'rcm_claim_status' AND rerun_id = 'INC12345'
ORDER BY event_ts;

-- -------------------------------------------------------------------------------------
-- Q10. SOURCE-SPECIFIC (kafka). Did the replay fill the gap in the data itself?
--     Landing rows carry their own provenance, so replayed rows are separable.
-- -------------------------------------------------------------------------------------
SELECT ingested_via, replay_run_id, kafka_partition,
       min(kafka_offset) AS min_offset, max(kafka_offset) AS max_offset, count(*) AS records
FROM {catalog}.landing.{topic_table}
WHERE ingest_date >= current_date() - INTERVAL 3 DAYS
GROUP BY ingested_via, replay_run_id, kafka_partition
ORDER BY kafka_partition, ingested_via;

-- Offsets present in landing but missing from curated - i.e. still not parsed.
SELECT l.kafka_partition, count(*) AS unparsed
FROM {catalog}.landing.{topic_table} l
LEFT ANTI JOIN {catalog}.curated.rcm_claim_status c
  ON  c.topic = l.topic AND c.kafka_partition = l.kafka_partition
  AND c.kafka_offset = l.kafka_offset
WHERE l.ingest_date >= current_date() - INTERVAL 3 DAYS
GROUP BY l.kafka_partition
ORDER BY l.kafka_partition;

-- -------------------------------------------------------------------------------------
-- Q11. SOURCE-SPECIFIC (kafka). Duplicate check. Should always return zero rows.
--     If it does not, Delta's idempotent-write markers are not doing their job - check
--     Q4's run_sequence first, then read docs/DESIGN.md "Re-runs and duplicates" before
--     doing anything else.
-- -------------------------------------------------------------------------------------
SELECT topic, kafka_partition, kafka_offset, count(*) AS copies
FROM {catalog}.landing.{topic_table}
WHERE ingest_date >= current_date() - INTERVAL 7 DAYS
GROUP BY topic, kafka_partition, kafka_offset
HAVING count(*) > 1
ORDER BY copies DESC
LIMIT 100;

-- -------------------------------------------------------------------------------------
-- Q12. Who changed what, and when? Delta's own history is the backstop for the control
--     table, and Change Data Feed is on for both operational tables (sql/01).
-- -------------------------------------------------------------------------------------
DESCRIBE HISTORY {ops_catalog}.ingestion.ingest_control;

SELECT source_key, source_type, enabled, failure_mode, batch_limit,
       replay_rerun_id, source_overrides, notes, updated_by, updated_at
FROM {ops_catalog}.ingestion.ingest_control
ORDER BY updated_at DESC;

-- Every watermark move, with the run that made it. The audit table cannot answer this -
-- its writes are best-effort - which is why state is a separate table.
SELECT source_key, state_key, state_value, updated_by_run, updated_at, _change_type
FROM table_changes('{ops_catalog}.ingestion.ingest_state', 0)
WHERE state_key = 'watermark' AND _change_type != 'update_preimage'
ORDER BY updated_at DESC
LIMIT 200;
