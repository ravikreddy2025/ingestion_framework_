-- =====================================================================================
-- SUPPORT RUNBOOK QUERIES
--
-- Triage order is deliberate: read the audit table BEFORE touching checkpoints and BEFORE
-- considering a replay. Most "the feed is broken" tickets are answered by Q1-Q3 without
-- changing anything.
--
-- The failure scenarios these map to are documented in docs/DESIGN.md.
-- =====================================================================================
-- TEMPLATE - {catalog} and {ops_catalog} are rendered from conf/environments/<env>.yaml
-- by notebooks/00_validate_config. Running these against the wrong environment is exactly
-- what the placeholders exist to prevent.

-- -------------------------------------------------------------------------------------
-- Q1. Did every topic run, and how did it end?
--     One row per topic per layer. This is the morning dashboard query.
-- -------------------------------------------------------------------------------------
SELECT
  topic_key,
  layer,
  max(event_ts)                             AS last_seen,
  sum(record_count)  FILTER (WHERE status = 'COMPLETED') AS records,
  sum(quarantined_count)                    AS quarantined,
  count_if(status = 'FAILED')               AS failures,
  count_if(status = 'SKIPPED')              AS skipped,
  max(CASE WHEN status = 'FAILED' THEN error_message END) AS last_error
FROM {catalog}.audit.stream_audit
WHERE audit_date >= current_date() - INTERVAL 1 DAY
GROUP BY topic_key, layer
ORDER BY failures DESC, topic_key, layer;

-- -------------------------------------------------------------------------------------
-- Q2. WHICH LAYER did it die on?
--     This is the query the per-layer audit rows exist for. A batch_id with a landing
--     COMPLETED but no curated COMPLETED died between the two writes.
-- -------------------------------------------------------------------------------------
SELECT
  batch_id,
  max(CASE WHEN layer = 'landing' THEN status END) AS landing_status,
  max(CASE WHEN layer = 'landing' THEN record_count END) AS landing_rows,
  max(CASE WHEN layer = 'curated' THEN status END) AS curated_status,
  max(CASE WHEN layer = 'curated' THEN record_count END) AS curated_rows,
  max(CASE WHEN layer = 'curated' THEN quarantined_count END) AS quarantined,
  max(CASE WHEN layer = 'stream'  THEN starting_offsets END) AS starting_offsets,
  max(CASE WHEN layer = 'stream'  THEN ending_offsets END)   AS ending_offsets,
  max(error_class) AS error_class,
  left(max(error_message), 400) AS error_head
FROM {catalog}.audit.stream_audit
WHERE topic_key = 'rcm_claim_status'
  AND audit_date >= current_date() - INTERVAL 7 DAYS
GROUP BY batch_id
ORDER BY batch_id DESC
LIMIT 50;

-- -------------------------------------------------------------------------------------
-- Q3. Is this a POISON BATCH? (same batch_id failing across multiple runs)
--     If so the stream is stuck and will never advance on its own - see Q5.
-- -------------------------------------------------------------------------------------
SELECT topic_key, batch_id, layer,
       count(*) AS failed_attempts,
       count(DISTINCT run_id) AS distinct_runs,
       min(event_ts) AS first_failure, max(event_ts) AS latest_failure,
       left(max(error_message), 500) AS error_head
FROM {catalog}.audit.stream_audit
WHERE status = 'FAILED' AND audit_date >= current_date() - INTERVAL 14 DAYS
GROUP BY topic_key, batch_id, layer
HAVING count(DISTINCT run_id) > 1
ORDER BY failed_attempts DESC;

-- -------------------------------------------------------------------------------------
-- Q4. Schema drift, and records the registry could not explain.
-- -------------------------------------------------------------------------------------
SELECT audit_date, explode(writer_schema_ids) AS writer_schema_id, count(*) AS batches
FROM {catalog}.audit.stream_audit
WHERE topic_key = 'rcm_claim_status' AND layer = 'curated'
  AND audit_date >= current_date() - INTERVAL 30 DAYS
GROUP BY audit_date, writer_schema_id
ORDER BY audit_date DESC, writer_schema_id;

SELECT quarantine_reason, writer_schema_id, count(*) AS records,
       min(quarantined_ts) AS first_seen, max(quarantined_ts) AS last_seen,
       min(kafka_offset) AS min_offset, max(kafka_offset) AS max_offset
FROM {catalog}.landing.rcm_claim_status_quarantine
WHERE ingest_date >= current_date() - INTERVAL 7 DAYS
GROUP BY quarantine_reason, writer_schema_id
ORDER BY records DESC;

-- =====================================================================================
-- FIXES - all of these are config or job parameters. None needs a deploy.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Q5. UNBLOCK A STUCK STREAM (poison batch, scenario 2 in docs/DESIGN.md).
--     Prefer registering the missing schema. Use this when that cannot happen quickly.
--     Bad records go to quarantine, the batch completes, the stream drains.
--     Recover them afterwards with the curated replay job (Q7), then set this back to 'fail'.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET on_deser_error = 'quarantine',
    change_reason  = 'INC12345 - unblock stuck stream, schema 5513 not yet registered',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6. EMERGENCY STOP. Takes effect on the next scheduled run. No deploy, no PR.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET enabled = false,
    change_reason = 'INC12345 - upstream producer emitting corrupt payloads',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6b. BULK ON/OFF - several topics at once. At scale (dozens of topics sharing a
--     cluster or a domain), a single incident often means "everything on this cluster",
--     not one topic_key. WHERE topic_key IN (...) is the direct equivalent of Q6 for a
--     known list; the two patterns below cover "by domain" and "by cluster" without
--     typing every topic_key out by hand.
--
--     A missing row is not an error (a newly onboarded topic runs on YAML defaults), but
--     it also means these UPDATEs only affect topics that ALREADY have a control row.
--     Q6c below finds topics with none - seed one first (sql/01, bottom) or add it here.
-- -------------------------------------------------------------------------------------

-- By an explicit list. Safest for an incident: what changed is exactly what you typed.
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET enabled = false,
    change_reason = 'INC12345 - planned maintenance on the rcm cluster',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- By domain or cluster. `domain` is a structural, PR-reviewed value from each topic's
-- config, so it is stable and safe to filter on. There is no `cluster` column on this
-- table by design - cluster is a Git-reviewed structural fact (conf/topics/<key>.yaml),
-- not something support toggles - so filter by `domain` (a reasonable proxy for "which
-- system these topics belong to") or list topic_keys explicitly as above.
UPDATE {ops_catalog}.ingestion.ingestion_topic_control c
SET enabled = false,
    change_reason = 'INC12345 - rcm domain paused for maintenance window',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE c.topic_key IN (
  -- domain is not on the control table - it is read from the shipped audit rows, which is
  -- the cheapest place already carrying it without joining back to conf/.
  SELECT DISTINCT topic_key
  FROM {catalog}.audit.stream_audit
  WHERE domain = 'rcm'
);

-- Re-enable the same set once the incident is closed. Keep the WHERE identical to the
-- disable statement so the "on" list matches the "off" list exactly.
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET enabled = true,
    change_reason = 'INC12345 - maintenance complete, resuming',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- -------------------------------------------------------------------------------------
-- Q6c. Which topics are OFF right now, and for how long? The dashboard query for "did
--     someone leave a topic disabled and forget", which is the main risk of Q6/Q6b.
-- -------------------------------------------------------------------------------------
SELECT topic_key, enabled, change_reason, updated_by, updated_at,
       datediff(current_timestamp(), updated_at) AS days_since_change
FROM {ops_catalog}.ingestion.ingestion_topic_control
WHERE enabled = false
ORDER BY updated_at;

-- -------------------------------------------------------------------------------------
-- Q7. Park a replay intent durably (optional - the replay job's own parameters win, so
--     this is only for a replay that must survive being re-triggered).
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET rerun_id               = 'INC12345',
    rerun_starting_offsets = '{"rcm.claim.status.v2":{"0":45231,"1":44870,"2":45009}}',
    change_reason          = 'INC12345 - replay the 6h consumer gap',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';

-- Clear it once done, so the next incident starts from a clean row.
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET rerun_id = NULL, rerun_starting_offsets = NULL, rerun_starting_timestamp = NULL,
    rerun_ending_offsets = NULL, rerun_ending_timestamp = NULL,
    curated_replay_landing_filter = NULL,
    change_reason = 'INC12345 - replay complete, controls cleared',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';

-- =====================================================================================
-- VERIFICATION
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Q8. Did the replay cover the gap? Replay rows are tagged, so they are separable.
-- -------------------------------------------------------------------------------------
SELECT ingested_via, replay_run_id, kafka_partition,
       min(kafka_offset) AS min_offset, max(kafka_offset) AS max_offset, count(*) AS records
FROM {catalog}.landing.{topic_table}
WHERE topic = 'rcm.claim.status.v2' AND ingest_date >= current_date() - INTERVAL 3 DAYS
GROUP BY ingested_via, replay_run_id, kafka_partition
ORDER BY kafka_partition, ingested_via;

-- Offsets present in landing but missing from curated - i.e. still not parsed.
SELECT l.kafka_partition, count(*) AS unparsed
FROM {catalog}.landing.{topic_table} l
LEFT ANTI JOIN {catalog}.curated.rcm_claim_status c
  ON  c.topic = l.topic AND c.kafka_partition = l.kafka_partition
  AND c.kafka_offset = l.kafka_offset
WHERE l.topic = 'rcm.claim.status.v2' AND l.ingest_date >= current_date() - INTERVAL 3 DAYS
GROUP BY l.kafka_partition
ORDER BY l.kafka_partition;

-- -------------------------------------------------------------------------------------
-- Q9. Duplicate check. Should always return zero rows.
--     If it does not, Delta's idempotent-write markers are not doing their job - read
--     docs/DESIGN.md "Re-runs and duplicates" before doing anything else.
-- -------------------------------------------------------------------------------------
SELECT topic, kafka_partition, kafka_offset, count(*) AS copies
FROM {catalog}.landing.{topic_table}
-- This table holds one topic, so the predicate below is redundant with the table you
-- picked - harmless, and kept because it makes the query self-documenting when copied
-- into an incident channel without the FROM line attached.
WHERE topic = 'rcm.claim.status.v2'
  AND ingest_date >= current_date() - INTERVAL 7 DAYS
GROUP BY topic, kafka_partition, kafka_offset
HAVING count(*) > 1
ORDER BY copies DESC
LIMIT 100;

-- -------------------------------------------------------------------------------------
-- Q10. Who changed the control table, and when?
-- -------------------------------------------------------------------------------------
DESCRIBE HISTORY {ops_catalog}.ingestion.ingestion_topic_control;

SELECT topic_key, enabled, on_deser_error, rerun_id, change_reason, updated_by, updated_at
FROM {ops_catalog}.ingestion.ingestion_topic_control
ORDER BY updated_at DESC;
