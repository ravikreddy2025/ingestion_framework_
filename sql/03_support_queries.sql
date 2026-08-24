-- =====================================================================================
-- SUPPORT RUNBOOK QUERIES
--
-- Triage order is deliberate: read the audit table BEFORE touching state or a checkpoint,
-- and BEFORE considering a replay. Most "the feed is broken" tickets are answered by
-- Q1-Q4 without changing anything.
--
-- THREE TABLES, THREE JOBS. Know which one you are in before you edit anything:
--
--   {ops_catalog}.{audit_schema}.ingest_audit   EVIDENCE. What every run did, per layer.
--                                             Read-only for support. Best-effort by
--                                             design (the job never fails over an audit
--                                             write), so a MISSING row is weak evidence -
--                                             never treat it as proof a run did not read.
--   {ops_catalog}.{control_schema}.ingest_control    CONFIGURATION. What support may
--                                             change at runtime, no deploy. Edit this.
--   {ops_catalog}.{control_schema}.ingest_state      TRUTH. Where each source actually got
--                                             to. Written by the job only, and support has
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
-- ingest_control ITSELF is also shared by every source type, and for the same reason a
-- column specific to one type is named `<source_type>_<setting>` rather than bare
-- (docs/build_log/DECISIONS.md D-01) - `kafka_failure_mode` is not `failure_mode`, because
-- a bare name on a table with an oracle row and a file row beside it would not say which
-- mechanism it turns. Queries below that set or read one are marked SOURCE-SPECIFIC too.
--
-- The failure scenarios these map to are documented in docs/DESIGN.md.
-- =====================================================================================
-- TEMPLATE - {catalog}, {ops_catalog}, {audit_schema} and {control_schema} are rendered
-- from conf/environments/<env>.yaml by notebooks/00_validate_config. Running these against
-- the wrong environment is exactly what the placeholders exist to prevent.

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
FROM {ops_catalog}.{audit_schema}.ingest_audit
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
FROM {ops_catalog}.{control_schema}.ingest_control c
LEFT JOIN (
  SELECT source_key, max(event_ts) AS last_seen
  FROM {ops_catalog}.{audit_schema}.ingest_audit
  GROUP BY source_key
) a ON a.source_key = c.source_key
WHERE a.last_seen IS NULL OR a.last_seen < current_timestamp() - INTERVAL 1 DAY
ORDER BY c.enabled DESC, a.last_seen NULLS FIRST;

-- -------------------------------------------------------------------------------------
-- Q2. WHICH LAYER did it die on?
--     This is the query the per-layer audit rows exist for. Grouped by run_id rather than
--     txn_version, because txn_version is a streaming microbatch id for one source type
--     and a run_sequence for another - run_id means the same thing for all of them.
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
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE source_key = 'rcm_claim_status'
  AND audit_date >= current_date() - INTERVAL 7 DAYS
GROUP BY run_id
ORDER BY started DESC
LIMIT 50;

-- A run with a 'run/STARTED' row and no 'run/COMPLETED' or 'run/FAILED' row died between
-- them, hard enough that even the failure row did not get written. Look at the driver log
-- for that run_id next; the audit table has told you everything it can.
SELECT source_key, run_id, min(event_ts) AS started
FROM {ops_catalog}.{audit_schema}.ingest_audit
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
FROM {ops_catalog}.{audit_schema}.ingest_audit
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
FROM {ops_catalog}.{control_schema}.ingest_state
ORDER BY source_key, state_key;

-- Did the watermark move on the last run that claimed to complete? Compare the two.
SELECT s.source_key,
       s.state_value                       AS watermark,
       s.updated_by_run                    AS moved_by_run,
       a.latest_completed_run,
       s.updated_by_run = a.latest_completed_run AS moved_on_the_last_run
FROM {ops_catalog}.{control_schema}.ingest_state s
LEFT JOIN (
  SELECT source_key, max_by(run_id, event_ts) AS latest_completed_run
  FROM {ops_catalog}.{audit_schema}.ingest_audit
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
FROM {ops_catalog}.{audit_schema}.ingest_audit
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
-- Q6. SOURCE-SPECIFIC (kafka). UNBLOCK A SOURCE STUCK ON BAD RECORDS.
--     Prefer fixing the cause - registering the missing schema, correcting the upstream
--     file. Use this when that cannot happen quickly: bad records go to quarantine, the
--     run completes, the backlog drains. Recover them afterwards with a replay (Q8), then
--     set this back to FAILFAST.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET kafka_failure_mode = 'QUARANTINE',
    notes              = 'INC12345 - unblock stuck source, schema 5513 not yet registered',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6b. SOURCE-SPECIFIC (kafka). Slow a source down without stopping it.
--     kafka_max_offsets_per_trigger caps how many offsets one microbatch reads - the
--     lever for getting a huge backlog through in survivable chunks. Oracle and file
--     sources have their own equivalent columns once Stage 4/5 add them.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET kafka_max_offsets_per_trigger = 50000,
    notes                        = 'INC12345 - drain a 3-day backlog in smaller batches',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6c. SOURCE-SPECIFIC (kafka). CHECKPOINT-RESET, incident use only.
--     Every operationally-overridable setting now has its own named, typed column on this
--     table - there is no more free-form JSON escape hatch for a source-specific setting
--     (docs/build_log/DECISIONS.md D-01 removed the old `source_overrides` column). A
--     setting with no dedicated column simply cannot be changed from here; that is a
--     decision made when the column list was designed, not an oversight to work around.
--     Setting the wrong source type's column for this row fails loudly, naming both.
--
--     kafka_checkpoint_reset_id bypasses Kafka's checkpoint-reset guard AND forks the
--     Delta txnAppId - use ONLY after confirming the checkpoint is genuinely gone.
--
--     THE ID MUST BE ONE THAT HAS NEVER BEEN USED FOR THIS SOURCE. Reusing one keeps the
--     OLD transaction identity, against which Delta already holds high versions, so every
--     write would be skipped as a duplicate and the run would report success having
--     ingested nothing. The job REFUSES to start in that state and names the id - but
--     check with Q6d first rather than finding out from a failed run.
--
--     Do NOT clear the field afterwards: it self-neutralises once the checkpoint exists
--     again, and reverting it would resurrect the exact collision it was set to avoid.
--     Full procedure: docs/RUNBOOK_SUPPORT.md 5.4a.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET kafka_checkpoint_reset_id = 'INC12345',
    notes                     = 'INC12345 - checkpoint lost in a workspace migration, confirmed gone',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q6d. SOURCE-SPECIFIC (kafka). Has this reset id been used before? RUN THIS FIRST.
--     A primary run carrying a reset id records it in rerun_id, which is otherwise NULL
--     on a primary run - so run_type tells the two meanings of that column apart. Any row
--     here means that id is spent: use the current incident's id instead.
-- -------------------------------------------------------------------------------------
SELECT run_id, rerun_id, min(event_ts) AS first_used, max(event_ts) AS last_used
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE source_key = 'rcm_claim_status'
  AND run_type   = 'primary'
  AND rerun_id IS NOT NULL
GROUP BY run_id, rerun_id
ORDER BY last_used DESC;

-- -------------------------------------------------------------------------------------
-- Q7. EMERGENCY STOP. Takes effect on the next scheduled run. No deploy, no PR.
--     It stops replays too - disabling is absolute, which is the point.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = false,
    notes   = 'INC12345 - upstream producer emitting corrupt payloads',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- -------------------------------------------------------------------------------------
-- Q7b. BULK ON/OFF. At scale an incident usually means "everything from this system", not
--     one source_key. An explicit list is the safest form: what changed is exactly what
--     you typed.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = false,
    notes   = 'INC12345 - planned maintenance on the rcm systems',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- By source TYPE - "pause every file load while the storage account is migrated".
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = false,
    notes   = 'INC12345 - ADLS migration window',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_type = 'file';

-- By domain. `domain` is a structural, PR-reviewed value from each source's config, so it
-- is stable and safe to filter on - but it is deliberately NOT a column on this table
-- (support does not own it). Read it from the audit rows, which already carry it.
UPDATE {ops_catalog}.{control_schema}.ingest_control c
SET enabled = false,
    notes   = 'INC12345 - rcm domain paused for maintenance window',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE c.source_key IN (
  SELECT DISTINCT source_key FROM {ops_catalog}.{audit_schema}.ingest_audit WHERE domain = 'rcm'
);

-- Re-enable the same set once the incident is closed. Keep the WHERE identical to the
-- disable statement, so the "on" list matches the "off" list exactly.
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = true,
    notes   = 'INC12345 - maintenance complete, resuming',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key IN ('rcm_claim_status', 'rcm_prior_auth', 'rcm_eligibility_check');

-- -------------------------------------------------------------------------------------
-- Q7c. Which sources are OFF right now, and for how long? The dashboard query for "did
--     someone leave a source disabled and forget", which is the main risk of Q7/Q7b.
--     The second half finds sources with NO control row, which the UPDATEs cannot touch.
--     `enabled` is the only behaviour toggle every source type shares - a per-type lever
--     like kafka_failure_mode is only meaningful for that type's own rows; SELECT * to see
--     every column, including the ones NULL here because they belong to a different type.
-- -------------------------------------------------------------------------------------
SELECT source_key, source_type, enabled, notes, updated_by, updated_at,
       datediff(current_timestamp(), updated_at) AS days_since_change
FROM {ops_catalog}.{control_schema}.ingest_control
WHERE enabled = false
ORDER BY updated_at;

-- Sources that have run but have no control row. Not an error - they run on their YAML -
-- but an UPDATE above would silently affect none of them.
SELECT DISTINCT a.source_key, a.source_type
FROM {ops_catalog}.{audit_schema}.ingest_audit a
LEFT ANTI JOIN {ops_catalog}.{control_schema}.ingest_control c ON c.source_key = a.source_key
ORDER BY a.source_key;

-- -------------------------------------------------------------------------------------
-- Q8. Park a replay intent durably. Optional: the replay job's own parameters win, so
--     this is only for a replay that must survive being re-triggered.
--
--     replay_rerun_id is REQUIRED for any replay - it is what isolates the replay's
--     checkpoint and its Delta txnAppId from the primary run's. replay_controls is a JSON
--     object of source-specific bounds, validated on the next run against that source
--     type's spec, and it WINS over a named control column for the same setting.
-- -------------------------------------------------------------------------------------
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET replay_rerun_id = 'INC12345',
    replay_controls = '{"starting_offsets": "{\"rcm.claim.status.v2\":{\"0\":45231}}"}',
    notes           = 'INC12345 - replay the 6h consumer gap',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';

-- Clear it once done, so the next incident starts from a clean row. Leaving a replay
-- parked means the next scheduled run is a replay.
UPDATE {ops_catalog}.{control_schema}.ingest_control
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
FROM {ops_catalog}.{audit_schema}.ingest_audit
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
--     SELECT * to see every source type's own columns - this lists only the
--     framework-owned ones, which is what every row has in common.
-- -------------------------------------------------------------------------------------
DESCRIBE HISTORY {ops_catalog}.{control_schema}.ingest_control;

SELECT source_key, source_type, enabled,
       replay_rerun_id, replay_controls, notes, updated_by, updated_at
FROM {ops_catalog}.{control_schema}.ingest_control
ORDER BY updated_at DESC;

-- Every watermark move, with the run that made it. The audit table cannot answer this -
-- its writes are best-effort - which is why state is a separate table.
SELECT source_key, state_key, state_value, updated_by_run, updated_at, _change_type
FROM table_changes('{ops_catalog}.{control_schema}.ingest_state', 0)
WHERE state_key = 'watermark' AND _change_type != 'update_preimage'
ORDER BY updated_at DESC
LIMIT 200;

-- =====================================================================================
-- STANDING HEALTH CHECKS - not incident queries. Run these on a schedule, or read them in
-- the weekly review. Each answers a question that has no failure attached to it, which is
-- exactly why nobody thinks to ask it.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Q13. LAST COMMITTED END OFFSET PER PARTITION. Read this BEFORE any checkpoint reset.
--
--     A reset restarts the stream from `latest`, so these offsets are the LOWER BOUND of
--     the gap the reset leaves behind - record them, then backfill with a bounded Kafka
--     replay starting exactly here. This is step 1 of docs/RUNBOOK_SUPPORT.md 5.4a, and
--     the rest of that procedure is worthless without it.
--
--     position_end holds a Kafka offsets JSON for a kafka source. The same column means a
--     cursor value for a database source and a file boundary for a file source, so read
--     source_type before reading this.
-- -------------------------------------------------------------------------------------
WITH last_completed AS (
  SELECT position_end,
         row_number() OVER (ORDER BY event_ts DESC) AS recency
  FROM {ops_catalog}.{audit_schema}.ingest_audit
  WHERE source_key = 'rcm_claim_status'
    AND layer      = 'stream'
    AND status     = 'COMPLETED'
    AND position_end IS NOT NULL
)
SELECT offsets.key   AS kafka_partition,
       offsets.value AS last_committed_end_offset
FROM last_completed
LATERAL VIEW explode(
  from_json(get_json_object(position_end, '$["rcm.claim.status.v2"]'), 'map<string,bigint>')
) AS offsets
WHERE recency = 1
ORDER BY int(offsets.key);

-- -------------------------------------------------------------------------------------
-- Q14. WHICH SOURCES ARE RUNNING WITH DATA-LOSS PROTECTION DISABLED?
--
--     fail_on_data_loss is STRUCTURAL - it lives in Git, not in this table - so it cannot
--     be read from ingest_control and cannot be changed from here. It is recorded on every
--     audit row instead, inside source_detail, precisely so this question is answerable
--     without reading a Git branch that may since have moved on.
--
--     `false` means the source is allowed to skip records Kafka aged out before it read
--     them: gaps become silent. That is a legitimate, signed-off setting for some feeds and
--     a forgotten incident workaround for others, and the only way to tell is to ask the
--     domain. Anything on this list with no owner behind it is a finding.
-- -------------------------------------------------------------------------------------
SELECT source_type,
       source_key,
       max(event_ts) AS last_seen,
       get_json_object(max_by(source_detail, event_ts), '$.fail_on_data_loss') AS fail_on_data_loss
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE layer = 'run'
  AND audit_date >= current_date() - INTERVAL 7 DAYS
  AND source_detail IS NOT NULL
GROUP BY source_type, source_key
HAVING lower(fail_on_data_loss) = 'false'
ORDER BY source_key;

-- -------------------------------------------------------------------------------------
-- Q15. WHICH SOURCES LOST MORE THAN 5% OF THEIR LAST RUN TO QUARANTINE OR RESCUE?
--
--     A stream in QUARANTINE mode does not fail - that is the point, and it is also why a
--     rising quarantine rate stays invisible until someone opens the quarantine table. The
--     count is on the audit row, so this needs neither.
--
--     5% is a starting threshold, not a validated one: tune it per domain once there is a
--     baseline. A source that normally sits at 0 and moves to 1% is a finding this misses.
-- -------------------------------------------------------------------------------------
WITH last_run AS (
  SELECT source_type, source_key, run_id, record_count, quarantined_count, event_ts,
         row_number() OVER (PARTITION BY source_key ORDER BY event_ts DESC) AS recency
  FROM {ops_catalog}.{audit_schema}.ingest_audit
  WHERE layer = 'run' AND status = 'COMPLETED'
    AND quarantined_count IS NOT NULL
    AND audit_date >= current_date() - INTERVAL 7 DAYS
)
SELECT source_type, source_key, run_id, event_ts,
       record_count, quarantined_count,
       round(100.0 * quarantined_count / nullif(record_count, 0), 2) AS pct_quarantined
FROM last_run
WHERE recency = 1
  AND quarantined_count > 0.05 * record_count
ORDER BY pct_quarantined DESC;

-- -------------------------------------------------------------------------------------
-- Q16. IS ANY SOURCE PERMANENTLY BEHIND?
--
--     pending_work is what was STILL OUTSTANDING when the run ended: Kafka lag for a
--     streaming source, unprocessed files for a file source, NULL for a source that cannot
--     cheaply know. NULL IS NOT ZERO and must not be read as "caught up" - for Kafka it
--     means the runtime reported no latest offset (VB-05).
--
--     One run ending behind is normal on a busy topic. The same source ending behind on
--     every run, with the figure GROWING, is a source that will never catch up.
-- -------------------------------------------------------------------------------------
SELECT source_type, source_key,
       count(*)                       AS runs,
       max(pending_work)              AS worst,
       max_by(pending_work, event_ts) AS most_recent,
       count_if(pending_work IS NULL) AS runs_that_could_not_tell
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE layer = 'run' AND status = 'COMPLETED'
  AND audit_date >= current_date() - INTERVAL 7 DAYS
GROUP BY source_type, source_key
HAVING max(pending_work) > 0
ORDER BY most_recent DESC;
