-- =====================================================================================
-- TABLE MAINTENANCE - run on a schedule by resources/job_maintenance.yml.
--
-- Nothing in this framework prunes or compacts anything on its own. `delta.autoOptimize`
-- (set on every table framework/tables.py creates) compacts small files as they are written,
-- but it does
-- not bound total size and it does not remove the superseded files that every MERGE,
-- DELETE and OPTIMIZE leaves behind. That is what this file is for.
--
-- THE TWO RETENTIONS - DO NOT MERGE THEM
-- --------------------------------------
--   landing_retention_days   how long a landing ROW is kept        default 7300 (20 years)
--   vacuum_retention_hours   how long superseded FILES are kept    default 168 (7 days)
--
-- The first is a data-retention policy. The second is time-travel depth: raising it to
-- match the first would retain every version of every file for twenty years and make
-- storage cost unbounded. They are unrelated numbers.
--
-- PARAMETERS are supplied by the job definition. :landing_table is a full 3-tier name and
-- differs per task - landing is one table per topic, so the job runs this file once per
-- topic. The retention values come from bundle variables, so they live in databricks.yml
-- and can differ per environment.
--
-- Note this file uses SQL :parameters, not the {placeholder} convention of sql/01-03. Those
-- three are provisioning scripts a human renders and runs once; this one is executed by a
-- job, which can bind parameters directly. Rendering it would just add a manual step.
--
-- :ops_catalog locates the audit table, which lives under the ops catalog, not :catalog
-- (docs/build_log/DECISIONS.md D-06). Its schema is the literal 'audit' rather than a
-- third bound parameter, because every environment uses that name (see
-- conf/environments/<env>.yaml vars.audit_schema) and this file already carries enough
-- parameters. resources/job_maintenance.yml binds it from the ${var.ops_catalog} bundle
-- variable, and tests/test_shipped_config.py asserts that variable and the environment
-- file agree.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- 1. Compact. Landing is the one that needs it most: it is append-only and written by a
--    streaming query several times a day.
--    Partition-scoped to recent data - re-optimising years of settled partitions every
--    night is expensive and changes nothing.
-- -------------------------------------------------------------------------------------
OPTIMIZE IDENTIFIER(:landing_table)
  WHERE ingest_date >= current_date() - INTERVAL 7 DAYS;

-- The audit table is shared by every topic, so this repeats across per-topic tasks. That is
-- harmless: OPTIMIZE on already-compacted files is close to a no-op.
OPTIMIZE IDENTIFIER(:ops_catalog || '.audit.ingest_audit')
  WHERE audit_date >= current_date() - INTERVAL 7 DAYS;

-- -------------------------------------------------------------------------------------
-- 2. Reclaim storage from superseded files.
--    VACUUM below Delta's 7-day floor requires disabling a safety check and can break a
--    concurrent reader. If you think you need that, you want a different fix.
-- -------------------------------------------------------------------------------------
VACUUM IDENTIFIER(:landing_table) RETAIN :vacuum_retention_hours HOURS;
VACUUM IDENTIFIER(:ops_catalog || '.audit.ingest_audit')   RETAIN :vacuum_retention_hours HOURS;

-- -------------------------------------------------------------------------------------
-- 3. LANDING RETENTION - deliberately NOT enabled.
--
-- The policy is 20 years, so at the default this statement would delete nothing for two
-- decades. It is here so the policy is expressed, parameterised and reviewable now, not
-- because it has work to do today.
--
-- Enabling automatic deletion of raw payloads is a decision for whoever owns the data, not
-- something a maintenance job should start doing on its own. Two things to settle first:
--
--   * does the same 20 years apply to the QUARANTINE tables? They retain full raw payloads
--     too, so the same reasoning applies - but it was never stated.
--   * does it apply to CURATED? Curated is derived and can be rebuilt from landing, so it
--     may warrant a shorter window rather than the same one.
--
-- The delete is cheap when it does run: ingest_date is the landing partition key, so this
-- drops whole partitions rather than rewriting files.
--
--   DELETE FROM IDENTIFIER(:landing_table)
--   WHERE ingest_date < current_date() - make_interval(0, 0, 0, :landing_retention_days);
--
-- Verify what it WOULD remove before enabling it - this is safe to run any time:
-- -------------------------------------------------------------------------------------
SELECT
  count(*)                                    AS rows_past_retention,
  min(ingest_date)                            AS oldest,
  max(ingest_date)                            AS newest_past_retention,
  count(DISTINCT topic)                       AS topics_affected
FROM IDENTIFIER(:landing_table)
WHERE ingest_date < current_date() - make_interval(0, 0, 0, :landing_retention_days);
