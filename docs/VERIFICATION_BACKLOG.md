# Verification Backlog

Assumptions this codebase makes that cannot be checked from a local Python environment --
no Kafka, no Oracle, no ADLS, no Databricks workspace, no JVM. Each entry names the exact
command, query or notebook cell a human runs on real infrastructure to confirm or refute the
assumption, and what to change in the code if it turns out wrong.

**Ordering: most damaging first.** "Damaging" is read as *how badly and how invisibly* a
wrong guess would hurt, not how likely the guess is to be wrong. A wrong type mapping that
silently corrupts a column ranks above a missing network route that fails loudly on the
first run, because the second one gets fixed on day one and the first one gets fixed after
a downstream team notices bad numbers. Entries VB-01 -- VB-04 concern the Oracle source
(Stage 4) and VB-06 -- VB-07 concern the Files source (Stage 5); they are seeded now because
CORE.md section 3 asks for all thirteen up front, before either source is built.

Status key: OPEN (not yet checked) / CONFIRMED (checked, assumption held) / REFUTED (checked,
code must change -- see "If it fails").

---

### VB-02 -- What Spark type does Oracle `NUMBER` without precision/scale map to on the target DBR and driver?
- **Stage / file:** Stage 4 (Oracle). Will govern `sources/oracle/types.py` type mapping.
- **Why it matters:** `NUMBER` with no declared precision/scale is legal in Oracle and common
  in older schemas. The Spark <-> Oracle JDBC mapping for it has changed across driver and
  Spark versions (sometimes `DecimalType(38,10)`, sometimes a lossy `DoubleType`). Guessing
  wrong does not throw -- it silently rounds or truncates every value in that column, on
  every row, forever, and nothing in a row count or a schema check would show it.
- **How to check:**
  ```sql
  -- On the target DBR cluster, against a real Oracle NUMBER (no precision/scale) column:
  CREATE TABLE scratch.number_probe AS
  SELECT * FROM oracle_table WHERE ROWNUM <= 5;
  DESCRIBE TABLE EXTENDED scratch.number_probe;
  ```
  or from a notebook: `spark.read.format("jdbc").options(**opts).load().schema["the_number_column"].dataType`
- **Expected:** A decimal type wide enough to hold the source data losslessly, e.g.
  `DecimalType(38, 10)`.
- **If it fails:** If Spark silently narrows to `DoubleType`, `sources/oracle/types.py` must
  force an explicit `customSchema` or `oracle.jdbc.mapDateToTimestamp`-style JDBC property
  for every such column, discovered from Oracle's `ALL_TAB_COLUMNS` at onboarding time, not
  left to the driver default.
- **Status:** OPEN

### VB-03 -- Does Oracle `DATE` map to date or timestamp, and under which driver property?
- **Stage / file:** Stage 4 (Oracle). Will govern `sources/oracle/types.py`.
- **Why it matters:** Oracle's `DATE` always carries a time component (unlike ANSI SQL
  `DATE`). Whether the Oracle JDBC driver surfaces that to Spark as `DateType` (silently
  dropping the time) or `TimestampType` depends on a driver connection property. A wrong
  assumption here silently truncates every timestamp in that column to midnight -- the kind
  of bug that survives code review and unit tests because both types "work", just one of
  them is wrong.
- **How to check:**
  ```python
  # against a real Oracle DATE column known to hold a non-midnight time
  df = spark.read.format("jdbc").options(**opts).load()
  df.select("the_date_column").schema  # DateType or TimestampType?
  df.select("the_date_column").show(5, truncate=False)  # time component present?
  ```
  Try with and without `oracle.jdbc.mapDateToTimestamp=true` in the connection options.
- **Expected:** `TimestampType`, time component intact, once the driver property is set
  correctly for this driver version.
- **If it fails:** Set the required driver property in `sources/oracle/spec.py`'s connection
  options as a structural (non-overridable) default, and document it in
  `conf/defaults/oracle.yaml`.
- **Status:** OPEN

### VB-04 -- Which Oracle types in our tables have no clean Spark mapping (LOB, RAW, INTERVAL, TZ types)?
- **Stage / file:** Stage 4 (Oracle). Will govern `sources/oracle/types.py` and the cursor
  query in `sources/oracle/query.py`.
- **Why it matters:** `BLOB`/`CLOB` read through plain JDBC (not `oracle.jdbc.ReadBlobAsBytes`
  or similar) can silently return truncated or `NULL` values under `fetchsize` pressure;
  `INTERVAL YEAR TO MONTH` / `INTERVAL DAY TO SECOND` and `TIMESTAMP WITH TIME ZONE` have no
  first-class Spark equivalent at all. Onboarding an Oracle table with one of these columns,
  unverified, is the single most likely way this framework ships incorrect data without an
  error anywhere.
- **How to check:** For each candidate source table:
  ```sql
  SELECT column_name, data_type, data_length, data_precision, data_scale
  FROM all_tab_columns
  WHERE owner = '<SCHEMA>' AND table_name = '<TABLE>'
  ORDER BY column_id;
  ```
  Then read a sample of rows containing any LOB/RAW/INTERVAL/TZ column via
  `spark.read.format("jdbc")` and diff against `SELECT ... FROM` the same rows via a native
  Oracle client (SQL Developer / sqlplus).
- **Expected:** Either a clean Spark mapping exists, or the column is explicitly excluded
  from the `query`/`dbtable` projection with a documented reason.
- **If it fails:** Exclude the column at the source (in the `filter_criteria`/projection,
  never by silently dropping it after the fact) and record the exclusion in that source's
  YAML comments.
- **Status:** OPEN

### VB-01 -- Does Spark's JDBC `query` option work with `partitionColumn`, or is a parenthesised subquery in `dbtable` required?
- **Stage / file:** Stage 4 (Oracle). Will govern `sources/oracle/query.py`.
- **Why it matters:** These two options are documented as historically mutually exclusive in
  some Spark versions; the workaround is a parenthesised subquery passed as `dbtable`
  instead of `query`. Getting this wrong does not error -- Spark silently falls back to a
  single-partition read, which "works" in dev on a small table and then times out or saturates
  one Oracle session in prod on a large one. That gap between "looks fine" and "falls over at
  scale" is exactly the kind of failure a small team is least equipped to triage under
  pressure.
- **How to check:**
  ```python
  # On the target DBR/Spark version, against a real Oracle table:
  df = spark.read.format("jdbc").options(
      url=jdbc_url, dbtable="(SELECT * FROM schema.table WHERE 1=1) q",
      partitionColumn="id", lowerBound="1", upperBound="1000000", numPartitions="8",
      fetchsize="10000",
  ).load()
  df.rdd.getNumPartitions()  # == 8, or ==1 (silent fallback)?
  ```
  Repeat with `query=` in place of the parenthesised `dbtable` subquery to see whether it
  raises, is ignored, or works.
- **Expected:** The parenthesised-subquery-in-`dbtable` form partitions correctly; behaviour
  of `query` + `partitionColumn` together is known either way.
- **If it fails:** `sources/oracle/query.py` must always build a parenthesised subquery for
  `dbtable` when `partitionColumn` is set, never use `query` for a partitioned read.
- **Status:** OPEN

### VB-09 -- Which Delta MERGE schema-evolution mechanism exists on the target DBR -- session config flag, or builder method?
- **Stage / file:** Stage 1+ (spine) / `curated_writer._merge_curated` today; will move to
  `framework/writers.py`. CORE section 11 item 14.
- **Why it matters:** The current code already hedges (`hasattr(merge, "withSchemaEvolution")`
  with a session-flag fallback), so a wrong guess about which DBR ships which mechanism is
  self-correcting at runtime -- but the untested branch is exactly the one nobody has run yet,
  and the flag-based fallback mutates and restores a *session-level* Spark conf, which is
  unsafe if any other job shares that session concurrently.
- **How to check:** On the target DBR:
  ```python
  from delta.tables import DeltaTable
  dt = DeltaTable.forName(spark, "some.test.table")
  merge = dt.alias("t").merge(some_df.alias("s"), "t.id = s.id")
  hasattr(merge, "withSchemaEvolution")
  ```
  Also confirm the serverless environment version (1 vs 2+) if running on serverless jobs
  compute -- the builder method is documented as raising `AttributeError` on version 1.
- **Expected:** `withSchemaEvolution()` is present on the target DBR / serverless env version.
- **If it fails:** Confirm the session-flag fallback path is exercised by a real MERGE at
  least once (it currently has no non-Spark-marked test), and consider serialising merges
  that rely on the flag if the compute is ever shared across concurrent jobs.
- **Status:** OPEN

### VB-06 -- Is `_metadata` available on the target DBR, and how does `rescuedDataColumn` behave per file format?
- **Stage / file:** Stage 5 (Files / Auto Loader).
- **Why it matters:** `_metadata` (file path, size, modification time) and
  `rescuedDataColumn` (captures fields that don't fit the expected schema) are the two
  mechanisms a Files source uses to avoid silently dropping malformed or unexpected data.
  Their exact behaviour -- what gets rescued, under which format (CSV/JSON/Parquet/Avro),
  and on which DBR they became available -- is undocumented enough to be worth confirming
  before the Files source is built around them.
- **How to check:**
  ```python
  df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/.../schema")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/.../landing_probe"))
  df.selectExpr("_metadata", "_rescued_data").writeStream.format("memory") \
    .queryName("probe").trigger(availableNow=True).start().awaitTermination()
  spark.sql("select * from probe").show(truncate=False)
  ```
  Run against a file with an extra unexpected column and a file with a wrong type in a known
  column.
- **Expected:** `_metadata` populates; `_rescued_data` captures the unexpected/mistyped
  fields rather than silently dropping or nulling them.
- **If it fails:** The Files source's quarantine path (mirroring Kafka's) must key off
  whatever mechanism actually works on this DBR/format combination, and the design in
  STAGE_5 must be revisited before implementation.
- **Status:** OPEN

### VB-05 -- Is `sources[0].latestOffset` populated in `StreamingQueryProgress` under `availableNow`?
- **Stage / file:** `sources/kafka/listener.py::record_progress` and `_pending` (moved here
  in Stage 3 from the retired `kafka_ingest/audit.py`).
- **Why it matters:** TWO things now depend on this progress payload, and they fail
  differently. `position_start` / `position_end` come from `startOffset` / `endOffset`: if
  `availableNow` populates those only on the final micro-batch, or not at all until the
  query drains, the audit trail silently under-reports and support reads it first during an
  incident. `pending_work` (added Stage 3) is `sum(latestOffset - endOffset)`, and it is
  what makes a permanently-lagging source visible at all -- Q16 in
  `sql/03_support_queries.sql` is built on it. If `latestOffset` is absent the column is
  NULL, which is DESIGNED to be the honest answer rather than a wrong one: NULL reads as
  "the source could not tell", 0 would read as "fully caught up". So a missing
  `latestOffset` does not corrupt anything -- it just means the lag question has no answer
  on this runtime, and Q16 would quietly return nothing forever.
- **How to check:**
  ```python
  # after a real availableNow run against a live topic:
  query = df.writeStream.foreachBatch(...).trigger(availableNow=True).start()
  query.awaitTermination()
  for p in query.recentProgress:
      print(p["batchId"], p.get("sources"))
  ```
- **Expected:** Every micro-batch's progress event carries a populated `startOffset` /
  `endOffset` per source, not just the final one -- AND a populated `latestOffset`.
- **If it fails:** For `startOffset` / `endOffset`, `listener.record_progress` needs a
  fallback (e.g. reading offsets from the checkpoint's `offsets` directory) for the batches
  where Spark omits them. For `latestOffset` specifically, do NOT substitute a computed
  value from a Kafka admin client -- that is a runtime dependency this project does not
  take. Either accept that `pending_work` is always NULL for Kafka and say so on the column,
  or drop Q16's Kafka rows. Check `runs_that_could_not_tell` in Q16 to see which case you
  are in.
- **Status:** OPEN

### VB-10 -- Does the `from_avro` writer/reader startup self-check pass on the target runtime?
- **Stage / file:** `src/kafka_ingest/curated_writer.py::assert_from_avro_semantics`; will
  move to `sources/kafka/run.py` or a shared parse helper.
- **Why it matters:** The whole curated-decode path depends on `jsonFormatSchema` being the
  *writer* schema and `options.avroSchema` being the *reader* schema -- CORE section 11 notes
  this exact mapping has moved across Spark versions. The code already contains a runtime
  self-check for this (so a wrong guess fails loudly, once, on the first run, rather than
  silently) -- this VB is to confirm that self-check itself passes on the actual target DBR,
  since it has only ever run against a local/CI PySpark build.
- **How to check:** On the target DBR, in a notebook or via `01_run_unit_tests`:
  ```python
  from kafka_ingest.curated_writer import assert_from_avro_semantics
  assert_from_avro_semantics(spark)  # raises RuntimeError with a clear message if not
  ```
- **Expected:** No exception; log line "from_avro writer/reader schema semantics verified".
- **If it fails:** Confirm the DBR/Spark version meets the 13.3 LTS / Spark 3.4+ floor named
  in the code's own error message before debugging further.
- **Status:** OPEN

### VB-11 -- Can executors read UC Volumes on the target compute access mode (Kafka keystore/truststore)?
- **Stage / file:** `src/kafka_ingest/security.py` module docstring; will move to
  `framework/security.py`. Affects the `cp_onprem_antifraud` (mTLS) and `cp_onprem_rcm`
  (SASL+truststore) cluster profiles in `conf/clusters.yaml`.
- **Why it matters:** `ssl.truststore.location` / `ssl.keystore.location` are opened by the
  Kafka client on the *executors*, not the driver. If the compute's access mode cannot FUSE-
  mount UC Volumes on executors, every task on that cluster fails at connect time -- for
  every topic on an mTLS or truststore-bearing cluster, not just one. Loud, but total, and
  worth confirming before onboarding the Anti-Fraud or RCM domains rather than during their
  first production run.
- **How to check:** Run `notebooks/02_check_connectivity.py` on the target cluster/serverless
  environment; it is designed to check exactly this. Independently, from a `foreachPartition`
  UDF: `os.path.exists("/Volumes/<cat>/ingestion/certs/.../truststore.jks")`.
- **Expected:** `True` on every executor, not just the driver.
- **If it fails:** Either move the affected clusters to a compute access mode that supports
  executor Volume reads, or build the documented `SparkContext.addFile()` +
  `SparkFiles.get()` staging fallback named in `security.py`'s module docstring -- it is
  explicitly not built today because no in-scope cluster needed it.
- **Status:** OPEN

### VB-08 -- Is a UC Volume supported as a Structured Streaming checkpoint location on serverless jobs compute?
- **Stage / file:** `conf/defaults.yaml` (`checkpoint_root`), `src/kafka_ingest/config.py`
  (`TopicConfig.checkpoint_path`). CORE section 10 lists this as a decision returned to a
  human, pending this VB.
- **Why it matters:** Every topic's checkpoint path is required to start with `/Volumes/`
  (enforced in `config.py`). If UC Volumes are not a supported checkpoint backend on
  serverless jobs compute specifically (as opposed to classic clusters), every primary run
  fails at `.option("checkpointLocation", ...)` before touching Kafka at all.
- **How to check:** On the target serverless jobs environment:
  ```python
  (spark.readStream.format("rate").load()
     .writeStream.format("delta")
     .option("checkpointLocation", "/Volumes/<catalog>/ingestion/checkpoints/_probe")
     .trigger(availableNow=True)
     .toTable("scratch.checkpoint_probe"))
  ```
- **Expected:** Succeeds without a "checkpoint location must be on DBFS/ABFSS" style error.
- **If it fails:** `checkpoint_root` must point at an `abfss://` path instead, and the
  `/Volumes/` requirement in `config.py`'s `__post_init__` must be relaxed or changed to
  accept both.
- **Status:** OPEN

### VB-07 -- Auto Loader directory-listing vs file-notification mode -- which is viable in this tenancy?
- **Stage / file:** Stage 5 (Files). CORE section 10 recommends Auto Loader over a
  processed-files ledger, pending this VB.
- **Why it matters:** File-notification mode needs Event Grid subscriptions and queue
  infrastructure the ingestion job's service principal may not have permission to create or
  use; directory-listing mode has no such dependency but costs more as the number of files
  under a path grows. Choosing wrong is a cost and latency problem discovered gradually, not
  a hard failure.
- **How to check:** Confirm with the platform/infra team whether Event Grid + Queue Storage
  provisioning is available and permitted for this workload; if so, prototype
  `.option("cloudFiles.useNotifications", "true")` against one ADLS container and confirm
  events arrive.
- **Expected:** A clear answer on which mode this tenancy supports, backed by a working
  prototype if notification mode is chosen.
- **If it fails:** Fall back to directory-listing mode and document the expected listing
  cost/latency in `docs/CONFIGURATION.md` for the Files source once it exists.
- **Status:** OPEN

### VB-12 -- Serverless egress to brokers, registry, Oracle and ADLS -- is network configuration in place?
- **Stage / file:** Infra, not code. Affects every source once deployed via
  `databricks.yml` (serverless jobs compute).
- **Why it matters:** Every network call this framework makes -- Kafka brokers, Schema
  Registry (`schema_resolver.py`), and (from Stage 4/5) Oracle and ADLS -- originates from
  serverless egress. Without NCC private endpoints or firewall allowlisting of the NCC's
  stable egress IPs, every one of them fails at connect time. Total blocker, but immediate
  and loud on the very first run.
- **How to check:** Run `notebooks/02_check_connectivity.py` against each target environment
  after NCC configuration is claimed complete; it exercises secret read + registry fetch
  (Kafka connect + Oracle/ADLS once those sources exist) without writing any data.
- **Expected:** All checks in that notebook pass for every cluster/registry profile in
  `conf/clusters.yaml` / `conf/registries.yaml`.
- **If it fails:** This is an infra ticket to the platform team, not a code change -- flag it
  as a blocking dependency in the stage report for whichever stage first needs the missing
  route.
- **Status:** OPEN

### VB-13 -- Does `databricks bundle validate -t dev` pass, and does the wheel build?
- **Stage / file:** `databricks.yml`, `resources/*.yml`, `pyproject.toml`. Explicitly listed
  in CORE section 3 as something never to run or claim to have run locally.
- **Why it matters:** Nothing in this repository has ever been validated against the actual
  Databricks CLI or a real bundle deploy. A YAML typo, a bad variable reference, or a job
  parameter name mismatch between `resources/*.yml` and `entrypoints/__init__.py`'s argparse
  flags would only surface here -- and would block every other stage's work from ever
  reaching a cluster, regardless of how correct the Python is.
- **How to check:**
  ```bash
  databricks bundle validate -t dev
  python -m build --wheel   # the artifact the bundle packages
  databricks bundle deploy -t dev   # only after validate is clean
  ```
- **Expected:** `validate` reports no errors; the wheel builds; `deploy` (run separately, by
  a human, once ready) succeeds.
- **If it fails:** Fix the specific YAML/CLI error `validate` reports -- do not guess at a fix
  and move on without re-running it.
- **Status:** OPEN

### VB-14 -- Is the oldest runtime we must support DBR 16.4 LTS, and is its Python 3.12?
- **Stage / file:** `pyproject.toml` (`requires-python`, `[tool.mypy] python_version`),
  `resources/*.yml` (`spark_version`), and the local `.venv`. Added in Stage 1.
- **Why it matters:** the stated target is **DBR 16.4 LTS or later**, so
  `requires-python = ">=3.12"` is a floor with no ceiling: 3.12 is the oldest interpreter
  this code must run on, and a newer runtime must keep working without a release here. If
  the floor is too HIGH, `pip install` of the wheel fails on the cluster outright -- loud,
  and fixed in a minute. If it is too LOW, nothing fails; the code simply may use syntax or
  stdlib the real runtime does not have, and that surfaces at import time on the cluster
  rather than in CI. The Spark version rides along with it: the local `.venv` installs
  `pyspark==3.5.2` so that Spark-marked tests, from Stage 3 onward, run against the Spark
  the oldest supported cluster actually has. Note that `resources/job_ingest_primary.yml`
  still carries a commented `spark_version: "15.4.x-scala2.12"` from before this decision;
  whichever answer is right, those two files must agree.
- **How to check:** In the target workspace, on the cluster the jobs will actually use:
  ```python
  import sys, pyspark
  print(sys.version)            # expect 3.12.x
  print(pyspark.__version__)    # expect 3.5.2
  print(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))
  ```
  Or, without a cluster: `databricks clusters spark-versions` and the DBR release notes for
  whichever version the job clusters are pinned to.
- **Expected:** Python 3.12.x and Spark 3.5.2 on the OLDEST runtime any job uses, i.e.
  DBR 16.4 LTS. Newer runtimes in the fleet are fine and expected -- the question is only
  what the oldest one is.
- **If it fails:** Set the `requires-python` floor and `[tool.mypy] python_version` in
  `pyproject.toml` to the OLDEST supported runtime's minor version, set `spark_version` in
  `resources/*.yml` to match, rebuild the local `.venv` on that Python, and reinstall
  `pyspark` at that runtime's Spark version. All four must move together -- changing one
  alone is what produces a local environment that disagrees with the cluster without
  saying so.
- **Status:** OPEN

---

### VB-15 -- Does the `ingest_state` MERGE actually upsert, is one-run-per-source_key true, and does partitioning isolate concurrent sources?
- **Stage / file:** Stage 2, extended in the decisions-and-stage2-followup pass,
  `framework/state.py` (`StateStore.write_state`, `next_run_sequence`),
  `sql/01_operational_config.sql`.
- **Why it matters:** `next_run_sequence` reads the current value, adds one and MERGEs it
  back, and the number it returns becomes the Delta `txnVersion` for every append a batch
  source makes. Three things are assumed and none can be checked here. First, that the
  MERGE upserts -- if the `whenMatchedUpdateAll` branch silently did nothing, every run
  would read the same value, take the same `txnVersion`, and Delta would drop every append
  after the first as a duplicate. That failure looks exactly like a source with no new data:
  a green job, an audit row saying N rows were presented, and nothing in the table. Second,
  that only one run per `source_key` is ever in flight -- two concurrent runs would both
  read N and both take N+1, and the second's writes would be dropped the same way. Third
  (docs/build_log/DECISIONS.md D-04, added in this pass): that `PARTITIONED BY (source_key)`
  plus deletion vectors actually gives Delta file-level conflict isolation between
  DIFFERENT sources' concurrent MERGEs -- this is a distinct claim from the second one
  above (same source vs different sources), and neither the partitioning nor the deletion
  vectors have run against a real concurrent workload anywhere yet.
- **How to check:** In the target workspace, against a real state table:
  ```sql
  -- after provisioning with sql/01, then running one source twice
  SELECT source_key, state_key, state_value, updated_by_run, updated_at
  FROM {ops_catalog}.{control_schema}.ingest_state
  WHERE state_key = 'run_sequence' ORDER BY source_key;

  DESCRIBE HISTORY {ops_catalog}.{control_schema}.ingest_state;   -- expect MERGE, not INSERT
  ```
  For the same-source concurrency half: confirm in Workflows that the ingestion job for one
  `source_key` has `max_concurrent_runs: 1`, and that no second schedule targets the same
  source. For the cross-source partitioning half: trigger two DIFFERENT sources' jobs at the
  same time against the same table and confirm neither MERGE fails with a Delta concurrent-
  modification exception; `DESCRIBE HISTORY` should show both commits landing without a
  retry, and `numTargetFilesAdded` / `numTargetFilesRemoved` should each stay scoped to one
  source's partition.
- **Expected:** exactly ONE row per (source_key, 'run_sequence'), whose `state_value`
  increments by one per run and whose `updated_by_run` is the latest run's id. `DESCRIBE
  HISTORY` shows MERGE operations, and `numTargetRowsUpdated` is 1 from the second run on.
  Two different sources' concurrent MERGEs both commit cleanly, touching disjoint files.
- **If it fails:** If the MERGE does not update, the condition or the `whenMatchedUpdateAll`
  branch in `StateStore.write_state` is wrong -- fix it there, not by adding a DELETE and an
  INSERT, which would not be atomic. If same-source concurrent runs are real,
  `next_run_sequence` needs a genuinely atomic allocation and the docstring's stated
  assumption must be retracted. If cross-source concurrent MERGEs still conflict despite the
  partitioning, deletion vectors may need to be paired with a retry/backoff around
  `write_state`, or the state table may need to move off a single shared table entirely --
  both are larger changes than this pass, so treat a failure here as a blocker for D-04, not
  something to patch quietly.
- **Status:** OPEN

### VB-16 -- Can the ingestion service principal create the audit and state tables?
- **Stage / file:** Stage 2, `framework/runner.py` (`_ensure_framework_tables`),
  `framework/tables.py`. Grants themselves moved out of `sql/` in the
  decisions-and-stage2-followup pass (docs/build_log/DECISIONS.md D-02) -- see
  `docs/RUNBOOK_CLIENT_IT.md`'s "Unity Catalog privileges" table.
- **Why it matters:** every run issues `CREATE TABLE IF NOT EXISTS` for the audit table and
  the state table before it dispatches, so a fresh environment works without anyone running
  the provisioning SQL first. That needs `CREATE TABLE` on `{ops_catalog}.{audit_schema}`
  and on `{ops_catalog}.{control_schema}`, plus `USE SCHEMA` on both. If the grant is absent
  the run fails at start-up -- loud, and fixed in minutes, which is why this is here rather
  than designed around. What must NOT happen is the opposite: the framework does not issue
  GRANTs itself, deliberately, because a job that can grant is a job that can grant itself
  more.
- **No longer a blocker.** D-02 settled that grants are Terraform-owned and recorded the
  exact privilege list the ingestion service principal and the support group need; this
  entry is now "confirm Terraform granted what that list says" rather than an open design
  question.
- **How to check:** As the job's service principal, in the target workspace:
  ```sql
  SHOW GRANTS `sp-kafka-ingestion` ON SCHEMA {ops_catalog}.{control_schema};
  SHOW GRANTS `sp-kafka-ingestion` ON SCHEMA {ops_catalog}.{audit_schema};
  ```
  Then run one source end to end in a freshly provisioned environment.
- **Expected:** `USE SCHEMA` and `CREATE TABLE` on both schemas, and `SELECT`/`MODIFY` on
  `ingest_state` and the audit table but `SELECT` only on `ingest_control` -- matching
  `docs/RUNBOOK_CLIENT_IT.md`'s privilege table exactly.
- **If it fails:** Either the Terraform did not grant what the privilege list says (fix the
  Terraform, not the code), or a decision is needed that the job should NOT create its own
  tables -- in which case drop the `_ensure_framework_tables` call from `framework/runner.py`
  and make running the provisioning SQL a prerequisite, stated in the runbook.
- **Status:** OPEN

### VB-17 -- Do `sql/01_operational_config.sql` and `sql/02_layer_tables.sql` execute as written?
- **Stage / file:** Stage 2, both SQL files.
- **Why it matters:** these are the provisioning scripts for every environment and nothing
  here can execute a single statement of them. Four things in them are assumed: that Delta
  accepts the named `CONSTRAINT ... CHECK` clauses inside `CREATE TABLE`
  (`kafka_failure_mode_valid`, `source_key_present`, `state_key_present`), that `NOT NULL`
  on a column inside `CREATE TABLE` is accepted alongside them, that `PARTITIONED BY
  (source_key)` combined with `'delta.enableDeletionVectors' = 'true'` in the same
  `CREATE TABLE` is accepted (added for `ingest_state` in the decisions-and-stage2-followup
  pass, D-04), and that the `{ops_catalog}` / `{catalog}` / `{audit_schema}` /
  `{control_schema}` / `{logs_schema}` placeholders are rendered before the file is run --
  an unrendered one fails with a parse error, which is the good case. A wrong CHECK
  constraint is the bad case: the table is created without it and the rule it was meant to
  enforce silently is not enforced.
- **How to check:** Render both files for one environment (see
  `notebooks/00_validate_config`, section "Render the provisioning SQL") and run them in a
  SQL warehouse against a scratch catalog. Then:
  ```sql
  DESCRIBE EXTENDED {ops_catalog}.{control_schema}.ingest_control;   -- constraints listed?
  DESCRIBE EXTENDED {ops_catalog}.{control_schema}.ingest_state;     -- PARTITIONED BY (source_key)?
  DESCRIBE EXTENDED {ops_catalog}.{audit_schema}.ingest_audit;
  ```
- **Expected:** every statement succeeds, and the CHECK constraints and the partitioning
  both appear in `DESCRIBE EXTENDED`. Inserting a control row with
  `kafka_failure_mode = 'nonsense'` is rejected.
- **If it fails:** Fix the exact statement the warehouse rejects. Do NOT drop a constraint to
  make the script run -- if `kafka_failure_mode` cannot be constrained in DDL, the
  equivalent check belongs in `framework/control.py` where the column is read, and this
  file must say so.
- **Status:** OPEN

### VB-18 -- Does the magic-byte comparison behave the same way on the target DBR?
- **Stage / file:** `sources/kafka/wire.py::malformed_reason_col` and `writer_schema_id_col`.
- **Why it matters:** Both functions decide whether a record is Confluent-framed by
  comparing byte 0 against `0x00`. The obvious way to write that is to compare the BINARY
  column against a Python bytes literal (`F.substring("value", 1, 1) != F.lit(bytearray([0]))`),
  and how Spark treats a BINARY-to-bytes comparison -- and whether `lit()` even accepts a
  `bytearray` -- is exactly the kind of thing that has moved between versions. So both
  functions instead compare HEX TEXT: `hex(substring(value, 1, 1)) = '00'`, whose semantics
  are not in question on any version.

  The consequence of getting it wrong is not an error. A comparison that always evaluated
  false would classify EVERY record as `BAD_MAGIC_BYTE`, and under `FAILFAST` that fails the
  first batch loudly -- the good case. Under `QUARANTINE` it would send an entire healthy
  topic to the quarantine table, run after run, reporting success each time.

  This is not merely assumed: `tests/test_kafka_registry.py::test_each_malformed_input_gets_
  its_own_reason` **executed and passed on a local Spark 3.5.2** during Stage 3, covering all
  three reasons plus the valid case. That is strong evidence for the runtime the project
  targets (DBR 16.4 LTS ships Spark 3.5.2 -- VB-14) but it is a local open-source Spark, not
  Databricks Runtime, and photon/ANSI defaults differ.
- **How to check:** Run the spark-marked tests on a real cluster:
  ```
  pytest -m spark tests/test_kafka_registry.py -q
  ```
  Or, in a notebook on the target DBR:
  ```python
  from kafka_ingest.sources.kafka import wire
  rows = [(None,), (b"\x00\x01\x02",), (b"\x99nope",), (b"\x00\x00\x00\x12\x67\x02",)]
  df = spark.createDataFrame(rows, "value BINARY")
  df.select(wire.malformed_reason_col("value").alias("reason"),
            wire.writer_schema_id_col("value").alias("schema_id")).show()
  ```
- **Expected:** `NULL_VALUE_TOMBSTONE`, `TRUNCATED_PAYLOAD`, `BAD_MAGIC_BYTE`, then
  `reason = NULL` with `schema_id = 4711` on the well-formed row. Run it with
  `spark.sql.ansi.enabled` both on and off -- neither should throw.
- **If it fails:** Fix the comparison in `wire.py` only; every caller reads the column and
  none re-derives the rule. Do NOT relax the branch order (NULL, then length, then byte 0):
  measuring before indexing is what keeps a zero-length value from throwing rather than
  being classified.
- **Status:** OPEN
