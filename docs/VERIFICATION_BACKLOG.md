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
- **Stage / file:** `src/kafka_ingest/audit.py::StreamAuditListener.onQueryProgress` today;
  will move to `framework/audit.py`.
- **Why it matters:** The stream-layer audit row's `ending_offsets` is read from
  `progress.sources[0].endOffset`. If `availableNow` handles this differently from a
  continuous trigger (e.g. populates it only on the final micro-batch, or not at all until
  the query fully drains), the audit trail -- the thing support reads first during an
  incident -- silently under-reports without any error surfacing anywhere.
- **How to check:**
  ```python
  # after a real availableNow run against a live topic:
  query = df.writeStream.foreachBatch(...).trigger(availableNow=True).start()
  query.awaitTermination()
  for p in query.recentProgress:
      print(p["batchId"], p.get("sources"))
  ```
- **Expected:** Every micro-batch's progress event carries a populated `startOffset` /
  `endOffset` per source, not just the final one.
- **If it fails:** `audit._progress_dict` / `onQueryProgress` need a fallback (e.g. reading
  offsets from the checkpoint's `offsets` directory) for the batches where Spark omits them.
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
