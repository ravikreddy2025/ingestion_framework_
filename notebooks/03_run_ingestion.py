# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Run ingestion interactively
# MAGIC
# MAGIC The first notebook that actually connects to Kafka, Oracle or ADLS and writes data.
# MAGIC Works for any source type - the entrypoint is the same one the scheduled job calls.
# MAGIC
# MAGIC ## Start in dev
# MAGIC The Environment widget selects `conf/environments/<env>.yaml`, which decides the
# MAGIC catalog, the connection endpoints and the secret scopes. In `dev` every table and
# MAGIC checkpoint/state key lives under the dev catalog, so you can reset freely.
# MAGIC
# MAGIC ## WARNING - never delete a PRODUCTION checkpoint (Kafka, Files)
# MAGIC Batch ids restart at 0 and Delta then skips every write as a duplicate: the job
# MAGIC reports success and ingests nothing. The framework refuses to start in that state, but
# MAGIC the guard only fires once landing holds rows for the source. To reprocess prod data,
# MAGIC use a replay job (Kafka) or the checkpoint-reset procedure (Files) - see
# MAGIC `docs/RUNBOOK_SUPPORT.md`.
# MAGIC
# MAGIC ## WARNING - never hand-edit an Oracle watermark outside the one sanctioned procedure
# MAGIC `docs/RUNBOOK_SUPPORT.md` §8.4 (Q18) is the only supported way, and only after
# MAGIC confirming the value from the audit table.
# MAGIC
# MAGIC This notebook calls the **same `framework.runner.run()`** the scheduled job calls. It
# MAGIC is a thin driver, not a parallel implementation - there is no notebook-only code path.

# COMMAND ----------

import logging
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, f"{REPO_ROOT}/src")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
                    stream=sys.stdout, force=True)

# COMMAND ----------

dbutils.widgets.text("config_root", f"{REPO_ROOT}/conf", "1. Config root")
dbutils.widgets.text("source_key", "vector_patient_events", "2. Source key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")
dbutils.widgets.dropdown(
    "run_type", "primary",
    ["primary", "kafka_replay", "curated_replay", "oracle_replay"], "4. Run type"
)
dbutils.widgets.text("rerun_id", "", "5. Rerun id (replays only)")
dbutils.widgets.text("replay_starting_offsets", "", "6. Kafka: starting offsets JSON")
dbutils.widgets.text("replay_starting_timestamp", "", "7. Kafka: starting timestamp")
dbutils.widgets.text("replay_ending_offsets", "", "8. Kafka: ending offsets JSON (optional)")
dbutils.widgets.text("replay_ending_timestamp", "", "9. Kafka: ending timestamp (optional)")
dbutils.widgets.text("replay_landing_filter", "", "10. Kafka: landing filter (curated_replay)")
dbutils.widgets.text("replay_cursor_start", "", "11. Oracle: replay cursor start")
dbutils.widgets.text("replay_cursor_end", "", "12. Oracle: replay cursor end (optional)")

config_root = dbutils.widgets.get("config_root")
source_key = dbutils.widgets.get("source_key")
environment = dbutils.widgets.get("environment")
run_type = dbutils.widgets.get("run_type")

_REPLAY_PARAMETERS = (
    "replay_starting_offsets", "replay_starting_timestamp",
    "replay_ending_offsets", "replay_ending_timestamp",
    "replay_landing_filter", "replay_cursor_start", "replay_cursor_end",
)
job_parameters = {}
if run_type != "primary":
    rerun_id = dbutils.widgets.get("rerun_id")
    if not rerun_id:
        raise ValueError("A replay needs a rerun_id - it isolates the checkpoint/state and tags every row written.")
    job_parameters["rerun_id"] = rerun_id
    for name in _REPLAY_PARAMETERS:
        value = dbutils.widgets.get(name)
        if value:
            job_parameters[name] = value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve and preview - the same read `00_validate_config` does, before anything runs

# COMMAND ----------

from kafka_ingest.framework.config import read_source_type, resolve_config
from kafka_ingest.sources import file as file_source
from kafka_ingest.sources import kafka, oracle

_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}

source_type = read_source_type(config_root, source_key)
spec = _SOURCES[source_type].SOURCE_SPEC
preview = resolve_config(config_root, source_key, environment, spec, control={}, job_parameters=job_parameters)

print(f"""
ABOUT TO RUN
  source_key   {source_key}   (type: {source_type})
  environment  {environment}
  run_type     {run_type}
  enabled      {preview.enabled}
  layers       {preview.layers}
  audit_table  {preview.get('audit_table')}
  state_table  {preview.get('state_table')}
""")

if run_type == "primary" and environment == "prod":
    print("WARNING: PRIMARY run against PROD. For Kafka/Files this shares checkpoint state with")
    print("         the scheduled job; for Oracle it advances the shared watermark. An")
    print("         interactive run here has the same effect as a scheduled one.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run
# MAGIC For Kafka/Files this drains whatever is currently available under `availableNow` and
# MAGIC stops. For Oracle this reads one bounded interval. `framework.runner.run()` reads the
# MAGIC control table itself (no `control=` override here), exactly as the scheduled job does.

# COMMAND ----------

from kafka_ingest.framework import runner

result = runner.run(
    source_key=source_key,
    environment=environment,
    config_root=config_root,
    run_type=run_type,
    job_parameters=job_parameters,
    spark=spark,
)
print(f"""
RUN COMPLETE
  rows_read         {result.rows_read}
  rows_written      {result.rows_written}
  rows_quarantined  {result.rows_quarantined}
  position_start    {result.position_start}
  position_end      {result.position_end}
  pending_work      {result.pending_work}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What happened - read the audit table first, always
# MAGIC One row per (run, layer, status). For Kafka, a run with `landing COMPLETED` but no
# MAGIC `curated COMPLETED` died between the two writes - see `docs/DESIGN.md`'s failure tables.

# COMMAND ----------

display(
    spark.table(preview.get("audit_table"))
    .where(f"source_key = '{source_key}'")
    .orderBy("event_ts", ascending=False)
    .select("run_id", "layer", "status", "record_count", "quarantined_count", "duration_ms",
            "position_start", "position_end", "pending_work", "error_class", "error_message")
    .limit(40)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which layer did this run reach?
# MAGIC The pivot the per-layer rows exist for - the same shape as Q2 in
# MAGIC `sql/03_support_queries.sql`.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT run_id,
               max(CASE WHEN layer='run'     THEN status END) AS run_status,
               max(CASE WHEN layer='landing' THEN status END) AS landing_status,
               max(CASE WHEN layer='landing' THEN record_count END) AS landing_rows,
               max(CASE WHEN layer='curated' THEN status END) AS curated_status,
               max(CASE WHEN layer='curated' THEN record_count END) AS curated_rows,
               max(quarantined_count) AS quarantined
        FROM {preview.get("audit_table")}
        WHERE source_key = '{source_key}'
        GROUP BY run_id ORDER BY run_id DESC LIMIT 25
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check the landed data
# MAGIC The target table name is whatever this source resolved to - read it back from the
# MAGIC audit row rather than guessing, since Kafka derives it from the topic and Oracle/Files
# MAGIC derive it from their own settings.

# COMMAND ----------

_last_landing = (
    spark.table(preview.get("audit_table"))
    .where(f"source_key = '{source_key}' AND layer = 'landing' AND status = 'COMPLETED'")
    .orderBy("event_ts", ascending=False)
    .select("source_ref")
    .limit(1)
    .collect()
)
print("Last landing source_ref:", _last_landing[0]["source_ref"] if _last_landing else "(none yet)")
print("\nFind the exact landing/curated table name from 00_validate_config's SETTINGS output")
print("(landing_table / curated_table), then, e.g.:")
print("  display(spark.table('<landing_table>').orderBy('ingest_ts', ascending=False).limit(20))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate check - should always return zero rows
# MAGIC If this returns anything, the idempotent-write mechanism is not behaving as
# MAGIC documented. Stop and read `docs/DESIGN.md`'s "re-runs and duplicates" section (Kafka)
# MAGIC or its Oracle failure-scenario table before running anything else. Substitute the
# MAGIC landing table name and this source's own key columns
# MAGIC (`topic, kafka_partition, kafka_offset` for Kafka; the Oracle merge key for Oracle).

# COMMAND ----------

# display(
#     spark.sql("""
#         SELECT <key columns>, count(*) AS copies
#         FROM <landing_table>
#         GROUP BY <key columns>
#         HAVING count(*) > 1
#         LIMIT 50
#     """)
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resetting a DEV run
# MAGIC Only ever in dev. In preprod/prod, Kafka uses a **replay job**, Oracle corrects its
# MAGIC watermark via the one sanctioned Q18 procedure, and Files uses the checkpoint-reset
# MAGIC procedure - never a manual reset. See `docs/RUNBOOK_SUPPORT.md`.
# MAGIC
# MAGIC ```python
# MAGIC assert environment == "dev", "refusing to reset a non-dev environment"
# MAGIC # Kafka / Files: dbutils.fs.rm(<checkpoint_path from the SETTINGS output>, recurse=True)
# MAGIC # Then, for any source type:
# MAGIC # spark.sql(f"DROP TABLE IF EXISTS {preview.get('...table setting...')}")
# MAGIC ```
# MAGIC
# MAGIC `DROP TABLE` also clears the table's Delta transaction log, so the next dev run starts
# MAGIC under a clean idempotency history instead of one still carrying old versions.
