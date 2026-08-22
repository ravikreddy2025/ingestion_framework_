# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Run ingestion interactively
# MAGIC
# MAGIC The first notebook that actually connects to Kafka and writes data.
# MAGIC
# MAGIC ## Start in dev
# MAGIC The Environment widget selects `conf/environments/<env>.yaml`, which decides the
# MAGIC catalog, the broker and the secret scope. In `dev` every table and checkpoint lives
# MAGIC under the dev catalog, so you can delete the checkpoint and rerun freely.
# MAGIC
# MAGIC ## WARNING - never delete a PRODUCTION checkpoint
# MAGIC Batch ids restart at 0 and Delta then skips every write as a duplicate: the job
# MAGIC reports success and ingests nothing. The framework refuses to start in that state,
# MAGIC but the guard only fires once landing holds rows for the topic. To reprocess prod
# MAGIC data, use the kafka replay job. See docs/DESIGN.md section 4, scenario 6.
# MAGIC
# MAGIC This notebook calls the **same `pipeline.run()`** the scheduled job calls. It is a
# MAGIC thin driver, not a parallel implementation - there is no notebook-only code path.

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
dbutils.widgets.text("topic_key", "vector_patient_events", "2. Topic key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")
dbutils.widgets.text("control_table", "ops_dev.ingestion.ingestion_topic_control", "4. Control table")
dbutils.widgets.dropdown("run_type", "primary",
                         ["primary", "kafka_replay", "curated_replay"], "5. Run type")
dbutils.widgets.text("rerun_id", "", "6. Rerun id (replays only)")
dbutils.widgets.text("starting_offsets", "", "7. Starting offsets JSON (kafka_replay)")
dbutils.widgets.text("starting_timestamp", "", "8. Starting timestamp (kafka_replay)")
dbutils.widgets.text("landing_filter", "", "9. Landing filter (curated_replay)")

# COMMAND ----------

from kafka_ingest import pipeline
from kafka_ingest.config import resolve_topic_config
from kafka_ingest.security import SecretResolver

overrides = {
    "rerun_id": dbutils.widgets.get("rerun_id"),
    "starting_offsets": dbutils.widgets.get("starting_offsets"),
    "starting_timestamp": dbutils.widgets.get("starting_timestamp"),
    "landing_filter": dbutils.widgets.get("landing_filter"),
}

cfg = resolve_topic_config(
    spark=spark,
    config_root=dbutils.widgets.get("config_root"),
    topic_key=dbutils.widgets.get("topic_key"),
    control_table=dbutils.widgets.get("control_table"),
    environment=dbutils.widgets.get("environment"),
    run_type=dbutils.widgets.get("run_type"),
    overrides=overrides,   # empty strings are filtered out by the resolver
)

print(f"""
ABOUT TO RUN
  environment  {cfg.environment}
  run type     {cfg.run.run_type}
  topic        {cfg.topic}
  enabled      {cfg.enabled}
  broker       {cfg.cluster.bootstrap_servers}
  checkpoint   {cfg.checkpoint_path}
  landing      {cfg.landing_table}   PARTITIONED BY {cfg.landing_partition_by}
  curated      {cfg.curated_table}   PARTITIONED BY {cfg.curated_partition_by}
  audit        {cfg.audit_table}
""")

# Guardrails against the two mistakes that are expensive rather than merely annoying.
if cfg.run.run_type == "primary" and cfg.environment == "prod":
    print("WARNING: PRIMARY run against PROD. This shares offset state with the scheduled")
    print("         job - an interactive run here advances the production checkpoint.")
if cfg.run.is_replay and not cfg.run.rerun_id:
    raise ValueError("A replay needs a rerun_id - it isolates both the checkpoint and the txnAppId.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run
# MAGIC Under `trigger: availableNow` this drains whatever is currently on the topic and stops.
# MAGIC Landing and curated are written from the same microbatch, in one query, with audit
# MAGIC rows emitted between the two layers.

# COMMAND ----------

pipeline.run(spark, cfg, SecretResolver())
print("Run complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What happened - read the audit table first, always
# MAGIC One row per (batch, layer, status). A batch with a landing COMPLETED but no curated
# MAGIC COMPLETED died between the two writes.

# COMMAND ----------

display(
    spark.table(cfg.audit_table)
    .where(f"topic_key = '{cfg.topic_key}'")
    .orderBy("event_ts", ascending=False)
    .select("batch_id", "layer", "status", "record_count", "quarantined_count",
            "writer_schema_ids", "reader_schema_id", "duration_ms",
            "starting_offsets", "ending_offsets", "error_class", "error_message")
    .limit(40)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which layer did each batch reach?
# MAGIC The pivot the per-layer rows exist for - same shape as Q2 in
# MAGIC `sql/03_support_queries.sql`.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT batch_id,
               max(CASE WHEN layer='landing' THEN status END) AS landing_status,
               max(CASE WHEN layer='landing' THEN record_count END) AS landing_rows,
               max(CASE WHEN layer='curated' THEN status END) AS curated_status,
               max(CASE WHEN layer='curated' THEN record_count END) AS curated_rows,
               max(CASE WHEN layer='curated' THEN quarantined_count END) AS quarantined
        FROM {cfg.audit_table}
        WHERE topic_key = '{cfg.topic_key}'
        GROUP BY batch_id ORDER BY batch_id DESC LIMIT 25
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check the data

# COMMAND ----------

print("LANDING - raw bytes verbatim, schema id from the wire header, CloudEvent columns")
display(
    spark.table(cfg.landing_table)
    .where(f"topic = '{cfg.topic}'")
    .select("topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "writer_schema_id",
            "wire_format_valid", "payload_bytes", "ce_id", "ce_type", "ce_time",
            "ingested_via", "replay_run_id")
    .orderBy("kafka_offset", ascending=False)
    .limit(20)
)

# COMMAND ----------

print("CURATED - payload kept NESTED. Query it with payload.<field>, read it with to_json().")
display(spark.table(cfg.curated_table).orderBy("kafka_offset", ascending=False).limit(20))

# COMMAND ----------

# The payload struct rendered as readable JSON - useful when eyeballing a nested record.
display(
    spark.table(cfg.curated_table)
    .selectExpr("kafka_offset", "event_date", "ce_id", "to_json(payload) AS payload_json")
    .orderBy("kafka_offset", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate check - should always return zero rows
# MAGIC If this returns anything, Delta's idempotent-write markers are not behaving as
# MAGIC documented. Stop and read docs/DESIGN.md section 4 before running anything else.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT topic, kafka_partition, kafka_offset, count(*) AS copies
        FROM {cfg.landing_table}
        WHERE topic = '{cfg.topic}'
        GROUP BY topic, kafka_partition, kafka_offset
        HAVING count(*) > 1
        LIMIT 50
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine (only if `on_deser_error: quarantine`)

# COMMAND ----------

if cfg.on_deser_error == "quarantine" and spark.catalog.tableExists(cfg.quarantine_table):
    display(
        spark.table(cfg.quarantine_table)
        .groupBy("quarantine_reason", "writer_schema_id").count()
        .orderBy("count", ascending=False)
    )
else:
    print(f"Topic is on_deser_error='{cfg.on_deser_error}' - no quarantine table in play.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resetting a DEV run
# MAGIC Only ever in dev. In preprod/prod use the **kafka replay job** instead - it gets its
# MAGIC own checkpoint AND its own txnAppId, so it cannot collide with the primary stream.
# MAGIC
# MAGIC ```python
# MAGIC assert cfg.environment == "dev", "refusing to reset a non-dev environment"
# MAGIC dbutils.fs.rm(cfg.checkpoint_path, recurse=True)
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {cfg.landing_table}")
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {cfg.curated_table}")
# MAGIC ```
# MAGIC
# MAGIC Landing is one table per topic, so `DROP TABLE` is safe here and preferable to a
# MAGIC filtered `DELETE`: it also clears the table's Delta transaction log, so the next dev
# MAGIC run starts under a clean `txnAppId` history instead of one still carrying old batch
# MAGIC ids. In preprod/prod, never do this - use `checkpoint_reset_id` (RUNBOOK_SUPPORT §5.4a)
# MAGIC if a primary checkpoint is genuinely lost there.
