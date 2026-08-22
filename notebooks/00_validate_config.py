# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Validate configuration
# MAGIC
# MAGIC **The safest thing to run first.** Resolves a topic's configuration and prints it.
# MAGIC
# MAGIC * Does **not** connect to Kafka
# MAGIC * Does **not** read any secret
# MAGIC * Does **not** create or write to any table
# MAGIC
# MAGIC It proves the five config layers merge, the cluster/registry references exist, every
# MAGIC {placeholder} resolves, the table names are well-formed 3-tier UC names, and the
# MAGIC checkpoint paths are Volume-backed.

# COMMAND ----------

import os
import sys

# In a Databricks Git folder the working directory is the notebook's own directory, so the
# package sits at ../src. Override REPO_ROOT below if you imported the folder elsewhere.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if os.path.isdir(f"{REPO_ROOT}/src"):
    sys.path.insert(0, f"{REPO_ROOT}/src")
else:
    raise RuntimeError(
        f"Could not find {REPO_ROOT}/src. Set REPO_ROOT manually to the folder that "
        "contains src/, conf/ and tests/."
    )
print("REPO_ROOT:", REPO_ROOT)

# COMMAND ----------

dbutils.widgets.text("config_root", f"{REPO_ROOT}/conf", "1. Config root")
dbutils.widgets.text("topic_key", "vector_patient_events", "2. Topic key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")
dbutils.widgets.text("control_table", "ops_dev.ingestion.ingestion_topic_control", "4. Control table")

config_root = dbutils.widgets.get("config_root")
topic_key = dbutils.widgets.get("topic_key")
environment = dbutils.widgets.get("environment")
control_table = dbutils.widgets.get("control_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is deployable, and where?
# MAGIC Topic files starting with `_` are templates and are deliberately skipped.

# COMMAND ----------

import glob

from kafka_ingest.config import available_environments

print("Environments:", available_environments(config_root))
print("\nDeployable topic keys:")
for path in sorted(glob.glob(f"{config_root}/topics/*.yaml")):
    name = os.path.basename(path)[:-5]
    if not name.startswith("_"):
        print(f"  - {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the selected topic
# MAGIC Five layers, later winning per key:
# MAGIC `defaults.yaml` -> `environments/<env>.yaml` -> `topics/<key>.yaml` ->
# MAGIC control table -> job parameters.

# COMMAND ----------

from kafka_ingest.config import resolve_topic_config

cfg = resolve_topic_config(
    spark=spark,
    config_root=config_root,
    topic_key=topic_key,
    control_table=control_table,
    environment=environment,
)

print(f"""
TOPIC          {cfg.topic}   (key: {cfg.topic_key}, domain: {cfg.domain})
ENVIRONMENT    {cfg.environment}
ENABLED        {cfg.enabled}

SOURCE
  cluster      {cfg.cluster.name}  [{cfg.cluster.auth_mode}]
  bootstrap    {cfg.cluster.bootstrap_servers}
  registry     {cfg.registry.name}  [{cfg.registry.auth_mode}]
  registry url {cfg.registry.url}
  subject      {cfg.subject}

TARGETS
  landing      {cfg.landing_table}     (one per topic)
  curated      {cfg.curated_table}     (one per topic)
  quarantine   {cfg.quarantine_table}
  audit        {cfg.audit_table}       (SHARED by every topic)

STREAMING
  checkpoint   {cfg.checkpoint_path}
  trigger      {cfg.trigger}
  start offset {cfg.starting_offsets}    (first run only)
  batch cap    {cfg.max_offsets_per_trigger}
  group prefix {cfg.group_id_prefix}
  data loss    fail_on_data_loss={cfg.fail_on_data_loss}

DESERIALIZATION
  reader mode  {cfg.reader_schema_mode}  (pinned id: {cfg.reader_schema_id})
  on error     {cfg.on_deser_error}

LAYOUT
  landing PARTITIONED BY  {cfg.landing_partition_by}
  curated PARTITIONED BY  {cfg.curated_partition_by}
  curated dedup keys      {cfg.curated_dedup_keys}   (nested: payload.<field>)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare across environments
# MAGIC The same topic file, resolved for each environment. Catalogs and endpoints must
# MAGIC differ; the topic name and subject must not.

# COMMAND ----------

for env in available_environments(config_root):
    other = resolve_topic_config(spark=spark, config_root=config_root, topic_key=topic_key,
                                 control_table=control_table, environment=env)
    print(f"{env:<9} landing={other.landing_table:<40} broker={other.cluster.bootstrap_servers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which secrets and certificates will this topic need?
# MAGIC Names only - no values are read here.

# COMMAND ----------

print("SECRET SCOPE / KEYS EXPECTED")
for label, scope, key in [
    ("kafka username", cfg.cluster.secret_scope, cfg.cluster.sasl_username_key),
    ("kafka password", cfg.cluster.secret_scope, cfg.cluster.sasl_password_key),
    ("truststore pw", cfg.cluster.secret_scope, cfg.cluster.truststore_password_key),
    ("keystore pw", cfg.cluster.secret_scope, cfg.cluster.keystore_password_key),
    ("key pw", cfg.cluster.secret_scope, cfg.cluster.key_password_key),
    ("registry user", cfg.registry.secret_scope, cfg.registry.username_key),
    ("registry pass", cfg.registry.secret_scope, cfg.registry.password_key),
]:
    if key:
        print(f"  {label:<16} {scope} / {key}")

print("\nCERTIFICATE FILES EXPECTED (must exist and be readable)")
for label, path, who in [
    ("kafka truststore", cfg.cluster.truststore_path, "EXECUTORS"),
    ("kafka keystore", cfg.cluster.keystore_path, "EXECUTORS"),
    ("registry cert", cfg.registry.client_cert_path, "driver only"),
    ("registry key", cfg.registry.client_key_path, "driver only"),
    ("registry CA", cfg.registry.ca_bundle_path, "driver only"),
]:
    if path:
        print(f"  {label:<18} {path}")
        print(f"  {'':<18} read by {who}  |  visible from driver: {os.path.exists(path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### If any executor-read certificate is listed above
# MAGIC Confirm the executors can see it too - the driver check is not sufficient.
# MAGIC `02_check_connectivity` does this properly; uncomment here for a quick look.

# COMMAND ----------

# path = cfg.cluster.truststore_path
# import os as _os
# visible = spark.range(8).repartition(8).rdd.map(lambda _: _os.path.exists(path)).collect()
# print(f"{path}\nvisible from all executors: {all(visible)}  {visible}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render the provisioning SQL for this environment
# MAGIC
# MAGIC `sql/01`, `sql/02` and `sql/03` are templates holding `{catalog}` / `{ops_catalog}`
# MAGIC placeholders, exactly like `conf/`. This resolves them from the SAME
# MAGIC `conf/environments/<env>.yaml` the job reads, so the provisioning SQL cannot drift
# MAGIC from the table names the code will actually use.
# MAGIC
# MAGIC It **prints** the SQL. It does not run it - creating schemas, tables and GRANTs is a
# MAGIC deliberate act, and running DDL as a side effect of a validation notebook is not.
# MAGIC Copy the output into a SQL editor, or save it, and review it before executing.

# COMMAND ----------

import glob

from kafka_ingest.config import _read_yaml, _substitute

_env_doc = _read_yaml(os.path.join(config_root, "environments", f"{environment}.yaml"))
_scope = dict(_env_doc.get("vars", {}) or {})
# ops_catalog is a deployment value (it lives in databricks.yml, not conf/), so it is not in
# `vars:`. Default it to the convention and let the widget override it if yours differs.
_scope.setdefault("ops_catalog", f"ops_{environment}")

print(f"Rendering sql/*.sql for environment '{environment}' with:")
for _k, _v in sorted(_scope.items()):
    print(f"  {{{_k}}} -> {_v}")

for _path in sorted(glob.glob(f"{REPO_ROOT}/sql/*.sql")):
    _raw = open(_path, encoding="utf-8").read()
    print("")
    print("=" * 86)
    print(f"-- RENDERED: {os.path.basename(_path)}")
    print("=" * 86)
    # Same substitution the config loader uses, so an unresolved placeholder is a hard error
    # here for the same reason it is there - a table literally named "{catalog}.landing..."
    # fails much later and much less clearly.
    print(_substitute(_raw, _scope, os.path.basename(_path)))

# COMMAND ----------

print("Configuration resolved successfully. Nothing was connected to or written.")
