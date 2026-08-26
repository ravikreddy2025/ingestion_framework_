# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Validate configuration
# MAGIC
# MAGIC **The safest thing to run first.** Resolves one source's configuration and prints it,
# MAGIC for any source type - Kafka, Oracle or Files.
# MAGIC
# MAGIC * Does **not** connect to Kafka, Oracle or ADLS
# MAGIC * Does **not** read any secret
# MAGIC * Does **not** create or write to any table
# MAGIC
# MAGIC It proves the five config layers merge, the connection references (cluster/registry/
# MAGIC jdbc/storage) exist, every `{placeholder}` resolves, the table name is a well-formed
# MAGIC 3-tier UC name, and (for Kafka/Files) the checkpoint path is Volume-backed.
# MAGIC
# MAGIC This notebook is a thin driver: everything it calls is the same
# MAGIC `kafka_ingest.framework.config` module the real job uses. There is no notebook-only
# MAGIC code path.

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
dbutils.widgets.text("source_key", "vector_patient_events", "2. Source key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")

config_root = dbutils.widgets.get("config_root")
source_key = dbutils.widgets.get("source_key")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is deployable, and where?
# MAGIC Source files starting with `_` are templates and are deliberately skipped - the same
# MAGIC rule `framework/config.py` and the shipped-config tests apply.

# COMMAND ----------

import glob

from kafka_ingest.framework.config import available_environments, read_source_type

print("Environments:", available_environments(config_root))
print("\nDeployable sources:")
for path in sorted(glob.glob(f"{config_root}/sources/*.yaml")):
    name = os.path.basename(path)[:-5]
    if not name.startswith("_"):
        print(f"  - {name:<32} source_type={read_source_type(config_root, name)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the selected source
# MAGIC Five layers, later winning per key:
# MAGIC `defaults.yaml` -> `defaults/<type>.yaml` -> `environments/<env>.yaml` ->
# MAGIC `sources/<key>.yaml` -> control table (empty here - see the note below) -> job parameters.
# MAGIC
# MAGIC **No control table is read.** This notebook resolves against an empty layer 4/5 so it
# MAGIC needs no Spark session and no table to exist - it shows what the source runs on by
# MAGIC default, which is what "validate the YAML" means. To see the effect of a real control
# MAGIC row, read it yourself with `spark.table(...)` and pass the row's columns as `control=`.

# COMMAND ----------

# The same {kafka, oracle, file} mapping framework/runner.py's `_SOURCES` uses. Duplicated
# here rather than imported, because this notebook has no RunContext to build and no run to
# dispatch - it only needs each source type's SOURCE_SPEC to validate against.
from kafka_ingest.framework.config import resolve_config
from kafka_ingest.sources import file as file_source
from kafka_ingest.sources import kafka, oracle

_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}

source_type = read_source_type(config_root, source_key)
spec = _SOURCES[source_type].SOURCE_SPEC

cfg = resolve_config(config_root, source_key, environment, spec, control={}, job_parameters={})

print(f"SOURCE_KEY     {cfg.source_key}   (type: {cfg.source_type})")
print(f"ENVIRONMENT    {cfg.environment}")
print(f"ENABLED        {cfg.enabled}")
print(f"LAYERS         {cfg.layers}")
print()
print("SETTINGS (as resolved, before the control table and job parameters are applied):")
for key in sorted(cfg.settings):
    print(f"  {key:<28} {cfg.settings[key]!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare across environments
# MAGIC The same source file, resolved for each environment. Catalogs and endpoints must
# MAGIC differ; the topic name / Oracle table / file path must not.

# COMMAND ----------

for env in available_environments(config_root):
    other = resolve_config(config_root, source_key, env, spec, control={}, job_parameters={})
    identity_key = {"kafka": "topic", "oracle": "source_table", "file": "source_path"}[source_type]
    print(f"{env:<9} {identity_key}={other.settings.get(identity_key)!r:<40}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which secrets will this source need?
# MAGIC Names only - no values are read here. The register (cluster/registry/jdbc/storage)
# MAGIC this source references is looked up and its secret KEY NAMES printed, for whichever
# MAGIC registers this source type actually uses.

# COMMAND ----------

_REGISTER_KEYS_BY_TYPE = {
    "kafka": (("clusters", "cluster"), ("registries", "registry")),
    "oracle": (("jdbc", "jdbc_ref"),),
    "file": (("storage", "storage_ref"),),
}

print("SECRET SCOPE / KEYS EXPECTED")
for register, setting_name in _REGISTER_KEYS_BY_TYPE[source_type]:
    profile_name = cfg.settings.get(setting_name)
    if not profile_name:
        print(f"  ({setting_name} not set - likely a Unity Catalog Volume path with no register)")
        continue
    profile = cfg.profile(register, profile_name)
    scope = profile.get("secret_scope")
    for key, value in sorted(profile.items()):
        if key != "secret_scope" and str(key).endswith("_key") and value:
            print(f"  {register}.{profile_name:<24} {scope} / {value}")

print("\nCERTIFICATE FILES EXPECTED (must exist and be readable) - Kafka / registry only")
if source_type == "kafka":
    cluster = cfg.profile("clusters", cfg.settings["cluster"])
    registry = cfg.profile("registries", cfg.settings["registry"])
    for label, path, who in [
        ("kafka truststore", cluster.get("truststore_path"), "EXECUTORS"),
        ("kafka keystore", cluster.get("keystore_path"), "EXECUTORS"),
        ("registry cert", registry.get("client_cert_path"), "driver only"),
        ("registry key", registry.get("client_key_path"), "driver only"),
        ("registry CA", registry.get("ca_bundle_path"), "driver only"),
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

# path = cfg.profile("clusters", cfg.settings["cluster"])["truststore_path"]
# import os as _os
# visible = spark.range(8).repartition(8).rdd.map(lambda _: _os.path.exists(path)).collect()
# print(f"{path}\nvisible from all executors: {all(visible)}  {visible}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render the provisioning SQL for this environment
# MAGIC
# MAGIC `sql/01`-`sql/03` are templates holding `{catalog}` / `{ops_catalog}` / `{audit_schema}`
# MAGIC / `{control_schema}` placeholders, exactly like `conf/`. This resolves them from the
# MAGIC SAME `conf/environments/<env>.yaml` the job reads, so the provisioning SQL cannot drift
# MAGIC from the table names the code will actually use.
# MAGIC
# MAGIC It **prints** the SQL. It does not run it - creating schemas and tables is a
# MAGIC deliberate act, and running DDL as a side effect of a validation notebook is not.
# MAGIC Copy the output into a SQL editor, or save it, and review it before executing.

# COMMAND ----------

import glob

from kafka_ingest.framework.config import _read_yaml, _substitute

_env_doc = _read_yaml(os.path.join(config_root, "environments", f"{environment}.yaml"))
_scope = dict(_env_doc.get("vars", {}) or {})

print(f"Rendering sql/*.sql for environment '{environment}' with:")
for _k, _v in sorted(_scope.items()):
    print(f"  {{{_k}}} -> {_v}")

for _path in sorted(glob.glob(f"{REPO_ROOT}/sql/01_operational_config.sql")
                     + glob.glob(f"{REPO_ROOT}/sql/02_layer_tables.sql")
                     + glob.glob(f"{REPO_ROOT}/sql/03_support_queries.sql")):
    _raw = open(_path, encoding="utf-8").read()
    print("")
    print("=" * 86)
    print(f"-- RENDERED: {os.path.basename(_path)}")
    print("=" * 86)
    # Same substitution the config loader uses, so an unresolved placeholder is a hard error
    # here for the same reason it is there - a table literally named "{catalog}.landing..."
    # fails much later and much less clearly. sql/02's {topic_table} is a Kafka-only,
    # source-derived token and is left unresolved by design in this generic render.
    try:
        print(_substitute(_raw, _scope, os.path.basename(_path), deferred=frozenset({"topic_table"})))
    except Exception as exc:  # noqa: BLE001 - show the error inline rather than aborting the notebook
        print(f"(could not fully render: {exc})")

# COMMAND ----------

print("Configuration resolved successfully. Nothing was connected to or written.")
