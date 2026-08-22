# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Check secrets, certificates and Schema Registry connectivity
# MAGIC
# MAGIC Run this **after** `00_validate_config` passes and **before** `03_run_ingestion`.
# MAGIC
# MAGIC * Reads secrets - proves the scope exists and the service principal has READ
# MAGIC * Fetches a schema from the registry - proves the network path and credentials work
# MAGIC * **Does not connect to Kafka. Does not write anything.**
# MAGIC
# MAGIC No secret VALUE is ever printed. Run it once per environment: dev, preprod and prod
# MAGIC use different scopes and different endpoints, so passing in one proves nothing about
# MAGIC the others.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, f"{REPO_ROOT}/src")

dbutils.widgets.text("config_root", f"{REPO_ROOT}/conf", "1. Config root")
dbutils.widgets.text("topic_key", "vector_patient_events", "2. Topic key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")
dbutils.widgets.text("control_table", "ops_dev.ingestion.ingestion_topic_control", "4. Control table")

# COMMAND ----------

from kafka_ingest.config import resolve_topic_config
from kafka_ingest.security import SecretResolver, build_kafka_options, build_registry_auth, redact

cfg = resolve_topic_config(
    spark=spark,
    config_root=dbutils.widgets.get("config_root"),
    topic_key=dbutils.widgets.get("topic_key"),
    control_table=dbutils.widgets.get("control_table"),
    environment=dbutils.widgets.get("environment"),
)
secrets = SecretResolver()
print(f"Topic:       {cfg.topic}")
print(f"Environment: {cfg.environment}")
print(f"Cluster:     {cfg.cluster.name} [{cfg.cluster.auth_mode}] -> {cfg.cluster.bootstrap_servers}")
print(f"Registry:    {cfg.registry.name} [{cfg.registry.auth_mode}] -> {cfg.registry.url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Kafka connection options
# MAGIC Builds the exact options the Spark Kafka source will receive. Every credential-bearing
# MAGIC value is redacted before display. **This reads secrets but makes no network call.**

# COMMAND ----------

try:
    options = build_kafka_options(cfg.cluster, secrets)
    print("Secrets resolved and Kafka options built:\n")
    for key, value in sorted(redact(options).items()):
        print(f"  {key:<42} {value}")
except Exception as exc:
    print(f"FAILED: {type(exc).__name__}: {exc}")
    print(f"\nCheck the scope named in conf/environments/{cfg.environment}.yaml exists,")
    print("and that the job's service principal has READ on it.")
    print('Try:   dbutils.secrets.list("<scope>")')
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Certificate visibility
# MAGIC The Kafka keystore/truststore are opened by the Kafka client on the **executors**.
# MAGIC A driver-only check is not sufficient, so this checks both.

# COMMAND ----------

cert_paths = [p for p in (cfg.cluster.truststore_path, cfg.cluster.keystore_path) if p]

if not cert_paths:
    print("No JVM certificates required for this cluster (public CA / SASL only).")
else:
    import os as _os
    for path in cert_paths:
        driver_ok = _os.path.exists(path)
        seen = spark.range(8).repartition(8).rdd.map(lambda _, p=path: _os.path.exists(p)).collect()
        status = "OK" if driver_ok and all(seen) else "PROBLEM"
        print(f"[{status}] {path}")
        print(f"         driver: {driver_ok}   executors: {sum(seen)}/{len(seen)} can read it")
    print("\nIf executors cannot read a Volume path, do NOT onboard this mTLS topic on this")
    print("compute profile. See docs/CONFIGURATION.md -> 'mTLS prerequisites'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Schema Registry
# MAGIC Fetches the subject's latest version. Proves URL, network path, credentials and that
# MAGIC the subject name is correct - the four things that break first.

# COMMAND ----------

import json

from kafka_ingest.schema_resolver import SchemaRegistryClient

client = SchemaRegistryClient(cfg.registry, build_registry_auth(cfg.registry, secrets))

try:
    schema_id, schema_json = client.get_latest(cfg.subject)
    print(f"Subject '{cfg.subject}' -> latest schema id {schema_id}\n")
    print(json.dumps(json.loads(schema_json), indent=2)[:3000])
except Exception as exc:
    print(f"FAILED: {type(exc).__name__}: {exc}")
    print("\n404         -> wrong subject name (check TopicNameStrategy), or the wrong registry")
    print("401/403     -> registry credentials; check the keys in conf/registries.yaml and")
    print(f"               the scope in conf/environments/{cfg.environment}.yaml")
    print("unreachable -> network path; on serverless this is usually a missing NCC rule")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Round-trip a specific schema id
# MAGIC This is the lookup the parser performs for every distinct `writer_schema_id` found in
# MAGIC a microbatch.

# COMMAND ----------

print(f"Re-fetching schema id {schema_id} by id (the per-record resolution path)...")
by_id = client.get_schema_by_id(schema_id)
assert by_id == schema_json, "schema fetched by id differs from the subject's latest"
print("OK - by-id resolution works, and the second call was served from the run cache.")

print("\nConnectivity checks passed. Kafka itself has NOT been contacted; the first real")
print("connection happens in 03_run_ingestion.")
