# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Check secrets, certificates and connectivity
# MAGIC
# MAGIC Run this **after** `00_validate_config` passes and **before** `03_run_ingestion`.
# MAGIC Works for any source type - Kafka, Oracle or Files - selected by what the source's
# MAGIC own `source_type:` declares.
# MAGIC
# MAGIC * Reads secrets - proves the scope exists and the service principal has READ
# MAGIC * Kafka: fetches a schema from the registry (a real network call; does not touch the
# MAGIC   broker). Oracle: opens and immediately closes one JDBC connection (a real network
# MAGIC   call; runs no query). Files: lists the source path (a real call against the storage
# MAGIC   account or Volume; reads no file content).
# MAGIC * **No test writes anything, and no row of business data is read.**
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
dbutils.widgets.text("source_key", "vector_patient_events", "2. Source key")
dbutils.widgets.dropdown("environment", "dev", ["dev", "preprod", "prod"], "3. Environment")

config_root = dbutils.widgets.get("config_root")
source_key = dbutils.widgets.get("source_key")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

from kafka_ingest.framework.config import read_source_type, resolve_config
from kafka_ingest.framework.security import SecretResolver, redact
from kafka_ingest.sources import file as file_source
from kafka_ingest.sources import kafka, oracle

_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}

source_type = read_source_type(config_root, source_key)
spec = _SOURCES[source_type].SOURCE_SPEC
cfg = resolve_config(config_root, source_key, environment, spec, control={}, job_parameters={})
secrets = SecretResolver()

print(f"Source:      {source_key}  (type: {source_type})")
print(f"Environment: {cfg.environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka: broker options, certificate visibility, Schema Registry

# COMMAND ----------

if source_type == "kafka":
    from kafka_ingest.sources.kafka.config import ClusterProfile, RegistryProfile
    from kafka_ingest.sources.kafka.registry import SchemaRegistryClient
    from kafka_ingest.sources.kafka.security import build_kafka_options, build_registry_auth

    cluster = ClusterProfile(name=cfg.settings["cluster"], **dict(cfg.profile("clusters", cfg.settings["cluster"])))
    registry = RegistryProfile(
        name=cfg.settings["registry"], **dict(cfg.profile("registries", cfg.settings["registry"]))
    )
    print(f"Cluster:  {cluster.name} [{cluster.auth_mode}] -> {cluster.bootstrap_servers}")
    print(f"Registry: {registry.name} [{registry.auth_mode}] -> {registry.url}")

    print("\nStep 1 - Kafka connection options (reads secrets, no network call):")
    try:
        options = build_kafka_options(cluster, secrets)
        for key, value in sorted(redact(options).items()):
            print(f"  {key:<42} {value}")
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}")
        print(f"Check the scope named in conf/environments/{environment}.yaml exists, and that")
        print("the job's service principal has READ on it. Try: dbutils.secrets.list('<scope>')")
        raise

    cert_paths = [p for p in (cluster.truststore_path, cluster.keystore_path) if p]
    print("\nStep 2 - Certificate visibility (Kafka client opens these on the EXECUTORS):")
    if not cert_paths:
        print("  No JVM certificates required for this cluster (public CA / SASL only).")
    else:
        for path in cert_paths:
            driver_ok = os.path.exists(path)
            seen = spark.range(8).repartition(8).rdd.map(lambda _, p=path: os.path.exists(p)).collect()
            status = "OK" if driver_ok and all(seen) else "PROBLEM"
            print(f"  [{status}] {path}")
            print(f"           driver: {driver_ok}   executors: {sum(seen)}/{len(seen)} can read it")
        print("\n  If executors cannot read a Volume path, do NOT onboard this mTLS topic on this")
        print("  compute profile. See docs/CONFIGURATION.md section 2, mTLS prerequisites.")

    print("\nStep 3 - Schema Registry (a real HTTP call; the broker is never contacted):")
    client = SchemaRegistryClient(registry, build_registry_auth(registry, secrets))
    subject = cfg.settings["subject"]
    try:
        schema_id, schema_json = client.get_latest(subject)
        print(f"  Subject '{subject}' -> latest schema id {schema_id}")
    except Exception as exc:
        print(f"  FAILED: {type(exc).__name__}: {exc}")
        print("  404         -> wrong subject name (check TopicNameStrategy), or the wrong registry")
        print("  401/403     -> registry credentials; check the keys in conf/registries.yaml and")
        print(f"                 the scope in conf/environments/{environment}.yaml")
        print("  unreachable -> network path; on serverless this is usually a missing NCC rule")
        raise
    print("  Re-fetching by id (the per-record resolution path)...")
    by_id = client.get_schema_by_id(schema_id)
    assert by_id == schema_json, "schema fetched by id differs from the subject's latest"
    print("  OK - by-id resolution works, and the second call was served from the run cache.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oracle: connection options, and one connect-and-close

# COMMAND ----------

if source_type == "oracle":
    from kafka_ingest.sources.oracle.config import ORACLE_DRIVER, JdbcProfile
    from kafka_ingest.sources.oracle.security import build_connection_options

    jdbc_ref = cfg.settings["jdbc_ref"]
    jdbc = JdbcProfile(name=jdbc_ref, **dict(cfg.profile("jdbc", jdbc_ref)))
    print(f"JDBC profile: {jdbc.name} -> {jdbc.url}")

    print("\nStep 1 - connection options (reads secrets, no network call):")
    try:
        options = build_connection_options(jdbc, secrets)
        for key, value in sorted(redact(options).items()):
            print(f"  {key:<12} {value}")
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}")
        print(f"Check the scope named in conf/environments/{environment}.yaml exists, and that")
        print("the job's service principal has READ on it.")
        raise

    print("\nStep 2 - open and immediately close one JDBC connection. No query is run.")
    try:
        jvm = spark._sc._jvm
        jvm.java.lang.Class.forName(ORACLE_DRIVER)
        conn = jvm.java.sql.DriverManager.getConnection(jdbc.url, options["user"], options["password"])
        conn.close()
        print("  OK - connected and closed cleanly.")
    except Exception as exc:
        print(f"  FAILED: {type(exc).__name__}: {exc}")
        print("  ClassNotFoundException -> the Oracle JDBC driver is not installed on this")
        print("                            cluster (VB-22) - a cluster library or init script")
        print("                            is a platform task, not something this repo does.")
        print("  Connection refused / timeout -> network path; on serverless this is usually a")
        print("                                  missing NCC rule (VB-12).")
        print("  ORA-01017 -> credentials wrong; check the secret keys named in conf/jdbc.yaml")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Files: storage session options (or none, for a Unity Catalog Volume), and a listing

# COMMAND ----------

if source_type == "file":
    from kafka_ingest.sources.file.config import StorageProfile, build as build_file_config
    from kafka_ingest.sources.file.security import build_storage_options
    from kafka_ingest.framework.security import apply_session_options
    from kafka_ingest.framework import tables as framework_tables

    file_cfg = build_file_config(cfg, "primary", framework_tables)
    print(f"source_path: {file_cfg.full_source_path}")

    if file_cfg.storage is None:
        print("\nThis is a Unity Catalog Volume path (D-13) - no storage_ref, no credentials")
        print("of this framework's own. Access is governed entirely by Unity Catalog grants")
        print("on the Volume itself.")
        restore = lambda: None  # noqa: E731 - nothing was applied, nothing to restore
    else:
        print(f"\nStorage profile: {file_cfg.storage_ref} [{file_cfg.storage.auth_mode}] -> {file_cfg.storage.endpoint}")
        print("Step 1 - session options (reads secrets, no network call):")
        try:
            options = build_storage_options(file_cfg.storage, secrets)
            for key, value in sorted(redact(options).items()):
                print(f"  {key:<70} {value}")
        except Exception as exc:
            print(f"FAILED: {type(exc).__name__}: {exc}")
            print(f"Check the scope named in conf/environments/{environment}.yaml exists, and")
            print("that the job's service principal has READ on it.")
            raise
        restore = apply_session_options(spark, options)

    print("\nStep 2 - list the source path (touches the storage account or Volume; reads no")
    print("file content):")
    try:
        entries = dbutils.fs.ls(file_cfg.full_source_path)
        print(f"  OK - {len(entries)} entr{'y' if len(entries) == 1 else 'ies'} found.")
        for entry in entries[:10]:
            print(f"    {entry.path}")
    except Exception as exc:
        print(f"  FAILED: {type(exc).__name__}: {exc}")
        print("  Path not found -> confirm source_path / the Volume exists")
        print("  Permission denied -> Unity Catalog grant on the Volume, or the storage")
        print("                       account's own access control")
        raise
    finally:
        restore()

# COMMAND ----------

print("\nConnectivity checks passed for this source's own systems. No business data was read,")
print("and Kafka's broker / Oracle's tables / the file source's file contents were not touched")
print("beyond what is described above. The first real read happens in 03_run_ingestion.")
