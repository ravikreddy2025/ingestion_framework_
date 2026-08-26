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
# MAGIC * Then four **first-connection verification probes**, one per open
# MAGIC   `docs/VERIFICATION_BACKLOG.md` entry a developer must clear before any real
# MAGIC   ingestion (VB-19, VB-27, VB-01, VB-29 - see `docs/RUNBOOK_DEVELOPER.md`'s
# MAGIC   "First-connection verification" table). **Every probe is read-only: no write to a
# MAGIC   landing table, no state advanced.** VB-27 is the one exception that writes
# MAGIC   anything at all, and it writes only to a scratch table it creates and drops itself.
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
dbutils.widgets.text(
    "vb27_scratch_table", "<CHANGE_ME_catalog>.<CHANGE_ME_schema>.vb27_column_order_probe", "4. VB-27 scratch table"
)

config_root = dbutils.widgets.get("config_root")
source_key = dbutils.widgets.get("source_key")
environment = dbutils.widgets.get("environment")
vb27_scratch_table = dbutils.widgets.get("vb27_scratch_table")

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
        print(f"\naccess_mode: {file_cfg.access_mode} (D-15) - no storage_ref, no credentials")
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

# MAGIC %md
# MAGIC ## First-connection verification probes
# MAGIC
# MAGIC Four probes from `docs/VERIFICATION_BACKLOG.md`, ordered by that file's own damage
# MAGIC ranking. Each is safe to run before any real ingestion: **read-only, no write to a
# MAGIC landing table, no state advanced.** VB-27 is the only one that writes anything at
# MAGIC all, and it writes to - then drops - a scratch table of its own, never a real table.
# MAGIC
# MAGIC See `docs/RUNBOOK_DEVELOPER.md`'s "First-connection verification" table for what each
# MAGIC one proves and what to change in the code if it fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ### VB-29 - do this environment's secret scopes resolve, and do the key names
# MAGIC ### `conf/jdbc.yaml` / `conf/storage.yaml` declare actually exist in them?
# MAGIC
# MAGIC Read-only: `dbutils.secrets.list()` returns key **names** only, never a value, so
# MAGIC this never touches an actual credential. Runs regardless of which `source_key` is
# MAGIC selected above - `cfg.registers` already carries every profile in every register for
# MAGIC the selected environment, not just the one this source references.

# COMMAND ----------

print(f"\nVB-29 - secret scopes and key names for environment '{environment}':")

_scopes: dict[str, list[str]] = {}
for register_name, profiles in cfg.registers.items():
    for profile_name, profile in profiles.items():
        scope = profile.get("secret_scope")
        if scope:
            _scopes.setdefault(scope, []).append(f"{register_name}.{profile_name}")

print(f"\nStep 1 - {len(_scopes)} distinct secret scope(s) named across every register:")
_listed: dict[str, list[str]] = {}
for scope, users in sorted(_scopes.items()):
    try:
        keys = [entry.key for entry in dbutils.secrets.list(scope)]
        _listed[scope] = keys
        print(f"  [OK]      {scope:<28} ({len(keys)} key(s))  used by {', '.join(users)}")
    except Exception as exc:
        print(f"  [FAILED]  {scope:<28} {type(exc).__name__}: {exc}  used by {', '.join(users)}")
        print(f"            Check the scope exists in conf/environments/{environment}.yaml and that")
        print("            the job's service principal has READ on it.")

print("\nStep 2 - every key name conf/jdbc.yaml and conf/storage.yaml declare, checked")
print("against the scope each profile resolves to (names only, never fetched):")
_JDBC_KEY_FIELDS = ("username_key", "password_key")
_STORAGE_KEY_FIELDS = ("account_key_secret_key", "client_id_secret_key", "client_secret_secret_key")
for register_name, key_fields in (("jdbc", _JDBC_KEY_FIELDS), ("storage", _STORAGE_KEY_FIELDS)):
    for profile_name, profile in sorted(cfg.registers.get(register_name, {}).items()):
        scope = profile.get("secret_scope")
        available = _listed.get(scope)
        for field in key_fields:
            key_name = profile.get(field)
            if not key_name:
                continue  # not every profile sets every field - e.g. only one storage auth_mode applies
            if available is None:
                print(f"  [SKIPPED] {register_name}.{profile_name}.{field}='{key_name}' - scope '{scope}' above failed")
            elif key_name in available:
                print(f"  [OK]      {register_name}.{profile_name}.{field}='{key_name}' found in '{scope}'")
            else:
                print(f"  [MISSING] {register_name}.{profile_name}.{field}='{key_name}' NOT in '{scope}'")
                print(f"            Populate this key in the '{scope}' scope, or fix the field name in")
                print(f"            conf/{register_name}.yaml if it was typed wrong.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VB-01 - does a partitioned Oracle JDBC read actually produce `numPartitions` tasks?
# MAGIC
# MAGIC Read-only: `partition_bounds()` is one `SELECT MIN/MAX` round trip, and
# MAGIC `.rdd.getNumPartitions()` reads Spark's own partitioning plan - it does not execute
# MAGIC the query or fetch a single row.

# COMMAND ----------

if source_type == "oracle":
    from kafka_ingest.framework import tables as framework_tables
    from kafka_ingest.sources.oracle import config as oracle_config
    from kafka_ingest.sources.oracle import query as oracle_query
    from kafka_ingest.sources.oracle import reader as oracle_reader

    oracle_cfg = oracle_config.build(cfg, "primary", framework_tables)
    print(f"\nVB-01 - partitioned read for '{oracle_cfg.source_ref}':")
    if not oracle_cfg.partition_column or oracle_cfg.num_partitions <= 1:
        print(
            f"  SKIPPED - partition_column={oracle_cfg.partition_column!r}, "
            f"num_partitions={oracle_cfg.num_partitions}. This source reads serially; VB-01 only "
            "matters once a partition_column and num_partitions > 1 are configured."
        )
    else:
        if oracle_cfg.is_cursor:
            high_water_row = oracle_reader.read_scalar_row(
                spark, oracle_cfg, secrets, oracle_query.high_water_query(oracle_cfg)
            )
            base_query = oracle_query.build_query(oracle_cfg, run_high_water=str(high_water_row["high_water"]))
        else:
            base_query = oracle_query.build_query(oracle_cfg)
        bounds = oracle_reader.partition_bounds(spark, oracle_cfg, secrets, base_query)
        df = oracle_reader.read(spark, oracle_cfg, secrets, base_query, bounds)
        actual = df.rdd.getNumPartitions()
        expected = oracle_cfg.num_partitions
        status = "OK" if actual == expected else "SILENT FALLBACK"
        print(f"  bounds={bounds}  expected numPartitions={expected}  actual={actual}  [{status}]")
        if actual != expected:
            print("  Spark silently read this serially despite partition_column being set. Per VB-01,")
            print("  sources/oracle/reader.py must always build a parenthesised dbtable subquery when")
            print("  partitionColumn is set, never the bare `query` option for a partitioned read.")
else:
    print(f"\nVB-01 only applies to an Oracle source; source_type is '{source_type}'. Skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VB-19 - does the rendered `TO_TIMESTAMP` watermark literal compare correctly
# MAGIC ### against a real Oracle cursor column?
# MAGIC
# MAGIC Read-only: both queries below are `SELECT COUNT(*)`, over a wide, fixed bound - no
# MAGIC row content is fetched and no state is touched. Uses the framework's own
# MAGIC `build_query()`, the exact code path a real run takes, against a `TO_DATE` control
# MAGIC rendered the same way but naming a different Oracle function - if the two counts
# MAGIC disagree, the comparison in `sources/oracle/query.py::_literal()` is wrong for this
# MAGIC column's actual Oracle type (VB-03's question, arriving here as a predicate).

# COMMAND ----------

if source_type == "oracle" and oracle_cfg.is_cursor and oracle_cfg.cursor_type == "timestamp":
    _wide_start, _wide_end = "1901-01-01 00:00:00", "2999-12-31 23:59:59"
    to_timestamp_query = oracle_query.build_query(oracle_cfg, last_watermark=_wide_start, run_high_water=_wide_end)
    to_date_query = to_timestamp_query.replace("TO_TIMESTAMP(", "TO_DATE(")
    print(f"\nVB-19 - comparing the two literal forms over {oracle_cfg.cursor_column}:")
    print(f"  Framework's TO_TIMESTAMP query:\n    {to_timestamp_query}")
    print(f"\n  TO_DATE control query:\n    {to_date_query}")
    count_sql_a = f"SELECT COUNT(*) AS c FROM ({to_timestamp_query}) q"
    count_sql_b = f"SELECT COUNT(*) AS c FROM ({to_date_query}) q"
    count_a = oracle_reader.read_scalar_row(spark, oracle_cfg, secrets, count_sql_a)
    count_b = oracle_reader.read_scalar_row(spark, oracle_cfg, secrets, count_sql_b)
    n_a = count_a["c"] if count_a else None
    n_b = count_b["c"] if count_b else None
    print(f"\n  TO_TIMESTAMP count={n_a}   TO_DATE count={n_b}   [{'OK' if n_a == n_b else 'MISMATCH'}]")
    if n_a != n_b:
        print("  The two literal forms disagree over the same bounds. Change the format model or")
        print("  the function in sources/oracle/query.py::_literal() - it is the only place a")
        print("  watermark becomes SQL, and every predicate goes through it.")
elif source_type == "oracle":
    print(
        f"\nVB-19 - SKIPPED. cursor_type={oracle_cfg.cursor_type!r}, is_cursor={oracle_cfg.is_cursor}. "
        "VB-19 concerns the TO_TIMESTAMP literal path, which only a timestamp cursor takes."
    )
else:
    print(f"\nVB-19 only applies to an Oracle source; source_type is '{source_type}'. Skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VB-27 - does a Delta append reconcile columns by NAME when order differs?
# MAGIC
# MAGIC The only probe here that writes anything - to the scratch table named by the
# MAGIC `vb27_scratch_table` widget, which it creates and then **drops itself** at the end.
# MAGIC Never touches a real landing table. Change the widget to a catalog.schema you can
# MAGIC create a table in if the default does not resolve for you.

# COMMAND ----------

print(f"\nVB-27 - column-order-vs-name probe, using scratch table '{vb27_scratch_table}':")
try:
    spark.sql(f"CREATE TABLE {vb27_scratch_table} (a STRING, b STRING, c STRING) USING DELTA")
    reordered = spark.createDataFrame([("C", "A", "B")], "c STRING, a STRING, b STRING")
    reordered.write.format("delta").mode("append").saveAsTable(vb27_scratch_table)
    row = spark.table(vb27_scratch_table).collect()[0]
    landed_by_name = row["a"] == "A" and row["b"] == "B" and row["c"] == "C"
    print(f"  wrote columns (c, a, b) = ('C', 'A', 'B'); read back a={row['a']} b={row['b']} c={row['c']}")
    print(f"  [{'OK - reconciled by NAME' if landed_by_name else 'POSITIONAL - see VB-27'}]")
    if not landed_by_name:
        print("  sources/file/landing.py::project() must end with an explicit .select() naming every")
        print("  column in the exact order tables.landing_columns() declares - the same discipline")
        print("  Kafka's and Oracle's projections already follow.")
except Exception as exc:
    print(f"  FAILED: {type(exc).__name__}: {exc}")
    print("  Change the vb27_scratch_table widget to a catalog.schema you have CREATE TABLE on.")
    raise
finally:
    spark.sql(f"DROP TABLE IF EXISTS {vb27_scratch_table}")

# COMMAND ----------

print("\nConnectivity checks passed for this source's own systems. No business data was read,")
print("and Kafka's broker / Oracle's tables / the file source's file contents were not touched")
print("beyond what is described above. The first real read happens in 03_run_ingestion.")
