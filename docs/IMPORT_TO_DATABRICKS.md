# Importing this package into Databricks

Three ways in, depending on what you are doing. Start with **Option A** to read and test
the code; use **Option C** for anything that runs on a schedule.

---

## Option A — Git folder (recommended for the team's first pass)

Best for reading the code, running the notebooks and running the tests on a cluster.

1. Push `final_code/` to your Git provider (Azure DevOps, GitHub, …).
2. In Databricks: **Workspace → Repos → Add Repo**, paste the URL, clone.
3. Open `notebooks/00_validate_config` and attach a cluster (DBR 13.3 LTS or later).
4. Run it. It resolves a topic's configuration and prints it — **no Kafka connection, no
   secrets read, nothing written.** It is the safest possible first thing to run.

The notebooks add `../src` to `sys.path`, so nothing needs installing for this path.

---

## Option B — Upload the folder directly (no Git)

For a quick look without a repo.

1. Zip `final_code/`.
2. **Workspace → (your folder) → Import → File**, upload the zip, choose *Import as folder*.
3. Same as above from step 3.

Files under `/Workspace/...` are readable with plain `open()`, so `--config-root` works
unchanged. Note there is no version history on this path — fine for evaluation, not for
production.

---

## Option C — Databricks Asset Bundle (the production path)

Deploys code, config, job definitions and schedules as one versioned unit.

```bash
pip install databricks-cli          # v0.2xx+ (the `databricks bundle` CLI)
databricks configure                # or set DATABRICKS_HOST / DATABRICKS_TOKEN

cd final_code
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run ingest_primary -t dev
```

Edit `databricks.yml` first — every `workspace.host` and every variable under `targets:`
is a 🔴 MUST CHANGE placeholder. See [CONFIGURATION.md §6](CONFIGURATION.md#6-deployment-config--databricksyml).

---

## Compute requirements

| Requirement | Why |
|---|---|
| **DBR 13.3 LTS or later** (Spark 3.4+) | `from_avro` must accept a reader schema via the `avroSchema` option. The job self-checks this at startup and fails clearly if unsupported. |
| **DBR 15.4 LTS or later, and serverless environment version 2+** — recommended, not required | Curated replay after a schema change uses `withSchemaEvolution()` on that runtime; older runtimes fall back to a session-scoped legacy flag automatically. Both paths work; confirm which one yours takes the first time you run a curated replay after an additive schema change. See `docs/DESIGN.md` §9. |
| **Unity Catalog enabled** | 3-tier names, Volumes for certs and checkpoints, secret governance. |
| **Executors can read UC Volumes** | mTLS topics only — the Kafka client opens the keystore on the executors. [Verify it.](CONFIGURATION.md#mtls-prerequisites--verify-once-per-cluster-before-onboarding) |
| **Network reachability** | To every bootstrap endpoint and every Schema Registry. On serverless this means NCC private endpoints. |

`spark-avro` and the Kafka source are **built into DBR** — no `%pip install pyspark`, no
extra jars. (That is only a concern for local development; see the main README.)

---

## Running the tests on a Databricks cluster

Open `notebooks/01_run_unit_tests` and run it, or from any notebook:

```python
%pip install pytest pyyaml
dbutils.library.restartPython()
```

```python
import os, sys, pytest
repo = "/Workspace/Repos/<you>/kafka-ingest/final_code"
sys.path.insert(0, f"{repo}/src")
sys.path.insert(0, f"{repo}/tests")
os.chdir(repo)
pytest.main(["-q", "tests"])
```

**What runs where:**

| Suite | Local laptop | Databricks cluster |
|---|---|---|
| `test_config`, `test_security`, `test_shipped_config` | ✅ | ✅ |
| `test_kafka_source`, `test_schema_resolver` (registry half), `test_pipeline`, `test_writers` | ✅ (needs `pip install pyspark`) | ✅ |
| `test_schema_resolver` (`-m spark`), `test_curated_writer`, `test_audit_and_tables` | needs a JDK **and** the `spark-avro` jar | ✅ **runs out of the box** |

The Spark-marked tests are *easier* on Databricks than locally — the runtime already has
everything. Running them on the target DBR is also the only way to confirm the `from_avro`
reader/writer behaviour on **your** runtime rather than on a local Spark build.

None of the tests connect to Kafka, read a secret, or write to a table. They are safe to
run on any cluster.

---

## First run against real Kafka — suggested order

Do these in order. Each one fails fast and specifically if a prerequisite is missing.

1. **Provision.** Render and run `sql/01_operational_config.sql` and `sql/02_layer_tables.sql`
   for your environment (both are `{catalog}` templates - `notebooks/00_validate_config` can
   render them for you), or let the framework create landing/curated/quarantine/audit on
   first run. Either way, run `sql/01_operational_config.sql` at least once to get the
   control table and its grants.
2. **Config only.** `notebooks/00_validate_config` — proves the YAML resolves and the
   table names are well-formed. Touches nothing.
3. **Secrets and registry.** `notebooks/02_check_connectivity` — reads the secrets and
   fetches a schema from the registry. Still does not touch Kafka or write anything.
4. **One topic, interactively.** `notebooks/03_run_ingestion` with a *non-production*
   `checkpoint_root` and *scratch* target tables. Watch the audit table fill up.
5. **Schedule it.** `databricks bundle deploy -t dev`, then let `ingest_primary` run.

If step 4 misbehaves, delete the scratch checkpoint directory and rerun — with scratch
tables that is completely safe, and it is exactly the loop the framework is designed for.

---

## Common first-run errors

| Error | Cause | Fix |
|---|---|---|
| `ConfigError: ... must be a Unity Catalog Volume path` | A cert or checkpoint path is on DBFS | Move it to a Volume. Validation is intentional. |
| `ConfigError: could not read secret ... from scope ...` | Scope missing, or the SP lacks `READ` | `dbutils.secrets.list("<scope>")` |
| `SchemaResolutionError: ... unreachable ... NCC private endpoint` | No network path to the registry | Firewall / NCC rule |
| `SchemaResolutionError: ... no entry for /schemas/ids/N (HTTP 404)` | Records were produced against a **different** registry | Check `registry:` in the topic YAML |
| `RuntimeError: from_avro reader/writer schema self-check failed` | DBR older than 13.3 LTS, or a runtime whose `from_avro` maps the writer/reader arguments differently than expected | Upgrade to 13.3 LTS+. There is no config workaround - `reader_schema_mode: writer` does not exist (curated stores payload as one struct column, so a per-writer-schema mode cannot work); see `docs/CONFIGURATION.md` "reader_schema_mode - pick one". |
| Job hangs then times out on connect | `bootstrap_servers` wrong, or no network path | Most common onboarding error |
| Replay ran but ingested nothing | Replay pointed at an existing checkpoint | Use a **new** `rerun_id` — it derives an isolated checkpoint |
