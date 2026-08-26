# Importing this package into Databricks

Three ways in, depending on what you are doing. Start with **Option A** to read and test
the code; use **Option C** for anything that runs on a schedule. Works the same way
regardless of which source type (Kafka, Oracle, Files) you are onboarding first — the
package layout and the entrypoints are shared.

---

## Option A — Git folder (recommended for the team's first pass)

Best for reading the code, running the notebooks and running the tests on a cluster.

1. Push `final_code/` to your Git provider (Azure DevOps, GitHub, …).
2. In Databricks: **Workspace → Repos → Add Repo**, paste the URL, clone.
3. Open `notebooks/00_validate_config` and attach a cluster (DBR 16.4 LTS or later).
4. Run it. It resolves one source's configuration and prints it — **no Kafka/Oracle/ADLS
   connection, no secrets read, nothing written.** It is the safest possible first thing to
   run.

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
databricks bundle run ingest_primary -t dev   # or ingest_oracle / ingest_file
```

`ingest_primary`, `ingest_oracle` and `ingest_file` are separate jobs, one per source type
— see [`docs/CONFIGURATION.md` §7](CONFIGURATION.md#7-job-parameters) for why they are not
one job with three kinds of task. `replay_kafka`, `replay_curated` and the `maintenance`
job are the other three `databricks bundle run` targets `resources/*.yml` defines.

Edit `databricks.yml` first — every `workspace.host` and every variable under `targets:`
is a 🔴 MUST CHANGE placeholder. See [CONFIGURATION.md §6](CONFIGURATION.md#6-deployment-config--databricksyml).

---

## Compute requirements

| Requirement | Why |
|---|---|
| **DBR 16.4 LTS or later** (Python 3.12, Spark 3.5.2) | The floor this codebase is written against — see `pyproject.toml`'s `requires-python` and VB-14. There is no ceiling: a newer runtime must keep working without a release here. |
| **Unity Catalog enabled** | 3-tier names, Volumes for certs/checkpoints/schema locations, secret governance. |
| **Executors can read UC Volumes** | mTLS Kafka topics only — the Kafka client opens the keystore on the executors. [Verify it.](CONFIGURATION.md#mtls-prerequisites--verify-once-per-cluster-before-onboarding) |
| **Network reachability** | To every Kafka bootstrap endpoint, every Schema Registry, every Oracle listener, and every ADLS account a `storage_ref`-governed file source uses. On serverless this means NCC private endpoints. A Unity Catalog Volume file source needs no ADLS network egress at all. |
| **Oracle JDBC driver, if onboarding an Oracle source** | **Not bundled with Databricks Runtime, and not installed by anything in this repository** (VB-22). Add it as a cluster library or via an init script before the first Oracle run — `resources/job_ingest_oracle.yml`'s header comment says so explicitly. |

`spark-avro` and the Kafka source are **built into DBR** — no `%pip install pyspark`, no
extra jars. (That is only a concern for local development; see
[`docs/RUNBOOK_DEVELOPER.md` §1](RUNBOOK_DEVELOPER.md#1-local-setup--no-cluster-no-kafka-no-oracle-no-adls).)

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
| Everything under `pytest -m "not spark"` — every `test_framework_*`, `test_kafka_*`, `test_oracle_*`, `test_file_*`, `test_shipped_*`, `test_offline_validation`, `test_entrypoints` case not marked `@pytest.mark.spark` | ✅, once the `pyspark` **package** is importable (`pip install pyspark==3.5.2`) — no JVM is started and no `SparkSession` is built by anything in this suite | ✅ |
| The `@pytest.mark.spark` cases (mostly inside `test_kafka_registry.py`, `test_kafka_curated.py`, `test_kafka_tables.py`, `test_kafka_listener.py`) — `pytest -m spark` | needs a JDK **and** the `spark-avro` jar, see `RUNBOOK_DEVELOPER.md` §1 | ✅ **runs out of the box** |

The Spark-marked tests are *easier* on Databricks than locally — the runtime already has
everything. Running them on the target DBR is also the only way to confirm Kafka's
`from_avro` reader/writer behaviour on **your** runtime rather than on a local Spark build.

None of the tests connect to Kafka, Oracle or ADLS, read a secret, or write to a real table.
They are safe to run on any cluster.

---

## First run against real infrastructure — suggested order

Do these in order, for whichever source type you are onboarding. Each one fails fast and
specifically if a prerequisite is missing.

1. **Provision.** Render and run `sql/01_operational_config.sql` for your environment (a
   `{placeholder}` template — `notebooks/00_validate_config` can render it for you), or let
   the framework create the control/state tables itself on first run. Either way, run
   `sql/01_operational_config.sql` at least once to get the control table.
2. **Config only.** `notebooks/00_validate_config` — proves the YAML resolves and the
   table name is well-formed. Touches nothing.
3. **Secrets and connectivity.** `notebooks/02_check_connectivity` — reads the secrets and,
   depending on source type, fetches a schema from the registry (Kafka), opens and closes
   one JDBC connection (Oracle), or lists the source path (Files). Still does not read or
   write any business data.
4. **One source, interactively.** `notebooks/03_run_ingestion` with a *non-production*
   checkpoint/state key and *scratch* target tables. Watch the audit table fill up.
5. **Schedule it.** `databricks bundle deploy -t dev`, then let the matching job
   (`ingest_primary` / `ingest_oracle` / `ingest_file`) run.

If step 4 misbehaves in dev, delete the scratch checkpoint directory (Kafka/Files) or clear
the `ingest_state` row (Oracle) and rerun — with scratch tables that is completely safe, and
it is exactly the loop the framework is designed for. Never do this in preprod/prod — use a
replay job (Kafka), the Q18 watermark-correction procedure (Oracle), or the checkpoint-reset
procedure (Files) instead; see `docs/RUNBOOK_SUPPORT.md`.

---

## Common first-run errors

| Error | Cause | Fix |
|---|---|---|
| `ConfigError: ... must be a Unity Catalog Volume path` | A cert or checkpoint path is on DBFS | Move it to a Volume. Validation is intentional. |
| `ConfigError: could not read secret ... from scope ...` | Scope missing, or the SP lacks `READ` | `dbutils.secrets.list("<scope>")` |
| `SchemaResolutionError: ... unreachable ... NCC private endpoint` (Kafka) | No network path to the registry | Firewall / NCC rule |
| `SchemaResolutionError: ... no entry for /schemas/ids/N (HTTP 404)` (Kafka) | Records were produced against a **different** registry | Check `registry:` in the source YAML |
| `ClassNotFoundException: oracle.jdbc.OracleDriver` (Oracle) | The JDBC driver is not installed on this cluster | Platform task — add it as a cluster library or init script (VB-22) |
| `RuntimeError: from_avro reader/writer schema self-check failed` (Kafka) | A runtime whose `from_avro` maps the writer/reader arguments differently than expected | There is no config workaround — see `docs/CONFIGURATION.md` §4, "`reader_schema_mode` — pick one" |
| `REFUSING TO RUN: checkpoint ... is missing` (Kafka, Files) | The checkpoint-reset guard — see `docs/DESIGN.md` | Do not delete landing rows. Use `docs/RUNBOOK_SUPPORT.md`'s checkpoint-reset procedure. |
| Job hangs then times out on connect | Broker/JDBC host wrong, or no network path | Most common onboarding error |
| Replay ran but ingested nothing (Kafka) | Replay pointed at an existing checkpoint | Use a **new** `rerun_id` — it derives an isolated checkpoint |
