# Multi-Source Ingestion Framework

A config-driven ingestion framework on Databricks. Three source types share one
configuration, control, audit, state and logging spine:

| Source | Execution model | Layers |
|---|---|---|
| **Kafka** | Structured Streaming, `availableNow`, `foreachBatch` | landing, curated, quarantine |
| **Oracle** | JDBC batch, incremental by cursor or filter | landing only |
| **Files (ADLS / UC Volumes)** | Auto Loader, `availableNow` | landing only |

Adding a source of an existing type is **config only** — one YAML file and one job task,
no code change. Adding a fourth source *type* is a new package under `sources/` and zero
changes under `framework/` — see [docs/DESIGN.md §12](docs/DESIGN.md#12-adding-a-source-type)
for what that actually takes, and the grep gate that proves it stays true.

---

## Read in this order

> ### New to this codebase? Start with [docs/NAVIGATION.md](docs/NAVIGATION.md)
> Every file mapped: a role-based start-here table, a ten-minute path to the architecture,
> and a trace of one record through every module — once per source type.

| You are | Read |
|---|---|
| **A developer** maintaining or extending this | [docs/RUNBOOK_DEVELOPER.md](docs/RUNBOOK_DEVELOPER.md) → then [docs/DESIGN.md](docs/DESIGN.md) |
| **Production support** | [docs/RUNBOOK_SUPPORT.md](docs/RUNBOOK_SUPPORT.md) — SQL and job parameters only, no code reading |
| **Client IT / architecture** | [docs/RUNBOOK_CLIENT_IT.md](docs/RUNBOOK_CLIENT_IT.md) — design, security, prerequisites, data protection |

| Document | Contents |
|---|---|
| [docs/NAVIGATION.md](docs/NAVIGATION.md) | Map of every file. Where to start, what to ignore, "I want to… → go to…" |
| [docs/DESIGN.md](docs/DESIGN.md) | The spine, the source contract, per-source failure scenarios, design decisions |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Every setting, tiered **MUST CHANGE / NICE TO CHANGE / NO CHANGE REQUIRED** |
| [docs/VERIFICATION_BACKLOG.md](docs/VERIFICATION_BACKLOG.md) | Assumptions that need a real Kafka/Oracle/ADLS/Databricks environment to confirm |
| [docs/IMPORT_TO_DATABRICKS.md](docs/IMPORT_TO_DATABRICKS.md) | Get it into a workspace and run the tests |

---

## Environment: local development only

There is **no** Kafka, Oracle, ADLS, Databricks workspace, cluster or JVM in this
repository's own environment. The only gate that can be run here is:

```bash
ruff check src tests
ruff format --check src tests
pytest -m "not spark" -q
```

Everything that needs real infrastructure is written defensively and recorded in
[docs/VERIFICATION_BACKLOG.md](docs/VERIFICATION_BACKLOG.md) instead of asserted as working.

---

## What it does

```
framework/          config, control, security, state, audit, tables, writers, runner, logs,
                     checkpoint  -- the spine every source shares. Never names a source type.
sources/kafka/       spec.py + run(ctx)  -- readStream, foreachBatch, landing+curated+quarantine
sources/oracle/      spec.py + run(ctx)  -- JDBC batch read, cursor/filter incremental, landing only
sources/file/        spec.py + run(ctx)  -- Auto Loader, availableNow, landing only
entrypoints/         run_ingest.py, run_replay.py -- thin argparse wrappers, one per shape
```

A source's entire public surface is `SOURCE_SPEC` (data: which keys it accepts, where) and
`run(ctx) -> RunResult` (one function). There is no `read()` / `parse()` / `write()` /
`validate()` on that contract — a Kafka `foreachBatch` body and a bounded JDBC read share
governance, not steps. See [docs/DESIGN.md §1-2](docs/DESIGN.md) for the full argument.

| Property | How |
|---|---|
| No duplicates on re-run | Delta idempotent writes (`txnAppId`/`txnVersion`) on appends; a MERGE's idempotency comes from its key instead |
| One shared audit table | `{ops_catalog}.{audit_schema}.ingest_audit` — one row per (run, layer, status), every source type |
| One shared control table | `{ops_catalog}.{control_schema}.ingest_control` — support edits it, no deploy; a column specific to one source type is named `<source_type>_<setting>` |
| Durable state, not derived from audit | `{ops_catalog}.{control_schema}.ingest_state` — watermarks and run sequences. State writes **raise** on failure; audit writes never do |
| Structural fields are never operationally overridable | Partitioning, merge/dedup keys, target names, Oracle's `source_schema`/`source_table`/`filter_criteria`, a file's target path — all PR-only |
| No plaintext secrets | Every credential via `dbutils.secrets.get()`; scope/key **names** come from config, values never appear in this repository |

---

## Onboarding a source

Every source type follows the same shape: copy a template, answer the questions it asks,
add one job task, run the config test, open a PR. No Python changes, ever.

### Kafka

1. Copy [`conf/sources/_TEMPLATE.yaml`](conf/sources/_TEMPLATE.yaml) to
   `conf/sources/<source_key>.yaml`. Fill in `topic`, `domain`, `cluster`, `registry`,
   `subject`. Confirm the topic's partition count and retention with the producing team —
   `min_partitions` and the job schedule both depend on them.
2. Add a task to `resources/job_ingest_primary.yml` with the new `source-key`.
3. `pytest tests/test_shipped_config.py -q`, then open a PR.

### Oracle

1. Copy [`conf/sources/_TEMPLATE_oracle.yaml`](conf/sources/_TEMPLATE_oracle.yaml). Ask the
   source team the four questions at the top of the template — the cursor column and when
   it is stamped, the stable key (`merge_keys`), the partition column, and any LOB/RAW/
   INTERVAL/TZ columns — before filling in anything else.
2. `CREATE SCHEMA IF NOT EXISTS <catalog>.oracle_<schema>;` in every environment — the
   framework creates tables, never schemas.
3. Add a task to `resources/job_ingest_oracle.yml`.
4. `pytest tests/test_shipped_config.py -q`, then open a PR.

### Files (ADLS / UC Volumes)

1. Copy [`conf/sources/_TEMPLATE_file.yaml`](conf/sources/_TEMPLATE_file.yaml). Ask whoever
   owns the landing zone the four questions at the top — the full schema and whether it
   drifts, whether files are ever rewritten in place, whether the filename carries data,
   and roughly how many files land per day.
2. Prefer a Unity Catalog Volume `source_path` when one is available (no credentials, no
   `storage_ref`); otherwise reference a profile in `conf/storage.yaml`.
   `CREATE SCHEMA IF NOT EXISTS <catalog>.<target_schema>;` in every environment first.
3. Add a task to `resources/job_ingest_file.yml`.
4. `pytest tests/test_shipped_config.py -q`, then open a PR.

**There is no replay job for the file source, and none is planned** — a missing checkpoint
already makes Auto Loader re-read everything on its own. See
[docs/RUNBOOK_SUPPORT.md §9](docs/RUNBOOK_SUPPORT.md) for the recovery procedure instead.

---

## Replay, in one paragraph per source

- **Kafka** — `kafka_replay` re-pulls from the broker from an offset or timestamp into an
  isolated checkpoint and Delta transaction identity; `curated_replay` re-parses landing
  with no broker contact, so it works past Kafka retention. Both need a `rerun_id`.
- **Oracle** — `oracle_replay` re-extracts a cursor interval you bound explicitly
  (`replay_cursor_start`/`replay_cursor_end`). It never writes `ingest_state`, so the
  scheduled delta load is undisturbed.
- **Files** — no replay job. Recovery is the checkpoint-reset procedure: delete the
  affected landing partition(s) first, set a fresh `file_checkpoint_reset_id`, run the
  normal job. A bounded re-read narrows `source_path`/`path_glob` for that one run.

Full playbooks, SQL and job parameters only: [docs/RUNBOOK_SUPPORT.md](docs/RUNBOOK_SUPPORT.md).

---

## Testing

```bash
ruff check src tests            # lint. CI runs exactly this.
pytest -m "not spark" -q        # config, control, state, audit, writers, every source's
                                 # spec/config/run against stand-ins. No JVM. This is the CI gate.
pytest -q                       # everything, including the few Spark-marked tests
```

`azure-pipelines.yml` runs lint plus the fast suite on every pull request, plus the CORE
section 7 grep gate (`framework/` must never name a source type outside `runner.py`'s
`_SOURCES` dict), plus `databricks bundle deploy`.

Run the full suite (including the Spark-marked tests) on Databricks via
`notebooks/01_run_unit_tests` — DBR ships Spark, so nothing extra is needed there. See
[docs/RUNBOOK_DEVELOPER.md §1](docs/RUNBOOK_DEVELOPER.md) for local setup on a laptop,
including the two non-obvious prerequisites for the Spark-marked tests.

**No test connects to Kafka, Oracle, ADLS, reads a secret, or writes to a real table.**

---

## Before go-live

The full list, ordered by how much breaks if the assumption is wrong, is
[docs/VERIFICATION_BACKLOG.md](docs/VERIFICATION_BACKLOG.md). The three most dangerous:

1. **Oracle JDBC driver version** (VB-22) — nothing here installs it, and its version
   decides whether `NUMBER`/`DATE` type mappings (VB-02, VB-03) are even askable questions.
2. **`customSchema` semantics** (VB-23) — if it is read as the complete schema rather than
   a per-column override, every unnamed column silently drops.
3. **`ingest_state`'s MERGE actually upserting** (VB-15) — every source type's idempotency
   depends on it; a silent no-op here looks exactly like a healthy job doing nothing.
