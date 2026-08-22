# Navigation Guide

This page tells you which files matter to *you*, and in what order.

---

## Start here (30 seconds)

| You want to... | Open | Time |
|---|---|---|
| **Understand what this thing does** | [README.md](../README.md), "What it does" | 5 min |
| **Understand how the code works** | [DESIGN.md](DESIGN.md) then `src/kafka_ingest/pipeline.py` | 45 min |
| **Run it / extend it** | [RUNBOOK_DEVELOPER.md](RUNBOOK_DEVELOPER.md) | 30 min |
| **Operate it in production** | [RUNBOOK_SUPPORT.md](RUNBOOK_SUPPORT.md) | 30 min |
| **Review the design or security posture** | [RUNBOOK_CLIENT_IT.md](RUNBOOK_CLIENT_IT.md) | 25 min |
| **Onboard a new topic** | [`conf/topics/_TEMPLATE.yaml`](../conf/topics/_TEMPLATE.yaml) | 10 min |
| **Get it into a workspace** | [IMPORT_TO_DATABRICKS.md](IMPORT_TO_DATABRICKS.md) | 20 min |
| **Look up one config setting** | [CONFIGURATION.md](CONFIGURATION.md) | 2 min |

**If you read nothing else:** `src/kafka_ingest/pipeline.py`. Its module docstring is the
whole design in miniature, and every other module exists to serve it.

---

## The 10-minute path to understanding the whole thing

Do these four, in order. You will understand the architecture without reading any code.

1. **[README.md](../README.md) → "What it does"** — the three tables and the one-query flow.
2. **[DESIGN.md](DESIGN.md) §1–2** — the layer model and the dependency graph.
3. **[DESIGN.md](DESIGN.md) §4** — re-runs and duplicates. *This is the section that
   explains why the code is shaped the way it is.* Skip everything else before skipping this.
4. **`conf/topics/vector_patient_events.yaml`** — 7 lines that define a whole feed. Proves
   the "adding a topic needs no code" claim faster than any prose.

---

## Follow one record through the code

The most efficient way to read the codebase. One Kafka message, start to finish, in the
order the files actually execute:

| # | File | What happens to the record |
|---|---|---|
| 1 | `entrypoints/ingest_primary.py` | Job starts. Parses 4 arguments, nothing else. |
| 2 | `config.py` | Merges 5 config layers → one `TopicConfig`. No Spark yet. |
| 3 | `security.py` | Secret *names* → real credentials from Key Vault. |
| 4 | `pipeline.py` → `run_streaming` | Guards against a deleted checkpoint, starts the query. |
| 5 | `kafka_source.py` | Builds the `readStream`. The record is now in a microbatch. |
| 6 | `pipeline.py` → `process_microbatch` | **The heart.** Everything below happens here. |
| 7 | `landing_writer.py` | Parses the wire header + CloudEvent headers → writes LANDING. |
| 8 | `audit.py` | Emits `landing COMPLETED`. |
| 9 | `schema_resolver.py` | Looks up the record's writer schema by id from the registry. |
| 10 | `curated_writer.py` | Avro-decodes it → writes CURATED (payload nested). |
| 11 | `audit.py` | Emits `curated COMPLETED`. Batch commits. |

`tables.py` sits alongside (DDL for whatever gets written). That is all 10 modules.

---

## Complete file map

### Documentation — `docs/` (7 files)

| File | Read it when |
|---|---|
| [NAVIGATION.md](NAVIGATION.md) | You are here |
| [DESIGN.md](DESIGN.md) | **Before changing any code.** File lineage, dependency graph, failure scenarios, design decisions, open items |
| [CONFIGURATION.md](CONFIGURATION.md) | Looking up a setting. Every option, tiered must/nice/no-change |
| [RUNBOOK_DEVELOPER.md](RUNBOOK_DEVELOPER.md) | Setting up locally, extending the code, raising a PR |
| [RUNBOOK_SUPPORT.md](RUNBOOK_SUPPORT.md) | Production incident, onboarding, decommissioning. SQL only |
| [RUNBOOK_CLIENT_IT.md](RUNBOOK_CLIENT_IT.md) | Reviewing architecture, security, prerequisites, data protection |
| [IMPORT_TO_DATABRICKS.md](IMPORT_TO_DATABRICKS.md) | First time getting this into a workspace |

### The framework — `src/kafka_ingest/` (10 modules)

Listed in dependency order. Each is independently testable.

| Module | Job | Open it when |
|---|---|---|
| `config.py` | 5-layer config merge → `TopicConfig`. Validation. **No PySpark import.** | Adding a setting, or debugging a config error |
| `security.py` | Key Vault secrets + Volume certs → connection options. Redaction | Auth or credential problems |
| `kafka_source.py` | `readStream` / batch read. Primary vs replay positioning | Offsets, triggers, replay bounds |
| `schema_resolver.py` | Confluent wire-format columns + Schema Registry client | Schema lookups, magic-byte parsing |
| `landing_writer.py` | Raw bytes + CloudEvent extraction → landing | CloudEvent columns, landing schema |
| `curated_writer.py` | Per-writer-schema Avro decode → curated + quarantine | Parsing, `payload` shape, dedup, `event_date` |
| `audit.py` | Per-batch, per-layer status rows + the listener | Adding an audit column |
| `tables.py` | DDL and partitioning for every table this owns | Changing a table's columns or layout |
| `pipeline.py` | **Start here.** Chained microbatch, run shapes, startup guard | Understanding the whole thing |
| `entrypoints/` | 3 thin argparse wrappers. ~40 lines each | Adding a job parameter |

### Configuration — `conf/` (10 files)

| File | Purpose |
|---|---|
| `defaults.yaml` | Layer 1. Settings common to every topic in every environment |
| `environments/{dev,preprod,prod}.yaml` | Layer 2. Catalog, brokers, secret scopes per environment |
| `topics/<topic_key>.yaml` | Layer 3. What is unique to one topic (~7 lines). `vector_patient_events.yaml` also carries an optional layer 3a — see below |
| `topics/_TEMPLATE.yaml` | **Copy this to onboard.** Inert; skipped by the validator |
| `clusters.yaml` | Register of which Kafka clusters exist (auth mode, key names, certs) |
| `registries.yaml` | Register of which Schema Registries exist |

**Rare, layer 3a — one topic, one environment.** A topic file may nest an `environments:`
block for a setting that differs in exactly one environment for that topic alone.
`vector_patient_events.yaml` is the working example — read it directly, or
[docs/CONFIGURATION.md](CONFIGURATION.md) §4 for the full precedence rule.

### SQL — `sql/` (4 files)

All four are **templates**: they hold `{catalog}` / `{ops_catalog}` placeholders, exactly like
`conf/`. Render them for an environment with `notebooks/00_validate_config` (last section) —
it substitutes from `conf/environments/<env>.yaml`, so the provisioning SQL cannot drift from
the table names the code uses. Never hand-edit a per-environment copy.

| File | Who runs it |
|---|---|
| `01_operational_config.sql` | Platform, once per environment. Control table + grants |
| `02_layer_tables.sql` | Platform, once per environment. Landing/audit/quarantine DDL (the code also creates these) |
| `03_support_queries.sql` | **Support, daily.** Triage queries Q1–Q10 and the no-deploy fixes |
| `04_maintenance.sql` | The maintenance job. `OPTIMIZE`/`VACUUM`, plus the retention policy (its `DELETE` is deliberately commented out) |

### Notebooks — `notebooks/` (4 files)

Run in order. Each is safe up to the one before the next.

| Notebook | Touches | Safe to run? |
|---|---|---|
| `00_validate_config` | Nothing | Always — no Kafka, no secrets, no writes |
| `01_run_unit_tests` | Nothing | Always — full test suite on the cluster |
| `02_check_connectivity` | Secrets + registry | Reads secrets, no Kafka, no writes |
| `03_run_ingestion` | **Kafka + tables** | Real run. Start in `dev` |

### Deployment and CI — root + `resources/` (6 files)

| File | Purpose |
|---|---|
| `databricks.yml` | Bundle definition. Targets `dev` / `preprod` / `prod`, plus the retention and warehouse variables |
| `azure-pipelines.yml` | CI: lint + fast suite on every PR, then `databricks bundle deploy`. The deploy stage needs credentials wiring before it runs |
| `resources/job_ingest_primary.yml` | The scheduled daily job. One task per topic |
| `resources/job_replay.yml` | Both replay jobs, triggered by hand |
| `resources/job_maintenance.yml` | Weekly `OPTIMIZE`/`VACUUM`. Deletes nothing. Needs `sql_warehouse_id` set |
| `pyproject.toml` | Package metadata, the 3 console entrypoints, and the ruff/mypy/pytest config |

### Tests — `tests/` (10 files)

`pytest -m "not spark"` is the CI gate and needs no JVM. The `spark`-marked tests need a JDK
and the `spark-avro` jar locally; on a cluster `notebooks/01_run_unit_tests` runs everything.

| File | Proves |
|---|---|
| `test_config.py` | 5-layer merge precedence, replay checkpoint isolation, validation |
| `test_shipped_config.py` | **Every shipped topic resolves in every environment.** CI gate |
| `test_security.py` | Auth options for PLAIN/SCRAM/mTLS, credential redaction |
| `test_kafka_source.py` | Source positioning, triggers, timestamp handling |
| `test_schema_resolver.py` | Registry lookup/caching, wire-format byte parsing |
| `test_curated_writer.py` | **The central test:** mixed schema versions in one batch. Plus CloudEvents, `event_date`, quarantine |
| `test_audit_and_tables.py` | Audit row ↔ schema ↔ DDL alignment, and `sql/02` ↔ `tables.py` column drift |
| `test_pipeline.py` | Run-shape dispatch, `txnAppId` identity and stability, the checkpoint-reset guard |
| `test_writers.py` | **What the writers hand Delta:** idempotency markers, append-vs-MERGE, schema evolution on replay |
| `conftest.py` | Fixtures: fake Spark, a recording write path, a stand-in `delta.tables`, a synthetic 5-layer config tree |

---

## "I want to..." lookup

| Task | Go to |
|---|---|
| Add a topic | `conf/topics/_TEMPLATE.yaml` + RUNBOOK_DEVELOPER §3 |
| Add a Kafka cluster or registry | `conf/clusters.yaml` + every `conf/environments/*.yaml` |
| Change a catalog or endpoint | `conf/environments/<env>.yaml` — **never a topic file** |
| Stop a topic right now | Control table, `enabled = false`. RUNBOOK_SUPPORT §5.8 |
| Stop several topics at once (shared cluster/domain incident) | `sql/03_support_queries.sql` Q6b/Q6c. RUNBOOK_SUPPORT §5.8 |
| Unblock a stuck stream | Control table, `on_deser_error`. RUNBOOK_SUPPORT §5.2 |
| Replay from an offset | `job_replay.yml` params. RUNBOOK_SUPPORT §5.5 |
| Fix badly-parsed data | Curated replay. RUNBOOK_SUPPORT §5.6 |
| Decommission a topic | RUNBOOK_SUPPORT §4 — **order matters** |
| Restart a topic after its primary checkpoint was deleted | Control table, `checkpoint_reset_id`. RUNBOOK_SUPPORT §5.4a — a Kafka replay alone does **not** resolve this |
| Add a CloudEvent attribute | `landing_writer.py` + `tables.py`. RUNBOOK_DEVELOPER §5 |
| Add an audit column | `audit.py` + `tables.py` + `sql/02_layer_tables.sql` |
| Override one setting for one topic in one environment (rare) | `conf/topics/<key>.yaml` `environments:` block — working example: `vector_patient_events.yaml`. CONFIGURATION.md §4 |
| Change Delta table properties (auto-optimize, CDF, retention floor) | `conf/defaults.yaml` `table_properties`, or a topic's own. CONFIGURATION.md, "Table properties" |
| Understand duplicates on re-run | **DESIGN.md §4** |
| Understand why payload is nested | DESIGN.md §5 |
| Debug a failing job | RUNBOOK_DEVELOPER §6, then `sql/03_support_queries.sql` Q2 |

---

## Files you can safely ignore at first

- `tests/*` — read `test_curated_writer.py` only if you want to see the framework's central
  claim proved. The rest are guards, not documentation.
- `sql/02_layer_tables.sql` — the code creates these tables itself. This file exists so the
  schema is reviewable in a PR and can be pre-granted.
- `conf/topics/_TEMPLATE.yaml` — inert until you copy it.
- `notebooks/01_run_unit_tests` — useful once, on a new runtime.
- `src/kafka_ingest/entrypoints/*` — ~40 lines each, and they contain no logic by design.

## Outside this folder

| Path | What it is |
|---|---|
| `../internal/` | The AI designer prompt for rebuilding this framework. Not client-facing |
| `../archive/` | The superseded v1 Bronze/Silver implementation. **Do not deploy or copy from it** — the layer model, table layout and monitoring design all changed |

---

## Two things that will save you an hour

1. **Read DESIGN.md §4 before touching `pipeline.py` or either writer.** The idempotency
   mechanism is subtle, and the one failure mode that looks like success (a deleted
   checkpoint) is documented there with the guard that prevents it.
2. **Never hardcode a catalog in a topic file.** It works in prod and silently breaks dev.
   Use `{catalog}` in `defaults.yaml`. `tests/test_shipped_config.py` resolves every topic
   in every environment specifically to catch this.
