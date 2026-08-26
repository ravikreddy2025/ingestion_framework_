# Navigation Guide

This page tells you which files matter to *you*, and in what order.

---

## Start here, by role

| You are | Read | Time |
|---|---|---|
| **A developer** | [RUNBOOK_DEVELOPER.md](RUNBOOK_DEVELOPER.md), then this page's "10-minute path" and "trace" sections | 45 min |
| **A support engineer** | [RUNBOOK_SUPPORT.md](RUNBOOK_SUPPORT.md) — SQL and job parameters only, no code | 30 min |
| **An architect / client IT reviewer** | [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md), then [DESIGN.md](DESIGN.md) | 30 min |
| **Onboarding a source** | The README's "Onboarding a source" section, then the relevant `conf/sources/_TEMPLATE*.yaml` | 15 min |
| **Looking up one setting** | [CONFIGURATION.md](CONFIGURATION.md) | 2 min |

---

## The ten-minute path to understanding the architecture

Do these five, in order. You will understand the shape of the whole thing without reading
any implementation code.

1. **[../README.md](../README.md)** — three source types, one spine, the shared tables.
2. **[DESIGN.md §1](DESIGN.md#1-the-architecture)** — the spine plus source packages, and
   the `contracts.py` skeleton (`SourceSpec`, `RunContext`, `RunResult`).
3. **[DESIGN.md §2](DESIGN.md#2-why-the-source-contract-has-exactly-one-method)** — *why*
   the contract is one function, not `read()`/`parse()`/`write()`/`validate()`. This is the
   single decision everything else in `framework/` follows from.
4. **`conf/sources/vector_patient_events.yaml`** (Kafka), **`oracle_claim_header.yaml`**
   (Oracle), **`file_claims_inbound.yaml`** (File) — three worked examples, a few lines
   each. Faster proof that "onboarding is config only" is true than any prose.
5. **The trace below**, for whichever source type you touch first.

---

## Trace one record through every module, in execution order

The single most useful section on this page. Three traces, one per source type, in the
order the files actually execute.

### A Kafka message

| # | File | What happens to the record |
|---|---|---|
| 1 | `entrypoints/run_ingest.py` | Job starts. Parses `--source-key`, `--environment`, `--config-root`. Calls `framework/runner.py::run()`. |
| 2 | `framework/runner.py` | Reads `source_type: kafka` from the source's YAML, picks the `kafka` module from `_SOURCES`, reads the control table, resolves the full five-layer config, allocates a `run_sequence`, builds `RunContext`, dispatches. |
| 3 | `framework/config.py` | Five-layer merge (`defaults.yaml` → `defaults/kafka.yaml` → environment → source file → control table → job parameters) into one `ResolvedConfig`. No PySpark import. |
| 4 | `sources/kafka/config.py` | Builds `KafkaConfig` — its own frozen dataclass — from the framework's `ResolvedConfig`. Derives `checkpoint_path`, `txn_app_id`, resolves `cluster`/`registry` profiles. |
| 5 | `sources/kafka/run.py` → `run(ctx)` | The guard: `framework/checkpoint.py::guard_against_checkpoint_reset` refuses to start if the checkpoint is gone and landing already holds rows. |
| 6 | `sources/kafka/reader.py` | Builds the `readStream` — the four non-negotiable options (`fetchsize`-equivalents for Kafka: `includeHeaders`, `minPartitions`, `maxOffsetsPerTrigger`, the four partition options), primary vs replay positioning. The record is now inside a microbatch. |
| 7 | `sources/kafka/run.py` → `process_microbatch` | **The heart.** Everything below happens here, once per microbatch, on the same cached frame. |
| 8 | `sources/kafka/wire.py` | Confluent wire-format column expressions: magic byte, writer schema id, malformed-reason triage. |
| 9 | `sources/kafka/landing.py` | Projects raw bytes + Kafka columns + CloudEvent columns → the landing frame. Nothing is interpreted yet. |
| 10 | `framework/writers.py` → `append()` | Landing write, with `txnAppId`/`txnVersion` idempotency markers (primary), or `merge()` insert-if-absent (replay). |
| 11 | `framework/audit.py` | `landing COMPLETED` row, via `AuditWriter.emit()`. |
| 12 | `sources/kafka/registry.py` | Looks up each distinct `writer_schema_id` found in the batch, cached per run. |
| 13 | `sources/kafka/curated.py` | Avro-decodes each record with **its own** writer schema, dedups, splits into curated + quarantine. |
| 14 | `framework/writers.py` | Curated write: append (primary) or MERGE with `update_matched=True` and schema evolution (replay). |
| 15 | `framework/audit.py` | `curated COMPLETED` row, `source_detail` carrying the writer/reader schema ids. |
| 16 | `sources/kafka/listener.py` | `StreamAuditListener` reads Spark's own `StreamingQueryProgress` after `awaitTermination()` for `position_start`/`position_end`/`pending_work`. |
| 17 | `framework/runner.py` | The run-level `COMPLETED` audit row. Returns `RunResult` to the entrypoint. |

`sources/kafka/tables.py` sits alongside step 9-14 (DDL for landing/curated/quarantine,
created once at the top of `run()` from the resolved reader schema, before any row is
read). `sources/kafka/security.py` sits alongside step 6 (secrets → broker/registry
connection options).

### An Oracle row

| # | File | What happens to the row |
|---|---|---|
| 1 | `entrypoints/run_ingest.py` | Same entrypoint as Kafka — the dispatch is on `source_type`, not on which job called it. |
| 2 | `framework/runner.py` | Picks the `oracle` module, resolves config, allocates `run_sequence` (Oracle's `txnVersion`, since there is no microbatch id), dispatches. |
| 3 | `framework/config.py` | Merges `defaults.yaml` → `defaults/oracle.yaml` → environment → source file → control table (`oracle_fetch_size`, `oracle_num_partitions`, `oracle_incremental_mode`) → job parameters (a replay's cursor bounds). |
| 4 | `sources/oracle/config.py` | Builds `OracleConfig`. Resolves the `jdbc_ref` profile into a `JdbcProfile` (builds the JDBC URL from validated parts — never a pasted `user/password@host`). Derives `landing_table` from `{source_schema}`/`{source_table}` tokens, `merge_on` from `merge_keys + cursor_column`. |
| 5 | `sources/oracle/run.py` → `run(ctx)` | **Step 1 of the order that is the whole design**: capture the high-water mark — `SELECT MAX(cursor)` over what this extract can see — before reading anything. |
| 6 | `sources/oracle/query.py` | Builds the closed-interval predicate (`cursor > :last AND cursor <= :high`) as a SQL literal (Spark's JDBC source cannot bind parameters to a `dbtable` subquery), plus any `filter_criteria` / `dynamic_date_filter`. |
| 7 | `sources/oracle/reader.py` | **Step 2, the read**: probes partition bounds with `SELECT MIN/MAX` over the same query (never from stale configured bounds), then reads via `spark.read.format("jdbc")` with `fetchsize` always set, the query as a parenthesised `dbtable` subquery, and the four partition options together or not at all. |
| 8 | `sources/oracle/types.py` | Refuses a column the driver mapped to an unmappable type; compares the resolved schema against the existing landing table and stops the run before writing on a non-additive change. |
| 9 | `sources/oracle/landing.py` | Projects the extract + provenance columns (`ingest_ts`, `ingest_date`, `run_id`, `txn_version`, …), as SQL expression strings — no PySpark import. |
| 10 | `sources/oracle/tables.py` | Creates the landing table from the resolved schema, if it does not exist yet. |
| 11 | `framework/writers.py` | **Step 3, the write**: MERGE on `merge_keys + cursor_column` (or append, if `merge_keys: []` was waived) — `partition_predicate="true"`, because landing is partitioned by the date it was *written*, not a property of the row. |
| 12 | `sources/oracle/run.py` | **Step 4, only now**: `ctx.state.write_state(...)` advances the watermark — via `framework/state.py`, which **raises** on failure, unlike audit. |
| 13 | `framework/audit.py` | The run-level audit row, `source_detail` carrying the **exact SQL** this run sent to Oracle. |

A crash anywhere before step 12 leaves the watermark exactly where it was, so the next run
re-extracts the same interval — see [DESIGN.md §10](DESIGN.md#10-oracle----the-watermark-and-what-can-go-wrong).

### A file line

| # | File | What happens to the line |
|---|---|---|
| 1 | `entrypoints/run_ingest.py` | Same entrypoint again. |
| 2 | `framework/runner.py` | Picks the `file` module, resolves config, allocates `run_sequence`, dispatches. |
| 3 | `framework/config.py` | Merges `defaults.yaml` → `defaults/file.yaml` → environment → source file → control table (`file_failure_mode`, `file_max_files_per_trigger`, `file_checkpoint_reset_id`) → job parameters. |
| 4 | `sources/file/config.py` | Builds `FileConfig`. Decides Volume-governed vs `storage_ref`-governed from `source_path`'s own shape; derives `checkpoint_path` / `schema_location_path` from `{source_key}`. |
| 5 | `sources/file/run.py` → `run(ctx)` | The **same shared guard** Kafka uses: `framework/checkpoint.py::guard_against_checkpoint_reset`, called with this source's own checkpoint path, landing table and control column. |
| 6 | `framework/security.py` → `apply_session_options()` | For a `storage_ref`-governed source only: sets ADLS Gen2 session/Hadoop configuration for the duration of the read, then restores whatever was there before. A Volume-governed source applies none of this. |
| 7 | `sources/file/reader.py` | Builds the Auto Loader `readStream` — `cloudFiles.schemaLocation` always set, `rescuedDataColumn` hardcoded to `_rescued_data`, `schema_mode: provided` calls `.schema(ddl_string)`. |
| 8 | `sources/file/run.py` → `process_microbatch` | Once per microbatch. |
| 9 | `sources/file/landing.py` | Projects source columns verbatim + `_metadata`-derived columns + `filename_columns` (via `regexp_extract`) + provenance columns. |
| 10 | `sources/file/tables.py` | Creates the landing table (source columns + `filename_columns` + the fixed metadata block, `_rescued_data` counted once even though Auto Loader's real read schema already carries it) if it does not exist. |
| 11 | `framework/writers.py` → `append()` | Always an append, with idempotency markers — there is no MERGE path for this source; it is landing-only, primary-only. |
| 12 | `framework/audit.py` | `landing COMPLETED`, with the rescued-row count on `quarantined_count` — there is no separate quarantine table for this source; `_rescued_data` on the landing row **is** its quarantine. |
| 13 | `sources/file/run.py` | Reads `query.recentProgress` for `position_start`/`position_end` (Auto Loader's own opaque offset JSON, not interpreted further). |
| 14 | `framework/runner.py` | The run-level audit row. Returns `RunResult`. |

`failure_mode` decides what a non-zero rescued count does to the *run*: `FAILFAST` (the
platform default) refuses the batch; `QUARANTINE` lands it and only reports the count.

---

## Complete file map

### Documentation — `docs/` (8 files)

| File | Read it when |
|---|---|
| [NAVIGATION.md](NAVIGATION.md) | You are here |
| [DESIGN.md](DESIGN.md) | **Before changing any code.** The spine, the source contract, per-source failure scenarios, design decisions, "adding a source type" |
| [CONFIGURATION.md](CONFIGURATION.md) | Looking up a setting. Every option, tiered MUST CHANGE / NICE TO CHANGE / NO CHANGE REQUIRED |
| [RUNBOOK_DEVELOPER.md](RUNBOOK_DEVELOPER.md) | Setting up locally, extending the code, raising a PR |
| [RUNBOOK_SUPPORT.md](RUNBOOK_SUPPORT.md) | Production incident, onboarding, decommissioning. SQL and job parameters only |
| [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md) | Reviewing architecture, security, prerequisites, data protection |
| [VERIFICATION_BACKLOG.md](VERIFICATION_BACKLOG.md) | What is unverified without real infrastructure, ordered by damage |
| [IMPORT_TO_DATABRICKS.md](IMPORT_TO_DATABRICKS.md) | First time getting this into a workspace |

### The framework spine — `src/kafka_ingest/framework/` (11 modules)

Never names a source type outside `runner.py`'s `_SOURCES` dict — the CORE section 7 grep
gate, enforced in CI, is what makes that a fact rather than a convention.

| Module | Job | Open it when |
|---|---|---|
| `contracts.py` | `SourceSpec`, `RunContext`, `RunResult` — the entire source contract. No PySpark import. | Understanding what a source can and cannot depend on |
| `config.py` | Five-layer load, merge, placeholder substitution, spec-driven validation → `ResolvedConfig`. No PySpark import. | Adding a setting, debugging a config error, changing precedence |
| `control.py` | Layer 4: turns one control-table row into a plain override dict. Missing row OK, duplicates fatal, every override validated against the source's spec. | Adding a control-table column, debugging "why didn't my override apply" |
| `security.py` | `SecretResolver`, `redact()` (re-exported from `logs.py`), `apply_session_options()`. Knows nothing about what it is connecting to. | Secret resolution problems, session-scoped credentials for a new source type |
| `state.py` | Durable watermark + run sequence. Writes **raise** on failure. | A watermark or run-sequence bug — read this alongside its opposite-contract sibling, `audit.py` |
| `audit.py` | The one shared audit table. Writes **never raise**. | Adding an audit column, debugging what a run reported |
| `tables.py` | Target-name rendering + validation, `CREATE TABLE IF NOT EXISTS`. The only module issuing DDL. | Table naming, `PARTITIONED BY`/`CLUSTER BY` layout |
| `writers.py` | `append()` with idempotency markers, `merge()` with a mandatory partition predicate, `split_quarantine()`. | Anything about how a batch reaches Delta |
| `checkpoint.py` | The checkpoint-reset guard, shared by every checkpoint-based source (Kafka, File). | A checkpoint-reset incident, or a third checkpoint-based source type |
| `runner.py` | The run lifecycle: resolve → validate names → ensure framework tables → allocate run sequence → build `RunContext` → dispatch → audit. `_SOURCES` lives here. | Understanding the whole thing, or adding a source type's one dispatch line |
| `logs.py` | Structured log lines (`source_type`/`source_key`/`run_id` on every one), redaction by key-name hint list. | A credential leaking into a log line, or a new connection-option naming convention |

### Kafka — `src/kafka_ingest/sources/kafka/` (10 files)

| File | Job |
|---|---|
| `spec.py` | `SOURCE_SPEC` — every key this source accepts, structural/operational split, `control_columns`, `target_tokens=("topic_table",)` |
| `config.py` | `KafkaConfig`, built from the framework's `ResolvedConfig`. Value-level validation. No PySpark import. |
| `security.py` | Cluster/registry profiles + resolved secrets → broker and registry connection options |
| `registry.py` | Schema Registry REST client, driver-side, cached per run |
| `wire.py` | Confluent wire-framing as column expressions, including malformed-payload triage |
| `reader.py` | The four non-negotiable reader options; primary and both replay shapes |
| `landing.py` | Raw bytes + CloudEvents projection. Nothing interpreted |
| `curated.py` | Per-writer-schema decode, dedup, curated schema derivation, quarantine split |
| `tables.py` | DDL and creation for the three Kafka-owned tables |
| `listener.py` | `StreamAuditListener` — audit rows and position tracking from Spark's own streaming metrics |
| `run.py` | The guard, the three run shapes, the microbatch body, the writes |

### Oracle — `src/kafka_ingest/sources/oracle/` (8 files)

| File | Job |
|---|---|
| `spec.py` | `SOURCE_SPEC` — `structural_keys` include everything that decides *what* is extracted; only `fetch_size`/`num_partitions`/`incremental_mode` are operational |
| `config.py` | `OracleConfig`, `JdbcProfile` (builds the URL from validated parts), `ReplayControls` |
| `query.py` | The extraction query: the closed interval, `filter_criteria`/`dynamic_date_filter`, watermark-to-SQL-literal rendering |
| `reader.py` | Read options, the partition-bounds probe, the two shapes of read (serial / partitioned) |
| `types.py` | `customSchema` rendering, resolved-schema reporting, drift detection between runs |
| `landing.py` | Provenance-column projection, as SQL expression strings — no PySpark import |
| `tables.py` | Landing DDL, built from the schema the read resolved |
| `run.py` | The lifecycle — capture high water → read → write → advance. The only place the ordering lives |

### Files — `src/kafka_ingest/sources/file/` (6 files)

| File | Job |
|---|---|
| `spec.py` | `SOURCE_SPEC` — `layers=("landing",)`, `target_tokens=("target_schema", "target_table")` |
| `config.py` | `FileConfig` — decides Volume-governed vs `storage_ref`-governed from `source_path`'s shape |
| `security.py` | `StorageProfile` + resolved secrets → ADLS Gen2 session options (`account_key` / `service_principal`) |
| `reader.py` | `cloudFiles.*` Auto Loader options |
| `landing.py` | Source columns + `_metadata` + `filename_columns` + provenance projection |
| `tables.py` | Landing DDL, built from the resolved read schema plus the fixed metadata block |
| `run.py` | The guard (shared with Kafka), the microbatch body, the write |

### Entrypoints — `src/kafka_ingest/entrypoints/` (2 files)

| File | Job |
|---|---|
| `run_ingest.py` | `--source-key --environment --config-root [--job-run-id]` → `runner.run()`. One job definition serves every source of every type. |
| `run_replay.py` | Adds `--run-type` (required, no default) and every `replay_*` job parameter any shipped source declares → `runner.run()`. |

### Configuration — `conf/` (source-of-truth layout)

| Path | Layer | Purpose |
|---|---|---|
| `defaults.yaml` | 1 | Common to every source of every type: the three framework tables, table properties |
| `defaults/{kafka,oracle,file}.yaml` | 1b | Common to every source of ONE type |
| `environments/{dev,preprod,prod}.yaml` | 2 | `vars:` (catalog, ops_catalog, the three ops-catalog schemas), `defaults:`/`defaults_by_type:` overlays, register overlays |
| `sources/<source_key>.yaml` | 3 | One file per source. `source_type:` selects the spec. Optional `environments:` sub-block is layer 3a |
| `sources/_TEMPLATE*.yaml` | — | Copy these to onboard. Inert — the config validator skips underscore-prefixed files |
| `clusters.yaml`, `registries.yaml`, `jdbc.yaml`, `storage.yaml` | registers | What connections exist, in every environment. Discovered by listing `conf/*.yaml` — a new register is a new file, no code change |

### SQL — `sql/` (4 files)

All four are **templates** rendered from `conf/environments/<env>.yaml` by
`notebooks/00_validate_config` — never hand-edit a per-environment copy.

| File | Who runs it |
|---|---|
| `01_operational_config.sql` | Platform, once per environment. `ingest_control` + `ingest_state` — no `GRANT`s (Terraform-owned, D-02) |
| `02_layer_tables.sql` | Platform, optionally. Kafka's landing/quarantine + the shared audit table — the code creates these itself on first run |
| `03_support_queries.sql` | **Support, daily.** Q1-Q16 generic across every source type, Q17-Q24 Oracle-specific |
| `04_maintenance.sql` | The maintenance job. `OPTIMIZE`/`VACUUM`, plus the (deliberately commented-out) retention `DELETE` |

### Notebooks — `notebooks/` (4 files)

One escalation path per run: resolve config, touching nothing → run the tests → check
connectivity without reading data → the first real run. Thin drivers only — no logic lives
in a notebook.

| Notebook | Touches | Safe to run? |
|---|---|---|
| `00_validate_config` | Nothing | Always — no network, no secrets, no writes |
| `01_run_unit_tests` | Nothing | Always — the full suite, on a cluster with Spark already present |
| `02_check_connectivity` | Secrets + registry/driver | Reads secrets, resolves connection options; still no data movement |
| `03_run_ingestion` | The real source | Real run. Start in `dev` |

### Deployment and CI — root + `resources/` (7 files)

| File | Purpose |
|---|---|
| `databricks.yml` | Bundle definition. Targets `dev`/`preprod`/`prod`; `ops_catalog`/`data_catalog`/retention variables |
| `azure-pipelines.yml` | CI: lint + fast suite + the CORE section 7 grep gate, then `databricks bundle deploy` |
| `resources/job_ingest_primary.yml` | Daily Kafka ingestion, one task per topic |
| `resources/job_ingest_oracle.yml` | Daily Oracle extraction, one task per table. A separate job — different schedule negotiation, different blast radius |
| `resources/job_ingest_file.yml` | Daily file ingestion, one task per drop zone. Also separate, for the same reason |
| `resources/job_replay.yml` | `replay_kafka` and `replay_curated`, triggered by hand. Oracle's replay runs through the same mechanism as any other `run_type` — no separate job template |
| `resources/job_maintenance.yml` | Weekly `OPTIMIZE`/`VACUUM` per source's landing table. Does not delete data |

### Tests — `tests/` (36 files)

`pytest -m "not spark" -q` is the CI gate and needs no JVM, only the `pyspark` **package**
importable (module-level imports in `framework/audit.py` and elsewhere need it, even though
no test in the fast suite ever builds a real `SparkSession`).

| File group | Proves |
|---|---|
| `test_framework_*.py` | Config merge/validation, control-table rules, state writes raising, audit writes never raising, table naming/creation, append/merge idempotency mechanics, the checkpoint guard, the run lifecycle |
| `test_kafka_*.py` | Kafka's spec, config, reader options, wire-format parsing, curated decode, tables, the run shapes |
| `test_oracle_*.py` | Oracle's spec, config, query builder, reader options, type handling, tables, the watermark lifecycle |
| `test_file_*.py` | The file source's spec, config, security, tables, the run shapes |
| `test_shipped_config.py` | Every real `conf/sources/*.yaml` resolves in every environment, cross-product, for every source type |
| `test_shipped_jobs.py` | Every job template names a source that exists, of the right type, with sane retry/concurrency settings — and that the runbook's cited queries exist |
| `test_shipped_sql.py` | No `.sql` file references a retired identifier; support never writes to `ingest_state`; no `GRANT` anywhere |
| `test_offline_validation.py` | The five CORE section 3 offline substitutes: YAML parses, job entrypoints resolve, every `source_type` has a package, every register reference resolves, `required_keys ⊆ structural_keys` |
| `conftest.py` | `FakeSpark`, recording write stand-ins, a synthetic multi-layer config tree per source type |

---

## "I want to… → go to…" lookup

| Task | Go to |
|---|---|
| Add a Kafka topic / Oracle table / file drop zone | The README's "Onboarding a source" section |
| Add a Kafka cluster, registry, JDBC connection or storage account | `conf/clusters.yaml` / `registries.yaml` / `jdbc.yaml` / `storage.yaml` + every `conf/environments/*.yaml` |
| Change a catalog, endpoint or secret scope | `conf/environments/<env>.yaml` — **never a source file** |
| Stop a source right now | Control table, `enabled = false`. RUNBOOK_SUPPORT §5 |
| Unblock a Kafka/File source stuck on bad records | `kafka_failure_mode` / `file_failure_mode` = `QUARANTINE`. RUNBOOK_SUPPORT §5.2 / §9.2 |
| Restart after a checkpoint reset (Kafka or File) | `kafka_checkpoint_reset_id` / `file_checkpoint_reset_id`, single-use. RUNBOOK_SUPPORT §5.4a / §9.4 |
| Replay Kafka data / re-parse Kafka data | `kafka_replay` / `curated_replay`. RUNBOOK_SUPPORT §5.5 / §5.6 |
| Re-extract an Oracle window | `oracle_replay`. RUNBOOK_SUPPORT §8.5 |
| Switch an Oracle table between full and delta | `oracle_incremental_mode`. RUNBOOK_SUPPORT §8.6 |
| Force a re-read for a file source | The checkpoint-reset procedure. RUNBOOK_SUPPORT §9.7 |
| Add a source type | [DESIGN.md §12](DESIGN.md#12-adding-a-source-type) |
| Add an audit column | `framework/audit.py` (`AUDIT_SCHEMA` + `AUDIT_DDL_COLUMNS`) + `sql/02_layer_tables.sql` |
| Add a control-table column for a new source-type lever | That source's `SOURCE_SPEC.control_columns` + `sql/01_operational_config.sql` — see D-01 |
| Understand duplicates on re-run | [DESIGN.md's per-source "Failure scenarios" tables](DESIGN.md) |
| Debug a failing job | RUNBOOK_DEVELOPER §6, then `sql/03_support_queries.sql` Q1-Q3 |

---

## Files a newcomer can safely ignore at first

- `tests/*` — read one `test_shipped_config.py` case only if you want to see the
  cross-product proof; the rest are guards, not documentation.
- `sql/02_layer_tables.sql` — Kafka's own `run()` creates these tables itself. This file
  exists so the shape is reviewable in a PR and can be pre-provisioned.
- `conf/sources/_TEMPLATE*.yaml` — inert until copied.
- `notebooks/01_run_unit_tests` — useful once, on a new runtime.
- `src/kafka_ingest/entrypoints/*` — under 100 lines combined, and they contain no logic
  by design.
- `docs/build_log/` — the record of *how* this framework was built, stage by stage. Useful
  for understanding why a decision was made, not for using the framework day to day.

---

## Two things that will save you an hour

1. **Read [DESIGN.md](DESIGN.md)'s per-source failure-scenario tables before touching
   `run.py` in any source package.** Each documents which failures are self-healing, which
   duplicate, which lose rows, and the no-code-change fix for each — most incidents are
   already answered there.
2. **Never hardcode a catalog, schema, storage account or JDBC host in a source file.**
   It works in whichever environment you tested and silently breaks the others.
   `tests/test_shipped_config.py` resolves every source in every environment specifically
   to catch this.
