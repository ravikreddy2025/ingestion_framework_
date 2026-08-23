# Stage 0 Report -- Orientation and the verification backlog

No file under `src/`, `conf/`, `resources/` or `sql/` was changed in this stage. This is
reconnaissance and a deliverable for the human, per `.claude/build/STAGE_0_orientation.md`.

---

## Inventory

Verdicts are against the target layout in CORE section 4.3
(`framework/` + `sources/kafka|oracle|file/` + `entrypoints/`). "Stage" names which later
stage is expected to act on the file; Stage 0 acts on none of them.

### `src/kafka_ingest/` -- the current Kafka-only package

| File | What it does | Verdict | Target / stage |
|---|---|---|---|
| `__init__.py` | Package docstring, module map, `__version__` | REWRITE | Docstring describes a Kafka-only module map; rewrite once framework/sources split lands (Stage 1-3) |
| `config.py` | Five-layer YAML+control-table merge, `TopicConfig`, `KafkaClusterProfile`, `SchemaRegistryProfile`, `RunContext`, placeholder substitution, `table_name_for()` | REWRITE | Splits into `framework/contracts.py` + `framework/config.py` (generic, spec-driven) + `sources/kafka/spec.py` (cluster/registry profiles, Kafka structural/operational keys). The single biggest rewrite in the repo -- Stage 1 for the generic half, Stage 3 for the Kafka half |
| `security.py` | Secret/cert resolution -> connection options; `SecretResolver`, `build_kafka_options`, `build_registry_auth`, `redact()` | REWRITE (split) | `SecretResolver` + `redact()` are already source-agnostic -> `framework/security.py`. `build_kafka_options` / `build_registry_auth` are Kafka-specific -> `sources/kafka/`. Stage 1 + Stage 3 |
| `kafka_source.py` | Builds the Kafka `readStream`/`spark.read` reader; primary vs replay positioning; trigger resolution | MOVE | Wholesale into `sources/kafka/` (reader construction). Logic is already source-scoped and needs no framework dependency. Stage 3 |
| `schema_resolver.py` | Confluent wire-format column expressions; Schema Registry REST client | MOVE | Wholesale into `sources/kafka/`. Entirely Kafka/Avro-specific already. Stage 3 |
| `landing_writer.py` | Kafka landing projection (CloudEvent columns, wire-format columns) + append/MERGE write | REWRITE (split) | Projection is Kafka-specific -> `sources/kafka/`. The append-with-txnAppId / MERGE-on-key mechanics are the pattern `framework/writers.py` should generalize (CORE 4.3), so the *write* half is reusable scaffolding, not a copy-paste. Stage 1 + Stage 3 |
| `curated_writer.py` | Per-writer-schema Avro decode, dedup, curated schema derivation, append/MERGE-with-schema-evolution write | REWRITE (split) | Avro decode/dedup is Kafka-specific -> `sources/kafka/`. The MERGE-with-schema-evolution fallback (`withSchemaEvolution()` vs session flag) is the one non-trivial mechanism worth lifting into `framework/writers.py` so Oracle's MERGE path (Stage 4) doesn't reinvent it. Stage 1 + Stage 3 |
| `audit.py` | `AUDIT_SCHEMA`, `AuditWriter`, `StreamAuditListener` (Structured Streaming progress -> audit rows) | REWRITE (split) | `AuditWriter`/`AUDIT_SCHEMA` generalize into `framework/audit.py` under the CORE 5.4 shared schema (`source_type`, `source_key`, `source_ref`, `position_start/end`, `source_detail` replace `topic_key`/`topic`/offset columns). `StreamAuditListener` is Structured-Streaming-only -> `sources/kafka/`. Stage 2 + Stage 3 |
| `tables.py` | DDL constants and `ensure_*` / `_create` helpers for landing, curated, quarantine, audit | REWRITE (split) | `KAFKA_COLUMNS` / `CLOUDEVENT_COLUMNS` are payload columns, not framework columns (CORE 5.1 says these stay put) -> `sources/kafka/`. `_create()`, `table_exists()`, `_properties_clause()`, and the audit DDL generalize into `framework/tables.py`. Stage 2 + Stage 3 |
| `pipeline.py` | `run()` dispatch, `foreachBatch` microbatch body, checkpoint-reset guard, `txn_app_id` derivation, three run shapes | REWRITE (split) | The single dispatch point (`run()`), run-id generation, and the general shape of "read -> write -> commit -> advance state" belong in `framework/runner.py`. The `foreachBatch` chaining, the checkpoint-reset guard (fundamentally a Structured Streaming concept), and the three Kafka run shapes are `sources/kafka/run.py`. This and `config.py` are the two files where getting the split right matters most. Stage 1 + Stage 3 |
| `entrypoints/__init__.py` | `add_common_arguments`, `bootstrap()` (parse args -> resolve config) | REWRITE | `--topic-key` becomes `--source-key`; `resolve_topic_config` becomes the generic `framework/config.py` entry point. Stage 3 (or wherever entrypoints are unified) |
| `entrypoints/ingest_primary.py` | Primary-run entrypoint, one job serves every topic | REWRITE | Becomes `entrypoints/run_ingest.py`, source-agnostic (CORE 4.3) |
| `entrypoints/replay_kafka.py` | Kafka-sourced replay entrypoint (offset/timestamp bounds, checkpoint isolation) | REWRITE (merge) | Folds into `entrypoints/run_replay.py`, source-aware, alongside Oracle/File replay shapes |
| `entrypoints/replay_curated.py` | Curated-only replay entrypoint (re-parse landing, no broker) | REWRITE (merge) | Also folds into `entrypoints/run_replay.py`. Note this run type has no Oracle/File equivalent named in CORE -- worth a decision note in whichever stage does this (is a "re-derive layer N from layer N-1" replay generic, or Kafka-only?) |

### `tests/`

| File | Covers | Verdict |
|---|---|---|
| `conftest.py` | `FakeSecrets`, `FakeSpark`, `RecordingDataFrame`/`RecordingWriter`, fake `DeltaTable` stand-ins | KEEP, light rewrite | Infra is already source-agnostic in shape; call sites shift as production modules move, but the stand-in pattern itself survives |
| `test_config.py` | Five-layer resolution, replay validation, placeholder substitution | REWRITE | Splits to match `framework/config.py` + `sources/kafka/spec.py` tests |
| `test_security.py` | Auth option construction, redaction | REWRITE (split) | Generic `SecretResolver`/`redact` tests -> framework; Kafka option-building tests -> sources/kafka |
| `test_kafka_source.py` | Reader options, trigger resolution, timestamp parsing | MOVE | Follows `kafka_source.py` into `sources/kafka/` |
| `test_schema_resolver.py` | Wire-format parsing, registry client (has `pytest.importorskip("pyspark")` guard; wire-format tests are `@pytest.mark.spark`) | MOVE | Follows `schema_resolver.py` |
| `test_curated_writer.py` | Avro decode, dedup, curated schema derivation, MERGE-with-schema-evolution (heavily `@pytest.mark.spark`) | REWRITE (split) | Kafka decode tests move; MERGE-schema-evolution-mechanism tests are the candidate to lift into a `framework/writers.py` test |
| `test_writers.py` | Delta option/method assertions for landing + curated writes, via recording stand-ins, **no live Spark needed** | REWRITE (split) | Exactly the fast-suite pattern `framework/writers.py` needs; this file is close to a template for that test |
| `test_audit_and_tables.py` | Audit row/schema/DDL three-way agreement, DDL<->Python column-list drift check (`pytest.importorskip("pyspark")` guard) | REWRITE (split) | Generic agreement-test pattern -> framework; Kafka-specific DDL columns -> sources/kafka |
| `test_pipeline.py` | Dispatch, `txn_app_id`/`run_id` derivation, checkpoint-reset guard -- explicitly documented as needing no Spark | REWRITE (split) | Dispatch/run_id tests -> framework/runner tests; checkpoint-reset guard -> sources/kafka (it is a Structured Streaming concept) |
| `test_shipped_config.py` | Validates the *actual* `conf/topics/*.yaml` against every environment | REWRITE | Becomes the CORE section 3 "full source x environment cross-product" test once `conf/sources/*.yaml` (multi-type) exists (Stage 6) |

### `conf/`

| File | Verdict | Notes |
|---|---|---|
| `defaults.yaml` | REWRITE (split) | `topic_defaults:` is entirely Kafka-shaped (consumer_group_prefix, starting_offsets, trigger, reader_schema_mode...). Splits into a genuinely common `conf/defaults.yaml` (if anything survives -- table_properties fallback, maybe) + `conf/defaults/kafka.yaml` |
| `environments/dev.yaml`, `preprod.yaml`, `prod.yaml` | REWRITE (extend) | `vars:` and the `clusters:`/`registries:` overlay pattern survive as-is; `topic_defaults:` overlay needs the same split as `defaults.yaml`; each will eventually also carry `jdbc:`/`storage:` overlays |
| `clusters.yaml` | KEEP | Register pattern is exactly what CORE 6 wants to replicate for `jdbc.yaml`/`storage.yaml`; no change needed to this file itself |
| `registries.yaml` | KEEP | Same as above |
| `topics/_TEMPLATE.yaml` | MOVE + REWRITE | Directory renames to `conf/sources/`; template must gain a `source_type: kafka` line (CORE 6 requires every source file to declare it) |
| `topics/vector_patient_events.yaml` | MOVE + REWRITE | Same directory move + `source_type:` addition; content otherwise structurally sound as a Kafka example |
| `topics/rcm_claim_status.yaml` | MOVE + REWRITE | Same |
| `topics/antifraud_txn_alerts.yaml` | MOVE + REWRITE | Same |

### `sql/`

| File | Verdict | Notes |
|---|---|---|
| `01_operational_config.sql` | REWRITE | `ingestion_topic_control` (topic_key-keyed) becomes `ingest_control` (source_key/source_type/source_overrides JSON) per CORE 5.2 -- a genuinely different schema, not a rename |
| `02_layer_tables.sql` | REWRITE (split) | The `stream_audit` DDL must match the new generic audit schema (CORE 5.4); the landing table DDL stays Kafka-shaped and belongs with Stage 3's provisioning docs, not in a file that reads as framework-wide |
| `03_support_queries.sql` | REWRITE | References `topic_key` and `stream_audit` throughout; triage queries need the renamed columns and, eventually, multi-source triage |
| `04_maintenance.sql` | KEEP, light rename | VACUUM/OPTIMIZE-by-table-name logic is already source-agnostic; only comments/examples reference topics specifically |

### `resources/` (Databricks Asset Bundle job definitions)

| File | Verdict | Notes |
|---|---|---|
| `job_ingest_primary.yml` | REWRITE | One task per topic, `entry_point: ingest-primary`, `topic-key` parameter -- all rename to source-key/`run_ingest`; whether one job still serves every source *type* or splits per type is a design question for whichever stage touches this |
| `job_replay.yml` | REWRITE | Two jobs (`replay_kafka`, `replay_curated`) map to entrypoints being merged into one `run_replay.py`; job-level parameters need to become source-aware |
| `job_maintenance.yml` | REWRITE, light | Per-topic maintenance tasks; renames land here too but the underlying VACUUM/OPTIMIZE shape does not change |

### Top-level

| File | Verdict | Notes |
|---|---|---|
| `databricks.yml` | KEEP | `ops_catalog`/`data_catalog`/`control_table` variables and the sync/include mechanism are already source-agnostic; comments reference Kafka but the mechanism does not |
| `azure-pipelines.yml` | KEEP | CI gate is exactly the three CORE-mandated commands; nothing here assumes Kafka |
| `pyproject.toml` | REWRITE, light | Dependencies (PyYAML, requests) already match the CORE invariant exactly; `[project.scripts]` entry points (`ingest-primary`, `replay-kafka`, `replay-curated`) need to follow the entrypoints rename |
| `.gitignore` | KEEP | Generic, nothing Kafka-specific |
| `README.md`, `docs/DESIGN.md`, `docs/CONFIGURATION.md`, `docs/NAVIGATION.md`, `docs/RUNBOOK_CLIENT_IT.md`, `docs/RUNBOOK_DEVELOPER.md`, `docs/RUNBOOK_SUPPORT.md`, `docs/IMPORT_TO_DATABRICKS.md` | REWRITE | All Kafka-only prose, all explicitly Stage 7's job per CORE layout. Out of scope before then |
| `notebooks/00_validate_config.py` | REWRITE | Resolves and prints a `TopicConfig`; needs a source-agnostic equivalent |
| `notebooks/01_run_unit_tests.py` | KEEP, light | Just runs the suite on-cluster; needs no real change beyond staying in sync with whatever the suite becomes |
| `notebooks/02_check_connectivity.py` | REWRITE | Currently checks Kafka secrets/registry connectivity only; needs Oracle/ADLS checks once those sources exist |
| `notebooks/03_run_ingestion.py` | REWRITE | Interactive Kafka run notebook; needs a source-agnostic equivalent |
| `src/kafka_ingest.egg-info/*`, `**/__pycache__/*`, `.pytest_cache/*`, `.ruff_cache/*` | DELETE (not tracked) | Build/cache artifacts, already gitignored; not part of the source inventory |

---

## Verification backlog

Created at `docs/VERIFICATION_BACKLOG.md`, all thirteen seeded entries (VB-01 through VB-13)
filled in, ordered most-damaging-first. Ordering rationale (silent data corruption ranks
above a loud infra blocker) is stated at the top of that file.

## CLAUDE.md check

Read against the inventory above. **No changes made.** Specifically checked:

- The `## Layout` section already describes the *target* tree (`framework/` + `sources/` +
  `entrypoints/`) and correctly annotates `src/kafka_ingest/` as "being restructured into"
  it -- it does not claim that tree exists yet, so it is not wrong, just forward-looking.
- The `framework/` leak-detection grep in `## Invariants` names `runner.py` and `_SOURCES`,
  neither of which exists yet either -- same situation, correctly forward-looking, left as is.
- Runtime dependencies (`PyYAML`, `requests`) match `pyproject.toml` exactly.
- The exit-gate commands match what Stage 0 actually ran (below).
- No path or module name currently stated in CLAUDE.md was found to be wrong against the
  actual repository contents.
- No true invariant was found missing that would fit in the one-screen budget. One candidate
  considered and rejected: "the fast test suite requires the `pyspark` package (not a JVM) to
  be installed" -- true and worth knowing (see Blocked/flags below), but it is a local dev-
  environment fact, not a standing invariant about the code, and CLAUDE.md already says
  "Development tooling may be added" without enumerating which. Recorded here and in the
  flags below instead of spent on the one-screen budget.

## What will make later stages harder (flagged, not fixed)

- `tests/test_pipeline.py` and `tests/test_writers.py` import `kafka_ingest.pipeline` /
  `kafka_ingest.audit` at module level with **no** `pytest.importorskip("pyspark")` guard,
  unlike `test_schema_resolver.py`, `test_curated_writer.py` and `test_audit_and_tables.py`,
  which all guard it. Both files are explicitly documented (in their own docstrings) as
  needing no live Spark and belonging in the fast suite -- but because `pipeline.py` and
  `audit.py` import `pyspark.sql` at module scope, the *fast* suite still cannot be collected
  at all in a Python environment with no `pyspark` package installed, even though it never
  constructs a real `SparkSession`. This bit Stage 0 directly (see below) and will bite every
  later stage's CI story identically unless the package-vs-JVM distinction is made explicit
  somewhere (e.g. in `docs/RUNBOOK_DEVELOPER.md`'s setup instructions, or by adding the same
  `importorskip` guard for consistency).
- `config.py` and `pipeline.py` are the two files where the framework/source split is not a
  clean cut -- both interleave generic mechanism (five-layer merge; dispatch, run-id, txn
  identity) with Kafka-specific detail (cluster/registry profiles; foreachBatch chaining,
  checkpoint-reset guard) line by line rather than in separable blocks. Budget real time for
  these two in Stage 1/3, not just a mechanical move.
- `tables.py`'s `KAFKA_COLUMNS` / `CLOUDEVENT_COLUMNS` blocks are physically inside what
  reads like a shared module today; CORE 5.1 is explicit that these stay Kafka-owned, but a
  future skim of this file before Stage 3 could easily assume otherwise and try to genericize
  them.
- The `topic_key` -> `source_key` rename (CORE 5.1) touches nearly every file in this
  inventory -- config dataclass fields, control-table columns, audit columns, CLI flags, SQL,
  job YAML, docs, and every conf file name. No single stage will "do the rename"; it happens
  piecemeal as each file is otherwise touched, so a stage that renames only part of it
  (e.g. the Python but not the SQL) is expected and not itself a bug, but worth checking for
  stragglers in Stage 6/7.
- `entrypoints/replay_curated.py`'s run type ("re-parse an already-landed layer with no
  source contact") has no obviously equivalent shape named anywhere in CORE for Oracle or
  Files. Whoever unifies replay entrypoints (CORE 4.3's single `run_replay.py`) will need to
  decide whether this is a generic "re-derive layer N" replay or stays Kafka-only; flagging
  now so it doesn't get silently dropped or silently forced generic.
- `pyproject.toml`'s own comment already documents that `ruff format --check` currently fails
  on 20 of 22 files, deliberately deferred as "a separate, single-purpose commit." Stage 0
  reproduced this (see Exit gate below) -- it is expected, not a new finding, and is *not*
  something this stage's report is flagging as new risk.

---

## Exit gate

```
$ python -m ruff check src tests
All checks passed!

$ python -m ruff format --check src tests
22 files would be reformatted, 2 files already formatted
```
`ruff format --check` failing is pre-existing and already documented in `pyproject.toml`
(see flags above) -- not something introduced by, or fixed in, this stage.

```
$ python -m pytest -m "not spark" -q
189 passed, 34 deselected in 7.58s
```

The local environment initially had neither `pytest`, `ruff`, `PyYAML`, `requests` nor
`pyspark` installed (only a bare Python 3.14). All five were `pip install`-ed as local dev
tooling before the gate could run at all -- `PyYAML`/`requests` because they are the
project's actual runtime dependencies (CLAUDE.md invariant, `pyproject.toml`), the rest as
dev tooling, which CLAUDE.md's invariants explicitly permit ("Development tooling may be
added; runtime dependencies may not"). `pyspark` was needed only so the fast-suite modules
listed above can be *imported*; no JVM was installed, used, or is available, and no Spark
session was ever constructed -- every `-m "not spark"` test runs against the `FakeSpark` /
recording stand-ins in `tests/conftest.py`.

---

## Five lists

### 1. Done and verified
- Full repository inventory read and tabulated above, verdicts assigned against CORE 4.3's
  target layout.
- `docs/VERIFICATION_BACKLOG.md` created with all 13 seeded entries (VB-01..VB-13), fully
  filled in per the CORE 3 format, ordered most-damaging-first with the ordering rationale
  stated in the file.
- `docs/build_log/` created with a one-line `README.md`.
- Baseline recorded: `python -m pytest -m "not spark" -q` -> **189 passed, 34 deselected**.
  Proof: pytest output above.
- `python -m ruff check src tests` -> **All checks passed**. Proof: output above.
- CLAUDE.md read in full and checked line-by-line against the inventory; no inaccuracy
  found (see "CLAUDE.md check" above).

### 2. Done but not verifiable here
- Nothing in this stage depended on infrastructure this environment lacks -- Stage 0 is
  pure reconnaissance. The thirteen VB entries themselves are all "not verifiable here by
  construction" and are the deliverable, not a gap in this stage's own work.

### 3. Not reproduced
- `pyproject.toml` documents `ruff format --check` as failing on 20 of 22 files, deliberately
  unadopted pending a dedicated formatting commit. This stage's run found **22 files would be
  reformatted, 2 files already formatted** -- consistent with what is documented (the
  discrepancy of "20" vs "22" in the comment is stale prose, not a real drift; not worth an
  edit under CLAUDE.md's "smallest correct change" rule for a Stage 0 that touches no code).

### 4. Blocked
- Nothing. The local environment was missing `pytest`/`ruff`/`pyyaml`/`requests`/`pyspark`
  entirely; all were installed as described under "Exit gate" rather than blocking the stage.

### 5. Decisions for the human
- CORE section 10's eight items all remain open and are covered by VB-05 through VB-13 in
  the backlog; none are re-litigated here.
- New candidate decision surfaced by this stage: whether `replay_curated`'s "re-derive a
  layer from the layer below it, no source contact" run shape is a generic replay type
  available to every source, or stays a Kafka-only concept. CORE does not name an Oracle or
  Files equivalent. Recommendation: keep it Kafka-only unless a concrete Oracle/Files need
  for it appears -- Oracle has no landing-equivalent replay target defined in CORE (Oracle
  curated layer is explicitly out of scope, CORE section 10), and Files' Auto Loader has no
  raw-bytes layer to re-derive from. What would change this: a Files or Oracle source
  onboarding that needs to fix a parse bug without re-reading the origin system.

---

## Test count

Before this stage: N/A (first stage; no prior baseline exists).
After this stage: **189 passed, 34 deselected** (`pytest -m "not spark" -q`). No test was
added, removed or modified -- Stage 0 changes nothing under `tests/`.

## New VB entries added this stage

VB-01 through VB-13 (all thirteen seeded entries from `STAGE_0_orientation.md`, none added
beyond the seed list).
