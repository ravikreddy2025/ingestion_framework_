# CORE -- Multi-Source Ingestion Framework Redesign

**This file lives in the repository at `.claude/build/CORE.md`.** To run a stage, tell the
agent:

> Read `.claude/build/CORE.md` and `.claude/build/STAGE_<n>_<name>.md`, then execute that
> stage.

`CLAUDE.md` at the repo root loads automatically and carries the invariants in short form;
this file is the full brief behind them.

For Stage 3 (Kafka) also supply `detailed_kafka_ingestion_designer_prompt.md` -- its sections
2, 6 and 9 carry Kafka design intent and API traps this file does not repeat. It is not in the
repository, so paste it for that stage only.

---

## 1. What is being built

`final_code/` is today a Kafka-only ingestion framework. It is being rebuilt as a
**multi-source ingestion framework** covering Kafka, Oracle and ADLS file loads, extensible
to BigQuery, with one shared configuration, control, audit, state and logging spine.

| | |
|---|---|
| Deployment status | **Nothing is deployed. Nothing is in production.** One week into development. |
| Redesign licence | **Total.** Module layout, class structure, table names, config keys and the Kafka source itself are all open. |
| Real constraint | **Team learning curve.** A small team must read and own this. Simplicity is the goal. |
| Environment | **Local Python only.** No Databricks, no Kafka, no Oracle, no cluster, no network. |

Because nothing is deployed there are **no migrations, no backfills, no compatibility views,
and no constraints on table names, `txnAppId` strings or checkpoint paths.** Design them
correctly and move on.

| Source | Execution model | Stage |
|---|---|---|
| **Kafka** | Structured Streaming, `availableNow`, `foreachBatch` | 3 |
| **Oracle** | JDBC batch, incremental by cursor or filter | 4 |
| **Files (ADLS)** | Auto Loader, `availableNow` | 5 |
| **BigQuery** | TBD | Contract and docs only, no code |

**The goal of the architecture:** adding a source type is a new package under `sources/` and
**zero changes** under `framework/`. Section 7 gives the falsifiable test for this.

---

## 2. How to work

Section 0 of the original designer prompt still overrides your defaults -- especially **do not
invent APIs** and **stop at the requirement**.

**Session context.** Each stage runs in a fresh session that remembers nothing of the previous
ones. Your context is the repository itself plus two things in it:

- `CLAUDE.md` at the repo root -- the invariants that must hold at all times. Read it first.
- `docs/build_log/DECISIONS.md` -- questions a stage returned and the human answered. **These
  are settled. Do not re-litigate them.** If one is genuinely unworkable, say so in your
  report rather than quietly doing something else.
- `docs/build_log/STAGE_*_REPORT.md` -- what earlier stages did, decided and deferred. **Read
  all of them before starting.** They are short, and they carry reasoning the code does not.

**The stage ritual. Follow it exactly:**

0. Read `CLAUDE.md` and every file in `docs/build_log/`.
1. Read the stage file in full.
2. Print the list of files you will create and the files you will edit.
3. Make the changes.
4. Run the exit-gate commands (section 8).
5. Write the stage report (section 9).
6. **Stop. Do not begin another stage.**

Eight rules:

1. **You have no network and no cluster.** Never run, and never claim to have run, anything
   needing Kafka, Oracle, ADLS, Databricks or a JVM. When a decision depends on real
   infrastructure, write the code defensively and **add a verification-backlog entry**
   (section 3).
2. **Do not invent APIs.** If you are not certain a method, option or config key exists with
   the exact name and signature, stop and say so. **A config key that silently does nothing is
   the worst possible output of this project.**
3. **Simplicity beats cleverness, and the reason is the team.** Given a clever solution and one
   a new joiner reads in a sitting, choose the readable one. Comments explain *why*, never
   *what*.
4. **Abstract only where there are three implementations.** Kafka, Oracle and Files justify
   exactly one abstraction (section 4). Nothing else does. Section 7 is not negotiable.
5. **Every test you write must run with plain `pytest` and no Spark.** That is the only gate
   you can execute. Spark-marked tests may be written but go in the backlog as unrun.
6. **Smallest correct change.** Redesign is licensed; rewriting things that already work
   because you would have written them differently is not.
7. **Locate by file and symbol, never by line number.** If a symbol named in a stage file does
   not exist, report that and skip rather than fixing the nearest similar thing.
8. **If you are unsure whether something is in scope, it is not.** One sentence in the stage
   report, then move on.

---

## 3. Environment constraints and the verification backlog

### You have
Python, `pytest`, `ruff`, the repository. That is all.

### Never attempt
- Connecting to Kafka, Schema Registry, Oracle, ADLS or a Databricks workspace.
- `databricks bundle validate` or `databricks bundle deploy` (no CLI, no auth).
- Spark-marked tests (no JVM, no `spark-avro` jar).
- `pip install` of a JDBC driver or connector to "check" it.
- Reporting any of the above as passing.

### Instead: `docs/VERIFICATION_BACKLOG.md`

A first-class deliverable, handed to the human to run on a real environment. Every entry uses
this exact format:

```markdown
### VB-nn -- <one-line question>
- **Stage / file:** where this matters
- **Why it matters:** what breaks, and how visibly, if the assumption is wrong
- **How to check:** the exact command, query or notebook cell to run
- **Expected:** what a passing result looks like
- **If it fails:** what to change in the code, and where
- **Status:** OPEN
```

Seeded in Stage 0 with VB-01 to VB-13. Add entries whenever you hit an unverifiable
assumption, and list new ones in every stage report.

### Offline substitutes you must build (Stage 6)

- `yaml.safe_load` succeeds on every file under `conf/` and `resources/` and on
  `databricks.yml`.
- Every job template's referenced entrypoint file exists on disk.
- Every `source_type` in `conf/sources/*.yaml` has a matching package in `sources/`.
- Every register reference (`jdbc_ref`, `storage_ref`, cluster, registry) named in any source
  or environment file exists in its register file.
- The full source x environment cross-product config resolution test.

---

## 4. Architecture

### 4.1 The one abstraction

Do not unify read/parse/write across sources. A streaming `foreachBatch` body and a bounded
JDBC read share governance, not steps. Any step-level contract across them will leak.

**The contract is one function per source, plus one declarative spec.**

```python
# sources/<name>/__init__.py -- the ENTIRE public surface of a source
from .spec import SOURCE_SPEC          # data, no PySpark import
from .run import run                   # def run(ctx: RunContext) -> RunResult
```

Follow these skeletons exactly:

```python
# framework/contracts.py -- the spec half imports no PySpark
@dataclass(frozen=True)
class SourceSpec:
    source_type: str
    required_keys: frozenset[str]
    structural_keys: frozenset[str]      # allowed in YAML layers 1-3
    operational_keys: frozenset[str]     # allowed in the control table
    mutually_exclusive: tuple[tuple[str, ...], ...]
    layers: tuple[str, ...]              # ("landing",) / ("landing","curated","quarantine")

@dataclass(frozen=True)
class RunContext:
    cfg: Any            # the source's own frozen config dataclass
    spark: Any
    audit: Any
    state: Any
    writers: Any
    tables: Any
    log: Any
    run_id: str
    run_type: str       # primary | kafka_replay | curated_replay | oracle_replay | file_replay
    run_sequence: int

@dataclass(frozen=True)
class RunResult:
    rows_read: int
    rows_written: dict[str, int]     # layer -> count
    rows_quarantined: int
    position_start: str | None       # JSON or scalar, as text
    position_end: str | None
    source_detail: str | None        # JSON
```

**Do not add `read()`, `parse()`, `write()` or `validate()` to the source contract.** The
framework's writers are on `ctx` for sources that want them; a source that does not use them
is not broken.

### 4.2 Validation is data-driven, never branched

Config validation must never contain `if source_type == ...`. The framework reads
`SOURCE_SPEC` and applies its existing guarantees per source type:

- unknown key -> error naming the key and the source type
- missing required key -> error
- operational override of a structural key -> **ignored**, and a test asserts it
- mutually exclusive keys both set -> error naming both
- unresolved `{placeholder}` -> hard error naming the setting and the token

**`framework/config.py` must have no PySpark import, and neither must any
`sources/<name>/spec.py`.** That is what keeps config tests running locally. `SOURCE_SPEC`
lives in `spec.py`; `run()` lives in `run.py`.

### 4.3 Module layout

```
framework/
  contracts.py    SourceSpec, RunContext, RunResult. No PySpark.
  config.py       five-layer load + merge + placeholders + spec-driven validation. No PySpark.
  control.py      read the operational control table -> layer-4 override dict
  security.py     secrets, certs, JDBC credentials -> ready-to-use connection options
  state.py        run sequence + watermark store. Writes are MANDATORY.
  audit.py        generic per-run / per-layer status rows. Writes NEVER raise.
  tables.py       target-name resolution and DDL per layer
  writers.py      append / MERGE, txn markers, quarantine split, partition predicates
  runner.py       run lifecycle: build context, dispatch, audit, finalise
  logs.py         structured lines carrying source_key + run_id; secret redaction
  checkpoint.py   checkpoint-reset guard shared by every checkpoint-based source (added in
                  Stage 5b once Kafka's and Files' own copies had already drifted once --
                  see docs/build_log/DECISIONS.md D-14)
sources/
  kafka/          spec.py, run.py, and its parse/write helpers
  oracle/         spec.py, run.py, query.py, types.py
  file/           spec.py, run.py
entrypoints/
  run_ingest.py   parse args, resolve config, call runner
  run_replay.py   replay entrypoints, source-aware
```

`runner.py` holds **the only** source-type mapping, as a module-level dict literal:

```python
_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}
```

No registry class. No entry-point discovery. No dynamic import by string.

---

## 5. Shared tables

### 5.1 Naming

| Concept | Column | Notes |
|---|---|---|
| Config filename stem | **`source_key`** | Globally unique across source types. Control-table primary key. Replaces `topic_key`. |
| Source-side identifier | **`source_ref`** | Topic name, `SCHEMA.TABLE`, path glob. Replaces `topic`. |
| Source type | **`source_type`** | `kafka` / `oracle` / `file` |
| Read boundary | **`position_start`, `position_end`** (STRING) | Kafka offsets JSON, Oracle watermark, file boundary. One column, three meanings -- **document that on the column**. |
| Source extras | **`source_detail`** (STRING, JSON) | Anything that does not deserve a column. |

`kafka_partition` and `kafka_offset` stay as-is **in the Kafka landing/curated tables** --
payload columns, not framework columns. Do not genericise data columns.

**`source_detail` is a JSON string, not a MAP** -- a new source never forces a table
alteration.

### 5.2 Control table -- `{ops_catalog}.{control_schema}.ingest_control`

**Superseded by `docs/build_log/DECISIONS.md` D-01, applied before Stage 3 -- see that entry
for the reasoning and treat it, not the sketch below, as authoritative.** Two changes from
the original sketch: `failure_mode` and `batch_limit` became per-source-type PREFIXED
columns (`kafka_failure_mode`, `kafka_max_offsets_per_trigger`, `oracle_fetch_size`,
`oracle_num_partitions`, `oracle_incremental_mode`, `file_failure_mode`,
`file_max_files_per_trigger`, ...), each declared by that source's own `SOURCE_SPEC.
control_columns`; and `source_overrides` was removed entirely -- a prefixed column is the
only place a source-specific operational setting lives now, so there is no second
mechanism to keep in step. The table also moved into the ops catalog's own
`{control_schema}` schema, not `{ops_catalog}` directly (D-06 -- see 5.4 below).

```
source_key            STRING NOT NULL   -- primary key
source_type           STRING NOT NULL
enabled               BOOLEAN
replay_rerun_id       STRING
replay_controls       STRING (JSON)
notes                 STRING
updated_by            STRING
updated_at            TIMESTAMP
-- plus each source type's own prefixed columns, e.g. kafka_failure_mode,
-- kafka_checkpoint_reset_id, oracle_fetch_size, file_max_files_per_trigger
```

Enforced rules:

- **Missing row is not an error** -- it means no overrides, so a new source runs the moment its
  YAML merges. **Duplicate rows are** an error.
- **Setting a prefixed column for the wrong source type is an error**, not a silent ignore
  (D-01) -- there is no `source_overrides` JSON left to validate against `operational_keys`
  the way this section originally described.
- **Structural fields are not overridable**: partitioning, dedup/merge keys, target names,
  Oracle's `source_schema` / `source_table` / `filter_criteria`, the file target path.

### 5.3 State table -- `{ops_catalog}.{control_schema}.ingest_state`

Kafka resumes from a Spark checkpoint. **Oracle has no checkpoint.** Do not derive the
watermark from the audit table: audit writes are best-effort and must never raise, which is
correct for audit and disqualifying for state.

```
source_key       STRING NOT NULL
state_key        STRING NOT NULL   -- 'watermark' | 'run_sequence'
state_value      STRING
value_type       STRING
updated_at       TIMESTAMP
updated_by_run   STRING
```

- **`watermark`** -- Oracle's last committed cursor value. Advanced **only after** the landing
  write commits.
- **`run_sequence`** -- monotonically increasing integer per `source_key`, used as `txnVersion`
  for sources with no Spark batch id. Gives Oracle and Files the same Delta idempotent-write
  protection Kafka gets from its batch id.

Keep it key/value. Do not add a column per source concept.

### 5.4 Ops catalog schemas (`docs/build_log/DECISIONS.md` D-06)

Framework metadata lives in one ops catalog split into schemas by purpose, each an
environment `vars:` entry alongside `ops_catalog`: `{ops_catalog}.{audit_schema}.
ingest_audit`, `{ops_catalog}.{control_schema}.ingest_control` and `...ingest_state`, and a
reserved-but-unwritten `{ops_catalog}.{logs_schema}`. This postdates the single-schema
sketch in 5.2/5.3/5.5 as originally drafted -- every table path above and below is
schema-qualified, not just catalog-qualified.

### 5.5 Audit table -- `{ops_catalog}.{audit_schema}.ingest_audit`

One shared table, one row per (run, layer, status). Everything it carries today plus
`source_type`, `source_key`, `source_ref`, `position_start`, `position_end`, `source_detail`.

Keep the existing three-way agreement test: **audit row, its `StructType`, and its DDL must
all agree.**

---

## 6. Configuration model

Five layers unchanged. Layout:

```
conf/defaults.yaml                 common to every source of every type
conf/defaults/<source_type>.yaml   per-source-type defaults              (NEW)
conf/environments/<env>.yaml       vars, endpoints, secret scopes, tuning
conf/sources/<source_key>.yaml     one file per source; MUST declare source_type
conf/clusters.yaml                 register: Kafka clusters
conf/registries.yaml               register: Schema Registries
conf/jdbc.yaml                     register: JDBC connections            (NEW)
conf/storage.yaml                  register: ADLS accounts/containers    (NEW)
```

**Registers behave exactly as they do today**: they record what exists (auth mode, secret KEY
names, endpoints); the environment file overrides named profiles in them; a profile named in
an environment file that does not exist in its register is an error. `jdbc.yaml` and
`storage.yaml` are the same pattern -- **do not invent a different mechanism.**

**Placeholders unchanged**: `{catalog}` and environment `vars:` reach source settings *and*
connection profiles; `{source_key}` (renamed from `{topic_key}`) and `{domain}` reach source
settings only.

A source file must never contain a catalog or table name -- except where target naming is
derived from source metadata, as Oracle's is. That derivation lives in `framework/tables.py`,
driven by a pattern in `conf/defaults/oracle.yaml`.

---

## 7. Anti-over-engineering

Forbidden, and not negotiable, because a small team has to own this:

- A base class or interface with one implementation.
- A class hierarchy for sources. The contract is a module with a function and a spec.
- A plugin registry, entry-point discovery, or dynamic import by string. One dict literal.
- A dependency-injection container. `RunContext` is a frozen dataclass built in one place.
- A generic connector framework, a DSL, or a config UI.
- A `SourceConfig` superclass accumulating every source's optional fields. Each source gets its
  own frozen dataclass; the framework handles them via the spec, not inheritance.
- Retry or circuit-breaker frameworks beyond what the libraries and Spark provide.
- A Kafka admin client, or any runtime dependency beyond `PyYAML` and `requests`.

**The falsifiable gate** (added to CI in Stage 6, but check it yourself from Stage 1 onward):

```bash
grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
  | grep -v 'runner.py:.*_SOURCES'
# must return nothing
```

If `framework/` knows any source's name outside the dispatch dict, the spine has leaked.

**Readability bar:** a support engineer with solid PySpark and no prior exposure must open any
one module and understand its job without tracing indirection. Target **ten framework modules
plus one package per source**. If a function grows past roughly one screen, split it -- but
only into functions called from exactly one place, not into new abstractions.

---

## 8. Exit-gate commands

Run after **every** stage. These are the only commands you can execute.

```bash
ruff check src tests
ruff format --check src tests
pytest -m "not spark" -q
```

**Do not run** `databricks bundle validate`, unmarked `pytest` (it will try Spark), or anything
touching a network. State that explicitly in each stage report.

- **The fast suite must be green before and after every stage.** If a stage makes it red, the
  stage is wrong, not the test.
- **A test you added must be able to fail.** Break the line it covers, watch it fail, restore
  it. Report which tests you verified this way.
- **Never report a command as passing if you did not run it.**

---

## 9. Stage report format

**Write the report to `docs/build_log/STAGE_<n>_REPORT.md` in the repository, and also print
it.** Each stage runs in a fresh session with no memory of earlier ones -- the build log is how
the next session learns what was decided and why. Before starting any stage, **read every
existing file in `docs/build_log/`.**

Five short lists. Not prose.

1. **Done and verified** -- what changed, and the command whose output proves it.
2. **Done but not verifiable here** -- what changed and which VB id covers it.
3. **Not reproduced** -- anything in the stage file that did not hold against the current code,
   and what you actually found. A useful result, not a failure.
4. **Blocked** -- what you stopped on rather than guessing, with the candidates considered.
5. **Decisions for the human** -- section 10 items plus anything where you chose between two
   defensible options; state which and what would change your mind.

Then two lines: **test count before and after** (from actual pytest output), and **new VB
entries added this stage**.

---

## 10. Decisions returned to a human

Implement around these, mark the decision point, list them in the report. **Never decide them
silently.**

| Decision | Recommendation | What would change it |
|---|---|---|
| Auto Loader vs a processed-files ledger | Auto Loader | Prohibitive ADLS listing costs, or notification-mode resources unavailable |
| Oracle curated layer | Landing only | A consumer needing a conformed Oracle model inside this framework |
| Oracle `merge_keys` default | Require unless explicitly waived | A table with no stable key, or prohibitive MERGE cost |
| `ingest_state` separate from audit | Yes, separate | Only if audit writes become mandatory |
| Checkpoint root: UC Volume or `abfss://` | Decide after VB-08 | Verified evidence either way |
| Kafka schedule cadence | Propose, do not set | Cost and producing-team expectations |
| Oracle schedule per table | Needs the source purge window | -- |
| `min_partitions` / `batch_limit` per source | Ship a documented default; do not guess | The producing team's partition count and volume |

---

## 11. API traps

Original designer prompt sections 9 and 10 apply in full. Additions -- **every one is a
verification-backlog entry, not something to assert:**

1. JDBC `query` vs `partitionColumn` -- historically mutually exclusive; a parenthesised
   subquery in `dbtable` is the workaround. Wrong choice silently serialises the read. (VB-01)
2. `partitionColumn` accepts numeric, date and timestamp columns only.
3. `lowerBound` / `upperBound` **do not filter**. They only shape the split.
4. **Oracle JDBC `fetchsize` defaults to 10 rows.** Not setting it is a catastrophe.
5. Oracle `DATE` carries a time component; the mapping depends on a driver property. (VB-03)
6. `NUMBER` without precision/scale has a version-dependent Spark mapping. (VB-02)
7. Oracle folds unquoted identifiers to upper case; quoted ones are case-sensitive.
8. `sessionInitStatement` runs **per connection**, i.e. per partition. Keep it cheap and
   idempotent.
9. Auto Loader `schemaLocation` is a checkpoint-like resource with its own lifecycle.
10. `cloudFiles.includeExistingFiles` is evaluated **on first start only**.
11. `rescuedDataColumn` behaviour varies by format. (VB-06)
12. `_metadata` column availability is runtime-dependent. (VB-06)
13. Delta idempotent writes apply to appends, are tracked per table, and need a **stable** app
    id -- which is why `run_sequence` lives in `ingest_state`, not in memory.
14. **Delta MERGE schema evolution**: a session config flag and a builder method are both
    candidates and availability is version-dependent. **Do not guess.** (VB-09)
15. PySpark has no `Trigger` class -- `.trigger(availableNow=True)`.
16. The Spark Kafka source overrides `kafka.group.id`; use `groupIdPrefix`.
17. The *streaming* Kafka source has no ending-offset option -- a bounded replay needs
    `spark.read`.
18. An existing checkpoint always wins over `startingOffsets`.

---

## 12. Out of scope

- **BigQuery implementation.** Contract and docs only.
- Downstream marts, business logic, CDC/SCD, a data-quality framework, a config UI,
  multi-cloud abstraction, alerting beyond the jobs' own notifications.
- File archiving or deletion in the landing zone.
- A curated layer for Oracle or Files.
- **Anything requiring a network connection.**
- Performance work beyond the read-parallelism settings named in the Oracle and Kafka stages.

If you think one is genuinely necessary, say why in one sentence and let the human decide.
