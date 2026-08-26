# Design

For the team taking this over. Read this before changing code.

**Scope: Kafka → landing/curated. Oracle → landing. Files → landing.** Nothing downstream
of a source's own layers is in this package. **Each source's own design, failure-scenario
table and design decisions live in their own sibling file** —
[DESIGN_KAFKA.md](DESIGN_KAFKA.md), [DESIGN_ORACLE.md](DESIGN_ORACLE.md),
[DESIGN_FILES.md](DESIGN_FILES.md) — so a reader who only touches one source type is not
carrying the other two while they read. This file holds what is genuinely shared: the
architecture, why the source contract has exactly one method, the configuration model, and
the framework-wide lookups (common changes, testing, the unverified-claims list, deliberate
non-abstractions, adding a source type).

---

## 1. The architecture: spine plus source packages

```
framework/       contracts, config, control, security, state, audit, tables, writers,
                 checkpoint, runner, logs. Never names a source type outside runner.py.
sources/kafka/   spec.py + run(ctx)   -- readStream, foreachBatch, landing+curated+quarantine
sources/oracle/  spec.py + run(ctx)   -- JDBC batch read, cursor/filter incremental, landing only
sources/file/    spec.py + run(ctx)   -- Auto Loader, availableNow, landing only
entrypoints/     run_ingest.py, run_replay.py -- argparse, then framework/runner.py::run()
```

**A source's entire public surface is two things**, and nothing else may be added to it:

```python
# framework/contracts.py -- the spec half imports no PySpark
@dataclass(frozen=True)
class SourceSpec:
    source_type: str
    required_keys: frozenset[str]
    structural_keys: frozenset[str]      # allowed in YAML layers 1-3
    operational_keys: frozenset[str]     # allowed in the control table / job parameters
    mutually_exclusive: tuple[tuple[str, ...], ...]
    layers: tuple[str, ...]              # ("landing",) / ("landing","curated","quarantine")
    target_tokens: frozenset[str] = frozenset()
    control_columns: Mapping[str, str] = MappingProxyType({})

@dataclass(frozen=True)
class RunContext:
    cfg: Any            # this source's resolved config (framework/config.py::ResolvedConfig)
    spark: Any
    audit: Any           # framework/audit.py::AuditWriter -- writes never raise
    state: Any           # framework/state.py::StateStore -- writes raise on failure
    writers: Any         # framework/writers.py module -- append() / merge()
    tables: Any          # framework/tables.py module -- target()/targets()/ensure_table()
    log: Any             # framework/logs.py::RunLog
    run_id: str
    run_type: str        # "primary", or a source-specific replay type
    run_sequence: int

@dataclass(frozen=True)
class RunResult:
    rows_read: int
    rows_written: dict[str, int]     # layer -> count
    rows_quarantined: int
    position_start: str | None       # JSON or scalar, as text
    position_end: str | None
    source_detail: str | None        # JSON
    pending_work: int | None = None  # outstanding work at run end. None = "cannot know"
```

**Do not add `read()`, `parse()`, `write()` or `validate()` to this contract.** The
framework's writers, tables and state helpers are on `ctx` for a source that wants them; a
source that does not use one is not broken. `RunContext` is built in exactly one place
(`framework/runner.py`) and must never grow into a dependency-injection container — if a
source needs something that is not on it, the honest fix is usually that the source should
build it itself.

`framework/runner.py`'s dispatch is a module-level dict literal, not a registry class or
dynamic import by string:

```python
_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}
```

The **CORE section 7 grep** is what keeps this true rather than aspirational — it fails CI
if `framework/` ever names a source type outside this one line:

```bash
grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
  | grep -v 'runner.py:.*_SOURCES'
```

### The run lifecycle, in one place

`framework/runner.py::run()` is short enough to read end to end and is the one function
every entrypoint calls:

```
read the control table -> resolve five-layer config -> validate every target name
  -> ensure the framework's own tables (audit, state) -> allocate a run sequence
  -> build RunContext -> dispatch to _SOURCES[source_type].run(ctx) -> audit -> return
```

Everything a source does is behind `module.run(ctx)`; everything the framework guarantees —
config validation, the disabled short-circuit, audit rows before and after dispatch —
happens here and nowhere else. A support engineer reading this one function can tell what
any run did before and after the source-specific part.

---

## 2. Why the source contract has exactly one method

Because the three sources agree on *governance* and disagree on *mechanism*, and a
contract can only usefully encode what is genuinely shared.

Take the obvious four-method interface — `read`, `parse`, `write`, `validate` — and try
to fit the three real sources into it. Kafka reads a stream and does everything inside a
`foreachBatch` body, once per microbatch, with Spark controlling when that body runs and
committing offsets around it; its "write" is two writes to two tables plus a quarantine
split, and its "parse" needs the Schema Registry, resolved once per run before the first
batch. Oracle reads a bounded result set exactly once, over a closed interval it computed
from a watermark it must read before the read and advance only after the write commits;
it has no parse step at all, because JDBC already returned typed columns. Auto Loader
reads with its own checkpoint and its own schema-inference resource, whose lifecycle is
neither Kafka's checkpoint nor Oracle's watermark. `read()` would return a stream for one,
a DataFrame for another, and a query object for the third; `parse()` would be a no-op for
two of the three; `write()` would be called once per run by one caller and once per
microbatch by another. Every method would need a comment explaining which sources actually
use it, which is the definition of an abstraction that is not paying for itself.

What the three *do* share is everything around that: the same five-layer configuration
with the same validation, the same control table, the same audit rows, the same state
table, the same run identity, the same disabled short-circuit, the same logging. That is
exactly what `RunContext` carries in and `RunResult` carries out. So the framework owns the
lifecycle, hands the source everything it needs, and calls it once — and the source owns
its own shape entirely.

The practical test is the one a new joiner applies: a source is one directory, and the two
things you can do with it are read `spec.py` to see what it accepts and read `run.py` to see
what it does. There is no base class to look up, no method resolution order, and no
question of which hook fires when. Adding a fourth source type is a new directory plus one
line in `_SOURCES`, and the grep gate above is what proves that stays true.

The cost is real and worth stating: two sources that genuinely could share a step — say a
checkpoint-reset guard — must reach for a shared helper (`framework/checkpoint.py`, used by
Kafka and Files) rather than inherit one. That is the trade taken deliberately. A helper
called from two places is something a reader can follow; a base method called from nowhere
visible is not.

---

## 3. Configuration model

Five layers, generic across every source type; the full reference with every setting is
[CONFIGURATION.md](CONFIGURATION.md). In outline:

```
1. conf/defaults.yaml                 common to every source, of every type
1b. conf/defaults/<source_type>.yaml  common to every source of ONE type
2.  conf/environments/<env>.yaml      vars, per-environment tuning, register overlays
3.  conf/sources/<source_key>.yaml    unique to one source
     3a. source:.environments.<env>  RARE - unique to one source IN ONE environment
      ^-- STRUCTURAL: Git, PR-reviewed, next deploy
4. operational control table          support-team overrides
5. Workflows job parameters           one-off overrides
      ^-- OPERATIONAL: next run, no deploy
```

Later wins per key; absent keys fall through. `{placeholder}` tokens in layers 1-3 resolve
after the merge — an unresolved one is a hard error, except for a source type's own
`target_tokens`, which the source itself fills at the top of `run()`.

| | **Structural** (1-3) | **Operational** (4-5) |
|---|---|---|
| Holds | Connection, tables, **partitioning**, merge/dedup keys, checkpoint root, what is extracted | enable/disable, standing tuning knobs, failure mode, **replay controls** |
| Changed by | Platform + domain engineers | **Support team** |
| Process | PR → deploy | `UPDATE` |
| Effect | Next deploy | **Next run — no deploy** |

**Validation is entirely data-driven.** `framework/config.py` contains no
`if source_type == ...` anywhere, and never may: every guarantee (unknown key, missing
required key, mutually exclusive keys, an operational override of a structural field being
ignored, structural YAML setting an operational-only key) comes from the calling source's
own `SOURCE_SPEC`.

**A missing control-table row is not an error** (it means "no overrides"), so a source runs
the moment its YAML merges. **Duplicate rows are** an error.

Note what is *not* operational for any source type: table names, partition columns and
merge/dedup keys are structural everywhere. Changing a table's physical layout or what it
merges on should require a PR — every source's own test module asserts this.

---

## 4. Where to make common changes

| I want to… | Edit | Deploy needed? |
|---|---|---|
| Add a source of an existing type | See the README's "Onboarding a source" section | Yes (PR) |
| Add a Kafka cluster/registry, JDBC connection or storage account | The relevant register file + every `conf/environments/*.yaml` | Yes (PR) |
| Change a partition column | The source's own config file **and** back-fill/rewrite the table | Yes (PR) |
| Stop a source now | `enabled = false` in the control table | **No** |
| Unblock a stuck Kafka/File source | `kafka_failure_mode` / `file_failure_mode = QUARANTINE` | **No** |
| Replay Kafka data, or re-parse it | Kafka replay / curated replay job parameters | **No** |
| Re-extract an Oracle window | `oracle_replay` job parameters | **No** |
| Switch Oracle between full and delta | `oracle_incremental_mode` in the control table | **No** |
| Add a CloudEvent attribute (Kafka) | `CE_ATTRIBUTES` in `sources/kafka/landing.py` + the DDL blocks in `sources/kafka/tables.py` | Yes |
| Change the audit row shape | `AUDIT_SCHEMA` in `framework/audit.py` + `AUDIT_DDL_COLUMNS` + `sql/02_layer_tables.sql` | Yes |
| Add a source type | §8 below | Yes (PR, plus the grep gate must stay clean) |

The last two are the only framework-wide ones, and a test asserts the audit schema and DDL
stay in step (`tests/test_framework_audit.py`).

---

## 5. Testing

```bash
ruff check src tests             # lint - the azure-pipelines.yml CI gate
ruff format --check src tests    # formatter - enforced since Stage 3
pytest -m "not spark" -q         # config, control, state, audit, writers, every source's
                                  # own spec/config/run against stand-ins. No JVM needed.
pytest -q                        # everything, including the few Spark-marked tests
```

The tests worth knowing about:

| Test | Proves |
|---|---|
| `tests/test_framework_runner.py` | The dispatch, the disabled short-circuit, run-id derivation, that a source's stub cannot pretend to be implemented |
| `tests/test_framework_config.py` | Five-layer merge precedence, spec-driven validation, structural-vs-operational rules |
| `tests/test_framework_checkpoint.py` | The shared checkpoint-reset guard's full behaviour matrix, proven once for both Kafka and Files |
| `tests/test_framework_writers.py` | `txnAppId`/`txnVersion` mechanics, that `merge()` cannot be called without a partition predicate |
| `tests/test_kafka_curated.py` | Mixed writer-schema versions in one batch, `event_date` derivation, quarantine split |
| `tests/test_oracle_run.py` | The watermark lifecycle order, the merge key including the cursor column, that a replay never writes state |
| `tests/test_file_run.py` | The rescued-row / `failure_mode` interaction, the shared guard's wiring |
| `tests/test_shipped_config.py` | Every real `conf/sources/*.yaml` resolves in every environment, across all three source types — the CI gate for configuration |
| `tests/test_shipped_jobs.py` | Every job template names a real, correctly-typed source, and the runbook's cited queries actually exist |
| `tests/test_offline_validation.py` | YAML parses; every job entrypoint resolves; every `source_type` has a package; every register reference resolves; every notebook's `kafka_ingest` imports resolve against the current source tree |

Run the full suite on Databricks via `notebooks/01_run_unit_tests` — DBR has Spark and
`spark-avro` built in, so it runs with no setup. No test connects to Kafka, Oracle or ADLS,
reads a secret, or writes to a table.

---

## 6. The unverified-claims list

Every assumption this codebase makes that needs real infrastructure to confirm is tracked
in [`docs/VERIFICATION_BACKLOG.md`](VERIFICATION_BACKLOG.md), ordered by how much breaks if
it is wrong — not restated here, to avoid two copies drifting apart. The three most
dangerous, as of Stage 7:

1. **VB-22** — the Oracle JDBC driver is not installed anywhere in this repository, and its
   version gates the two most damaging entries below.
2. **VB-02 / VB-03** — the Spark type mapping for Oracle `NUMBER` (no precision/scale) and
   `DATE` on the target driver/DBR. A wrong guess silently corrupts or truncates every value
   in that column, forever, with no error anywhere.
3. **VB-15** — whether `ingest_state`'s MERGE actually upserts. Every batch-style source's
   idempotency (Oracle's `run_sequence`) depends on it; a silent no-op looks exactly like a
   healthy job with nothing new to write.

Every module that depends on an open VB entry names it in a comment at the point of use —
`sources/oracle/types.py`, `sources/oracle/query.py`, `sources/file/reader.py`, and
`framework/writers.py::merge()`'s schema-evolution branch are the most concentrated
examples.

---

## 7. Deliberate non-abstractions

Forbidden across the whole framework, and not negotiable, because a small team has to own
this — CORE section 7's list, restated here as what was *not* built and why:

- **A base class or interface for sources.** The contract is a module with a function and a
  spec (§2 above).
- **A class hierarchy for sources, or a `SourceConfig` superclass.** Each source gets its own
  frozen config dataclass; the framework handles them via `SOURCE_SPEC`, never inheritance.
- **A plugin registry, entry-point discovery, or dynamic import by string.** One dict
  literal, `runner.py::_SOURCES`.
- **A dependency-injection container.** `RunContext` is a frozen dataclass built in one
  place.
- **A generic connector framework, a DSL, or a config UI.**
- **Retry or circuit-breaker frameworks** beyond what the libraries and Spark provide —
  Spark opens and closes its own JDBC connections per partition, so a driver-side pool would
  pool nothing.
- **A Kafka admin client, or any runtime dependency beyond `PyYAML` and `requests`.**
- **A second checkpoint-reset guard.** Kafka and Files share one implementation
  (`framework/checkpoint.py`) rather than each carrying a near-identical copy — promoted
  once the duplication itself became the cost worth avoiding, not because three
  implementations existed (Oracle has no checkpoint at all).
- **A schema-migration or reconciliation utility**, for any source.
- **File archiving, moving or deletion** for the file source — that belongs to whoever owns
  the landing zone.

Each source's own, narrower "deliberately not built" list — the choices specific to that
source type, like Kafka's single serialization format or Files' lack of a replay job — lives
in that source's own design file: [DESIGN_KAFKA.md](DESIGN_KAFKA.md),
[DESIGN_FILES.md](DESIGN_FILES.md). Oracle has none beyond what this framework-wide list
already covers.

The falsifiable gate, run in CI on every PR:

```bash
grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
  | grep -v 'runner.py:.*_SOURCES'
```

If this returns anything, the spine has leaked.

---

## 8. Adding a source type

This is the falsifiable answer to CORE section 1's goal: "adding a source
type is a new package under `sources/` and **zero changes** under `framework/`." A new
package provides exactly these, nothing more:

| # | What | Where |
|---|---|---|
| 1 | `SOURCE_SPEC` — the keys this source type accepts, structural/operational split, control columns, target tokens, mutually-exclusive pairs. **No PySpark import.** | `sources/<type>/spec.py` |
| 2 | `run(ctx: RunContext) -> RunResult` — the one function. Everything else in the package (`config.py`, `security.py`, `reader.py`, ...) is this source's own, called from nowhere the framework can see. | `sources/<type>/run.py` |
| 3 | One dict entry, `"<type>": <module>` | `framework/runner.py`'s `_SOURCES` |
| 4 | Platform-wide defaults for every source of this type | `conf/defaults/<type>.yaml` |
| 5 | A register file (`conf/<kind>.yaml`) — **only if** the type needs a new connection kind Kafka's/Oracle's/Files' registers do not already cover. Discovered by the framework listing `conf/*.yaml`, so adding one is a new file, not a code change. | `conf/<kind>.yaml` |
| 6 | One job template, matching the operational shape (schedule, concurrency, retries) this source's runs actually need | `resources/job_ingest_<type>.yml` |
| 7 | An inert onboarding template — the underscore prefix keeps it out of the deployable set | `conf/sources/_TEMPLATE_<type>.yaml` |
| 8 | A design file for this source's own re-run mechanics, failure scenarios and decisions, following the pattern [DESIGN_KAFKA.md](DESIGN_KAFKA.md)/[DESIGN_ORACLE.md](DESIGN_ORACLE.md)/[DESIGN_FILES.md](DESIGN_FILES.md) already set | `docs/DESIGN_<TYPE>.md` |

**Nothing under `framework/` changes.** Every one of the items above lives in the new
package, a new conf file, a new resource file or a new sibling doc — `framework/config.py`,
`control.py`, `state.py`, `audit.py`, `tables.py`, `writers.py`, `runner.py` (beyond the one
`_SOURCES` line), `security.py`, `checkpoint.py` and `logs.py` are all untouched. The CORE
section 7 grep gate is what enforces this, mechanically rather than by review (§7 above).

**What is deliberately not in this list**, because CORE section 7 forbids it even for a
fourth source type: a base class or shared interface for sources, a plugin registry or
dynamic import by string, a config UI, or a generic connector abstraction. Kafka, Oracle and
Files justify exactly the one abstraction already built (`SourceSpec` + `RunContext` +
`RunResult`); nothing here raises that count.

### BigQuery — contract notes only, no code (`docs/build_log/DECISIONS.md` D-08)

D-08 is still open: the right shape for a BigQuery source is not yet known, and CORE section
12 keeps it out of scope beyond this note. Two questions the eventual design must answer
before any `sources/bigquery/` package exists:

1. **Which connector, and is it available on the target runtime?** Databricks Runtime does
   not bundle a BigQuery connector by default, the way it does not bundle the Oracle JDBC
   driver (VB-22) — whichever connector is chosen needs the same "is it installed, which
   version" verification Oracle's did.
2. **Direct read, or export-to-GCS staging?** This decides which of the two existing sources
   BigQuery ends up looking like, per D-08's own table: a bounded query plus a watermark
   looks like **Oracle** (reuses the cursor/watermark machinery in `sources/oracle/` almost
   exactly); exporting to GCS and reading the exported objects looks like **Files** (reuses
   Auto Loader, at the cost of an export-orchestration step this framework does not
   currently own). Federation was considered for Oracle and rejected (D-07) for reasons that
   likely apply here too, but that is a question to settle explicitly, not assume.

Also unresolved, per D-08: whether cross-cloud egress from GCP to Azure is acceptable in cost
and policy, and who owns the GCP-side credentials. None of this is guessed at here — it is
recorded so the eventual design starts from these questions rather than from a blank page.
