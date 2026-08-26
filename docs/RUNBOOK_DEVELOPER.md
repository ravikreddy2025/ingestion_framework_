# Developer Runbook

For the engineers who own, extend and deploy this codebase.

**Lost in the file tree?** [NAVIGATION.md](NAVIGATION.md) maps every file and traces one
record of each source type through every module in execution order — the fastest way to
orient.

**Prerequisite reading:** [DESIGN.md](DESIGN.md), plus that source's own
[DESIGN_KAFKA.md](DESIGN_KAFKA.md) / [DESIGN_ORACLE.md](DESIGN_ORACLE.md) /
[DESIGN_FILES.md](DESIGN_FILES.md). This runbook tells you *how to do things*; the design
docs tell you *why the code is shaped the way it is*. Do not change core behaviour without
reading that source's failure-scenario table.

**Your first hour, in order:**

1. [NAVIGATION.md](NAVIGATION.md) — "The ten-minute path" (10 min)
2. `src/kafka_ingest/framework/contracts.py` and `runner.py` — the whole contract and the
   whole lifecycle, both short (15 min)
3. One trace in [NAVIGATION.md](NAVIGATION.md) — whichever source type you touch first (10 min)
4. §1 below — get the tests running locally (20 min)

---

## 1. Local setup — no cluster, no Kafka, no Oracle, no ADLS

**This is local development only.** There is no Kafka, Oracle, ADLS or Databricks workspace
reachable from here, and there never needs to be for everyday work: the fast test suite runs
entirely against recording stand-ins in `tests/conftest.py`.

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]" pyspark==3.5.2   # Windows; use .venv/bin on Linux/macOS
pytest -m "not spark" -q                                # fast gate, no JVM needed
```

`pyspark` is installed as **dev tooling**, not a runtime dependency (the project's own two
runtime dependencies are `PyYAML` and `requests` — nothing else may be added to that list).
It is needed only because `framework/audit.py` and other modules import `pyspark.sql` at
module scope, so even *collecting* the fast suite requires the package to be importable —
no JVM is started and no `SparkSession` is ever built by anything in `-m "not spark"`. Pin
the version to match the target runtime (currently DBR 16.4 LTS / Spark 3.5.2 — VB-14);
`azure-pipelines.yml` pins the identical version for the same reason.

For the JVM-backed (`-m spark`) tests you need three more things — all three, or they skip.
These are a small minority of the suite and are **not** part of the CI gate:

1. **A JDK with `JAVA_HOME` set.** `winget install Microsoft.OpenJDK.21`
2. **The `spark-avro` jar**, which is **not** in the PySpark pip package (it ships the Avro
   Java library, not the Spark connector that provides `from_avro`). Download it into
   `.venv/Lib/site-packages/pyspark/jars/`:
   ```
   https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.5.2/spark-avro_2.13-3.5.2.jar
   ```
3. **Do not set `spark.jars.packages` on Windows.** Ivy resolution shells through Hadoop's
   `Shell` class, needs `winutils.exe`, and kills the whole `SparkContext` with a
   misleading `HADOOP_HOME is unset` error. Without that setting the missing
   `winutils.exe` is a harmless warning and local Spark works fine.

```bash
ruff check src tests       # lint - the same command CI runs. Must return clean.
ruff format --check src tests  # formatter - enforced since Stage 3. Must return clean.

pytest -m "not spark" -q   # the fast suite - THIS IS THE ONLY GATE YOU CAN RUN LOCALLY
                            # end to end. No JVM. This is the CI gate too.
pytest -m spark -q         # only the JVM-backed ones, if the three prerequisites above are met
pytest -q                  # everything (needs the same three prerequisites, or the spark ones skip)
```

**The three commands above are the whole gate.** Do not run, and never report as run,
anything needing Kafka, Oracle, ADLS or a Databricks workspace: `databricks bundle
validate`, a real JDBC connection, a real ADLS read. None of them are reachable from this
environment. What cannot be checked locally is written defensively and recorded in
[VERIFICATION_BACKLOG.md](VERIFICATION_BACKLOG.md) instead of asserted as working.

Local Spark is **not your target DBR**. A local pass is evidence, not proof of runtime
behaviour — which is why Kafka's `curated.assert_from_avro_semantics()` re-checks the
critical writer/reader schema assumption at job startup on the real cluster. Run
`notebooks/01_run_unit_tests` on a cluster before shipping a change that touches parsing.

---

## 2. Codebase tour, by module layout

```
framework/       the spine EVERY source shares. Never names a source type outside runner.py.
  contracts.py     SourceSpec, RunContext, RunResult - the whole contract. No PySpark import.
  config.py        five-layer load + merge + placeholders + spec-driven validation. No PySpark.
  control.py       operational control table -> a plain override dict
  security.py      secrets, redaction, session-scoped credentials. Names no system.
  state.py         durable watermark + run sequence. WRITES RAISE ON FAILURE.
  audit.py         one shared table, every source type. WRITES NEVER RAISE.
  tables.py        target-name rendering/validation + the only CREATE TABLE in the codebase
  writers.py       append (idempotency markers) / merge (mandatory partition predicate)
  checkpoint.py    the checkpoint-reset guard, shared by Kafka and Files
  runner.py        the run lifecycle. _SOURCES lives here.   <- start here
  logs.py          structured log lines, credential redaction by key-name hint
sources/
  kafka/           spec.py + run.py, and reader/registry/wire/landing/curated/tables/security/listener
  oracle/          spec.py + run.py, and query/reader/types/landing/tables/security/config
  file/            spec.py + run.py, and reader/landing/tables/security/config
entrypoints/
  run_ingest.py    argparse -> runner.run(). One job serves every source of every type.
  run_replay.py    argparse -> runner.run(run_type=...). Every replay_* parameter any
                   shipped source declares.
```

If you only read two files, read `framework/contracts.py` (the whole contract, ~120 lines)
and `framework/runner.py` (the whole lifecycle, one function). Every source package's
`run.py` module docstring is that source's design in miniature.

### Invariants — do not break these without a design discussion

| Invariant | Why | Enforced by |
|---|---|---|
| `framework/config.py` and every `sources/*/spec.py` import no PySpark | Keeps config/validation testable in plain CI, with no cluster | Convention + `tests/test_framework_config.py`'s `test_these_modules_import_no_pyspark` |
| `framework/` never names a source type outside `runner.py`'s `_SOURCES` | The falsifiable proof that adding a source type needs zero framework changes | The CORE section 7 grep, wired into `azure-pipelines.yml` |
| A source's entire public surface is `SOURCE_SPEC` + `run(ctx) -> RunResult` | No `read()`/`parse()`/`write()`/`validate()` — see DESIGN.md §2 for why a shared step-level contract leaks | Convention; `tests/test_framework_runner.py`'s dispatch tests |
| Structural fields are never operationally overridable | Partitioning, merge/dedup keys, target names, Oracle's `source_schema`/`source_table`/`filter_criteria`, a file's target path — a 3am `UPDATE` must never move what is already on disk | `framework/config.py::apply_overrides` (ignores, logs); `framework/control.py` (same rule, layer 4) |
| State writes raise on failure; audit writes never raise | Extraction correctness depends on state and must not depend on best-effort audit — the opposite ordering silently corrupts data | `framework/state.py::write_state` (unwrapped); `framework/audit.py::AuditWriter.emit` (try/except) |
| Every `framework/writers.merge()` call takes a partition predicate, no default | A MERGE with no predicate rewrites every partition it might match | `merge()`'s signature — asserted both as a `TypeError` and against `inspect.signature`, so a future default cannot silently reappear |
| Oracle's cursor extraction is a closed interval, never an open upper bound | `> last_watermark AND <= run_high_water`, captured before the read | `sources/oracle/query.py::build_query`; `sources/oracle/run.py`'s ordering |
| Watermark order: read → write → commit → **then** advance | A crash before the advance re-reads the same interval (safe); advancing first would silently skip a window | `sources/oracle/run.py::run()` — the four numbered steps in its own docstring |
| A replay never advances production state | An incident-scoped re-extraction must not strand or advance the scheduled run's position | `sources/oracle/run.py::_advance_watermark` (returns early for a replay); Kafka's replay uses an isolated checkpoint by construction |
| Runtime dependencies are exactly `PyYAML` and `requests` | Development tooling may be added; runtime dependencies may not | `pyproject.toml` |

---

## 3. Onboard a source

**One PR. No Python changes, ever.** The three source types follow the same shape; only the
template and the questions differ.

1. Copy the right template: [`conf/sources/_TEMPLATE.yaml`](../conf/sources/_TEMPLATE.yaml)
   (Kafka), [`_TEMPLATE_oracle.yaml`](../conf/sources/_TEMPLATE_oracle.yaml) (Oracle), or
   [`_TEMPLATE_file.yaml`](../conf/sources/_TEMPLATE_file.yaml) (File) to
   `conf/sources/<source_key>.yaml`. **The filename is the `source_key`** — globally
   unique across every source type, it becomes a job parameter, the control-table key, a
   checkpoint/state key and a column in every audit row. `lower_snake_case`. Renaming it
   later orphans a checkpoint or a watermark.
2. Fill in every `<ANGLE_BRACKET>`. Each template opens with the questions that cannot be
   answered from the Databricks side (partition count and retention for Kafka; the cursor
   column, stable key, partition column and exotic types for Oracle; schema drift risk,
   in-place rewrites and filename-encoded data for Files) — answer those with the owning
   team before filling in anything else.
3. Add a task to the matching job template — `resources/job_ingest_primary.yml` (Kafka),
   `job_ingest_oracle.yml` (Oracle) or `job_ingest_file.yml` (File) — copy an existing
   block, change `task_key` and `source-key`.
4. Validate locally, then deploy:
   ```bash
   pytest tests/test_shipped_config.py tests/test_shipped_jobs.py -q
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```
5. Run `notebooks/00_validate_config` → `02_check_connectivity` → `03_run_ingestion` in
   dev, pointing at scratch targets.

`test_shipped_config.py` is the safety net — it catches a non-3-tier name, an unknown
cluster/registry/jdbc/storage reference, a DBFS cert path, two sources sharing a checkpoint,
and two sources sharing a landing table, across the full source × environment cross
product. It already runs on every PR via `azure-pipelines.yml`.

### If the source needs a new connection profile

Add a block to `conf/clusters.yaml` / `registries.yaml` / `jdbc.yaml` / `storage.yaml` in
the same PR — all four are registers following the identical pattern: what is TRUE in every
environment lives in the register (auth mode, secret key names); what DIFFERS per
environment (endpoint, host, account, secret scope) is overlaid in
`conf/environments/<env>.yaml`. A profile named in an environment file that is not in its
register is a hard error listing the ones that are.

Coordinate two things with platform/infra **before** the PR:
- secret scope exists, keys are populated, the job's SP has `READ`
- for an mTLS Kafka cluster, or Oracle over a network path not yet confirmed: certs / JDBC
  driver / network reachability, per the relevant `docs/VERIFICATION_BACKLOG.md` entry.

---

## 4. Which layer does my change belong in?

Configuration merges five layers (one with a rare optional sub-layer), later winning per key:

```
conf/defaults.yaml -> conf/defaults/<type>.yaml -> conf/environments/<env>.yaml
                                                        |
                                                conf/sources/<key>.yaml
                                                        |
                                              environments:<env> (3a, rare)
                                                        |
                              operational control table  ->  job parameters
```

```
Is the value the same in dev, preprod AND prod?
  YES -> Is it the same for every source of every TYPE?
           YES -> conf/defaults.yaml
           NO  -> Is it the same for every source of ONE type?
                    YES -> conf/defaults/<type>.yaml
                    NO  -> conf/sources/<key>.yaml
  NO  -> Does it vary by environment only?  (catalog, broker, JDBC host, storage account, secret scope)
           YES -> conf/environments/<env>.yaml (`defaults:` for every type, `defaults_by_type:
                  <type>:` for one type)
           NO  -> Does it vary by BOTH one source AND one environment? (rare)
                    YES -> that source file's environments:<env> sub-layer (3a) - see
                           conf/sources/vector_patient_events.yaml for a real example
                    NO  -> the source file, and check whether the environment file should
                           carry a different default for that type instead

Does support need to change it during an incident, without a deploy?
  YES -> it must be in that source type's SOURCE_SPEC.operational_keys, and (if it needs a
         standing lever rather than a one-off replay parameter) declared in
         SOURCE_SPEC.control_columns with a matching column in sql/01_operational_config.sql.
```

Partitioning, merge/dedup keys and target names are deliberately **not operationally
overridable** — changing a table's physical layout or its dedup/merge semantics should
require review. Every source's own test module asserts an operational override of those is
ignored, so it is a tested contract, not a convention.

**Never hardcode a catalog, schema, storage account or JDBC host in a source file.**
It works in whichever environment you tested and silently breaks the others.
`tests/test_shipped_config.py` resolves every source in every environment specifically to
catch this.

---

## 5. Common code changes

### Add a source type

See [`DESIGN.md` §8](DESIGN.md#8-adding-a-source-type) for the full checklist and the grep
gate that proves nothing under `framework/` changed. In short: a new `sources/<type>/`
package with `spec.py` (`SOURCE_SPEC`, no PySpark import) and `run.py`
(`run(ctx) -> RunResult`), one dict entry in `runner.py`'s `_SOURCES`, a
`conf/defaults/<type>.yaml`, an onboarding template, a `docs/DESIGN_<TYPE>.md` following the
pattern `DESIGN_KAFKA.md`/`DESIGN_ORACLE.md`/`DESIGN_FILES.md` already set, and — only if
the type needs a connection kind none of the existing four registers cover — a new register
file.

### Add a control-table lever for an existing source type

1. That source's `spec.py` — add the setting to `operational_keys` (and, if it should also
   have a reviewed default, to `structural_keys` too), and add the prefixed column to
   `control_columns` (`<type>_<setting>`).
2. `sql/01_operational_config.sql` — add the column, `<type>_<setting>`, with a `CHECK`
   constraint if the value set is bounded.
3. That source's own `config.py` or `run.py` — read the setting.

The framework never learns the column's name; `framework/control.py` reads whatever the
calling spec declares.

### Add an audit column

1. `framework/audit.py` → `AUDIT_DDL_COLUMNS`, `AUDIT_SCHEMA` (a `StructField`), and
   `AuditWriter.build_row`.
2. `sql/02_layer_tables.sql` → keep the reference DDL in step.

A test asserts all three agree column-for-column — drift surfaces as a confusing Delta
schema error on the first append otherwise, not before.

### Change a partition column

Config change plus a **table rewrite** — Delta cannot repartition in place. Plan it:
create the new table, backfill (a curated replay for Kafka's curated layer; a
`CREATE TABLE AS SELECT` for a landing table), swap names, then update the YAML.

### Change what Kafka's curated layer looks like

The shape decisions (payload stays nested; no `ce_extensions` map; no array explosion) are
documented in `DESIGN.md`'s Kafka section and pinned by tests. Flattening the payload or
exploding arrays breaks the `(topic, kafka_partition, kafka_offset)` merge key that makes
replay idempotent — read that section first, then update the test deliberately rather than
deleting it.

### Add a run shape (e.g. a new replay type)

Add a function to that source's `run.py` next to its existing run shapes, dispatch it in
`run()` on `ctx.run_type`, and add the parameters `entrypoints/run_replay.py` needs to pass
them through as job parameters. Do **not** add a parallel implementation of the microbatch
or extraction body — each source has exactly one, and keeping it that way is what makes the
audit and idempotency stories hold for every run shape.

---

## 6. Debugging

| Symptom | First look |
|---|---|
| Job fails at startup with `ConfigError` | The message names the file and key. Validation is deliberately loud. |
| `REFUSING TO RUN ... checkpoint ... is missing` | The checkpoint-reset guard (`framework/checkpoint.py`), Kafka or File. **Do not delete landing rows to get past this.** Support's sanctioned bypass is the source's own `<type>_checkpoint_reset_id`, single-use — RUNBOOK_SUPPORT §5.4a / §9.4. |
| `from_avro reader/writer schema self-check failed` (Kafka) | DBR older than the target floor, or the runtime changed `from_avro` semantics. |
| `SchemaResolutionError ... HTTP 404` (Kafka) | Records were produced against a **different** registry than the source's YAML points at. |
| `SchemaResolutionError ... unreachable ... NCC` (Kafka) | Network path. On serverless, a missing private endpoint. |
| Oracle run stops before writing, naming a column and two types | Schema drift — a type change or a dropped column. Not a control-table fix; decide deliberately (`sources/oracle/types.py::assert_no_drift`). |
| A file batch is refused naming `_rescued_data` | A row did not fit `schema:`. `file_failure_mode = QUARANTINE` unblocks it; the mismatched rows land with `_rescued_data` populated. |
| The same batch/run fails repeatedly | Poison batch (Kafka) or a source repeatedly refusing the load (Oracle). See the support runbook — the fix is usually a control-table change, not code. |
| Duplicate rows in landing | Check `ingest_control` — was `merge_keys` waived (Oracle), or was a checkpoint reset reused? Read `DESIGN.md`'s failure-scenario table for that source first. |

Logging is plain `logging` at INFO, written to stdout so it lands in the Databricks driver
log. Every line carries `source_type`/`source_key`/`run_id`
(`framework/logs.py::RunLog`), and every connection-options map passes through
`redact()` before it is logged — **never log a raw options dict.**

---

## 7. PR checklist

- [ ] `ruff check src tests` is clean — CI runs exactly this
- [ ] `ruff format --check src tests` is clean
- [ ] `pytest -m "not spark" -q` passes (this is the PR gate)
- [ ] `pytest tests/test_shipped_config.py tests/test_shipped_jobs.py -q` passes if you
      touched `conf/` or `resources/`
- [ ] `pytest -m spark -q` passes locally if you touched Kafka parsing or projections, and
      the three local-setup prerequisites are met
- [ ] `databricks bundle validate -t dev`
- [ ] The CORE section 7 grep returns nothing if you touched `framework/`:
      ```bash
      grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
        | grep -v 'runner.py:.*_SOURCES'
      ```
- [ ] No secret value, table name, path or endpoint hardcoded in Python or in a source file
- [ ] New/changed config keys documented in `docs/CONFIGURATION.md` **with a tier marker**,
      and mirrored inline in the relevant YAML file
- [ ] If you changed a DDL constant, the matching Python constant (and `sql/01`/`sql/02`)
      changed too
- [ ] If you changed behaviour described in `DESIGN.md`, that doc changed too
- [ ] A new test can fail: break the line it covers, watch it fail, restore it
- [ ] Comments explain **why**, not what

---

## 8. Known gaps to be aware of

**The list lives in one place: [`docs/VERIFICATION_BACKLOG.md`](VERIFICATION_BACKLOG.md).**
It is not duplicated here on purpose — a second copy is exactly how one goes stale while the
other is updated. Read that file for the current, complete, damage-ordered list.

The three most likely to bite a developer first:

1. **The Oracle JDBC driver is not installed anywhere in this repository** (VB-22), and
   nothing here can install it — that is a platform task. Its version also decides two of
   the most damaging entries in the backlog (`NUMBER`/`DATE` type mapping, VB-02/VB-03).
2. **`ingest_state`'s MERGE actually upserting is unverified** (VB-15) — every source
   type's re-run idempotency for a batch read depends on it, and a silent no-op here looks
   exactly like a healthy job with nothing new to write.
3. **Delta MERGE schema evolution mechanism is runtime-dependent** (VB-09) — `writers.merge`
   already hedges between `withSchemaEvolution()` and a session-flag fallback; confirm which
   branch your runtime takes the first time a curated replay or an Oracle landing MERGE
   needs to widen a schema.
