# Design

For the team taking this over. Read this before changing code.

**Scope: Kafka → landing/curated. Oracle → landing. Files → landing.** Nothing downstream
of a source's own layers is in this package.

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

## 4. Kafka — re-runs and duplicates

**The most important section for anyone changing `sources/kafka/run.py`.**

### Two mechanics everything follows from

1. **Checkpoint protocol.** Spark writes the batch's offset range to `<checkpoint>/offsets/N`
   *before* running `foreachBatch`, and `<checkpoint>/commits/N` *after* it returns
   cleanly. On restart, if `offsets/N` exists but `commits/N` does not, Spark
   **re-executes batch N over the identical offset range**. A retry never reads a
   different set of records.
2. **Delta idempotent writes.** `txnAppId` + `txnVersion` (the microbatch id) are recorded
   **per table**. A repeat write with an already-seen version is skipped. That is why one
   shared `txn_app_id` is correct: landing skips itself while curated still proceeds.

`txn_app_id` is derived from `source_key` + run type + replay lineage
(`sources/kafka/config.py`). It **must stay stable across restarts** — never make it random
per run.

### Failure scenarios

| # | Failure | Duplicates? | Fix — no code change |
|---|---|---|---|
| 1 | Curated fails, transient | No | Re-run |
| 2 | Curated fails persistently (unregistered schema) | No | Register schema, **or** `kafka_failure_mode → QUARANTINE` |
| 3 | Driver dies between landing and curated | No | Re-run |
| 4 | Landing write itself fails | No | Fix cause, re-run |
| 5 | Job timeout mid-run | No | Re-run; lower `max_offsets_per_trigger` |
| 6 | **Primary checkpoint deleted** | **No — silent data loss instead** | Kafka checkpoint-reset procedure (RUNBOOK_SUPPORT §5.4a) |
| 7 | Batch succeeded, curated data wrong | No | Curated replay job |
| 8 | Records genuinely missed | No | Kafka replay job |

#### Scenario 1-3 — self-healing

```
Batch 5, attempt 1:  landing append (X,5) → COMMITTED
                     curated write        → FAILS
                     no commits/5         → run fails
Batch 5, retry:      landing append (X,5) → SKIPPED by Delta
                     curated write        → SUCCEEDS
                     commits/5 written    → done
```

#### Scenario 2 — poison batch, the one that actually hurts

A *persistent* parse failure retries forever; the topic never advances. Not a duplicate
problem — a stuck stream. Fixes, in order of preference:

1. Register the missing schema. Next run succeeds naturally.
2. `UPDATE ... SET kafka_failure_mode = 'QUARANTINE'` (see `sql/03_support_queries.sql` Q6).
   Bad records go to quarantine with raw bytes, the batch completes, the stream drains.
   Recover them later with the **curated replay** job, then set it back to `FAILFAST`.

#### Scenario 6 — the only failure that looks like success

Deleting the primary checkpoint restarts `batchId` at 0. Delta has already recorded high
versions for this `txnAppId`, so appends are **skipped as duplicates**. The job reports
success and writes nothing.

`framework/checkpoint.py::guard_against_checkpoint_reset` refuses to run when the primary
checkpoint is missing **and** the landing table already holds rows for this source. A
genuine first run is unaffected: its own landing table has no rows yet.

**Never delete a primary checkpoint. Use the Kafka replay job** to recover the missed *data*
— it derives a different checkpoint *and* a different `txnAppId` from `rerun_id`, so neither
collides. Replay does **not**, by itself, unblock the *primary* job: the guard keeps refusing
every trigger until support sets `kafka_checkpoint_reset_id` in the control table, which
forks the txnAppId lineage the same way `rerun_id` does, so the restarted primary also gets
an identity with no committed history to collide with. See
[`RUNBOOK_SUPPORT.md` §5.4a](RUNBOOK_SUPPORT.md#5-4a-restarting-the-primary-after-a-genuine-checkpoint-loss).

### ⚠️ Verify this on your DBR before go-live

`txnAppId` / `txnVersion` behaviour is taken from Delta's documented contract. In
particular, confirm whether the skip triggers on `txnVersion ≤ last recorded` or only on
`==`; scenario 6 depends on it. Test by killing a job deliberately between the landing and
curated writes, then re-running and checking `sql/03_support_queries.sql` **Q11**
(duplicate check) returns zero rows.

## 5. Kafka design decisions

#### Payload stays nested

Curated stores the parsed record as **one `payload` STRUCT**, not flattened columns.

- Every curated table has the same recognisable outer shape, so an operator moving between
  topics does not relearn the columns.
- No collision is possible between business fields and framework columns.
- Nested structures survive as structs/arrays/maps: query `payload.patient.id`, read with
  `SELECT to_json(payload)`.
- Business fields in config are therefore written as `payload.<field>` — e.g.
  `curated_dedup_keys: [payload.claim_id]`.

**Arrays are not exploded.** Curated is 1:1 with Kafka records. Exploding would break the
`(topic, kafka_partition, kafka_offset)` merge key that makes replay idempotent. Fan-out is
a modelling decision that belongs downstream.

#### Reader schema is mandatory; a "writer" mode does not exist

`from_avro(payload, jsonFormatSchema, options)` maps onto Avro's two-schema resolution:
positional = **writer** (how the bytes were encoded), `avroSchema` option = **reader**
(the shape you want out). The reader schema fixes the `payload` struct's type.

Because curated stores payload as one struct column, two writer versions decoded *without*
a common reader schema would produce two incompatible struct types that cannot share a
table. So there is no "use each writer schema as-is" mode — only `registry_latest` and
`pinned_id`. Avro resolution fills reader-only fields from defaults and drops writer-only
fields, so a mixed-version batch lands cleanly.

That positional/option mapping is easy to get backwards and has moved between Spark
versions, so `sources/kafka/curated.py::assert_from_avro_semantics()` **proves it at
runtime**, once per run, with a 5-byte synthetic record. A DBR upgrade that changed the
behaviour fails the job at startup instead of silently mis-decoding production data.

#### Partitioning, not Liquid Clustering

Delta allows `PARTITIONED BY` or `CLUSTER BY`, never both.

- **landing** `(ingest_date)` — one table per topic, so `topic` is constant inside it and
  gives per-topic file isolation for retention and replay. `ingest_date` stops one topic's
  partition growing without bound.
- **curated** `(event_date)` — already one table per topic. `event_date` = date of
  `ce_time`, falling back to `kafka_timestamp`. Late-arriving events write into older
  partitions; that is expected.

#### MERGE vs append

| | Primary | Replay |
|---|---|---|
| **landing** | append + `txnAppId`/`txnVersion` | MERGE on record key, **insert-if-absent only** |
| **curated** | append + `txnAppId`/`txnVersion` | MERGE on record key, **upsert** |

Append on the primary path because exactly-once is already guaranteed by checkpoint +
idempotent markers; MERGE would scan the target every batch to discover it has nothing to do.

MERGE on replays because a replay overlaps existing data *by definition*.

**The asymmetry is the important part.** Landing records *what arrived* — overwriting a
row's provenance with replay metadata would destroy the thing replays are meant to be
distinguishable by. Curated records *what the data means* — replacing a bad parse is the
entire point of a curated replay.

`framework/writers.py::merge()` requires a partition predicate with no default. Landing's
own replay call passes `"true"` deliberately: landing is partitioned by `ingest_date` (the
date a row was *written*), so a replayed record's `ingest_date` never matches its original
twin's — any derived bound would match nothing and insert duplicates. Curated's replay call
computes a real `event_date BETWEEN` bound, because `event_date` is a property of the record
itself and both copies share it.

#### CloudEvents

CloudEvents v1.0, Kafka binary content mode: context attributes arrive as `ce_*` headers
and the event data is the Kafka value — exactly the Avro payload. Eight attributes are
promoted to typed columns in both layers. Header matching is case-insensitive.

Two judgement calls:

- **`ce_time` is stored as STRING, verbatim.** RFC3339 UTC sorts correctly as a string, and
  parsing here would either silently NULL a malformed value or throw under ANSI mode.
  `event_date` is derived separately with a regex guard so a malformed `ce_time` can never
  fail a batch.
- **No `ce_extensions` MAP.** Kafka permits duplicate header keys and map construction from
  them errors on duplicates. Non-standard attributes stay in `kafka_headers`.

#### Hand-rolled `foreachBatch`, not Lakeflow Declarative Pipelines

LDP is the better default for most medallion work, and is the right tool downstream of
curated. It does not fit *this* layer, for three concrete reasons:

1. **Per-record writer-schema resolution needs a driver-side registry call inside the
   batch.** The query plan is a function of the schema ids *in that batch*, so it cannot be
   declared up front.
2. **Replay checkpoint isolation is not expressible.** LDP owns its checkpoints; the replay
   pattern here depends on *choosing* the checkpoint path per run so a starting-offset
   override is honoured at all.
3. **Bounded replay and the asymmetric MERGE are per-run imperative decisions.**

If LDP gains user-controlled per-run checkpoint/offset overrides, revisit this.

### Kafka — deliberately not built

- **Pluggable serialization formats.** Avro-via-Schema-Registry is the only format in
  scope; `sources/kafka/registry.py` refuses non-`AVRO` `schemaType` with a clear error.
- **A Kafka client factory.** There is one client.
- **Executor-local cert staging** (`SparkContext.addFile`). Documented in
  `sources/kafka/security.py` as the fallback if a compute profile cannot read UC Volumes
  from executors (VB-11).
- **Per-partition replay timestamps.** `replay_starting_timestamp` covers what support asks
  for.
- **`ce_extensions` map**, **payload flattening**, **array explosion** — see above.

---

## 6. Where to make common changes

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
| Add a source type | §12 below | Yes (PR, plus the grep gate must stay clean) |

The last two are the only framework-wide ones, and a test asserts the audit schema and DDL
stay in step (`tests/test_framework_audit.py`).

---

## 7. Testing

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
| `tests/test_offline_validation.py` | YAML parses; every job entrypoint resolves; every `source_type` has a package; every register reference resolves |

Run the full suite on Databricks via `notebooks/01_run_unit_tests` — DBR has Spark and
`spark-avro` built in, so it runs with no setup. No test connects to Kafka, Oracle or ADLS,
reads a secret, or writes to a table.

---

## 8. The unverified-claims list

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

## 9. Deliberate non-abstractions

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

The falsifiable gate, run in CI on every PR:

```bash
grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
  | grep -v 'runner.py:.*_SOURCES'
```

If this returns anything, the spine has leaked.

---

## 10. Oracle — the watermark, and what can go wrong

### The order, and why it is the whole design

    capture the high water -> read the closed interval -> write landing -> THEN advance

A crash anywhere before the advance leaves the stored watermark where it was, so the next
run re-extracts the same interval — which the merge key absorbs. Advancing first would mean
a crash silently skipped a window, and nothing downstream could detect that.

`ingest_state` holds the watermark, not the audit table. Audit writes are best-effort by
design and must never raise; extraction correctness must not depend on best-effort writes.

### The closed interval

`cursor > last_watermark AND cursor <= high_water`, with the upper bound captured at run
start from `MAX(cursor)` over the rows this extract can see — never from a clock, because a
clock reading is ahead of every committed row by definition and would move the watermark
past rows still in flight.

**The gap this does not close:** a transaction already open when the high water was
captured, carrying a cursor value below it, that commits after the extract has read past
that value, is never seen. That is inherent to a cursor over a wall-clock column, and how
much it matters is a property of the SOURCE APPLICATION — whether it stamps the cursor at
statement time or at commit. It is VB-25, it is stated at the top of `sources/oracle/run.py`,
and it is a question for the source team at onboarding rather than something this code can
detect.

### Failure scenarios

| # | Failure | Duplicates? | Rows lost? | Fix — no code change |
|---|---|---|---|---|
| 1 | Transient JDBC failure (network, session killed) | No | No | Re-run. The watermark never moved, so the same interval is re-read. |
| 2 | Write fails after a successful read | No | No | Re-run. Same interval; nothing was committed and nothing advanced. |
| 3 | Crash between the write and the watermark advance | **Only if `merge_keys` waived** | No | Re-run. With merge keys the re-read de-duplicates; a waived source appends the interval twice — Q19 finds the duplicates. |
| 4 | Watermark manually corrupted (edited too far forward) | No | **Yes — silently** | Q18: set it back to a known-good value from the audit table's `position_end`, then re-run. A watermark set BACKWARDS is safe with merge keys and duplicates without them. |
| 5 | Cursor values arrive out of order (late commit, low cursor) | No | **Yes — silently** | Not fixable by re-running: the interval has been read. VB-25. Remedies in order: a safety lag on the high water, `incremental_mode: full`, or a change-tracking mechanism. |
| 6 | Source table altered — new column | No | No | Nothing. Additive changes are allowed and Delta widens the target. |
| 6b | Source table altered — type changed, or a column removed | No | No | **The run stops before writing**, naming the column and both types. Decide deliberately: ALTER the landing table, pin the old type with `column_types`, or recreate. |
| 7 | Source table dropped or renamed | No | No | The read fails loudly. Fix the source file (or the source), then re-run. |
| 8 | A full load run against a `merge_keys: []` source | **Yes** | No | Expected: that source appends. Delete the duplicate `ingest_date` partition, or set merge keys. |

### Idempotency

`txnAppId = ingest::oracle::<source_key>`, `txnVersion = run_sequence` from `ingest_state`.
One identity per source, with **no fork for a replay** — unlike Kafka's, which forks on the
rerun id. The difference is what supplies the version: Kafka's is a microbatch id that
restarts at 0 in a replay's own checkpoint, while `run_sequence` is allocated on every run
of every type and therefore always increases.

The markers apply to APPENDS only; Delta does not honour them on a MERGE, so a merging
source's idempotency comes from its merge key instead.

### Why landing keeps every version

The merge key is `merge_keys + cursor_column`, so `(CLAIM_ID, LAST_UPDATE_DT)` identifies a
version of a claim. Two consequences, both deliberate:

* landing is a retained mirror — a replay or a historical reprocessing can see what the
  source held at a point in time, which merging on the business key alone would destroy;
* the key does not change when `oracle_incremental_mode` does. A full run keyed on the
  business key alone would match every historical version of a claim with one source row
  and overwrite all of them — a control-table UPDATE causing data loss.

A source with **no** cursor has no version identity, so its merge updates matched rows: the
key identifies the row, a match means it changed, and the mirror would go stale otherwise.

---

## 11. Files — Auto Loader, the shared checkpoint-reset guard, and what is deliberately
not built

### Why Auto Loader, and why `availableNow` always

CORE section 10 decided this ahead of Stage 5: Auto Loader (`cloudFiles`) over a hand-rolled
processed-files ledger. A ledger is Auto Loader re-implemented with worse listing
performance and a new correctness surface — tracking which files have been seen is exactly
what `cloudFiles.schemaLocation` and the stream checkpoint already do. The consequence
accepted knowingly: **this source is checkpoint-based**, exactly like Kafka's primary
stream, with everything that implies about restart safety.

There is no per-source `trigger:` the way Kafka has one — this source is always a bounded
`availableNow` run, because that is the only shape a scheduled Workflows run has a natural
end under, and there is no case in this framework's scope for a genuinely continuous file
stream.

### The checkpoint-reset guard is shared, not duplicated

Kafka and Files call the same implementation, `framework/checkpoint.py::
guard_against_checkpoint_reset`, rather than each carrying its own copy. Both sources are
checkpoint-based; Oracle has no checkpoint at all, so its correctness rests on
`ingest_state`'s watermark instead — this remains two callers, not three, and the promotion
was justified by the cost of the two copies having already drifted once (see below), not
by clearing CORE section 7 rule 4's "three implementations" bar.

The shared function checks "does the landing table hold any row at all" — no `topic`-style
filter, because every source that calls it already has one landing table per source (Kafka's
included: `{catalog}.landing.{topic_table}` is one table per topic).

The shared refusal message does not claim a replay mechanism unconditionally: the file
source has no replay job (this section's own "deliberately not built" list, below), so the merged
message states only what is true for every caller — set this source's own control column to
an unused incident id. The reset-engaged log event also carries no per-source prefix; every
log line already carries `source_type` and `source_key` (`framework/logs.py`).

### `cloudFiles.schemaLocation` and the reset guard — the decision

`schemaLocation` is a second checkpoint-like resource, living beside the stream checkpoint
under the same Volume root, keyed by the same `source_key` (`sources/file/config.py`
`checkpoint_path` / `schema_location_path` are siblings). **Decision: the reset guard covers
the stream checkpoint directly and the schema location indirectly, through that shared
`source_key` scoping — it is not probed or reset separately.**

Reasoning: a `checkpoint_reset_id` forks the stream's Delta transaction identity so a
restarted stream has no committed versions to collide with. It does **not** delete or move
`schema_location_path` — nothing in this framework does, ever, matching the "do not build a
processed-files ledger, do not build file archiving" scope boundary below. A fresh stream
under a forked identity re-applies (`schema_mode: provided`) or re-infers
(`hints`/`infer`) against whatever is already at that path for this `source_key`, which is
unaffected by the reset. The failure mode this would NOT catch — a schema recorded before an
incident no longer matching reality — is the same as an ordinary schema-mode-`infer` risk
documented in `docs/CONFIGURATION.md`, not something specific to a reset.

### Failure scenarios

| # | Failure | Duplicates? | Rows lost? | Fix — no code change |
|---|---|---|---|---|
| 1 | A malformed file mid-batch (unparseable row) | No | No, if `failure_mode: QUARANTINE` | The batch lands with `_rescued_data` populated for that row; investigate and re-onboard the fix. Under `FAILFAST` (the default) the whole batch is refused and retries until fixed or the mode is flipped. |
| 2 | A file rewritten in place, under the same path, after Auto Loader has already processed it | No | **Yes — silently** | Auto Loader tracks files it has SEEN, not their content hash by default; a rewrite under the same name is not re-read. Producers must write under a NEW name (a convention, not something this code enforces) — document this in the onboarding checklist for a landing zone at risk of it. |
| 3 | A file arrives late (after the run that would ordinarily have picked it up) | No | No | The next scheduled run picks it up — `availableNow` drains whatever is present, whenever it runs. Nothing to do. |
| 4 | Schema drift between files (a new column, a changed type) | Depends on `schema_mode` | Depends | `provided`: a genuinely new column is dropped unless `rescuedDataColumn` catches it (VB-06); a type mismatch across files becomes rescued data or a cast failure depending on format. `infer`/`hints`: the inferred schema can change between runs with no review — this is exactly why `provided` is the platform default. |
| 5 | `cloudFiles.schemaLocation` deleted | Behaves like a genuine first run for schema purposes | No, if the stream checkpoint is intact | Auto Loader re-infers or re-applies the schema on the next microbatch; the STREAM checkpoint (a separate resource) still prevents re-reading already-processed files. If the stream checkpoint is ALSO gone, this is the ordinary checkpoint-reset scenario above. |

### Deliberately not built

- **File archiving, moving or deletion.** Moving or deleting source files after ingest is a
  data-loss-shaped operation that belongs to whoever owns the landing zone, not to this
  framework. **Open item for the incoming team:** if a landing zone accumulates files
  without bound, that is an operational concern for its owner to solve (a lifecycle policy
  on the storage account is the usual answer), not something this job does on their behalf.
- **A processed-files ledger.** Auto Loader's own checkpoint already is one.
- **A second checkpoint-reset guard.** See above.
- **A format abstraction layer.** `file_format` is a config value passed to Auto Loader
  directly; there is no strategy-pattern class per format.
- **Schema inference caching, or a schema registry for files.** `cloudFiles.schemaLocation`
  already is the former; there is no equivalent of Kafka's Schema Registry for file drops
  in this framework's scope.
- **SAS-token and managed-identity storage auth** (`docs/build_log/DECISIONS.md` D-12).
  Only `account_key` and `service_principal` are implemented, stated here as a limitation,
  not an oversight: each of the excluded modes needs either a token-provider class this
  project cannot verify exists on the target runtime, or workspace-level Unity Catalog
  wiring outside this repository's control, so adding one is a future code change with its
  own verification, not a config guess — the same restraint `sources/oracle/config.py`
  applies to JDBC auth (`auth_mode: basic` only). See `sources/file/security.py`'s module
  docstring for the mechanism these two modes do use.
- **A file replay job or entrypoint** (`docs/build_log/DECISIONS.md` D-10, settled). A
  fresh, missing checkpoint already makes Auto Loader re-read the whole path on its own
  (`cloudFiles.includeExistingFiles` defaults to `true`), files persist in ADLS so there is
  no retention window to race the way a Kafka replay races broker retention, and this
  source is landing-only, so there is no re-parse-from-landing shape either. Recovery
  reuses the existing checkpoint-reset procedure deliberately — `docs/CONFIGURATION.md` §11
  and `docs/RUNBOOK_SUPPORT.md` §9 carry the three-step version.

### Unity Catalog Volume source paths, and the simplification they set up (D-13)

`source_path` may name a Unity Catalog Volume directly
(`/Volumes/<catalog>/<schema>/<volume>/...`) instead of a path within an ADLS container
named by `storage_ref`. A Volume path is governed by Unity Catalog grants on the Volume
itself: `sources/file/config.py` applies no `storage_ref` and builds no
`fs.azure.*` session options for it, and `sources/file/run.py` applies no session
configuration around the read at all in that case. The two forms are mutually exclusive —
setting `storage_ref` alongside a Volume path is a config error naming both, because there
is no honest answer to which one governs the read.

This is **preferred**, not forced: the shipped worked example keeps its existing
`storage_ref` form, since switching it was not asked for and doing so without confirming
Volumes are reachable from the target compute for that workload would be exactly the kind
of unverified assumption this project exists to keep out of shipped configuration (VB-28).

**The planned simplification, if VB-28 comes back "Volumes everywhere":** `conf/storage.yaml`,
`sources/file/security.py`, and `framework/security.py`'s `apply_session_options` (added for
exactly this source's session-scoped credentials, VB-26) all become deletable —
a Volume path takes no credential from this framework at all. Not attempted now: narrowing to
one credential path is a decision for whoever answers VB-28, not something to guess at while
both are still plausibly needed in different environments. If that day comes,
`apply_session_options` is worth a second look before deleting it outright — nothing else in
the framework uses it today, but a future source needing session-scoped, non-`.option()`
credentials (the same shape ADLS Gen2 has) would want it again.

---

## 12. Adding a source type

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

**Nothing under `framework/` changes.** Every one of the seven items above lives in the new
package, a new conf file, or a new resource file — `framework/config.py`, `control.py`,
`state.py`, `audit.py`, `tables.py`, `writers.py`, `runner.py` (beyond the one `_SOURCES`
line), `security.py`, `checkpoint.py` and `logs.py` are all untouched. The CORE section 7
grep gate is what enforces this, mechanically rather than by review (§9 above).

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
