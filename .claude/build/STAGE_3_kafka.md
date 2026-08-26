# STAGE 3 -- Kafka source

**Paste `00_CORE.md` AND `detailed_kafka_ingestion_designer_prompt.md` before this file.**
Stage 2 must be green.

The original designer prompt carries the Kafka design intent (its §2 layers and CloudEvents,
§6 schema resolution) and the API traps (§9) that CORE does not repeat. You need both.

Nothing is deployed, so **redesign the Kafka code freely.** Simpler is better. Behaviour
requirements come from the designer prompt; six specific fixes come from this file.

---

## Work

### 1. Move Kafka onto the spine

`sources/kafka/spec.py` -- `SOURCE_SPEC` with `layers = ("landing", "curated", "quarantine")`,
the structural and operational key sets, and the mutually exclusive replay controls. **No
PySpark import in this file.**

`sources/kafka/run.py` -- `run(ctx) -> RunResult`. Owns the whole streaming execution: the
`readStream`, the `foreachBatch` body, the per-layer audit calls, the guard, the reader-schema
resolution. Use `ctx.writers`, `ctx.audit`, `ctx.tables`, `ctx.log`.

Split parse and write helpers into sibling modules if `run.py` grows past a screen or two --
but only into functions called from exactly one place.

### 2. Six fixes to fold in

#### (a) `checkpoint_reset_id` must be single-use -- highest severity

The reset override forks the Delta transaction identity, which is what makes bypassing the
guard safe. That safety depends entirely on the id being **new each time**:

1. Checkpoint lost. Support sets `checkpoint_reset_id = 'INC-1042'`. Identity forks, batches
   0..N commit versions 0..N. Correct.
2. Checkpoint exists again; guard short-circuits; the stale field is inert. Correct.
3. Months later the checkpoint is lost again. Support sees `INC-1042` already there and leaves
   it. Guard bypassed. Batch ids restart at 0. The app id is unchanged and Delta holds version
   N against it.
4. **Every write is skipped as a duplicate. The job reports success and ingests nothing.**

Three parts:

- **Record the reset id on the audit row.** Reuse `rerun_id` if `run_type` disambiguates,
  otherwise add a column and update DDL + `StructType` + writer together. State which you
  chose.
- **One new refusal branch**, after the existing early returns: *checkpoint absent AND
  `checkpoint_reset_id` set AND that id already in audit history for this source -> raise.*
  The error names the id, explains that reuse makes Delta skip every write, and says to use an
  unused incident id.
- **Write the resume procedure** into `docs/RUNBOOK_SUPPORT.md`: run the "last committed end
  offset per partition" support query and record the offsets -> set a **fresh** reset id ->
  let the primary run resume from `latest` -> backfill the gap with a bounded Kafka replay ->
  clear nothing, the field self-neutralises once the checkpoint exists. Add that query to
  `sql/03_support_queries.sql`.

**The reset feature is intentional and load-bearing. Do not remove, narrow or replace it.**

#### (b) Reader options -- all four must be present

| Option | Requirement |
|---|---|
| `includeHeaders` | `"true"`. Every `ce_*` column depends on it; absence yields NULLs, not an error. Not configurable. |
| `maxOffsetsPerTrigger` | Set to a value, never unbounded. Operationally overridable via `batch_limit`. |
| `minPartitions` | Without it, parallelism is capped at the topic's partition count regardless of cluster size. |
| `failOnDataLoss` | Default `"true"`. |

`minPartitions` is a **plain integer in `conf/defaults/kafka.yaml`, not a computed
multiplier** -- deriving it needs a Kafka admin client, and dependencies stay `PyYAML` +
`requests`. Comment it: *"roughly 4x the topic's partition count; only ever splits offset
ranges, never merges them, so over-setting is cheap."* Add a line to the Kafka onboarding
template telling the onboarder to confirm the partition count with the producing team.

#### (c) Curated MERGE needs a partition predicate

Curated is partitioned by `event_date`; the replay merge key is
`(kafka_partition, kafka_offset)`. Without a bound, every replay rewrites the whole history.

```python
bounds = batch_df.agg(F.min("event_date"), F.max("event_date")).collect()[0]
cond = (f"t.event_date BETWEEN '{bounds[0]}' AND '{bounds[1]}' "
        "AND t.kafka_partition = s.kafka_partition "
        "AND t.kafka_offset = s.kafka_offset")
```

The batch is cached so the aggregation is cheap. Skip the merge on an empty batch rather than
formatting `None` into the string. `framework/writers.merge()` already requires a predicate
(Stage 2), so this is supplying it, not adding the rule.

#### (d) Malformed payload triage

Three inputs currently produce a garbage schema id and a misleading quarantine reason. One
`when()` chain before `from_avro`:

```python
F.when(F.col("value").isNull(), F.lit("NULL_VALUE_TOMBSTONE"))
 .when(F.length("value") < 5, F.lit("TRUNCATED_PAYLOAD"))
 .when(F.substring("value", 1, 1) != F.lit(bytearray([0])), F.lit("BAD_MAGIC_BYTE"))
 .otherwise(F.lit(None))
```

Rows with a reason go to quarantine; the good path is unchanged. The byte comparison behaves
differently than you expect on BINARY (designer prompt §9.2) -- **add a VB entry** rather than
asserting it works.

#### (e) Job hardening

`max_concurrent_runs: 1` and `queue.enabled: false` on every job template. Two drivers on one
checkpoint corrupts `offsets/`, which leads straight to the reset procedure (a) hardens. Set
`max_retries: 3`, `min_retry_interval_millis: 300000`. Keep `retry_on_timeout` stated per task,
never folded into a YAML anchor.

#### (f) Smaller items

- `unpersist()` in a `finally`, not only the success path -- the poison-batch path is where a
  leak accumulates.
- Record `latestOffset` alongside `endOffset` so a permanently-lagging source is visible
  (VB-05). Make the column NULL-tolerant. Generalise it to "pending work at end of run": Kafka
  lag, file count not yet processed; Oracle leaves it NULL rather than inventing one.
- Replace `time.sleep(2)` in the listener drain with reading `query.recentProgress` after
  `awaitTermination()`.
- **Cadence vs retention** -- `docs/CONFIGURATION.md`, MUST-READ row: *the schedule must be at
  most one third of the source's retention or purge window; confirm it with the producing team
  at onboarding.*
- Two support queries in `sql/03_support_queries.sql`: sources running with `failOnDataLoss`
  disabled, and sources whose last run quarantined or rescued more than 5% of input.

---

## Do not build

- A source-format abstraction. Confluent-framed Avro is the only format.
- A reset-history table, auto-generated reset ids, or auto-clearing of the field.
- A Kafka admin client.
- A third `reader_schema_mode`.
- Anything that flattens the nested `payload` struct or explodes rows.

---

## Files

**Create:** `sources/kafka/spec.py`, `sources/kafka/run.py`, parse/write helpers as needed
**Edit:** `conf/defaults/kafka.yaml`, `resources/job_ingest_*.yml`,
`sql/03_support_queries.sql`, `docs/RUNBOOK_SUPPORT.md`, `docs/CONFIGURATION.md`, existing
Kafka tests

---

## Exit gate

- `pytest -m "not spark" -q` green, test count up.
- **Reset-id reuse raises.** Test it: id already in audit history, checkpoint absent -> raise.
  If it proceeds, (a) is not done whatever the report says.
- A test asserts all four reader options are present with their expected values.
- A test asserts the curated MERGE condition contains an `event_date` bound, and that the
  append path does not.
- Three malformed-payload tests, one per reason, each asserting raw bytes are retained.
- Every pre-existing quarantine test passes unmodified.
- The CORE section 7 grep returns nothing.

Then write the stage report (CORE section 9) and **stop**.
