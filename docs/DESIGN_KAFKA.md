# Design — Kafka

This source's own re-run mechanics, failure-scenario table and design decisions. Read
[DESIGN.md](DESIGN.md) first for the shared architecture, the source-contract rationale and
the configuration model — this file assumes you already have those.

---

## Re-runs and duplicates

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

## Design decisions

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
