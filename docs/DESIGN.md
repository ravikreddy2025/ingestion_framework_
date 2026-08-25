# Design

For the team taking this over. Read this before changing code.

**Scope: Kafka → landing → curated.** Nothing downstream of curated is in this package.

---

## 1. What the framework does

```
                       ┌──────────────────────────── ONE streaming query ───────────────────────────┐
                       │                                                                            │
  Confluent Kafka ──readStream──▶ foreachBatch(batch_df, batch_id)                                  │
                       │              │                                                             │
                       │              ├─▶ audit  landing STARTED                                    │
                       │              ├─▶ project + write  LANDING   (raw bytes, + CloudEvent cols) │
                       │              ├─▶ audit  landing COMPLETED   record_count=N                 │
                       │              ├─▶ audit  curated STARTED                                    │
                       │              ├─▶ parse the SAME cached batch_df                            │
                       │              ├─▶ write CURATED  (+ quarantine)                             │
                       │              └─▶ audit  curated COMPLETED   record_count=M                 │
                       │                                                                            │
                       └─▶ StreamingQueryListener ─▶ audit  stream COMPLETED  + Kafka offsets ──────┘
```

One read from Kafka. Landing and curated are written from the **same in-memory microbatch**
inside one `foreachBatch`. Curated never re-reads Kafka and never re-reads the landing table.

Trigger is `availableNow`: drain what is on the topic, then stop. Scheduled once daily.

### The three tables

| Layer | Tables | Partitioned by | Contents |
|---|---|---|---|
| **landing** | **ONE per topic** | `(ingest_date)` | Kafka value bytes **verbatim**, incl. the 5-byte Confluent header, + Kafka columns + CloudEvent columns |
| **curated** | **ONE PER TOPIC** | `(event_date)` | Same Kafka + CloudEvent columns, plus the parsed payload as **one nested `payload` STRUCT** |
| **audit** | ONE for the whole estate | `(audit_date)` | One row per (batch, layer, status) transition |
| quarantine | One per topic (optional) | `(ingest_date)` | Records that could not be parsed, raw bytes retained |

Landing and curated deliberately share their Kafka and CloudEvent column names, so any
curated row joins back to its landing row on `(topic, kafka_partition, kafka_offset)` —
which is also the MERGE key that makes replay idempotent.

---

## 2. File lineage

Read the modules in this order. It is the order data flows through them.

| # | File | Owns | Depends on |
|---|---|---|---|
| 1 | `config.py` | Two-tier config → one `TopicConfig`. Validation. Checkpoint-path derivation. | *(no PySpark — testable in plain CI)* |
| 2 | `security.py` | Key Vault secrets + UC Volume certs → Kafka/registry connection options. Redaction. | `config` |
| 3 | `kafka_source.py` | `readStream` / bounded batch read. Primary vs replay positioning. Trigger. | `config`, `security` |
| 4 | `schema_resolver.py` | Confluent wire-format column expressions. Schema Registry REST client + cache. | `config`, `security` |
| 5 | `landing_writer.py` | Landing projection (incl. CloudEvent extraction) + write. | `config`, `schema_resolver` |
| 6 | `curated_writer.py` | Per-writer-schema Avro decode → curated + quarantine, and their writes. | `config`, `schema_resolver`, `landing_writer` |
| 7 | `audit.py` | Audit row construction, `AuditWriter`, `StreamAuditListener`. | `config` |
| 8 | `tables.py` | DDL + partitioning for landing / quarantine / audit. | `config` |
| 9 | `pipeline.py` | The chained `foreachBatch` body, the four run shapes, the startup guard. | **all of the above** |
| 10 | `entrypoints/*.py` | argparse → resolve config → `pipeline.run()`. Nothing else. | `config`, `security`, `pipeline` |

### Dependency graph

```
                    config.py  ◀── (everything depends on this; it depends on nothing)
                    ╱    │    ╲
           security.py   │     tables.py
              ╱   ╲      │        │
  kafka_source   schema_resolver  │
        │            ╱      ╲     │
        │   landing_writer   │    │        audit.py
        │        │      ╲    │    │          │
        │        │    curated_writer         │
        │        │           │               │
        └────────┴──── pipeline.py ──────────┘
                            │
                      entrypoints/
```

No cycles. `config.py` is a leaf on purpose: it imports no PySpark, which is what lets the
config and validation tests run in CI without a Spark install.

### Supporting files

| Path | Purpose |
|---|---|
| `conf/clusters.yaml` | Kafka cluster profiles (bootstrap, auth mode, secret **names**) |
| `conf/registries.yaml` | Schema Registry profiles — auth is independent of Kafka auth |
| `conf/topics/<key>.yaml` | One per topic. **The filename is the `topic_key`.** |
| `conf/topics/_TEMPLATE.yaml` | Copy-to-onboard. Inert; skipped by the validator. |
| `sql/01_operational_config.sql` | Control table + grants (support-editable tier) |
| `sql/02_layer_tables.sql` | Landing / audit / quarantine DDL + grants. A `{catalog}`/`{topic_table}` **template** now landing is per-topic - the code creates the real tables itself; this is for pre-provisioning |
| `sql/03_support_queries.sql` | Triage queries and the no-deploy fixes |
| `sql/04_maintenance.sql` | `OPTIMIZE`/`VACUUM`, run weekly by `resources/job_maintenance.yml`. The retention `DELETE` is written but commented out - see §9 item 4 |
| `resources/job_*.yml` | Databricks Workflows definitions (DAB) |
| `notebooks/00–03` | Escalating sequence: config → tests → connectivity → real run |

---

## 3. Configuration: five layers, two tiers

```
1. conf/defaults.yaml            common to every topic in every environment
2. conf/environments/<env>.yaml  vars (catalog), per-env defaults, cluster/registry endpoints
3. conf/topics/<key>.yaml        unique to one topic
     3a. topic:.environments.<env>  RARE - unique to one topic IN ONE environment, nested
                                    in the same file. Working example:
                                    conf/topics/vector_patient_events.yaml
      ^-- STRUCTURAL: Git, PR-reviewed, next deploy
4. operational control table     support-team overrides
5. Workflows job parameters      one-off overrides
      ^-- OPERATIONAL: next run, no deploy
```

Later wins per key; absent keys fall through. `{catalog}`, `{topic_key}`, `{topic_table}` and `{domain}`
placeholders in layers 1–3 (3a included) resolve after the merge — an unresolved one is a
hard error.

3a stays inside layer 3 in every other respect: still Git, still PR-reviewed, still deployed
by DAB. It exists only for a value specific to both one topic and one environment — a
platform-wide environment difference belongs in layer 2, and a runtime toggle belongs in
layer 4. Full precedence and rationale: `docs/CONFIGURATION.md` §4.

| | **Structural** (1–3) | **Operational** (4–5) |
|---|---|---|
| Holds | Topic, cluster, registry, tables, **partitioning**, checkpoint root, dedup keys | enable/disable, trigger, batch size, failure mode, reader schema, **replay controls** |
| Changed by | Platform + domain engineers | **Support team** |
| Process | PR → deploy | `UPDATE` |
| Effect | Next deploy | **Next run — no deploy** |

The environment comes from `--environment ${bundle.target}`, so a DAB target name must
match an `environments/<name>.yaml` file. The data catalog is **not** a bundle variable —
DAB substitution does not reach files under `sync.include`, so `conf/` is copied verbatim
and the catalog lives in `vars.catalog` where the code can read it.

A **topic file that hardcodes a catalog works in prod and silently breaks dev**, which is
why `tests/test_shipped_config.py` resolves the full (topic × environment) cross product.

A **missing control row is not an error** (it means "no overrides"), so a topic runs the
moment its YAML merges. **Duplicate rows are** an error.

Note what is *not* operational: table names, partition columns and dedup keys are
structural. Changing a table's physical layout should require a PR, so those fields are
deliberately absent from the override set. `tests/test_config.py` asserts this.

---

## 4. Re-runs and duplicates

**The most important section in this document.**

### Two mechanics everything follows from

1. **Checkpoint protocol.** Spark writes the batch's offset range to `<checkpoint>/offsets/N`
   *before* running `foreachBatch`, and `<checkpoint>/commits/N` *after* it returns
   cleanly. On restart, if `offsets/N` exists but `commits/N` does not, Spark
   **re-executes batch N over the identical offset range**. A retry never reads a
   different set of records.
2. **Delta idempotent writes.** `txnAppId` + `txnVersion=batchId` are recorded **per
   table**. A repeat write with an already-seen version is skipped. That is why one shared
   `txn_app_id` is correct: landing skips itself while curated still proceeds.

`txn_app_id` is derived from `topic_key` + run type + replay lineage (`pipeline._make_txn_app_id`).
It **must stay stable across restarts** — never make it random per run.

### Failure scenarios

| # | Failure | Duplicates? | Fix — no code change |
|---|---|---|---|
| 1 | Curated fails, transient | No | Re-run |
| 2 | Curated fails persistently (unregistered schema) | No | Register schema, **or** `on_deser_error → quarantine` |
| 3 | Driver dies between landing and curated | No | Re-run |
| 4 | Landing write itself fails | No | Fix cause, re-run |
| 5 | Job timeout mid-run | No | Re-run; lower `max_offsets_per_trigger` |
| 6 | **Primary checkpoint deleted** | **No — silent data loss instead** | Kafka replay job |
| 7 | Batch succeeded, curated data wrong | No | Curated replay job |
| 8 | Records genuinely missed | No | Kafka replay job |

#### Scenario 1–3 — self-healing

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
2. `UPDATE ... SET on_deser_error = 'quarantine'` (see `sql/03_support_queries.sql` Q5).
   Bad records go to quarantine with raw bytes, the batch completes, the stream drains.
   Recover them later with the **curated replay** job, then set it back to `fail`.

#### Scenario 6 — the only failure that looks like success

Deleting the primary checkpoint restarts `batchId` at 0. Delta has already recorded high
versions for this `txnAppId`, so appends are **skipped as duplicates**. The job reports
success and writes nothing.

`pipeline.guard_against_checkpoint_reset()` refuses to run when the primary checkpoint is
missing **and** the landing table already holds rows for this topic. A genuine first run is
unaffected: its own landing table has no rows yet — which is also why a migration that
pre-loads **curated** from a legacy system before the stream ever starts is unaffected too;
the guard never inspects curated.

**Never delete a primary checkpoint. Use the Kafka replay job** to recover the missed *data*
— it derives a different checkpoint *and* a different `txnAppId` from `rerun_id`, so neither
collides. Replay does **not**, by itself, unblock the *primary* job: the guard keeps refusing
every trigger until support sets `checkpoint_reset_id` in the control table, which forks
`_make_txn_app_id`'s lineage the same way `rerun_id` does, so the restarted primary also gets
an identity with no committed history to collide with. See
[`RUNBOOK_SUPPORT.md` §5.4a](RUNBOOK_SUPPORT.md#5-4a-restarting-the-primary-after-a-genuine-checkpoint-loss).

### ⚠️ Verify this on your DBR before go-live

`txnAppId` / `txnVersion` behaviour is taken from Delta's documented contract, **not
measured** — there was no Delta available in the environment this was written in. In
particular, confirm whether the skip triggers on `txnVersion ≤ last recorded` or only on
`==`; scenario 6 depends on it. Test by killing a job deliberately between the landing and
curated writes, then re-running and checking `sql/03_support_queries.sql` **Q9**
(duplicate check) returns zero rows.

---

## 5. Design decisions

### Payload stays nested

Curated stores the parsed record as **one `payload` STRUCT**, not flattened columns.

- Every curated table has the same recognisable outer shape, so an operator moving between
  topics does not relearn the columns.
- No collision is possible between business fields and framework columns — which is why
  the framework columns are plainly named (`topic`, `ingest_ts`) instead of underscore-prefixed.
- Nested structures survive as structs/arrays/maps: query `payload.patient.id`, read with
  `SELECT to_json(payload)`.
- Business fields in config are therefore written as `payload.<field>` — e.g.
  `curated_dedup_keys: [payload.claim_id]`.

**Arrays are not exploded.** Curated is 1:1 with Kafka records. Exploding would break the
`(topic, kafka_partition, kafka_offset)` merge key that makes replay idempotent. Fan-out is
a modelling decision that belongs downstream.

### Reader schema is mandatory; `writer` mode does not exist

`from_avro(payload, jsonFormatSchema, options)` maps onto Avro's two-schema resolution:
positional = **writer** (how the bytes were encoded), `avroSchema` option = **reader**
(the shape you want out). The reader schema fixes the `payload` struct's type.

Because curated stores payload as one struct column, two writer versions decoded *without*
a common reader schema would produce two incompatible struct types that cannot share a
table. So there is no "use each writer schema as-is" mode — only `registry_latest` and
`pinned_id`. Avro resolution fills reader-only fields from defaults and drops writer-only
fields, so a mixed-version batch lands cleanly.

That positional/option mapping is easy to get backwards and has moved between Spark
versions, so `curated_writer.assert_from_avro_semantics()` **proves it at runtime**, once
per run, with a 5-byte synthetic record. A DBR upgrade that changed the behaviour fails the
job at startup instead of silently mis-decoding production data.

### Partitioning, not Liquid Clustering

Delta allows `PARTITIONED BY` or `CLUSTER BY`, never both.

- **landing** `(ingest_date)` — one table per topic, so `topic` is constant inside it and a
  low-cardinality column essentially every read filters on, and it gives per-topic file
  isolation for retention and replay. `ingest_date` stops one topic's partition growing
  without bound.
- **curated** `(event_date)` — already one table per topic, so `topic` is constant.
  `event_date` = date of `ce_time`, falling back to `kafka_timestamp`. Late-arriving events
  write into older partitions; that is expected.

Liquid Clustering would be the better default for a *single-topic* landing table. It is not
available here because partitioning was the requirement, and the two are exclusive.

### MERGE vs append

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

### CloudEvents

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

### Hand-rolled `foreachBatch`, not Lakeflow Declarative Pipelines

LDP is the better default for most medallion work, and is the right tool downstream of
curated. It does not fit *this* layer, for three concrete reasons:

1. **Per-record writer-schema resolution needs a driver-side registry call inside the
   batch.** The query plan is a function of the schema ids *in that batch*, so it cannot be
   declared up front.
2. **Replay checkpoint isolation is not expressible.** LDP owns its checkpoints; the replay
   pattern here depends on *choosing* the checkpoint path per run so a `startingOffsets`
   override is honoured at all.
3. **Bounded replay and the asymmetric MERGE are per-run imperative decisions.**

If LDP gains user-controlled per-run checkpoint/offset overrides, revisit this.

---

## 6. Deliberately not built

Flagged as future extension points, not implemented:

- **Pluggable serialization formats.** Avro-via-Schema-Registry is the only format in
  scope; `schema_resolver` refuses non-`AVRO` `schemaType` with a clear error.
- **A Kafka client factory.** There is one client.
- **Executor-local cert staging** (`SparkContext.addFile`). Documented in `security.py` as
  the fallback if a compute profile cannot read UC Volumes from executors.
- **Per-partition replay timestamps.** `startingTimestamp` covers what support asks for.
- **`ce_extensions` map**, **payload flattening**, **array explosion** — see above.

---

## 7. Where to make common changes

| I want to… | Edit | Deploy needed? |
|---|---|---|
| Add a topic | `conf/topics/<key>.yaml` + a task in `resources/job_ingest_primary.yml` | Yes (PR) |
| Add a Kafka cluster or registry | `conf/clusters.yaml` / `conf/registries.yaml` | Yes (PR) |
| Change a partition column | `conf/topics/<key>.yaml` **and** back-fill/rewrite the table | Yes (PR) |
| Stop a topic now | `enabled = false` in the control table | **No** |
| Unblock a stuck stream | `on_deser_error = 'quarantine'` | **No** |
| Replay from an offset/time | Kafka replay job parameters | **No** |
| Re-parse bad curated data | Curated replay job parameters | **No** |
| Add a CloudEvent attribute | `CE_ATTRIBUTES` in `landing_writer.py` + the DDL blocks in `tables.py` | Yes |
| Change the audit row shape | `AUDIT_SCHEMA` in `audit.py` + `AUDIT_DDL_COLUMNS` in `tables.py` | Yes |

The last two are the only ones needing changes in two places, and a test asserts the two
stay in step (`tests/test_audit_and_tables.py`).

---

## 8. Testing

```bash
pytest -m "not spark"   # config, security, registry, shipped-config, writers, dispatch.
                         # No JVM needed. This is the azure-pipelines.yml CI gate.
pytest                  # everything, including the Spark-backed tests
```

The tests worth knowing about:

| Test | Proves |
|---|---|
| `test_curated_writer.py::test_one_microbatch_with_two_writer_schema_versions_parses_correctly` | The central claim — mixed schema versions in one batch |
| `test_curated_writer.py::test_payload_stays_nested_and_is_not_exploded` | The shape decision, so a change is deliberate |
| `test_curated_writer.py::test_event_date_prefers_ce_time_and_falls_back_to_kafka_timestamp` | The partition key never NULLs or throws |
| `test_config.py::test_primary_and_replay_checkpoints_never_collide` | Replay isolation |
| `test_writers.py::test_primary_landing_append_carries_the_idempotency_markers` | **The txnAppId/txnVersion mechanics this section rests on** - not simulated, asserted on the exact writer options |
| `test_writers.py::test_curated_replay_allows_the_schema_to_widen` | Schema evolution on the replay MERGE path (§5, §9 item 6) |
| `test_pipeline.py::test_guard_refuses_when_the_checkpoint_vanished_but_data_exists` | Scenario 6 - the failure that looks like success |
| `test_shipped_config.py` | Every shipped YAML resolves in every environment - runs on every PR via `azure-pipelines.yml` |
| `test_audit_and_tables.py` | Audit row ↔ schema ↔ DDL alignment, and `sql/02` ↔ `tables.py` column drift |

Run them on Databricks via `notebooks/01_run_unit_tests` — DBR has Spark and `spark-avro`
built in, so the whole suite runs with no setup. No test connects to Kafka, reads a secret,
or writes to a table.

---

## 9. Open items for the incoming team

1. **Verify `txnAppId` behaviour on your DBR** (§4). This is the one unverified assumption
   the correctness of re-runs rests on.
2. **Confirm UC Volume readability from executors** before onboarding any mTLS topic —
   `notebooks/02_check_connectivity` has the check.
3. **Confirm serverless network reachability** to every broker and registry, or move
   on-prem topics to classic compute (`resources/job_ingest_primary.yml` header).
4. **Landing retention is set to 20 years** (`landing_retention_days` in `databricks.yml`,
   overridable per target). The `DELETE` that enforces it is written and parameterised in
   `sql/04_maintenance.sql` but is deliberately **commented out** — switching on automatic
   deletion of raw payloads is the data owner's call, not a default. Two questions remain
   open: does the same window apply to the **quarantine** tables (they hold full payloads
   too), and to **curated** (derived, so arguably shorter)?
   `OPTIMIZE`/`VACUUM` now run weekly via `resources/job_maintenance.yml`; that job needs
   `sql_warehouse_id` set before it will deploy.
5. **`record_count` caveat** (§4, and `audit.py`): it counts rows *presented*, not rows
   Delta inserted.
6. **Curated replay schema evolution is runtime-dependent.** `curated_writer._merge_curated`
   prefers Delta's `withSchemaEvolution()` (DBR 15.4 LTS+, and serverless environment
   version 2+) and falls back to scoping the legacy
   `spark.databricks.delta.schema.autoMerge.enabled` flag around the single merge. Confirm
   which branch your runtime takes the first time you run a curated replay after an additive
   schema change — that is the scenario the fallback exists for.
7. **The checkpoint guard's filesystem probe** (`pipeline._checkpoint_offsets_exist`) uses
   `os.stat` so that "cannot reach the Volume" raises rather than being mistaken for
   "checkpoint deleted". Confirm the driver on your compute profile can stat the checkpoint
   Volume — on serverless this is the branch most likely to surprise you.
8. **`azure-pipelines.yml`'s deploy stage is not wired up.** It calls `databricks bundle
   deploy`, but the service connection (or variable group) and the per-target service
   principal credentials it needs are commented placeholders, not real values — platform
   configuration only a human can supply. The test stage runs today without any of this.
9. **A pre-loaded curated table (Cloudera migration) must match `curated_schema()`'s derived
   schema exactly** — same columns, same order, all-nullable. `ensure_curated_table` is
   `CREATE TABLE IF NOT EXISTS`, so if the migration creates the table first with a
   different shape, that call silently no-ops and the mismatch only surfaces as a write
   failure on the first real batch, not at migration time. Landing itself is unaffected by
   pre-loaded historical data either way — `guard_against_checkpoint_reset` never inspects
   curated, only landing (§4, Scenario 6).


---

## 10. Oracle — the watermark, and what can go wrong

Added in Stage 4. Sections 1–9 describe the Kafka source; this one is self-contained.

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

Added in Stage 5. Sections 1–10 describe Kafka and Oracle; this one is self-contained.
(Sections 6–9 predate the framework/sources split and still refer to modules — `pipeline.py`,
`curated_writer.py`, `conf/topics/`, `azure-pipelines.yml` — that no longer exist under
those names; not corrected here, as it is outside this stage's scope, but worth flagging so
nobody trusts them as current.)

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

### The checkpoint-reset guard is reused, not redesigned

The STAGE_5 brief is explicit: "the file source is checkpoint-based... wire that in; do not
write a second guard." `sources/file/run.py`'s `_guard_against_checkpoint_reset` mirrors
`sources/kafka/run.py`'s function of the same name field-for-field — same three states (checkpoint
intact / gone-with-a-fresh-reset-id / gone-with-no-reset-id-and-data-already-landed), same
refusal message shape, same single-use reset-id check against the audit table.

**It is a deliberate, small duplication, not a shared framework function.** Two sources
(Kafka and this one) need it; CORE section 2 rule 4 sets the bar for a new abstraction at
**three** implementations, and Oracle has no checkpoint at all — its correctness rests on
`ingest_state`'s watermark instead. Promoting the guard to `framework/` today would be the
premature abstraction rule 4 exists to prevent. If a third checkpoint-based source ever
arrives, this is the first place to look.

**What differs from Kafka's, and why:** Kafka's "already landed" check filters landing by
`topic`, because one Kafka cluster's checkpoint namespace is shared across topics. A file
source's landing table belongs to exactly one file source — there is no equivalent
namespace to disambiguate — so the check here is simply "does the landing table hold any
row at all."

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
documented in `docs/CONFIGURATION.md`, not something specific to a reset. If Auto Loader's
actual behaviour on encountering a stale schema location after a fresh-identity restart
turns out to matter in practice, add a VB entry then — nothing here is asserted with
confidence beyond "the guard's job is the Delta identity fork, and it does that regardless
of `schemaLocation`."

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
- **SAS-token and managed-identity storage auth.** Only `account_key` and
  `service_principal` are implemented — see `sources/file/security.py`.
