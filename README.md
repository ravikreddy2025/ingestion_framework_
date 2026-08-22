# Kafka → Databricks Landing/Curated Ingestion Framework

Config-driven, multi-topic, multi-cluster ingestion from Confluent Kafka into Unity Catalog
Delta tables. One job definition serves every topic across Vector, RCM, GMA, Anti-Fraud and
DWH-PES.

**Scope: Kafka → landing → curated.** Nothing downstream of curated is in this package.

---

## Read in this order

> ### 🧭 New to this codebase? Start with [docs/NAVIGATION.md](docs/NAVIGATION.md)
> Every file mapped: what each one is for, a 10-minute path to understanding the
> architecture, and a trace of one Kafka record through all 10 modules in execution order.

### Start with the runbook for your role

| You are | Read |
|---|---|
| **A developer** maintaining or extending this | [docs/RUNBOOK_DEVELOPER.md](docs/RUNBOOK_DEVELOPER.md) → then [docs/DESIGN.md](docs/DESIGN.md) |
| **Production support** | [docs/RUNBOOK_SUPPORT.md](docs/RUNBOOK_SUPPORT.md) — SQL and job parameters only, no code reading |
| **Client IT / architecture** | [docs/RUNBOOK_CLIENT_IT.md](docs/RUNBOOK_CLIENT_IT.md) — design, security, prerequisites, data protection |

### Reference documents

| Document | Contents |
|---|---|
| [docs/NAVIGATION.md](docs/NAVIGATION.md) | **Map of every file.** Where to start, what to ignore, "I want to… → go to…" |
| **[docs/DESIGN.md](docs/DESIGN.md)** | **File lineage, dependency graph, failure scenarios, design decisions.** Read before changing code. |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Every setting, tiered **must / nice / no change** |
| [docs/IMPORT_TO_DATABRICKS.md](docs/IMPORT_TO_DATABRICKS.md) | Get it into a workspace and run the tests |

Then run `notebooks/00_validate_config` — it resolves config and prints it, touching nothing.

---

## What it does

```
Kafka ──readStream──▶ foreachBatch(batch_df, batch_id)
                          ├─ audit  landing STARTED
                          ├─ LANDING    raw bytes verbatim + Kafka + CloudEvent columns
                          ├─ audit  landing COMPLETED
                          ├─ audit  curated STARTED
                          ├─ parse the SAME cached batch
                          ├─ CURATED    payload as one NESTED struct
                          └─ audit  curated COMPLETED
```

One read from Kafka. Both layers written from the **same in-memory microbatch**, in one
Structured Streaming query, on `Trigger.AvailableNow`, scheduled once daily.

| Layer | Tables | Partitioned by |
|---|---|---|
| **landing** | **ONE per topic** | `(ingest_date)` |
| **curated** | **ONE per topic** | `(event_date)` |
| **audit** | ONE shared table | `(audit_date)` |

| Property | How |
|---|---|
| No duplicates on re-run | Delta idempotent writes (`txnAppId`/`txnVersion`); a retry replays the **same** batch id over the **same** offsets — [DESIGN §4](docs/DESIGN.md) |
| Schema-drift safe | Each record decoded with **its own** writer schema from the wire header — not one "latest" schema pinned at query start |
| Replayable past Kafka retention | Landing holds the original bytes; a curated replay never touches the broker |
| Heterogeneous sources | Cluster and registry chosen **per topic**; SASL/PLAIN, SCRAM-256/512 and mTLS side by side |
| No plaintext secrets | Every credential via `dbutils.secrets.get()`; scope/key **names** come from config |
| UC-first | Certs and checkpoints on UC Volumes; DBFS paths **rejected** by validation |

---

## Layout

```
conf/                        STRUCTURAL config — Git, PR-driven, tier-marked
  clusters.yaml              Kafka cluster profiles
  registries.yaml            Schema Registry profiles
  topics/_TEMPLATE.yaml      Copy this to onboard a topic
  topics/<topic_key>.yaml    One per topic — the FILENAME is the topic_key

sql/                         TEMPLATES — {catalog} placeholders, rendered per environment
  01_operational_config.sql  Control table + grants (support-editable tier)
  02_layer_tables.sql        Landing / audit / quarantine DDL + grants
  03_support_queries.sql     Triage queries and the no-deploy fixes
  04_maintenance.sql         OPTIMIZE / VACUUM, and the retention policy

azure-pipelines.yml          CI (lint + fast suite) and the bundle deploy stage

src/kafka_ingest/            (dependency order — see DESIGN.md §2)
  config.py                  Two-tier config -> TopicConfig. No PySpark import.
  security.py                Key Vault secrets + Volume certs -> connection options
  kafka_source.py            readStream / batch read, primary vs replay positioning
  schema_resolver.py         Wire-format parsing + Schema Registry client
  landing_writer.py          Raw bytes + CloudEvent columns -> landing
  curated_writer.py          Per-writer-schema decode -> curated (+ quarantine)
  audit.py                   Per-batch, per-layer status rows
  tables.py                  DDL + partitioning
  pipeline.py                Chained foreachBatch body, run shapes, startup guard
  entrypoints/               ingest_primary, replay_kafka, replay_curated

notebooks/                   Run in order on a cluster
  00_validate_config.py      Resolve config. No Kafka, no secrets, no writes.
  01_run_unit_tests.py       Full pytest suite on the cluster
  02_check_connectivity.py   Secrets, certs, Schema Registry. Still no Kafka.
  03_run_ingestion.py        The real thing, interactively

resources/                   Databricks Workflows definitions (DAB)
  job_ingest_primary.yml     Daily ingestion, one task per topic
  job_replay.yml             The two replay jobs
  job_maintenance.yml        Weekly OPTIMIZE / VACUUM. Does not delete data.
tests/                       unit tests (fast suite + Spark-backed suite)
```

---

## Onboarding a topic

**Structural change = PR. Operational change = SQL `UPDATE`. Code change = neither.**

1. **PR:** copy [`conf/topics/_TEMPLATE.yaml`](conf/topics/_TEMPLATE.yaml) to
   `conf/topics/<topic_key>.yaml`, fill in the 🔴 fields. Do not name any table: landing,
   curated and quarantine are all derived from the Kafka topic name. Only `audit_table` is
   shared, and it comes from `conf/defaults.yaml` too.
2. **PR:** add a task to `resources/job_ingest_primary.yml` with the new `topic-key` —
   five copy-paste lines.
3. **Optional SQL:** insert a control row. The topic runs on YAML defaults without one.
4. **Tables:** none. Landing, curated, quarantine and audit are all created by the first
   run — curated included, from a schema derived off the Avro reader schema before any row
   is read. No manual DDL, and no table whose shape Spark guessed.
5. **Code:** none. Confirm by reading
   [`entrypoints/ingest_primary.py`](src/kafka_ingest/entrypoints/ingest_primary.py) — it
   parses three parameters and calls `pipeline.run()`.

Validate before deploying:

```bash
pytest tests/test_shipped_config.py -q
databricks bundle validate -t dev
```

---

## Support runbook

**Read the audit table first.** Q1–Q3 in
[`sql/03_support_queries.sql`](sql/03_support_queries.sql) answer most tickets without
changing anything. The per-layer rows make "which layer did it die on?" a lookup:

```sql
SELECT batch_id,
       max(CASE WHEN layer='landing' THEN status END) AS landing_status,
       max(CASE WHEN layer='curated' THEN status END) AS curated_status
FROM <audit_table> WHERE topic_key = '<topic>' GROUP BY batch_id ORDER BY batch_id DESC;
```

### A batch failed — do I need to do anything?

Usually **no**. A retry replays the *same* batch over the *same* offsets, Delta skips the
already-committed landing write, and curated is written. Just re-run (Workflows *Repair
run*, or the next schedule).

### The same batch_id keeps failing (poison batch)

The stream is stuck and will not advance on its own. Either register the missing schema, or
unblock it with no deploy:

```sql
UPDATE ops_prod.ingestion.ingestion_topic_control
SET on_deser_error = 'quarantine',
    change_reason = 'INC12345 - unblock stuck stream',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';
```

Bad records go to quarantine with raw bytes retained; the stream drains. Recover them later
with the curated replay job, then set it back to `fail`.

### Choose the right replay

| Symptom | Job | Touches Kafka? | Works past retention? |
|---|---|---|---|
| Data missing at source; consumer gap; **checkpoint was deleted** | **replay_kafka** | Yes | No |
| Bytes fine but parsed wrong; schema registered late; records quarantined | **replay_curated** | No | Yes |

If unsure, it is almost always `replay_curated` — landing already has the bytes.

**Kafka replay:** Workflows → *REPLAY from Kafka* → set `topic_key`, `rerun_id` (e.g. your
incident number), and **one** of `starting_offsets` / `starting_timestamp`. It runs against
its own checkpoint *and* its own Delta `txnAppId`, so it cannot collide with the primary
stream. Optionally set an ending offset/timestamp to cap it.

**Curated replay:** Workflows → *REPLAY Curated from Landing* → set `topic_key`,
`rerun_id`, and `landing_filter` (e.g. `writer_schema_id = 5513`). The topic predicate is
added automatically.

Every replayed row is tagged `ingested_via` and `replay_run_id`. Landing inserts-if-absent;
curated upserts. Re-running the same `rerun_id` is safe.

### ⚠️ Never delete a primary checkpoint

Batch ids restart at 0 and Delta then skips every write as a duplicate — **the job reports
success and ingests nothing**. The framework refuses to start in that state, but the fix is
always the kafka replay job. [DESIGN §4, scenario 6](docs/DESIGN.md).

---

## Failure handling

Default is **FAILFAST**: a record that cannot be parsed fails the batch, the run fails, the
job alerts, and a `FAILED` audit row names the layer, batch id and error.

`on_deser_error: quarantine` routes bad records aside instead:

| `quarantine_reason` | Meaning | Recovery |
|---|---|---|
| `malformed_wire_format` | No `0x00` magic byte, or under 5 bytes | Producer is not using the Confluent serializer. Fix upstream. |
| `schema_resolution_failed` | Registry has no such schema id | Register it, then **replay_curated** filtered on that `writer_schema_id` |
| `avro_decode_failed` | Bytes did not parse against the writer schema | Inspect the raw bytes; the record is retained in full |

---

## Testing

```bash
ruff check src tests notebooks   # lint. CI runs exactly this.
pytest -m "not spark"            # config, security, registry, writers, dispatch. No JVM. CI gate.
pytest                           # everything, including the Spark-backed tests
```

CI is `azure-pipelines.yml`: lint plus the fast suite on every pull request, then
`databricks bundle deploy`. The deploy stage still needs its service connection and approval
gates wired up — see the comments in that file.

Run on Databricks via `notebooks/01_run_unit_tests` — DBR has Spark and `spark-avro` built
in, so the whole suite runs with no setup. **No test connects to Kafka, reads a secret, or
writes to a table.**

<details>
<summary>Running the JVM-backed tests on a laptop (three non-obvious prerequisites)</summary>

Verified on Windows with Python 3.14.7, PySpark 4.2.0 and Microsoft OpenJDK 21.

1. **A JDK, with `JAVA_HOME` set.** `winget install Microsoft.OpenJDK.21`
2. **`spark-avro` is not in the PySpark pip package.** It ships the Avro *Java library* but
   not the Spark connector providing `from_avro`. Drop the jar into
   `<venv>/Lib/site-packages/pyspark/jars/`:
   `https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.2.0/spark-avro_2.13-4.2.0.jar`
3. **Do NOT set `spark.jars.packages` on Windows.** Ivy resolution shells through Hadoop's
   `Shell` class, needs `winutils.exe`, and kills the `SparkContext` with a misleading
   `HADOOP_HOME is unset` error. Without it, the missing `winutils.exe` is a harmless warning.

Local Spark is 4.2.0, **not your DBR version** — a local pass is evidence, not proof. That
is why `assert_from_avro_semantics()` re-checks the writer/reader mapping at job startup on
the real cluster.
</details>

---

## Before go-live

The open items are listed in [DESIGN §9](docs/DESIGN.md). The one that matters most:

**Verify Delta's `txnAppId`/`txnVersion` behaviour on your DBR.** It is the mechanism the
whole no-duplicates story rests on, and it was reasoned from the documented contract rather
than measured — no Delta was available in the environment this was written in. Test it by
killing a job deliberately between the landing and curated writes, re-running, and checking
that Q9 in `sql/03_support_queries.sql` returns zero rows.
