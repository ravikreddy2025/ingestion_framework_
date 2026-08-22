# Developer Runbook

For the engineers who own, extend and deploy this codebase.

**Lost in the file tree?** [NAVIGATION.md](NAVIGATION.md) maps every file and traces one
Kafka record through the 10 modules in execution order — the fastest way to orient.

**Prerequisite reading:** [DESIGN.md](DESIGN.md). This runbook tells you *how to do things*;
DESIGN.md tells you *why the code is shaped the way it is*. Do not change core behaviour
without reading §4 (re-runs and duplicates) and §5 (design decisions).

**Your first hour, in order:**

1. [NAVIGATION.md](NAVIGATION.md) — "The 10-minute path" (10 min)
2. `src/kafka_ingest/pipeline.py` — read the module docstring, then `process_microbatch` (15 min)
3. [DESIGN.md](DESIGN.md) §4 — re-runs and duplicates (15 min)
4. §1 below — get the tests running locally (20 min)

---

## 1. Local setup

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]" pyspark      # Windows; use .venv/bin on Linux/macOS
pytest -m "not spark"                              # fast gate, no JVM needed
```

For the JVM-backed tests you need three things — all three, or they skip:

1. **A JDK with `JAVA_HOME` set.** `winget install Microsoft.OpenJDK.21`
2. **The `spark-avro` jar**, which is **not** in the PySpark pip package (it ships the Avro
   Java library, not the Spark connector that provides `from_avro`). Download it into
   `.venv/Lib/site-packages/pyspark/jars/`:
   ```
   https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.2.0/spark-avro_2.13-4.2.0.jar
   ```
3. **Do not set `spark.jars.packages` on Windows.** Ivy resolution shells through Hadoop's
   `Shell` class, needs `winutils.exe`, and kills the whole `SparkContext` with a
   misleading `HADOOP_HOME is unset` error. Without that setting the missing
   `winutils.exe` is a harmless warning and local Spark works fine.

```bash
ruff check src tests notebooks   # lint - the same command CI runs
ruff format src tests            # formatter: CONFIGURED BUT NOT YET ADOPTED, see below
mypy                             # type check, report-only for now

pytest -m "not spark"  # the fast suite - this is the CI gate. No JVM needed.
pytest                 # everything, including the Spark-backed tests
pytest -m spark        # only the JVM-backed ones
pytest tests/test_curated_writer.py -q -k mixed    # the central correctness test
```

**Do not run `ruff format` casually.** It is configured to the line length the code already
uses, but it has never been applied, and running it would rewrite almost every file.
Adopting it should be its own commit containing nothing else, so that the reformatting diff
never has to be reviewed alongside a behavioural change.

**mypy is report-only.** It currently reports around two dozen `Optional`-narrowing findings
- places where a type says a value may be `None` and the code knows it is not, because
config validation already guaranteed it. They are worth closing over time; none is a bug.

Local Spark is **not your DBR version**. A local pass is evidence, not proof — which is why
`assert_from_avro_semantics()` re-checks the critical assumption at job startup on the
real cluster. Run `notebooks/01_run_unit_tests` on a cluster before shipping.

---

## 2. Codebase tour in five minutes

Read in dependency order. Each module has a docstring that explains its job and its
edge cases; the ordering below is the order data flows. For a per-file "open it when…"
table and the execution-order trace, see [NAVIGATION.md](NAVIGATION.md).

```
config.py         two-tier config -> TopicConfig.   No PySpark import (keeps CI cheap)
security.py       secrets + certs -> connection options
kafka_source.py   readStream / batch read, primary vs replay positioning
schema_resolver.py wire-format columns + Schema Registry client
landing_writer.py raw bytes + CloudEvent columns -> landing
curated_writer.py per-writer-schema decode -> curated (+ quarantine)
audit.py          per-batch, per-layer status rows
tables.py         DDL + partitioning
pipeline.py       chained foreachBatch body, run shapes, startup guard   <- start here
entrypoints/      argparse -> pipeline.run().  Nothing else.
```

If you only read one file, read `pipeline.py`. Its module docstring is the design in
miniature.

### Invariants — do not break these without a design discussion

| Invariant | Why | Enforced by |
|---|---|---|
| `config.py` imports no PySpark | Keeps config/validation testable in plain CI | Convention + `tests/test_config.py` running without Spark |
| `txn_app_id` is deterministic | Delta only dedups retried batches if the app id is stable across restarts | `pipeline._make_txn_app_id` |
| A replay never shares a checkpoint **or** a `txnAppId` with primary | Otherwise the offset override is silently ignored and production state advances | `TopicConfig.checkpoint_path`, `tests/test_config.py` |
| Landing stores `value` byte-identical to the wire | Curated replay past Kafka retention depends on it | `tests/test_curated_writer.py::test_landing_keeps_the_original_bytes_verbatim` |
| Curated is 1:1 with Kafka records (no `explode`) | Preserves the `(topic, kafka_partition, kafka_offset)` merge key | `test_payload_stays_nested_and_is_not_exploded` |
| Audit writes never raise | An audit failure must not fail a good batch | `AuditWriter.emit` try/except |
| Projection column order == DDL column order | Delta rejects the first append otherwise | `test_landing_column_order_matches_the_ddl` |

---

## 3. Onboard a topic

**One PR. No Python changes, ever.**

1. Copy [`conf/topics/_TEMPLATE.yaml`](../conf/topics/_TEMPLATE.yaml) →
   `conf/topics/<topic_key>.yaml`. The **filename is the `topic_key`** — it becomes the
   job parameter, the control-table key, a checkpoint path segment and a column in every
   audit row. `lower_snake_case`. Renaming it later orphans the checkpoint.
2. Fill in every `<ANGLE_BRACKET>`. The tier markers tell you what must change:
   - do not name any table — landing, curated and quarantine are all derived from the
     Kafka topic name (`table_name_for()`; dots/hyphens → underscores). Set `table_name:`
     only if the derived name collides with another topic's or reads badly.
   - most topics need nothing beyond the template's `<ANGLE_BRACKET>` fields. Reach for the
     template's commented `environments:` block only if one setting must differ in exactly
     one environment for this topic — `conf/topics/vector_patient_events.yaml` is a real,
     working example; see `docs/CONFIGURATION.md` §4 for when this is (and is not) the
     right layer.
3. Verify the Schema Registry subject. It is usually `<topic>-value`, but
   RecordNameStrategy subjects look completely different:
   ```bash
   curl -u $KEY:$SECRET https://<registry>/subjects | jq
   ```
4. Add a task to `resources/job_ingest_primary.yml` — copy an existing block, change
   `task_key` and `topic-key`.
5. Validate locally, then deploy:
   ```bash
   pytest tests/test_shipped_config.py -q
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```
6. Run `notebooks/00_validate_config` → `02_check_connectivity` → `03_run_ingestion` in
   dev, pointing at scratch targets.

`test_shipped_config.py` is the safety net — it catches a non-3-tier name, an unknown
cluster reference, a DBFS cert path, two topics sharing a checkpoint, and two topics
sharing a landing or curated table (each is one table per topic now — sharing one is a
bug, not the old design). It already runs on every PR via `azure-pipelines.yml`.

### If the topic needs a new cluster or registry

Add a block to `conf/clusters.yaml` / `conf/registries.yaml` in the same PR. Nothing else
changes — the framework already supports SASL/PLAIN, SCRAM-256/512 and mTLS side by side,
and registry auth is independent of Kafka auth.

Coordinate two things with platform/infra **before** the PR:
- secret scope exists, keys are populated, the job's SP has `READ`
- for mTLS: certs uploaded to a UC Volume, and **executors can read them** (the check is in
  `notebooks/02_check_connectivity`)

---

## 4. Which layer does my change belong in?

Configuration merges five layers (one with a rare optional sub-layer), later winning per key:

```
conf/defaults.yaml  ->  conf/environments/<env>.yaml  ->  conf/topics/<key>.yaml
                                                              |
                                                         environments:<env> (3a, rare)
                                                              |
                              control table  ->  job parameters
```

```
Is the value the same in dev, preprod AND prod?
  YES -> Is it the same for every topic?
           YES -> conf/defaults.yaml
           NO  -> conf/topics/<key>.yaml
  NO  -> Does it vary by environment only?  (catalog, broker, secret scope)
           YES -> conf/environments/<env>.yaml
           NO  -> Does it vary by BOTH one topic AND one environment? (rare)
                    YES -> that topic file's environments:<env> sub-layer (3a) - see
                           conf/topics/vector_patient_events.yaml for a real example
                    NO  -> topic file, and check whether the environment file should
                           carry a different default for it

Does support need to change it during an incident, without a deploy?
  YES -> it must be in OPERATIONAL_OVERRIDE_FIELDS (config.py) and the control table.
```

Partitioning and dedup keys are deliberately **not operationally overridable** — changing a
table's physical layout or its dedup semantics should require review. `test_config.py`
asserts an operational override of those is ignored, so it is a tested contract, not a
convention.

**Never hardcode a catalog in a topic file.** It works in prod and silently breaks dev.
Use `{catalog}` in `defaults.yaml` instead; `tests/test_shipped_config.py` resolves every
topic in every environment specifically to catch this.

---

## 5. Common code changes

### Add a CloudEvent attribute

Two files, and a test will fail until they agree:

1. `landing_writer.py` → add the header name to `CE_ATTRIBUTES`
2. `tables.py` → add the column to `CLOUDEVENT_COLUMNS`

It flows into landing, curated and quarantine automatically, because all three compose that
same DDL block.

### Add an audit column

1. `audit.py` → `AUDIT_SCHEMA` (a `StructField`) and `AuditWriter.build_row` (the value)
2. `tables.py` → `AUDIT_DDL_COLUMNS`
3. `sql/02_layer_tables.sql` → keep the reference DDL in step

`test_audit_and_tables.py` asserts all of these match. On an existing table you also need
an `ALTER TABLE ... ADD COLUMN`.

### Change a partition column

Config change plus a **table rewrite** — Delta cannot repartition in place. Plan it:
create the new table, backfill (a curated replay for curated; a `CREATE TABLE AS SELECT`
for landing), swap names, then update the YAML.

### Change what curated looks like

The two shape decisions are documented in DESIGN.md §5 and pinned by tests. If you flatten
the payload or explode arrays you will break the merge key that makes replay idempotent —
read that section first, then update the test deliberately rather than deleting it.

### Add a run shape (e.g. a dry-run mode)

Add a function to `pipeline.py` next to `run_streaming` / `run_bounded_replay` /
`run_curated_replay`, dispatch it in `run()`, and add a thin entrypoint. Do **not** add a
parallel implementation of the microbatch body — `process_microbatch` is the single place
landing and curated are written, and keeping it that way is what makes the audit and
idempotency stories hold everywhere.

---

## 6. Debugging

| Symptom | First look |
|---|---|
| Job fails at startup with `ConfigError` | The message names the file and key. Validation is deliberately loud. |
| `REFUSING TO RUN ... checkpoint ... is missing` | The reset guard. **Do not delete landing rows to get past this** — read DESIGN.md §4 scenario 6. Support's sanctioned bypass is `checkpoint_reset_id` (RUNBOOK_SUPPORT §5.4a), not a code change. |
| `from_avro reader/writer schema self-check failed` | DBR older than 13.3 LTS, or the runtime changed `from_avro` semantics. |
| `SchemaResolutionError ... HTTP 404` | Records were produced against a **different** registry than the topic YAML points at. |
| `SchemaResolutionError ... unreachable ... NCC` | Network path. On serverless, a missing private endpoint. |
| Batch fails repeatedly at the same `batch_id` | Poison batch. See the support runbook — the fix is usually a control-table change, not code. |
| Curated rows missing but landing has them | Query Q8 in `sql/03_support_queries.sql` (LEFT ANTI JOIN). Then a curated replay. |

Logging is plain `logging` at INFO, written to stdout so it lands in the Databricks driver
log. Every Kafka/registry options map passes through `security.redact()` before it is
logged — **never log a raw options dict.**

---

## 7. PR checklist

- [ ] `ruff check src tests notebooks` is clean — CI runs exactly this
- [ ] `pytest -m "not spark"` passes (this is the PR gate)
- [ ] `pytest` passes locally (and `pytest -m spark` if you touched parsing or projections)
- [ ] `pytest tests/test_shipped_config.py` passes — catches config typos
- [ ] `databricks bundle validate -t dev`
- [ ] No secret value, table name, path or endpoint hardcoded in Python
- [ ] New/changed config keys documented in `docs/CONFIGURATION.md` **with a tier marker**
- [ ] If you changed a DDL constant, the matching Python constant changed too
- [ ] If you changed behaviour described in `DESIGN.md`, that doc changed too
- [ ] Comments explain **why**, not what

---

## 8. Known gaps to be aware of

**The list lives in one place: [`DESIGN.md` §9](DESIGN.md#9-open-items-for-the-incoming-team).**
It is not duplicated here on purpose — two copies of the same list is exactly how a copy
goes stale while the other gets updated, which is what happened to the previous version of
this section. If you pick one up, read DESIGN.md §9 for the current, complete list and its
reasoning.

The two most likely to bite a developer first:

1. **Delta `txnAppId`/`txnVersion` is unverified on real Delta.** The whole no-duplicates
   story rests on it. Verify with a deliberate mid-batch kill, then Q9.
2. **Curated replay's schema-evolution mechanism is runtime-dependent** (DBR 15.4 LTS+ vs
   the legacy fallback). Confirm which branch your runtime takes — DESIGN.md §9 item 6.
