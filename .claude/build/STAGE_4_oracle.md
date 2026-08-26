# STAGE 4 -- Oracle source

**Paste `00_CORE.md` before this file.** Stage 3 must be green.

**Four sub-steps. Report after each. Do not merge them into one pass.** Sub-step 4a is where
most of Oracle's correctness lives and it is fully testable on your machine; 4b is the part
you cannot execute at all.

---

## 4a -- Config, spec and query builder (pure Python, fully testable)

### Configuration

All keys optional except `source_table`.

```yaml
# conf/sources/oracle_claim_header.yaml
source_type: oracle
jdbc_ref: oracle_prod_core           # profile in conf/jdbc.yaml
source_schema: CLAIMS                # optional; default in conf/defaults/oracle.yaml
source_table: CLAIM_HEADER           # REQUIRED -- the only mandatory key

# extraction shape: EITHER sql_query OR the column/filter set. Never both.
sql_query: null
columns: [CLAIM_ID, MEMBER_ID]
filter_column: STATUS
filter_criteria: "IN ('A','P')"
dynamic_date_filter:
  column: LAST_UPDATE_DT
  window: "P7D"

# incremental behaviour
incremental_mode: cursor             # full | cursor | filter
cursor_column: LAST_UPDATE_DT
cursor_type: timestamp               # timestamp | number
merge_keys: [CLAIM_ID]               # optional -- changes correctness, see 4c

# read parallelism -- not optional tuning
partition_column: CLAIM_ID
num_partitions: 8
fetch_size: 10000
```

### Validation rules

- `sql_query` mutually exclusive with `columns`, `filter_column`, `filter_criteria`,
  `dynamic_date_filter`. Both set -> error naming both keys.
- `incremental_mode: cursor` requires `cursor_column` + `cursor_type`.
- `incremental_mode: filter` requires `filter_column` + `filter_criteria`.
- `filter_criteria` is a SQL fragment: **structural only, never operationally overridable**,
  validated against a conservative allowlist the way `_require_safe_identifier` already
  validates identifiers. Support must not be able to inject SQL via the control table.
- `sql_query`, if present, must be a single `SELECT`. Reject any statement separator or
  DML/DDL keyword.

### `sources/oracle/query.py`

Pure string construction. No Spark, no database.

1. Base: `sql_query` if set, else `SELECT <columns or *> FROM <schema>.<table>`.
2. Append the static filter, if set.
3. Append the dynamic date filter, if set.
4. Append the incremental predicate, if `incremental_mode: cursor` (see 4c).
5. Return the query, and have the caller record it in the audit row's `source_detail`.

**Record the query in audit.** "What did this run actually ask Oracle for" is the first
question of every Oracle incident and is not reconstructable from config once dynamic filters
and watermarks are involved.

### Target naming

`{catalog}.oracle_{source_schema}.{source_table}`, lower-cased, pattern in
`conf/defaults/oracle.yaml`, resolved by `framework/tables.py`.

- **Oracle folds unquoted identifiers to upper case.** `CLAIMS.CLAIM_HEADER` is the real name;
  `{catalog}.oracle_claims.claim_header` is the UC target. Normalise in one function; state
  the rule in `docs/CONFIGURATION.md`.
- A name legal in Oracle but illegal in Unity Catalog fails at **config load**.

### 4a exit gate

Query-builder tests exhaustive across every combination of `sql_query` / `columns` / filters /
dynamic date / cursor. Every validation rule tested. All pure Python, all green.

---

## 4b -- JDBC read (cannot be executed here)

`sources/oracle/run.py` read path and `sources/oracle/types.py`.

```python
(spark.read.format("jdbc")
  .option("url", url)
  .option("dbtable", f"({query}) t")        # VB-01: query vs partitionColumn
  .option("user", user).option("password", pwd)
  .option("driver", "oracle.jdbc.OracleDriver")
  .option("fetchsize", cfg.fetch_size)      # Oracle JDBC default is 10 rows
  .option("partitionColumn", cfg.partition_column)
  .option("lowerBound", lo).option("upperBound", hi)
  .option("numPartitions", cfg.num_partitions)
  .option("queryTimeout", cfg.query_timeout)
  .option("sessionInitStatement", cfg.session_init)
  .load())
```

Without `partitionColumn` / `numPartitions` a JDBC read is **single-threaded regardless of
cluster size**. This is Oracle's equivalent of Kafka's `minPartitions` -- not optional tuning.

`conf/jdbc.yaml` register: connection profiles with auth mode, secret scope and KEY names,
following the same pattern as `conf/clusters.yaml`. Credentials resolved through
`framework/security.py`. **No credential may appear in a logged or audited options map** --
extend the existing test.

### Datatypes -- `sources/oracle/types.py`

- Let the driver's default mapping stand where unambiguous.
- Optional per-source `column_types` map rendered into the JDBC `customSchema` option.
- **Log the resolved Spark schema into `source_detail` every run**, and fail the run if it
  differs non-additively from the existing landing table's schema. A silent type change on an
  Oracle column is otherwise invisible until a consumer breaks.
- **Fail loudly with the column name and Oracle type for any unmapped type** rather than
  stringifying silently.
- `NUMBER` without precision, `DATE`, LOB, `RAW`, `INTERVAL` and TZ types are VB-02 to VB-04.
  Write code that accommodates whichever answer comes back; **assert nothing**.

### 4b exit gate

Code written, unit-tested against fakes for everything that does not need a driver. **Say
plainly in the report that nothing here was executed**, and list the VB entries covering it.

---

## 4c -- Watermark lifecycle and the boundary bug

**The unsafe pattern** is `WHERE cursor > :last_watermark` with no upper bound. Rows committed
in Oracle *during* the extract may or may not appear depending on read timing, and the
watermark then advances past them. It passes every test written against a static table.

**The rule -- every cursor run uses a closed interval**, upper bound captured at run start:

```sql
WHERE cursor_col > :last_watermark AND cursor_col <= :run_high_water
```

`:run_high_water` comes from `SELECT MAX(cursor_col)` (or the DB clock for a timestamp cursor)
taken in the same read, and is written to `ingest_state` **only after** the landing write
commits.

**Ties at the boundary** are the remaining hazard with a non-unique cursor such as a
second-granularity timestamp:

| `merge_keys` | Predicate | Write mode | Property |
|---|---|---|---|
| **set** | `>= :last_watermark` | MERGE on `merge_keys` | Tie-safe. Re-reads the boundary and de-duplicates. **Recommended default.** |
| **absent** | `> :last_watermark` | append | Faster. **Rows sharing the boundary cursor value can be lost.** |

**Superseded by `docs/build_log/DECISIONS.md` D-09 decision 2, decided during this stage's
own review:** "absent" in the row above is not a supported starting point. A cursor source
that omits `merge_keys` entirely fails config load with a hard `ConfigError`
(`sources/oracle/config.py::_require_a_merge_key_decision`) rather than a startup WARN --
the lossy-append path is reachable only through the explicit waiver `merge_keys: []`, which
is what makes the omission-vs-waiver distinction a config-time decision instead of a
silent default. Make absence deliberate: the explicit `[]` waiver, plus a MUST-READ row in
`docs/CONFIGURATION.md`. **Assert the documented loss in a test** so it is a known property
rather than a surprise.

### Idempotency and ordering

- `txnAppId = f"ingest::oracle::{source_key}"`, `txnVersion = run_sequence` from
  `ingest_state`.
- **Order, non-negotiable**: read -> write landing -> commit -> **then** advance the
  watermark. A crash before the advance re-extracts the same interval, which the txn markers
  or the MERGE key absorb.
- **A replay never writes `ingest_state`.** Assert it.
- Layers: `landing` only. `layers = ("landing",)`.

### 4c exit gate

Watermark advances only after a committed write (assert with a fake writer). A replay never
advances it. A crash between write and advance re-extracts the same interval. The
`merge_keys`-absent boundary loss is asserted.

---

## 4d -- Operationalise

- One job template under `resources/`, calling `entrypoints/run_ingest.py` with
  `--source-key`. `max_concurrent_runs: 1`, `queue.enabled: false`.
- Inert onboarding template `conf/sources/_TEMPLATE_oracle.yaml`, with the partition-count and
  cursor-column questions the onboarder must ask the source team.
- `docs/CONFIGURATION.md` rows for every Oracle key, tiered MUST / NICE / NO CHANGE.
- Oracle failure-scenario table in `docs/DESIGN.md`: transient JDBC failure; write failure
  after read; crash between write and watermark advance; watermark manually corrupted; cursor
  values arriving out of order; source table dropped or altered.
- Oracle incident playbook in `docs/RUNBOOK_SUPPORT.md`, SQL and job parameters only.

---

## Do not build

- A curated layer for Oracle.
- A schema-migration or reconciliation utility.
- A connection pool, or any retry framework around JDBC.
- A generic SQL builder or query DSL -- one function, one shape, fully tested.
- Anything that lets `filter_criteria` come from the control table.

---

## Files

**Create:** `sources/oracle/spec.py`, `sources/oracle/run.py`, `sources/oracle/query.py`,
`sources/oracle/types.py`, `conf/sources/_TEMPLATE_oracle.yaml`,
`resources/job_ingest_oracle.yml`
**Edit:** `conf/jdbc.yaml`, `conf/defaults/oracle.yaml`, `framework/security.py`,
`docs/CONFIGURATION.md`, `docs/DESIGN.md`, `docs/RUNBOOK_SUPPORT.md`

---

## Exit gate (all four sub-steps)

- `pytest -m "not spark" -q` green, test count up.
- Query-builder tests exhaustive.
- Watermark lifecycle tests pass, including the replay-never-advances case.
- Boundary-tie behaviour asserted for both `merge_keys` settings.
- No credential in any logged or audited options map.
- The CORE section 7 grep returns nothing.
- VB entries added for everything in 4b you could not execute.

Then write the stage report (CORE section 9) and **stop**.
