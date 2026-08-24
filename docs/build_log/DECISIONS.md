# Decisions -- settled by the human

Decisions returned by a stage report and answered. **These are settled. Do not re-litigate
them.** Each entry states the decision, why, and what work it implies.

Any stage that finds a decision here genuinely unworkable must say so in its report rather
than quietly doing something else.

---

## D-01 -- Source-specific control-table columns carry a source prefix

**Decided:** there is no truly universal control table across source types, and pretending
otherwise adds confusion about how to use a column rather than removing it. So:

- A column that means **exactly the same thing for every source** has **no prefix**. It is
  framework-owned.
- A column that belongs to **one source type** is named **`<source_type>_<setting>`**.
  Every other source ignores it.
- The same rule applies to every source type added in future.

**Resulting table shape** (`ingest_control`):

```
-- framework-owned, apply to every source
source_key                     STRING NOT NULL   -- primary key
source_type                    STRING NOT NULL
enabled                        BOOLEAN
replay_rerun_id                STRING
replay_controls                STRING (JSON)     -- per-source replay parameters
notes                          STRING
updated_by                     STRING
updated_at                     TIMESTAMP

-- kafka only
kafka_failure_mode             STRING            -- FAILFAST | QUARANTINE
kafka_max_offsets_per_trigger  BIGINT
kafka_checkpoint_reset_id      STRING

-- oracle only (added in Stage 4)
oracle_fetch_size              INT
oracle_num_partitions          INT

-- file only (added in Stage 5)
file_failure_mode              STRING
file_max_files_per_trigger     BIGINT
file_checkpoint_reset_id       STRING
```

**Consequences, all of which are the point:**

1. **`batch_limit` and `failure_mode` are gone as shared columns.** They became
   `kafka_max_offsets_per_trigger`, `oracle_fetch_size`, `file_max_files_per_trigger` and
   so on. A support engineer now reads a column name that says which knob it turns, instead
   of one word meaning three different mechanisms.
2. **`source_overrides` JSON is removed.** With prefixed columns there is no second place a
   source-specific setting can live, and two mechanisms for one job is the confusion this
   decision exists to prevent. Adding a source type already requires a deploy (new package,
   job template, conf files), so it may also require an `ALTER TABLE`. That is honest, not a
   regression.
3. **`replay_controls` stays JSON and stays unprefixed.** It carries structured replay
   parameters whose *shape* differs per source (Kafka offsets or timestamps, an Oracle
   cursor range, a file path or date range). A JSON document validated against the source's
   own spec is different from a scalar column with three meanings. Revisit only if a reader
   is ever confused by it.
4. **Setting a prefixed column for the wrong source type is an error**, not a silent
   ignore. The message names the column and the mismatch -- e.g. *"`kafka_checkpoint_reset_id`
   is set for `oracle_claim_header`, whose source_type is `oracle`"*. A typo in the prefix is
   the mistake this catches.

**CRITICAL for the grep gate:** `framework/control.py` must **not** contain the strings
`kafka_`, `oracle_` or `file_`. Each `SOURCE_SPEC` declares its own control columns -- add a
`control_columns: Mapping[str, str]` field (column name -> setting name) -- and
`framework/control.py` reads whatever the spec declares. The framework knows the *rule*, never
the *names*. This is the same reason `target_tokens` exists.

---

## D-02 -- Grants are Terraform-owned; the framework and the SQL scripts do not grant

**Decided:** grants are provisioned by Terraform, outside this repository. User group and
role details do not exist yet.

**Work this implies:**

1. **Remove `GRANT` statements from `sql/01_operational_config.sql` and
   `sql/02_layer_tables.sql`.** Replace with a header comment stating that grants are
   Terraform-owned and pointing at the privilege list below.
2. **Record the privileges the framework needs**, so whoever writes the Terraform has a
   specification rather than a guess. Put it in `docs/RUNBOOK_CLIENT_IT.md`, which is already
   the platform-prerequisites document. At minimum, for the ingestion service principal:
   `USE CATALOG` on the ops and data catalogs; `USE SCHEMA` + `CREATE TABLE` + `MODIFY` +
   `SELECT` on each schema the framework writes; `SELECT` on the control schema.
3. **`CREATE SCHEMA` is also out of scope for the framework.** Schemas are pre-created.
   Stage 4 must therefore add "create the `oracle_<schema>` UC schema before the first run"
   to the Oracle onboarding checklist, or the first onboarding fails on an undocumented step.
4. **VB-16 stays open but is no longer a blocker** -- it becomes "confirm Terraform granted
   what the list above says", to be checked when a workspace exists.

---

## D-03 -- `batch_id` is renamed `txn_version`

**Decided:** accepted. The column carries the Delta `txnVersion` for every source type --
a streaming microbatch id, a batch source's `run_sequence`, or `-1`. `batch_id` reads as
Kafka vocabulary in a source-neutral table; `txn_version` says what it is and matches the
Delta concept it feeds.

Rename in `framework/audit.py` (schema + writer), the audit DDL, and every test and support
query that names it. Nothing is deployed, so there is no migration.

---

## D-04 -- `ingest_state` is partitioned by `source_key`

**Decided:** partition now rather than wait for contention.

A run sequence is allocated on **every** run of **every** source type -- conditioning that on
source type is the branch the grep gate forbids, and conditioning it on configuration would
let a misconfigured environment silently lose idempotency protection. The cost is one MERGE
per run into one shared table, and with many sources on the same schedule those MERGEs are
concurrent.

Delta detects conflicts at file granularity, so **partitioning by `source_key` makes
concurrent MERGEs from different sources touch disjoint files and stop conflicting.**

- `PARTITIONED BY (source_key)` on `ingest_state`. Not `CLUSTER BY` -- Delta allows one or
  the other, never both, and partitioning is what buys the conflict isolation here.
- Enable deletion vectors on the table.
- Small-file growth is not a concern: a handful of rows per source, and the maintenance job
  already covers the framework's tables.
- **VB-15 is extended** to cover concurrency: does the `ingest_state` MERGE upsert correctly
  *and* survive N concurrent runs from different sources?

---

## D-05 -- Framework-owned configuration keys

**Decided:** accepted as built. `audit_table`, `state_table`, `control_table`,
`table_properties` and `rerun_id` are framework-owned keys in `framework/config.py`, joining
`domain` and `enabled`.

`conf/defaults.yaml` sets these for every source of every type. Without framework ownership,
every `SOURCE_SPEC` would have to declare keys that only the framework reads, and a source
author could misspell one into silence.

`rerun_id` is operational-only: a replay id checked into Git re-applies on every future
deploy.

**Work this implies:** the framework-owned key list must be documented in exactly one place.
`docs/CONFIGURATION.md` is that place (Stage 7). Until then this entry is the record.

---

## D-06 -- One ops catalog, three schemas

**Decided:** all framework metadata lives in a single ops catalog, split into schemas by
purpose:

| Object | Location |
|---|---|
| Audit rows | `{ops_catalog}.{audit_schema}.ingest_audit` |
| Control table | `{ops_catalog}.{control_schema}.ingest_control` |
| State table | `{ops_catalog}.{control_schema}.ingest_state` |
| Logs | `{ops_catalog}.{logs_schema}.*` -- **reserved, nothing written there yet** |

**This changes what Stage 2 built.** The audit table currently resolves to
`{catalog}.audit.ingest_audit` in the *data* catalog. Move it to the ops catalog. Nothing is
deployed, so this is a configuration and DDL change only.

**State lives in the control schema**, not its own -- it is operational state with the same
blast radius and the same audience as the control table. Revisit only if the two acquire
different access requirements.

**The `logs` schema is reserved, not used.** Create it in the provisioning SQL so the shape
is visible and Terraform can grant on it, but write nothing to it in this pass. Structured
logging currently goes to the driver log.

**Everything above must be parameterised, not literal**, because it is substituted per
environment at deploy time:

- `ops_catalog` is already an environment `vars:` entry. Add `audit_schema`,
  `control_schema` and `logs_schema` alongside it, defaulted in `conf/defaults.yaml` and
  overridable per environment.
- The three table settings in `conf/defaults.yaml` become
  `{ops_catalog}.{audit_schema}.ingest_audit` and so on -- no hardcoded schema names anywhere
  in Python or SQL.
- The existing test that asserts `ops_catalog` matches `databricks.yml` extends to the three
  schema names.

---

## Work list produced by these decisions

Apply as a short pass on its own branch **before Stage 3**, since all of it is Stage 2
territory and Stage 3 is already the largest stage in the project.

| # | Change | Files |
|---|---|---|
| 1 | Prefixed control columns; drop `source_overrides`; add `SourceSpec.control_columns`; wrong-source-type error | `framework/contracts.py`, `framework/control.py`, `sources/kafka/spec.py`, `sql/01_operational_config.sql`, tests |
| 2 | Remove `GRANT` from both SQL files; add the privilege list | `sql/01`, `sql/02`, `docs/RUNBOOK_CLIENT_IT.md` |
| 3 | Rename `batch_id` -> `txn_version` | `framework/audit.py`, audit DDL, `sql/03`, tests |
| 4 | `PARTITIONED BY (source_key)` + deletion vectors on `ingest_state`; extend VB-15 | `framework/tables.py` or `sql/02`, `docs/VERIFICATION_BACKLOG.md` |
| 5 | Audit table moves to the ops catalog; add `audit_schema` / `control_schema` / `logs_schema` vars | `conf/defaults.yaml`, `conf/environments/*.yaml`, `databricks.yml`, `sql/01`, `sql/02`, tests |

**Gate:** `ruff check`, `ruff format --check`, `pytest -m "not spark" -q` all green, test
count at or above 403, and the CORE section 7 grep still returns nothing -- item 1 is the one
most likely to break it.
