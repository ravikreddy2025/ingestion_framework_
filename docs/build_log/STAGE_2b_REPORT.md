# Stage 2b Report -- The six settled decisions, applied

Branch `decisions-and-stage2-followup`, continued from the commit that recorded D-01
through D-06 in `docs/build_log/DECISIONS.md`. This is the "short pass on its own branch
before Stage 3" that DECISIONS.md's own work list asks for -- five items, all Stage 2
territory, applied here so Stage 3 starts from a settled control-table shape rather than
inheriting six re-litigable decisions.

No new source is built. Every change is to the framework spine, the provisioning SQL, the
shipped `conf/`, and the tests that hold them in step.

---

## What changed, per decision

```
D-01  ingest_control: kafka_failure_mode / kafka_max_offsets_per_trigger /
      kafka_checkpoint_reset_id replace the shared failure_mode / batch_limit /
      checkpoint_reset_id columns. source_overrides JSON is gone. SourceSpec gains
      control_columns (column -> setting); a column set for the wrong source type is a
      named, run-time error, not a silent ignore.
D-02  Every GRANT statement removed from sql/01 and sql/02. docs/RUNBOOK_CLIENT_IT.md gains
      a "Unity Catalog privileges" table -- the specification for whoever writes the
      Terraform.
D-03  audit.batch_id renamed txn_version, everywhere: framework/audit.py, the DDL, sql/02,
      sql/03's Q2 comment.
D-04  ingest_state gains PARTITIONED BY (source_key) and
      'delta.enableDeletionVectors' = 'true', in both framework/state.py and sql/01.
D-05  Already built (Stage 2); nothing to do here beyond leaving it alone.
D-06  ingest_audit moves from {catalog}.audit to {ops_catalog}.{audit_schema};
      ingest_control / ingest_state move to {ops_catalog}.{control_schema}. A reserved,
      empty {logs_schema} is created. Three new environment vars, identical across
      dev/preprod/prod.
```

---

## 1. Done and verified

- **`framework/contracts.py`** -- `SourceSpec.control_columns: Mapping[str, str] = {}`
  added, docstring explains the column-name convention and points at D-01.
- **`framework/control.py`** -- rewritten. `_FRAMEWORK_SETTING_COLUMNS` (just `enabled`,
  `replay_rerun_id` now) replaces the old universal `_SETTING_COLUMNS`; overrides are built
  from `{**_FRAMEWORK_SETTING_COLUMNS, **spec.control_columns}`; `_JSON_COLUMNS` is now
  `("replay_controls",)` only. New `_check_no_foreign_control_columns()` raises when a
  non-null column belongs to a source type other than the row's own, naming both the
  column and the mismatch. The function contains no source-type literal anywhere -- it
  takes `other_control_columns` (column -> owning source_type) as a parameter.
- **`framework/runner.py`** -- new module-level `_CONTROL_COLUMN_OWNERS`, built from every
  `_SOURCES` entry's `SOURCE_SPEC.control_columns`, passed into `control.read_control()`.
  This is the "one place in framework/ allowed to know source types" building the registry
  `control.py` needs without `control.py` ever seeing a literal type name.
- **`sources/kafka/spec.py`** -- declares its three real control columns now
  (`control_columns={"kafka_failure_mode": "failure_mode", "kafka_max_offsets_per_trigger":
  "max_offsets_per_trigger", "kafka_checkpoint_reset_id": "checkpoint_reset_id"}`), same
  precedent as Stage 2's `target_tokens` -- the control table's *shape* is settled
  independently of when the source itself is built. `max_offsets_per_trigger` and
  `checkpoint_reset_id` are the legacy Kafka module's own field names (grep-verified, not
  invented); `failure_mode` is D-01's own choice of column, applied literally. The three
  setting names are deliberately **not** yet in `operational_keys` -- see decision 1 below.
- **`framework/audit.py`** -- `batch_id` -> `txn_version` throughout: DDL column,
  `StructType` field, `emit()`/`build_row()` parameter, `audit_id` format string,
  `NO_BATCH_ID` -> `NO_TXN_VERSION`.
- **`framework/state.py`** -- `ensure_state_table()` now passes `partition_by=["source_key"]`
  and merges `{"delta.enableDeletionVectors": "true"}` into the table's properties.
- **`framework/tables.py`** -- `effective_properties()` split out of `properties_clause()`
  so `state.py` can add one property on top of whatever configuration already produces,
  without losing the autoOptimize fallback when a config sets no `table_properties` at all
  (a real bug caught by mutation-testing this change -- see below).
- **`sql/01_operational_config.sql`** -- `ingest_control` rewritten with the three
  `kafka_*` columns, `source_overrides` dropped, `CONSTRAINT failure_mode_valid` renamed
  `kafka_failure_mode_valid`; `ingest_state` gains `PARTITIONED BY (source_key)` and the
  deletion-vectors property; both tables (and a new reserved `{logs_schema}`) qualified
  under `{ops_catalog}.{control_schema}` / `{ops_catalog}.{logs_schema}`; every `GRANT`
  removed.
- **`sql/02_layer_tables.sql`** -- `ingest_audit` qualified under
  `{ops_catalog}.{audit_schema}`, `batch_id` -> `txn_version`; every `GRANT` removed.
- **`sql/03_support_queries.sql`** -- every table qualifier updated; Q6/Q6b/Q6c marked
  SOURCE-SPECIFIC (kafka) and rewritten against the three real columns (Q6c now
  demonstrates `kafka_checkpoint_reset_id`, the one remaining incident-only lever, since
  `source_overrides` no longer exists); Q7c and Q12's generic cross-type listings drop the
  now-type-specific columns; Q2's comment renamed to `txn_version`.
- **`sql/04_maintenance.sql`** -- both audit-table `OPTIMIZE`/`VACUUM` statements now
  reference `:ops_catalog` instead of `:catalog` (see Blocked, below, for the one thing
  this does not finish).
- **`conf/defaults.yaml`** -- `audit_table` / `state_table` / `control_table` re-pointed at
  `{ops_catalog}.{audit_schema}` / `{ops_catalog}.{control_schema}`.
- **`conf/environments/{dev,preprod,prod}.yaml`** -- each gains `audit_schema: audit`,
  `control_schema: ingestion`, `logs_schema: logs` under `vars:`, identical across all
  three (unlike `catalog`/`ops_catalog`, there is no reason for these to differ by
  environment).
- **`databricks.yml`** -- the comment describing where the three framework tables are
  named updated to match; no new bundle variable needed (`ops_catalog` already existed).
- **`docs/RUNBOOK_CLIENT_IT.md`** -- new "Unity Catalog privileges" table under section 6,
  the exact specification the removed `GRANT` statements encoded, split by the new
  three-schema ops-catalog layout.
- **`docs/VERIFICATION_BACKLOG.md`** -- VB-15 extended with the cross-source
  partitioning claim (distinct from its existing same-source-concurrency question); VB-16
  reframed as "no longer a blocker, confirm Terraform granted the list"; VB-17 updated for
  the renamed constraint and the new `ingest_state` clauses.
- **CORE section 7 grep returns nothing.** Run from `src/kafka_ingest/`:
  ```
  $ grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' framework/ \
      | grep -v 'runner.py:.*_SOURCES'
  $ echo $?
  1
  ```
  The new `_CONTROL_COLUMN_OWNERS` line in `runner.py` names no source type literally (it
  is built from `_SOURCES.items()`), so it needed no exemption of its own.
- **Exit gate**, all three commands run in the project `.venv` (Python 3.12.10,
  pyspark 3.5.2):
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  22 files would be reformatted, 32 files already formatted

  $ python -m pytest -m "not spark" -q
  415 passed, 34 deselected in 5.87s
  ```
  Formatter baseline **unchanged**: 22 files, same as every prior stage. Two files this
  pass touched (`tests/test_framework_control.py`, `tests/test_shipped_sql.py`) drifted
  from the formatter during editing and were reformatted before committing; nothing in the
  pre-existing 22-file baseline was touched.
- **`GRANT` no longer appears in any `sql/*.sql` file** (`grep -c "^GRANT" sql/*.sql` ->
  0 for all four), and `tests/test_shipped_sql.py::test_no_sql_file_grants_anything` holds
  it there.
- **Nine mutations run to prove new/changed tests can fail**, each restored afterwards:

  | Mutation | Test that failed |
  |---|---|
  | comment out `_check_no_foreign_control_columns` in `control.py` | `test_a_column_belonging_to_a_different_source_type_is_rejected`, `test_a_column_for_a_different_source_type_is_rejected_end_to_end` |
  | change `kafka_max_offsets_per_trigger`'s mapped setting | `test_the_shipped_kafka_spec_declares_its_control_columns` |
  | drop `replay_rerun_id` from `_FRAMEWORK_SETTING_COLUMNS` | `test_the_named_columns_become_settings` |
  | re-add a `GRANT` line to `sql/01` | `test_no_sql_file_grants_anything` |
  | revert `AUDIT_SCHEMA`'s field name to `batch_id` (DDL left as `txn_version`) | `test_the_schema_and_the_ddl_agree_column_for_column`, `test_the_row_supplies_every_column_the_schema_declares` |
  | drop `partition_by=["source_key"]` from `ensure_state_table` | `test_ensure_creates_a_table_partitioned_by_source_key_with_deletion_vectors` |
  | remove `PARTITIONED BY (source_key)` from `sql/01`'s `ingest_state` | `test_ingest_state_is_partitioned_with_deletion_vectors_in_the_provisioning_sql` |
  | revert `conf/defaults.yaml`'s `audit_table` to `{catalog}.audit.ingest_audit` | `test_the_frameworks_own_tables_all_live_under_the_ops_catalog` (all three environments) |
  | revert `tables.effective_properties`'s fallback to `{}` | `test_ensure_keeps_the_configured_properties_alongside_deletion_vectors`, `test_no_properties_at_all_still_gets_auto_compaction` |

## 2. Done but not verifiable here

- **VB-15's extension** -- whether `PARTITIONED BY (source_key)` genuinely gives Delta
  file-level conflict isolation between *different* sources' concurrent MERGEs (as opposed
  to the same source running twice, which VB-15 already covered) needs a real Delta table
  under concurrent load.
- **VB-16** -- can the ingestion service principal and support group actually get the
  privileges `docs/RUNBOOK_CLIENT_IT.md`'s new table specifies; needs a workspace and
  Terraform run.
- **VB-17** -- do `sql/01`/`sql/02` execute as written, now including the renamed
  `kafka_failure_mode_valid` CHECK and `ingest_state`'s new `PARTITIONED BY` + deletion
  vectors clauses together in one `CREATE TABLE`; needs a real SQL warehouse.

## 3. Not reproduced

- **DECISIONS.md's work-list file column for item 1 names only `sql/01_operational_config.sql`.**
  `sql/03_support_queries.sql` (and, more narrowly, `sql/04_maintenance.sql`) also
  referenced the retired columns and the old table locations directly, and leaving them
  stale would have broken the very tests Stage 2 built to catch exactly this drift
  (`tests/test_shipped_sql.py`). Updated both, following Stage 2's own addendum precedent
  of fixing runbook staleness proactively rather than deferring it.
- **D-06 says the three new schema vars are "defaulted in `conf/defaults.yaml`".**
  Literally, `conf/defaults.yaml` has no `vars:` mechanism -- only environment files do
  (`framework/config.py`'s `_substitute` scope is built from `env_doc.get("vars")`, never
  from `defaults.yaml`). Adding one would have been a `framework/config.py` change, outside
  this pass's file list and a larger change than the other five items. Instead, all three
  vars are added identically to every environment file's `vars:` block -- the same pattern
  `catalog`/`ops_catalog` already use -- and `conf/defaults.yaml` only *consumes* the new
  placeholders in its three table settings, which is what the decision achieves in
  practice.

## 4. Blocked

- **`resources/job_maintenance.yml` does not pass an `ops_catalog` parameter.** `sql/04`'s
  audit-table `OPTIMIZE`/`VACUUM` statements now need `:ops_catalog` (previously
  `:catalog`, since audit moved out of the data catalog), but the job definition's
  `parameters:` block only supplies `catalog`, `landing_retention_days` and
  `vacuum_retention_hours`. `resources/*.yml` is consistently Stage 6 territory in every
  prior stage's report (Stage 0's inventory, Stage 1 decision 7, Stage 2's own note about
  `topic-key`); wiring a new job parameter is a materially different kind of change from
  this pass's SQL/config/framework edits, so it is flagged here rather than made. Until
  Stage 6 adds it, the maintenance job's two audit-table statements will fail to bind.

## 5. Decisions for the human

1. **Kafka's `control_columns` maps to setting names not yet in `operational_keys`.**
   `kafka_failure_mode` -> `failure_mode`, `kafka_max_offsets_per_trigger` ->
   `max_offsets_per_trigger`, `kafka_checkpoint_reset_id` -> `checkpoint_reset_id`, derived
   by applying D-01's own `<source_type>_<setting>` rule to the column names D-01 already
   gives. `max_offsets_per_trigger` and `checkpoint_reset_id` are the legacy Kafka module's
   own field names (grep-verified against `src/kafka_ingest/config.py`); `failure_mode` is
   the one place this departs from the legacy `on_deser_error`, but that departure is D-01's
   choice of column name, not one I made. Until Stage 3 adds these to `operational_keys`, a
   control row that sets one of the three columns fails loudly with an unknown-key error --
   the honest state for a lever with no implementation behind it yet, not silent.
   *What would change it:* Stage 3 deciding to keep `on_deser_error` instead of aligning to
   `failure_mode` -- only `control_columns`' value changes, nothing else moves.
2. **The wrong-source-type check needed a registry `framework/control.py` cannot build
   itself**, since it only ever sees one spec and must never know a literal source-type
   name. Resolved by having `runner.py` build `column -> owning source_type` from every
   `SOURCE_SPEC.control_columns` and pass it into `read_control()` as a new optional
   parameter.
   *What would change it:* if this is judged too much machinery for a three-column table
   today, the lighter alternative is a static cross-spec test (no two specs'
   `control_columns` keys collide) -- catches a spec-authoring mistake but not a live
   data-entry mistake on a row, a materially weaker guarantee than what is built.
3. **`audit_schema` / `control_schema` / `logs_schema` default to `audit` / `ingestion` /
   `logs`.** Not specified by D-06. Chosen to minimize churn: `ingestion` and `audit`
   already existed as schema names before this pass (only relocated under `{ops_catalog}`
   for audit); `logs` is new and self-explanatory.
   *What would change it:* a naming preference from whoever reviews the Terraform this
   feeds.
4. **`resources/job_maintenance.yml`'s missing `ops_catalog` parameter** -- see Blocked,
   above. Definitely Stage 6's job; flagged so it is not mistaken for finished.
5. **No file under `docs/` was touched except `RUNBOOK_CLIENT_IT.md` (D-02's own target)
   and `VERIFICATION_BACKLOG.md`.** `docs/CONFIGURATION.md`, `DESIGN.md`,
   `RUNBOOK_SUPPORT.md`, `RUNBOOK_DEVELOPER.md`, `NAVIGATION.md` and the root `README.md`
   all still describe some pre-D-01/D-06 shape. CORE assigns the full documentation
   rewrite to Stage 7, and Stage 2's own report left the same files stale for the same
   reason.
   *What would change it:* a decision that the control-table rename is confusing enough,
   sooner, to warrant a small early doc fix rather than waiting for Stage 7.
6. **Oracle and File `SOURCE_SPEC.control_columns` are untouched, still defaulting to
   empty.** Consistent with their `structural_keys`/`operational_keys` already being empty
   Stage 1 stubs (Stage 4/5's job), and DECISIONS.md's work-list file column for item 1
   names only `sources/kafka/spec.py`.
   *What would change it:* nothing until Stage 4/5 exist to declare real columns for them.

---

**Test count:** 403 passed, 34 deselected before -> **415 passed, 34 deselected** after
(`pytest -m "not spark" -q`). By function definition: 16 added, 5 removed, of which all 5
removed are 1:1 renames of `source_overrides` / `batch_id` / partitioning-era tests into
their `replay_controls` / `txn_version` / D-04 equivalents (net +11 function definitions).
The extra +1 to reach +12 collected test cases nets out two effects: one new function
(`test_the_frameworks_own_tables_all_live_under_the_ops_catalog`) is parametrized over
three environments, contributing 3 collected cases; one existing parametrized test's data
table (`RETIRED` in `tests/test_shipped_sql.py`) lost an entry, because
`max_offsets_per_trigger` is now a legitimate substring of its own replacement column name
(`kafka_max_offsets_per_trigger`) -- see that file's note on the removed entries.

**New VB entries this stage:** none. VB-15 extended, VB-16 and VB-17 updated in place to
match this stage's changes -- see section 2, above.
