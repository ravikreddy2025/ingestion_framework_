# Stage 2 Report -- Shared tables: control, state, audit

Branch `stage-2-tables`, cut from `main` after Stage 1 (PR #2) merged.

Five framework modules created, the four `RunContext` slots Stage 1 left as `None` filled
in, and both provisioning SQL files rewritten. Nothing is deployed, so there is no
migration, no backfill and no compatibility view: the old names simply go.

---

## The shape of what was built

```
framework/control.py   layer 4 -> an override dict. Missing row OK, duplicates fatal,
                       every override validated against the source's SOURCE_SPEC.
framework/state.py     watermark + run_sequence, key/value. Writes MANDATORY, and raise.
framework/audit.py     one table for every source type. Writes NEVER raise.
framework/tables.py    target names from a pattern, name validation, CREATE TABLE.
framework/writers.py   append with txn markers; MERGE that cannot forget its predicate.
```

Two of these have deliberately opposite contracts, and both module docstrings say so at the
top, because the next person to read them side by side will otherwise "make them
consistent":

| | audit | state |
|---|---|---|
| A failed write | logged loudly, swallowed | **raises** |
| Consequence if it were the other way | a Delta hiccup on the audit table takes ingestion down | a watermark that silently fails to advance re-reads a window; one that silently advances skips one |
| Never do this | derive a watermark from it | wrap it in a retry, or in a `try` |

---

## The one naming convention added

**A source's target table for layer L is the setting `<L>_table`.** `landing_table`,
`curated_table`, `quarantine_table`. The layers come from `SOURCE_SPEC.layers`, so
`framework/tables.py` resolves targets for any source type without knowing one exists, and
`framework/config.py` accepts those keys by the same rule (`layer_table_keys`).

This is the mechanism CORE section 6 asks for -- "target naming ... derived from source
metadata, as Oracle's is. That derivation lives in `framework/tables.py`, driven by a
pattern in `conf/defaults/oracle.yaml`". `tables.render(pattern, tokens, where)` is the
derivation primitive: the SOURCE computes its own tokens (`source_schema`, `source_table`,
`topic_table`) and hands them over, so the pattern stays in configuration and the framework
never learns what a schema is. See decision 4 for the one part of this that Stage 3/4 still
has to solve.

---

## 1. Done and verified

- **`framework/control.py`** -- the three rules, each with a test.
  - missing row -> `{}` (`test_no_row_means_no_overrides`); missing TABLE -> `{}` with a
    warning (`test_no_control_table_at_all_still_runs`)
  - duplicate rows -> `ConfigError` naming the `source_key`
  - unknown key in `source_overrides` -> the SAME error a YAML typo produces, asserted by
    raising both and comparing the text
    (`test_the_unknown_key_error_reads_the_same_as_a_yaml_typo`)
  - a structural key in `source_overrides` is carried through and then IGNORED by
    `config.apply_overrides`, asserted end to end
    (`test_a_structural_key_is_carried_through_and_ignored_later`)
  Proof: 15 tests in `tests/test_framework_control.py`.
- **`framework/state.py`** -- `read_state` / `write_state` / `next_run_sequence` exactly as
  the stage file names them, as methods of a `StateStore` bound to (session, table, run_id).
  No PySpark import: the session is an argument, the row schema is a DDL string, and
  `delta.tables` is imported inside `write_state`. Proof: 14 tests in
  `tests/test_framework_state.py`, including a failing MERGE propagating.
- **`framework/audit.py`** -- one shared table. `source_type`, `source_key`, `source_ref`,
  `position_start`, `position_end`, `source_detail` added; `topic_key`/`topic` and the four
  Kafka-specific columns (`starting_offsets`, `ending_offsets`, `writer_schema_ids`,
  `reader_schema_id`, `checkpoint_path`, `spark_progress_json`) gone, folded into
  `source_detail` JSON. Three-way agreement now four-way: row keys == `AUDIT_SCHEMA` ==
  `AUDIT_DDL_COLUMNS` == `sql/02_layer_tables.sql`, all asserted. Audit-never-raises tested
  with a deliberately failing `createDataFrame`. Proof: 19 tests in
  `tests/test_framework_audit.py`.
- **`framework/tables.py`** -- non-3-tier name is an error; a name legal in the source
  system and illegal in Unity Catalog is an error at configuration load, proven through the
  runner (`test_an_illegal_target_name_fails_before_anything_connects`, which asserts the
  source was never dispatched); `PARTITIONED BY` **or** `CLUSTER BY`, never both. Proof: 22
  tests in `tests/test_framework_tables.py`.
- **`framework/writers.py`** -- `txnAppId`/`txnVersion` on appends only, both markers or
  neither, `txn_version=0` still gets them and `-1` does not. `merge()` takes
  `partition_predicate` as a required positional argument; asserted twice, once as a
  `TypeError` and once against `inspect.signature`, so a default added later cannot make the
  first assertion quietly stop happening. Quarantine split is total by construction
  (`coalesce(cond, false)`), so a NULL predicate cannot drop a row from both sides. Proof:
  21 tests in `tests/test_framework_writers.py`.
- **`framework/runner.py`** -- reads the control table, validates every target name, creates
  the framework's own two tables and allocates a run sequence, all before dispatch. Proof:
  `tests/test_framework_runner.py` is 18 tests -> 26, and all eighteen Stage 1 wrote are
  still green (their call sites changed from a string stand-in to conftest's `FakeSpark`,
  because the runner now writes before it dispatches).
- **`sql/01_operational_config.sql`** -- `ingest_control` (CORE 5.2) and `ingest_state`
  (CORE 5.3), with grants split in opposite directions: support edits control and reads
  state, the job reads control and writes state.
- **`sql/02_layer_tables.sql`** -- the audit table is the shared one, and its column list is
  compared against `framework/audit.py` in CI.
- **CORE section 7 grep returns nothing.** Run from `src/kafka_ingest/`:
  ```
  $ grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' framework/ \
      | grep -v 'runner.py:.*_SOURCES'
  $ echo $?
  1
  ```
- **Exit gate**, all three commands actually run, in the project `.venv` (Python 3.12.10,
  pyspark 3.5.2):
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  22 files would be reformatted, 31 files already formatted

  $ python -m pytest -m "not spark" -q
  364 passed, 34 deselected in 8.60s
  ```
  The formatter baseline is **unchanged**: 22 files at Stage 0, 22 files at Stage 1, 22
  files now. Every file this stage created is formatter-clean, and the four previously-clean
  files it edited (`framework/config.py`, `framework/runner.py`, `tests/conftest.py`,
  `tests/test_framework_runner.py`) were put back clean.
- **Eleven mutations run to prove the new tests can fail**, each restored afterwards:

  | Mutation | Test that failed |
  |---|---|
  | give `merge()`'s `partition_predicate` a default | `test_merge_cannot_be_called_without_a_partition_predicate` |
  | narrow `audit.emit`'s `except` to a type that cannot fire | `test_a_failed_audit_write_never_fails_the_run` |
  | wrap `state.write_state`'s MERGE in a `try` | `test_a_failed_write_raises` |
  | drop the `validate_override_keys` call in `control.py` | `test_an_unknown_key_in_source_overrides_is_rejected` |
  | stop raising on duplicate control rows | `test_duplicate_rows_raise_and_name_the_source_key` |
  | allow PARTITIONED BY and CLUSTER BY together | `test_partitioning_and_clustering_together_is_rejected` |
  | accept a two-part table name | `test_a_name_that_is_not_three_part_is_rejected` |
  | hardcode `run_sequence=0` in the runner | `test_a_run_sequence_is_allocated_before_the_source_runs` |
  | drop the `coalesce` from the quarantine split | `test_the_split_is_total_so_no_row_is_dropped_by_a_null` |
  | remove `source_ref` from `AUDIT_SCHEMA` | `test_the_schema_and_the_ddl_agree_column_for_column` |
  | drop `tables.validate_targets` from the runner | `test_an_illegal_target_name_fails_before_anything_connects` |

## 2. Done but not verifiable here

- **The state MERGE actually upserting**, and the one-run-per-`source_key` assumption
  `next_run_sequence` rests on -- **VB-15**. This is the most damaging thing this stage
  introduces: if the MERGE silently did not update, every run would read the same value,
  take the same Delta `txnVersion`, and every append after the first would be dropped as a
  duplicate. That failure looks exactly like a source with no new data.
- **The service principal being able to create the audit and state tables** -- **VB-16**.
  The runner issues `CREATE TABLE IF NOT EXISTS` for both before dispatch. If the grant is
  missing the run fails loudly at start-up, which is why this is a backlog entry rather than
  something designed around.
- **Both SQL files executing as written** -- **VB-17**. Named `CONSTRAINT ... CHECK` clauses
  inside `CREATE TABLE`, `NOT NULL` alongside them, and placeholder rendering. A rejected
  statement is the good case; a CHECK that is accepted and does not enforce is the bad one.
- **Delta MERGE schema evolution** -- still **VB-09**, unchanged. `writers.merge` ports
  Stage 0's two-mechanism approach (`withSchemaEvolution()` where the runtime has it, the
  session flag scoped to one operation otherwise) and both branches are tested against
  stand-ins; which branch a real runtime takes is not knowable here.

## 3. Not reproduced

- **The stage file says `framework/control.py` validates `source_overrides` against the
  source's `operational_keys`.** It validates against `known_keys(spec)` -- operational plus
  structural plus the framework's own -- because the very next line of the stage file says
  "Structural fields must be ignored if present, not rejected". Validating against
  `operational_keys` alone would reject them instead. The behaviour asked for is the one
  implemented: unknown -> error, structural -> ignored with a log line, and both are tested.
- **The stage file lists `sql/03_support_queries.sql` nowhere, but the rename made it
  stale.** It queried `ingestion_topic_control` and `stream_audit` by `topic_key`, all
  three of which this stage renamed. Initially left alone under CORE rule 8 and flagged
  here; **now rewritten** - see the addendum. `sql/02` and `sql/04` turned out to be stale
  too, which nothing had noticed.
- **`--control-table` was NOT added to `run_ingest.py`,** which Stage 1's report predicted
  ("Stage 2 adds one argument and one call"). The control table's name is now a
  configuration setting like the other two framework tables, so there is one mechanism -
  `conf/` - rather than two. See decision 2.
- **The CORE section 7 grep is case-sensitive, and every remaining mention is capitalised
  prose.** `framework/` contains eight lines naming a source type in a docstring or comment
  ("a Kafka `foreachBatch` body", "an Oracle watermark"), five of them written by Stage 1 and
  three by this stage. None is code; the grep passes; the convention Stage 1 set - source
  types appear only as illustration, and only capitalised - is what this stage followed.
  Flagged rather than changed, because Stage 6 wires this grep into CI and should decide
  deliberately whether to make it case-insensitive.

## 4. Blocked

- Nothing. Two things deliberately NOT built, one sentence each per CORE rule 8:
  - **GRANT and CREATE SCHEMA from Python.** The stage file says tables.py does "DDL and
    schema creation with grants"; a job that can GRANT is a job that can grant itself more,
    and the principal names are environment-specific, so both stayed in `sql/`. See
    decision 3.
  - **A `run_sequence` column on the audit table.** CORE 5.4 enumerates exactly six columns
    to add and this is not one of them, so `batch_id` carries the microbatch id for
    streaming sources and the run sequence for batch ones, said on the column. See
    decision 5.

## 5. Decisions for the human

1. **The control table's named columns are the levers every source type shares; anything
   type-specific goes in `source_overrides` JSON.** `control.py` holds one explicit
   column -> setting map (`enabled`, `failure_mode`, `batch_limit`, `checkpoint_reset_id`,
   `replay_rerun_id` -> `rerun_id`) and ignores every other column, so an admin adding a
   column of their own does not break every run. The consequence: a control row setting
   `failure_mode` for a source whose spec does not declare it fails with an unknown-key
   error. That is the intended answer -- but it means Stages 3-5 must declare these keys in
   each `SOURCE_SPEC` for the corresponding column to work.
   *What would change it:* deciding that `failure_mode` and `batch_limit` are universal
   enough to be framework-owned keys, like `enabled`. I did not, because
   `checkpoint_reset_id` demonstrably is not (it is meaningless for a database source) and
   splitting the five columns across two rules is harder to explain than one rule.

2. **`control_table` is a configuration setting, not a job parameter.** All three framework
   tables are now named in `conf/defaults.yaml` -- `audit_table` in the data catalog,
   `state_table` and `control_table` in `{ops_catalog}` -- so there is one place a reader
   looks. The runner merges layers 1-3 once to find `control_table`, reads the control
   table, then resolves the full five layers with the overrides applied. Two passes over a
   few small YAML files buys one mechanism instead of two.
   `ops_catalog` was added to each environment's `vars:` and a test asserts it matches
   `databricks.yml`, exactly as `data_catalog` already was. **The duplication is gone** -
   `databricks.yml`'s `control_table` variable and the `control-table` job parameter were
   removed rather than left to Stage 6; see the addendum.
   *What would change it:* nothing outstanding. The escape hatch an incident might want -
   pointing one run at a different table - is still there as an optional argument.

3. **The framework creates tables; it does not GRANT, and it does not CREATE SCHEMA.**
   `sql/01` and `sql/02` own both. The reasoning is in `framework/tables.py`'s docstring: a
   job that can grant is a job that can grant itself more, and the principal names
   (`ingestion-support`, `sp-kafka-ingestion`) are environment-specific strings that have no
   place in source-agnostic code.
   *What would change it:* a deployment model where nobody runs provisioning SQL at all. In
   that case `ensure_table` would need a `grants:` configuration block, and VB-16 becomes a
   blocker rather than a check.

4. **A target-name pattern may now name a value only the source knows, via a new
   `SourceSpec.target_tokens` field.** This adds a seventh field to the CORE 4.1 skeleton,
   which is the one place this stage departs from "follow these skeletons exactly", and it
   is the decision most worth a second opinion. The problem it solves is real and was
   blocking: `config.py` treats an unresolved `{placeholder}` as a hard error, and
   `conf/defaults/kafka.yaml` has always named its targets
   `{catalog}.<layer>.{topic_table}` -- so no shipped Kafka file could resolve through the
   framework loader at all.
   *Two alternatives rejected:* a second placeholder syntax for source-derived tokens (two
   things for a new joiner to learn instead of one); and having the runner ask the source
   for its tokens before validating (which puts a source's own vocabulary into
   `framework/`, and is what CORE section 7 exists to prevent).
   *The cost, stated plainly:* target-name validation now happens at two moments rather
   than one -- the runner checks every name configuration fully resolved, and the source
   checks the rest via `tables.target()` at the top of `run()`. Both are before any read.
   *What would change it:* if Stage 3 finds Kafka needs no token after all -- e.g. the
   topic-to-identifier mapping moves into configuration as an explicit `table_name:` -- the
   field would have one user (Oracle) and might not earn its keep.

5. **`batch_id` carries the Delta `txnVersion` for every source type, whatever produced
   it.** Streaming sources put their microbatch id there; batch sources put their
   `run_sequence`; a run with neither records `-1`. One column, stated on the column, the
   same trick CORE 5.1 applies to `position_start`/`position_end`. The alternative was a
   separate `run_sequence` column, which CORE 5.4 does not list among the six to add.
   *What would change it:* triage queries that need to distinguish the two without joining
   to `ingest_state`.

6. **The run sequence is allocated on every run, for every source type,** including Kafka,
   which will never read it. Conditioning it on source type is exactly the branch CORE
   section 7 forbids, and conditioning it on configuration would let a misconfigured
   environment silently lose idempotency protection. The cost is one MERGE per run on a
   table with a handful of rows per source.
   *What would change it:* evidence that the extra Delta commit is material at the run rate
   the platform actually schedules.

7. **`audit_table`, `state_table`, `control_table`, `table_properties` and `rerun_id` are
   now framework-owned keys** in `framework/config.py`, joining `domain` and `enabled`.
   This is an edit to a Stage 1 file that the stage file did not list. The justification is
   that `conf/defaults.yaml` sets these for every source of every type, so without it every
   `SOURCE_SPEC` would have to declare keys the framework reads and no source does -- and a
   source author could misspell one. `rerun_id` is operational-only for the standard reason:
   a replay id checked into Git re-applies on every future deploy.
   *What would change it:* nothing I can see; flagged only because it widens a list Stage 1
   deliberately kept short.

8. **The audit table is named `ingest_audit` and lives in the DATA catalog**
   (`{catalog}.audit.ingest_audit`), while control and state live in `{ops_catalog}`. The
   split is deliberate: audit sits next to the data whose arrival it records and is queried
   by the same people, control and state are operational and have a different blast radius.
   *What would change it:* an access model where the data catalog's readers must not see
   run metadata.

---

**Test count:** 263 passed, 34 deselected before -> **403 passed, 34 deselected** after
(`pytest -m "not spark" -q`). 140 tests added net; one legacy parametrised case removed
(`test_provisioning_sql_matches_the_python_ddl[audit]`), because the audit table it compared
moved to `framework/audit.py` and is now compared there instead.

**New VB entries this stage:** VB-15 (does the state MERGE upsert, and is one run per
`source_key` true), VB-16 (can the service principal create the audit and state tables),
VB-17 (do both SQL files execute as written).

---

## Addendum -- three of the items above, closed before the PR

The human read the five lists and asked for three of them to be fixed rather than carried
into a later stage. All three are now done on this branch. The sections above are amended
in place so a later session does not act on advice this addendum has already superseded.

### 1. The control table is named once (decision 2)

`databricks.yml`'s `control_table` variable and the `control-table` job parameter in both
job definitions are removed. They duplicated a name `conf/defaults.yaml` derives from
`vars.ops_catalog` -- and the copy was still pointing at `ingestion_topic_control`, a table
this branch renamed, which nothing would have caught.

The legacy Kafka loader falls back to the configured name when given no argument, so the
legacy entrypoints keep working with one fewer parameter. `--control-table` survives as an
OPTIONAL argument, which is the incident escape hatch decision 2 said an incident might
want. Two tests guard the shape rather than the symptom: the three framework tables must
resolve to legal three-part names in every environment, and `databricks.yml` must not
declare a control table again.

### 2. Target patterns with a source-derived token (decision 4)

`SourceSpec.target_tokens` implemented, `sources/kafka/spec.py` declares `topic_table`
because `conf/defaults/kafka.yaml` already uses it, and `framework/tables.py` gains
`target(cfg, layer, tokens)` which renders and then validates. See decision 4 above for
the reasoning, the rejected alternatives and the cost.

Note for Stage 3: this removes the blocker, it does not finish the job. The Kafka spec's
key sets are still empty, so a shipped Kafka file still does not resolve through the
framework loader -- `topic`, `cluster`, `registry` and the rest are still unknown keys.
What changed is that the target patterns are no longer the reason.

### 3. The support runbook, and a test so it cannot rot again

`sql/03_support_queries.sql` rewritten against the three tables as they now are, and
reorganised around what each is FOR -- audit is evidence and best-effort, control is what
support may change, state is where each source actually got to and is read-only for
support. It gained the two questions the old file could not answer (**which sources have
NOT run**, and **has the watermark moved**), and its source-specific queries are marked as
such, because one audit table now serves three source types.

`tests/test_shipped_sql.py` is the guard, and it earned itself immediately: on its first
run it found two more stragglers nobody had noticed -- `sql/02` still described quarantine
in terms of `on_deser_error`, and `sql/04` was still VACUUMing `audit.stream_audit`
nightly, which would have failed every night on a table that does not exist. It checks
four things: no `.sql` file references a retired identifier; every table the runbook
queries is one the provisioning scripts create; every column the runbook WRITES exists on
the control table; and the runbook never shows anyone how to UPDATE `ingest_state`, because
support has SELECT on it and a hand-moved watermark is a silent data-loss incident.

Reads are deliberately not checked -- they range over joins, aliases and JSON paths, and a
regex that tried would be wrong more often than the file is.

### Gate after the addendum

```
$ python -m ruff check src tests
All checks passed!

$ python -m ruff format --check src tests
22 files would be reformatted, 32 files already formatted

$ python -m pytest -m "not spark" -q
403 passed, 34 deselected
```

Formatter baseline still 22, unchanged since Stage 0. **Ten further mutations run**, each
restored afterwards:

| Mutation | Test that failed |
|---|---|
| drop the deferred-token branch in `_substitute` | `test_a_declared_token_survives_configuration_load` |
| defer every token instead of the declared ones | `test_an_undeclared_token_is_still_a_hard_error` |
| have `tables.target()` render without validating | `test_a_rendered_name_that_is_illegal_in_unity_catalog_is_rejected` |
| stop skipping deferred names in `validate_targets` | `test_the_runner_skips_a_deferred_name_and_says_so` |
| empty the Kafka spec's `target_tokens` | `test_the_shipped_kafka_spec_declares_the_token_its_defaults_use` |
| drop the legacy loader's control-table fallback | `test_the_control_table_is_read_from_configuration_when_no_argument_is_given` |
| reintroduce `topic_key` into `sql/03` | `test_no_sql_file_references_a_retired_identifier[topic_key-source_key]` |
| add an `UPDATE` on `ingest_state` to the runbook | `test_support_never_updates_the_state_table` |
| set a non-existent control column in an `UPDATE` | `test_the_support_updates_only_set_columns_the_control_table_has` |
| re-add `control_table` to `databricks.yml` | `test_the_bundle_does_not_also_name_the_control_table` |

### Still open from the five lists

Decisions 1, 3, 5, 6, 7 and 8 stand as written, and VB-15 to VB-17 are unchanged. The one
remaining known-stale artefact this branch leaves behind is the DOCUMENTATION -- `docs/`
still describes the Kafka-only framework throughout, which CORE assigns to Stage 7.
