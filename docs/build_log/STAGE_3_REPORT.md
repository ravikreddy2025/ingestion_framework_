# Stage 3 Report -- Kafka source

Branch `stage-3-kafka`, cut from `main` after Stage 2b (PR #4) merged.

The nine top-level Kafka modules become one package behind `SOURCE_SPEC` and `run(ctx)`.
Nothing outside `sources/kafka/` names a Kafka concept, the legacy five-layer loader is
gone, and `framework/config.py` is now the only configuration loader in the repository.

Three additions to the stage file were supplied with the request and are done: the
`security.py` split, Kafka's `operational_keys` populated (with `on_deser_error` renamed to
`failure_mode`), and `resources/job_maintenance.yml` passing `ops_catalog`.

---

## The shape of what was built

```
framework/security.py       SecretResolver + redact(). No source type named anywhere.
sources/kafka/
  spec.py       every key the framework validates against - all of them read
  config.py     the source's own frozen dataclass. VALUES validated here, KEYS by the
                framework. No PySpark import.
  security.py   profiles + resolved secrets -> broker and registry options
  registry.py   schema lookup by id, driver-side HTTP, cached per run
  wire.py       the Confluent framing as column expressions, incl. malformed triage
  reader.py     the four non-negotiable reader options; primary and both replay shapes
  landing.py    raw bytes + CloudEvents. Nothing interpreted.
  curated.py    per-writer-schema decode, quarantine split, the from_avro self-check
  tables.py     the three target tables' DDL and creation
  listener.py   audit rows from Spark's own metrics; pending work
  run.py        the guard, the run shapes, the microbatch body, the writes
entrypoints/    run_ingest.py + run_replay.py, for every source of every type
```

**The split is by SHAPE, not by sensitivity.** Resolving a secret and never logging one are
the same job for every source type. Turning a secret into an options map is shaped by the
system being connected to, so that half is Kafka's. `framework/security.py` re-exports
`redact()` from `logs.py` rather than reimplementing it: two lists of credential-bearing key
names would be two lists to keep in step, and the one that fell behind would be the one
leaking.

---

## 1. Done and verified

### The six fixes

- **(a) `checkpoint_reset_id` is single-use.** Three parts, all built:
  - The reset id is recorded on the audit row **via `rerun_id`**, not a new column - see
    decision 1 below for why. `AuditWriter.rerun_id` became a public attribute, seeded from
    configuration, exactly as `source_ref` already was.
  - `_guard_against_checkpoint_reset` gained the refusal branch, and its branch ORDER is
    now the logic: replay -> return; checkpoint exists -> return (which is what makes a
    stale id inert); reset id set -> refuse if spent, else warn and proceed; otherwise the
    original refusal. The reuse query excludes the current `run_id` explicitly, so the check
    does not depend on being called before the id reaches the audit writer - though it is,
    and `run()` says so.
  - `docs/RUNBOOK_SUPPORT.md` 5.4a is rewritten as five numbered steps with a checklist:
    record the offsets (Q13) -> confirm the id is unused (Q6d) -> set a fresh id ->
    backfill with a bounded replay -> **clear nothing**.
  - Proof: `test_a_reused_reset_id_is_refused`,
    `test_a_stale_reset_id_is_inert_once_the_checkpoint_exists_again`,
    `test_the_reuse_check_excludes_this_run_s_own_audit_rows`,
    `test_the_reset_id_is_recorded_on_the_audit_row_so_the_reuse_check_can_see_it`.
- **(b) All four reader options present.** `includeHeaders` is fixed at `"true"` and
  `include_headers` is gone from the spec entirely - a knob whose wrong setting yields NULL
  columns rather than an error should not be a knob. `max_offsets_per_trigger` and
  `min_partitions` are both `required_keys` with platform defaults, and both are rejected at
  zero or below rather than treated as "no limit". `min_partitions` is a plain integer with
  the prescribed comment; the onboarding template's step 4 tells the onboarder to confirm the
  partition count with the producing team. Proof:
  `test_all_four_required_reader_options_are_present_with_their_expected_values`, and
  `test_the_four_reader_options_are_configured_in_every_environment` over the real
  `conf/` x environment cross product.
- **(c) The curated MERGE carries an `event_date` bound**, computed from the cached batch.
  **Landing's deliberately does not**, and the call site says why at length: landing is
  partitioned by `ingest_date` - the date a row was WRITTEN - so a replayed record carries
  today's while its target twin carries the day it arrived. Any bound derived from the source
  frame would match nothing and INSERT DUPLICATES, which is worse than the full scan it would
  save. `partition_predicate="true"` is `framework/writers.merge()`'s own declared escape
  hatch for exactly this, used with the required explanation rather than a wrong bound.
  Curated is different precisely because `event_date` is a property of the RECORD.
  Proof: `test_a_curated_replay_merge_is_bounded_to_the_days_it_is_replaying`,
  `test_a_primary_curated_write_appends_partitioned_with_markers_and_no_event_date_bound`,
  `test_the_landing_merge_declares_that_no_partition_bound_is_derivable`.
- **(d) Malformed payload triage.** One `when()` chain in `wire.py` produces
  `NULL_VALUE_TOMBSTONE` / `TRUNCATED_PAYLOAD` / `BAD_MAGIC_BYTE`, each with its own
  explanatory detail, and each becomes its own quarantine group. `malformed_reason` is a
  landing column too, so landing alone says why a row is unusable. VB-18 covers the byte
  comparison - and see section 2, because it was partly answered.
- **(e) Job hardening.** `max_concurrent_runs: 1` and `queue.enabled: false` on all four job
  definitions; `max_retries: 3` and `min_retry_interval_millis: 300000` on the primary tasks;
  `retry_on_timeout` stated on every task including the three that inherit the maintenance
  anchor. See decision 4 - the replay job's concurrency drop is a real operational change.
- **(f) Smaller items.** `unpersist()` was already in a `finally` (section 3);
  `time.sleep(2)` is replaced by `listener.drain()` reading `query.recentProgress` after
  `awaitTermination()`; pending work is recorded per run as a new generic audit column.
  Cadence-vs-retention is a MUST-READ block in `docs/CONFIGURATION.md`. Two new standing
  support queries, plus two more the reset procedure needs.

### Everything else

- **`framework/audit.py`** -- `pending_work BIGINT` added to DDL, `StructType`, writer and
  `sql/02`; `RunResult` gained the matching field and `runner.py` maps it. NULL-tolerant by
  design: NULL means "the source could not tell", 0 would mean "fully caught up".
- **`sources/kafka/spec.py`** -- `operational_keys` is now
  `{failure_mode, max_offsets_per_trigger, checkpoint_reset_id}` plus the five `replay_*`
  controls. The last six are operational-ONLY, so Stage 2b's `checkpoint_reset_id`
  rejection-in-YAML behaviour is preserved through the generic mechanism rather than a
  special case. `tests/test_kafka_spec.py` walks the package and asserts every declared key
  is actually read, in both directions.
- **`on_deser_error` -> `failure_mode`, `fail`/`quarantine` -> `FAILFAST`/`QUARANTINE`.**
  The values now match the CHECK constraint on `ingest_control.kafka_failure_mode` - the
  column and the setting are one lever, and disagreeing spellings would mean support setting
  a value that looks legal and does nothing. This closes Stage 2b's decision 1.
- **`resources/job_maintenance.yml` passes `ops_catalog`**, which Stage 2b listed as
  Blocked. `sql/04`'s "NOT YET WIRED" note is removed.
- **CORE section 7 grep returns nothing.** Run from `src/kafka_ingest/`:
  ```
  $ grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' framework/ \
      | grep -v 'runner.py:.*_SOURCES'
  $ echo $?
  1
  ```
  It caught one real leak: `framework/security.py`'s docstring illustrated the split with
  "a broker's `kafka.*` keys, a database's JDBC properties". Rewritten without naming either.
- **Exit gate**, all three commands run:
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  56 files already formatted

  $ python -m pytest -m "not spark" -q
  487 passed, 36 deselected in 7.08s
  ```
  **The formatter baseline changed from "22 files would be reformatted" to zero**, and this
  is not a reformatting pass: 20 of those 22 were the legacy Kafka modules and their tests,
  which this stage deleted. The seven files this stage added that the formatter disagreed
  with were formatted individually. No pre-existing file was reformatted. `pyproject.toml`'s
  comment about deferring formatter adoption is now describing a situation that no longer
  exists - see decision 6.
- **Nineteen mutations run to prove the new tests can fail**, each restored afterwards:

  | Mutation | Test that failed |
  |---|---|
  | never refuse a reused reset id | `test_a_reused_reset_id_is_refused` |
  | reuse check stops excluding the current run | `test_the_reuse_check_excludes_this_run_s_own_audit_rows` |
  | reset id never reaches the audit row | `test_the_reset_id_is_recorded_on_the_audit_row_so_the_reuse_check_can_see_it` |
  | curated MERGE loses its `event_date` bound | `test_a_curated_replay_merge_is_bounded_to_the_days_it_is_replaying` |
  | landing replay updates matched rows | `test_a_landing_replay_merges_insert_if_absent_and_never_rewrites_the_arrival_record` |
  | cache released only on the success path | `test_the_cache_is_released_on_the_failure_path_too` |
  | `includeHeaders` switched off | `test_include_headers_is_not_configurable` (+2) |
  | `minPartitions` no longer set | `test_all_four_required_reader_options_are_present_with_their_expected_values` (+1) |
  | `maxOffsetsPerTrigger` set only when truthy (the old behaviour) | `test_the_batch_cap_is_operationally_overridable` (+1) |
  | timestamp replay silently skips to latest | `test_a_timestamp_replay_errors_rather_than_silently_skipping_to_latest` |
  | pending work returns 0 rather than NULL | `test_pending_work_is_null_whenever_it_cannot_be_known` (3 cases) |
  | final progress never drained | `test_the_final_progress_is_read_from_the_query_rather_than_waited_for` |
  | all three malformed inputs share one reason | `test_each_malformed_input_gets_its_own_reason` (+1) -- **ran on real Spark** |
  | a control column maps to a setting nothing reads | `test_every_control_column_maps_to_a_setting_that_is_operationally_overridable` (+2) |
  | `checkpoint_reset_id` becomes settable in YAML | `test_an_incident_lever_cannot_be_checked_into_yaml` (+1) |
  | the impossible reader-schema mode is accepted | `test_the_reader_schema_mode_that_would_break_the_payload_struct_does_not_exist` |
  | a non-Volume certificate path is allowed | `test_a_store_that_is_not_on_a_volume_is_refused_not_warned_about` (+1) |
  | `pending_work` dropped from the audit schema | `test_the_schema_and_the_ddl_agree_column_for_column` (+1) |
  | a scope-less secret reference reaches dbutils | `test_an_incomplete_secret_reference_is_refused_before_it_reaches_dbutils` (3 cases) |

## 2. Done but not verifiable here

- **The three malformed-payload quarantine tests are written but UNRUN.** They need the
  `spark-avro` connector, which the PySpark pip package does not bundle, so they skip. What
  DID run is the column expression underneath them - see the next item.
- **VB-18 is partly answered, and the entry says so.** A local Spark session turned out to
  be available on this machine (section 3), so
  `tests/test_kafka_registry.py::test_each_malformed_input_gets_its_own_reason` **executed
  and passed on Spark 3.5.2**, covering all three reasons plus the valid case, along with
  five other wire-format tests. That is the version DBR 16.4 LTS ships, so it is strong
  evidence - but it is open-source Spark, not Databricks Runtime, and ANSI/Photon defaults
  differ. VB-18 stays OPEN and records both the evidence and its limits.
- **VB-05 now has a second thing depending on it.** `pending_work` is
  `sum(latestOffset - endOffset)`. If `latestOffset` is absent under `availableNow` the
  column is NULL forever and Q16 quietly returns nothing. The entry is rewritten to say so,
  and to say explicitly that the fix is NOT a Kafka admin client.
- **VB-10** (the `from_avro` writer/reader self-check) is unchanged and still unrun for the
  same connector reason.
- **VB-08** (Volume as a checkpoint location) is what `_checkpoint_offsets_exist` depends on
  being answerable at all: it deliberately distinguishes "absent" from "unreadable", and a
  compute profile where the driver cannot stat the Volume turns every run into the refusal
  branch. Unchanged, still OPEN.

## 3. Not reproduced

- **`unpersist()` was already in a `finally`.** Stage-file item (f) reads as though it were
  on the success path only; the retired `pipeline.process_microbatch` already had
  `finally: landing_df.unpersist()`, and `_write_and_count` had its own. Both carried over
  unchanged, and a test now pins the behaviour rather than leaving it to survive by luck.
- **A local JVM Spark IS available on this machine.** CORE section 3 lists Spark-marked
  tests under "Never attempt", on the assumption of no JVM. `pytest -m spark` runs here:
  **6 passed, 27 skipped**. The 27 skips are all `spark-avro`-dependent. This does not change
  the gate - it stays `pytest -m "not spark"` - but it means the wire-format layer got real
  verification this stage, and a future stage should know the option exists.
- **The stage file's file list says "Edit: existing Kafka tests".** Every one of the eight
  legacy Kafka test files was replaced rather than edited, because every module they import
  ceased to exist. Behaviour coverage is carried over test-for-test; the retired
  `test_curated_writer.py` is the clearest case - it survives as `test_kafka_curated.py`
  with the same assertions and three new triage tests.
- **`batch_id` on the DATA rows was renamed `txn_version` too.** D-03 renamed only the audit
  column. Leaving the landing/curated/quarantine column called `batch_id` while the audit
  table called the same number `txn_version` is exactly the drift D-03 exists to remove, and
  nothing is deployed. Flagged rather than assumed - see decision 5.

## 4. Blocked

- Nothing. Three things deliberately NOT done, one sentence each per CORE rule 8:
  - **`docs/DESIGN.md`, `NAVIGATION.md`, `RUNBOOK_DEVELOPER.md`, `README.md` and
    `notebooks/*` still describe the retired module layout** - CORE assigns the documentation
    rewrite to Stage 7, Stage 2 and Stage 2b left the same files stale for the same reason,
    and the notebooks import modules that no longer exist so they are broken until then.
  - **No `ce_extensions` MAP, no third `reader_schema_mode`, no reset-history table** - all
    three are explicitly on the stage file's "do not build" list.
  - **Nothing was done about `sql/04`'s literal `'audit'` schema name** - it is correct for
    every shipped environment and adding a fourth bound parameter is churn, but it is the one
    place a schema name is hardcoded outside `conf/`.

## 5. Decisions for the human

1. **The reset id is recorded in `rerun_id`, not a new audit column.** The stage file offers
   both. `run_type` genuinely disambiguates: the guard returns early for every replay, so a
   reset only ever happens on a primary run, and `rerun_id` is otherwise NULL there. So
   `run_type = 'primary' AND rerun_id IS NOT NULL` means "a reset", by construction, and
   that is exactly what Q6d queries. The cost is one shared column carrying two related
   meanings; the benefit is not widening a table every source type shares for one source
   type's lever. `AuditWriter.rerun_id` became writable to make it work, mirroring
   `source_ref`.
   *What would change it:* a second source type needing to record a reset id at the same
   time as a replay id on one row - impossible today, since a reset only applies to a
   primary run.

2. **The landing MERGE passes `partition_predicate="true"`, and that is deliberate.** It is
   the one place in the codebase that uses `writers.merge()`'s escape hatch. The alternative
   - bounding on the source frame's `ingest_date` - would be actively wrong: it would match
   nothing and insert duplicates, which is worse than the full scan. The third option,
   adding an "original ingest date" column to landing so a replay could bound on it, was
   considered and rejected as a schema change to solve a problem that only affects replays.
   *What would change it:* a landing table large enough that a replay's full-partition scan
   is itself the incident - at which point the honest fix is a column, not a wrong bound.

3. **`failure_mode` uses `FAILFAST`/`QUARANTINE`, not `fail`/`quarantine`.** D-01 chose the
   column name and `sql/01` already carried a CHECK constraint on those two upper-case
   values, so the setting follows the column rather than the other way round.
   *What would change it:* nothing, unless someone prefers the lower-case spelling enough to
   change the constraint too. They must move together.

4. **The replay job's `max_concurrent_runs` drops from 3 to 1, and this has a real cost.**
   The stage file says `max_concurrent_runs: 1` and `queue.enabled: false` on every job
   template, and the reasoning it gives (two drivers, one checkpoint) applies to the primary.
   For the replay job it means two DIFFERENT sources can no longer be recovered in parallel
   from the same job - they have to be run in sequence. I applied it as written because the
   instruction is explicit and a replay is a supervised human action, but it is the one place
   in this stage where following the instruction narrows an existing operational capability.
   *What would change it:* an incident where several sources need recovering at once. The
   fix would be `max_concurrent_runs: 3` on `replay_kafka` only, with a comment that each
   run must name a different `source_key`.

5. **`batch_id` on the data rows is now `txn_version`.** See section 3. One name for one
   number across the audit table and the layer tables.
   *What would change it:* a preference for keeping the streaming vocabulary on the data
   rows, where the number really is a microbatch id for Kafka. It is `-1` for a bounded
   replay, though, which is what made `batch_id` misleading there.

6. **`pyproject.toml` still says the formatter "has NOT been run across the codebase" and
   would rewrite 20 of 22 files.** That statement is now false - the baseline is zero - but
   editing that comment is a `pyproject.toml` change with no behavioural content and I left
   it rather than fold it into this stage.
   *What would change it:* Stage 6 wiring `ruff format --check` into CI, at which point the
   comment should be replaced with "the formatter is enforced".

7. **`framework/security.py` re-exports `redact()` rather than owning it.** The alternative -
   moving the implementation out of `logs.py` and having `logs.py` import it back - is
   arguably the better dependency direction (security owns redaction, logging uses it) but
   touches a module with thirteen green tests to move code that is already correct.
   *Recommendation:* leave it; revisit if a third caller appears.

---

**Test count:** 415 passed, 34 deselected before -> **487 passed, 36 deselected** after
(`pytest -m "not spark" -q`). Eight legacy Kafka test files (163 cases) were removed with
the modules they tested; ten files replace them. Net +72 in the fast suite, and the
spark-marked count rises from 34 to 36 - the two additions are the malformed-reason column
tests, which are the ones that actually ran.

**New VB entries this stage:** VB-18 (the magic-byte comparison on the target DBR). VB-05
rewritten to cover `pending_work` as well as the offset columns, and to rule out a Kafka
admin client as the fix.
