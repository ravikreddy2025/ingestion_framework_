# Stage 4 Report -- Oracle source

Branch `stage-4-oracle`, cut from `main` after Stage 3 (PR #5) merged. **One section per
sub-step**, written as each sub-step's exit gate goes green. Sub-steps 4b (JDBC read), 4c
(watermark lifecycle) and 4d (operationalise) are not started.

---

# Sub-step 4a -- config, spec and query builder

Pure Python, and all of it ran. Nothing in this sub-step needs a driver, a database or a
cluster, which is why the stage file puts most of Oracle's correctness here.

## What was built

```
sources/oracle/
  spec.py     every key the framework validates against - all of them read in 4a
  config.py   the source's own frozen dataclass. VALUES validated here, KEYS by the
              framework. No PySpark import.
  query.py    one function. The extraction query, including the closed interval.
conf/defaults/oracle.yaml   the landing-name pattern, fetch_size, num_partitions, mode
sql/01_operational_config.sql   the two oracle_* control columns (D-01)
```

`run.py` is untouched and still raises `NotImplementedError`; 4b writes it.

## 1. Done and verified

- **`sources/oracle/spec.py`.** `source_table` is the only key a source file must carry;
  `incremental_mode`, `fetch_size` and `num_partitions` are also `required_keys` so that
  DELETING a platform default is an error rather than a silent fallback -- the same
  reasoning Stage 3 applied to `min_partitions`. `sql_query` is declared mutually exclusive
  with each of `columns` / `filter_column` / `filter_criteria` / `dynamic_date_filter` as
  four PAIRS rather than one group of five, because those four are legal together and only
  their combination with `sql_query` is not. Proof:
  `test_a_hand_written_query_excludes_every_generated_clause`,
  `test_the_generated_clauses_are_not_exclusive_of_each_other`,
  `test_every_declared_key_is_actually_read_by_this_package` (15 keys, both directions).
- **Only tuning is operationally overridable.** `operational_keys` is exactly
  `{fetch_size, num_partitions}` -- neither changes WHICH rows are extracted. Everything
  that decides what is extracted (`source_schema`, `source_table`, `filter_criteria`,
  `merge_keys`, `cursor_column`, `sql_query`) is structural, so a control-table override of
  it is ignored. For `filter_criteria` that is a security boundary, not only a correctness
  one: it is the one SQL fragment in the configuration. Proof:
  `test_what_is_extracted_is_never_operationally_overridable` (7 keys),
  `test_a_control_row_cannot_change_what_is_extracted`.
- **`filter_criteria` is validated against a conservative allowlist.** Statement separators,
  both comment forms, and 19 DML/DDL keywords matched as whole words -- so `LAST_UPDATE_DT`
  and `CREATE_DT` are unaffected while `IN (SELECT ...)` and `IN ('A') UNION SELECT` are
  refused. `sql_query` is checked by SHAPE instead (must open `SELECT` or `WITH`, no
  separator, no comment marker), because the keyword list would reject its own `SELECT`.
  Proof: `test_a_filter_fragment_that_is_more_than_a_predicate_is_refused` (6 cases),
  `test_an_ordinary_predicate_is_accepted` (5 cases),
  `test_a_column_called_update_dt_is_not_mistaken_for_a_keyword`,
  `test_sql_query_must_be_a_read`, `test_sql_query_may_not_hide_a_second_statement`.
- **The closed interval, built and asserted.** `build_query()` emits
  `cursor > :last AND cursor <= :high`; the upper bound is a REQUIRED argument, not a
  defaulted one, so a caller that has not captured the high-water mark gets an error rather
  than an open-ended read. The lower bound's operator follows `merge_keys`: `>=` with them
  (tie-safe, the MERGE de-duplicates), `>` without. Proof:
  `test_a_cursor_run_always_has_an_upper_bound`,
  `test_a_cursor_run_without_an_upper_bound_is_refused`,
  `test_the_first_cursor_run_reads_everything_up_to_the_upper_bound`,
  `test_merge_keys_make_the_lower_bound_inclusive`, and
  **`test_waiving_merge_keys_excludes_the_boundary_and_that_loses_ties`** -- the documented
  loss, asserted as a known property. (The lifecycle around it -- capture, write, commit,
  then advance -- is 4c and is NOT built.)
- **`merge_keys` absence is refused, not inferred.** `merge_keys: []` is the waiver and it
  is accepted; no `merge_keys` at all under `incremental_mode: cursor` is a config error
  naming both options. CORE section 10's "require unless explicitly waived", made
  mechanical. Proof: `test_merge_keys_must_be_a_decision_not_a_default`,
  `test_the_waiver_is_an_empty_list_and_it_is_accepted`.
- **Watermarks become SQL in exactly one function.** `_literal()` accepts a plain number or
  an ISO-8601 timestamp and refuses everything else, including a hand-edited `ingest_state`
  value -- "only this framework writes that table" is a claim about a table support can
  UPDATE, so it is checked rather than assumed. Timestamps render with an explicit Oracle
  format model so the comparison does not depend on the session's `NLS_DATE_FORMAT`. Proof:
  `test_a_timestamp_watermark_is_rendered_with_an_explicit_format_model` (3 shapes),
  `test_a_watermark_that_is_not_a_timestamp_never_reaches_the_query` (5 cases),
  `test_a_number_cursor_refuses_anything_that_is_not_a_number` (4 cases).
- **Target naming, and the case rule in one place.** `{catalog}.oracle_{source_schema}.
  {source_table}` in `conf/defaults/oracle.yaml`, with both tokens declared in
  `target_tokens` and filled by `config.target_tokens()` -- which lower-cases for Unity
  Catalog while `source_ref` upper-cases for Oracle. A name legal in Oracle and illegal in
  UC (`CLAIM$HEADER`) fails at config load, naming the SETTING rather than the rendered
  pattern. Proof: `test_the_landing_target_is_derived_from_the_oracle_name`,
  `test_the_configured_case_does_not_change_the_target`,
  `test_a_name_legal_in_oracle_and_illegal_in_unity_catalog_fails_at_config_load`.
- **`numPartitions` without a `partitionColumn` is a config error.** Spark cannot split a
  read without a column and bounds; given only a count it issues one query on one executor,
  so this fails as slowness at scale rather than as an error. Proof:
  `test_a_partition_count_without_a_partition_column_is_refused`.
- **The query-builder cross product.** Every combination of base (table / column list /
  hand-written query) x filter (none / static / dynamic) x incremental (none / merge /
  append), asserting the fragments, the predicate ORDER and that there is exactly one
  `WHERE`. The six `sql_query` + generated-clause cells are skipped because the spec rejects
  them as configurations at all, and `test_oracle_spec.py` is where that is asserted. Proof:
  `test_every_combination_of_the_four_parts_builds` (27 cases, 6 skipped),
  `test_the_filters_are_appended_in_the_documented_order`.
- **The control-table chain, end to end.** `oracle_fetch_size` and `oracle_num_partitions`
  added to `sql/01` with a positivity CHECK, declared in `SOURCE_SPEC.control_columns`, and
  carried through `framework/control.py` -> `framework/config.py` -> `OracleConfig` without
  `framework/` learning the string `oracle_`. Proof:
  `test_a_control_row_setting_both_columns_resolves_end_to_end`,
  `test_the_control_columns_are_the_two_levers_support_has`.
- **CORE section 7 grep returns nothing:**
  ```
  $ grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
      | grep -v 'runner.py:.*_SOURCES'
  $ echo $?
  1
  ```
- **Exit gate, all three commands run:**
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  61 files already formatted

  $ python -m pytest -m "not spark" -q
  630 passed, 6 skipped, 36 deselected in 9.47s
  ```
- **Nine mutations run to prove the new tests can fail**, each restored afterwards:

  | Mutation | Result |
  |---|---|
  | cursor predicate loses its upper bound | 31 failed |
  | boundary operator always `>` regardless of merge keys | 10 failed |
  | missing high-water mark no longer refused | 1 failed |
  | absent `merge_keys` silently accepted | 1 failed |
  | `filter_criteria` allowlist removed | 6 failed |
  | `num_partitions` / `partition_column` check removed | 1 failed |
  | target tokens no longer lower-cased | 2 failed |
  | UC-identifier check removed from `target_tokens()` | **0 failed** -- see below |
  | UC-identifier check removed, after the test was tightened | 1 failed |

  The eighth mutation is the one worth reading: removing the check did not fail anything,
  because `framework/tables.py` rejects the assembled name anyway. The source-level check
  earns its place only by naming the SETTING at fault (`source_table`) where the framework
  can name only the pattern (`landing_table`) -- so the test now asserts that, and the
  mutation fails. The check would otherwise have been redundant code with a redundant test.

## 2. Done but not verifiable here

- **VB-19 (new)** -- the rendered `TO_TIMESTAMP(...)` literal and its format model, and how
  it compares against a `DATE` cursor column. The rendering is asserted; that Oracle accepts
  and compares it as intended is not checkable here.
- **VB-20 (new)** -- `SYSTIMESTAMP` as the anchor for `dynamic_date_filter`. Whose clock and
  whose time zone that is decides how much history each run actually reads.
- **VB-01** is unchanged and still governs 4b: whether the parenthesised subquery in
  `dbtable` is required alongside `partitionColumn`. 4a produces a query string; nothing in
  4a decides which JDBC option carries it.
- **VB-03 / VB-04** now have a second thing depending on them: the cursor column's Oracle
  type decides whether `_literal()`'s `TO_TIMESTAMP` is the right function at all. Recorded
  in VB-19's "If it fails" rather than by editing those entries.

## 3. Not reproduced

- **`_require_safe_identifier` does not exist.** The stage file says `filter_criteria` should
  be validated "the way `_require_safe_identifier` already validates identifiers". That
  function lived in the legacy `src/kafka_ingest/config.py`, which Stage 3 deleted; the only
  survivors are `framework/tables.py`'s `validate_name` (three-part UC names) and
  `sources/kafka/config.py`'s private `_SAFE_ID`. Per CORE rule 7 the symbol was not
  substituted with the nearest similar thing: `sources/oracle/config.py` grew its own
  `_ORACLE_IDENTIFIER` (Oracle's rules, including `$` and `#`) and `_UC_IDENTIFIER`, because
  the two alphabets genuinely differ and that difference is the bug being caught.
- **`source_schema` ships with no default, so it is effectively required.** The stage file
  calls it "optional; default in `conf/defaults/oracle.yaml`". There is no honest
  platform-wide schema to put there -- one would silently apply to every future source that
  forgot its own -- so the file ships without it and `config.py` raises naming the key and
  both places it can be set. If a client turns out to extract from exactly one schema, one
  line in `conf/defaults/oracle.yaml` makes the stage file's wording true with no code
  change.
- **No `conf/sources/oracle_*.yaml` and no `conf/jdbc.yaml` profile yet.** Both were
  deliberately left for 4b: `tests/test_shipped_config.py` resolves every non-underscore
  file in `conf/sources/` against `kafka.SOURCE_SPEC`, so a shipped Oracle source breaks 22
  tests until that file learns to partition by `source_type`. A `jdbc.yaml` profile is worse
  than merely early -- every field in it (host, port, service name, secret key names) is
  read by `framework/security.py` and the URL builder, which 4b writes. Shipping a profile
  now would be shipping keys nothing reads. 4a validates that `jdbc_ref` NAMES an existing
  profile and stores the raw mapping; 4b types it.
- **One existing test had to change.**
  `test_framework_control.py::test_a_spec_with_no_control_columns_declared_defaults_to_empty`
  asserted the dataclass default by pointing at the Oracle stub, which now declares two
  columns. It builds its own `SourceSpec` instead, so Stage 5 will not have to touch it
  again.

## 4. Blocked

- Nothing. Two things deliberately not done, one sentence each per CORE rule 8:
  - **No `sources/oracle/types.py`, no JDBC options, no `run()`** -- all three are 4b, and
    writing them now would mean declaring spec keys before the code that reads them exists.
  - **No `docs/CONFIGURATION.md` rows and no `_TEMPLATE_oracle.yaml`** -- both are 4d, and
    the MUST-READ row about `merge_keys` needs 4c's lifecycle to be true before it is
    written down.

## 5. Decisions for the human

**Answered in review, and now settled in `DECISIONS.md` D-09:** 1 (`num_partitions` stays
1), 3 (a missing `merge_keys` fails the load), plus confirmation that `source_schema` is
configured per table and that the target is `oracle_<source_schema>.<source_table>`. D-09
also adds NEW work to 4b: `incremental_mode` becomes operationally overridable so support
can switch a source between full and delta without a deploy, and a replay can bound the
cursor interval explicitly. The five entries below are left as written -- they are the
record of what was decided and why.


1. **`num_partitions` ships as 1, not 8.** The stage file's example shows 8. A platform-wide
   default above 1 would make every source that has not yet chosen a `partition_column` a
   config error on day one, and choosing a partition column needs the value distribution
   only the source team knows -- the wrong bounds produce skew, not an error. A serial read
   is the safe wrong answer.
   *What would change it:* a decision that every Oracle source must declare a partition
   column at onboarding, in which case the default becomes 8 and the missing-column error
   becomes the enforcement.
2. **`incremental_mode` defaults to `full`.** A source that forgets to declare its mode
   re-extracts everything, which is visible in the row count and the run time. The
   alternative default, `cursor`, needs a cursor column nobody chose and fails by missing
   rows.
   *What would change it:* nothing I can see; recorded because it is a default with a real
   cost on a large table.
3. **`merge_keys: []` is the waiver, and its absence is an error.** The stage file asks for a
   startup WARN naming the risk. A warning in a driver log is not a decision anybody made,
   so absence is refused at config load and the WARN in 4c will cover the waived case each
   run. The result is that every Oracle cursor source in Git has an explicit answer.
   *What would change it:* onboarding friction, if a team has many keyless tables -- but the
   fix is then to write `merge_keys: []` in the template, which is still an explicit answer.
4. **Watermarks are rendered as SQL literals, not bind variables.** The stage file's 4c
   sketch uses `:last_watermark`. Spark's JDBC source has no way to bind parameters to a
   `dbtable` subquery, so a bind would arrive at Oracle as text. `_literal()` is therefore
   the safety boundary and refuses anything that is not a number or an ISO-8601 timestamp.
   *What would change it:* nothing available in this connector.
5. **`sql_query` with nothing to append is passed through verbatim; with a predicate it
   becomes `SELECT * FROM (<query>) src`.** Two shapes rather than one. The alternative --
   always wrapping -- is more uniform but means the audit row never shows what the source
   file actually says, and that comparison is the first thing anyone does during an
   incident.
   *What would change it:* a preference for one shape in the audit row over fidelity to the
   source file.

---

**Test count:** 487 passed, 36 deselected before -> **630 passed, 6 skipped, 36 deselected**
after (`pytest -m "not spark" -q`). Net +143, all in the three new Oracle test files; the 6
skips are the `sql_query` cells of the cross product that are not buildable configurations.

**New VB entries this sub-step:** VB-19 (the `TO_TIMESTAMP` literal and its format model),
VB-20 (`SYSTIMESTAMP` as the dynamic window's anchor).
