# Stage 4 Report -- Oracle source

Branch `stage-4-oracle`, cut from `main` after Stage 3 (PR #5) merged. **One section per
sub-step**, written as each sub-step's exit gate goes green. Sub-step 4d (operationalise)
is not started.

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


---

# Sub-step 4b -- the JDBC read

**NOTHING IN THIS SUB-STEP WAS EXECUTED AGAINST A DATABASE.** There is no Oracle here, no
JDBC driver, and no JVM that could load one. What ran is every unit test below, against
recording stand-ins; what a real read does with the options they assert is VB-01, VB-12 and
VB-21 to VB-24. That distinction is the point of the sub-step's own exit gate and it is
stated here rather than buried.

Sub-step 4b also carries the new work from `DECISIONS.md` D-09: support can switch a source
between a full and a delta load without a deploy, and a replay can bound the cursor interval
explicitly.

## What was built

```
sources/oracle/
  security.py   profile + resolved secrets -> JDBC connection options. No PySpark import.
  reader.py     the read options, the two shapes of read, and the partition-bounds probe
  types.py      customSchema going in, the resolved schema coming out, drift between runs
  config.py     JdbcProfile (builds the URL from parts), ReplayControls, the 4b keys
  query.py      + bounds_query(); a replay's start bound is always inclusive
conf/jdbc.yaml                  two worked profiles - service-name and SID forms
conf/environments/*.yaml        the per-environment jdbc overlay (host, service, scope)
conf/defaults/oracle.yaml       query_timeout, and why session_init/column_types have no default
sql/01_operational_config.sql   oracle_incremental_mode + its CHECK
```

`run.py` is still the Stage 1 stub. 4c assembles these into `run(ctx)` with the watermark
lifecycle, which is the piece that decides ordering and must not be half-built.

## 1. Done and verified

- **The three non-negotiable read options.** `fetchsize` is always set (the driver's own
  default is ten rows per round trip); the query goes in `dbtable` as a parenthesised
  subquery and never in the `query` option, which is the form that works under either answer
  to VB-01; `driver` is named rather than left to JVM auto-discovery. Proof:
  `test_the_fetch_size_is_always_set`,
  `test_the_query_is_passed_as_a_parenthesised_subquery_in_dbtable`,
  `test_the_driver_class_is_named`.
- **All four partition options, or none.** A serial read sets none of them, because
  `numPartitions` alone splits nothing; a partitioned read sets all four. Proof:
  `test_a_serial_read_sets_none_of_the_partition_options`,
  `test_a_partitioned_read_sets_all_four`.
- **Partition bounds are READ, not configured**, with a `SELECT MIN/MAX` over the same query
  the extract will run - so a filtered extract is sliced over the rows it will actually
  return, and no configured pair can go stale into skew. The probe never opens the extract's
  connections, a serial read never pays for it at all, and an extract matching no rows falls
  back to a serial read rather than handing Spark `lowerBound=None`. Proof:
  `test_the_bounds_come_from_the_query_the_run_will_extract`,
  `test_the_bounds_probe_never_opens_the_extract_s_connections`,
  `test_no_partition_column_means_no_probe_at_all`,
  `test_an_empty_extract_falls_back_to_a_serial_read`.
- **No credential can reach a log line, an audit row, or the URL.** The password is masked by
  name through the framework's own redactor, on the FULL read options map and not just the
  connection half; and the URL is BUILT from validated parts, so `user/password@host` pasted
  into `host:` is refused rather than becoming the one option whose name gives a redactor no
  hint. Proof: `test_no_credential_appears_in_a_redacted_read_options_map`,
  `test_no_credential_survives_redaction_of_the_options_map`,
  `test_a_rendered_log_line_never_carries_the_password`,
  `test_a_host_that_is_not_a_hostname_is_refused` (5 cases).
- **`conf/jdbc.yaml` follows the register pattern exactly**, with no new mechanism: two
  profiles recording auth mode and secret KEY names, overlaid per environment with host,
  service name/SID and secret scope. Both URL forms are covered because many on-premise
  instances still present a SID. Proof: `test_a_service_name_profile_builds_the_modern_url_form`,
  `test_a_sid_profile_builds_the_older_url_form`,
  `test_exactly_one_of_service_name_and_sid_is_required`,
  `test_the_shipped_profiles_resolve_in_every_environment`.
- **Type handling, all three jobs.** `customSchema` renders only the overridden columns and
  refuses a type this module cannot verify; the resolved schema becomes plain data for the
  audit row; a column the driver mapped to `void` stops the run rather than landing NULLs.
  Proof: `test_overrides_render_in_the_option_s_own_grammar`,
  `test_a_type_this_module_cannot_verify_is_refused` (5 cases),
  `test_a_column_the_driver_could_not_map_stops_the_run`.
- **Schema drift is non-additive-only, and it is a failure.** A changed type or a column that
  stopped being returned stops the run and names both types; a NEW column is allowed, because
  adding one is the ordinary way a source table evolves and failing on it would make every
  source-side release an ingestion incident. Proof: `test_a_type_change_is_reported_with_both_types`,
  `test_a_column_that_stopped_being_returned_is_reported`,
  `test_a_new_column_is_additive_and_allowed`,
  `test_assert_no_drift_names_the_table_and_every_change`.
- **D-09, the full-vs-delta switch.** `incremental_mode` is now operationally overridable via
  `ingest_control.oracle_incremental_mode` (with a CHECK constraint), and it is the ONE
  operational key that changes which rows are extracted. What the increment MEANS -
  `cursor_column`, `cursor_type`, `merge_keys`, `filter_criteria` - stays structural, so an
  override of any of them is still ignored. Proof:
  `test_support_can_switch_a_delta_source_to_a_full_load_without_a_deploy`,
  `test_an_override_still_cannot_redefine_the_increment`,
  `test_the_mode_switch_is_the_one_operational_key_that_changes_what_is_extracted`.
- **D-09, the replay window.** `replay_cursor_start` / `replay_cursor_end` are
  operational-only and ride in the framework-owned `replay_controls` JSON. A replay requires a
  rerun id, a cursor source and a start bound; the bounds on a scheduled run are refused
  outright, because a scheduled run that honoured them would re-extract the same window
  forever. A replay's start bound is always INCLUSIVE, even where `merge_keys` are waived -
  it is a boundary a human typed, and excluding it would silently drop the rows they named.
  Proof: `test_a_replay_bounds_the_cursor_interval_explicitly`,
  `test_a_replay_without_a_start_bound_is_refused`,
  `test_a_replay_without_a_rerun_id_is_refused`,
  `test_replaying_a_source_that_has_no_cursor_is_refused`,
  `test_replay_bounds_on_a_scheduled_run_are_refused`,
  `test_a_replay_start_bound_is_always_inclusive`.
- **`session_init` is checked by shape.** It runs once per JDBC CONNECTION - i.e. once per
  partition - so anything that wrote would execute `num_partitions` times. An ALTER SESSION or
  a PL/SQL block is accepted; a SELECT, a DML statement, a chained statement and a comment
  marker are not. Proof: `test_a_session_statement_that_is_not_one_cheap_setting_is_refused`
  (4 cases), `test_an_alter_session_or_a_plsql_block_is_accepted` (2 cases).
- **CORE section 7 grep returns nothing** (run from the repository root over
  `src/kafka_ingest/framework/`; exit status 1, no output).
- **Exit gate, all three commands run:**
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  67 files already formatted

  $ python -m pytest -m "not spark" -q
  708 passed, 6 skipped, 36 deselected in 34.72s
  ```
- **Fourteen mutations run to prove the new tests can fail**, each restored afterwards. Every
  one failed at least one test:

  | Mutation | Result |
  |---|---|
  | `fetchsize` no longer set | 1 failed |
  | the `query` option instead of a `dbtable` subquery | 3 failed |
  | `numPartitions` dropped from a partitioned read | 1 failed |
  | an empty extract keeps its NULL bounds | 1 failed |
  | a serial read still pays for the bounds probe | 2 failed |
  | host validation removed (a credential could enter the URL) | 5 failed |
  | service_name/sid exclusivity removed | 2 failed |
  | a replay without a start bound accepted | 1 failed |
  | replay bounds accepted on a scheduled run | 1 failed |
  | `session_init` unchecked | 4 failed |
  | an unmapped column allowed to land as NULL | 1 failed |
  | a type change no longer reported as drift | 2 failed |
  | a dropped column no longer reported as drift | 2 failed |
  | a replay's start bound becomes exclusive | 1 failed |

## 2. Done but not verifiable here

Everything in this sub-step that touches a database. Explicitly:

- **No JDBC read was executed, and no connection was opened.** Every read test asserts the
  OPTIONS MAP handed to `spark.read`, against a recording stand-in.
- **VB-21 (new)** -- is the partition-bounds probe cheap, and are the slices even? A probe
  that full-scans turns a partitioned run into two scans, and nothing in the run's own
  numbers would show it.
- **VB-22 (new)** -- is the Oracle JDBC driver installed on the target cluster, and which
  version? The class-not-found case is loud; the VERSION is not, and it is what decides
  VB-02 and VB-03.
- **VB-23 (new)** -- does `customSchema` override only the columns it names, or is it read as
  the complete schema? If the latter, `column_types` would silently drop every column it does
  not mention.
- **VB-24 (new)** -- is `sessionInitStatement` honoured alongside a `dbtable` subquery, and
  how often does it run? If it is ignored, a source relying on it reads different values with
  no error at all.
- **VB-01** is unchanged and is the reason for the `dbtable` form. **VB-02 / VB-03 / VB-04**
  now have their fix path built (`column_types` -> `customSchema`, and `extra_options` on the
  jdbc profile for driver properties) but remain unanswered.
- **VB-12** covers whether the cluster can reach the database at all.

## 3. Not reproduced

- **`framework/security.py` needed no change.** The stage file lists it under "Edit". Stage 3
  had already split it - `SecretResolver` and `redact()` are source-agnostic and were reused
  as they are - and adding anything JDBC-shaped to it would trip the CORE section 7 grep. The
  Oracle half went to `sources/oracle/security.py`, mirroring `sources/kafka/security.py`.
- **The read path is in `reader.py`, not `run.py`.** The stage file says
  "`sources/oracle/run.py` read path". Kafka already puts reader construction in
  `sources/kafka/reader.py`, and a support engineer asking "how is the read built?" should
  find the same answer in both packages. `run.py` stays the run's shape, which is what 4c
  fills in.
- **`lowerBound` / `upperBound` are not configuration.** The stage file's sketch shows
  `lo` / `hi` without saying where they come from. Configured bounds go stale silently -
  an id range grows and every new row lands in the last slice - so they are probed instead.
  VB-21 is the check on that choice.
- **No `sources/oracle/run.py` and no shipped Oracle source file yet.** The first is 4c by
  the stage file's own split; the second waits on `tests/test_shipped_config.py` learning to
  partition by `source_type`, which is 4d's operationalise work.

## 4. Blocked

- Nothing. Two things deliberately not done, one sentence each per CORE rule 8:
  - **No retry, no connection pool, no circuit breaker around JDBC** - all three are on the
    stage file's "do not build" list, and Spark opens and closes its own connections per
    partition, so a driver-side pool would pool nothing.
  - **No `ALL_TAB_COLUMNS` probe to recover Oracle's own type names** - it would make the
    "unmapped type" message name the Oracle type rather than the Spark one, at the cost of a
    second round trip and a second thing to keep in step; VB-23 may force the question, and
    it should be answered then rather than guessed at now.

## 5. Decisions for the human

1. **Partition bounds are probed, not configured.** One extra round trip per partitioned run,
   in exchange for bounds that cannot go stale into skew. VB-21 is the measurement.
   *What would change it:* a probe that costs a second full scan - at which point the honest
   fix is optional explicit bounds for the tables where a DBA has a better answer, not
   dropping the bounds and keeping `numPartitions`.
2. **One `txnAppId` per source, with no fork for a replay** - unlike Kafka's, which forks on
   the rerun id. Oracle's `txnVersion` is the durable `run_sequence`, allocated on every run
   of every type, so a replay always carries a higher version and cannot collide with what
   the primary lineage committed. This follows the stage file's own
   `ingest::oracle::{source_key}` exactly.
   *What would change it:* `run_sequence` ceasing to be monotonic across run types.
3. **`query_timeout` ships as 0 - no timeout.** A first full extract of a large table
   legitimately runs for a long time, and a platform-wide timeout would kill it at the same
   point every night with an error that looks like a network fault. It is a per-source setting
   once that source's normal run time is known.
   *What would change it:* a shared Oracle instance where a runaway session is an operational
   incident for other users - then a generous platform default (say two hours) is better than
   none.
4. **Only `auth_mode: basic` exists.** Wallets and Kerberos each need a file or a ticket
   staged on the executors, which is a compute-profile problem rather than a configuration
   one, so adding either is a code change and no half-supported mode is selectable from YAML.
   *What would change it:* a client whose Oracle estate mandates wallet auth - worth knowing
   before 4d writes the onboarding checklist.
5. **A replay's start bound is inclusive even where `merge_keys` are waived.** The scheduled
   run's operator rule (`>` without merge keys) is about not re-reading; a replay is about
   re-reading deliberately, and dropping the boundary row the operator typed would be a
   surprise in the one situation where somebody is watching.
   *What would change it:* nothing I can see; recorded because the two rules differ.

---

**Test count:** 630 passed, 6 skipped before -> **708 passed, 6 skipped, 36 deselected**
after (`pytest -m "not spark" -q`). Net +78 across three new test files and the D-09
additions to the two existing ones.

**New VB entries this sub-step:** VB-21 (the bounds probe), VB-22 (the JDBC driver and its
version), VB-23 (`customSchema` semantics), VB-24 (`sessionInitStatement` semantics).


---

# Sub-step 4c -- the watermark lifecycle

`run(ctx)` exists now, and with it the ordering the whole source is built around:

    capture the high water -> read the closed interval -> write -> THEN advance

Everything in this sub-step is either that order or a case where one of its steps must NOT
happen. All of it ran: a source is handed a RunContext and nothing else, so the lifecycle is
testable end to end against stand-ins, with no database and no JVM.

## What was built

```
sources/oracle/
  run.py       the lifecycle. The only place the ordering lives.
  landing.py   the provenance columns, as SQL expression strings (so: no PySpark import)
  tables.py    the landing DDL, built from the schema the read resolved
  config.py    + merge_on, update_matched_rows, landing_partition_by
  query.py     + high_water_query()
conf/defaults/oracle.yaml   landing_partition_by, and why it is not a cursor-derived date
```

## 1. Done and verified

- **The order, asserted directly.** `test_the_watermark_is_written_after_the_write_and_never_before`
  records the write and the state write into one list and asserts `["write", "state"]`. A
  state write that landed first would mean a crash during the write silently skipped the
  interval it claimed to have read.
- **A failed write leaves the watermark where it was.** The next run re-extracts the same
  interval, which the MERGE key absorbs. Proof:
  `test_a_failed_write_leaves_the_watermark_where_it_was`.
- **A replay never writes `ingest_state` - and never reads it either.** Its bounds come from
  the operator, so a re-extraction cannot strand production state at a bound somebody typed
  once. An unbounded replay ("from there to now") still gets a real upper bound. Proof:
  `test_a_replay_never_writes_state`, `test_a_replay_reads_no_stored_watermark_at_all`,
  `test_an_unbounded_replay_still_gets_a_real_upper_bound`.
- **A full run does not touch the watermark, and says so naming the mode** (D-09). Clearing
  it would make the switch back to delta re-read the entire table. Proof:
  `test_a_full_run_does_not_touch_the_watermark`.
- **An extract with no cursor values reads nothing at all.** One probe, no extract, no write,
  no advance - an empty table or a filter matching nothing is an ordinary Tuesday, not an
  error. Proof: `test_an_extract_with_no_cursor_values_reads_nothing_and_advances_nothing`.
- **THE MERGE KEY INCLUDES THE CURSOR COLUMN**, and that is the most consequential decision
  in this sub-step. `(CLAIM_ID, LAST_UPDATE_DT)` identifies a VERSION of a claim rather than
  the claim, which is what keeps landing the retained mirror D-07 depends on instead of
  collapsing it to current state - and it makes the key STABLE across D-09's mode switch. A
  full run keyed on the business key alone would match every historical version of a claim
  with one source row and overwrite all of them. Proof:
  `test_a_source_with_merge_keys_merges_on_the_key_and_the_cursor`,
  `test_the_merge_key_does_not_change_when_the_mode_does`,
  `test_a_source_with_no_cursor_updates_matched_rows`.
- **The landing MERGE passes `partition_predicate="true"`, deliberately.** Landing is
  partitioned by the date a row was WRITTEN, so a re-extracted row carries today's while its
  target twin carries the day it first arrived; any bound derived from the frame would match
  NOTHING and insert duplicates. This is the second use of `framework/writers.merge()`'s
  declared escape hatch, and it is the same argument Stage 3 made for Kafka's landing. Proof:
  `test_the_landing_merge_declares_that_no_partition_bound_is_derivable`.
- **A source that waived merge keys appends with the Delta idempotency markers**
  (`ingest::oracle::<source_key>` + the durable `run_sequence`), and gets a WARN on EVERY run
  naming the boundary rows it can lose. A serial read gets one too. Both are legal, deliberate
  and invisible in a row count, which is the only kind of thing worth warning about every
  time. Proof: `test_a_source_that_waived_merge_keys_appends_with_idempotency_markers`,
  `test_the_waived_boundary_risk_is_warned_about_on_every_run`,
  `test_a_serial_read_is_warned_about`.
- **The extract is evaluated once.** Without the cache, `count()` and the write are two
  actions over a JDBC source - two full reads of the source table, and two chances for them to
  disagree about what Oracle held. Released in a `finally`, so the failure path does not pin a
  large extract until the executor is recycled. Proof:
  `test_the_extract_is_evaluated_once_and_released_afterwards`,
  `test_the_cache_is_released_on_the_failure_path_too`.
- **The landing table is created explicitly**, from the schema the read resolved, with the
  platform's TBLPROPERTIES and `PARTITIONED BY (ingest_date)` - an implicitly created table
  would be the only one people query without auto-compaction. Proof:
  `test_the_landing_table_is_created_from_the_resolved_schema_before_the_write`.
- **Schema drift stops the run before it writes**, and the framework's own seven provenance
  columns are excluded from the comparison - without that, every run after the first would
  fail. Proof: `test_a_type_change_against_the_existing_table_stops_the_run`,
  `test_the_frameworks_own_columns_are_not_mistaken_for_drift`,
  `test_a_column_the_driver_could_not_map_stops_the_run_before_any_write`.
- **The projection and the DDL agree, column for column, in order.** Two definitions of
  landing's provenance columns exist and a drift between them surfaces as a confusing Delta
  error on the first append and nowhere earlier. Proof:
  `test_the_projection_and_the_ddl_name_the_same_columns_in_the_same_order`.
- **An Oracle column that would shadow a provenance column is refused**, with the column name
  and the fix - the error Delta gives for a duplicate column names neither this framework nor
  the source table. Proof: `test_a_source_column_that_would_shadow_a_provenance_column_is_refused`.
- **The audit row carries the query the run actually ran**, which is the first question of
  every Oracle incident and is not reconstructable from configuration once a dynamic window
  and a watermark are involved. `pending_work` is NULL, because knowing it would take another
  round trip and a confident zero is the claim a lagging feed makes falsely. Proof:
  `test_the_audit_row_carries_the_query_this_run_actually_ran`,
  `test_pending_work_is_null_because_this_source_cannot_know_it`.
- **CORE section 7 grep returns nothing.**
- **Exit gate, all three commands run:**
  ```
  $ python -m ruff check src tests
  All checks passed!

  $ python -m ruff format --check src tests
  71 files already formatted

  $ python -m pytest -m "not spark" -q
  745 passed, 6 skipped, 36 deselected in 22.69s
  ```
- **Fifteen mutations run, each restored afterwards.** Fourteen failed a test immediately:

  | Mutation | Result |
  |---|---|
  | the watermark advances BEFORE the write | 2 failed |
  | a replay advances the watermark | 1 failed |
  | an unbounded replay loses its upper bound | 1 failed |
  | an empty extract still reads and writes | 1 failed |
  | the extract cache is never released | 2 failed |
  | the extract is not cached (read twice) | 1 failed |
  | the landing merge is bounded to today | 1 failed |
  | provenance columns compared as drift | 1 failed |
  | an unmapped column allowed through | 1 failed |
  | the cursor dropped from the merge key | 4 failed |
  | matched rows always rewritten | 1 failed |
  | a shadowing source column allowed | 2 failed |
  | an unchecked literal reaches the projection | 1 failed |
  | the full-run case stops being named in the log | 1 failed |

  **The fifteenth survived, and it changed the code.** Deleting the `if not cfg.is_cursor:
  return` guard from `_advance_watermark()` failed nothing, because a non-cursor run reaches
  it with `high_water=None` anyway - the guard was unreachable protection, and an unreachable
  guard is a comment that looks like code. The two cases were collapsed into ONE guard whose
  logged reason names which case it was, and a test now asserts that reason. Both mutations of
  the collapsed form fail.

## 2. Done but not verifiable here

- **No JDBC read was executed.** Every read in these tests is recorded, not performed.
- **VB-25 (new)** -- the one gap a closed interval does NOT close: a transaction already open
  when the high-water mark is captured, carrying a cursor value below it, that commits after
  the extract has read past that value. It is never seen, and nothing downstream can detect
  it. How much it matters is a property of the SOURCE application (does it stamp the cursor at
  statement time or at commit?), which is why the entry's "how to check" is a conversation
  with the source team plus a reconciliation query rather than a code change. The module
  docstring says the same thing at the top of `run.py`.
- **VB-15** (does the `ingest_state` MERGE actually upsert?) now has the watermark depending
  on it as well as the run sequence.
- **VB-09** (Delta MERGE schema evolution) is reached by this source's landing MERGE the
  first time an Oracle table gains a column.

## 3. Not reproduced

- **The stage file's `txnAppId` is used as written** (`ingest::oracle::{source_key}`), with no
  fork for a replay - see 4b decision 2. `run_sequence` is monotonic across run types, so a
  replay always carries a higher version and cannot collide.
- **The high-water mark is `MAX(cursor)`, never the database clock.** The stage file offers
  both. A clock reading is ahead of every committed row by definition, so it would move the
  watermark past rows still in flight; `MAX(cursor)` at least never claims to have read past
  the last row it saw. Neither closes VB-25.
- **`landing_partition_by` is `ingest_date`, not a cursor-derived date.** A cursor is optional
  and its column differs per source, so a partition column that exists for some sources and
  not others is a layout nobody can reason about. The cost is the `true` merge predicate,
  which is stated at the call site.
- **One existing test changed.** `test_framework_runner.py::test_the_shipped_source_stubs_
  refuse_to_pretend` derived its list as "everything except kafka"; Oracle is now implemented,
  so the list is `_SOURCES` minus an explicit implemented set. Stage 5 empties it.

## 4. Blocked

- Nothing. Two things deliberately not done, one sentence each per CORE rule 8:
  - **No safety lag on the high-water mark** - it is VB-25's first remedy, it needs a number
    only the source team has, and applying one without `merge_keys` would trade silent loss
    for silent duplication.
  - **No reconciliation or row-count-check utility** - the stage file's "do not build" list
    rules out a schema-migration or reconciliation utility, and VB-25's check is a query a
    human runs once per table at onboarding.

## 5. Decisions for the human

1. **The merge key is `merge_keys + cursor_column`, so landing keeps every version of a row.**
   The alternative - merging on the business key alone - makes landing a current-state mirror,
   which is smaller and faster and destroys the history replay depends on. It would also make
   D-09's full/delta switch destructive on a table that already holds several versions.
   *What would change it:* a source table where version history is genuinely unwanted and
   storage matters more - at which point the honest form is a documented per-source setting,
   not a silent change of meaning.
2. **The landing MERGE scans the whole table (`partition_predicate="true"`).** Correct, and
   not free: a daily delta into a large landing table rewrites nothing but must SCAN
   everything to find its matches. The alternative is a cursor-derived partition column, which
   only exists for timestamp cursors.
   *What would change it:* a landing table where the merge scan is itself the incident. The
   fix would then be a partition column derived from the cursor for the sources that have one,
   and it should be a decision here rather than an optimisation somebody applies quietly.
3. **A source with merge keys and no cursor UPDATES matched rows.** With no cursor the key
   identifies the row rather than a version, so a match means the source row changed and the
   mirror goes stale unless it is rewritten. That is the one path where landing overwrites
   what it previously recorded.
   *What would change it:* wanting snapshot history for full-load sources too, which needs a
   snapshot date in the key rather than an update.
4. **VB-25 is a limitation, not a bug, and the code says so out loud.** It is at the top of
   `run.py` and in the backlog with three remedies costed. It should be raised with the source
   team at onboarding rather than discovered during a reconciliation six months in.

---

**Test count:** 708 passed, 6 skipped before -> **745 passed, 6 skipped, 36 deselected**
after (`pytest -m "not spark" -q`). Net +37 across two new test files.

**New VB entries this sub-step:** VB-25 (rows committed below the high-water mark).
