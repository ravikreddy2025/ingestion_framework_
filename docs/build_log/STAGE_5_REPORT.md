# Stage 5 Report — File source (ADLS via Auto Loader)

Read against `.claude/build/CORE.md`, `.claude/build/STAGE_5_files.md` and every prior file
in `docs/build_log/`. Stage 4 was green (770 passed, 6 skipped, 36 deselected) before this
stage started.

---

## 1. Done and verified

Command whose output proves each claim: `ruff check src tests`, `ruff format --check src
tests`, `pytest -m "not spark" -q` — all three green throughout, final state:
`866 passed, 7 skipped, 36 deselected`.

**The contract**
- `sources/file/spec.py` — `SOURCE_SPEC`: `layers=("landing",)`, the two Kafka-shaped
  standing levers (`failure_mode`, `max_files_per_trigger`) settable in both YAML and the
  control table, `checkpoint_reset_id` operational-only, `control_columns` matching
  `docs/build_log/DECISIONS.md` D-01's table exactly (`file_failure_mode`,
  `file_max_files_per_trigger`, `file_checkpoint_reset_id`). `target_tokens =
  {target_schema, target_table}`. Verified: `tests/test_file_spec.py` (18 tests, including
  the "every declared key is actually read" sweep over the whole package).
- `sources/file/run.py` — `run(ctx)`. Auto Loader `readStream` + `foreachBatch`, landing
  only, `availableNow` always. `failure_mode` decides what a batch holding a rescued
  (schema-mismatched) row does to the RUN, since there is no separate quarantine table:
  `FAILFAST` refuses it, `QUARANTINE` lands it and reports the count on the audit row.
  Verified: `tests/test_file_run.py` (23 tests: the microbatch body's order and failure
  handling, mirroring `tests/test_kafka_run.py`'s `batch_session` technique of
  monkeypatching `landing.project` so the real-Spark-only projection is not needed).
- **The checkpoint-reset guard is Kafka's guard, reused, not redesigned** — the STAGE_5
  brief's explicit instruction. `sources/file/run.py::_guard_against_checkpoint_reset`
  mirrors `sources/kafka/run.py`'s function field-for-field: same three states, same
  refusal message shape, same single-use reset-id check against the audit table. The one
  difference is deliberate and stated in both the code and `docs/DESIGN.md` §11: no
  `topic`-style filter on the "already landed" check, because this source's landing table
  belongs to exactly one file source. Verified: 12 guard tests in `tests/test_file_run.py`,
  covering intact/first-run/no-table/refused/fresh-reset/reused-reset/stale-reset-inert/
  unreadable-Volume, the same coverage `tests/test_kafka_run.py` has for Kafka's.
  **Deliberately NOT promoted to `framework/`**: CORE section 2 rule 4 sets the bar at
  three implementations; only two sources (Kafka, this one) need it, and Oracle has no
  checkpoint at all. Recorded in `docs/DESIGN.md` §11 as the reasoning, not left implicit.

**Configuration**
- `sources/file/config.py` — `FileConfig` + `build()`. Value-level validation: `schema:`
  required for `schema_mode: provided` and rejected otherwise (18 tests in
  `tests/test_file_config.py`); `format_options` validated against a known set PER
  `file_format` (csv/json/parquet/avro), rejecting an option valid for one format but not
  another; `filename_columns` regexes compiled and checked for exactly one capture group at
  config load; `source_path` rejected outright if it looks like a full URL (`"://" in
  source_path`) — the account and container come from `storage_ref`, never from the source
  file, extending CORE section 6's "no catalog in a source file" rule to storage accounts.
- `StorageProfile` — two auth modes, `account_key` and `service_principal`, each validated
  on construction exactly as `sources/oracle/config.py`'s `JdbcProfile` is. Everything else
  ADLS supports (SAS tokens, managed identity, UC credential passthrough) is deliberately
  not implemented, matching Oracle's "one JDBC auth mode" restraint — adding one is a code
  change, not a config guess.
- `conf/storage.yaml` — the fourth register, identical pattern to `clusters.yaml` /
  `registries.yaml` / `jdbc.yaml`: auth mode and secret KEY NAMES in the register, account /
  container / secret SCOPE overlaid per environment in `conf/environments/<env>.yaml`.
  Filled in for `dev`, `preprod` and `prod`.
- `conf/defaults/file.yaml` — every platform default the spec's `required_keys` needs:
  `max_files_per_trigger: 1000`, `schema_mode: provided` (with the silent-type-drift
  reasoning stated in the file, not only in the docs), `listing_mode: directory` (VB-07),
  `failure_mode: FAILFAST`, the Volume-backed checkpoint/schema-location roots.
- `conf/sources/_TEMPLATE_file.yaml` — the four onboarding questions from the STAGE_5
  brief's "Configuration" section, adapted into the same "MUST CHANGE / ask first" shape
  `_TEMPLATE_oracle.yaml` uses.
- `conf/sources/file_claims_inbound.yaml` — worked example (not in the stage's literal file
  list, added for the same reason `oracle_claim_header.yaml` exists: `resources/
  job_ingest_file.yml` needs a real task to reference, and the shipped-config test suite
  needs a real source to exercise). CSV, pipe-delimited, provided schema,
  `filename_columns` promoting a business date out of the file name.

**Tables and writes**
- `sources/file/tables.py` — `landing_columns()` builds the DDL from the resolved read
  schema (source columns) + `filename_columns` (STRING) + `FIXED_METADATA_DDL`
  (`_file_path`, `_file_name`, `_file_size`, `_file_modification_time`, `_rescued_data`,
  plus the standard provenance columns). `_rescued_data` is excluded from "source columns"
  even when Auto Loader's real read schema already carries it, and declared exactly once —
  verified by `tests/test_file_tables.py::test_rescued_data_in_the_raw_schema_is_not_
  declared_twice`, which would have caught the duplicate-column bug I found and fixed
  while writing this module (see §3 below for how it was found).
- `sources/file/landing.py` — the projection: source columns verbatim, `_metadata`-derived
  columns, `filename_columns` via `regexp_extract`, provenance columns. Not unit-tested
  directly (uses real `pyspark.sql.functions`) — see §2, mirroring the fact that
  `sources/kafka/landing.py`'s `project()` has no dedicated test file either.
- `sources/file/reader.py` — `cloudFiles.*` options: `schemaLocation` always set (STAGE_5
  brief: it is a checkpoint-like resource with its own lifecycle, used for more than
  inference bookkeeping even when a schema is provided), `maxFilesPerTrigger` always set,
  `rescuedDataColumn` hardcoded to `_rescued_data` (not a knob — see §5 item 2),
  `pathGlobFilter` always set. `schema_mode: provided` calls `.schema(ddl_string)`.

**Security**
- `sources/file/security.py` — `build_storage_options()`: `account_key` mode builds
  `fs.azure.account.key.<account>.dfs.core.windows.net`; `service_principal` mode builds
  the four OAuth client-credentials options. Both are documented, standard Hadoop-Azure
  (ABFS driver) configuration keys, not invented. Verified: `tests/test_file_security.py`,
  including that no credential reaches a log line via the EXISTING `redact()` hint list —
  `account.key` and `secret` were already covered, so no widening was needed.
- `framework/security.py` — added `apply_session_options()`: set session/Hadoop
  configuration for one call, then restore whatever was there before, mirroring
  `framework/writers.py`'s existing schema-evolution-flag restore pattern. Generic (no
  source-type knowledge), because ADLS Gen2 credentials are read by the Hadoop FileSystem
  from SESSION configuration rather than per-reader `.option()` calls the way every other
  connection map in this framework works — see VB-26 for what is unverified about that on
  the target compute. **The CORE section 7 grep gate stayed clean**: an early docstring
  draft named "kafka" and "JDBC" to explain the contrast, which the grep caught; reworded
  to describe the shape generically instead. Re-ran the grep after the fix — clean.

**Operationalisation**
- `sql/01_operational_config.sql` — added the three `file_*` control columns and their
  CHECK constraints (`file_failure_mode_valid`, `file_max_files_per_trigger_positive`),
  exactly `docs/build_log/DECISIONS.md` D-01's table. Not in the STAGE_5 file list
  literally, but D-01 is a settled decision naming these columns "added in Stage 5" — doing
  the spec/control-column half without the DDL half would leave the control table unable to
  hold what `sources/file/spec.py` declares.
- `resources/job_ingest_file.yml` — one job, one task (`file_claims_inbound`), same
  `max_concurrent_runs: 1` / dropped-not-queued shape as the other two ingestion jobs, same
  reasoning documented inline.
- `docs/CONFIGURATION.md` §10, `docs/DESIGN.md` §11, `docs/RUNBOOK_SUPPORT.md` §9 — written
  to the same depth as the Oracle sections they follow. `docs/DESIGN.md` §11 also states the
  `cloudFiles.schemaLocation` / reset-guard decision the STAGE_5 brief explicitly asked for.

**Tests extended (not just added)**
- `tests/test_shipped_config.py` — `FILE_KEYS` / `FILE_ENVS`, plus a "FILE SOURCES" section
  mirroring the Oracle one (10 new tests): resolves in every environment, storage profile
  reachable per environment, no landing-table collision across all three source types now,
  `_TEMPLATE_file` parametrized into the existing template-inertness test.
- `tests/test_shipped_jobs.py` — a parallel `file_job` fixture and 7 tests mirroring the
  Oracle job tests (concurrency, dropped-not-queued, entrypoint/source-key well-formedness,
  source-type match, environment plumbing, retry settings, template inertness).
- `tests/test_framework_runner.py` — `_IMPLEMENTED` grew to include `"file"`; the
  parametrized "stub refuses to pretend" test is now correctly empty (by construction, as
  its own comment always said it would be) rather than failing against a real
  implementation.
- `tests/conftest.py` — `file_config_root`, `write_file_source`, `make_file_cfg`,
  `make_file_ctx`, `file_cfg`, mirroring the Oracle fixtures section exactly, including
  copying `conf/defaults/file.yaml` verbatim from the repository rather than a paraphrase.

---

## 2. Done but not verifiable here

- **Everything needing a live workspace or a JVM** — no Kafka, Oracle, ADLS, Databricks or
  Spark available locally, per CORE section 3. Nothing was run or claimed to have run that
  needed any of them.
- **`sources/file/landing.py`'s real projection** (`project()`, `rescued_count()`) uses
  actual `pyspark.sql.functions` (`regexp_extract`, `withColumn` chains) and has no
  dedicated unit test, matching the existing gap for `sources/kafka/landing.py` (no
  `test_kafka_landing.py` exists in this repository either). Exercised only indirectly, via
  `process_microbatch` tests that monkeypatch `landing.project` itself. VB-06 already covers
  whether `_metadata` and `rescuedDataColumn` behave as documented on the target DBR/format
  combination — this reuses that entry rather than adding a new one for the same question.
- **VB-26 (new)** — whether session-scoped `spark.conf.set()` for ADLS credentials is
  visible where the FileSystem actually opens the path (driver for listing, executors for
  reading), and whether it is safe on serverless jobs compute if sessions are ever shared
  across concurrent jobs.
- **VB-27 (new)** — whether a Delta append reconciles an incoming DataFrame's columns by
  NAME when its order differs from the target table's. This source is the first one in the
  framework whose projected column order does not match its own DDL order by construction
  (`_rescued_data`'s position in Auto Loader's real read schema is not specified), so it is
  the first to actually depend on Delta's documented (but here unexercised) append-by-name
  behaviour rather than happening to match positionally.
- **VB-07** (seeded in Stage 0) — Auto Loader listing vs notification mode. Implemented
  defensively per its own recommendation: `listing_mode: directory` is the platform
  default, `notification` is available as a config value, and nothing about the choice is
  guessed at beyond mapping it onto `cloudFiles.useNotifications`.

---

## 3. Not reproduced

- **A real bug, found and fixed while writing `sources/file/tables.py`.** My first draft
  declared `_rescued_data` unconditionally in `FIXED_METADATA_DDL` without excluding it from
  the "source columns" derived from the raw read schema. Since Auto Loader adds
  `_rescued_data` to the REAL schema (unlike `_metadata`, which is virtual and never appears
  in `schema.fields`), this would have produced a DDL with the column declared twice — a
  `CREATE TABLE` failure on the very first run, in every environment. Caught by writing
  `tests/test_file_tables.py::test_rescued_data_in_the_raw_schema_is_not_declared_twice`
  before trusting the module, not by any external review. Fixed by excluding `_rescued_data`
  (and every `filename_columns` name) from the "source columns" list before the DDL is
  assembled. Recorded here because CORE section 9 asks for "a useful result, not a
  failure" — this is the useful result: the fix is in the code and the regression test
  is what proves it stays fixed.
- **The STAGE_5 brief's illustrative example config used `partition_by:` and a literal
  `abfss://...` URL in `source_path:`.** Neither survived into the shipped design — see
  §5 items 1 and 3 for why, and §5 for both flagged as decisions rather than silent
  deviations.

---

## 4. Blocked

Nothing. Every requirement in `STAGE_5_files.md`'s "Work" and "Exit gate" sections has a
corresponding file, test, or documented decision above.

---

## 5. Decisions for the human

1. **`schema_mode: hints` carries no value in this stage, read literally from the brief's
   own validation rule** ("`schema` is required when `schema_mode: provided`, and rejected
   otherwise" — read as applying to `hints` too). Implemented as
   `cloudFiles.inferColumnTypes` (infer, but try harder for real types) rather than
   `cloudFiles.schemaHints` (a human-named partial schema), which is the more common reading
   of the word "hints" in Auto Loader's own vocabulary. **What would change it:** if the
   intent was actually `schemaHints`, a `schema_hints:` key (or reusing `schema:` for it
   too) is a small follow-up — flagging now rather than guessing which was meant.
2. **`failure_mode` for this source gates on the RESCUED-ROW COUNT, not on file-level
   read failures.** The STAGE_5 brief says `rescuedDataColumn` "is this source's
   quarantine" but does not spell out what `failure_mode` should do with it. Implemented
   as: `FAILFAST` (platform default) refuses a batch containing any rescued row;
   `QUARANTINE` lands it and reports the count on the audit row — the same two-value
   vocabulary Kafka's `failure_mode` uses, applied to the one signal this source has for
   "something did not fit." **What would change it:** if rescued rows are expected to be
   routine and low-volume for some source, a threshold (e.g. "fail only above N% of the
   batch") would be a different, larger design — not built here because nothing in the
   brief asked for it and CORE section 2 rule 6 says smallest correct change.
3. **`source_path` is the path WITHIN the container, never a full `abfss://` URL** — a
   deliberate departure from the STAGE_5 brief's illustrative
   `source_path: "abfss://.../claims/inbound/"`. Keeping the account/container in
   `source_path` would either duplicate what `storage_ref` already names (and let the two
   disagree) or bake one environment's account into a source file, breaking CORE section 6's
   "a source file must never contain a catalog or table name" the same way a literal
   `abfss://acct.dfs.core.windows.net/...` bakes in one environment's storage account.
   `FileConfig.full_source_path` builds the real URL from `storage_ref`'s resolved profile.
   Documented in `docs/CONFIGURATION.md` §10 and enforced (`"://" in source_path` is a
   config error).
4. **`target_schema` / `target_table` as explicit source-file settings**, filled into the
   landing pattern via the SAME `target_tokens` deferred-placeholder mechanism Oracle uses
   for `{source_schema}`/`{source_table}` — even though a file feed has no source-side
   schema/table to derive them from the way Oracle's Oracle-side identifiers give it one.
   Chosen over letting a source file set `landing_table:` directly, which would either
   contain a literal catalog (forbidden) or need its own separate mechanism. **What would
   change it:** none anticipated — this is the same shape as Oracle's, not a new one.
5. **`landing_partition_by`, not the brief's illustrative `partition_by:`** — for
   consistency with Kafka's and Oracle's identical setting name. A one-word rename, flagged
   because it is a literal difference from the brief's example YAML.
6. **The checkpoint-reset guard was duplicated into `sources/file/run.py`, not promoted to
   `framework/`.** See §1. CORE section 2 rule 4's bar is three implementations; this stage
   makes it two (Kafka, file). Recorded as the "first place to look" if a third
   checkpoint-based source type is ever added — a Databricks-native mechanism that would
   want this exact pattern is easy to imagine (any streaming-checkpoint-based reader), so
   this is likely to come up again rather than being a one-off.
7. **No `file_replay` job or entrypoint was built.** The STAGE_5 file list does not mention
   `entrypoints/run_replay.py` or `resources/job_replay.yml`, and CORE's `RunContext.
   run_type` comment already anticipates `file_replay` as a future value without requiring
   it now. Consequence, stated explicitly in `docs/RUNBOOK_SUPPORT.md` §9.4: after a
   checkpoint-reset restart, there is currently no tool to backfill the gap the way a Kafka
   replay or an Oracle replay would — an operator has to re-present the missing files under
   new names, or this becomes a real follow-up piece of work. Flagging this as a genuine
   operational gap, not a silent one.
8. **`resources/job_maintenance.yml` and `sql/04_maintenance.sql` were NOT extended** to
   cover this source's landing table. Verified this matches precedent exactly: Oracle's
   `oracle_claim_header` landing table was not added to either in Stage 4 (checked via
   `grep -c oracle resources/job_maintenance.yml` → 0). Both source types now share this
   gap; worth a dedicated pass rather than a one-line addition per source, since the
   maintenance job's current shape (one hardcoded task list) does not scale past Kafka's
   three eponymous topics either.
9. **Only `account_key` and `service_principal` storage auth modes are implemented.** SAS
   tokens and Unity Catalog credential passthrough are real, commonly-used alternatives,
   deliberately not built for the same reason Oracle implements exactly one JDBC auth mode:
   each would need a token-provider class or workspace-level wiring this project cannot
   verify exists on the target runtime, so adding one is a future code change with its own
   verification, not a config guess today.

---

**Test count:** 770 passed, 6 skipped before → **866 passed, 7 skipped, 36 deselected**.

**New VB entries this stage:** VB-26 (session-scoped ADLS credentials via `spark.conf.set()`
on the target compute), VB-27 (Delta append column reconciliation by name vs position).
VB-06 and VB-07, seeded in Stage 0 for this stage, are now load-bearing rather than
speculative — referenced directly from `sources/file/reader.py`, `sources/file/tables.py`
and `conf/defaults/file.yaml` rather than only from the backlog file.
