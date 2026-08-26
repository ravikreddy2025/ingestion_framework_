# Stage 6 Report — Extensibility gate, offline validation, CI

Read against `CLAUDE.md`, `.claude/build/CORE.md`, `.claude/build/STAGE_6_gates.md` and every
file in `docs/build_log/`. Five additions were supplied with the request; D-10 through D-14
were recorded in `docs/build_log/DECISIONS.md` before any implementation, as instructed.
Stage 5b was green (865 passed, 7 skipped, 36 deselected) before this stage started.

Branch `stage-6-gates`, cut from `main` after Stage 5b (PR #8) merged.

---

## A pre-existing environment correction, made before anything else

The local environment had only a bare, unpinned Python 3.14 on `PATH` — not the project's own
`.venv` (Python 3.12.10, `pyspark==3.5.2`) that Stage 1 built specifically to match the target
runtime. Discovered this before trusting any gate output, switched to
`.venv/Scripts/python.exe` for every command from that point on, and re-ran everything already
done up to that point to confirm it held under the correct interpreter (it did, byte-for-byte
the same pass/fail counts). Recorded here because every "ran and passed" claim below is against
`.venv`, not the bare interpreter, and a future session hitting the same bare-`python` situation
should know to check for `.venv` first.

---

## 1. Done and verified

Command whose output proves each claim: `ruff check src tests`, `ruff format --check src
tests`, `pytest -m "not spark" -q` — all three green throughout, final state: **947 passed, 7
skipped, 36 deselected**.

### The source-isolation gate

- **`azure-pipelines.yml`** gained a new PR step running the exact CORE section 7 command,
  failing the build when it finds anything. Verified three ways: (a) it currently returns
  nothing — confirmed before any Stage 6 edit and again after every edit; (b) reproduced the
  CI script's own logic locally against the clean tree (`PASS: clean`); (c) planted a stray
  `kafka` mention in `framework/logs.py` as a drill, confirmed the script's logic reports
  `FAIL: matches found`, then reverted it (`git status` clean afterward, not committed).
- **Zero grep-gate matches needed removing this stage.** The gate was already clean, inherited
  from Stage 5b's cleanup — nothing in Stage 6's own work touched `framework/` at all except
  reading it (D-13 and the CI fix are both outside `framework/`).

### Offline validation tests — `tests/test_offline_validation.py` (new, 50 tests)

All five CORE section 3 / STAGE_6 items, generic over source type:

1. `yaml.safe_load` on every file under `conf/` and `resources/`, plus `databricks.yml`.
2. Every job template's `python_wheel_task.entry_point` resolves through
   `pyproject.toml`'s `[project.scripts]` to a real file with that function actually defined
   (via `ast.parse`, not `import` — no Spark session needed for this check specifically); every
   `sql_task.file.path` resolves to a real file relative to `resources/`.
3. Every `source_type` declared in `conf/sources/*.yaml` has a package under `sources/`
   exposing `SOURCE_SPEC` and a callable `run`, and `SOURCE_SPEC.source_type` matches the
   directory name. The reverse direction is also pinned: every entry in `runner._SOURCES`
   resolves the same way.
4. Every `cluster` / `registry` / `jdbc_ref` / `storage_ref` named in a source file, and every
   profile name an environment file overlays under `clusters:` / `registries:` / `jdbc:` /
   `storage:`, exists in its register file — read directly from YAML, not through the full
   config-resolution path, so a failure here names the exact register/reference mismatch
   rather than whatever incidental thing broke first during a full resolve.
5. Every `SOURCE_SPEC.required_keys` is a subset of `structural_keys` — verified true today for
   all three shipped specs. The other half STAGE_6 asks for (`structural_keys` and
   `operational_keys` disjoint) is **not** enforced — see section 3, below.

Verified each category can fail: a broken YAML file, a wheel entry-point pointed at a
non-existent function, a source file declaring an unknown `source_type`, a bad register
reference, and a required key not in `structural_keys` — five targeted mutations, each
producing the expected failure, each reverted before committing.

### The cross-product test — extended `tests/test_shipped_config.py` (now 127 tests)

Chose to extend the existing file rather than add `tests/test_cross_product.py`, per the stage
file's own "(or extend the existing cross-product test)" — `test_shipped_config.py` already
*was* that test, built incrementally by Stages 3, 4d and 5. Added exactly what STAGE_6 names
that was not already covered per source type:

- **A mechanical, forward-compatible placeholder sweep**, one test per source type, over every
  string-valued dataclass field of the resolved config *and* its cluster/registry/jdbc/storage
  profile — so a field added in a future stage is covered by construction, not by whoever
  remembers to add it to a hand-picked list (the existing per-field assertions stay; this is
  additional, not a replacement).
- **A cross-*type* checkpoint-collision check.** Every existing check compared sources within
  one type; nothing compared Kafka's and the file source's checkpoints against each other,
  the only two checkpoint-based types.
- **An explicit pin that topic name / Oracle schema+table / file `source_path` never vary by
  environment** — true by construction today (nothing overrides them in the `environments:`
  sub-layer), but nothing asserted it, and STAGE_6 names it explicitly.

Verified two of these can fail: forced the checkpoint-collision test to see a duplicate path,
and added a per-environment `topic:` override to `vector_patient_events.yaml` — both failed as
expected, both reverted.

### `docs/DESIGN.md` — section 12, "Adding a source type" + BigQuery contract notes

The seven things a new package must provide, table form, ending with "nothing under
`framework/` changes" and naming the grep gate as what enforces it — and the two questions a
BigQuery design must answer (connector/runtime availability; direct read vs export-to-GCS,
which decides whether it looks like Oracle or like Files), per D-08. No code, no config key, no
register file for BigQuery — confirmed nothing under `src/`, `conf/` or `resources/` mentions
it.

### The five additions

- **D-10 (no file replay job).** Documented in three places, exactly as asked: an inline note
  in `conf/sources/_TEMPLATE_file.yaml`; a MUST-READ row in `docs/CONFIGURATION.md` §10; the
  three-step recovery procedure (delete the landing partition(s) first, set a fresh
  `file_checkpoint_reset_id`, run the normal job; bound a re-read by narrowing `source_path` /
  `path_glob`, not an offset window) as new §9.7 in `docs/RUNBOOK_SUPPORT.md`. See section 3
  for what the actual code shows about the generic `replay_rerun_id` column, which the
  documentation reflects rather than the instruction's literal wording — this is the one place
  the two diverge, and it is a "not reproduced" finding, not an oversight.
- **D-11 (maintenance coverage).** `resources/job_maintenance.yml` gained a
  `file_claims_inbound` task, same shape as the three Kafka ones; `sql/04_maintenance.sql`
  needed no change, since its `OPTIMIZE`/`VACUUM` are already source-agnostic (only a landing
  table name). Confirmed the `ops_catalog` parameter gap Stage 2b flagged as Blocked is
  already closed (Stage 3 closed it — verified by reading both the job file's parameter block
  and `sql/04`'s two `:ops_catalog` binds). Added two tests to `tests/test_shipped_config.py`
  asserting every maintenance task's `landing_table` actually matches what its source resolves
  to, for all three source types — the Oracle gap this same decision leaves open (see section
  5) would have been silently possible to reintroduce without this.
- **D-12 (storage auth).** `docs/DESIGN.md` §11 and `docs/CONFIGURATION.md` §10 now state the
  SAS-token/managed-identity exclusion's reason inline (token-provider verifiability / UC
  wiring outside this repo's control) rather than only pointing at the source docstring for it.
  Did **not** remove the `account_key` auth mode — see section 5's decision 1 for why, and for
  the literal-vs-intended-scope question this surfaced.
- **D-13 (UC Volume paths).** `sources/file/spec.py` (`storage_ref` moved out of
  `required_keys`), `sources/file/config.py` (`_storage()`, `FileConfig.is_uc_volume_path`,
  `full_source_path` returns the Volume path verbatim), `sources/file/run.py` (`run_streaming`
  applies zero storage options/session config for a Volume-governed source, via a small
  `_no_op_restore` rather than a special-cased `None`). Both forms coexist: the shipped worked
  example (`file_claims_inbound.yaml`, `account_key` auth) is untouched. Tested: a Volume path
  needs no `storage_ref` and applies no credentials (proven by stopping `build_stream_reader`
  with a sentinel exception right after the storage-option branch, so the assertion does not
  need a real Structured Streaming query — the same distance every other test in
  `test_file_run.py` keeps from that boundary); setting both is a config error naming both;
  omitting `storage_ref` for a non-Volume path is still a required-setting error. Documented in
  the onboarding template (both forms, Volume preferred), `docs/CONFIGURATION.md` §10, and
  `docs/DESIGN.md` §11 (including the planned simplification — `conf/storage.yaml`,
  `sources/file/security.py`, `framework/security.py::apply_session_options` all become
  deletable if VB-28 comes back "Volumes everywhere"). `tests/test_shipped_config.py`'s two
  storage-register tests were hardened to handle `cfg.storage is None` correctly, since my own
  change introduced that possibility even though no shipped source uses it yet.
- **D-14 (Stage 5 decisions + `framework/checkpoint.py` settled).** Recorded in
  `DECISIONS.md`. No code to revisit — nothing in this stage's work touched any of the five
  Stage 5 decisions or the checkpoint guard's own shape.

### A CI defect found and fixed while already in `azure-pipelines.yml`

`framework/audit.py` imports `pyspark.sql.types` at module level (confirmed by grep; every
source's own `run.py` or a sibling module does too), so merely *collecting* the fast suite —
not running a single test — requires the `pyspark` package to be importable. The CI
"Install package and dev dependencies" step ran `pip install -e ".[dev]"` only, which does
**not** include `pyspark` — `docs/RUNBOOK_DEVELOPER.md`'s own local-setup line already installs
it as a separate `pip install -e ".[dev]" pyspark` step, confirming this was always meant to be
a second, explicit install, just never mirrored into CI. As written, the pipeline's own "Unit
tests" step could not have collected a single test. Fixed by pinning `pyspark==3.5.2` in the
same install line, matching VB-14 and the project's own `.venv` — not left unpinned, so a
future "latest" resolution cannot silently drift from the runtime the code is written against.
This was already inside `azure-pipelines.yml`, the exact file this stage's file list names, so
fixing it here rather than flagging-and-walking-past was the smaller total change.

---

## 2. Done but not verifiable here

- **VB-28 (new)** covers D-13's own open question: whether Unity Catalog Volumes or
  `abfss://` is the right answer per environment, and whether the target compute can actually
  reach a Volume the way it needs to. Both forms are implemented and tested; which one a real
  environment should use is not decided by this stage, deliberately.
- Every existing VB entry's own "how to check" — none of Stage 6's work depended on Kafka,
  Oracle, ADLS or a Databricks workspace being reachable; everything built and run this stage
  is pure Python, YAML and AST parsing.
- Whether `azure-pipelines.yml`'s corrected dependency-install step actually produces a green
  pipeline on a real Azure DevOps agent — there is no CI runner here, only the local
  reproduction of the grep script's own logic and the fact that `pip install pyspark==3.5.2`
  is exactly what a local `.venv` already does successfully. Worth a first real PR to confirm.

---

## 3. Not reproduced

- **STAGE_6_gates.md's offline-validation item 5 asks that `structural_keys` and
  `operational_keys` be disjoint sets.** This does not hold against the current code, and
  should not be forced to: `CORE.md` section 4.2's own `contracts.py` docstring documents a
  key in *both* sets as the ordinary case ("settable in YAML and overridable at run time
  without a deploy"), and every shipped `SOURCE_SPEC` relies on exactly that overlap —
  Kafka's `failure_mode` / `max_offsets_per_trigger`, Oracle's `fetch_size` / `num_partitions`
  / `incremental_mode`, File's `failure_mode` / `max_files_per_trigger`. `docs/build_log/
  DECISIONS.md` D-01 built this deliberately and Stages 2b through 5 each shipped a spec
  depending on it, with dedicated tests. Asserting literal disjointness would either fail
  immediately against three stages' worth of settled, tested design, or force silently gutting
  it — both of which CORE's own rule forbids ("the fast suite must be green before and after
  every stage... if a stage makes it red, the stage is wrong, not the test"). Implemented only
  the other half of item 5 (`required_keys ⊆ structural_keys`, true and valuable on its own),
  and recorded the reasoning in the test module's own docstring as well as here, rather than
  silently dropping half the ask or silently breaking three sources to satisfy it.
- **D-10's literal claim that "the generic `rerun_id` ... gives you \[a fresh checkpoint]"
  does not hold, checked against the actual code.** `framework/audit.py`'s `AuditWriter`
  reads the control table's `replay_rerun_id` (mapped to the framework-owned `rerun_id`
  setting) in exactly one place — to label the audit row — and nothing in `sources/file/
  config.py` or `run.py` reads it at all. The file source's `VALID_RUN_TYPES` is `("primary",)`
  only, so `run_type` can never fork into a replay shape the way Kafka's does (Kafka's replay
  entrypoint derives a *separate* checkpoint directory per `rerun_id`; the file source has no
  such derivation). Setting the generic `replay_rerun_id` column for a file source's control
  row today only tags the audit row — it does not touch the checkpoint or the read path.
  `file_checkpoint_reset_id`, used together with the checkpoint being genuinely absent, is the
  lever that actually matters. Documented the verified mechanism rather than the literal
  instruction text, in `docs/CONFIGURATION.md`, `docs/RUNBOOK_SUPPORT.md` and `docs/DESIGN.md`
  — a runbook step that does not do what it claims is worse than an admittedly-narrower true
  one, and CLAUDE.md's honesty invariant is explicit that a key/mechanism that silently does
  nothing is the worst possible output here.
- **D-12's heading, "storage auth is service principal only," does not match the shipped
  code.** Stage 5 built and shipped two auth modes — `account_key` and `service_principal` —
  and the repository's own worked example (`conf/sources/file_claims_inbound.yaml`) uses
  `account_key`. The instruction's own elaboration names only SAS tokens and managed identity
  as excluded, and its action is "document... as a stated limitation," not "remove a mode." Read
  the heading as an imprecise restatement of Stage 5's decision 9 rather than a mandate to
  retire a shipped, tested auth mode, and implemented the documentation accordingly — see
  section 5, decision 1, for the case this reading could be wrong.

---

## 4. Blocked

Nothing. Every item in STAGE_6_gates.md's "Work" and "Exit gate" sections, and all five
additions, has a corresponding file, test, or documented decision above.

---

## 5. Decisions for the human

1. **D-12's "service principal only" was read as excluding SAS tokens and managed identity,
   not `account_key`.** `account_key` is a shipped, tested Stage 5 auth mode, used by the
   repository's own worked example. Removing it is a real code and config change — a
   migration for anything already using it, even though nothing is deployed today — and the
   instruction's own body only asks for documentation. Implemented the conservative,
   non-destructive reading.
   *What would change it:* explicit confirmation that `account_key` should be retired for new
   onboarding going forward. If so, that is a follow-up pass: remove the mode from
   `StorageProfile`/`conf/storage.yaml`'s pattern, decide what happens to
   `file_claims_inbound.yaml`'s existing use of it, and update every place this stage just
   documented both modes.
2. **D-11 covers the file source's maintenance gap only, per the literal instruction — Oracle's
   remains open.** `docs/build_log/STAGE_5_REPORT.md` decision 8 already flagged Oracle's
   `oracle_claim_header` landing table as sharing this gap with the file source, calling it
   "worth a dedicated pass." This stage closes only the half it was asked to.
   *What would change it:* a follow-up instruction to close Oracle's half too — mechanically
   identical to what this stage did for the file source (one task in `job_maintenance.yml`, no
   SQL change).
3. **The `rerun_id`/`replay_rerun_id` finding (section 3) may be worth a small, separate follow-
   up: should the framework refuse to accept `replay_rerun_id` for a source type whose
   `VALID_RUN_TYPES` has no replay shape, the same way `checkpoint_reset_id` is refused when
   set in YAML for the wrong reason?** Today it is silently accepted and merely unused for
   File, which is not incorrect (D-05 makes `rerun_id` a framework-owned, generically-available
   key) but is exactly the kind of "accepted but does nothing" shape CLAUDE.md's honesty
   invariant warns about, one layer down from a config key — this is a control-table value, not
   a YAML key, so the existing "operational-only key rejected in YAML" mechanism does not
   apply to it as-is.
   *What would change it:* if a second source type ever ships with no replay shape either
   (making this a recurring rather than one-off question), or if a support engineer is ever
   observed setting the generic column expecting the fork Kafka's gives them.
4. **The offline-validation "structural/operational disjoint" question (section 3) is worth
   a direct read from whoever wrote STAGE_6_gates.md.** If it was meant literally, that is a
   materially different architecture change (removing the "in both sets" case from three
   already-shipped specs) and deserves its own decision entry, not a quiet Stage 6 patch. If it
   was a drafting slip against CORE 4.2's actual design, no further action is needed beyond
   what this report already records.
5. **`azure-pipelines.yml`'s pyspark fix (section 1) has not been confirmed against a real
   Azure DevOps agent.** The reasoning (module-level `import pyspark.sql.types` in
   `framework/audit.py`, confirmed by direct grep) is airtight as far as Python's import
   semantics go, but "the pipeline goes green" is a claim only a real CI run can make.
   *What would change it:* the first PR against this branch actually running the pipeline.

---

**Test count:** 865 passed, 7 skipped, 36 deselected before → **947 passed, 7 skipped, 36
deselected** after (`pytest -m "not spark" -q`). Net +82: 50 in the new
`tests/test_offline_validation.py`, 7 in `sources/file` config/run tests (D-13), 8 in the
maintenance-coverage drift checks (D-11), 17 in the cross-product completeness additions.

**New VB entries this stage:** VB-28 (will file sources use Unity Catalog Volumes or
`abfss://` in each environment, and are Volumes reachable from the target compute — D-13).
Every other entry (VB-01 through VB-27) was re-read, confirmed still filled in per the CORE
section 3 format, and re-ordered by damage into one list rather than left as the original
seeded batch plus a numerically-appended tail — see `docs/VERIFICATION_BACKLOG.md`'s own
intro for the four tiers and the reasoning.
