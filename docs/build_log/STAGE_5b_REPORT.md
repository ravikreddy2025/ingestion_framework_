# Stage 5b Report — Hoist the checkpoint-reset guard into framework/

Read against `CLAUDE.md`, `.claude/build/CORE.md` and every file in `docs/build_log/`. This
is not one of the eight staged passes in `.claude/build/` — it is a short, self-contained
pass requested directly, in the same spirit as the D-01..D-09 work list `DECISIONS.md`
describes as "apply as a short pass on its own branch." Stage 5 was green (866 passed, 7
skipped, 36 deselected) before this pass started.

---

## 1. Done and verified

Command whose output proves each claim: `ruff check src tests`, `ruff format --check src
tests`, `pytest -m "not spark" -q` — all three green throughout, final state: **865 passed,
7 skipped, 36 deselected**.

**The hoist**
- `framework/checkpoint.py` (new) — `guard_against_checkpoint_reset(ctx, *, checkpoint_path,
  landing_table, checkpoint_reset_id, is_replay, control_column)`, plus its two private
  helpers `_checkpoint_offsets_exist` and `_reset_id_already_used`, and one new public
  helper, `control_column_for(spec, setting)` — the reverse lookup against
  `SourceSpec.control_columns` the task asked for, so the caller (not this module) resolves
  its own control-table column name. Ten framework modules become eleven; CORE section 7's
  "target ten" is a guideline the section itself weighs against the readability bar, and a
  ~155-line module doing one clearly-named job read in under a minute is what that bar asks
  for — recorded as a judgement call in §5.
- `sources/kafka/run.py` and `sources/file/run.py` — `_checkpoint_offsets_exist`,
  `_guard_against_checkpoint_reset` and `_reset_id_already_used` deleted from both (import
  os` dropped from both, now unused). Each `run()` calls
  `checkpoint_guard.guard_against_checkpoint_reset(...)` with its own `cfg.checkpoint_path`,
  `cfg.landing_table`, `cfg.checkpoint_reset_id`, `cfg.is_replay`, and a
  `_RESET_ID_CONTROL_COLUMN` computed once at import time via
  `checkpoint_guard.control_column_for(SOURCE_SPEC, CHECKPOINT_RESET_ID)` — resolving to
  `"kafka_checkpoint_reset_id"` and `"file_checkpoint_reset_id"` respectively, confirmed by
  a smoke import (`python -c "...control_columns..."`) and by
  `tests/test_framework_checkpoint.py::test_control_column_for_reverse_looks_up_*`.
- CORE section 7 grep gate re-run directly against `framework/` after every edit; clean
  throughout (one early docstring draft named a source and was reworded — see §3).

**The topic-filter question**
- Checked whether Kafka's `topic` filter on the "already landed" check
  (`.where(f"topic = '{cfg.topic}'")`) is still needed now that landing is one table per
  source. It is not, and hasn't been since landing tables stopped being shared: `conf/
  defaults/kafka.yaml` resolves `landing_table` from `"{catalog}.landing.{topic_table}"` —
  one physical table per topic — and `docs/DESIGN.md` §5 ("Partitioning, not Liquid
  Clustering") already states this explicitly ("landing (ingest_date) — one table per
  topic, so `topic` is constant inside it"). Every row in a Kafka source's landing table
  already carries that source's own topic, so filtering on it and not filtering on it
  select the same rows — the filter was redundant defence, not real isolation, and dropping
  it does not change behaviour. The shared `guard_against_checkpoint_reset` therefore checks
  only "does `landing_table` hold any row at all," matching what the file source's guard
  already did. Updated `docs/DESIGN.md` §11 to record this (it previously gave the topic
  filter as the reason the two guards differed; they don't differ, and now don't need to).

**The merged messages**
- Both refusal messages (the reused-reset-id one and the no-reset-id-with-data one) merged
  into one text each, parameterised on `control_column` so the same string reads correctly
  for either source per the task's instruction. One deliberate content change beyond
  parameterisation: Stage 5's Kafka text advised "use the kafka replay job with a new
  rerun_id" — the file source has never had a replay job (`docs/build_log/
  STAGE_5_REPORT.md` decision 7), so a shared message asserting that mechanism
  unconditionally would invent a capability for file that does not exist, which CORE
  section 2 rule 2 forbids. Dropped rather than generalised into vague language; see §5.
- The reset-engaged log event lost its per-source prefix (`kafka_checkpoint_reset_engaged` /
  `file_checkpoint_reset_engaged` → `checkpoint_reset_engaged`) and its `txn_app_id` field.
  Every `RunLog` line already carries `source_type` and `source_key`
  (`framework/logs.py::RunLog.line`), so the prefix repeated information already on the
  line; `txn_app_id` was decorative and the two sources derive it differently, so keeping it
  out lets the shared function's parameters stay primitives only. Both changes recorded in
  §5. Updated the two `kafka_checkpoint_reset_engaged` references in
  `docs/RUNBOOK_SUPPORT.md` §5.4a to the new name.

**Tests**
- `tests/test_framework_checkpoint.py` (new, 15 tests) — the full behaviour matrix, run
  once against the shared function with generic stand-ins (no kafka/file config
  resolution): checkpoint intact / genuine first run / landing table absent / refusal with
  data / replay never blocked / unreadable Volume treated as an error not "missing" / fresh
  reset id warns and lets through / reused reset id refused / stale reset id inert once the
  checkpoint exists again / reuse check excludes the current run's own audit rows / reuse
  check treats a missing audit table as "never used" / `control_column_for`'s reverse
  lookup against both real specs. The reused-reset-id and fresh-reset-id tests are
  parametrized over both sources' real control-column strings to prove the same code path
  produces the right name for either.
- `tests/test_kafka_run.py` and `tests/test_file_run.py` — the guard section trimmed from
  12 and 9 tests respectively to 3 and 2: what is left proves WIRING (this source's own
  `checkpoint_path`, `landing_table`, `control_column` and — kafka only — `is_replay` reach
  the shared guard), not the guard's own logic, which is now proven once. Both files'
  module docstrings updated to say so.
- Verified two of the new/changed tests can fail: (1) temporarily made
  `framework/checkpoint.py::_reset_id_already_used` always return `False` — both
  parametrizations of `test_a_reused_reset_id_is_refused` failed with "DID NOT RAISE
  RuntimeError"; restored, suite green. (2) temporarily hardcoded
  `control_column="wrong_column"` in `sources/kafka/run.py`'s guard call —
  `test_the_guard_refuses_when_the_checkpoint_vanished_but_data_exists` in
  `tests/test_kafka_run.py` failed on the `"kafka_checkpoint_reset_id" in message`
  assertion; restored, suite green.
- Net test count: 21 guard tests (12 kafka + 9 file) → 20 (15 framework + 3 kafka + 2 file).
  One fewer test overall, not one fewer scenario: the "allows" cases (checkpoint intact /
  first run / landing table absent) were duplicated per source purely because the guard was
  duplicated per source; `KafkaConfig.checkpoint_path` / `.landing_table` derivation is
  already covered separately in `tests/test_kafka_config.py`, so nothing lost coverage.

---

## 2. Done but not verifiable here

Nothing new. This pass touched no behaviour that depends on a live Kafka, Oracle, ADLS or
Databricks workspace beyond what Stage 5 already flagged (VB-08 covers the checkpoint
Volume itself; this pass changes only what runs *around* that same `os.stat` probe, not the
probe's own environment dependency).

---

## 3. Not reproduced

- One early docstring draft in `framework/checkpoint.py` referenced `sources/kafka/run.py`
  by path in prose, which the CORE section 7 grep gate caught (the gate matches the literal
  string `kafka` case-sensitively, including inside a path in a comment). Reworded to
  describe "each checkpoint-based source's run()" generically instead — the same shape
  Stage 5's `framework/security.py::apply_session_options` docstring already had to work
  around, per that stage's report. Not a finding about the current code, just the gate
  doing its job during the edit itself; re-ran the grep after the fix and it was clean.

---

## 4. Blocked

Nothing. The task's two questions — hoist the three functions, and check whether Kafka's
`topic` filter is still needed — both had a clean, evidenced answer in the current code and
`docs/DESIGN.md`.

---

## 5. Decisions for the human

1. **`framework/checkpoint.py` is an eleventh framework module**, where CORE section 7 says
   "target ten framework modules." Considered folding the guard into `framework/state.py`
   or `framework/audit.py` instead — rejected both: the guard is not about `ingest_state`'s
   watermark/run-sequence store (state.py's stated job), and it only *reads* the audit
   table as best-effort evidence rather than owning audit's writer contract, so bolting it
   onto either would make that module's one job into two. A new, narrowly-scoped module
   matches the same section's readability bar ("a support engineer... must open any one
   module and understand its job without tracing indirection") better than a wider one
   would. **What would change it:** if a reviewer weighs the "ten modules" target as a hard
   cap rather than a guideline, folding this into `state.py` (the closer of the two
   conceptually — both are about run-to-run correctness rather than best-effort audit) is
   the fallback.
2. **Dropped Kafka's "use the kafka replay job with a new rerun_id" recommendation** from
   the merged no-reset-id-with-data refusal, rather than generalising it into vague
   "use this source's replay mechanism if it has one" language. The file source has no
   replay job (`STAGE_5_REPORT.md` decision 7), so asserting one unconditionally would be
   inventing an API per CORE section 2 rule 2, and a hedge vague enough to be true for both
   would tell a support engineer less than nothing does. **What would change it:** a file
   replay job, if one is ever built — at that point both sources have real recourse and the
   line can come back honestly.
3. **Dropped the per-source prefix and `txn_app_id` from the reset-engaged log event and
   its fields.** `RunLog` already stamps `source_type`/`source_key` on every line, so
   `checkpoint_reset_engaged` carries the same information `kafka_checkpoint_reset_engaged`
   did; `txn_app_id` was decorative and available on each source's own `*_run_resolved` line
   already. Updated the two literal references in `docs/RUNBOOK_SUPPORT.md` §5.4a.
   **What would change it:** if an operator's external log-search tooling (outside this
   repo) has a saved query keyed on the old prefixed event name, this is a breaking rename
   worth flagging to them before it ships — not something this repo can see or guard
   against itself.
4. **Test count went from 21 to 20** rather than staying flat or growing, because three
   "allows" scenarios that were duplicated per source (checkpoint intact / first run /
   landing table absent) collapsed into one generic version each in
   `tests/test_framework_checkpoint.py`. **What would change it:** if a reviewer wants every
   original per-source scenario re-proven explicitly rather than proven once generically
   plus a small wiring set, that's a defensible, more expensive alternative — flagging the
   tradeoff made rather than assuming it's the only reasonable one.

---

**Test count:** 866 passed, 7 skipped, 36 deselected before → **865 passed, 7 skipped, 36
deselected after**.

**New VB entries this stage:** none. No new unverifiable assumption was introduced; this
pass consolidates existing, already-flagged behaviour rather than adding any.
