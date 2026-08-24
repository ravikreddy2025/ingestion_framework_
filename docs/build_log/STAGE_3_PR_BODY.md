# Stage 3: Kafka source

`gh` is not available on this machine, so this is the PR body to paste at
https://github.com/ravikreddy2025/ingestion_framework_/pull/new/stage-3-kafka
with the title **Stage 3: Kafka source**. Delete this file once the PR is open.

---

The nine top-level Kafka modules become one package behind `SOURCE_SPEC` and `run(ctx)`.
The legacy five-layer loader is gone, so `framework/config.py` is now the only configuration
loader in the repository, and the CORE section 7 grep over `framework/` returns nothing.

Full reasoning: `docs/build_log/STAGE_3_REPORT.md`.

## 1. Done and verified

- **`checkpoint_reset_id` is single-use.** The reset works by forking the Delta transaction
  identity; reusing an id keeps the old one, and every write would be skipped as a duplicate
  while the run reported success. The guard now refuses that state and names the spent id.
  The id is recorded on the audit row via `rerun_id`, which `run_type` disambiguates.
- **All four reader options always present.** `includeHeaders` fixed at `"true"` and no
  longer configurable; `max_offsets_per_trigger` and `min_partitions` required with platform
  defaults and rejected at zero; `failOnDataLoss` stays structural.
- **The curated MERGE carries an `event_date` bound.** Landing's deliberately does not, and
  the call site says why: a replayed row carries today's `ingest_date` while its target twin
  carries the day it arrived, so any bound from the batch would match nothing and insert
  duplicates.
- **Malformed payloads triaged into three reasons** - tombstone, truncated, bad magic byte -
  because they are three different producing-team conversations.
- **Job hardening**: `max_concurrent_runs: 1` and `queue.enabled: false` on every template,
  `max_retries: 3` on the primary tasks, `retry_on_timeout` stated per task.
- **`unpersist()` in a `finally`** (already true; now pinned by a test), the listener drain
  reads `query.recentProgress` instead of sleeping, and pending work is recorded per run.
- **Three additions from the request**: `security.py` split into a framework half and a Kafka
  half; `operational_keys` populated with `failure_mode` / `max_offsets_per_trigger` /
  `checkpoint_reset_id`, with `on_deser_error` renamed to `failure_mode`; and
  `resources/job_maintenance.yml` now passes `ops_catalog`, which Stage 2b flagged as blocked.
- **Nineteen mutations run** to prove the new tests can fail, each restored - the table is in
  the stage report.

## 2. Done but not verifiable here

- The three malformed-payload **quarantine** tests are written but skip: they need the
  `spark-avro` connector. The column expression under them **did** run and pass on a local
  Spark 3.5.2 (VB-18).
- VB-05 now has `pending_work` depending on it as well as the offset columns.
- VB-10 (the `from_avro` self-check) unchanged and still unrun, same connector reason.

## 3. Not reproduced

- `unpersist()` was **already** in a `finally` - the stage file reads as though it were not.
- **A local JVM Spark is available on this machine**, contrary to CORE section 3's
  assumption: `pytest -m spark` gives 6 passed, 27 skipped. The gate is unchanged.
- Every legacy Kafka test file was **replaced**, not edited - the modules they imported no
  longer exist. Coverage is carried over test-for-test.
- `batch_id` on the **data** rows was renamed `txn_version` too; D-03 renamed only the audit
  column, and leaving the two disagreeing is the drift D-03 exists to remove.

## 4. Blocked

Nothing. Three things deliberately not done: the Stage 7 documentation set (`DESIGN.md`,
`NAVIGATION.md`, `README.md`, `notebooks/`) still describes the retired layout; nothing from
the stage file's "do not build" list was built; `sql/04`'s literal `'audit'` schema name is
left as the one hardcoded schema outside `conf/`.

## 5. Decisions for the human

1. **The reset id reuses the `rerun_id` audit column** rather than adding one - `run_type`
   disambiguates, because a reset only ever happens on a primary run.
2. **The landing MERGE passes `partition_predicate="true"`**, the one use of
   `writers.merge()`'s escape hatch, because no correct bound is derivable.
3. **`failure_mode` uses `FAILFAST`/`QUARANTINE`** to match the CHECK constraint already in
   `sql/01`.
4. **The replay job's `max_concurrent_runs` drops 3 -> 1**, as the stage file instructs. This
   narrows an existing capability: two different sources can no longer be recovered in
   parallel from that job. Flagged because it is the one place following the instruction
   costs something operationally.
5. **`batch_id` -> `txn_version` on the data rows.**
6. **`pyproject.toml`'s note about the formatter is now false** and was left alone.
7. **`framework/security.py` re-exports `redact()`** rather than owning the implementation.

---

**Test count:** 415 passed, 34 deselected -> **487 passed, 36 deselected**
(`pytest -m "not spark" -q`).

**Gate:** `ruff check` clean, `ruff format --check` clean (the 22-file baseline was the
legacy modules this stage deleted; the seven new files the formatter disagreed with were
formatted individually, and no pre-existing file was touched).

**New VB entries:** VB-18 (the magic-byte comparison on the target DBR). VB-05 rewritten to
cover `pending_work`.
