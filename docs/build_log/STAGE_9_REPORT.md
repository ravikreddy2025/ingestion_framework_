# Stage 9 Report — Explicit `access_mode` for the file source (D-15)

Read against `CLAUDE.md`, `.claude/build/CORE.md` and every file in `docs/build_log/`
(STAGE_0 through STAGE_8, and `DECISIONS.md` D-01 through D-14). This is not one of the
eight staged passes in `.claude/build/` — no `.claude/build/STAGE_9_*.md` exists — it was
specified directly, in the same spirit as the D-01..D-09 work list and the Stage 5b/Stage 8
passes. Stage 8 was green (976 passed, 7 skipped, 36 deselected) before this stage started.

**Files created:** `conf/sources/file_membership_eligibility.yaml` (the second worked
example — `access_mode: volume`), this report.

**Files edited:** `docs/build_log/DECISIONS.md` (new D-15), `src/kafka_ingest/sources/file/
spec.py`, `src/kafka_ingest/sources/file/config.py`, `src/kafka_ingest/sources/file/run.py`,
`conf/sources/file_claims_inbound.yaml`, `conf/sources/_TEMPLATE_file.yaml`,
`resources/job_ingest_file.yml`, `docs/CONFIGURATION.md`, `docs/DESIGN_FILES.md`,
`docs/NAVIGATION.md`, `docs/RUNBOOK_SUPPORT.md`, `docs/VERIFICATION_BACKLOG.md`,
`notebooks/02_check_connectivity.py`, `tests/conftest.py`, `tests/test_file_config.py`,
`tests/test_file_run.py`, `tests/test_file_spec.py`, `tests/test_offline_validation.py`,
`tests/test_shipped_config.py`.

---

## What changed, and why

`docs/build_log/DECISIONS.md` D-13 let a file source read from a Unity Catalog Volume
instead of an ADLS container — but *which* form applied was **inferred** from whether
`source_path` happened to start with `/Volumes/`. The task asked for an explicit
`access_mode: volume | adls` switch instead, with the two forms' keys mutually rejecting
across modes, validated at config load, mirroring how Oracle's `sql_query` vs `columns`
exclusivity and `incremental_mode`'s cursor/filter requirements are validated. Recorded as
**D-15** in `DECISIONS.md` before implementing, per CLAUDE.md's working method — D-13's
mechanism is *superseded*, not removed outright: the capability (read from a Volume with no
credentials) is unchanged, only how a source declares which form it uses.

**The mechanism, in one sentence:** `access_mode` is a new required, structural-only key;
`volume` mode requires `volume_path` and rejects `storage_ref`/`source_path`; `adls` mode
requires `storage_ref` + `source_path` and rejects `volume_path`; the validation lives in
`sources/file/config.py::_access()` (mode-conditional required/rejected checks with a
message naming the key and the mode), not in `SourceSpec.mutually_exclusive` — that
framework field only expresses "at most one of these two keys may be set" and cannot express
"required in mode X, forbidden in mode Y" with the mode named in the message, which the task
explicitly asked for. This is the same shape `sources/oracle/config.py::
_check_mode_requirements` already uses for `incremental_mode`'s cursor/filter requirements —
a source's own config module owns a cross-field rule that depends on a *value*, not merely
on which keys are present.

---

## 1. Done and verified

Command whose output proves each claim: `ruff check src tests`, `ruff format --check src
tests`, `pytest -m "not spark" -q` — all three green throughout, final state: **1003
passed, 7 skipped, 36 deselected** (was 976 passed, 7 skipped, 36 deselected — **+27**, 0
removed, 0 modified-in-place beyond the ones the redesign itself required).

**The mechanism**
- `sources/file/spec.py` — `access_mode` added to both `required_keys` and
  `structural_keys`; `volume_path` added to `structural_keys`; `source_path` **removed**
  from `required_keys` (still structural, now conditionally required — it is not required
  under `access_mode: volume`). Module docstring rewritten to describe the mode-conditional
  rule instead of D-13's shape-inference rule. Verified:
  `tests/test_file_spec.py::test_every_declared_key_is_actually_read_by_this_package`,
  parametrized over the full structural+operational key set including the two new keys, and
  `test_what_is_read_and_where_it_lands_is_never_operationally_overridable`, extended to
  cover `access_mode` and `volume_path` alongside `storage_ref`/`source_path`.
- `sources/file/config.py` — `FileConfig.access_mode: str` and `.volume_path: str | None`
  added; `.storage_ref` / `.storage` / `.source_path` are now `None` under `access_mode:
  volume` (was: `None` when `source_path` looked like a Volume path). The shape-inference
  `_storage()` is replaced by `_access()`, which validates the four keys against
  `access_mode`'s value and returns `(access_mode, volume_path, storage_ref, storage,
  source_path)`. `full_source_path` branches on `access_mode` instead of `cfg.storage is
  None`. `volume_path`'s shape (`/Volumes/<catalog>/<schema>/<volume>/...`, three non-empty
  segments then a trailing path) is checked with a new `_VOLUME_PATH_SHAPE` regex at config
  load — the `{catalog}` placeholder inside it is already resolved by
  `framework/config.py::_substitute` before this module ever sees the value (an ordinary
  `vars:` placeholder, not a deferred `target_token`), so an unresolved `{catalog}` was
  already a hard error before this stage; nothing new was built for that half beyond the
  shape check itself. `is_uc_volume_path` is removed — every caller now compares
  `access_mode` directly. Verified: `tests/test_file_config.py`'s rewritten Volume-mode
  section (13 tests: needs no `storage_ref`, reads as-is with no `abfss://` wrapping,
  rejects `storage_ref` set under `volume` mode naming the key and the mode, rejects
  `source_path` set under `volume` mode naming the key and the mode, requires `volume_path`
  under `volume` mode, four malformed-shape cases rejected, `adls` mode still requires
  `storage_ref`/`source_path`, rejects `volume_path` set under `adls` mode naming the key
  and the mode, an unknown `access_mode` value is rejected, and the fixture default resolves
  as `adls`/`volume_path is None`).
- `sources/file/run.py` — the ADLS-session-options branch now compares
  `self.cfg.access_mode == file_config.ACCESS_MODE_VOLUME` instead of `self.cfg.storage is
  None` (the two were always equivalent by construction, but the explicit comparison is what
  the task asked for — "`apply_session_options` is never called for it" reads directly off
  `access_mode` now, not off a derived `None`). `ctx.log.info("file_run_resolved", ...)`
  gained `access_mode=cfg.access_mode`. Docstrings updated from D-13 to D-15. Verified:
  `tests/test_file_run.py::test_a_volume_source_applies_no_storage_options`, rewritten to
  set `access_mode="volume"` / `volume_path=...` / `storage_ref=None` / `source_path=None`
  instead of inferring Volume-ness from `source_path`'s prefix.

**Two shipped file sources, one per mode, covering the cross-product test**
- `conf/sources/file_claims_inbound.yaml` — `access_mode: adls` added; everything else
  (its `storage_ref` / `source_path` form) is unchanged, per the same restraint
  `docs/build_log/DECISIONS.md` D-12/D-13 already applied to this file — switching a
  shipped, working example was not asked for.
- `conf/sources/file_membership_eligibility.yaml` (new) — `access_mode: volume`, reading
  `/Volumes/{catalog}/landing/membership/eligibility_inbound/` directly, a distinct landing
  zone (not a second config pointed at the same `claims/inbound` data) so the two worked
  examples do not collide on meaning or on `landing_table`. `resources/job_ingest_file.yml`
  gained a matching `file_membership_eligibility` task, mirroring `file_claims_inbound`'s
  task shape exactly, with a one-line comment on why it needs no ADLS NCC egress. Verified:
  `tests/test_shipped_jobs.py`'s existing file-job tests, parametrized automatically over
  every task in the YAML (no test hardcodes a task count).
- **The cross-product requirement is met by construction**, not by a new test: `FILE_KEYS`
  / `FILE_ENVS` in `tests/test_shipped_config.py` are already derived by globbing
  `conf/sources/*.yaml` and filtering on `source_type: file`, so the new source
  automatically joined every existing file-source test across all three environments.
  Confirmed directly: `pytest --collect-only -k file_membership_eligibility` lists 15 cases
  across `dev`/`preprod`/`prod`, including `test_every_shipped_file_source_resolves` and
  `test_no_placeholder_survives_anywhere_in_a_resolved_file_config` for all three
  environments — proving `{catalog}` inside `volume_path` resolves correctly per
  environment, which is the concrete form of "confirm the catalog placeholder resolves per
  environment" the task asked for.
- `tests/test_shipped_config.py` — three tests that read `cfg.is_uc_volume_path` now read
  `cfg.access_mode == file_config.ACCESS_MODE_VOLUME` instead (storage-account reachability,
  storage-register-reference, placeholder sweep). A fourth,
  `test_the_source_side_identity_never_varies_by_environment`, needed more than a
  find/replace: comparing `cfg.source_path` across environments is vacuously true for a
  Volume-mode source (`source_path` is `None` in every environment by construction), which
  would have silently stopped testing identity for that mode. Rewritten to branch on
  `access_mode`: `adls` mode still compares `source_path` directly; `volume` mode strips the
  environment-varying `/Volumes/<catalog>/` prefix (a Volume path legitimately differs by
  environment there, since a Volume is catalog-scoped, unlike `source_path` which
  deliberately carries no catalog at all) and compares the schema/volume/path suffix that
  must not vary. This is a **new finding**, not something the task named directly — see
  "Not reproduced" below.

**Onboarding and docs**
- `conf/sources/_TEMPLATE_file.yaml` — the two source-path forms rewritten as one
  `access_mode: <volume|adls>` line plus two commented blocks, matching the task's own
  example shape. Item 5 of the numbered onboarding checklist rewritten around the explicit
  choice.
- `docs/CONFIGURATION.md` §10 (Files) — the MUST CHANGE table's `storage_ref`/`source_path`
  row replaced with three rows (`access_mode`, `volume_path`, `storage_ref`/`source_path`);
  the "Unity Catalog Volume source paths" and "Storage register" subsections reworded around
  `access_mode` instead of a Volume-shaped `source_path`; the operational-overrides
  paragraph's structural-key list gained `access_mode`/`volume_path`.
- `docs/DESIGN_FILES.md` — the D-13 section rewritten as "`access_mode`, and the
  simplification it sets up (D-15, supersedes D-13's shape-inference)", stating the
  mode-conditional rejection mechanism, naming both shipped worked examples, and restating
  the VB-28-gated "planned simplification" (delete `conf/storage.yaml`'s auth block,
  `sources/file/security.py`, `framework/security.py::apply_session_options` if Volumes win
  everywhere) against the new key — the task's explicit ask.
- `docs/NAVIGATION.md`, `docs/RUNBOOK_SUPPORT.md`, `notebooks/02_check_connectivity.py` —
  every "decides from `source_path`'s shape" phrase replaced with "`access_mode` decides";
  the bounded-re-read checklist in `RUNBOOK_SUPPORT.md` §9.7 now names `volume_path`
  alongside `source_path` as the field to narrow, since the old text named only
  `source_path`, which is `None` for a Volume-mode source.
- `docs/VERIFICATION_BACKLOG.md` — **VB-28 rewritten** per the task: title and body now ask
  "which `access_mode` does each environment use" instead of inferring the question from
  `source_path`'s shape; its "How to check" / "If it fails" sections point at
  `access_mode`/`volume_path` instead of a Volume-shaped `source_path`. **New VB-30**: does
  Auto Loader's stream checkpoint and `cloudFiles.schemaLocation` behave identically when
  the *stream source itself* reads from a Volume path (`access_mode: volume`) as they do
  reading `abfss://` (`access_mode: adls`) — explicitly scoped as a *different* question from
  VB-08 (whether a UC Volume works as a checkpoint *location* at all, already OPEN, tier 3 —
  loud) and from VB-06 (per-format `rescuedDataColumn`/`_metadata` behaviour, already OPEN).
  Placed in tier 1 (silent, per-row data corruption) with a note explaining the placement
  against VB-08's tier-3 placement, and its "How to check" names the two new shipped sources
  by key so whoever runs it has a concrete pair to compare rather than a hypothetical one.

**Gate**
- `ruff check src tests` → `All checks passed!`
- `ruff format --check src tests` → `85 files already formatted`
- `pytest -m "not spark" -q` → `1003 passed, 7 skipped, 36 deselected`
- CORE section 7 grep gate, re-run after every edit to `sources/file/*.py`:
  `grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ | grep -v 'runner.py:.*_SOURCES'`
  returns nothing — this stage touched only `sources/file/` and `docs/`/`conf/`/`tests/`,
  never `framework/`.
- **Two deliberate breaks, watched fail, then restored**, proving the new tests can fail:
  1. Emptied the `access_mode == volume` rejection loop in `_access()` — both
     `test_a_volume_source_with_storage_ref_set_is_rejected` and
     `test_a_volume_source_with_source_path_set_is_rejected` failed with `DID NOT RAISE
     ConfigError`; restored from a pre-edit copy, re-ran `ruff check`/`format --check`/full
     suite, all green again.
  2. Replaced the `_VOLUME_PATH_SHAPE.match(...)` check with `if False:` — all four
     parametrizations of `test_a_malformed_volume_path_is_rejected` failed with `DID NOT
     RAISE ConfigError`; restored the same way, gate re-confirmed green.
- `python -c "import ast; ast.parse(...)"` on every edited `.py` file and
  `yaml.safe_load(...)` on every edited/new `.yaml`/`.yml` file, confirming syntactic
  validity independent of the test suite (belt and braces — the test suite already exercises
  all of these, but this is what CORE's "never claim to have run something" bar asks for
  when a file is edited by hand).

## 2. Done but not verifiable here

- **VB-28 (rewritten) and VB-30 (new) both need a real Databricks workspace** — no Kafka,
  Oracle, ADLS, Databricks or JVM available locally, per CORE section 3. Nothing about
  either was run or claimed to run; both are written against the real, current framework
  functions and shipped sources.
- **`sources/file/reader.py` and `landing.py` are unchanged by this stage** — `cfg.
  full_source_path` already abstracted the two forms before this stage, so Auto Loader's own
  `.load(cfg.full_source_path)` call needed no edit. This stage did not re-verify VB-06/VB-26
  (already OPEN, unaffected by this redesign) — see VB-30 for the one *new* unverified
  question this stage's own change actually introduces.

## 3. Not reproduced

- **The task's instruction did not mention `test_the_source_side_identity_never_varies_by_
  environment`**, but leaving it as a literal find/replace (`is_uc_volume_path` →
  `access_mode == ACCESS_MODE_VOLUME`) would have made it silently stop testing anything for
  a Volume-mode source, since `cfg.source_path` is `None` in every environment by
  construction under that mode — a vacuously-true assertion, not a real check. Rewritten as
  described in "Done and verified" above. Recorded here per CORE section 9's "a useful
  result, not a failure": the fix is in the code and `pytest --collect-only -k
  file_membership_eligibility` confirms the rewritten test now actually runs against the new
  shipped source.
- **Everything else in the task's own requirements list has a corresponding edit** — no
  other deviation found.

## 4. Blocked

Nothing. Every requirement in the task list has a corresponding file, test, or documented
decision above.

## 5. Decisions for the human

1. **`file_membership_eligibility` is a genuinely new worked example**, not the same
   `claims/inbound` landing zone read a second way. The task's own illustrative example used
   `volume_path: /Volumes/{catalog}/landing/claims/inbound/` for the Volume-mode block, which
   reads as the same data as `file_claims_inbound.yaml`'s existing `adls`-mode example. Two
   shipped sources both named "claims inbound," one per `abfss://` and one via a Volume,
   pointed at what would in reality be the same files, would either double-ingest that data
   into two different landing tables or read as a copy-paste artifact rather than a genuine
   second onboarding — so a distinct landing zone (`membership/eligibility`) was chosen
   instead, clean but different from the task's own illustrative path. **What would change
   it:** if the task's `claims/inbound` volume path was meant literally (e.g. the same
   landing zone genuinely is reachable both ways in some environment, and both forms should
   be exercised against it), rename `file_membership_eligibility.yaml` to something under
   `files_claims`/`claims_inbound_volume` and repoint `volume_path` — a small follow-up, not
   a design change.
2. **`_VOLUME_PATH_SHAPE`'s regex requires a non-empty path after the three
   catalog/schema/volume segments** (`/Volumes/<catalog>/<schema>/<volume>/<something>`) —
   a bare `/Volumes/<catalog>/<schema>/<volume>/` with nothing after it is rejected as
   malformed. This was a judgement call, not named in the task: a Volume path with nothing
   after the volume name would read the entire Volume root, which is a plausible real
   configuration, not necessarily a mistake. **What would change it:** if that shape should
   be legal, drop the trailing `/.+` requirement — a one-character regex change, flagged now
   rather than guessed at silently. Kept the stricter interpretation because a config typo
   that truncates a real path down to the Volume root (dropping a landing-zone subpath by
   accident) is exactly the "config key that silently does nothing" failure CLAUDE.md's
   honesty invariant most wants caught at load time.
3. **VB-30's placement in tier 1 rather than tier 3** — it sits next to VB-08 conceptually
   (both are about Volume paths and Auto Loader) but VB-08 is tier 3 (loud: a checkpoint
   location that plain does not work fails to start) while VB-30 is tier 1 (silent: two
   checkpoint mechanisms that both start but disagree quietly on what "already seen" means).
   **What would change it:** if whoever answers VB-28 first confirms Volumes are viable as a
   checkpoint location at all (closing VB-08), VB-30 becomes the more urgent of the two and
   could reasonably move earlier within tier 1 — not attempted here since VB-08 is still
   OPEN and re-ordering ahead of an open, unrelated question would be guessing at its answer.

---

**Test count:** 976 passed, 7 skipped, 36 deselected before → **1003 passed, 7 skipped, 36
deselected** after. Net **+27**: 13 in `tests/test_file_config.py`'s access-mode section (up
from 5 pre-existing D-13 tests it replaced — a net +8 there), 1 in `tests/test_file_spec.py`
(the widened key-parametrization), the rest distributed across `tests/test_shipped_config.py`
(the second shipped file source joining every existing FILE_ENVS-parametrized test) and
`tests/test_offline_validation.py` (the new source file's YAML-parse and notebook-import
sweeps).

**New VB entries added this stage:** VB-30 (does Auto Loader's checkpoint/schemaLocation
behave identically under `access_mode: volume` vs `access_mode: adls`). VB-28 was rewritten
in place, not replaced with a new id, since it is the same underlying question the task
asked to rephrase.
