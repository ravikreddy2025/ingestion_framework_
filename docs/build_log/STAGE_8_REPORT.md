# Stage 8 Report — Verification backlog into runnable probes, DESIGN.md split, brief reconciliation

Read against `CLAUDE.md`, every file in `docs/build_log/` (STAGE_0 through STAGE_7 plus the
Stage 2b and Stage 7 addenda, and `DECISIONS.md` D-01 through D-14), `docs/DESIGN.md`,
`docs/VERIFICATION_BACKLOG.md`, and `docs/RUNBOOK_DEVELOPER.md`. This stage was not run from
a `.claude/build/STAGE_8_*.md` brief — none exists; the work was specified directly. Stage 7
was green (949 passed, 7 skipped, 36 deselected) before this stage started.

**Files created:** `docs/DESIGN_KAFKA.md`, `docs/DESIGN_ORACLE.md`, `docs/DESIGN_FILES.md`,
this report.

**Files edited:** `docs/DESIGN.md`, `docs/NAVIGATION.md`, `docs/CONFIGURATION.md`,
`docs/RUNBOOK_SUPPORT.md`, `docs/RUNBOOK_DEVELOPER.md`, `docs/VERIFICATION_BACKLOG.md`,
`docs/ARCHITECTURE_OVERVIEW.md`, `docs/IMPORT_TO_DATABRICKS.md`, `README.md`,
`tests/test_offline_validation.py`, `notebooks/02_check_connectivity.py`,
`src/kafka_ingest/framework/audit.py`, `.gitignore`, `.claude/build/CORE.md`,
`.claude/build/STAGE_4_oracle.md`, `.claude/build/STAGE_5_files.md`,
`.claude/build/STAGE_6_gates.md`.

---

## Part A — the five "what I would change" items

### 1. Split `docs/DESIGN.md` by source

Done as specified. `DESIGN.md` now holds only the shared model: §1 architecture, §2 why the
source contract has one method, §3 the configuration model, §4 where to make common
changes, §5 testing, §6 the unverified-claims list, §7 deliberate non-abstractions, §8
adding a source type (with the BigQuery contract-notes subsection). Each source's own
re-run mechanics, failure-scenario table and design decisions moved verbatim into a sibling
file — `DESIGN_KAFKA.md`, `DESIGN_ORACLE.md`, `DESIGN_FILES.md` — content preserved
character-for-character except heading numbers (which no longer apply once each file has
its own, and were dropped rather than renumbered per-file, since a one-topic file does not
need a table of contents).

Every cross-reference to the old section numbers was found by grepping every `docs/*.md`
and `README.md` for `DESIGN.md#` / `DESIGN.md §` and updated: `README.md`,
`docs/NAVIGATION.md` (including the Oracle trace's anchor and the file map, "I want
to…" lookup, and the "two things that will save you an hour" list), `docs/CONFIGURATION.md`
(the two Files-section pointers), `docs/RUNBOOK_SUPPORT.md`, `docs/RUNBOOK_DEVELOPER.md`,
`docs/ARCHITECTURE_OVERVIEW.md`, `docs/IMPORT_TO_DATABRICKS.md` and
`docs/VERIFICATION_BACKLOG.md`. A second grep after editing (`grep -rn "DESIGN\.md.*§\(4\|5\|9\|10\|11\|12\)"`) returned nothing outside `docs/build_log/` (left untouched — historical).

**Onboarding time, re-measured honestly, not restated from memory:** `docs/DESIGN.md` is now
358 lines (was 729 — the exact module Stage 7 §9 named as the one pushing onboarding past two
hours). The sibling file a developer actually needs adds 74 lines (Oracle), 127 (Files) or
198 (Kafka), so a developer touching one source type now reads roughly 432–556 total lines
of design doc instead of 729 — genuinely shorter, and a real file boundary instead of a
scroll position to locate. This is a line-count fact, not a re-timed stopwatch estimate;
`docs/NAVIGATION.md`'s "ten-minute path" section states it exactly this way rather than
inventing a new "under N hours" claim to replace Stage 7's, which was reached by different
means (actually reading the whole thing) that this stage did not repeat.

### 2. Notebook import check in `tests/test_offline_validation.py`

Done as specified, as a sixth offline-substitute check (module docstring updated to record
why a sixth exists beyond CORE's original five). `_kafka_ingest_imports()` AST-parses every
`notebooks/*.py` file for `import kafka_ingest...` / `from kafka_ingest... import ...`
statements (a notebook cannot be executed here — no `dbutils`, no cluster — so nothing else
in the suite would catch a stale one, which is exactly what happened to all four notebooks
between Stage 3 and Stage 7 per that stage's own research). Each import is then resolved for
real via `importlib`: the module via `import_module`, and each `from ... import X` name
either as an attribute of that module or, failing that, as a submodule import — the same two
paths Python's own `from package import name` takes, which matters because
`from kafka_ingest.sources import kafka, oracle` names *submodules*, not attributes, until
something has imported them.

**Verified to fail, then restored:** temporarily changed
`from kafka_ingest.framework import runner` to `...runner_totally_retired_name` in
`notebooks/03_run_ingestion.py`, ran `pytest tests/test_offline_validation.py -q -k notebook`,
confirmed the new parametrized case failed with the expected message, then
`git checkout -- notebooks/03_run_ingestion.py` to restore. In the final state (after Part
B's notebook edits below added a few more `kafka_ingest` imports of their own), this check
contributes 27 parametrized cases across the four notebooks — see "Test counts" for the
exact per-file breakdown — all passing against the real, current source tree.

### 3. (Closed — not part of this stage's scope; listed in the human's instructions as already resolved)

### 4. Reconcile `.claude/build/` stage briefs against the repo

Delegated to an independent research agent with instructions not to re-flag the two
examples named in the ask, since both were already checked and found resolved before
delegating:

- **`RUNBOOK_CLIENT_IT.md` vs `ARCHITECTURE_OVERVIEW.md`**: already reconciled. The file was
  renamed in the Stage 7 addendum, and `.claude/build/STAGE_7_docs.md` already names
  `docs/ARCHITECTURE_OVERVIEW.md` correctly — it was written that way from the start; the
  drift Stage 7's own report flagged was between the brief and the *pre-addendum* repo, not
  a brief that needs editing now.
- **`IMPORT_TO_DATABRICKS.md`**: already reconciled (rewritten in the Stage 7 addendum). No
  `.claude/build/*.md` file names it at all, so there was nothing to edit on the brief side.

**What the agent actually found** (seven real mismatches, none of them the two named
examples) and what was fixed:

1. `CORE.md` §5.2's control-table sketch predated D-01 (prefixed columns,
   `source_overrides` removed) — updated with the real column shape and a pointer to
   `DECISIONS.md`.
2. `CORE.md` §5.2/§5.3's table paths (`{ops_catalog}.ingest_control`) predated D-06's
   ops-catalog schema split — updated to `{ops_catalog}.{control_schema}.ingest_control`
   etc., and a new §5.4 records the schema split explicitly (renumbering the old §5.4 Audit
   table to §5.5).
3. `CORE.md` §4.3's module layout omitted `checkpoint.py`, the eleventh framework module
   (D-14) — added.
4. `CORE.md` and `STAGE_6_gates.md`'s source-isolation grep gate named `framework/`, which
   does not exist at the repo root — fixed to `src/kafka_ingest/framework/`, matching what
   `azure-pipelines.yml` actually runs. **`CLAUDE.md` at the repo root has the same wrong
   path in its own reproduction of this gate** (see "Decisions for the human" below — left
   alone, since it is outside `.claude/build/`, which is what this item scoped the work to).
5. `STAGE_4_oracle.md`'s boundary-tie table presented `merge_keys` "absent" as a supported
   append-with-loss state; D-09 decision 2 made that a hard `ConfigError`
   (`_require_a_merge_key_decision`) — annotated with the supersession and the real
   behaviour, table left in place as the historical record of the original design question.
6. `STAGE_5_files.md`'s config example used `partition_by:` — the real key is
   `landing_partition_by` (D-14 item 5).
7. `STAGE_5_files.md`'s config example used a full `abfss://` `source_path` — the real
   convention for a `storage_ref`-governed source is a path relative to the container
   (D-14 item 3; a full path is legitimate only under D-13's separate `/Volumes/...` form,
   which postdates this brief).

**A structural finding, not named in the ask, found while doing this work:**
`.claude/build/` has never been tracked by git. `.gitignore`'s unanchored `build/` rule
(intended for the wheel output `python -m build` writes into `build/`/`dist/`) also matched
`.claude/build/` at any depth, so `git log --all -- .claude/build/` is empty despite eight
stages having read from and reasoned about this directory. That means every prior stage's
edits to these briefs (this one included, had the bug not been fixed) produced no diff and
were invisible to any PR review. Fixed by anchoring the rule to `/build/`; `.claude/build/`
is now tracked and committed for the first time. See "Decisions for the human" — this is a
repository-structure change beyond the literal ask, done because the alternative (editing
files nobody would ever see the diff for) defeats the point of a reconciliation pass.

### 5. `AUDIT_DDL_COLUMNS`'s `layer` comment

Done, with one deviation from the literal instruction. The comment now reads
`'run (this file), or a source's own - e.g. Kafka's LAYER_STREAM'` rather than naming
`sources/kafka/listener.py::LAYER_STREAM` by its file path. **The literal path could not be
used**: `framework/audit.py` is a `framework/` file, and CORE section 7's source-isolation
grep (`grep -rInE '\b(kafka|oracle|...)\b' src/kafka_ingest/framework/`) is
**case-sensitive** — it matches lowercase `kafka`, which a path segment
`sources/kafka/listener.py` necessarily contains, but not the capitalized proper noun
`Kafka` the file's own module docstring already uses two paragraphs above ("a Kafka offsets
JSON, a database cursor value, a file boundary"). Naming the literal path would have broken
a CI-enforced invariant to satisfy a documentation instruction; used the same
capitalized-proper-noun convention this file already relies on instead, which names the
source and the exact constant (`LAYER_STREAM`) without the lowercase path segment. Confirmed
the gate is still clean after the edit (see Test counts below).

---

## Part B — the verification backlog into runnable probes

### New backlog entry: VB-29

Added, per the brief: "Confirm every secret scope named in `conf/environments/*.yaml`
resolves, and every key name in `conf/jdbc.yaml` and `conf/storage.yaml` exists in it. Print
key NAMES only, never values." Placed **first** in `VERIFICATION_BACKLOG.md`'s "loud and
infra-blocking" tier (ahead of VB-22) — the reasoning is recorded inline in the file: unlike
every other entry in that tier, a missing scope or key blocks every source type at the first
step of every run, not one driver or one route. That placement is a judgement call, listed
below as a decision for the human.

### The four probes, in `notebooks/02_check_connectivity.py`

Ordered by damage against the now-29-entry backlog (not the order the instruction listed
them in): **VB-19** (rank 3 overall), **VB-27** (rank 5), **VB-29** (rank 18, first in its
tier), **VB-01** (rank 19). Each is its own cell with its own `# MAGIC %md` explanation.

- **VB-19** — builds the real extraction query via `sources/oracle/query.py::build_query()`
  with a wide fixed watermark window (the framework's actual `TO_TIMESTAMP` literal path,
  not a reimplementation), builds a `TO_DATE` control by substituting the function name in
  the same rendered string, runs both as `SELECT COUNT(*)` via
  `sources/oracle/reader.py::read_scalar_row()`, and compares. Skips with an explanation for
  a non-cursor or non-timestamp source rather than reporting a false pass.
- **VB-27** — the literal probe `VERIFICATION_BACKLOG.md` already specified: creates a
  three-column scratch Delta table (named by a new `vb27_scratch_table` widget, default a
  `<CHANGE_ME>` placeholder), appends a DataFrame with reordered columns, reads it back, and
  checks whether the values landed by name or by position. Drops the scratch table in a
  `finally` regardless of outcome — the only probe of the four that writes anything, and it
  never touches a real table.
- **VB-29** — iterates every profile in every register (`cfg.registers`, already resolved
  for the selected environment by `resolve_config()` regardless of which specific
  `source_key` the notebook's widgets point at) to collect every distinct `secret_scope`,
  calls `dbutils.secrets.list(scope)` for each (key **names** only — `.get()` is never
  called, so no secret value is ever fetched into the notebook), then checks every
  `username_key`/`password_key` (`jdbc.yaml`) and `account_key_secret_key` /
  `client_id_secret_key` / `client_secret_secret_key` (`storage.yaml`) field against the
  listed names for that profile's scope.
- **VB-01** — reuses `sources/oracle/reader.py::partition_bounds()` and `read()` directly
  (the real code path, not a hand-rolled JDBC call) to build the actual partitioned
  DataFrame, then calls `.rdd.getNumPartitions()`. This reads Spark's own partition plan and
  triggers no data read at all — `getNumPartitions()` does not execute the query. Skips with
  an explanation for a source with no `partition_column` or `num_partitions <= 1`.

All four probes, and the notebook's existing sections, are explicitly documented as
read-only in the notebook's own top-level `%md` cell, updated to name the four VB ids and
state the one exception (VB-27's self-cleaning scratch table) plainly.

### `docs/RUNBOOK_DEVELOPER.md` §9 — First-connection verification

New section, a five-column table (VB id / what to run / expected result / what to change in
the code if it fails / who can run it), in the same damage order as the notebook cells
above. States explicitly which developer role can run each probe without needing every
other system reachable (e.g. VB-29 needs only workspace access, not Oracle or Kafka
connectivity; VB-27 needs only `CREATE TABLE` on one scratch schema).

---

## 1. Done and verified

- Part A items 1, 2, 4, 5 — all four done as described above, `pytest -m "not spark" -q` ->
  976 passed, 7 skipped, 36 deselected (was 949; +27, all new: 23 notebook-import cases +
  4 register-reference-style cases the new VB-29 test infrastructure did not itself need
  test cases for, since it is a notebook cell, not a pytest test — see the exact
  before/after breakdown in "Test counts" below).
- `ruff check src tests` -> `All checks passed!`; `ruff format --check src tests` ->
  `85 files already formatted` (after `ruff format` was run once to fix the new test's
  formatting).
- CORE section 7 grep, re-run after every edit to `framework/audit.py`:
  `grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ | grep -v 'runner.py:.*_SOURCES'`
  returns nothing.
- `python -c "import ast; ast.parse(...)"` on `notebooks/02_check_connectivity.py` after
  every edit — confirms the notebook stays valid Python throughout, since it cannot be
  executed here.
- The new notebook-import test proven able to fail (see item 2 above) — a real mutation,
  watched fail, reverted via `git checkout`.
- Every `DESIGN.md#N` cross-reference across `docs/*.md` and `README.md` re-grepped after
  editing; none point at a stale section number.

## 2. Done but not verifiable here

- The four notebook probes themselves (VB-19, VB-27, VB-29, VB-01) cannot be executed in
  this environment — no Databricks workspace, no Oracle, no Key Vault-backed secret scope.
  Written against the real, current framework functions (`build_query`, `read_scalar_row`,
  `partition_bounds`, `read`, `SecretResolver`'s underlying `dbutils.secrets.list`) and
  checked by hand against their actual signatures, the same "written defensively, not
  claimed to run" posture every prior stage's Spark-touching code has taken. This is not a
  new gap — it is the same category `docs/build_log/STAGE_7_REPORT.md` already recorded for
  the four notebooks' own rewrite.
- VB-29 itself, obviously — it is a new *entry*, and clearing it needs a real workspace.

## 3. Not reproduced

- **The two file-naming mismatches the instruction named as examples for item 4 —
  `RUNBOOK_CLIENT_IT` vs `ARCHITECTURE_OVERVIEW.md`, and `IMPORT_TO_DATABRICKS.md` — were
  already resolved** (both fixed in the Stage 7 addendum, before this stage started). The
  actual reconciliation work needed was in different files and content than the instruction's
  own examples suggested — see Part A item 4 above for what was actually found instead.
- **Item 5's literal instruction — name `sources/kafka/listener.py::LAYER_STREAM` in the
  `framework/audit.py` comment — could not be followed exactly**, because the literal
  lowercase path trips the CORE section 7 grep gate. See Part A item 5 above for what was
  done instead and why it still satisfies the intent (naming the distinction and the exact
  constant).

## 4. Blocked

Nothing. Every item in the human's instructions has a corresponding edit, or an explicit
"not reproduced" entry above explaining the one place a literal reading conflicted with a
standing invariant.

## 5. Decisions for the human

1. **`.claude/build/` is now tracked by git for the first time**, via the `.gitignore` fix
   described in Part A item 4. This is a repository-structure change beyond the literal
   ask — done because the alternative (a reconciliation pass whose edits produce no diff
   and are invisible to review, forever) defeats the point of doing the pass at all. *What
   would change it:* a preference to keep `.claude/build/` deliberately untracked (e.g. if
   it is meant to be a local-only planning scratchpad, refreshed from elsewhere each
   session) — in which case revert the `.gitignore` change and this stage's edits to
   `CORE.md`/`STAGE_4_oracle.md`/`STAGE_5_files.md`/`STAGE_6_gates.md` remain correct on
   disk but should not be committed.
2. **`CLAUDE.md` at the repo root has the same wrong `framework/` (vs
   `src/kafka_ingest/framework/`) path in its own copy of the CORE section 7 grep gate**,
   found by the same reconciliation pass that fixed `CORE.md` and `STAGE_6_gates.md`. Left
   alone: the instruction scoped this item to `.claude/build/` stage briefs, and `CLAUDE.md`
   is a different, root-level file with its own editing history. *What would change it:* an
   explicit instruction to fix it too — a one-line change, identical in shape to the two
   already made.
3. **VB-29's placement at the very top of the "loud and infra-blocking" tier** (ahead of
   VB-22) is a judgement call, not a re-derivation from first principles the way Stage 6's
   original re-sort was. The reasoning (it blocks every source type, not one) is recorded
   inline in `VERIFICATION_BACKLOG.md`. *What would change it:* a view that "blocks
   everything" is less specifically actionable than "blocks Oracle specifically until a
   platform task lands" (VB-22's own framing), in which case VB-29 belongs later in the
   tier rather than first.
4. **`docs/DESIGN.md`'s onboarding-time re-measurement is a line-count comparison, not a
   re-timed reading estimate** — see Part A item 1. Stage 7's original "two to two and a
   half hours" figure came from actually reading the whole document; this stage did not
   repeat that exercise, since the document that produced it no longer exists in that shape.
   *What would change it:* someone actually re-reading the split docs end to end and timing
   it, which would either confirm or refine the line-count-based estimate this stage
   recorded instead.

---

## Test counts

Both from actual `pytest` output.

- **Before this stage:** `949 passed, 7 skipped, 36 deselected`.
- **After this stage:** `976 passed, 7 skipped, 36 deselected`.

Net: **+27 passed**, all in `tests/test_offline_validation.py`'s new notebook-import-check
section — one parametrized case per `kafka_ingest` import statement found across the four
`notebooks/*.py` files, counted directly from `pytest --collect-only`: 17 in
`02_check_connectivity.py` (this stage's own new probe imports included), 5 in
`00_validate_config.py`, 4 in `03_run_ingestion.py`, 1 in `01_run_unit_tests.py`. None of
these existed as test cases before this stage added the check itself. No test was removed or
modified beyond the one new test function and its two small helper functions.

## New VB entries added this stage

**VB-29** — does every secret scope named in `conf/environments/*.yaml` resolve, and does
every key name in `conf/jdbc.yaml` / `conf/storage.yaml` exist in it. Full entry in
`docs/VERIFICATION_BACKLOG.md`, placed first in the "loud and infra-blocking" tier.
