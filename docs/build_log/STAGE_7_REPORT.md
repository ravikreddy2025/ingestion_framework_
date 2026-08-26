# Stage 7 Report — Documentation and final report

Read against `CLAUDE.md`, `.claude/build/CORE.md`, `.claude/build/STAGE_7_docs.md`, and every
file in `docs/build_log/` (STAGE_0 through STAGE_6, plus the addendum to STAGE_2 and the
STAGE_5b pass, and `DECISIONS.md` D-01 through D-14). Stage 6 was green (947 passed, 7
skipped, 36 deselected) before this stage started.

No branch/PR workflow was available in this environment for this session; the work below
was done directly, and the git-workflow steps in `CLAUDE.md` (branch, PR) should be applied
by whoever lands this before merging, per that file's instructions.

**Files created:** none, beyond this report — `docs/VERIFICATION_BACKLOG.md` was already the
one documentation file this project produces beyond what existed at Stage 0, and it needed no
changes this stage (see section 3 below).

**Files edited:** `README.md`; `docs/NAVIGATION.md`; `docs/DESIGN.md`; `docs/CONFIGURATION.md`;
`docs/RUNBOOK_DEVELOPER.md`; `docs/RUNBOOK_SUPPORT.md`; `docs/RUNBOOK_CLIENT_IT.md`;
`notebooks/00_validate_config.py`, `01_run_unit_tests.py`, `02_check_connectivity.py`,
`03_run_ingestion.py`; `conf/defaults.yaml`, `conf/defaults/{kafka,oracle,file}.yaml`,
`conf/environments/{dev,preprod,prod}.yaml` (inline tier markers only — no key added,
removed or renamed).

---

## What this stage found before writing anything

Two research passes (my own reading of every framework/source/conf/sql/resources file, and
an independent Explore agent covering the same ground) agreed on the single most important
finding: **`notebooks/*.py` were entirely stale**, unrelated to any change this stage made.
All four imported modules and called functions from the pre-Stage-3 design
(`kafka_ingest.config`, `kafka_ingest.pipeline`, `kafka_ingest.security`,
`kafka_ingest.schema_resolver`, `kafka_ingest.curated_writer`, `resolve_topic_config`,
`conf/topics/*.yaml`, `ingestion_topic_control`, `topic_key`, `on_deser_error`, `batch_id`) —
none of which exist anywhere in the current `src/kafka_ingest/`. They would have failed on
the first line that imported anything. This was not new drift introduced by Stage 7; it had
been true since Stage 3 retired those modules, and no stage's file list before this one
included `notebooks/`. All four are rewritten (section below).

The second largest finding was that `docs/DESIGN.md` sections 1–9 were **the original,
pre-refactor Kafka-only design document**, never touched by Stages 1–6 despite the
framework/sources split those stages built. Section 11 (Files, added in Stage 5) said so
explicitly: *"Sections 6–9 predate the framework/sources split and still refer to modules...
that no longer exist under those names; not corrected here."* This stage rewrites sections
1–9 in place; sections 10–12 (Oracle, Files, Adding a source type — added in Stages 4, 5 and
6 respectively) were already accurate against the current code and are kept, renumbered to
10–12 after inserting one new section (`## 5. Kafka design decisions`, promoted from a
subsection) restored the original numbering for those three.

`README.md`, `docs/NAVIGATION.md`, `docs/RUNBOOK_DEVELOPER.md` and `docs/RUNBOOK_SUPPORT.md`
sections 1–7 were the same story: pure pre-refactor Kafka prose (`topic_key`,
`on_deser_error`, `ingestion_topic_control`, `stream_audit`, `pipeline.py`), never updated
despite `RUNBOOK_SUPPORT.md` §8 (Oracle) and §9 (Files) and `CONFIGURATION.md` §9/§10
(Oracle/Files, since renumbered §10/§11) having been added and kept current by Stages 4–6.
`docs/RUNBOOK_CLIENT_IT.md` was the one exception: already updated for the D-06 three-schema
ops-catalog layout and the D-02 grants-are-Terraform table, but still Kafka-only in every
other section (data flow, security model, operating model, data protection, glossary).

---

## Work done, by document

### `README.md` — full rewrite
Three sources, one spine; onboarding in three short sections (Kafka/Oracle/Files); a
one-paragraph replay summary per source type; a link to NAVIGATION as the entry point.

### `docs/NAVIGATION.md` — full rewrite
Role-based start-here table; a five-step ten-minute path to the architecture; **three
traces, one per source type**, each walking a record through every module in the order it
actually executes (verified against the real `run()` bodies in `sources/kafka/run.py`,
`sources/oracle/run.py`, `sources/file/run.py` — not reconstructed from memory); a per-file
"open it when..." table covering the current `framework/` (11 modules) and all three source
packages; an "I want to... → go to..." lookup; an explicit ignore-list.

### `docs/DESIGN.md` — sections 1–9 rewritten, 10–12 kept and renumbered
New: the architecture (spine + packages, `contracts.py` skeleton reproduced verbatim from
the actual file, not retyped from the Stage 1 report); the source-contract rationale (why one
method, adapted from the Stage 1 report's own argument since it is still exactly right); the
generic five-layer configuration model; Kafka's own re-runs/duplicates section, failure-
scenario table and design decisions (payload nesting, reader-schema mandate, partitioning,
MERGE vs append, CloudEvents, why not Lakeflow Declarative Pipelines) — all carried over from
the original document since the underlying design did not change, only the module paths
citing it; a common-changes table; a testing section naming the current test files;
**a new "unverified-claims list" section that cross-references `VERIFICATION_BACKLOG.md`
by VB id instead of restating it** (the explicit ask in the stage brief); and a
**"deliberate non-abstractions" section** consolidating CORE section 7's forbidden list as
what was *not* built, framework-wide, in one place (previously scattered as Kafka's and
Files' own "deliberately not built" subsections only). Oracle (§10) and Files (§11) sections
were already accurate and are unchanged beyond renumbering and fixing the handful of internal
`§n` cross-references that moved.

### `docs/CONFIGURATION.md` — sections 1–8 rewritten, 9 rebuilt, 10–11 kept and renumbered
The five-layer model, cluster/registry config and the Kafka per-source section are rewritten
generically where the model is shared and Kafka-specifically where it is not. **Section 5
(the control table) is rewritten as one shared table with a framework-owned/source-owned
column split**, matching D-01, instead of describing the retired single-tier control table.
The stage file's two "section 9" headings (the pre-existing "Validating configuration before
you deploy" and Oracle's own section, both numbered 9) are resolved: validating-before-deploy
is now §9, Oracle is §10, Files is §11 — no content lost, only renumbered, and the duplicate
heading fixed.

### `docs/RUNBOOK_DEVELOPER.md` — full rewrite
Local setup framed explicitly as "no cluster, no Kafka, no Oracle, no ADLS" per CLAUDE.md's
invariant; a codebase tour by the current module layout; the invariants table (spec-driven
validation, the grep gate, the one-method contract, structural-vs-operational, the
state/audit write asymmetry, the mandatory merge predicate, the watermark ordering, the
runtime-dependency list) with each row naming what enforces it; onboarding for all three
source types; a debugging table covering all three; a PR checklist including the grep gate.

### `docs/RUNBOOK_SUPPORT.md` — sections 1–7 rewritten, 8–9 kept verbatim
Sections 1–7 (daily health check, triage, onboarding, decommissioning, Kafka playbooks,
escalation, what-support-can-change) are rewritten against the current table/column names
(`ingest_control`/`ingest_state`/`ingest_audit`, `source_key`, `kafka_failure_mode`, etc.) and
extended to cover all three source types where the workflow is genuinely shared (the daily
health check, decommissioning, escalation). **Section 8 ("Oracle incident playbooks") and
section 9 ("File incident playbooks") are untouched, verbatim** — `tests/test_shipped_jobs.py`
`test_every_oracle_query_the_runbook_cites_exists` reads this file, splits on the exact string
`"## 8. Oracle incident playbooks"`, and asserts `Q17`/`Q18`/`Q21`/`Q23`/`Q24` appear after
it; that heading and those citations are reproduced exactly, and the test was run to confirm
(`pytest tests/test_shipped_jobs.py -q` → 17 passed).

### `docs/RUNBOOK_CLIENT_IT.md` — extended for all three sources
Data flow, security model, environments, operating model, data integrity guarantees, data
protection, assurance, deliberate limitations and the glossary all now cover Kafka, Oracle
and Files rather than Kafka alone. Section 6 (platform prerequisites and the Unity Catalog
privileges table) was already current from Stage 2b's addendum and is kept, extended only
with the Oracle-driver and per-source-schema rows the earlier version did not need. See
"Not reproduced" below for the one naming question this raised.

### `notebooks/*.py` — full rewrite, all four
Rewritten against the actual current API (`framework.config.resolve_config`,
`framework.runner.run`, each source's `spec.py`/`config.py`/`security.py`), generically
across source types where a notebook's job is generic (`00_validate_config`,
`02_check_connectivity`, `03_run_ingestion`) and with one import path fixed where it was
Kafka-specific but only had one line wrong (`01_run_unit_tests`'s
`assert_from_avro_semantics` import). Every function call in every notebook was checked
against the real module it imports from before being written — none is invented. These
cannot be executed here (no Databricks, no `dbutils`), so they are **written defensively and
not claimed to run**; each notebook is short enough, and thin enough (a widget read, a
resolve/build call, a print or a `display()`), that the risk surface is the import paths and
the argument order, both checked by hand against source.

### `conf/**/*.yaml` — inline tier markers added
`conf/clusters.yaml`, `registries.yaml`, `jdbc.yaml` and `storage.yaml` already carried the
`[MUST CHANGE]` / `[NICE TO CHANGE]` / `[NO CHANGE REQUIRED]` convention (added in Stages 3–5
when `jdbc.yaml`/`storage.yaml` were created and backfilled onto the two Kafka registers);
the three `conf/sources/_TEMPLATE*.yaml` onboarding templates and the five worked-example
source files already used the equivalent `MUST CHANGE` / `OPTIONAL` / `RARE` section-header
convention. The five files that had neither — `conf/defaults.yaml`,
`conf/defaults/{kafka,oracle,file}.yaml`, and `conf/environments/{dev,preprod,prod}.yaml` —
now carry the same bracketed markers, matching `docs/CONFIGURATION.md`'s tiers exactly, with
a legend added to each file's header comment. No key was added, removed, renamed or
re-valued in any of these eight edits — comments only, confirmed by `ruff format --check`
being unaffected (YAML has no formatter in this project's gate) and by re-running the full
test suite after the edits (below).

### `docs/VERIFICATION_BACKLOG.md` — no changes
Read in full and checked against every module it references (`sources/oracle/*`,
`sources/file/*`, `framework/writers.py`, `framework/state.py`) during this stage's own
research pass. All 28 entries (VB-01 through VB-28) are still accurate against the current
code, still open, and already ordered by damage per Stage 6's own re-sort. Nothing this
stage did was code, so nothing this stage did could resolve or invalidate an entry. Left
untouched rather than touched-for-the-sake-of-it.

---

## 1. Done and verified — covering the whole project

**Framework spine (Stages 1, 2, 2b, 6).** `framework/contracts.py` (the three-dataclass
contract), `config.py` (five-layer spec-driven merge, no PySpark import), `control.py`
(missing-row-OK / duplicate-row-fatal / prefixed-column D-01 rules), `security.py`
(`SecretResolver`, redaction, `apply_session_options`), `state.py` (writes raise),
`audit.py` (writes never raise; one shared table), `tables.py` (target rendering/validation,
the only `CREATE TABLE`), `writers.py` (`append`/`merge`, mandatory partition predicate),
`checkpoint.py` (the shared Kafka/File reset guard), `runner.py` (the lifecycle, `_SOURCES`),
`logs.py` (redaction by key-name hint). Proof: `pytest -m "not spark" -q` → 947 passed
(unchanged by this stage — see Test counts below), and the CORE section 7 grep returning
nothing, re-run at the end of this stage.

**Kafka (Stage 3).** The nine legacy top-level modules folded into `sources/kafka/` behind
`SOURCE_SPEC` + `run(ctx)`; the checkpoint-reset single-use fix; all four non-negotiable
reader options; the curated-MERGE `event_date` bound vs landing's declared unbounded
`"true"` predicate; malformed-payload triage; job hardening.

**Oracle (Stage 4, sub-steps a–d).** The query builder and its closed interval; the
filter-criteria/`sql_query` SQL-injection-shaped allowlist; the mandatory `merge_keys`
decision; the JDBC read (fetchsize always set, `dbtable` subquery form, partition bounds
probed not configured); the watermark lifecycle order (capture → read → write → advance);
the merge key including the cursor column; D-09's full/delta operational switch and
explicit replay-bounds mechanism; the operational surface (job template, onboarding
template, worked example, support playbooks).

**Files (Stage 5, 5b).** Auto Loader with `availableNow` always; the checkpoint-reset guard
reused (Stage 5) then hoisted into `framework/checkpoint.py` and shared with Kafka (Stage
5b) after the two copies were found to have already drifted once; the storage register and
two auth modes; D-13's Unity Catalog Volume path support alongside the `storage_ref` form.

**Extensibility gate and offline validation (Stage 6).** The CORE section 7 grep wired into
`azure-pipelines.yml`; `tests/test_offline_validation.py`'s five checks; the cross-product
test extended with a placeholder sweep and a cross-*type* checkpoint-collision check; D-10
through D-14 all documented and, where they implied code, implemented (D-13's two-form
source path; D-11's file-source maintenance task).

**Documentation (this stage).** Every document in the Stage 7 file list rewritten or
extended as described above. Proof for the one place a document's exact text is
load-bearing: `pytest tests/test_shipped_jobs.py -q` → 17 passed, including
`test_every_oracle_query_the_runbook_cites_exists` against the rewritten
`RUNBOOK_SUPPORT.md`. Proof that no code, config value or test was disturbed by this stage:
`pytest -m "not spark" -q` → 947 passed, 7 skipped, 36 deselected — identical to the count
Stage 6 ended on.

---

## 2. Done but not verifiable here — covering the whole project

Every VB-01 through VB-28 entry in `docs/VERIFICATION_BACKLOG.md`, unchanged by this stage.
The three most dangerous, restated from that file's own damage ordering (not re-derived):

1. **VB-22** — the Oracle JDBC driver is not installed anywhere in this repository, is not
   bundled with Databricks Runtime, and its version gates the two entries below.
2. **VB-02 / VB-03** — the Spark type mapping for Oracle `NUMBER` (no precision/scale) and
   `DATE`, on the target driver/DBR. A wrong guess silently corrupts or truncates every
   value in that column, forever, with no error anywhere.
3. **VB-15** — whether `ingest_state`'s MERGE actually upserts. Every batch-style source's
   idempotency (`run_sequence`) depends on it; a silent no-op looks exactly like a healthy
   job with nothing new to write.

This stage's own notebook rewrites are in the same category as the rest of the codebase:
written against the real API, never executed, because there is no Databricks workspace here
to run them in. That is not a new gap this stage introduces — it is the same "written
defensively, recorded rather than claimed" posture every prior stage's Spark-touching code
has taken — but it is worth stating plainly here since notebooks are the one artifact in
this repository whose entire job is to be run interactively on a cluster, and none of the
four could be smoke-tested locally in any way stronger than "does every name it imports
exist in the current source tree" (checked by hand, module by module, against the real
files — not by execution).

---

## 3. Not reproduced — findings against the stage brief and existing docs

- **The stage brief's file list names `docs/ARCHITECTURE_OVERVIEW.md`; the repository has no
  such file.** The document matching that description — client-IT audience, non-
  implementation, data flow, security model, prerequisites, governance/grants, operating
  model, data protection, assurance, limitations, glossary — is `docs/RUNBOOK_CLIENT_IT.md`,
  and it already carried most of that structure from earlier stages. Per CORE rule 7
  ("locate by file and symbol, never by line number... if a symbol named in a stage file
  does not exist, report that and skip rather than fixing the nearest similar thing") and
  the "do not build new documentation files" instruction in this same stage file, I edited
  `RUNBOOK_CLIENT_IT.md` in place rather than creating a new `ARCHITECTURE_OVERVIEW.md` that
  would either duplicate it or need `RUNBOOK_CLIENT_IT.md` deleted — the latter being a
  larger, unrequested restructuring. Flagged as a decision for the human below.
- **`docs/IMPORT_TO_DATABRICKS.md` is not in this stage's file list, and is now the most
  stale document in the repository.** It still describes `TopicConfig`, `pipeline.run()`,
  `resolve_topic_config`, and a testing table naming eight retired test files
  (`test_config`, `test_security`, `test_pipeline`, `test_writers`, `test_curated_writer`,
  `test_audit_and_tables`, `test_kafka_source`, `test_schema_resolver`). This predates Stage
  7 — no prior stage's file list included it either — and per CORE rule 8 ("if you are
  unsure whether something is in scope, it is not") I left it untouched rather than
  expanding this stage's scope to cover a file nobody has asked for since Stage 0's
  inventory flagged it. Recorded as a decision for the human below, since it is the one
  remaining doc a new joiner following `README.md`'s original link list could still be
  misled by.
- **The stage brief's "Deliberate non-abstractions" ask for `docs/DESIGN.md` reads as
  wanting one consolidated list; the document previously had this scattered as two
  per-source "deliberately not built" subsections (Kafka's, Files') plus nothing
  framework-wide.** Built a new, explicit `## 9. Deliberate non-abstractions` section
  restating CORE section 7's forbidden list as what was not built, and left the two
  per-source lists in place under their own sections (they name source-specific
  non-choices — e.g. no third `reader_schema_mode` — that a framework-wide list would not
  capture). Both exist now; neither was deleted, since the per-source ones carry
  information the general one does not.
- **Oracle's own maintenance-job gap, flagged in Stage 5's report, Stage 6's D-11 decision,
  and now here for a third time, is still open.** `resources/job_maintenance.yml` covers
  the three Kafka topics and the file source; `oracle_claim_header`'s landing table has no
  `OPTIMIZE`/`VACUUM` task. D-11 explicitly scoped its own pass to "the file source only, as
  asked," leaving this open on purpose rather than by oversight — but three consecutive
  stage reports naming the same gap without closing it is itself worth surfacing as a
  decision rather than a fourth silent carry-forward. See below.

---

## 4. Blocked

Nothing. Every item in `STAGE_7_docs.md`'s "Work" and "Exit gate" sections has a
corresponding edit, or an explicit "not reproduced" entry above explaining why it was not
done as literally specified.

---

## 5. Decisions for the human

1. **`docs/RUNBOOK_CLIENT_IT.md` was edited in place rather than creating
   `docs/ARCHITECTURE_OVERVIEW.md`.** See "Not reproduced" above for the reasoning.
   *What would change it:* a preference for the file to actually be named
   `ARCHITECTURE_OVERVIEW.md` going forward — a straightforward `git mv` plus updating the
   handful of cross-references to it (`README.md`, `NAVIGATION.md`, `sql/02`'s comment,
   `tests/test_shipped_sql.py`'s comment), none of which is a behavioural change.
2. **`docs/IMPORT_TO_DATABRICKS.md` remains stale**, describing an entrypoint and test
   layout that no longer exists. It was out of this stage's file list, as it has been out of
   every stage's file list since Stage 0's inventory recorded it as "REWRITE... out of
   scope before [Stage 7]" — and Stage 7's own file list, read literally, still does not
   name it. *What would change it:* an explicit instruction to include it — at which point
   it is a mechanical rewrite following the same pattern as `RUNBOOK_DEVELOPER.md` §1
   (already rewritten this stage) and the notebooks (already rewritten this stage), and
   should take under half an hour.
3. **Oracle's maintenance-job gap (D-11, `resources/job_maintenance.yml`) is still open**,
   flagged for the third time across Stages 5, 6 and 7. It is a five-line addition —
   one more task in that file, no SQL change, mirroring the `file_claims_inbound` task
   already there — and D-11 already recorded the exact mechanism. *What would change it:*
   simply doing it. Recorded here rather than done silently because D-11 explicitly scoped
   its own pass away from it "per the instruction that produced this decision," and a
   documentation stage overriding that scoping on its own would be the kind of scope
   creep CORE rule 8 warns against.
4. **The `sql/03_support_queries.sql` `layer = 'stream'` filter in Q13 is correct, not a
   bug** — checked directly against `sources/kafka/listener.py::LAYER_STREAM = "stream"`
   during this stage's research pass, after initially suspecting a stale reference against
   `framework/audit.py`'s `LAYER_RUN = "run"`. Recorded here only because the two layer
   vocabularies (the framework's own `"run"` layer, and each source's own layer names,
   `"stream"` among Kafka's) are easy to conflate on a first read, and a future session
   should not re-investigate this as if it were newly discovered.

---

## 6. Stage gates

| Stage | Green? | What proves it |
|---|---|---|
| 0 — Orientation | Yes | `ruff check` clean; `pytest -m "not spark" -q` → 189 passed, 34 deselected; `docs/VERIFICATION_BACKLOG.md` seeded with VB-01..13 |
| 1 — Framework spine, config | Yes | Same three commands; 263 passed, 34 deselected; CORE §7 grep clean; 10 mutations proven to fail their tests |
| 2 — Shared tables (control/state/audit) | Yes | 364 passed, 34 deselected; 11 mutations proven |
| 2b — D-01..D-06 applied | Yes | 415 passed, 34 deselected (after its own addendum); 9 mutations proven; `GRANT` confirmed absent from `sql/*.sql` |
| 3 — Kafka source | Yes | 487 passed, 36 deselected; formatter baseline reached zero; 19 mutations proven, including one that ran on real local Spark |
| 4 — Oracle source (4a–4d) | Yes | 770 passed, 6 skipped, 36 deselected; CORE §7 grep clean at every sub-step; 46 mutations proven across the four sub-steps |
| 5 — File source | Yes | 866 passed, 7 skipped, 36 deselected; a real bug (duplicate `_rescued_data` column) found and fixed by a test written before trusting the module |
| 5b — Hoist the checkpoint guard | Yes | 865 passed, 7 skipped, 36 deselected (net −1 test, more scenarios proven generically); 2 mutations proven |
| 6 — Extensibility gate, offline validation, CI | Yes | 947 passed, 7 skipped, 36 deselected; CORE §7 grep wired into CI and drilled with a planted violation; 5 targeted mutations proven |
| 7 — Documentation (this stage) | Yes | `pytest -m "not spark" -q` → 947 passed, 7 skipped, 36 deselected (unchanged — a docs stage touches no test-covered code); `ruff check`/`ruff format --check` clean; CORE §7 grep clean; `test_shipped_jobs.py`'s runbook-citation test passes against the rewritten `RUNBOOK_SUPPORT.md` |

---

## 7. The complete verification backlog

Full text lives in `docs/VERIFICATION_BACKLOG.md`; reproduced here as a table in that file's
own damage order (most dangerous first), status column from the file (all `OPEN`).

| # | VB | One-line question | Tier |
|---|---|---|---|
| 1 | VB-02 | What Spark type does Oracle `NUMBER` (no precision/scale) map to? | Silent data corruption |
| 2 | VB-03 | Does Oracle `DATE` map to date or timestamp, under which driver property? | Silent data corruption |
| 3 | VB-19 | Does the rendered `TO_TIMESTAMP` watermark literal compare correctly against the cursor column? | Silent data corruption |
| 4 | VB-04 | Which Oracle types have no clean Spark mapping (LOB, RAW, INTERVAL, TZ)? | Silent data corruption |
| 5 | VB-27 | Does a Delta append reconcile columns by name when order differs (the file source)? | Silent data corruption |
| 6 | VB-23 | Does `customSchema` apply per column, or as the complete schema? | Silent data corruption |
| 7 | VB-15 | Does the `ingest_state` MERGE actually upsert; is one-run-per-source true; does partitioning isolate concurrent sources? | Silent data corruption |
| 8 | VB-25 | How often does Oracle commit a row whose cursor value is already below the watermark? | Silent data corruption |
| 9 | VB-20 | Is `SYSTIMESTAMP` the right anchor for the dynamic date window, and whose clock is it? | Silent data corruption |
| 10 | VB-18 | Does the magic-byte comparison behave the same way on the target DBR? | Silent data corruption |
| 11 | VB-06 | Is `_metadata` available, and how does `rescuedDataColumn` behave per format? | Silent data corruption |
| 12 | VB-09 | Which Delta MERGE schema-evolution mechanism exists — session flag or builder method? | Silent data corruption |
| 13 | VB-24 | Is `sessionInitStatement` honoured alongside a `dbtable` subquery? | Silent, narrower |
| 14 | VB-26 | Does session-scoped `spark.conf.set()` reliably authenticate `abfss://` reads? | Silent, narrower |
| 15 | VB-28 | Unity Catalog Volumes or `abfss://`, per environment — and are Volumes reachable? | Silent, narrower |
| 16 | VB-05 | Is `sources[0].latestOffset` populated in `StreamingQueryProgress` under `availableNow`? | Silent, narrower |
| 17 | VB-10 | Does the `from_avro` writer/reader self-check pass on the target runtime? | Silent, narrower |
| 18 | VB-22 | Is the Oracle JDBC driver installed, and which version? | Loud, infra-blocking |
| 19 | VB-01 | Does JDBC `query` work with `partitionColumn`, or is `dbtable` required? | Loud, infra-blocking |
| 20 | VB-11 | Can executors read UC Volumes on the target compute access mode? | Loud, infra-blocking |
| 21 | VB-08 | Is a UC Volume a supported Structured Streaming checkpoint location on serverless? | Loud, infra-blocking |
| 22 | VB-12 | Serverless egress to brokers, registry, Oracle and ADLS — is network config in place? | Loud, infra-blocking |
| 23 | VB-16 | Can the ingestion service principal create the audit and state tables? | Loud, infra-blocking |
| 24 | VB-17 | Do `sql/01` and `sql/02` execute as written? | Loud, infra-blocking |
| 25 | VB-13 | Does `databricks bundle validate` pass, and does the wheel build? | Loud, infra-blocking |
| 26 | VB-14 | Is the oldest supported runtime DBR 16.4 LTS / Python 3.12 / Spark 3.5.2? | Loud, infra-blocking |
| 27 | VB-21 | Is the partition-bounds probe cheap, and does it return usable bounds? | Cost/performance only |
| 28 | VB-07 | Auto Loader directory-listing vs notification mode — which is viable in this tenancy? | Cost/performance only |

**The three most dangerous** (restated from section 2 above, in the file's own words): VB-02
and VB-03 because a wrong Oracle type mapping corrupts every row of a column forever with no
error anywhere; VB-15 because a silently non-upserting state MERGE breaks re-run idempotency
for every batch-style source at once, and looks exactly like a healthy job.

---

## 8. Test counts

Both from actual `pytest` output, not estimated.

- **Before Stage 0** (the state Stage 0 measured on arrival): `189 passed, 34 deselected`.
- **After Stage 7** (this stage, just run): `947 passed, 7 skipped, 36 deselected`.

Net across the whole project: +758 passed, +7 newly-skipped (all `spark`-marked, needing a
JDK + `spark-avro` this environment does not have), +2 deselected categories added as the
Kafka/Oracle sub-suites grew. This stage added or removed zero tests — it is a documentation
and configuration-comment stage, and the count is identical to Stage 6's own ending count,
confirmed by running the gate, not assumed.

---

## 9. Team-onboarding note

**The five files to read, in order:**

1. `README.md` — what this is, three sources, one spine.
2. `docs/NAVIGATION.md` — the map, the ten-minute path, and the three per-source-type traces.
3. `src/kafka_ingest/framework/contracts.py` and `framework/runner.py` — the whole contract
   and the whole lifecycle, both short enough to read end to end.
4. `docs/DESIGN.md` — the why: the architecture, the source-contract rationale, then
   whichever of the three per-source sections (Kafka §4-5, Oracle §10, Files §11) matches
   the first thing you'll actually touch.
5. `docs/RUNBOOK_DEVELOPER.md` — how to actually get the tests running and make a change.

**Roughly how long, end to end:** reading all five, including the parts of `DESIGN.md`
relevant to one source type, is about **two to two and a half hours**. Getting comfortable
enough to make a first real change — which also means opening one source package
(`sources/file/` is the smallest and clearest starting point) and confirming the pattern
`NAVIGATION.md` described holds in the real code — pushes the honest total to **around three
hours**.

**That is over two hours, so: the module carrying too much is `docs/DESIGN.md` itself.**
It is the single document a new joiner is told to read for "why," and it now carries the
general architecture *and* three full per-source designs (Kafka's re-run mechanics, design
decisions and failure table; Oracle's watermark lifecycle and failure table; Files' Auto
Loader design and failure table) in one file, at over 700 lines — denser and longer than any
single code module in the repository (`sources/oracle/config.py`, the longest source file,
is 765 lines, but almost half of it is straightforward enumeration and validation a reader
skims rather than studies). A newcomer who only needs to work on the file source still has
to either read past two other sources' worth of design or hunt for the section boundary. It
was not split into per-source files in this stage because the stage brief's "do not build
new documentation files beyond the backlog" rule forbids it — recorded here, and again in
"what I would change" below, as the honest answer to why the number is what it is rather than
silently rounding it down to fit.

---

## 10. What I would change

Having now read the whole codebase, in the order a new joiner would:

1. **Split `docs/DESIGN.md` by source, once the "do not build new files" constraint lifts.**
   Keep the architecture, contract rationale and configuration model in `DESIGN.md`; move
   each source's own design, failure table and decisions to a sibling file
   (`DESIGN_KAFKA.md`, `DESIGN_ORACLE.md`, `DESIGN_FILES.md`). This is the direct fix for
   section 9's onboarding-time finding, and it was the first thing I wanted to do on
   finishing this stage's research pass rather than partway through writing it.
2. **The notebooks should have had even a mechanical smoke check** (an AST parse confirming
   every `import kafka_ingest....` name actually resolves against the current source tree)
   wired into `tests/test_offline_validation.py` from whichever stage first split the
   package. It would have caught the four-stage drift this session found the moment it
   happened, for the cost of a few lines — the same kind of cheap, mechanical check that
   entry already applies to job templates and register references.
3. **Oracle's maintenance-job gap should just be closed**, not flagged a fourth time. D-11's
   scoping to "file source only, as asked" was the right call for that pass specifically,
   but three stage reports later, closing a five-line, already-specified gap is smaller than
   the cost of a fourth session re-reading the same flag.
4. **`docs/RUNBOOK_CLIENT_IT.md` vs the brief's `ARCHITECTURE_OVERVIEW.md`** is a naming
   drift between the `.claude/build/` stage briefs and the repository as it was actually
   built, and it is not the only place the two disagree in small ways (see `docs/
   IMPORT_TO_DATABRICKS.md`, never named in any stage's file list despite Stage 0 flagging
   it for rewrite). A short pass reconciling the stage briefs' file lists against what the
   repository actually contains — the same kind of pass `DECISIONS.md`'s D-01..D-14 work
   list already does for code decisions — would remove this class of "which file did they
   mean" question for the next session.
5. **The `layer = 'stream'` vs `layer = 'run'` distinction in the audit table's `layer`
   column is genuinely subtle** (the framework's own lifecycle layer vs. each source's own
   layer vocabulary, sharing one column) and cost real time to confirm was correct rather
   than a stale reference during this stage's research. A one-line note on the
   `AUDIT_DDL_COLUMNS` `layer` column's comment in `framework/audit.py`, naming
   `sources/kafka/listener.py::LAYER_STREAM` as the one source-owned layer name that is not
   also one of that source's `SOURCE_SPEC.layers`, would have saved that time and will save
   it for the next reader too.
