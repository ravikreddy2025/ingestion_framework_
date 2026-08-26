# Decisions -- settled by the human

Decisions returned by a stage report and answered. **These are settled. Do not re-litigate
them.** Each entry states the decision, why, and what work it implies.

Any stage that finds a decision here genuinely unworkable must say so in its report rather
than quietly doing something else.

---

## D-01 -- Source-specific control-table columns carry a source prefix

**Decided:** there is no truly universal control table across source types, and pretending
otherwise adds confusion about how to use a column rather than removing it. So:

- A column that means **exactly the same thing for every source** has **no prefix**. It is
  framework-owned.
- A column that belongs to **one source type** is named **`<source_type>_<setting>`**.
  Every other source ignores it.
- The same rule applies to every source type added in future.

**Resulting table shape** (`ingest_control`):

```
-- framework-owned, apply to every source
source_key                     STRING NOT NULL   -- primary key
source_type                    STRING NOT NULL
enabled                        BOOLEAN
replay_rerun_id                STRING
replay_controls                STRING (JSON)     -- per-source replay parameters
notes                          STRING
updated_by                     STRING
updated_at                     TIMESTAMP

-- kafka only
kafka_failure_mode             STRING            -- FAILFAST | QUARANTINE
kafka_max_offsets_per_trigger  BIGINT
kafka_checkpoint_reset_id      STRING

-- oracle only (added in Stage 4)
oracle_fetch_size              INT
oracle_num_partitions          INT

-- file only (added in Stage 5)
file_failure_mode              STRING
file_max_files_per_trigger     BIGINT
file_checkpoint_reset_id       STRING
```

**Consequences, all of which are the point:**

1. **`batch_limit` and `failure_mode` are gone as shared columns.** They became
   `kafka_max_offsets_per_trigger`, `oracle_fetch_size`, `file_max_files_per_trigger` and
   so on. A support engineer now reads a column name that says which knob it turns, instead
   of one word meaning three different mechanisms.
2. **`source_overrides` JSON is removed.** With prefixed columns there is no second place a
   source-specific setting can live, and two mechanisms for one job is the confusion this
   decision exists to prevent. Adding a source type already requires a deploy (new package,
   job template, conf files), so it may also require an `ALTER TABLE`. That is honest, not a
   regression.
3. **`replay_controls` stays JSON and stays unprefixed.** It carries structured replay
   parameters whose *shape* differs per source (Kafka offsets or timestamps, an Oracle
   cursor range, a file path or date range). A JSON document validated against the source's
   own spec is different from a scalar column with three meanings. Revisit only if a reader
   is ever confused by it.
4. **Setting a prefixed column for the wrong source type is an error**, not a silent
   ignore. The message names the column and the mismatch -- e.g. *"`kafka_checkpoint_reset_id`
   is set for `oracle_claim_header`, whose source_type is `oracle`"*. A typo in the prefix is
   the mistake this catches.

**CRITICAL for the grep gate:** `framework/control.py` must **not** contain the strings
`kafka_`, `oracle_` or `file_`. Each `SOURCE_SPEC` declares its own control columns -- add a
`control_columns: Mapping[str, str]` field (column name -> setting name) -- and
`framework/control.py` reads whatever the spec declares. The framework knows the *rule*, never
the *names*. This is the same reason `target_tokens` exists.

---

## D-02 -- Grants are Terraform-owned; the framework and the SQL scripts do not grant

**Decided:** grants are provisioned by Terraform, outside this repository. User group and
role details do not exist yet.

**Work this implies:**

1. **Remove `GRANT` statements from `sql/01_operational_config.sql` and
   `sql/02_layer_tables.sql`.** Replace with a header comment stating that grants are
   Terraform-owned and pointing at the privilege list below.
2. **Record the privileges the framework needs**, so whoever writes the Terraform has a
   specification rather than a guess. Put it in `docs/RUNBOOK_CLIENT_IT.md`, which is already
   the platform-prerequisites document. At minimum, for the ingestion service principal:
   `USE CATALOG` on the ops and data catalogs; `USE SCHEMA` + `CREATE TABLE` + `MODIFY` +
   `SELECT` on each schema the framework writes; `SELECT` on the control schema.
3. **`CREATE SCHEMA` is also out of scope for the framework.** Schemas are pre-created.
   Stage 4 must therefore add "create the `oracle_<schema>` UC schema before the first run"
   to the Oracle onboarding checklist, or the first onboarding fails on an undocumented step.
4. **VB-16 stays open but is no longer a blocker** -- it becomes "confirm Terraform granted
   what the list above says", to be checked when a workspace exists.

---

## D-03 -- `batch_id` is renamed `txn_version`

**Decided:** accepted. The column carries the Delta `txnVersion` for every source type --
a streaming microbatch id, a batch source's `run_sequence`, or `-1`. `batch_id` reads as
Kafka vocabulary in a source-neutral table; `txn_version` says what it is and matches the
Delta concept it feeds.

Rename in `framework/audit.py` (schema + writer), the audit DDL, and every test and support
query that names it. Nothing is deployed, so there is no migration.

---

## D-04 -- `ingest_state` is partitioned by `source_key`

**Decided:** partition now rather than wait for contention.

A run sequence is allocated on **every** run of **every** source type -- conditioning that on
source type is the branch the grep gate forbids, and conditioning it on configuration would
let a misconfigured environment silently lose idempotency protection. The cost is one MERGE
per run into one shared table, and with many sources on the same schedule those MERGEs are
concurrent.

Delta detects conflicts at file granularity, so **partitioning by `source_key` makes
concurrent MERGEs from different sources touch disjoint files and stop conflicting.**

- `PARTITIONED BY (source_key)` on `ingest_state`. Not `CLUSTER BY` -- Delta allows one or
  the other, never both, and partitioning is what buys the conflict isolation here.
- Enable deletion vectors on the table.
- Small-file growth is not a concern: a handful of rows per source, and the maintenance job
  already covers the framework's tables.
- **VB-15 is extended** to cover concurrency: does the `ingest_state` MERGE upsert correctly
  *and* survive N concurrent runs from different sources?

---

## D-05 -- Framework-owned configuration keys

**Decided:** accepted as built. `audit_table`, `state_table`, `control_table`,
`table_properties` and `rerun_id` are framework-owned keys in `framework/config.py`, joining
`domain` and `enabled`.

`conf/defaults.yaml` sets these for every source of every type. Without framework ownership,
every `SOURCE_SPEC` would have to declare keys that only the framework reads, and a source
author could misspell one into silence.

`rerun_id` is operational-only: a replay id checked into Git re-applies on every future
deploy.

**Work this implies:** the framework-owned key list must be documented in exactly one place.
`docs/CONFIGURATION.md` is that place (Stage 7). Until then this entry is the record.

---

## D-06 -- One ops catalog, three schemas

**Decided:** all framework metadata lives in a single ops catalog, split into schemas by
purpose:

| Object | Location |
|---|---|
| Audit rows | `{ops_catalog}.{audit_schema}.ingest_audit` |
| Control table | `{ops_catalog}.{control_schema}.ingest_control` |
| State table | `{ops_catalog}.{control_schema}.ingest_state` |
| Logs | `{ops_catalog}.{logs_schema}.*` -- **reserved, nothing written there yet** |

**This changes what Stage 2 built.** The audit table currently resolves to
`{catalog}.audit.ingest_audit` in the *data* catalog. Move it to the ops catalog. Nothing is
deployed, so this is a configuration and DDL change only.

**State lives in the control schema**, not its own -- it is operational state with the same
blast radius and the same audience as the control table. Revisit only if the two acquire
different access requirements.

**The `logs` schema is reserved, not used.** Create it in the provisioning SQL so the shape
is visible and Terraform can grant on it, but write nothing to it in this pass. Structured
logging currently goes to the driver log.

**Everything above must be parameterised, not literal**, because it is substituted per
environment at deploy time:

- `ops_catalog` is already an environment `vars:` entry. Add `audit_schema`,
  `control_schema` and `logs_schema` alongside it, defaulted in `conf/defaults.yaml` and
  overridable per environment.
- The three table settings in `conf/defaults.yaml` become
  `{ops_catalog}.{audit_schema}.ingest_audit` and so on -- no hardcoded schema names anywhere
  in Python or SQL.
- The existing test that asserts `ops_catalog` matches `databricks.yml` extends to the three
  schema names.

---

## D-07 -- Oracle is ingested via JDBC, not queried via Lakehouse Federation

**Decided:** Oracle data is extracted with the Spark JDBC connector and landed in Databricks.
Unity Catalog's Lakehouse Federation was considered and rejected.

**Why**, recorded because this is the first question any reviewing architect asks:

1. **This is a lift-and-shift migration.** Transformations that today run on top of Oracle are
   being moved to Databricks. They need the data present in Databricks, not proxied to Oracle
   at query time.
2. **Federation gives no landing copy.** The design point of landing is a retained,
   time-partitioned mirror of what the source held -- that is what makes replay, audit and
   historical reprocessing possible. Federation reads live Oracle state and keeps nothing.
3. **Query-time dependency on Oracle is the thing the migration removes.** Federating would
   leave every migrated transformation dependent on Oracle availability, Oracle load, and the
   network between them -- reintroducing the coupling the migration exists to break.

**What would change it:** a use case that only needs to *read* current Oracle state
interactively, with no transformation, no history and no tolerance for a copy. That is not
this workload, and if one appears it should use federation directly rather than being added
to this framework.

**Work this implies:** Stage 7 records this in `docs/DESIGN.md` as a considered-and-rejected
alternative, with the three reasons above. Stage 4 needs nothing from it beyond proceeding.

---

## D-08 -- BigQuery: approach undecided, build nothing

**Status: OPEN. This is the one entry in this file that is not settled.**

The right approach for BigQuery is not yet known. Do not implement it, and do not let its
possible shapes influence the framework's design.

**The question to answer before any BigQuery work starts** is not "how do we build it" but
"which of these is it", because each looks like a different existing source:

| Approach | Looks like | Implication |
|---|---|---|
| Direct connector read, bounded query + watermark | **Oracle** | Reuses the cursor/watermark machinery almost exactly |
| Export to GCS, then read the exported objects | **Files** | Reuses Auto Loader; adds an export-orchestration step this framework does not currently own |
| Federation | Neither | No ingestion path needed at all -- but D-07's reasoning likely rules it out for the same reasons |

**Also unresolved and worth settling at the same time:** which connector is available on the
target runtime, whether cross-cloud egress from GCP to Azure is acceptable in cost and
policy, and who owns the GCP-side credentials.

**Until then:** Stage 6 writes the contract notes and the "adding a source type" section only.
No `sources/bigquery/` package, no config keys, no register file. The framework's extensibility
is proved by the grep gate, not by speculatively building a fourth source.

---

## D-09 -- Oracle: the four 4a defaults stand, and support can switch full vs delta

**Decided** in review of sub-step 4a. The first four confirm what 4a built; the fifth is
new work, folded into **sub-step 4b**.

1. **`num_partitions` defaults to 1.** A serial read is the safe wrong answer; a parallel
   read against a table whose value distribution nobody checked is not. Onboarding asks the
   source team for the partition column and sets both keys together.
2. **A cursor source with no `merge_keys` fails the load.** Not a warning. `merge_keys: []`
   remains the explicit waiver, and it is what a table with no stable key writes down.
3. **`source_schema` is configured per table**, in that table's own source file. There is no
   platform-wide default schema, so `conf/defaults/oracle.yaml` ships without one.
4. **The target is `{catalog}.oracle_<source_schema>.<source_table>`**, the table name
   unchanged from the source. Lower-cased for Unity Catalog; Oracle's upper-case name is what
   the audit row's `source_ref` carries.

5. **Support can switch a source between full and delta, and can bound a delta run for a
   replay, without a deploy.** This is the new work. It makes `incremental_mode`
   OPERATIONALLY OVERRIDABLE, which is a deliberate exception to the rule that structural
   fields decide what is extracted -- so the guardrails below are part of the decision, not
   an implementation detail:

   - New control column **`oracle_incremental_mode`** (`full` | `cursor` | `filter`),
     declared in `sources/oracle/spec.py` `control_columns` like the two tuning knobs.
   - Switching to `full` re-extracts everything. It is safe by construction where
     `merge_keys` are set (the MERGE absorbs the re-read) and DUPLICATES ROWS where they are
     waived. That asymmetry must be stated on the column comment and in the runbook.
   - **A full run must not advance the watermark**, and must not clear it: the source has no
     cursor bounds during that run, so it has nothing trustworthy to advance to. Switching
     back to `cursor` then re-reads from the last genuine delta boundary, which the MERGE
     absorbs. This is the same ordering rule as 4c's, applied to a mode change.
   - **Replay bounds an interval explicitly**: `replay_cursor_start` / `replay_cursor_end`,
     operational-ONLY (a bound checked into Git would re-apply on every future deploy),
     carried in the framework-owned `replay_controls` JSON exactly as Kafka's replay bounds
     are. They replace the stored watermark for that run only.
   - **A replay never writes `ingest_state`.** Already a 4c invariant; the replay bounds make
     it reachable, so 4c asserts it against these keys specifically.
   - Everything else stays structural. `cursor_column`, `cursor_type`, `merge_keys`,
     `filter_criteria`, `source_schema`, `source_table` are NOT overridable: switching which
     column is the cursor, or what the filter says, is a change to what the table means and
     belongs in a PR.

**Work this implies (sub-step 4b, and 4c for the lifecycle):**

| # | Change | Files |
|---|---|---|
| 1 | `incremental_mode` into `operational_keys` + `oracle_incremental_mode` control column | `sources/oracle/spec.py`, `sql/01_operational_config.sql`, tests |
| 2 | `replay_cursor_start` / `replay_cursor_end` as operational-only replay controls | `sources/oracle/spec.py`, `sources/oracle/config.py`, tests |
| 3 | A full run leaves the watermark alone; a replay never writes state | `sources/oracle/run.py` (4c), tests |
| 4 | The full-vs-delta switch and its duplicate-row caveat, as an operator procedure | `docs/CONFIGURATION.md`, `docs/RUNBOOK_SUPPORT.md` (4d) |

---

## Work list produced by these decisions

Apply as a short pass on its own branch **before Stage 3**, since all of it is Stage 2
territory and Stage 3 is already the largest stage in the project.

| # | Change | Files |
|---|---|---|
| 1 | Prefixed control columns; drop `source_overrides`; add `SourceSpec.control_columns`; wrong-source-type error | `framework/contracts.py`, `framework/control.py`, `sources/kafka/spec.py`, `sql/01_operational_config.sql`, tests |
| 2 | Remove `GRANT` from both SQL files; add the privilege list | `sql/01`, `sql/02`, `docs/RUNBOOK_CLIENT_IT.md` |
| 3 | Rename `batch_id` -> `txn_version` | `framework/audit.py`, audit DDL, `sql/03`, tests |
| 4 | `PARTITIONED BY (source_key)` + deletion vectors on `ingest_state`; extend VB-15 | `framework/tables.py` or `sql/02`, `docs/VERIFICATION_BACKLOG.md` |
| 5 | Audit table moves to the ops catalog; add `audit_schema` / `control_schema` / `logs_schema` vars | `conf/defaults.yaml`, `conf/environments/*.yaml`, `databricks.yml`, `sql/01`, `sql/02`, tests |

**Gate:** `ruff check`, `ruff format --check`, `pytest -m "not spark" -q` all green, test
count at or above 403, and the CORE section 7 grep still returns nothing -- item 1 is the one
most likely to break it.

---

## D-10 -- No file replay job or entrypoint. Documentation, not code.

**Decided and settled.** A fresh checkpoint already causes Auto Loader to re-read the whole
path (`cloudFiles.includeExistingFiles` defaults to `true`), files persist in ADLS so there
is no retention window to race against the way Kafka's replay races broker retention, and the
file source is landing-only, so there is no re-parse-from-landing replay shape either
(Kafka's `curated_replay` has no file equivalent because there is no curated layer to
re-derive). Building a `file_replay` run type and job would duplicate a recovery path the
checkpoint-reset mechanism already provides.

**What this framework actually verified, which narrows the recovery procedure to one lever:**
the framework-owned `replay_rerun_id` control column (mapped to the generic `rerun_id`
setting) is read in exactly one place for a primary run -- `framework/audit.py`'s
`AuditWriter.__init__` seeds `self.rerun_id` from it, purely as a label carried on every
audit row for this run. Nothing in `sources/file/config.py` or `sources/file/run.py` reads
`rerun_id` at all, and `sources/file/config.py`'s `VALID_RUN_TYPES = (RUN_TYPE_PRIMARY,)`
means `run_type` can never fork into a replay shape for this source the way it does for
Kafka (whose replay entrypoint derives a *separate* checkpoint directory,
`<checkpoint_root>/<topic_key>/replay/<rerun_id>`, per rerun id). So, for this source
specifically, setting `replay_rerun_id` alone -- with the checkpoint intact -- does **not**
cause a fresh read of anything; it only tags the audit row. The lever that actually unblocks
a re-read is `file_checkpoint_reset_id`, in combination with the checkpoint being genuinely
absent (deleted, or never having existed for this `source_key`) -- exactly the mechanism
`framework/checkpoint.py::guard_against_checkpoint_reset` already implements and
`docs/RUNBOOK_SUPPORT.md` section 9.3/9.4 already documents for the accidental-loss case.
This decision reuses that same mechanism *deliberately*, as the recovery procedure, rather
than asserting a second, unwired lever exists. Recorded here rather than silently written
into the runbook as fact, per `CLAUDE.md`'s honesty invariant -- a runbook step that does not
do what it claims is exactly the kind of thing that gets found out at 3am.

**Work this implies, documentation only, three places:**

1. `conf/sources/_TEMPLATE_file.yaml` -- an inline note that this source has no replay job,
   and where the procedure lives (`docs/RUNBOOK_SUPPORT.md` section 9).
2. `docs/CONFIGURATION.md` -- a MUST-READ row in the file-sources section stating that there
   is no replay job, that a full re-read follows from the checkpoint being reset via
   `file_checkpoint_reset_id` (not from the generic `replay_rerun_id` label alone), and that a
   bounded re-read narrows `source_path` / `path_glob` for that one recovery run rather than
   an offset or timestamp window.
3. `docs/RUNBOOK_SUPPORT.md` -- the recovery procedure, in order, stating plainly that step 1
   is not optional:
   1. **DELETE the affected landing partition(s) first.** File landing is append-only, so a
      re-read without this step appends everything again and silently duplicates the data.
   2. Set a fresh, previously unused `file_checkpoint_reset_id`.
   3. Run the normal file job.
   Bounded re-reads are done by narrowing `source_path` or `path_glob` for that one run, not
   by an offset window -- there is no such window for this source.

**What would change it:** a concrete need to reprocess a bounded window of already-checkpoint
-tracked files without deleting the checkpoint (which today has no answer beyond re-presenting
files under new names, `docs/build_log/STAGE_5_REPORT.md` decision 7) -- that is a genuinely
new capability, not a documentation gap, and would need its own stage.

---

## D-11 -- Maintenance coverage: extend the maintenance job to the file source, and close the ops_catalog gap if still open

**Decided.** `resources/job_maintenance.yml` and `sql/04_maintenance.sql` are extended to
cover the file source's landing table, the same `OPTIMIZE` + `VACUUM` shape every Kafka topic
and (still outstanding, see below) no Oracle table currently gets.

**The `ops_catalog` gap Stage 2b flagged as Blocked is confirmed CLOSED, not re-opened here.**
Stage 2b's report recorded: *"`resources/job_maintenance.yml` does not pass an `ops_catalog`
parameter... `sql/04`'s audit-table `OPTIMIZE`/`VACUUM` statements now need `:ops_catalog`...
Until Stage 6 adds it, the maintenance job's two audit-table statements will fail to bind."*
Stage 3's report records fixing this as one of three supplied additions: *"`resources/
job_maintenance.yml` passes `ops_catalog`, which Stage 2b listed as Blocked."* Reading the
current file confirms it: `job_maintenance.yml`'s `parameters:` block already declares
`ops_catalog: ${var.ops_catalog}` with a comment naming this exact history, and
`sql/04_maintenance.sql`'s two audit-table statements already bind `:ops_catalog`. Nothing to
do here beyond recording that the gap is shut, so a future session does not re-open it as if
newly discovered.

**Scope, stated precisely because a related gap is deliberately NOT closed here:** this
decision covers the FILE source only, as asked. Oracle's landing table
(`oracle_claim_header`) is not in either file either -- `docs/build_log/STAGE_5_REPORT.md`
decision 8 already flagged this as a shared gap across both non-Kafka sources, "worth a
dedicated pass rather than a one-line addition per source." Adding File without also adding
Oracle leaves that pass still owed; recorded as a decision for the human in the stage report
rather than silently expanded to cover it, since the instruction that produced this decision
named the file source specifically.

**What would change it:** a follow-up decision to close Oracle's maintenance gap in the same
pass, or a redesign of `job_maintenance.yml`'s one-task-per-table shape so a new source's
maintenance coverage does not require hand-editing a YAML list at all (STAGE_5's own decision
8 already floats this; not attempted here, as the "do not build a coverage gate" rule for this
stage plus "smallest correct change" both counsel against a template-generation mechanism for
three data points).

---

## D-12 -- Storage auth: SAS tokens and managed identity are deliberately not implemented

**Decided.** The file source's `conf/storage.yaml` register and `sources/file/security.py`
support exactly the two auth modes Stage 5 built -- `account_key` and `service_principal` --
and no others. SAS tokens and managed identity (and Unity Catalog credential passthrough) are
deliberately out of scope, documented as a stated limitation with the reason, not as an
oversight: each needs either a token-provider class this project cannot verify exists on the
target runtime, or workspace-level UC wiring outside this repository's control, so adding one
is a future code change with its own verification, not a config guess -- the same restraint
`sources/oracle/config.py` already applies to JDBC auth (`auth_mode: basic` only).

**Read against the instruction that produced it, and reconciled with what already exists:**
the instruction's heading called this "service principal only." Read literally that would mean
removing `account_key` -- which is the auth mode the repository's own shipped worked example,
`conf/sources/file_claims_inbound.yaml` (`storage_ref: adls_landing_prod`, `auth_mode:
account_key`), actually uses, and which Stage 5's own decision 9 recorded as a deliberate,
tested choice alongside `service_principal`. The instruction's own elaboration names only "SAS
tokens and managed identity" as excluded -- it does not say `account_key` should go -- and its
action is "document... as a stated limitation," not "remove a mode." Removing a shipped,
tested auth mode is exactly the kind of larger, re-litigating change `CLAUDE.md`'s working
method reserves for an explicit human decision, not something to infer from a heading. So this
decision keeps both `account_key` and `service_principal`, and narrows only what the
instruction's body actually asked to exclude. Flagged for the human in the stage report in
case "service principal only" was meant literally, going forward for new onboarding.

**Work this implies:** `docs/DESIGN.md` section 11's existing "Deliberately not built" bullet
on this topic is expanded to state the reason inline (token-provider / UC-wiring
verifiability) rather than only pointing at `sources/file/security.py`'s docstring for it.

**What would change it:** an explicit human confirmation that `account_key` should be retired
for new onboarding (existing usage would need a migration plan, since nothing is deployed
today but the worked example exists) -- at which point this becomes a real code and config
change, not a documentation one.

---

## D-13 -- File sources may read from a Unity Catalog Volume path directly, and this is preferred

**Decided.** A file source's `source_path` may now be a Unity Catalog Volume path
(`/Volumes/<catalog>/<schema>/<volume>/...`) instead of a path within an ADLS container named
by `storage_ref`. A Volume path is UC-governed: this framework applies **no** storage
credentials to it and requires **no** `storage_ref` for it. The `abfss://` + service-principal
/ account-key path keeps working unchanged for now -- this is an addition, not a replacement.

**Mechanism, so the two forms cannot silently combine into a wrong config:** `storage_ref`
moves out of `SOURCE_SPEC.required_keys` (it stays a valid, structural key) and
`sources/file/config.py` decides which form applies from `source_path`'s own shape: a path
starting with `/Volumes/` is Volume-governed, `storage_ref` must be **absent**, and no
`fs.azure.*` session options are ever built or applied for it; any other path requires
`storage_ref`, exactly as before. Setting `storage_ref` alongside a `/Volumes/...`
`source_path` is a config error naming both, because a source declaring both leaves no
honest answer to "which one governs this read."

**Preferred, but not forced:** the shipped worked example (`file_claims_inbound.yaml`) stays
on its existing `abfss://` + `account_key` form -- switching it was not asked for, and doing
so without knowing whether Volumes are actually reachable from the target compute for this
workload would be exactly the kind of unverified assumption `CLAUDE.md` exists to keep out of
shipped configuration. "Preferred" is expressed in the onboarding template and the
configuration reference: a new source should default to a Volume path when one is available,
falling back to `storage_ref` when it is not.

**New verification-backlog entry** (VB-28): will file sources use UC Volumes or `abfss://` in
each environment? Until answered, both paths are supported and neither is assumed universal.

**The planned simplification this sets up, recorded so nobody maintains two credential paths
indefinitely:** if UC Volumes turn out to be reachable in every environment this framework
targets, `conf/storage.yaml`'s auth block, `framework/security.py::apply_session_options`, and
VB-26 (the session-scoped ADLS credential question) can all be deleted -- a Volume path takes
no credentials from this framework at all, governed instead by Unity Catalog grants on the
Volume itself. Recorded in `docs/DESIGN.md` section 11 as a planned simplification contingent
on VB-28's answer, not attempted now, because narrowing to one credential path is a decision
for whoever answers VB-28, not something to guess at while both are still plausibly needed.

**What would change it:** VB-28 coming back "Volumes are not reachable/permitted for this
workload in at least one environment" -- in which case the two-path support this decision
adds is the permanent shape, not a transitional one, and the "planned simplification" note
above should be removed rather than carried forward indefinitely.

---

## D-14 -- Stage 5's five smaller decisions, and `framework/checkpoint.py`, are settled

**Decided.** Accepted as built, not revisited in this stage:

1. `schema_mode: hints` reads as `cloudFiles.inferColumnTypes`, not `cloudFiles.schemaHints`
   (`docs/build_log/STAGE_5_REPORT.md` decision 1).
2. `failure_mode` for the file source gates on the rescued-row count, not on file-level read
   failures (decision 2).
3. `source_path` is the path within the container only, never a full `abfss://` URL, for the
   `storage_ref`-governed form (decision 3) -- D-13 above adds a second form; it does not
   revisit this one.
4. `target_schema` / `target_table` are explicit source-file settings via the same
   `target_tokens` mechanism Oracle uses (decision 4).
5. `landing_partition_by`, not the brief's illustrative `partition_by:` (decision 5).

**`framework/checkpoint.py`** (`docs/build_log/STAGE_5b_REPORT.md`) -- the hoisted, shared
checkpoint-reset guard used by both Kafka and the file source -- is accepted as the eleventh
framework module, and its own decisions (folding it into `state.py`/`audit.py` was considered
and rejected; the merged refusal message deliberately drops Kafka's replay-job
recommendation for the file source; the reset-engaged log event lost its per-source prefix)
stand as written.

**What would change any of these:** the same triggers each stage report already named --
e.g. a third checkpoint-based source type reopening `framework/checkpoint.py`'s module-count
question, or evidence `hints` was meant as `schemaHints` after all. None of those triggers
occurred in this stage.
