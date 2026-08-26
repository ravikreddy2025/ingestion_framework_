# Support Runbook

For the production support team. **You do not need to read Python to use this document.**

> Lost in the file tree? [NAVIGATION.md](NAVIGATION.md) maps every file. The two you will
> actually use are `sql/03_support_queries.sql` (triage, Q1-Q24) and this page.

Everything here is a SQL statement or a Workflows job parameter. If a fix requires editing
code, it is an escalation — see section 6.

**Golden rules**

1. **Read the audit table first.** Before touching state or a checkpoint, before starting a
   replay.
2. **Never delete a checkpoint (Kafka or Files).** It causes silent data loss. Section 5.4
   explains why; if it happens anyway, 5.4a / 9.4 is the only sanctioned way back.
3. **Never hand-edit `ingest_state`** except via the one deliberate Q18 procedure (Oracle
   watermark correction). It is written by the ingestion job only.
4. **A failed run is usually self-healing.** Re-run before doing anything clever.

**Three tables, three jobs.** Every source type shares them — know which one you are in
before you touch anything:

| Table | Job | Support can |
|---|---|---|
| `{ops_catalog}.{audit_schema}.ingest_audit` | EVIDENCE. What every run did, per layer. Best-effort by design — a missing row is weak evidence, never proof a run did not read. | **Read only.** |
| `{ops_catalog}.{control_schema}.ingest_control` | CONFIGURATION. What support may change at runtime, no deploy. | **Read and write.** |
| `{ops_catalog}.{control_schema}.ingest_state` | TRUTH. Where each source (that has one) actually got to. Written by the job only. | **Read only** — except Q18, deliberately. |

---

## 1. Daily health check — one query, every source type

Run **Q1** in [`sql/03_support_queries.sql`](../sql/03_support_queries.sql). One row per
source per layer for the last 24h, across Kafka, Oracle and File sources at once — the
point of one shared audit table.

| What you see | Meaning | Action |
|---|---|---|
| Every source has its layer rows, `failures = 0` | Healthy | None |
| `skipped > 0` | Source is disabled in the control table | Check `notes` — was it left off by mistake? |
| A source missing entirely | Run Q1b — **which sources have NOT run** | Check the Workflows run history for that job |
| `quarantined > 0` and rising | Kafka records or file rows failing to fit their schema | §5.2 / §9.2 |
| `failures > 0` | Go to section 2 | |

Also worth a weekly glance:

- **Q14** — which sources are running with `fail_on_data_loss` (Kafka) disabled. That
  setting is structural (it lives in Git, not the control table), so it cannot be seen any
  other way — it is recorded in `source_detail` on every run precisely so this is answerable.
- **Q15** — which sources lost more than 5% of their last run to quarantine or rescue.
- **Q16** — is any source permanently behind (`pending_work` growing, not just non-zero once).
- **Q11 / Q19** (source-specific duplicate checks) should always return zero rows. If either
  does not, stop and escalate — section 6.

---

## 2. A run failed — which layer, and is it stuck?

Run **Q2**. It pivots the per-layer audit rows into one row per `run_id` — the ID that
means the same thing for every source type, unlike `txn_version` (a Kafka microbatch id for
one source type, a batch `run_sequence` for another):

| `run_status` | `landing_status` | `curated_status` (Kafka only) | What happened | Go to |
|---|---|---|---|---|
| `FAILED` | `COMPLETED` | *(null)* or `FAILED` | Died between landing and curated (Kafka) | §5.1 |
| `FAILED` | `STARTED`, no `COMPLETED` | | Landing itself failed | §5.3 / §8.2 / §9.1 |
| `COMPLETED` | `COMPLETED` | `COMPLETED` | This run is fine — look at a different one | — |
| *(a `run/STARTED` row with no matching `COMPLETED`/`FAILED`)* | | | The process died hard enough that even the failure row was not written | Look at the driver log for that `run_id` next — the audit table has told you everything it can |

Then run **Q3** to find out whether the *same* failure has repeated across multiple runs of
the same source. That distinction decides everything:

- **Failed once** → transient. Re-run. §5.1 / §8.1 / §9.1
- **Failing every run** → stuck. §5.2 (Kafka poison batch) / §8.2 (Oracle) / §9.2 (Files)

---

## 3. Onboarding a new source

Onboarding is a **joint** task. The development team raises a PR for the source's
configuration; you own the control-table row and the verification. The steps are the same
shape for every source type — only the pre-flight questions differ.

### Your part, before the PR is deployed

Confirm with the requesting team:

- **Kafka:** topic name and Schema Registry **subject** (usually `<topic>-value`); which
  cluster and registry; secret scope in every environment; certs on a UC Volume for mTLS;
  expected daily volume; whether history is wanted (`starting_offsets`).
- **Oracle:** which column is the cursor and when it is stamped; the table's stable key
  (`merge_keys`); a partition column and roughly how many rows; any LOB/RAW/INTERVAL/TZ
  columns; the JDBC profile and secret scope in every environment.
- **Files:** the full schema and whether it drifts; whether files are ever rewritten in
  place; whether the filename carries data; roughly how many files land per day; the
  storage profile (or Volume path) and secret scope in every environment.

### After the PR is deployed

1. Insert the control row (optional — the source runs on YAML defaults without one, but an
   explicit row gives you somewhere obvious to look):

```sql
INSERT INTO {ops_catalog}.{control_schema}.ingest_control
  (source_key, source_type, enabled, notes, updated_by, updated_at)
VALUES ('gma_provider_updates', 'kafka', true, 'onboarding GMA-4412',
        current_user(), current_timestamp());
```

2. Trigger the primary job for that source once, manually, **in dev first**.
3. Verify with Q1 — every layer the source has should show `COMPLETED`.
4. Verify with the source's duplicate check (Q11 for Kafka, Q19 for Oracle) — zero rows.
5. Promote to preprod, then prod. The same source file is used in all three; only the
   environment differs.
6. Hand back to the requesting team to validate the landed data.

### If the first run fails

| Error text contains | Cause | Fix |
|---|---|---|
| `must be a Unity Catalog Volume path` | A cert or checkpoint path is on DBFS | Dev team, config PR |
| `could not read secret ... from scope` | Scope missing, or the SP lacks READ | Platform/infra |
| `unknown environment` | Bundle target has no matching `conf/environments/<env>.yaml` | Dev team, config PR |
| `uses {catalog}, which is not defined` | Environment file is missing a `vars:` entry | Dev team, config PR |
| `no entry for /schemas/ids/N (HTTP 404)` (Kafka) | Records produced against a **different** registry | Dev team, config PR |
| `unreachable ... NCC private endpoint` | Network path missing | Platform/infra |
| Job hangs then times out | Broker/JDBC host wrong, or a firewall | Platform/infra |
| `ClassNotFoundException: oracle.jdbc.OracleDriver` | The Oracle JDBC driver is not installed on the cluster | Platform/infra — it is not bundled with DBR and nothing in this repo installs it |
| `... row(s) in this batch did not fit the configured schema` (Files) | `schema:` does not match the real files | Dev team, config PR — or `file_failure_mode = QUARANTINE` to unblock and investigate |

---

## 4. Decommissioning a stale source

**Order matters.** Doing this out of order either breaks the scheduled run or leaves
orphaned state. Do not skip step 1 or step 2.

### Step 1 — Confirm it is genuinely stale

```sql
SELECT layer, status, max(event_ts) AS last_seen, sum(record_count) AS records
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE source_key = 'legacy_feed_v1' AND audit_date >= current_date() - INTERVAL 90 DAYS
GROUP BY layer, status ORDER BY last_seen DESC;
```

- [ ] Domain owner has confirmed in writing
- [ ] **Downstream consumers of the target table identified and signed off** — check
      lineage in Unity Catalog before assuming nothing reads it
- [ ] Data retention / compliance position agreed (see step 4)

### Step 2 — Stop ingestion (immediate, reversible, no deploy)

```sql
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = false,
    notes = 'DECOM-123 - decommission agreed with GMA, ticket link',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'legacy_feed_v1';
```

Wait one scheduled cycle. Confirm a `SKIPPED` audit row appears — that proves the job ran
and deliberately consumed nothing. **This is your rollback point:** flip `enabled` back to
`true` and everything resumes from where it left off (Kafka/Files: the checkpoint is
untouched; Oracle: the watermark is untouched).

### Step 3 — Remove the configuration (development team, one PR)

Raise a ticket for the dev team to remove, **in the same PR**:

- the task from the source's job template (`job_ingest_primary.yml` / `job_ingest_oracle.yml`
  / `job_ingest_file.yml`)
- `conf/sources/legacy_feed_v1.yaml`

**Order matters:** if the YAML is removed while the job task still exists, the scheduled run
fails with "structural config file not found". Disable (step 2) → remove both → deploy.

### Step 4 — Data disposition

Decide per your retention policy. All three are optional and independent. Repeat per
environment.

```sql
-- Kafka: landing/curated/quarantine are all one table per topic, so decommissioning is a
-- DROP, not a filtered DELETE.
DROP TABLE IF EXISTS platform_prod.landing.legacy_feed_v1;
DROP TABLE IF EXISTS platform_prod.curated.legacy_feed_v1;
DROP TABLE IF EXISTS platform_prod.landing.legacy_feed_v1_quarantine;

-- Oracle / Files: one landing table, named by the source's own derivation.
DROP TABLE IF EXISTS platform_prod.oracle_claims.claim_header;
DROP TABLE IF EXISTS platform_prod.files_claims.claims_inbound;
```

**Keep the audit rows.** They are the historical record of the feed and they age out with
the audit table's own retention.

### Step 5 — Clean up state

Only **after** the PR from step 3 is deployed, or the job will recreate it:

```python
# In a notebook. Path pattern: <checkpoint_root>/<source_key>/... (Kafka, Files)
dbutils.fs.rm("/Volumes/platform_prod/ingestion/checkpoints/legacy_feed_v1", recurse=True)
```

```sql
DELETE FROM {ops_catalog}.{control_schema}.ingest_control WHERE source_key = 'legacy_feed_v1';
-- Oracle only, if it had a watermark:
DELETE FROM {ops_catalog}.{control_schema}.ingest_state WHERE source_key = 'legacy_feed_v1';
```

Ask platform/infra to revoke the Kafka/Oracle/storage credential if it was source-specific.

### If the source is ever re-onboarded under the same `source_key`

**Kafka / Files:** the framework will **refuse to start** if landing still holds rows for
that source but the checkpoint is gone. That guard is protecting you (§5.4 / §9.3). If step
4 did not `DROP TABLE` the landing table, you have two options: `DROP TABLE` it now, or keep
the history and set a fresh `<type>_checkpoint_reset_id` per §5.4a / §9.4 instead — either
way the re-onboarded stream needs a transaction identity with no prior commits. Onboarding
under a new `source_key` also works, and needs neither.

**Oracle:** a deleted `ingest_state` row simply means the next run starts as a first run —
a full extract if `incremental_mode: full`, or everything up to the current high water if
`cursor`. No guard fires either way.

### Decommission checklist

- [ ] Domain owner signed off
- [ ] Downstream consumers signed off
- [ ] `enabled = false`, `SKIPPED` row confirmed
- [ ] PR merged and deployed (job task + YAML removed)
- [ ] Data disposition actioned per policy, in every environment
- [ ] Checkpoint deleted (Kafka/Files)
- [ ] Control row (and, for Oracle, the state row) deleted
- [ ] Credential revoked

---

## 5. Kafka incident playbooks

### 5.1 A batch failed once — transient

**Symptom:** Q2 shows `landing COMPLETED`, no `curated COMPLETED`. Q3 shows the batch has
failed only in one run.

**What is actually happening:** the retry replays the **same** batch over the **same** Kafka
offsets. Delta skips the landing write it already committed and writes curated. No
duplicates are possible.

**Action:** Re-run. Workflows → the failed run → **Repair run**. Or let the next schedule
pick it up. Then confirm with Q2.

### 5.2 The same batch fails every run — poison batch

**Symptom:** Q3 shows one `run_id` failing repeatedly. The topic is stuck and the backlog is
growing.

**Cause is almost always** a schema the registry does not have.

**Fix A (preferred) — get the schema registered.** Raise with the producing team. Once
registered, the next run succeeds on its own. Nothing else to do.

**Fix B — unblock now, recover later.** No deploy needed — **Q6**:

```sql
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET kafka_failure_mode = 'QUARANTINE',
    notes              = 'INC12345 - unblock stuck source, schema 5513 unregistered',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';
```

Next run: the unparseable records go to the quarantine table **with their raw bytes kept**,
the batch completes, the stream drains.

**Then, once the schema is registered:**

1. Find what was quarantined — **Q5**.
2. Recover it with a **curated replay** (§5.6), filtered on that `writer_schema_id`.
3. Set `kafka_failure_mode` back to `NULL` (inherit `FAILFAST`).

**Do not leave a source on `QUARANTINE` permanently.** It converts loud failures into a
table nobody looks at.

### 5.3 Landing itself failed

**Symptom:** Q2 shows `landing STARTED` with no `COMPLETED`.

Nothing was written anywhere, so the retry is clean. Read `error_class` / `error_message`:

| Error | Owner |
|---|---|
| Permission / UNAUTHORIZED on the table or Volume | Platform/infra — SP grants |
| Kafka auth / SSL handshake | Platform/infra — credential or cert expiry |
| Storage or cluster failure | Re-run |

### 5.4 Someone deleted a Kafka checkpoint

**This is the one failure that looks like success.** Batch ids restart at 0; Delta has
already recorded higher versions and **skips every write as a duplicate**. The job reports
success and ingests nothing.

The framework refuses to start in this state with a message beginning `REFUSING TO RUN`.

**Do not work around it.** Do not delete landing rows to silence it. Recover with a **Kafka
replay** (§5.5) using a new `rerun_id` — it runs under its own checkpoint *and* its own
Delta transaction identity, so it cannot collide.

**Prevention:** there is never a reason for support to delete a checkpoint. To reprocess
data, use a replay job.

**Important:** a replay backfills the missing *data*. It does **not** unblock the *primary
job* — the primary will keep refusing to start, on every scheduled trigger, until you
complete 5.4a below. Do not treat "the replay finished" as "the incident is closed."

### 5.4a Restarting the primary after a genuine checkpoint loss

Use this only once you have confirmed the checkpoint is truly gone (not a transient Volume
access error — a message NOT beginning `REFUSING TO RUN` but instead reporting it "could
not determine whether the checkpoint exists" is a different, unrelated failure and means the
Volume, not the checkpoint, needs attention) and you accept starting that source's primary
stream over from `latest`.

**Do not try to work around this any other way.** In particular, do not delete or truncate
landing rows to make the guard's row-count check pass — that satisfies the code but not the
underlying problem: Delta remembers the primary's last committed transaction version
independently of which rows currently exist, indefinitely by default, so a plain restart
under the same identity would silently skip every write below the old watermark.

**THE RESET ID MUST BE ONE THIS SOURCE HAS NEVER USED.** The reset works by forking the
source's Delta transaction identity, so the restarted stream has no committed versions to
collide with. Reusing an id keeps the identity the last reset created — against which Delta
already holds high versions — and every write would be skipped as a duplicate, exactly like
the failure this whole procedure exists to escape. The job refuses to start in that state
and names the spent id, but check first rather than finding out from a failed run.

Run these five steps in order. Steps 1 and 5 are the ones people skip, and they are the two
that decide whether any data is lost.

---

**Step 1 — Record where the stream actually got to. DO THIS FIRST.**

The restart begins at `latest`, so everything between the last committed offset and the
restart is a gap only this query can tell you the size of. Once the stream restarts, the
evidence is gone.

Run **Q13** in `sql/03_support_queries.sql` (*"last committed end offset per partition"*),
substituting your source key and topic name. Paste the result into the incident:

```
kafka_partition   last_committed_end_offset
0                 45231
1                 44870
2                 45009
```

**Step 2 — Check the reset id you are about to use has never been used.**

Run **Q6d**. Any row means that id is spent — use the current incident's id instead.

```sql
SELECT run_id, rerun_id, min(event_ts) AS first_used, max(event_ts) AS last_used
FROM {ops_catalog}.{audit_schema}.ingest_audit
WHERE source_key = 'rcm_claim_status'
  AND run_type   = 'primary'
  AND rerun_id IS NOT NULL
GROUP BY run_id, rerun_id
ORDER BY last_used DESC;
```

**Step 3 — Set a FRESH reset id.** Use **Q6c**:

```sql
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET kafka_checkpoint_reset_id = 'INC12345',
    notes                     = 'INC12345 - primary checkpoint lost, restarting under a fresh identity',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';
```

On the next scheduled trigger the job logs `checkpoint_reset_engaged` (`source_key` and
`source_type` on the same line identify which source) and starts a fresh stream: a new
checkpoint, and a new Delta transaction identity with no committed history to collide with.
That fork is what makes the restart safe, not merely permitted.

**Step 4 — Backfill the gap with a bounded Kafka replay.**

The primary is now healthy but the window between step 1's offsets and the restart is
missing. Run the replay job (§5.5) with `replay_starting_offsets` set to **exactly the
offsets from step 1** and an ending bound at the restart point. The replay uses its own
checkpoint and its own transaction identity, so it cannot collide with the stream you have
just restarted.

**Step 5 — Clear nothing.**

`kafka_checkpoint_reset_id` is **not** a toggle, and it does not need tidying up. Once the
checkpoint exists again the field is inert for the guard — it only forks the app id, which
must STAY forked. Blanking it would revert to the original identity, which still carries the
pre-incident watermark, and would silently reproduce the exact bug this override exists to
avoid. Leaving it set is correct and permanent; `notes` and `updated_at` are the audit
trail of when and why, and Q6d is what stops it being reused.

---

**Checklist**

| | Step | Where |
|---|---|---|
| [ ] | Confirmed the checkpoint is genuinely gone, not an unreadable Volume | the error message |
| [ ] | Recorded the last committed end offset per partition | Q13 |
| [ ] | Confirmed the reset id has never been used for this source | Q6d |
| [ ] | Set a fresh `kafka_checkpoint_reset_id` | Q6c |
| [ ] | Primary ran and logged `checkpoint_reset_engaged` | driver log, or Q1 |
| [ ] | Backfilled the gap with a bounded replay from step 1's offsets | 5.5 |
| [ ] | Verified the gap is closed | 5.7 |
| [ ] | Left `kafka_checkpoint_reset_id` set | — |

### 5.5 Kafka replay — data is missing at source

Use when data never arrived: a consumer outage, a gap, or after section 5.4.

**Find the restart point.** From Q2, take the `read_to` (`position_end`) of the last
**successful** run:

```
{"rcm.claim.status.v2":{"0":45231,"1":44870,"2":45009}}
```

**Workflows → `[prod] Kafka Ingest - REPLAY from Kafka` → Run now with different parameters:**

| Parameter | Value |
|---|---|
| `source_key` | `rcm_claim_status` |
| `environment` | defaults to the deployed target — leave it |
| `rerun_id` | `INC12345` — your incident number. **Required.** |
| `replay_starting_offsets` | the JSON above |
| `replay_starting_timestamp` | *leave empty* — set **one** of offsets or timestamp, never both |
| `replay_ending_offsets` / `replay_ending_timestamp` | optional; set one to cap the replay window |

To replay by time instead, leave `replay_starting_offsets` empty and set
`replay_starting_timestamp` to `2026-08-11T09:00:00Z`. If any partition has no record at or
after that time the run **fails** rather than silently skipping — that is deliberate.

The replay runs under its own checkpoint and never touches the primary one. **The daily
schedule keeps running normally throughout.**

#### After the incident: clean up the replay checkpoint

Each Kafka replay creates its own directory under
`<checkpoint_root>/<source_key>/replay/<rerun_id>`. Nothing removes them, so they accumulate
one per incident, forever.

Once the incident is closed **and** you have confirmed the data landed (Q10), delete that
one directory:

```python
dbutils.fs.rm("/Volumes/<catalog>/ingestion/checkpoints/<source_key>/replay/<rerun_id>", recurse=True)
```

Delete **only** the `replay/<rerun_id>` directory you created. Never the `primary` directory
beside it, and never the `<source_key>` directory above it — see section 5.4 for what
happens then. If you might rerun the same `rerun_id`, leave it: re-running with the same id
resumes that replay rather than starting it again.

### 5.6 Curated replay — the bytes are fine, the parse was not

Use when records were quarantined, a schema was registered late, or curated data is wrong.
**Does not contact Kafka**, so it works long after Kafka retention has expired.

**Workflows → `[prod] Kafka Ingest - REPLAY Curated from Landing`:**

| Parameter | Value |
|---|---|
| `source_key` | `rcm_claim_status` |
| `rerun_id` | `SCHEMAFIX-88` |
| `replay_landing_filter` | `writer_schema_id = 5513` |

`replay_landing_filter` is any SQL condition over the landing table — `writer_schema_id = N`
is usually the precise one; `ingest_date BETWEEN '2026-08-01' AND '2026-08-03'` for a
window. It is **required** on purpose: re-parsing all history by accident is expensive.

The topic condition is added automatically, so you cannot accidentally rewrite another
topic's curated table.

### 5.7 After any replay — verify

- **Q10** — replay rows vs primary rows, and offsets present in landing but missing from
  curated. Every replayed row is tagged `ingested_via` and `replay_run_id`.
- **Q11** — duplicates. Must be zero.
- **Q9** — did the replay run, and what did it cover, across every source type.
- Clear any replay controls you parked in the control table (Q8).

### 5.8 Emergency stop

```sql
UPDATE {ops_catalog}.{control_schema}.ingest_control
SET enabled = false,
    notes = 'INC12345 - producer emitting corrupt payloads',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE source_key = 'rcm_claim_status';
```

Takes effect on the next run. The job still runs and writes a `SKIPPED` audit row, so
silence is never ambiguous. **Disabling stops replays too** — re-enable before replaying.

**More than one source affected** (a shared cluster, database or domain incident) — **Q7b**
in `sql/03_support_queries.sql` toggles a list, or every source of a given `source_type`, or
every source in a `domain`, in one statement. **Q7c** lists every currently-disabled
source and how long it has been off — run it after any incident involving Q7b, so nothing
gets left off by mistake.

---

## 6. Escalate to the development team when

- A duplicate check (Q11, Q19) returns rows — the idempotency mechanism is not behaving as
  designed
- The `REFUSING TO RUN` guard fires and a FRESH `<type>_checkpoint_reset_id` does not
  resolve it — a replay alone never resolves this guard, by design
- The same error recurs after both a re-run and a `QUARANTINE` flip
- An error mentions `from_avro`, `reader/writer schema`, or a self-check failure
- An error mentions `unknown environment` or an unresolved `{placeholder}`
- A fix would require editing YAML, Python, or a job definition
- Anything you would need to delete a checkpoint or drop **the shared audit/state/control
  table** to fix (dropping a source's own landing/curated/quarantine table during
  decommission, section 4, is routine and does not need escalation)

**Include in the escalation:** `source_key`, `source_type`, environment, the failing
`run_id`, the Q2 output for that run, and `error_class` / `error_message`.

---

## 7. What support can change — and what it cannot

| You own — no deploy | Needs a PR from the dev team |
|---|---|
| `enabled` (emergency stop, every source type) | Table names, catalogs, schemas |
| `kafka_failure_mode` / `file_failure_mode` (unblock a stuck source) | Partition columns, merge/dedup keys |
| `kafka_max_offsets_per_trigger` / `file_max_files_per_trigger` (shrink batches) | Cluster / registry / JDBC / storage profile references |
| `oracle_fetch_size` / `oracle_num_partitions` (reduce load on the source database) | Checkpoint or schema-location roots |
| `oracle_incremental_mode` (the full/delta switch — a recovery action, not tuning) | Broker/registry/JDBC endpoints, storage accounts, and secret scopes |
| All replay parameters | Adding or removing a source's config file |
| `kafka_checkpoint_reset_id` / `file_checkpoint_reset_id` *(incident use only, single-use)* | Anything in Python |
| — | Anything the control table has no named column for. **There is no free-form JSON escape hatch for a standing, source-specific setting** (D-01 removed the old `source_overrides` column) — a setting either has a dedicated column here, or support cannot change it without a PR |

The split is deliberate: things with production blast radius get code review, things needed
at 3am do not.

**One deliberate, narrow exception:** `replay_controls` (framework-owned, unprefixed) *is*
still a JSON column, but it carries only structured, incident-scoped replay bounds — not a
standing behaviour override — and is validated against that source's own spec on the next
run, so a typo in it produces the same error a YAML typo would.


---

## 8. Oracle incident playbooks

SQL and job parameters only — nothing here needs a deploy. Every query number refers to
`sql/03_support_queries.sql`.

### 8.1 An Oracle run failed once — transient

Symptom: one failed run, `error_class` is a JDBC or network error, the next run is fine.

1. Q17 — confirm the watermark did **not** move. It should still hold the previous run's
   `position_end`. That is the design: nothing advances until a write has committed.
2. Re-run the task. It re-extracts the identical interval.
3. Nothing else to do. With `merge_keys` set the re-read de-duplicates; without them the
   interval appends twice, and Q19 lists the duplicates.

### 8.2 Every run fails — the source database is refusing the load

Symptom: repeated failures, or a DBA asking why their database is busy at 03:30.

1. Reduce the load without a deploy — Q20:
   `oracle_num_partitions` down (fewer concurrent sessions) and/or `oracle_fetch_size`
   down (less memory per session, more round trips).
2. If it must stop entirely: `enabled = FALSE` (Q7). The watermark stays where it is, and
   the next enabled run covers the whole gap in one interval.
3. Escalate if the extract needs re-shaping — a `filter_criteria`, a projection, or a
   different cursor column is a PR, not a control-table change.

### 8.3 A run reported success but the data looks short

1. Q21 — what interval did that run actually cover? The audit row's `position_start` /
   `position_end` are the bounds, and `source_detail.query` is the **exact SQL** the run
   sent. Compare it against what you expected: a `dynamic_date_filter` or a
   `filter_criteria` narrows the extract in a way the row count alone will not explain.
2. Q17 — is the watermark where that run left it?
3. If rows exist in Oracle inside the interval and not in landing, this is VB-25 (a
   late-committing transaction with a low cursor value). It is not fixed by re-running:
   use 8.5 to re-extract the window explicitly, and raise the cursor-stamping question
   with the source team.

### 8.4 The watermark is wrong

A watermark **too far forward** silently skips rows. A watermark **too far back**
re-extracts — safe with merge keys, duplicating without them.

1. Q21 — find the last run you trust and take its `position_end`.
2. Q18 — set the watermark to that value. The query is deliberately a single-row MERGE with
   the source key spelled out; read the WHERE clause before running it.
3. Re-run the task. Confirm with Q17 and Q21.

**Never** set a watermark forward to "skip a problem". The rows in the gap are not
extracted by any later run.

### 8.5 Re-extract a window — the Oracle replay

Use when a window landed wrong, or the source corrected its own data.

```
databricks bundle run replay --params \
  source_key=oracle_claim_header,run_type=oracle_replay,rerun_id=INC12345,\
  replay_cursor_start=2026-08-01 00:00:00,replay_cursor_end=2026-08-02 00:00:00
```

* `rerun_id` is required and tags every row the replay writes (`replay_run_id`).
* The bounds replace the stored watermark **for this run only**. The scheduled delta load
  is not disturbed, and **the replay never writes `ingest_state`** — Q17 before and after
  should show the same value.
* The start bound is inclusive, always.
* Omit `replay_cursor_end` for "from there to now".
* Q22 afterwards: rows tagged with that `rerun_id`, and whether the interval is now whole.

### 8.6 Switch a table between delta and full — and back

The lever for "the delta load has been missing rows and we need a clean sweep".

1. **Check first** whether the source waives its merge keys — Q23. If it does, a full load
   **duplicates every row it re-reads**, because that source appends.
2. Q24 — set `oracle_incremental_mode = 'full'`. Takes effect on the next run.
3. Run it. It reads the whole table; expect it to take much longer than a delta run, and
   raise the task timeout first if the table is large.
4. Q24 again — set the column back to NULL (inherit `cursor` from the source file). The
   watermark was never touched by the full run, so the next delta run resumes from the last
   genuine delta boundary.

### 8.7 A source table was altered

* **A new column**: nothing to do. Additive changes are allowed and land automatically.
* **A type change, or a removed column**: the run stops **before writing**, naming the
  column and both types. This is not a control-table fix — decide deliberately with the
  owning team: ALTER the landing table, pin the old type with `column_types` (a PR), or
  accept the change and recreate the table.
* **Dropped or renamed table**: the read fails loudly. The source file is wrong, or the
  table is gone; both are a PR.

## 9. File incident playbooks

**This source shares Kafka's checkpoint-reset guard and its restart procedure** — the
column names and table names below differ, the steps and the reasoning do not. Where a step
is identical to §5.4a it says so rather than repeating five paragraphs.

### 9.1 A file run failed once — transient

Symptom: one failed run (a network blip against the storage account, a transient read
error), the next run is fine.

1. Q1/Q2 (generic, work for every source type) — confirm which layer failed and that it was
   not a repeat of the same failure.
2. Re-run the task. Structured Streaming re-executes the failed batch over the identical
   file set — the checkpoint records which files it started with BEFORE `foreachBatch` runs,
   and commits only after it returns cleanly — so a retry is not a second read of files
   already landed, and Delta's `txnAppId`/`txnVersion` markers suppress any duplicate.

### 9.2 A batch keeps failing on the same rescued rows — poison batch

Symptom: the same run fails repeatedly, and the error names rows that did not fit the
configured schema.

1. Confirm `file_failure_mode` is `FAILFAST` (the platform default) — that is what turns a
   non-empty rescued count into a refusal rather than a landed-with-a-warning batch.
2. To unblock immediately, no deploy:
   ```sql
   UPDATE {ops_catalog}.{control_schema}.ingest_control
   SET file_failure_mode = 'QUARANTINE',
       notes = 'INC12345 - unblocking to investigate rescued rows, see _rescued_data',
       updated_by = current_user(), updated_at = current_timestamp()
   WHERE source_key = 'file_claims_inbound';
   ```
3. Re-run. The batch lands, with the mismatched rows' `_rescued_data` column populated —
   query the landing table directly:
   ```sql
   SELECT *, _rescued_data FROM {catalog}.files_claims.claims_inbound
   WHERE _rescued_data IS NOT NULL
   ORDER BY ingest_ts DESC LIMIT 20;
   ```
4. Fix the root cause (a `schema:` update, a `format_options` correction) in a PR, THEN set
   `file_failure_mode` back to NULL (inherit `FAILFAST`) — leaving it on `QUARANTINE`
   indefinitely means the next genuinely bad batch lands silently instead of paging anyone.

### 9.3 Someone deleted (or corrupted) the checkpoint

**Identical to §5.4** with `file_checkpoint_reset_id` / `file_ingest::<source_key>` in place
of `kafka_checkpoint_reset_id` / the Kafka app id. The framework refuses to start with a
message beginning `REFUSING TO RUN`. Do not delete landing rows to silence it.

### 9.4 Restarting after a genuine checkpoint loss

Follow **§5.4a's five steps exactly**, substituting:

| §5.4a step | This source's equivalent |
|---|---|
| Record where the stream got to (Q13, Kafka-specific) | There is no per-partition offset query for Auto Loader's own position format. Instead, read the last COMPLETED run's `position_end` from the audit table — it carries Auto Loader's own offset JSON verbatim (`sources/file/run.py` `_record_positions`): `SELECT position_end FROM {ops_catalog}.{audit_schema}.ingest_audit WHERE source_key = 'file_claims_inbound' AND layer = 'landing' AND status = 'COMPLETED' ORDER BY event_ts DESC LIMIT 1;` |
| Check the reset id has never been used (Q6d) | Same query shape, generic across source types: `SELECT run_id, rerun_id, min(event_ts), max(event_ts) FROM {ops_catalog}.{audit_schema}.ingest_audit WHERE source_key = 'file_claims_inbound' AND run_type = 'primary' AND rerun_id IS NOT NULL GROUP BY run_id, rerun_id ORDER BY 4 DESC;` |
| Set a fresh reset id | `UPDATE {ops_catalog}.{control_schema}.ingest_control SET file_checkpoint_reset_id = 'INC12345', notes = '...', updated_by = current_user(), updated_at = current_timestamp() WHERE source_key = 'file_claims_inbound';` |
| Backfill the gap | **There is no file replay job, and none is planned** (`docs/build_log/DECISIONS.md` D-10, settled — unlike Kafka's, which has one). If the missing files are still sitting under `source_path` unprocessed, section 9.7 below is the recovery: it is this same checkpoint-loss procedure, applied deliberately. If the files were already consumed and are gone from the landing zone, the only path back is re-presenting them under new names, which is outside this framework's scope. |
| Clear nothing | Same: `file_checkpoint_reset_id` is not a toggle. Leaving it set is correct and permanent. |

### 9.5 Emergency stop

Same as every source type: `enabled = FALSE` in the control table for this `source_key`.
The job runs, reads nothing, writes a `SKIPPED` audit row so silence is never ambiguous —
Auto Loader's own checkpoint is untouched, so re-enabling resumes from where it left off.

### 9.6 A landing zone owner reports files "disappearing"

This framework never moves, renames or deletes a source file (`docs/DESIGN.md` §11,
"Deliberately not built"). If files are vanishing from the landing zone, that is happening
outside this job — check the storage account's own lifecycle policies and any other process
with write access to the container before assuming this job is responsible.

### 9.7 Forcing a full or bounded re-read (there is no replay job for this source)

`docs/build_log/DECISIONS.md` D-10, settled: no `file_replay` job or entrypoint exists, or is
planned. A fresh (missing) checkpoint already makes Auto Loader re-read everything under
`source_path` on its own (`cloudFiles.includeExistingFiles` defaults to `true`), so recovery
reuses §9.4's checkpoint-loss procedure **deliberately**, rather than as an accident recovery.

**Do not set `replay_rerun_id` expecting it to do this on its own.** This source has no
replay `run_type` to fork a separate checkpoint namespace the way Kafka's replay does — the
generic `rerun_id` column only labels the audit row for this source, and does not touch the
checkpoint or the read path. `file_checkpoint_reset_id`, used together with the checkpoint
being genuinely absent, is the lever that matters.

**Run these three steps in order. Step 1 is not optional:**

1. **Delete the affected landing partition(s) first.** Landing is append-only — a re-read
   without this step appends everything again and silently duplicates the data. Match the
   partition(s) to `landing_partition_by` (normally `ingest_date`); if you are not certain
   which partitions the re-read will touch, narrow with step 2's `source_path` / `path_glob`
   override first and use that to scope the `DELETE`.
2. **Set a fresh, previously unused `file_checkpoint_reset_id`** — check it has never been
   used for this `source_key` with the same query shape as Q6d (`sql/03_support_queries.sql`),
   substituting this source's table names. Reusing an id is refused by the same guard that
   protects Kafka's reset, for the same reason: every write would be skipped as a duplicate.
3. **Run the normal file job.**

**To bound the re-read to fewer files** — a targeted fix rather than the whole path —
temporarily narrow `source_path` or `path_glob` for that one run, in a PR or a one-off job
parameter override. **There is no offset or timestamp window for this source**, unlike
Kafka's or Oracle's replay bounds: narrowing the path is the only bounding mechanism there
is, and it only narrows *which files are listed*, not which rows within them.

**Checklist**

| | Step | Where |
|---|---|---|
| [ ] | Deleted the affected landing partition(s) | `DELETE ... WHERE ingest_date = ...` |
| [ ] | Confirmed the reset id has never been used for this source | the audit table, Q6d's shape |
| [ ] | Set a fresh `file_checkpoint_reset_id` | the control table |
| [ ] | Narrowed `source_path` / `path_glob` if this is a bounded re-read, not a full one | source YAML or job parameters |
| [ ] | Ran the normal file job and confirmed it logged `checkpoint_reset_engaged` | driver log, or Q1 |
| [ ] | Verified row counts against what was expected, and no duplicates | landing table |
