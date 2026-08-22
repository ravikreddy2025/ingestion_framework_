# Support Runbook

For the production support team. **You do not need to read Python to use this document.**

> Lost in the file tree? [NAVIGATION.md](NAVIGATION.md) maps every file. The two you will
> actually use are `sql/03_support_queries.sql` (triage) and this page.

Everything here is a SQL statement or a Workflows job parameter. If a fix requires editing
code, it is an escalation - see section 6.

**Golden rules**

1. **Read the audit table first.** Before touching a checkpoint, before starting a replay.
2. **Never delete a primary checkpoint.** It causes silent data loss. Section 5.4 explains why;
   if it happens anyway, 5.4a is the only sanctioned way back.
3. **A failed batch is usually self-healing.** Re-run before doing anything clever.

---

## 1. Daily health check

Run Q1 in [`sql/03_support_queries.sql`](../sql/03_support_queries.sql). One row per topic
per layer for the last 24h.

| What you see | Meaning | Action |
|---|---|---|
| Every topic has `landing` and `curated` rows, `failures = 0` | Healthy | None |
| `skipped > 0` | Topic is disabled in the control table | Check `change_reason` - was it left off by mistake? |
| A topic missing entirely | The job did not run for it | Check the Workflows run history |
| `quarantined > 0` and rising | Records failing to parse | Section 5.2 |
| `failures > 0` | Go to section 2 | |

Also worth a weekly glance: **Q9 (duplicate check)** should always return zero rows. If it
does not, stop and escalate - see section 6.

---

## 2. A topic failed - which layer?

Run **Q2**. It pivots the per-layer audit rows into one row per batch:

| `landing_status` | `curated_status` | What happened | Go to |
|---|---|---|---|
| `COMPLETED` | *(null)* or `FAILED` | Died between the two layers - the common case | 5.1 |
| `STARTED` | *(null)* | Landing itself failed (permissions, storage, Kafka auth) | 5.3 |
| `COMPLETED` | `COMPLETED` | This batch is fine - look at a different batch | - |
| *(nothing at all)* | | The job never started | Check Workflows / cluster |

Then run **Q3** to find out whether the *same* `batch_id` has failed across *multiple runs*.
That distinction decides everything:

- **Failed once** -> transient. Re-run. Section 5.1
- **Failing every run** -> poison batch, the stream is stuck. Section 5.2

---

## 3. Onboarding a new topic

Onboarding is a **joint** task. The development team raises a PR for the topic's
configuration; you own the control-table row and the verification.

### Your part

**Before the PR is deployed**, confirm with the requesting team:

- [ ] Kafka topic name and the Schema Registry **subject** (usually `<topic>-value`)
- [ ] Which cluster and which registry - they may be new
- [ ] Secret scope exists **in each environment** and the job's service principal has READ
- [ ] For mTLS topics: certificates are on a UC Volume
- [ ] Expected daily volume (drives `max_offsets_per_trigger`)
- [ ] Is history wanted? (`starting_offsets: earliest` vs `latest` - **first run only**)

**After the PR is deployed:**

1. Insert the control row (optional - the topic runs on YAML defaults without one, but an
   explicit row gives you somewhere obvious to look):

```sql
INSERT INTO ops_prod.ingestion.ingestion_topic_control
  (topic_key, enabled, change_reason, updated_by, updated_at)
VALUES ('gma_provider_updates', true, 'onboarding GMA-4412',
        current_user(), current_timestamp());
```

2. Trigger the primary job for that topic once, manually, **in dev first**.
3. Verify with Q1 - you want `landing COMPLETED` **and** `curated COMPLETED`.
4. Verify with Q9 - zero duplicates.
5. Promote to preprod, then prod. The same topic file is used in all three; only the
   environment differs.
6. Hand back to the requesting team to validate the curated data.

### If the first run fails

| Error text contains | Cause | Fix |
|---|---|---|
| `must be a Unity Catalog Volume path` | A cert path is on DBFS | Dev team, config PR |
| `could not read secret ... from scope` | Scope missing, or SP lacks READ | Platform/infra |
| `unknown environment` | Bundle target has no matching `conf/environments/<env>.yaml` | Dev team, config PR |
| `uses {catalog}, which is not defined` | Environment file is missing a `vars:` entry | Dev team, config PR |
| `no entry for /schemas/ids/N (HTTP 404)` | Records produced against a **different** registry | Dev team, config PR |
| `unreachable ... NCC private endpoint` | Network path missing | Platform/infra |
| Job hangs then times out | `bootstrap_servers` wrong, or firewall | Platform/infra |

---

## 4. Decommissioning a stale topic

**Order matters.** Doing this out of order either breaks the nightly run or leaves orphaned
state. Do not skip step 1 or step 2.

### Step 1 - Confirm it is genuinely stale

```sql
-- Last activity for this topic
SELECT layer, status, max(event_ts) AS last_seen, sum(record_count) AS records
FROM platform_prod.audit.stream_audit
WHERE topic_key = 'legacy_feed_v1' AND audit_date >= current_date() - INTERVAL 90 DAYS
GROUP BY layer, status ORDER BY last_seen DESC;
```

- [ ] Domain owner has confirmed in writing
- [ ] **Downstream consumers of the curated table identified and signed off** - check
      lineage in Unity Catalog before assuming nothing reads it
- [ ] Data retention / compliance position agreed (see step 4)

### Step 2 - Stop ingestion (immediate, reversible, no deploy)

```sql
UPDATE ops_prod.ingestion.ingestion_topic_control
SET enabled = false,
    change_reason = 'DECOM-123 - decommission agreed with GMA, ticket link',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'legacy_feed_v1';
```

Wait one scheduled cycle. Confirm a `SKIPPED` audit row appears - that proves the job ran
and deliberately consumed nothing. **This is your rollback point:** flip `enabled` back to
`true` and everything resumes.

### Step 3 - Remove the configuration (development team, one PR)

Raise a ticket for the dev team to remove, **in the same PR**:

- the task from `resources/job_ingest_primary.yml`
- `conf/topics/legacy_feed_v1.yaml`

**Order matters:** if the YAML is removed while the job task still exists, the nightly run
fails with "structural config file not found". Disable (step 2) -> remove both -> deploy.

### Step 4 - Data disposition

Decide per your retention policy. All three are optional and independent. Repeat per
environment.

```sql
-- Landing is one table per topic, so decommissioning is a DROP, not a filtered DELETE.
-- The table name is the Kafka topic with dots/hyphens replaced by underscores.
DROP TABLE IF EXISTS platform_prod.landing.legacy_feed_v1;

-- Curated table is this topic's alone.
DROP TABLE IF EXISTS platform_prod.curated.legacy_feed_v1;

-- Quarantine, if the topic used it.
DROP TABLE IF EXISTS platform_prod.landing.legacy_feed_v1_quarantine;
```

**Keep the audit rows.** They are the historical record of the feed and they age out with
the audit table's own retention.

### Step 5 - Clean up state

Only **after** the PR from step 3 is deployed, or the job will recreate it:

```python
# In a notebook. The path is <checkpoint_root>/<topic_key>/
dbutils.fs.rm("/Volumes/platform_prod/ingestion/checkpoints/legacy_feed_v1", recurse=True)
```

```sql
DELETE FROM ops_prod.ingestion.ingestion_topic_control WHERE topic_key = 'legacy_feed_v1';
```

Ask platform/infra to revoke the Kafka consumer credential if it was topic-specific.

### If the topic is ever re-onboarded under the same `topic_key`

The framework will **refuse to start** if landing still holds rows for that topic but the
checkpoint is gone. That guard is protecting you (see section 5.4). If step 4 did not `DROP
TABLE` the landing table (retention policy said keep it), you have two options: `DROP TABLE`
it now, or keep the history and set `checkpoint_reset_id` per 5.4a instead - either way the
re-onboarded stream needs a transaction identity with no prior commits, and those are the two
ways to get one. Onboarding under a new `topic_key` also works, and needs neither.

### Decommission checklist

- [ ] Domain owner signed off
- [ ] Downstream consumers signed off
- [ ] `enabled = false`, `SKIPPED` row confirmed
- [ ] PR merged and deployed (job task + YAML removed)
- [ ] Data disposition actioned per policy, in every environment
- [ ] Checkpoint deleted
- [ ] Control row deleted
- [ ] Kafka credential revoked

---

## 5. Incident playbooks

### 5.1 A batch failed once - transient

**Symptom:** Q2 shows `landing COMPLETED`, no `curated COMPLETED`. Q3 shows the batch has
failed only in one run.

**What is actually happening:** the retry replays the **same** batch over the **same** Kafka
offsets. Delta skips the landing write it already committed and writes curated. No
duplicates are possible.

**Action:** Re-run. Workflows -> the failed run -> **Repair run**. Or let the next schedule
pick it up. Then confirm with Q2.

### 5.2 The same batch fails every run - poison batch

**Symptom:** Q3 shows one `batch_id` failing across several `run_id`s. The topic is stuck
and the backlog is growing.

**Cause is almost always** a schema the registry does not have.

**Fix A (preferred) - get the schema registered.** Raise with the producing team. Once
registered, the next run succeeds on its own. Nothing else to do.

**Fix B - unblock now, recover later.** No deploy needed:

```sql
UPDATE ops_prod.ingestion.ingestion_topic_control
SET on_deser_error = 'quarantine',
    change_reason  = 'INC12345 - unblock stuck stream, schema 5513 unregistered',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';
```

Next run: the unparseable records go to the quarantine table **with their raw bytes kept**,
the batch completes, the stream drains.

**Then, once the schema is registered:**

1. Find what was quarantined - Q4.
2. Recover it with a **curated replay** (section 5.6), filtered on that `writer_schema_id`.
3. Set `on_deser_error` back to `'fail'`.

**Do not leave a topic on `quarantine` permanently.** It converts loud failures into a table
nobody looks at.

### 5.3 Landing itself failed

**Symptom:** Q2 shows `landing STARTED` with no `COMPLETED`.

Nothing was written anywhere, so the retry is clean. Read `error_class` / `error_message`:

| Error | Owner |
|---|---|
| Permission / UNAUTHORIZED on the table or Volume | Platform/infra - SP grants |
| Kafka auth / SSL handshake | Platform/infra - credential or cert expiry |
| Storage or cluster failure | Re-run |

### 5.4 Someone deleted a primary checkpoint

**This is the one failure that looks like success.** Batch ids restart at 0; Delta has
already recorded higher versions and **skips every write as a duplicate**. The job reports
success and ingests nothing.

The framework refuses to start in this state with a message beginning `REFUSING TO RUN`.

**Do not work around it.** Do not delete landing rows to silence it. Recover with a **Kafka
replay** (section 5.5) using a new `rerun_id` - it runs under its own checkpoint *and* its
own Delta transaction identity, so it cannot collide.

**Prevention:** there is never a reason for support to delete a checkpoint. To reprocess
data, use a replay job.

**Important:** a replay backfills the missing *data*. It does **not** unblock the *primary
job* - the primary will keep refusing to start, on every scheduled trigger, until you
complete 5.4a below. Do not treat "the replay finished" as "the incident is closed."

### 5.4a Restarting the primary after a genuine checkpoint loss

Use this only once you have confirmed the checkpoint is truly gone (not a transient Volume
access error - see the `NOT the same as the checkpoint being missing` message, which is a
different, unrelated failure and means the Volume, not the checkpoint, needs attention) and
you accept starting that topic's primary stream over from batch 0.

**Do not try to work around this any other way.** In particular, do not delete or truncate
landing rows to make the guard's row-count check pass - that satisfies the code but not the
underlying problem: Delta remembers the primary's last committed batch id independently of
which rows currently exist, indefinitely by default, so a plain restart under the same
identity would silently skip every write below the old watermark. The framework does not
special-case that combination for you.

```sql
UPDATE {ops_catalog}.ingestion.ingestion_topic_control
SET checkpoint_reset_id = 'INC12345',
    change_reason  = 'INC12345 - primary checkpoint deleted, restarting under a fresh identity',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';
```

Next scheduled trigger, the job logs `Checkpoint-reset override engaged` and starts a fresh
stream: new checkpoint, and a new Delta transaction identity that has no committed history to
collide with. This is what makes the restart safe, not merely permitted.

**Never blank `checkpoint_reset_id` back out afterward.** It is not a toggle - once a topic
has used it, that value IS the topic's transaction identity going forward. Clearing it
reverts to the original identity, which still carries the pre-incident watermark, and would
silently reproduce the exact bug this override exists to avoid. Leaving it set is correct and
permanent; `change_reason` and `updated_at` already give you the audit trail of when and why.

**Still run the replay (5.5)** for whatever window was missed between the checkpoint loss and
this restart - the two are independent: this step recovers the *stream*, the replay recovers
the *data*.

### 5.5 Kafka replay - data is missing at source

Use when data never arrived: a consumer outage, a gap, or after section 5.4.

**Find the restart point.** From Q2, take the `ending_offsets` of the last **successful**
batch:

```
{"rcm.claim.status.v2":{"0":45231,"1":44870,"2":45009}}
```

**Workflows -> `[prod] Kafka Ingest - REPLAY from Kafka` -> Run now with different parameters:**

| Parameter | Value |
|---|---|
| `topic_key` | `rcm_claim_status` |
| `environment` | defaults to the deployed target - leave it |
| `rerun_id` | `INC12345` - your incident number. **Required.** |
| `starting_offsets` | the JSON above |
| `starting_timestamp` | *leave empty* - set **one** of offsets or timestamp, never both |
| `ending_offsets` / `ending_timestamp` | optional; set one to cap the replay window |

To replay by time instead, leave `starting_offsets` empty and set `starting_timestamp` to
`2026-08-11T09:00:00Z`. If any partition has no record at or after that time the run
**fails** rather than silently skipping - that is deliberate.

The replay runs under its own checkpoint and never touches the primary one. **The daily
schedule keeps running normally throughout.**

#### After the incident: clean up the replay checkpoint

Each Kafka replay creates its own directory under
`<checkpoint_root>/<topic_key>/replay/<rerun_id>`. Nothing removes them, so they accumulate
one per incident, forever.

Once the incident is closed **and** you have confirmed the data landed (Q8 in
`sql/03_support_queries.sql`), delete that one directory:

```python
dbutils.fs.rm("/Volumes/<catalog>/ingestion/checkpoints/<topic_key>/replay/<rerun_id>", recurse=True)
```

Delete **only** the `replay/<rerun_id>` directory you created. Never the `primary` directory
beside it, and never the `<topic_key>` directory above it - see section 5.4 for what happens
then. If you might rerun the same `rerun_id`, leave it: re-running with the same id resumes
that replay rather than starting it again.

### 5.6 Curated replay - the bytes are fine, the parse was not

Use when records were quarantined, a schema was registered late, or curated data is wrong.
**Does not contact Kafka**, so it works long after Kafka retention has expired.

**Workflows -> `[prod] Kafka Ingest - REPLAY Curated from Landing`:**

| Parameter | Value |
|---|---|
| `topic_key` | `rcm_claim_status` |
| `rerun_id` | `SCHEMAFIX-88` |
| `landing_filter` | `writer_schema_id = 5513` |

`landing_filter` is any SQL condition over the landing table - `writer_schema_id = N` is
usually the precise one; `ingest_date BETWEEN '2026-08-01' AND '2026-08-03'` for a window.
It is **required** on purpose: re-parsing all history by accident is expensive.

The topic condition is added automatically, so you cannot accidentally rewrite another
topic's curated table.

### 5.7 After any replay - verify

- **Q8** - replay rows vs primary rows, and offsets present in landing but missing from
  curated. Every replayed row is tagged `ingested_via` and `replay_run_id`.
- **Q9** - duplicates. Must be zero.
- Clear any replay controls you parked in the control table (Q7).

### 5.8 Emergency stop

```sql
UPDATE ops_prod.ingestion.ingestion_topic_control
SET enabled = false,
    change_reason = 'INC12345 - producer emitting corrupt payloads',
    updated_by = current_user(), updated_at = current_timestamp()
WHERE topic_key = 'rcm_claim_status';
```

Takes effect on the next run. The job still runs and writes a `SKIPPED` audit row, so
silence is never ambiguous. **Disabling stops replays too** - re-enable before replaying.

**More than one topic affected** (a shared cluster or domain incident) - **Q6b** in
`sql/03_support_queries.sql` toggles a list, or every topic in a `domain`, in one
statement, so an incident touching several of the 50+ shipped topics does not mean typing
each `topic_key` by hand. **Q6c** lists every currently-disabled topic and how long it has
been off - run it after any incident involving Q6b, so nothing gets left off by mistake.

---

## 6. Escalate to the development team when

- Q9 returns duplicate rows - the idempotency mechanism is not behaving as designed
- The `REFUSING TO RUN` guard fires and `checkpoint_reset_id` (section 5.4a) does not
  resolve it - a Kafka replay alone never resolves this guard, by design (5.4)
- The same error recurs after both a re-run and a `quarantine` flip
- An error mentions `from_avro`, `reader/writer schema`, or a self-check failure
- An error mentions `unknown environment` or an unresolved `{placeholder}`
- A fix would require editing YAML, Python, or a job definition
- Anything you would need to delete a checkpoint or drop **the shared audit table** to
  fix (dropping a topic's own landing/curated/quarantine table during decommission,
  section 4, is routine and does not need escalation)

**Include in the escalation:** `topic_key`, environment, the failing `batch_id`, `run_id`,
the Q2 output for that batch, and `error_class` / `error_message`.

---

## 7. What support can change - and what it cannot

| You own - no deploy | Needs a PR from the dev team |
|---|---|
| `enabled` (emergency stop) | Table names, catalogs, schemas |
| `on_deser_error` (unblock a stuck stream) | Partition columns |
| `max_offsets_per_trigger` (shrink batches) | Cluster / registry / subject |
| `trigger` | Dedup keys |
| `reader_schema_mode` / `reader_schema_id` | Checkpoint root |
| `fail_on_data_loss` *(needs domain sign-off)* | Broker endpoints and secret scopes |
| All replay parameters | Adding or removing a topic's config file |
| `checkpoint_reset_id` *(incident use only, section 5.4a)* | Anything in Python |

The split is deliberate: things with production blast radius get code review, things needed
at 3am do not.
