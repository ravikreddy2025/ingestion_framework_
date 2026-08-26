# Design — Oracle

This source's own watermark design, failure-scenario table and decisions. Read
[DESIGN.md](DESIGN.md) first for the shared architecture, the source-contract rationale and
the configuration model — this file assumes you already have those.

---

## Oracle — the watermark, and what can go wrong

### The order, and why it is the whole design

    capture the high water -> read the closed interval -> write landing -> THEN advance

A crash anywhere before the advance leaves the stored watermark where it was, so the next
run re-extracts the same interval — which the merge key absorbs. Advancing first would mean
a crash silently skipped a window, and nothing downstream could detect that.

`ingest_state` holds the watermark, not the audit table. Audit writes are best-effort by
design and must never raise; extraction correctness must not depend on best-effort writes.

### The closed interval

`cursor > last_watermark AND cursor <= high_water`, with the upper bound captured at run
start from `MAX(cursor)` over the rows this extract can see — never from a clock, because a
clock reading is ahead of every committed row by definition and would move the watermark
past rows still in flight.

**The gap this does not close:** a transaction already open when the high water was
captured, carrying a cursor value below it, that commits after the extract has read past
that value, is never seen. That is inherent to a cursor over a wall-clock column, and how
much it matters is a property of the SOURCE APPLICATION — whether it stamps the cursor at
statement time or at commit. It is VB-25, it is stated at the top of `sources/oracle/run.py`,
and it is a question for the source team at onboarding rather than something this code can
detect.

### Failure scenarios

| # | Failure | Duplicates? | Rows lost? | Fix — no code change |
|---|---|---|---|---|
| 1 | Transient JDBC failure (network, session killed) | No | No | Re-run. The watermark never moved, so the same interval is re-read. |
| 2 | Write fails after a successful read | No | No | Re-run. Same interval; nothing was committed and nothing advanced. |
| 3 | Crash between the write and the watermark advance | **Only if `merge_keys` waived** | No | Re-run. With merge keys the re-read de-duplicates; a waived source appends the interval twice — Q19 finds the duplicates. |
| 4 | Watermark manually corrupted (edited too far forward) | No | **Yes — silently** | Q18: set it back to a known-good value from the audit table's `position_end`, then re-run. A watermark set BACKWARDS is safe with merge keys and duplicates without them. |
| 5 | Cursor values arrive out of order (late commit, low cursor) | No | **Yes — silently** | Not fixable by re-running: the interval has been read. VB-25. Remedies in order: a safety lag on the high water, `incremental_mode: full`, or a change-tracking mechanism. |
| 6 | Source table altered — new column | No | No | Nothing. Additive changes are allowed and Delta widens the target. |
| 6b | Source table altered — type changed, or a column removed | No | No | **The run stops before writing**, naming the column and both types. Decide deliberately: ALTER the landing table, pin the old type with `column_types`, or recreate. |
| 7 | Source table dropped or renamed | No | No | The read fails loudly. Fix the source file (or the source), then re-run. |
| 8 | A full load run against a `merge_keys: []` source | **Yes** | No | Expected: that source appends. Delete the duplicate `ingest_date` partition, or set merge keys. |

### Idempotency

`txnAppId = ingest::oracle::<source_key>`, `txnVersion = run_sequence` from `ingest_state`.
One identity per source, with **no fork for a replay** — unlike Kafka's, which forks on the
rerun id. The difference is what supplies the version: Kafka's is a microbatch id that
restarts at 0 in a replay's own checkpoint, while `run_sequence` is allocated on every run
of every type and therefore always increases.

The markers apply to APPENDS only; Delta does not honour them on a MERGE, so a merging
source's idempotency comes from its merge key instead.

### Why landing keeps every version

The merge key is `merge_keys + cursor_column`, so `(CLAIM_ID, LAST_UPDATE_DT)` identifies a
version of a claim. Two consequences, both deliberate:

* landing is a retained mirror — a replay or a historical reprocessing can see what the
  source held at a point in time, which merging on the business key alone would destroy;
* the key does not change when `oracle_incremental_mode` does. A full run keyed on the
  business key alone would match every historical version of a claim with one source row
  and overwrite all of them — a control-table UPDATE causing data loss.

A source with **no** cursor has no version identity, so its merge updates matched rows: the
key identifies the row, a match means it changed, and the mirror would go stale otherwise.
