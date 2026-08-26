# Design — Files

This source's own Auto Loader design, failure-scenario table and decisions. Read
[DESIGN.md](DESIGN.md) first for the shared architecture, the source-contract rationale and
the configuration model — this file assumes you already have those.

---

## Files — Auto Loader, the shared checkpoint-reset guard, and what is deliberately
not built

### Why Auto Loader, and why `availableNow` always

CORE section 10 decided this ahead of Stage 5: Auto Loader (`cloudFiles`) over a hand-rolled
processed-files ledger. A ledger is Auto Loader re-implemented with worse listing
performance and a new correctness surface — tracking which files have been seen is exactly
what `cloudFiles.schemaLocation` and the stream checkpoint already do. The consequence
accepted knowingly: **this source is checkpoint-based**, exactly like Kafka's primary
stream, with everything that implies about restart safety.

There is no per-source `trigger:` the way Kafka has one — this source is always a bounded
`availableNow` run, because that is the only shape a scheduled Workflows run has a natural
end under, and there is no case in this framework's scope for a genuinely continuous file
stream.

### The checkpoint-reset guard is shared, not duplicated

Kafka and Files call the same implementation, `framework/checkpoint.py::
guard_against_checkpoint_reset`, rather than each carrying its own copy. Both sources are
checkpoint-based; Oracle has no checkpoint at all, so its correctness rests on
`ingest_state`'s watermark instead — this remains two callers, not three, and the promotion
was justified by the cost of the two copies having already drifted once (see below), not
by clearing CORE section 7 rule 4's "three implementations" bar.

The shared function checks "does the landing table hold any row at all" — no `topic`-style
filter, because every source that calls it already has one landing table per source (Kafka's
included: `{catalog}.landing.{topic_table}` is one table per topic).

The shared refusal message does not claim a replay mechanism unconditionally: the file
source has no replay job (this section's own "deliberately not built" list, below), so the merged
message states only what is true for every caller — set this source's own control column to
an unused incident id. The reset-engaged log event also carries no per-source prefix; every
log line already carries `source_type` and `source_key` (`framework/logs.py`).

### `cloudFiles.schemaLocation` and the reset guard — the decision

`schemaLocation` is a second checkpoint-like resource, living beside the stream checkpoint
under the same Volume root, keyed by the same `source_key` (`sources/file/config.py`
`checkpoint_path` / `schema_location_path` are siblings). **Decision: the reset guard covers
the stream checkpoint directly and the schema location indirectly, through that shared
`source_key` scoping — it is not probed or reset separately.**

Reasoning: a `checkpoint_reset_id` forks the stream's Delta transaction identity so a
restarted stream has no committed versions to collide with. It does **not** delete or move
`schema_location_path` — nothing in this framework does, ever, matching the "do not build a
processed-files ledger, do not build file archiving" scope boundary below. A fresh stream
under a forked identity re-applies (`schema_mode: provided`) or re-infers
(`hints`/`infer`) against whatever is already at that path for this `source_key`, which is
unaffected by the reset. The failure mode this would NOT catch — a schema recorded before an
incident no longer matching reality — is the same as an ordinary schema-mode-`infer` risk
documented in `docs/CONFIGURATION.md`, not something specific to a reset.

### Failure scenarios

| # | Failure | Duplicates? | Rows lost? | Fix — no code change |
|---|---|---|---|---|
| 1 | A malformed file mid-batch (unparseable row) | No | No, if `failure_mode: QUARANTINE` | The batch lands with `_rescued_data` populated for that row; investigate and re-onboard the fix. Under `FAILFAST` (the default) the whole batch is refused and retries until fixed or the mode is flipped. |
| 2 | A file rewritten in place, under the same path, after Auto Loader has already processed it | No | **Yes — silently** | Auto Loader tracks files it has SEEN, not their content hash by default; a rewrite under the same name is not re-read. Producers must write under a NEW name (a convention, not something this code enforces) — document this in the onboarding checklist for a landing zone at risk of it. |
| 3 | A file arrives late (after the run that would ordinarily have picked it up) | No | No | The next scheduled run picks it up — `availableNow` drains whatever is present, whenever it runs. Nothing to do. |
| 4 | Schema drift between files (a new column, a changed type) | Depends on `schema_mode` | Depends | `provided`: a genuinely new column is dropped unless `rescuedDataColumn` catches it (VB-06); a type mismatch across files becomes rescued data or a cast failure depending on format. `infer`/`hints`: the inferred schema can change between runs with no review — this is exactly why `provided` is the platform default. |
| 5 | `cloudFiles.schemaLocation` deleted | Behaves like a genuine first run for schema purposes | No, if the stream checkpoint is intact | Auto Loader re-infers or re-applies the schema on the next microbatch; the STREAM checkpoint (a separate resource) still prevents re-reading already-processed files. If the stream checkpoint is ALSO gone, this is the ordinary checkpoint-reset scenario above. |

### Deliberately not built

- **File archiving, moving or deletion.** Moving or deleting source files after ingest is a
  data-loss-shaped operation that belongs to whoever owns the landing zone, not to this
  framework. **Open item for the incoming team:** if a landing zone accumulates files
  without bound, that is an operational concern for its owner to solve (a lifecycle policy
  on the storage account is the usual answer), not something this job does on their behalf.
- **A processed-files ledger.** Auto Loader's own checkpoint already is one.
- **A second checkpoint-reset guard.** See above.
- **A format abstraction layer.** `file_format` is a config value passed to Auto Loader
  directly; there is no strategy-pattern class per format.
- **Schema inference caching, or a schema registry for files.** `cloudFiles.schemaLocation`
  already is the former; there is no equivalent of Kafka's Schema Registry for file drops
  in this framework's scope.
- **SAS-token and managed-identity storage auth** (`docs/build_log/DECISIONS.md` D-12).
  Only `account_key` and `service_principal` are implemented, stated here as a limitation,
  not an oversight: each of the excluded modes needs either a token-provider class this
  project cannot verify exists on the target runtime, or workspace-level Unity Catalog
  wiring outside this repository's control, so adding one is a future code change with its
  own verification, not a config guess — the same restraint `sources/oracle/config.py`
  applies to JDBC auth (`auth_mode: basic` only). See `sources/file/security.py`'s module
  docstring for the mechanism these two modes do use.
- **A file replay job or entrypoint** (`docs/build_log/DECISIONS.md` D-10, settled). A
  fresh, missing checkpoint already makes Auto Loader re-read the whole path on its own
  (`cloudFiles.includeExistingFiles` defaults to `true`), files persist in ADLS so there is
  no retention window to race the way a Kafka replay races broker retention, and this
  source is landing-only, so there is no re-parse-from-landing shape either. Recovery
  reuses the existing checkpoint-reset procedure deliberately — `docs/CONFIGURATION.md` §11
  and `docs/RUNBOOK_SUPPORT.md` §9 carry the three-step version.

### `access_mode`, and the simplification it sets up (D-15, supersedes D-13's shape-inference)

`access_mode` is an **explicit**, required choice — `volume` or `adls` — not inferred from
`source_path`'s shape the way D-13 originally built it. `volume` mode names a Unity Catalog
Volume directly via `volume_path` (`/Volumes/<catalog>/<schema>/<volume>/...`) instead of a
path within an ADLS container named by `storage_ref`. A Volume path is governed by Unity
Catalog grants on the Volume itself: `sources/file/config.py` applies no `storage_ref` and
builds no `fs.azure.*` session options for it, and `sources/file/run.py` applies no session
configuration around the read at all in that case. The two modes are mutually exclusive by
construction — setting `storage_ref`/`source_path` under `access_mode: volume`, or
`volume_path` under `access_mode: adls`, is a config error naming the offending key and the
mode, checked in `sources/file/config.py::_access` at config load.

`volume` is **preferred**, not forced: the shipped `abfss://`-form worked example
(`file_claims_inbound.yaml`) keeps its existing `access_mode: adls`, since switching it was
not asked for and doing so without confirming Volumes are reachable from the target compute
for that workload would be exactly the kind of unverified assumption this project exists to
keep out of shipped configuration (VB-28). A second worked example,
`file_membership_eligibility.yaml`, ships with `access_mode: volume` so the cross-product
config test actually resolves a Volume-mode source in every environment.

**The planned simplification, if VB-28 comes back "Volumes everywhere":** `conf/storage.yaml`,
`sources/file/security.py`, and `framework/security.py`'s `apply_session_options` (added for
exactly this source's session-scoped credentials, VB-26) all become deletable —
a Volume path takes no credential from this framework at all. Not attempted now: narrowing to
one mode is a decision for whoever answers VB-28, not something to guess at while both are
still plausibly needed in different environments. If that day comes, `apply_session_options`
is worth a second look before deleting it outright — nothing else in the framework uses it
today, but a future source needing session-scoped, non-`.option()` credentials (the same
shape ADLS Gen2 has) would want it again.
