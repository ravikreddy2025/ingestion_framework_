# STAGE 5 -- File source (ADLS via Auto Loader)

**Paste `00_CORE.md` before this file.** Stage 4 must be green.

---

## Decision, already made

**Use Auto Loader (`cloudFiles`) with `availableNow`.** The alternative -- a processed-files
ledger in `ingest_state` -- is Auto Loader reimplemented with worse listing performance and a
new correctness surface.

Consequence to accept knowingly: **the file source is checkpoint-based**, so
`checkpoint_reset_id` and the reset guard apply to it exactly as they do to Kafka. Wire that
in; do not write a second guard.

---

## Configuration

```yaml
# conf/sources/file_claims_inbound.yaml
source_type: file
storage_ref: adls_landing_prod       # profile in conf/storage.yaml -- the path BELOW is
                                      # relative to that profile's container, never a full
                                      # abfss:// URL (docs/build_log/DECISIONS.md D-14 item 3)
source_path: "claims/inbound/"
path_glob: "*.csv"
file_format: csv                     # csv | json | parquet | avro
format_options:
  header: "true"
  delimiter: "|"
  encoding: "UTF-8"
schema_mode: provided                # provided | hints | infer
schema: "..."                        # required when schema_mode is 'provided'
target_schema: files_claims
target_table: claims_inbound
landing_partition_by: [ingest_date]  # not `partition_by:` (D-14 item 5)
filename_columns:
  business_date: "claims_(\\d{8})\\.csv"
max_files_per_trigger: 1000
```

Validation rules:

- `schema` is required when `schema_mode: provided`, and rejected otherwise.
- `format_options` keys are validated against a known set per `file_format` -- an unknown
  option is an error, not a silent pass-through. Spark ignores unknown reader options, so
  without this a typo'd `delimeter` produces a table full of one-column rows.
- Every `filename_columns` regex must compile at config load, and must contain exactly one
  capture group.
- `source_path` must match the `storage_ref` profile's container. A path pointing somewhere the
  register does not cover is an error.

---

## Work

### `sources/file/spec.py`

`SOURCE_SPEC` with `layers = ("landing",)`. No PySpark import.

### `sources/file/run.py`

- Auto Loader read with `cloudFiles.format`, `cloudFiles.schemaLocation`,
  `cloudFiles.maxFilesPerTrigger`, `pathGlobFilter`, and the format options.
- **`rescuedDataColumn` is this source's quarantine.** Enable it, land it, and surface its
  non-null count in the audit row the way Kafka's quarantine count is surfaced. A rising
  rescued-data count is the file equivalent of a rising quarantine trend, and it should be
  visible in the same daily health check.
- **Capture `_metadata`** -- file path, name, size, modification time -- as framework columns
  on every row. `position_start` / `position_end` in the audit row carry the file boundary.
- Derive `filename_columns` from the file path using the configured regexes.
- `availableNow` trigger, `foreachBatch` writing landing via `ctx.writers`.

### `conf/storage.yaml`

A register of ADLS accounts and containers -- account name, container, auth mode, secret scope
and KEY names. **Same pattern as `conf/clusters.yaml`.** A `storage_ref` naming a profile that
does not exist in the register is an error.

### Defaults

`conf/defaults/file.yaml`:

- `schema_mode: provided` is the default. `infer` is convenient in dev and a source of silent
  type drift in prod -- say that in a comment in the file, not only in the docs.
- `max_files_per_trigger` default.
- Checkpoint and schema-location roots, following the same pattern as Kafka's checkpoint root.

### Auto Loader listing mode

Directory listing vs file notification is **VB-07**. Default to listing, make the mode a config
key, and add the VB entry -- notification mode needs cloud resources that may not exist in this
tenancy.

### `schemaLocation` and the reset guard

`cloudFiles.schemaLocation` is a checkpoint-like resource with its own lifecycle. Decide
whether the reset guard should cover it as well as the stream checkpoint, state your decision
in `docs/DESIGN.md`, and add a VB entry if the behaviour is uncertain.

### Operationalise

- One job template under `resources/`, `max_concurrent_runs: 1`, `queue.enabled: false`.
- Inert onboarding template `conf/sources/_TEMPLATE_file.yaml`.
- `docs/CONFIGURATION.md` rows for every file key.
- File failure-scenario table in `docs/DESIGN.md`: malformed file mid-batch; a file rewritten
  in place; a file arriving late; schema drift between files; the schema location deleted.
- File incident playbook in `docs/RUNBOOK_SUPPORT.md`.

---

## Do not build

- **File archiving, moving or deletion.** Moving or deleting source files is a
  data-loss-shaped operation owned by whoever owns the landing zone. Note it as an open item
  in `docs/DESIGN.md`.
- A processed-files ledger.
- A second checkpoint-reset guard.
- A format abstraction layer -- `file_format` is a config value passed to Auto Loader, not a
  strategy pattern.
- Schema inference caching, or a schema registry for files.

---

## Files

**Create:** `sources/file/spec.py`, `sources/file/run.py`,
`conf/sources/_TEMPLATE_file.yaml`, `resources/job_ingest_file.yml`
**Edit:** `conf/storage.yaml`, `conf/defaults/file.yaml`, `framework/security.py`,
`docs/CONFIGURATION.md`, `docs/DESIGN.md`, `docs/RUNBOOK_SUPPORT.md`

---

## Exit gate

- `pytest -m "not spark" -q` green, test count up.
- A file source resolves in every environment, with different containers and secret scopes per
  environment and the same source file.
- An unknown `format_options` key is rejected.
- A `filename_columns` regex that does not compile, or has zero or two capture groups, is
  rejected.
- A `storage_ref` naming a non-existent profile is rejected, listing the valid names.
- The reset guard covers the file source's checkpoint -- test it the same way Kafka's is
  tested.
- No credential in any logged or audited options map.
- The CORE section 7 grep returns nothing.

Then write the stage report (CORE section 9) and **stop**.
