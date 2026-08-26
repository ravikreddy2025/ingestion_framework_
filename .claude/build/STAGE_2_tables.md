# STAGE 2 -- Shared tables: control, state, audit

**Paste `00_CORE.md` before this file.** Stage 1 must be green.

Nothing is deployed, so there is **no migration, no backfill and no compatibility view**.
Build the tables correctly the first time.

---

## Work

### 1. `framework/control.py`

Reads `{ops_catalog}.ingest_control` (CORE section 5.2) and returns the layer-4 override dict
for one `source_key`.

Three rules, each with a test:

- **Missing row is not an error** -- returns an empty override dict.
- **Duplicate rows are** an error, naming the `source_key`.
- **`source_overrides` JSON is validated against that source's `operational_keys`** from its
  `SOURCE_SPEC`, producing the same unknown-key error a YAML typo produces. This is the one
  thing most likely to be skipped, and skipping it puts a hole in the validation everything
  else is careful about.

Structural fields must be ignored if present, not rejected -- matching how the YAML layers
already behave.

### 2. `framework/state.py`

`{ops_catalog}.ingest_state` (CORE section 5.3). Two operations only:

```python
def read_state(source_key: str, state_key: str) -> str | None: ...
def write_state(source_key: str, state_key: str, value: str, value_type: str, run_id: str) -> None: ...
```

Plus `next_run_sequence(source_key) -> int`, which reads, increments and writes atomically
enough for a single-run-per-source-key world (CORE says one run per `source_key`; state the
assumption in the docstring).

**State writes are mandatory and must raise on failure.** This is the difference from audit,
and the reason this is a separate table. Put that sentence in the module docstring -- someone
will otherwise "make it consistent" with audit and silently break Oracle.

### 3. `framework/audit.py`

One shared table, one row per (run, layer, status). Extend the existing schema with
`source_type`, `source_key`, `source_ref`, `position_start`, `position_end`, `source_detail`;
rename `topic` and the offset columns per CORE section 5.1.

- **Audit writes must never raise.** Wrap them; log loudly instead. Test that a failing
  underlying write does not propagate.
- Keep the three-way agreement: the audit row, its `StructType`, and its DDL must all agree,
  and the existing test must still pass with the new columns.
- Document on the `position_*` columns that they carry three different meanings depending on
  `source_type`. A support engineer will read that column before they read any doc.

### 4. `framework/tables.py`

Target-name resolution per layer and per source type, driven by patterns in
`conf/defaults/<source_type>.yaml` -- never hardcoded. DDL and schema creation with grants.

Rules to enforce:

- Non-3-tier table name is an error.
- A name legal in the source system but illegal in Unity Catalog is an error **at config
  load**, not at write time.
- `PARTITIONED BY` or `CLUSTER BY`, never both.

### 5. `framework/writers.py`

Append and MERGE helpers with the Delta idempotency markers.

- `txnAppId` / `txnVersion` set on **appends** only.
- **Every MERGE must take a partition predicate.** Make it a required argument, not an
  optional one -- a MERGE without partition pruning rewrites whole table histories, and making
  it required means nobody can forget.
- Quarantine split helper.

### 6. SQL

Update `sql/01_operational_config.sql` and `sql/02_layer_tables.sql` with the new DDL, using
the `{catalog}` / `{ops_catalog}` placeholder convention the project already has. Grants
included.

---

## Do not build

- A migration or backfill script -- nothing is deployed.
- Compatibility views -- nothing is consuming the old names.
- A per-source column on `ingest_state`. It is key/value on purpose.
- A generic table-metadata registry.
- A retry wrapper around state writes. Let them raise.

---

## Files

**Create:** `framework/control.py`, `framework/state.py`, `framework/audit.py`,
`framework/tables.py`, `framework/writers.py`
**Edit:** `sql/01_operational_config.sql`, `sql/02_layer_tables.sql`, existing audit/tables
tests

---

## Exit gate

- `pytest -m "not spark" -q` green, test count up.
- Audit row / `StructType` / DDL three-way agreement test passes.
- Control-table test: missing row returns empty; duplicate rows raise; unknown key in
  `source_overrides` raises with the same message shape as a YAML typo.
- Audit-never-raises test passes with a deliberately failing underlying write.
- A test proves `writers.merge()` cannot be called without a partition predicate.
- The CORE section 7 grep returns nothing.

Then write the stage report (CORE section 9) and **stop**.
