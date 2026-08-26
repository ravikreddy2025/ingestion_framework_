# STAGE 6 -- Extensibility gate, offline validation, CI

**Paste `00_CORE.md` before this file.** Stage 5 must be green.

This stage adds no features. It adds the checks that prove the redesign achieved what it was
for, and the offline substitutes for validation you cannot run.

---

## Work

### 1. The source-isolation gate

Add to `azure-pipelines.yml` as a PR step:

```bash
grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' src/kafka_ingest/framework/ \
  | grep -v 'runner.py:.*_SOURCES'
```

The step fails if this returns anything. If `framework/` knows any source's name outside the
dispatch dict, the spine has leaked and adding BigQuery will cost shared-code edits.

**If it currently returns matches, fix them now** -- that is the point of this stage. Report
each match and how you removed it. If a match cannot be removed without an abstraction, report
it as blocked rather than building one.

### 2. Offline validation tests

Five tests, all pure Python, standing in for the cluster validation you cannot run:

1. `yaml.safe_load` succeeds on **every** file under `conf/` and `resources/` and on
   `databricks.yml`.
2. Every job template's referenced entrypoint file **exists on disk**.
3. Every `source_type` in `conf/sources/*.yaml` has a matching package in `sources/`, and that
   package exposes `SOURCE_SPEC` and `run`.
4. Every register reference -- `jdbc_ref`, `storage_ref`, cluster ref, registry ref -- named in
   any source or environment file **exists in its register file**.
5. Every `SOURCE_SPEC`'s `structural_keys` and `operational_keys` are disjoint, and every
   `required_key` appears in `structural_keys`.

### 3. The cross-product test

**Every shipped source, of every type, resolves in every environment.** Wire it into the PR
gate. This is the highest-value test in the project and it now covers three source types.

Assert within it:

- No placeholder survives substitution in any resolved table name, path or endpoint.
- No two environments share a catalog, a table or a checkpoint.
- No two sources share a checkpoint or a target table.
- Topic name, Oracle schema/table and file path stay **identical** across environments while
  catalogs, endpoints and secret scopes differ.

### 4. `docs/DESIGN.md` -- "Adding a source type"

A short, concrete section listing exactly what a new package must provide:

- `sources/<type>/spec.py` with `SOURCE_SPEC` (no PySpark import)
- `sources/<type>/run.py` with `run(ctx) -> RunResult`
- one line in `runner._SOURCES`
- `conf/defaults/<type>.yaml`
- a register file, only if it needs a new connection kind
- one job template under `resources/`
- an inert onboarding template under `conf/sources/`

**Nothing under `framework/` changes.** Say that explicitly, and name the grep gate as what
enforces it.

### 5. BigQuery contract notes

No code. In the same DESIGN section, record the two questions the BigQuery design must answer:

- Which connector, and is it available on the target runtime?
- Direct read, or export-to-GCS staging? That choice decides whether BigQuery looks like
  Oracle (bounded query, watermark) or like Files (Auto Loader over exported objects).

### 6. Verification backlog review

Re-read `docs/VERIFICATION_BACKLOG.md`. Confirm every entry added during Stages 1 to 5 is
properly filled in, re-order by damage, and add anything the CI work surfaced.

---

## Do not build

- A coverage gate, a build matrix, or a release pipeline.
- A plugin scaffolding generator or a `new_source.py` script.
- Integration tests against real Delta, Kafka, Oracle or ADLS.
- Any new abstraction to make the grep gate pass. If the gate cannot pass without one, report
  it blocked.

---

## Files

**Create:** `tests/test_offline_validation.py`, `tests/test_cross_product.py` (or extend the
existing cross-product test)
**Edit:** `azure-pipelines.yml`, `docs/DESIGN.md`, `docs/VERIFICATION_BACKLOG.md`

---

## Exit gate

- The grep gate returns nothing, and the pipeline step that runs it exists.
- All five offline validation tests pass.
- The cross-product test is green across all three source types in all environments.
- `pytest -m "not spark" -q` green, test count up.
- `ruff check` and `ruff format --check` clean.

Then write the stage report (CORE section 9) and **stop**.

**Report explicitly:** every grep-gate match you had to remove, and what you did about it. That
list is the honest measure of how well Stage 1's abstraction held.
