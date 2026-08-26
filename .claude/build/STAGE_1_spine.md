# STAGE 1 -- Framework spine and config model

**Paste `00_CORE.md` before this file.** Stage 0 must be complete.

This is the stage that decides whether the whole redesign works. Everything after it is
filling in shapes this stage defines. **The main risk is over-abstraction** -- read CORE
section 7 again before you start.

---

## Work

### 1. `framework/contracts.py`

`SourceSpec`, `RunContext`, `RunResult` exactly as skeletoned in CORE section 4.1. Frozen
dataclasses. **No PySpark import.**

Nothing else goes in this file. No helpers, no base classes, no factory functions.

### 2. `framework/config.py`

Port the existing five-layer loader and make it spec-driven per CORE section 4.2:

- Loads `conf/defaults.yaml`, then `conf/defaults/<source_type>.yaml`, then the environment
  file, then `conf/sources/<source_key>.yaml`, then the control-table overrides, then job
  parameters. Later wins per key; absent keys fall through.
- Reads `source_type` from the source file and fetches that source's `SOURCE_SPEC` to drive
  validation -- unknown key, missing required key, mutually exclusive keys, structural fields
  not operationally overridable, unresolved placeholder.
- **No `if source_type == ...` anywhere.** If you write one, the spec is missing a field.
- **No PySpark import.** Assert this in a test that fails if anyone adds one.

Keep every error message the existing code has. They are unusually good and the team will read
them more often than the code.

### 3. `framework/logs.py`

Structured log lines carrying `source_type`, `source_key`, `run_id`. Secret redaction. Port
the existing redaction test and extend it to cover JDBC and storage option dicts.

### 4. `framework/runner.py`

The run lifecycle, and **the only** place that knows source-type names:

```python
_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}
```

At this stage the three source modules are stubs -- a `spec.py` with a minimal `SOURCE_SPEC`
and a `run.py` whose `run()` raises `NotImplementedError`. That is correct and expected;
Stages 3 to 5 fill them in.

`runner.run(source_key, environment)` should: resolve config -> build `RunContext` ->
dispatch -> write the audit row -> return. Audit and state are stubs until Stage 2; wire the
call sites now so Stage 2 is a fill-in, not a restructure.

### 5. `entrypoints/run_ingest.py`

Thin. Parse `--source-key` and `--environment`, call `runner`. No logic.

### 6. Reorganise `conf/`

Per CORE section 6:

- `conf/topics/*.yaml` -> `conf/sources/*.yaml`, adding `source_type: kafka` to each and
  **changing nothing else**.
- Create `conf/defaults/kafka.yaml`, `conf/defaults/oracle.yaml`, `conf/defaults/file.yaml`.
  Kafka's carries what is Kafka-specific from the current `defaults.yaml`; the other two are
  headers and comments for now.
- Create `conf/jdbc.yaml` and `conf/storage.yaml` as empty registers with a header comment
  explaining the register pattern.
- Rename the `{topic_key}` placeholder to `{source_key}` throughout.

---

## Do not build

- A `BaseSource` class, or any class hierarchy for sources.
- `read()` / `parse()` / `write()` / `validate()` on the source contract.
- A registry class, entry-point discovery, or dynamic import by string.
- A `SourceConfig` superclass. Each source gets its own frozen dataclass in Stages 3 to 5.
- A DI container, a service locator, or a context builder with more than one call site.
- Any new dependency.

---

## Files

**Create:** `framework/contracts.py`, `framework/config.py`, `framework/logs.py`,
`framework/runner.py`, `entrypoints/run_ingest.py`, `sources/{kafka,oracle,file}/spec.py`
(stubs), `sources/{kafka,oracle,file}/run.py` (stubs), `conf/defaults/{kafka,oracle,file}.yaml`,
`conf/jdbc.yaml`, `conf/storage.yaml`
**Edit:** `conf/topics/*` -> `conf/sources/*`, `conf/defaults.yaml`, `conf/environments/*`,
existing config tests

---

## Exit gate

- `pytest -m "not spark" -q` green.
- A test asserts `framework/config.py` imports no PySpark, and passes.
- Config tests cover, separately: defaults-only; environment-over-defaults;
  source-over-environment; control-table-over-YAML; job-parameters-over-control-table; an
  absent key falling through; unknown key rejected; mutually exclusive keys rejected;
  structural field override ignored; unresolved placeholder rejected.
- The CORE section 7 grep returns nothing.
- `ruff check` and `ruff format --check` clean.

Then write the stage report (CORE section 9) and **stop**.

**Report explicitly:** the full contents of `contracts.py`, and one paragraph on why the source
contract has exactly one method. If you cannot justify it in a paragraph, you have built the
wrong thing.
