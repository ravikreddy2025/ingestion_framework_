# Multi-source ingestion framework

A config-driven ingestion framework on Databricks: Kafka, Oracle (JDBC) and ADLS file loads
share one configuration, control, audit, state and logging spine. Extensible to BigQuery.

**This repository is being rebuilt in eight staged passes.** The brief lives in
`.claude/build/` -- read `CORE.md` there before doing any redesign work, then the file for the
stage you were asked to run. Progress and decisions so far are in `docs/build_log/`.

---

## Invariants -- these hold at all times

**Environment**
- Local development only. There is **no** Kafka, Oracle, ADLS, Databricks workspace, cluster or
  JVM available. Never run, and never report as run, anything that needs one.
- The only runnable gate is:
  ```
  ruff check src tests
  ruff format --check src tests
  pytest -m "not spark" -q
  ```
  Not `databricks bundle validate`. Not unmarked `pytest` -- it will try Spark.

**Architecture**
- `framework/config.py` and every `sources/*/spec.py` import **no PySpark**. This is what keeps
  config tests running without a cluster.
- `framework/` never names a source type outside the `_SOURCES` dict in `runner.py`. Verify:
  ```
  grep -rInE '\b(kafka|oracle|bigquery|autoloader|cloudFiles|jdbc)\b' framework/ | grep -v 'runner.py:.*_SOURCES'
  ```
  must return nothing.
- A source's entire public surface is `SOURCE_SPEC` (data) and `run(ctx) -> RunResult` (one
  function). Do not add `read()` / `parse()` / `write()` / `validate()` to that contract.
- No base class or interface with one implementation. No class hierarchy for sources. No plugin
  registry, DI container, or dynamic import by string.

**Data correctness**
- Structural config fields -- partitioning, dedup/merge keys, target names, Oracle
  `source_schema` / `source_table` / `filter_criteria`, file target path -- are **never**
  operationally overridable.
- **State writes must raise on failure. Audit writes must never raise.** That asymmetry is
  deliberate: extraction correctness depends on state, and must not depend on best-effort
  audit.
- Every MERGE takes a partition predicate. `writers.merge()` requires it as an argument.
- Oracle cursor extraction uses a **closed interval** -- `> :last_watermark AND <=
  :run_high_water`. Never an open upper bound.
- Order for watermark sources: read -> write -> commit -> **then** advance the watermark.
- A replay never advances production state.

**Dependencies**
- Runtime dependencies are exactly `PyYAML` and `requests`. Development tooling may be added;
  runtime dependencies may not.

**Honesty**
- Do not invent an API, option or config key. A key that silently does nothing is the worst
  possible outcome here.
- Any assumption you cannot verify locally becomes an entry in `docs/VERIFICATION_BACKLOG.md`,
  never a confident comment in the code.
- Never report a command as passing if you did not run it.

**Working method**
- Stage reports go in `docs/build_log/STAGE_<n>_REPORT.md`. Read every file there before
  starting work -- each stage runs in a fresh session and that log carries reasoning the code
  does not.
- One stage per session. Finish it, run the gate, write the report, stop.

**Git workflow -- one branch and one PR per stage**
- Never commit to `main`. At the start of a stage, branch from an up-to-date `main`:
  `git checkout main && git pull && git checkout -b stage-<n>-<short-name>`
- Commit as you go, in logical units. Do not squash a whole stage into one commit -- the
  per-commit diff is how the stage gets reviewed.
- At the end of the stage, after the gate is green and the report is written:
  `git push -u origin stage-<n>-<short-name>`
- Then open a PR into `main`. Title: `Stage <n>: <name>`. Body: the five lists from the stage
  report, plus the test count before and after, plus any new VB entries. If `gh` is available
  use `gh pr create`; if not, print the PR URL git returns on push and stop.
- **Never merge your own PR, and never push to `main`.** The human reviews and merges.
- Never use `--no-verify`, `--force`, or amend a pushed commit.

---

## Layout

```
src/kafka_ingest/       (being restructured into framework/ + sources/)
  framework/            config, control, security, state, audit, tables, writers, runner, logs
  sources/              kafka/, oracle/, file/  -- each: spec.py + run.py
  entrypoints/          run_ingest.py, run_replay.py
conf/                   defaults, environments, sources, registers (clusters/registries/jdbc/storage)
sql/                    operational config, layer tables, support queries
resources/              Databricks Asset Bundle job definitions
docs/                   README, NAVIGATION, DESIGN, CONFIGURATION, runbooks,
                        VERIFICATION_BACKLOG, build_log/
.claude/build/          the staged rebuild brief -- CORE.md + STAGE_0..7
```
