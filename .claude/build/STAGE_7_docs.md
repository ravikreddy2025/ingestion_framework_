# STAGE 7 -- Documentation and final report

**Paste `00_CORE.md` before this file.** Stage 6 must be green.

Update the existing documents **in place**. The only new documentation file this project
produces is `docs/VERIFICATION_BACKLOG.md` from Stage 0.

The audience question that governs this stage: **a new joiner opens this repository on their
first morning. Can they be productive by lunch?** That is the actual requirement, not
completeness.

---

## Work

### `README.md`
What the framework does now -- three sources, one spine. Onboarding a source of each type, in
three short sections. Replay runbook summary. Link to NAVIGATION.

### `docs/NAVIGATION.md`
The map. Must include:

- A "start here" table by role -- developer, support engineer, architect.
- A ten-minute path to understanding the architecture.
- **A trace of one record through every module in execution order, once per source type.**
  Three traces: a Kafka message, an Oracle row, a file line. This is the single most useful
  page in the whole document set for a new joiner.
- A per-file "open it when..." table covering the new layout.
- An "I want to... -> go to..." lookup.
- An explicit list of files a newcomer can safely ignore.

### `docs/DESIGN.md`
- The architecture: spine plus source packages, and why the source contract has one method.
- The source contract, with the `contracts.py` skeleton inline.
- **Per-source failure-scenario tables** -- Kafka, Oracle, File. Each: what happens, do
  duplicates occur, how to fix it without a code change.
- "Adding a source type" (written in Stage 6).
- Deliberate non-abstractions: what was not built, and why.
- A "where to make common changes" lookup.
- The unverified-claims list, **cross-referenced to VB ids** rather than restating them.

### `docs/CONFIGURATION.md`
Every setting, grouped by source type, tiered **MUST CHANGE / NICE TO CHANGE / NO CHANGE
REQUIRED**, with what breaks if each is wrong.

**Mirror those markers inline in the YAML files themselves**, so the file being edited tells
the reader whether they should be editing it.

Three rows that must be present and marked MUST-READ:
- Schedule cadence vs source retention or purge window (one third rule).
- Oracle `merge_keys` absent -> boundary-tie rows can be lost.
- File `schema_mode: infer` -> silent type drift.

### `docs/RUNBOOK_DEVELOPER.md`
- **Local setup with no cluster** -- what runs, what does not, and the three commands that
  constitute the gate.
- Codebase tour following the module layout.
- Invariants that must not be broken: `config.py` has no PySpark import; the grep gate;
  structural fields are not operationally overridable; state writes raise, audit writes do not;
  every MERGE takes a partition predicate.
- Adding a source type (link to DESIGN).
- Onboarding a source of each existing type.
- Debugging table.
- PR checklist.

### `docs/RUNBOOK_SUPPORT.md`
SQL and job parameters only -- no code reading required.

- **One daily health check across all sources**, not three. It should answer: did everything
  run, is anything lagging, is anything quarantining or rescuing above threshold, is anything
  running with `failOnDataLoss` disabled.
- Onboarding and decommissioning, in the correct order.
- **Per-source incident playbooks**, mapped to the failure-scenario tables.
- The checkpoint-reset resume sequence (from Stage 3), prominently -- it is the procedure most
  likely to be needed under pressure.
- Replay procedures per source.
- Escalation criteria.
- An explicit "what support can and cannot change" table, **including the JSON overrides
  column**.

### `docs/ARCHITECTURE_OVERVIEW.md` (client IT audience)
Non-implementation: data flow across three sources, security model including the new JDBC and
storage surfaces, platform prerequisites, governance and grants, operating model, data
protection considerations per source, assurance summary, deliberate limitations, glossary.

### Notebooks
One escalation path per source type: resolve config touching nothing -> run the tests -> check
connectivity without reading data -> the first real run. Thin drivers only; no logic in a
notebook.

---

## Do not build

- New documentation files beyond the backlog.
- A generated API reference.
- Diagrams that need a rendering toolchain -- ASCII or Mermaid in the markdown is fine.
- Rewriting docstrings that are already good. Extend where behaviour changed.

---

## Files

**Edit:** `README.md`, `docs/NAVIGATION.md`, `docs/DESIGN.md`, `docs/CONFIGURATION.md`,
`docs/RUNBOOK_DEVELOPER.md`, `docs/RUNBOOK_SUPPORT.md`, `docs/ARCHITECTURE_OVERVIEW.md`,
`docs/VERIFICATION_BACKLOG.md`, `notebooks/*`, `conf/**/*.yaml` (inline markers)

---

## Exit gate

- `pytest -m "not spark" -q` green.
- Every document updated; no new documentation file except the backlog.
- Every YAML file carries inline MUST / NICE / NO CHANGE markers matching CONFIGURATION.md.

---

## Final report

The five lists from CORE section 9, covering the whole project, plus:

### 1. Stage gates
For each of the eight stages: green or not, and what proves it.

### 2. The complete verification backlog
Reproduce `docs/VERIFICATION_BACKLOG.md` in full, ordered by how much breaks if the assumption
is wrong. Flag the three most dangerous.

### 3. Test counts
Before Stage 0 and after Stage 7, both from actual pytest output, never estimated.

### 4. Team-onboarding note
- The five files a new joiner should read, **in order**.
- Roughly how long the whole codebase takes to understand end to end.
- **If that number is over two hours, name the module carrying too much and say why.** This is
  the project's actual success criterion -- a small team has to own this code -- so answer it
  honestly rather than optimistically.

### 5. What you would change
One short list: anything you built the way this brief specified but would have done
differently, with one sentence each. You have seen the whole codebase by now and the human has
not seen it in this shape. Say what you noticed.
