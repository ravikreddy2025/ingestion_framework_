# STAGE 0 -- Orientation and the verification backlog

**Paste `00_CORE.md` before this file.**

**No file under `src/` or `conf/` changes in this stage.** This is reconnaissance and a
deliverable for the human.

---

## Work

### 1. Inventory the codebase

Read `final_code/` and print a table: every file, one line on what it does, and a
`KEEP / REWRITE / DELETE / MOVE` verdict against the target layout in CORE section 4.3.

Do not act on the verdicts. This is the map the later stages navigate by, and it is the first
thing that tells the human how much of the existing code survives.

### 2. Create `docs/VERIFICATION_BACKLOG.md`

Use the exact entry format from CORE section 3. Seed it with these thirteen. Fill in every
field properly -- "how to check" must be a command or query someone can actually run, not a
description of one.

| ID | Question |
|---|---|
| VB-01 | Does Spark's JDBC `query` option work with `partitionColumn`, or is a parenthesised subquery in `dbtable` required? |
| VB-02 | What Spark type does Oracle `NUMBER` without precision/scale map to on the target DBR and driver? |
| VB-03 | Does Oracle `DATE` map to date or timestamp, and under which driver property? |
| VB-04 | Which Oracle types in our tables have no clean Spark mapping (LOB, RAW, INTERVAL, TZ types)? |
| VB-05 | Is `sources[0].latestOffset` populated in `StreamingQueryProgress` under `availableNow`? |
| VB-06 | Is `_metadata` available on the target DBR, and how does `rescuedDataColumn` behave per file format? |
| VB-07 | Auto Loader directory-listing vs file-notification mode -- which is viable in this tenancy? |
| VB-08 | Is a UC Volume supported as a Structured Streaming checkpoint location on serverless jobs compute? |
| VB-09 | Which Delta MERGE schema-evolution mechanism exists on the target DBR -- session config flag, or builder method? |
| VB-10 | Does the `from_avro` writer/reader startup self-check pass on the target runtime? |
| VB-11 | Can executors read UC Volumes on the target compute access mode (Kafka keystore/truststore)? |
| VB-12 | Serverless egress to brokers, registry, Oracle and ADLS -- is network configuration in place? |
| VB-13 | Does `databricks bundle validate -t dev` pass, and does the wheel build? |

Order the file by **how much breaks if the assumption is wrong**, most damaging first. State
that ordering at the top of the file.

### 3. Record the baseline

Run `pytest -m "not spark" -q` and record the test count. Every later stage reports against
this number.

### 4. Check `CLAUDE.md`

`CLAUDE.md` already exists at the repository root and loads automatically in every session.
Read it and confirm it is accurate against what you found in the inventory.

- If a stated path or module name is already wrong, correct it.
- If the inventory revealed an invariant that is true and missing, add one line.
- **Do not lengthen it.** It sits in context on every turn of every session, so every line
  costs. One screen is the budget. Detail belongs in `.claude/build/CORE.md`, which is read on
  demand.

Report any change you made and why.

### 5. Create `docs/build_log/`

Create the directory and put a one-line `README.md` in it saying what it is for. Stage reports
land here from this stage onward.

### 6. Flag what you already see

While reading, note anything in the existing code that will make the later stages harder --
a module that mixes config and Spark, a hardcoded topic assumption, a test that will not
survive the rename. One line each. Do not fix anything.

---

## Files

**Create:** `docs/VERIFICATION_BACKLOG.md`, `docs/build_log/README.md`,
`docs/build_log/STAGE_0_REPORT.md`
**Edit:** `CLAUDE.md` only if the inventory showed something in it is wrong

---

## Exit gate

- Backlog file exists with all thirteen entries, fully filled in, ordered by damage.
- `CLAUDE.md` checked against the inventory; still one screen.
- `docs/build_log/` exists and holds this stage's report.
- Inventory table printed.
- Baseline test count recorded.
- Nothing changed under `src/`, `conf/`, `resources/` or `sql/`.

Then write the stage report (CORE section 9) and **stop**.
