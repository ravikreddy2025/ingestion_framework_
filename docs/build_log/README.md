# Build log

One `STAGE_<n>_REPORT.md` per stage of the multi-source ingestion rebuild (see
`.claude/build/CORE.md`). Each stage runs in a fresh session with no memory of earlier ones;
read every file here before starting a new stage, in order -- it carries the reasoning and
decisions the code itself does not.
