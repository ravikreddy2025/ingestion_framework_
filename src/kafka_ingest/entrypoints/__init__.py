"""Job entrypoints. One thin file per job shape, and nothing else in the package.

Each does exactly three things: parse job parameters, configure logging, call
framework/runner.py. All behaviour lives in the modules they compose, so a new run shape is
a twenty-line file rather than a fork of the ingestion logic - and adding a source, or a
source TYPE, never touches either of them.

    run_ingest.py   the scheduled primary run, for every source of every type
    run_replay.py   a replay, for whichever replay shapes a source type implements

There is deliberately no shared `bootstrap()` helper here. The two files have four
arguments in common and the sharing would save four lines each, at the cost of a reader
having to open a third file to learn what a job actually does.
"""
