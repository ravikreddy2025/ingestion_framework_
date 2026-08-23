"""One package per source type.

A source's ENTIRE public surface is `SOURCE_SPEC` (data) and `run(ctx) -> RunResult` (one
function). Adding a source type is a new package here and zero changes under framework/ -
except the one line in framework/runner.py's `_SOURCES` dict that makes it reachable.
"""
