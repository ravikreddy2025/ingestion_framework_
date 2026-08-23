"""The source-agnostic spine: config, control, security, state, audit, tables, writers,
runner, logs.

Nothing in this package may name a source type - not in a module name, not in a string,
not in a comment - except the `_SOURCES` dispatch dict in runner.py. CORE section 7 states
that as a grep over this directory which must return nothing; it is the one falsifiable
test of whether the architecture holds. If the spine leaks, adding a fourth source type
means editing the framework, and the whole point of the layout is gone.

(The grep's own pattern is not repeated here for the obvious reason.)
"""
