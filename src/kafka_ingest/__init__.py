"""Config-driven multi-source ingestion framework for Databricks.

One configuration, control, audit, state and logging spine, with one package per source
type behind it. Kafka lands and curates; Oracle and file loads land only.

    framework/    config, control, security, state, audit, tables, writers, runner, logs
    sources/      one package per source type. Public surface: SOURCE_SPEC and run(ctx)
    entrypoints/  argparse, then framework/runner.py

Read framework/contracts.py first - it is the smallest file and it defines the only three
things every other module agrees on. Then docs/NAVIGATION.md, which traces one record
through every module in execution order.
"""

__version__ = "0.2.0"
