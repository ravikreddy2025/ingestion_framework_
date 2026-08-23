"""Primary ingestion. One job definition serves every source, of every type.

    databricks bundle run run_ingest --params source-key=vector_patient_events

Thin on purpose: parse arguments, call framework/runner.py. Adding a source, or a source
TYPE, never touches this file.
"""

from __future__ import annotations

import argparse

from ..framework import logs, runner


def build_parser() -> argparse.ArgumentParser:
    """Split out so the argument surface is assertable in a test without running a job."""
    parser = argparse.ArgumentParser(description="Config-driven multi-source ingestion")
    parser.add_argument(
        "--config-root",
        required=True,
        help="Directory holding defaults.yaml, environments/, sources/ and the registers "
        "(a /Workspace path deployed by DAB, or a UC Volume path).",
    )
    parser.add_argument(
        "--source-key",
        required=True,
        help="Config key - the sources/<key>.yaml filename, e.g. vector_patient_events",
    )
    parser.add_argument(
        "--environment",
        required=True,
        help="dev | preprod | prod - selects conf/environments/<env>.yaml. The DAB job "
        "definitions default this to ${bundle.target}, so it is never typed twice.",
    )
    parser.add_argument("--job-run-id", default=None, help="Databricks job run id, for audit correlation")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logs.configure()
    runner.run(
        source_key=args.source_key,
        environment=args.environment,
        config_root=args.config_root,
        job_run_id=args.job_run_id,
    )


if __name__ == "__main__":
    main()
