"""Replay entrypoint. One file, source-aware, never conflated with a primary run.

    databricks bundle run replay --params source_key=...,run_type=kafka_replay,rerun_id=INC123

WHAT A REPLAY IS, AND WHAT IT IS NOT
------------------------------------
`--run-type` names the shape, and the shapes solve different problems. Confusing them
during an incident is expensive, so the parameter is REQUIRED and has no default:

  kafka_replay     data is MISSING or WRONG AT SOURCE. Re-pulls from the broker from a
                   given offset or timestamp, into an ISOLATED checkpoint with its own
                   Delta transaction identity. Bounded by the broker's retention.
                   This is ALSO the correct tool when a primary checkpoint has been lost
                   and the gap has to be refilled.
  curated_replay   the bytes are fine, the PARSE was not. Re-reads the landing table, never
                   contacts the broker, and therefore works forever regardless of retention.

Everything below is passed through as a JOB PARAMETER, which means it is validated against
the source type's own SOURCE_SPEC exactly like a YAML key: a misspelled replay control is
an unknown-key error naming the key, not a silently ignored one. Which parameters a given
source type accepts is that type's spec - this file names none of them, and adding a
replayable source type does not change it.
"""

from __future__ import annotations

import argparse

from ..framework import logs, runner

# Job parameters that carry a replay's bounds. The CLI spelling is `--replay-x`; the
# setting name is `replay_x`. Kept here as data rather than as one argparse call per name
# so that adding one is a single line.
_REPLAY_PARAMETERS = (
    "replay_starting_offsets",
    "replay_starting_timestamp",
    "replay_ending_offsets",
    "replay_ending_timestamp",
    "replay_landing_filter",
)


def build_parser() -> argparse.ArgumentParser:
    """Split out so the argument surface is assertable in a test without running a job."""
    parser = argparse.ArgumentParser(description="Replay a source into its own isolated lineage")
    parser.add_argument("--config-root", required=True, help="Directory holding defaults.yaml, environments/, sources/")
    parser.add_argument("--source-key", required=True, help="Config key - the sources/<key>.yaml filename")
    parser.add_argument("--environment", required=True, help="dev | preprod | prod")
    parser.add_argument(
        "--run-type",
        required=True,
        help="The replay shape this source type implements, e.g. kafka_replay or curated_replay. "
        "No default: choosing the wrong shape during an incident is the mistake this prevents.",
    )
    parser.add_argument(
        "--rerun-id",
        required=True,
        help="REQUIRED. Isolates BOTH the checkpoint path and the Delta transaction identity, "
        "and tags every row the replay writes.",
    )
    for name in _REPLAY_PARAMETERS:
        parser.add_argument(f"--{name.replace('_', '-')}", default=None)
    parser.add_argument("--job-run-id", default=None, help="Databricks job run id, for audit correlation")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logs.configure()
    parameters = {"rerun_id": args.rerun_id}
    for name in _REPLAY_PARAMETERS:
        value = getattr(args, name, None)
        # Empty strings are dropped, not passed on: a Workflows parameter left blank
        # arrives as "" and would otherwise look like a deliberately empty setting.
        if value:
            parameters[name] = value
    runner.run(
        source_key=args.source_key,
        environment=args.environment,
        config_root=args.config_root,
        run_type=args.run_type,
        job_parameters=parameters,
        job_run_id=args.job_run_id,
    )


if __name__ == "__main__":
    main()
