"""Kafka-sourced replay: re-pull records from the broker from a given offset/timestamp.

Use this when data is MISSING or WRONG AT SOURCE - a consumer outage, a checkpoint that
had to be reset, a producer that re-published corrected records.

Do NOT use this to fix a bad parse; that is replay_curated, which needs no broker and works
past Kafka retention.

This is ALSO the correct tool if someone has deleted a primary checkpoint. Never restart
the primary stream in that situation - see pipeline.guard_against_checkpoint_reset().

Parameters may come from the operational control table or from the Workflows UI. UI
parameters win, so an urgent replay needs no UPDATE statement first.

    --rerun-id             REQUIRED. Isolates the replay checkpoint AND the Delta txnAppId,
                           and tags every row written.
    --starting-offsets     JSON, e.g. {"orders":{"0":45231,"1":44870}}   (-2 earliest, -1 latest)
    --starting-timestamp   ISO-8601 or epoch millis, applied to all partitions
    --ending-offsets       optional; makes the replay a bounded batch read
    --ending-timestamp     optional; same, by time

Exactly one of --starting-offsets / --starting-timestamp is required (config validates it).
"""

from __future__ import annotations

import argparse
import logging

from .. import pipeline
from ..config import RUN_TYPE_KAFKA_REPLAY
from . import add_common_arguments, bootstrap

LOG = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka-sourced replay into landing and curated")
    add_common_arguments(parser)
    parser.add_argument("--rerun-id", required=True,
                        help="Short identifier, e.g. INC12345 - becomes a checkpoint path segment")
    parser.add_argument("--starting-offsets", default=None, help="Spark startingOffsets JSON")
    parser.add_argument("--starting-timestamp", default=None, help="ISO-8601 or epoch millis")
    parser.add_argument("--ending-offsets", default=None, help="Bounded replay: endingOffsets JSON")
    parser.add_argument("--ending-timestamp", default=None, help="Bounded replay: ISO-8601 or millis")
    args = parser.parse_args()

    spark, cfg, secrets = bootstrap(
        args,
        RUN_TYPE_KAFKA_REPLAY,
        overrides={
            "rerun_id": args.rerun_id,
            "starting_offsets": args.starting_offsets,
            "starting_timestamp": args.starting_timestamp,
            "ending_offsets": args.ending_offsets,
            "ending_timestamp": args.ending_timestamp,
        },
    )
    LOG.warning(
        "REPLAY '%s' for topic '%s'. Checkpoint: %s (isolated from primary). "
        "Landing inserts-if-absent, curated upserts. Rows tagged ingested_via='kafka_replay'.",
        cfg.run.rerun_id, cfg.topic, cfg.checkpoint_path,
    )
    pipeline.run(spark, cfg, secrets)


if __name__ == "__main__":
    main()
