"""Curated-only replay: re-parse existing landing rows. The broker is not contacted.

Use this when the BYTES ARE FINE but the interpretation was not:
  * a schema was registered after the records arrived, so they were quarantined
  * the reader schema was wrong or has since been corrected
  * a bug in this framework's parse path has been fixed

Because landing holds the original wire bytes, this works indefinitely - long after the
records have aged out of Kafka. That is precisely why it is a separate operation from a
Kafka replay and not a mode of one.

    --rerun-id         REQUIRED. Stamped on every rewritten curated row.
    --landing-filter   REQUIRED. SQL predicate over the landing table, e.g.
                       "ingest_date BETWEEN '2026-08-01' AND '2026-08-03'"
                       or "writer_schema_id = 4711"
                       Required deliberately: re-parsing all history by accident is an
                       expensive way to find out you meant one day.

The topic predicate is added automatically. Landing is one table per topic, so it matches
everything - but it makes a mistyped table name produce an empty replay rather than another
topic's rows in this curated table.
"""

from __future__ import annotations

import argparse
import logging

from .. import pipeline
from ..config import RUN_TYPE_CURATED_REPLAY
from . import add_common_arguments, bootstrap

LOG = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-parse landing into curated, no Kafka read")
    add_common_arguments(parser)
    parser.add_argument("--rerun-id", required=True, help="Short identifier, e.g. SCHEMAFIX-88")
    parser.add_argument("--landing-filter", required=True,
                        help="SQL predicate over the landing table")
    args = parser.parse_args()

    spark, cfg, secrets = bootstrap(
        args,
        RUN_TYPE_CURATED_REPLAY,
        overrides={"rerun_id": args.rerun_id, "landing_filter": args.landing_filter},
    )
    LOG.warning(
        "CURATED REPLAY '%s' for topic '%s' over %s WHERE %s. Curated rows matching "
        "(topic, kafka_partition, kafka_offset) are UPDATED in place.",
        cfg.run.rerun_id, cfg.topic, cfg.landing_table, cfg.run.landing_filter,
    )
    pipeline.run(spark, cfg, secrets)


if __name__ == "__main__":
    main()
