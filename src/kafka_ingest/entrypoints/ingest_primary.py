"""Primary daily ingestion. One job definition serves every topic.

    databricks bundle run ingest_primary --params topic_key=vector_patient_events

Adding a topic never touches this file.
"""

from __future__ import annotations

import argparse
import logging

from .. import pipeline
from ..config import RUN_TYPE_PRIMARY
from . import add_common_arguments, bootstrap

LOG = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka -> landing/curated primary ingestion")
    add_common_arguments(parser)
    args = parser.parse_args()

    spark, cfg, secrets = bootstrap(args, RUN_TYPE_PRIMARY)
    LOG.info(
        "Resolved config for '%s' [%s]: cluster=%s (%s) registry=%s landing=%s curated=%s enabled=%s",
        cfg.topic_key, cfg.environment, cfg.cluster.name, cfg.cluster.bootstrap_servers,
        cfg.registry.name, cfg.landing_table, cfg.curated_table, cfg.enabled,
    )
    pipeline.run(spark, cfg, secrets)


if __name__ == "__main__":
    main()
