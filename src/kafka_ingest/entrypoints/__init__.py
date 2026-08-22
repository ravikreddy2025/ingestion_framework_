"""Job entrypoints.

Each entrypoint does three things and nothing else: parse job parameters, resolve the
topic config, call pipeline.run(). All behaviour lives in the modules they compose, so a
new run shape is a new twenty-line file rather than a fork of the ingestion logic.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from ..config import TopicConfig, resolve_topic_config
from ..security import SecretResolver


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        stream=sys.stdout,
        force=True,
    )


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Parameters every job takes. Names match the Workflows job-parameter keys."""
    parser.add_argument(
        "--config-root",
        required=True,
        help="Directory holding clusters.yaml, registries.yaml and topics/ "
             "(a /Workspace path deployed by DAB, or a UC Volume path).",
    )
    parser.add_argument("--topic-key", required=True, help="Config key, e.g. vector_patient_events")
    parser.add_argument(
        "--environment",
        required=True,
        help="dev | preprod | prod - selects conf/environments/<env>.yaml. The DAB job "
             "definitions default this to ${bundle.target}, so it is never typed twice.",
    )
    parser.add_argument(
        "--control-table",
        required=True,
        help="3-tier name of the operational control table, "
             "e.g. ops_prod.ingestion.ingestion_topic_control",
    )
    parser.add_argument("--job-run-id", default=None, help="Databricks job run id, for audit correlation")


def bootstrap(args: argparse.Namespace, run_type: str, overrides: Optional[Dict[str, Any]] = None) -> tuple:
    """Resolve (spark, cfg, secrets) from parsed arguments."""
    configure_logging()
    spark = SparkSession.builder.getOrCreate()
    secrets = SecretResolver()
    merged: Dict[str, Any] = {"job_run_id": args.job_run_id}
    merged.update(overrides or {})
    cfg: TopicConfig = resolve_topic_config(
        spark=spark,
        config_root=args.config_root,
        topic_key=args.topic_key,
        control_table=args.control_table,
        environment=args.environment,
        run_type=run_type,
        overrides=merged,
    )
    return spark, cfg, secrets
