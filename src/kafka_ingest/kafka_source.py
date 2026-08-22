"""Build the Kafka reader for primary runs and for replays.

Three shapes of read, one option builder:

  primary            readStream, resumes from the checkpoint under .../<topic>/primary
  unbounded replay   readStream, isolated checkpoint .../<topic>/replay/<rerun_id>,
                     starting at an explicit offset map or timestamp, running to latest
  bounded replay     spark.read (batch), explicit start AND end - the only way to cap a
                     replay, because the streaming Kafka source has no endingOffsets

The checkpoint isolation is the load-bearing detail. Spark resolves startingOffsets ONLY
when a checkpoint has no committed state; an existing checkpoint always wins. Pointing a
replay at the primary checkpoint would therefore silently ignore the requested offsets
*and* advance production state. Hence: replays never share a checkpoint with primary, and
`rerun_id` is mandatory (validated in config.RunContext).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamReader

from .config import RUN_TYPE_KAFKA_REPLAY, ConfigError, TopicConfig
from .security import SecretResolver, build_kafka_options, redact

LOG = logging.getLogger(__name__)


def build_source_options(cfg: TopicConfig, secrets: SecretResolver) -> Dict[str, str]:
    """Assemble every option for the Kafka source: connection, auth, position, safety."""
    options: Dict[str, str] = build_kafka_options(cfg.cluster, secrets)
    options["subscribe"] = cfg.topic
    options["includeHeaders"] = str(cfg.include_headers).lower()
    options["failOnDataLoss"] = str(cfg.fail_on_data_loss).lower()

    # Spark overrides kafka.group.id; groupIdPrefix is the supported way to make the
    # consumer identifiable in broker-side monitoring.
    options["groupIdPrefix"] = cfg.group_id_prefix

    if cfg.max_offsets_per_trigger:
        # Under Trigger.AvailableNow this does not cap the run - it splits the backlog
        # into multiple bounded microbatches, which keeps a large first run from
        # producing one enormous batch.
        options["maxOffsetsPerTrigger"] = str(cfg.max_offsets_per_trigger)

    options.update(_position_options(cfg))
    LOG.info("Kafka source options for topic '%s': %s", cfg.topic, redact(options))
    return options


def _position_options(cfg: TopicConfig) -> Dict[str, str]:
    """Start position. For primary runs this only matters on the very first run."""
    run = cfg.run
    if run.run_type != RUN_TYPE_KAFKA_REPLAY:
        return {"startingOffsets": cfg.starting_offsets}

    position: Dict[str, str] = {}
    if run.starting_offsets:
        position["startingOffsets"] = run.starting_offsets
    else:
        # startingTimestamp applies one timestamp to every partition, which is what a
        # support engineer means by "replay from 09:00". Per-partition timestamps would
        # use startingOffsetsByTimestamp; not exposed because nobody has needed it and an
        # unused knob is a knob that will be misconfigured.
        position["startingTimestamp"] = str(to_epoch_millis(run.starting_timestamp))
        # If a partition has no record at/after the timestamp, fail rather than silently
        # skipping to latest - a silent skip during incident recovery looks like success.
        position["startingOffsetsByTimestampStrategy"] = "error"

    if run.ending_offsets:
        position["endingOffsets"] = run.ending_offsets
    elif run.ending_timestamp:
        position["endingTimestamp"] = str(to_epoch_millis(run.ending_timestamp))
    return position


def to_epoch_millis(value: str) -> int:
    """Accept either epoch millis or an ISO-8601 timestamp.

    Support engineers type '2026-08-11T09:00:00Z' under pressure; job parameters that
    come from an automated retry carry raw millis. Both must work.
    A naive timestamp is interpreted as UTC and logged as such - guessing a local zone
    would move the replay window by hours without telling anyone.
    """
    value = str(value).strip()
    if value.lstrip("-").isdigit():
        return int(value)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigError(
            f"'{value}' is neither epoch millis nor an ISO-8601 timestamp "
            "(e.g. 2026-08-11T09:00:00Z)."
        ) from exc
    if parsed.tzinfo is None:
        LOG.warning("Timestamp '%s' has no timezone; interpreting as UTC.", value)
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def build_stream_reader(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> DataFrame:
    """Streaming read - primary runs and unbounded replays."""
    reader: DataStreamReader = spark.readStream.format("kafka")
    for key, value in build_source_options(cfg, secrets).items():
        reader = reader.option(key, value)
    LOG.info(
        "Streaming read for topic '%s' (%s) -> checkpoint %s",
        cfg.topic, cfg.run.run_type, cfg.checkpoint_path,
    )
    return reader.load()


def build_batch_reader(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> DataFrame:
    """Bounded batch read - a replay with an explicit end offset/timestamp.

    No checkpoint is involved at all, which is a feature: a bounded replay is a pure
    function of (start, end) and can be re-run to identical effect. Idempotency comes
    from the MERGE key, not from checkpoint state.
    """
    if not cfg.run.is_bounded:
        raise ConfigError("build_batch_reader called for an unbounded run - use build_stream_reader.")
    options = build_source_options(cfg, secrets)
    # groupIdPrefix / maxOffsetsPerTrigger are streaming-only; passing them to a batch
    # read is accepted but meaningless, so drop them to keep the plan honest.
    for streaming_only in ("groupIdPrefix", "maxOffsetsPerTrigger"):
        options.pop(streaming_only, None)
    reader = spark.read.format("kafka")
    for key, value in options.items():
        reader = reader.option(key, value)
    LOG.info("Bounded batch read for topic '%s' rerun_id=%s", cfg.topic, cfg.run.rerun_id)
    return reader.load()


def resolve_trigger(cfg: TopicConfig) -> Dict[str, object]:
    """Map the config string onto kwargs for DataStreamWriter.trigger(**kwargs).

    PySpark has no Trigger class (that is the Scala API) - the trigger is expressed as a
    single keyword argument, so this returns the kwargs dict.

    Default is availableNow: drain everything currently on the topic, then stop - the
    correct shape for a once-daily scheduled job, and the only trigger under which a
    Workflows run has a natural end.
    """
    spec = (cfg.trigger or "availableNow").strip()
    lowered = spec.lower()
    if lowered in ("availablenow", "available_now"):
        return {"availableNow": True}
    if lowered == "once":
        # Deprecated by Spark in favour of availableNow; kept only for a topic that was
        # explicitly tuned around single-batch semantics.
        return {"once": True}
    if lowered.startswith("processingtime="):
        return {"processingTime": spec.split("=", 1)[1].strip()}
    raise ConfigError(
        f"trigger '{spec}' not understood. Use 'availableNow', 'once', or "
        "'processingTime=<interval>' (e.g. processingTime=5 minutes)."
    )
