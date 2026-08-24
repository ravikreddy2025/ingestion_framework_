"""Building the Kafka reader, for primary runs and for both shapes of replay.

Three shapes of read, one option builder:

  primary            readStream, resumes from the checkpoint under .../<source_key>/primary
  unbounded replay   readStream, isolated checkpoint .../<source_key>/replay/<rerun_id>,
                     starting at an explicit offset map or timestamp, running to latest
  bounded replay     spark.read (batch), explicit start AND end - the only way to cap a
                     replay, because the STREAMING Kafka source has no endingOffsets option

The checkpoint isolation is the load-bearing detail. Spark resolves startingOffsets ONLY
when a checkpoint has no committed state; an existing checkpoint always wins. Pointing a
replay at the primary checkpoint would therefore silently ignore the requested offsets AND
advance production state, which is why `rerun_id` is mandatory (validated in config.py).

FOUR READER OPTIONS ARE NOT NEGOTIABLE
--------------------------------------
  includeHeaders          "true", always, not configurable. Every ce_* column is read from
                          Kafka headers; without it they are all NULL - and a table full of
                          NULLs is not an error, which is exactly why it must not be a knob.
  maxOffsetsPerTrigger    always set. Unset means one microbatch for the entire backlog:
                          a first run on a retained topic becomes a single enormous batch
                          whose failure costs the whole run.
  minPartitions           without it, read parallelism is capped at the topic's partition
                          count no matter how large the cluster is.
  failOnDataLoss          "true" by default. False means silently accepting gaps.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ...framework.config import ConfigError
from ...framework.security import SecretResolver, redact
from .config import RUN_TYPE_KAFKA_REPLAY, KafkaConfig
from .security import build_kafka_options

LOG = logging.getLogger(__name__)

# Options that mean something to a streaming read and nothing to a batch read. Passing
# them to spark.read is accepted and ignored, which makes a plan lie about what it does.
_STREAMING_ONLY = ("groupIdPrefix", "maxOffsetsPerTrigger", "minPartitions")


def build_source_options(cfg: KafkaConfig, secrets: SecretResolver) -> dict[str, str]:
    """Every option for the Kafka source: connection, auth, position, safety."""
    options: dict[str, str] = build_kafka_options(cfg.cluster, secrets)
    options["subscribe"] = cfg.topic

    # Not derived from configuration: see the module docstring. A topic that emits no
    # CloudEvents simply gets NULL ce_* columns, which is a fact about the producer.
    options["includeHeaders"] = "true"

    options["failOnDataLoss"] = str(cfg.fail_on_data_loss).lower()

    # Spark OVERRIDES kafka.group.id; groupIdPrefix is the supported way to make this
    # consumer identifiable in broker-side monitoring.
    options["groupIdPrefix"] = cfg.group_id_prefix

    # Under Trigger.AvailableNow this does not cap the RUN - it splits the backlog into
    # several bounded microbatches. That is the point: it bounds the blast radius of one
    # failed batch, it does not limit what the run consumes.
    options["maxOffsetsPerTrigger"] = str(cfg.max_offsets_per_trigger)

    # Only ever SPLITS offset ranges, never merges them, so over-setting is cheap and
    # under-setting silently caps throughput at the topic's partition count.
    options["minPartitions"] = str(cfg.min_partitions)

    options.update(_position_options(cfg))
    LOG.info("Kafka source options for topic '%s': %s", cfg.topic, redact(options))
    return options


def _position_options(cfg: KafkaConfig) -> dict[str, str]:
    """Where to start, and for a bounded replay, where to stop.

    For a primary run this only matters on the very first run: after that the checkpoint
    exists and beats every option here.
    """
    if cfg.run_type != RUN_TYPE_KAFKA_REPLAY:
        return {"startingOffsets": cfg.starting_offsets}

    replay = cfg.replay
    position: dict[str, str] = {}
    if replay.starting_offsets:
        position["startingOffsets"] = replay.starting_offsets
    else:
        # startingTimestamp applies ONE timestamp to every partition, which is what a
        # support engineer means by "replay from 09:00". Per-partition timestamps would
        # need startingOffsetsByTimestamp; not exposed, because nobody has needed it and
        # an unused knob is a knob that will eventually be misconfigured.
        position["startingTimestamp"] = str(to_epoch_millis(replay.starting_timestamp))
        # If a partition has no record at or after the timestamp, FAIL rather than
        # silently skipping to latest: a silent skip during incident recovery produces a
        # run that reports success and recovers nothing.
        position["startingOffsetsByTimestampStrategy"] = "error"

    if replay.ending_offsets:
        position["endingOffsets"] = replay.ending_offsets
    elif replay.ending_timestamp:
        position["endingTimestamp"] = str(to_epoch_millis(replay.ending_timestamp))
    return position


def to_epoch_millis(value: str) -> int:
    """Accept either epoch millis or an ISO-8601 timestamp.

    Support engineers type '2026-08-11T09:00:00Z' under pressure; job parameters from an
    automated retry carry raw millis. Both must work.

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
            f"'{value}' is neither epoch millis nor an ISO-8601 timestamp (e.g. 2026-08-11T09:00:00Z)."
        ) from exc
    if parsed.tzinfo is None:
        LOG.warning("Timestamp '%s' has no timezone; interpreting as UTC.", value)
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def build_stream_reader(spark: Any, cfg: KafkaConfig, secrets: SecretResolver) -> Any:
    """Streaming read - primary runs and unbounded replays."""
    reader = spark.readStream.format("kafka")
    for key, value in build_source_options(cfg, secrets).items():
        reader = reader.option(key, value)
    LOG.info("Streaming read for topic '%s' (%s) -> checkpoint %s", cfg.topic, cfg.run_type, cfg.checkpoint_path)
    return reader.load()


def build_batch_reader(spark: Any, cfg: KafkaConfig, secrets: SecretResolver) -> Any:
    """Bounded batch read - a replay with an explicit end offset or timestamp.

    No checkpoint is involved at all, which is a feature: a bounded replay is a pure
    function of (start, end) and re-running it has identical effect. Idempotency comes from
    the MERGE key, not from checkpoint state.
    """
    if not cfg.replay.is_bounded:
        raise ConfigError("build_batch_reader called for an unbounded run - use build_stream_reader.")
    options = build_source_options(cfg, secrets)
    for streaming_only in _STREAMING_ONLY:
        options.pop(streaming_only, None)
    reader = spark.read.format("kafka")
    for key, value in options.items():
        reader = reader.option(key, value)
    LOG.info("Bounded batch read for topic '%s' rerun_id=%s", cfg.topic, cfg.replay.rerun_id)
    return reader.load()


def resolve_trigger(cfg: KafkaConfig) -> dict[str, object]:
    """Map the configured string onto kwargs for DataStreamWriter.trigger(**kwargs).

    PySpark has no Trigger class - that is the Scala API - so the trigger is expressed as a
    single keyword argument, which is why this returns a kwargs dict rather than an object.

    availableNow is the default: drain everything currently on the topic, then stop. It is
    the right shape for a scheduled job and the only trigger under which a Workflows run
    has a natural end.
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
