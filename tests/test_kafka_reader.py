"""sources/kafka/reader.py - the options handed to the Kafka source.

Every assertion here is about a value Spark will act on. Four of them are the options the
stage brief calls non-negotiable, and three of those four fail SILENTLY when wrong: no
includeHeaders means NULL CloudEvent columns rather than an error, no maxOffsetsPerTrigger
means one unbounded microbatch, and no minPartitions means read parallelism capped at the
topic's partition count no matter how large the cluster is. Nothing about a run that hit
any of the three would look wrong afterwards, which is exactly why they are pinned.
"""

from __future__ import annotations

import pytest

from conftest import make_kafka_cfg
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.kafka.config import RUN_TYPE_KAFKA_REPLAY
from kafka_ingest.sources.kafka.reader import (
    build_source_options,
    resolve_trigger,
    to_epoch_millis,
)

OFFSETS = '{"demo.events.v1": {"0": 100, "1": 250}}'


def _options(config_root, secrets, **kwargs):
    return build_source_options(make_kafka_cfg(config_root, **kwargs), secrets)


# --------------------------------------------------------------------------------------
# The four options that must always be present
# --------------------------------------------------------------------------------------


def test_all_four_required_reader_options_are_present_with_their_expected_values(config_root, secrets):
    options = _options(config_root, secrets)
    assert options["includeHeaders"] == "true"
    # 100 rather than the fixture's platform default: the dev environment file overrides it
    # under `defaults_by_type: kafka:`, which is the layering this option has to survive.
    assert options["maxOffsetsPerTrigger"] == "100"
    assert options["minPartitions"] == "32"
    assert options["failOnDataLoss"] == "true"


def test_include_headers_is_not_configurable(config_root, secrets):
    """Every ce_* column is read from Kafka headers. Turning them off yields NULLs, not an
    error - a table that looks fine and has lost its event metadata - so it is not a knob.
    Setting it in YAML is an unknown key, and the option is still 'true'."""
    from kafka_ingest.sources import kafka

    assert "include_headers" not in kafka.SOURCE_SPEC.structural_keys
    assert _options(config_root, secrets)["includeHeaders"] == "true"


def test_the_batch_cap_is_operationally_overridable(config_root, secrets):
    """The one lever support pulls to drain a backlog in survivable chunks, with no
    deploy - it is ingest_control.kafka_max_offsets_per_trigger."""
    assert _options(config_root, secrets, max_offsets_per_trigger=50000)["maxOffsetsPerTrigger"] == "50000"


def test_data_loss_protection_can_be_switched_off_but_only_in_yaml(config_root, secrets):
    """fail_on_data_loss is STRUCTURAL: accepting silent gaps needs domain sign-off and a
    PR, not a 3am UPDATE. Q14 in sql/03 lists everything currently running with it off."""
    from kafka_ingest.sources import kafka

    assert "fail_on_data_loss" not in kafka.SOURCE_SPEC.operational_keys
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  fail_on_data_loss: false\n")
    assert _options(config_root, secrets)["failOnDataLoss"] == "false"


# --------------------------------------------------------------------------------------
# Connection and identity
# --------------------------------------------------------------------------------------


def test_the_consumer_is_identified_by_prefix_not_by_group_id(config_root, secrets):
    """Spark OVERRIDES kafka.group.id - setting it is a silent no-op, so the framework must
    never appear to set it."""
    options = _options(config_root, secrets)
    assert options["groupIdPrefix"] == "dbx-demo"
    assert "kafka.group.id" not in options


def test_the_topic_is_subscribed_by_name(config_root, secrets):
    assert _options(config_root, secrets)["subscribe"] == "demo.events.v1"


def test_the_broker_endpoint_comes_from_the_environment_not_the_source_file(config_root, secrets):
    assert _options(config_root, secrets)["kafka.bootstrap.servers"] == "dev-broker:9092"


# --------------------------------------------------------------------------------------
# Position
# --------------------------------------------------------------------------------------


def test_a_primary_run_uses_the_configured_starting_offsets(config_root, secrets):
    """Which only matters on the very first run: after that the checkpoint wins."""
    options = _options(config_root, secrets)
    assert options["startingOffsets"] == "latest"
    assert "endingOffsets" not in options


def test_an_offset_replay_sets_the_offsets_json_verbatim(config_root, secrets):
    options = _options(
        config_root, secrets, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", replay_starting_offsets=OFFSETS
    )
    assert options["startingOffsets"] == OFFSETS
    assert "startingTimestamp" not in options


def test_a_timestamp_replay_errors_rather_than_silently_skipping_to_latest(config_root, secrets):
    """A partition with no record at or after the timestamp must fail the run. Advancing to
    latest during incident recovery produces a run that reports success and recovers
    nothing, which is the failure this whole framework is shaped around avoiding."""
    options = _options(
        config_root,
        secrets,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        rerun_id="INC1",
        replay_starting_timestamp="2026-08-11T09:00:00Z",
    )
    assert options["startingTimestamp"] == "1786438800000"
    assert options["startingOffsetsByTimestampStrategy"] == "error"


def test_a_bounded_replay_carries_an_end_position(config_root, secrets):
    options = _options(
        config_root,
        secrets,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        rerun_id="INC1",
        replay_starting_offsets=OFFSETS,
        replay_ending_offsets='{"demo.events.v1": {"0": 900}}',
    )
    assert options["endingOffsets"] == '{"demo.events.v1": {"0": 900}}'


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1786438800000", 1786438800000),
        ("2026-08-11T09:00:00Z", 1786438800000),
        ("2026-08-11T09:00:00+00:00", 1786438800000),
    ],
)
def test_iso_and_epoch_are_both_accepted(value, expected):
    """Support engineers type ISO under pressure; an automated retry carries raw millis."""
    assert to_epoch_millis(value) == expected


def test_a_naive_timestamp_is_treated_as_utc_and_says_so(caplog):
    """Guessing a local zone would move the replay window by hours without telling anyone."""
    with caplog.at_level("WARNING"):
        assert to_epoch_millis("2026-08-11T09:00:00") == 1786438800000
    assert "UTC" in caplog.text


def test_an_unparseable_timestamp_is_rejected():
    with pytest.raises(ConfigError, match="ISO-8601"):
        to_epoch_millis("last tuesday")


# --------------------------------------------------------------------------------------
# Trigger
# --------------------------------------------------------------------------------------


def test_the_default_trigger_is_available_now(config_root):
    """PySpark has no Trigger class - the trigger is a keyword argument, which is why this
    returns a kwargs dict rather than an object."""
    assert resolve_trigger(make_kafka_cfg(config_root)) == {"availableNow": True}


def test_a_processing_time_trigger_is_parsed(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  trigger: processingTime=5 minutes\n")
    assert resolve_trigger(make_kafka_cfg(config_root)) == {"processingTime": "5 minutes"}


def test_an_unknown_trigger_is_rejected_with_the_valid_forms(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  trigger: continuous\n")
    with pytest.raises(ConfigError, match="availableNow"):
        resolve_trigger(make_kafka_cfg(config_root))


# --------------------------------------------------------------------------------------
# Batch reads
# --------------------------------------------------------------------------------------


class _RecordingReader:
    """Records the option chain instead of contacting a broker."""

    def __init__(self):
        self.format_used = None
        self.options = {}
        self.loaded = False

    def format(self, name):
        self.format_used = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        self.loaded = True
        return "dataframe"


class _ReaderSpark:
    def __init__(self):
        self.read = _RecordingReader()
        self.readStream = _RecordingReader()


def test_a_bounded_replay_drops_the_streaming_only_options(config_root, secrets):
    """The streaming Kafka source has no endingOffsets, so a bounded replay is a BATCH
    read - and passing streaming-only options to one is accepted and ignored, which makes
    the plan lie about what it does."""
    from kafka_ingest.sources.kafka.reader import build_batch_reader

    spark = _ReaderSpark()
    cfg = make_kafka_cfg(
        config_root,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        rerun_id="INC1",
        replay_starting_offsets=OFFSETS,
        replay_ending_offsets='{"demo.events.v1": {"0": 900}}',
    )
    build_batch_reader(spark, cfg, secrets)
    assert spark.read.format_used == "kafka"
    for streaming_only in ("groupIdPrefix", "maxOffsetsPerTrigger", "minPartitions"):
        assert streaming_only not in spark.read.options
    # includeHeaders is NOT streaming-only - a replay's landing rows need ce_* columns too.
    assert spark.read.options["includeHeaders"] == "true"
    assert spark.read.options["endingOffsets"]


def test_an_unbounded_run_refuses_the_batch_reader(config_root, secrets):
    from kafka_ingest.sources.kafka.reader import build_batch_reader

    cfg = make_kafka_cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", replay_starting_offsets=OFFSETS)
    with pytest.raises(ConfigError, match="build_stream_reader"):
        build_batch_reader(_ReaderSpark(), cfg, secrets)


def test_the_streaming_reader_keeps_every_option(config_root, secrets):
    from kafka_ingest.sources.kafka.reader import build_stream_reader

    spark = _ReaderSpark()
    build_stream_reader(spark, make_kafka_cfg(config_root), secrets)
    assert spark.readStream.format_used == "kafka"
    assert spark.readStream.options["minPartitions"] == "32"
    assert spark.readStream.loaded is True


def test_no_credential_reaches_a_logged_options_map(config_root, secrets, caplog):
    """The INFO line this emits is the one a support engineer reads during an incident, and
    it is built from the same map that carries the JAAS string."""
    with caplog.at_level("INFO"):
        _options(config_root, secrets)
    assert "***REDACTED***" in caplog.text
    assert "value" not in caplog.text.split("Kafka options")[1].split("\n")[0].replace("demo.events.v1-value", "")
