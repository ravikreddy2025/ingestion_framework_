"""Source positioning: what actually gets handed to the Kafka connector.

These assertions are the guard against the two failure modes that look like success:
a replay that silently resumes from a checkpoint, and a timestamp replay that silently
jumps to latest for partitions with no matching record.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="kafka_source imports pyspark.sql")

from conftest import FakeSpark
from kafka_ingest.config import (
    RUN_TYPE_KAFKA_REPLAY,
    ConfigError,
    resolve_topic_config,
)
from kafka_ingest.kafka_source import (
    build_source_options,
    resolve_trigger,
    to_epoch_millis,
)


def _cfg(config_root, **overrides):
    run_type = overrides.pop("run_type", "primary")
    return resolve_topic_config(
        FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
        run_type=run_type, overrides=overrides,
    )


# --------------------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------------------


def _millis(iso: str) -> int:
    """Expected value computed, not hardcoded - a wrong constant here would 'prove' a bug."""
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


REPLAY_START_ISO = "2026-08-11T09:00:00+00:00"
REPLAY_END_ISO = "2026-08-11T15:00:00+00:00"


def test_iso_and_epoch_are_both_accepted():
    assert to_epoch_millis("1754902800000") == 1754902800000
    assert to_epoch_millis("2026-08-11T09:00:00Z") == _millis(REPLAY_START_ISO)


def test_naive_timestamp_is_treated_as_utc():
    assert to_epoch_millis("2026-08-11T09:00:00") == to_epoch_millis("2026-08-11T09:00:00Z")


def test_unparseable_timestamp_is_rejected():
    with pytest.raises(ConfigError, match="neither epoch millis nor an ISO-8601"):
        to_epoch_millis("yesterday-ish")


# --------------------------------------------------------------------------------------
# Positioning
# --------------------------------------------------------------------------------------


def test_primary_run_uses_the_configured_starting_offsets(config_root):
    options = build_source_options(_cfg(config_root), _FakeSecrets())
    assert options["startingOffsets"] == "earliest"
    assert "startingTimestamp" not in options
    assert options["failOnDataLoss"] == "true"
    assert options["subscribe"] == "demo.events.v1"


def test_offset_replay_sets_starting_offsets_json(config_root):
    cfg = _cfg(
        config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
        starting_offsets='{"demo.events.v1":{"0":100,"1":250}}',
    )
    options = build_source_options(cfg, _FakeSecrets())
    assert options["startingOffsets"] == '{"demo.events.v1":{"0":100,"1":250}}'
    assert "startingTimestamp" not in options


def test_timestamp_replay_errors_rather_than_skipping_to_latest(config_root):
    """A partition with no record at/after the timestamp must fail the run.

    Silently advancing to latest during incident recovery produces a run that reports
    success while having recovered nothing.
    """
    cfg = _cfg(
        config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
        starting_timestamp="2026-08-11T09:00:00Z",
    )
    options = build_source_options(cfg, _FakeSecrets())
    assert options["startingTimestamp"] == str(_millis(REPLAY_START_ISO))
    assert options["startingOffsetsByTimestampStrategy"] == "error"


def test_bounded_replay_carries_an_end_position(config_root):
    cfg = _cfg(
        config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
        starting_timestamp="2026-08-11T09:00:00Z", ending_timestamp="2026-08-11T15:00:00Z",
    )
    options = build_source_options(cfg, _FakeSecrets())
    assert options["endingTimestamp"] == str(_millis(REPLAY_END_ISO))
    assert cfg.run.is_bounded


def test_group_id_prefix_is_set_not_group_id(config_root):
    """Spark overrides kafka.group.id; setting it would be a silent no-op."""
    options = build_source_options(_cfg(config_root), _FakeSecrets())
    assert options["groupIdPrefix"] == "dbx-demo"
    assert "kafka.group.id" not in options


def test_max_offsets_per_trigger_is_forwarded_when_set(config_root):
    options = build_source_options(_cfg(config_root, max_offsets_per_trigger=500000), _FakeSecrets())
    assert options["maxOffsetsPerTrigger"] == "500000"


# --------------------------------------------------------------------------------------
# Triggers
# --------------------------------------------------------------------------------------


def test_default_trigger_is_available_now(config_root):
    assert resolve_trigger(_cfg(config_root)) == {"availableNow": True}


def test_processing_time_trigger_is_parsed(config_root):
    assert resolve_trigger(_cfg(config_root, trigger="processingTime=5 minutes")) == {
        "processingTime": "5 minutes"
    }


def test_unknown_trigger_is_rejected(config_root):
    with pytest.raises(ConfigError, match="not understood"):
        resolve_trigger(_cfg(config_root, trigger="hourly-ish"))


class _FakeSecrets:
    def get(self, scope, key):
        return f"{scope}/{key}"
