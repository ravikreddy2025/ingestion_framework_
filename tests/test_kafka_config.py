"""sources/kafka/config.py - the value-level rules the spec cannot express.

The framework validates KEYS against SOURCE_SPEC (tests/test_framework_config.py covers
that machinery). This file covers what only the source can know: which values are legal,
which combinations are, and the three derived properties that decide whether a replay can
collide with the primary stream.
"""

from __future__ import annotations

import pytest

from conftest import make_kafka_cfg, make_kafka_ctx
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.kafka import config as kafka_config
from kafka_ingest.sources.kafka.config import (
    RUN_TYPE_CURATED_REPLAY,
    RUN_TYPE_KAFKA_REPLAY,
    table_name_for,
)

OFFSETS = '{"demo.events.v1": {"0": 100, "1": 250}}'


def _with_yaml(config_root, **settings):
    """Add settings to the source's YAML and resolve.

    STRUCTURAL settings have to arrive this way. A job parameter naming one is IGNORED by
    design (framework/config.py), so a test that set `reader_schema_mode` as a parameter
    would silently assert nothing - which is the same trap the ignore rule protects the
    real system from.
    """
    lines = "".join(f"  {key}: {value}\n" for key, value in settings.items())
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n" + lines)
    return make_kafka_cfg(config_root)


# --------------------------------------------------------------------------------------
# Table naming
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "topic, expected",
    [
        ("vector.patient.events.v1", "vector_patient_events_v1"),
        ("rcm-claim-status.v2", "rcm_claim_status_v2"),
        ("plain", "plain"),
    ],
)
def test_a_table_name_is_derived_from_the_kafka_topic(topic, expected):
    assert table_name_for(topic) == expected


def test_an_underivable_topic_name_fails_loudly_rather_than_being_mangled():
    with pytest.raises(ConfigError, match="table_name"):
        table_name_for("9lives.events")


def test_all_three_layers_use_the_same_derived_name(config_root):
    cfg = make_kafka_cfg(config_root)
    for table in (cfg.landing_table, cfg.curated_table):
        assert table.endswith("demo_events_v1")
    assert cfg.quarantine_table.endswith("demo_events_v1_quarantine")


def test_a_source_can_override_the_derived_name_without_naming_a_catalog(config_root, tmp_path):
    """`table_name:` overrides the NAME only - the catalog and schema still come from
    conf/defaults, which is what keeps a catalog out of a source file."""
    path = f"{config_root}/sources/demo_topic.yaml"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n  table_name: renamed_events\n")
    cfg = make_kafka_cfg(config_root)
    assert cfg.landing_table == "cat_dev.landing.renamed_events"
    assert cfg.curated_table == "cat_dev.curated.renamed_events"


# --------------------------------------------------------------------------------------
# Value-level validation
# --------------------------------------------------------------------------------------


def test_the_reader_schema_mode_that_would_break_the_payload_struct_does_not_exist(config_root):
    """Two writer versions decoded without a common reader schema produce two struct types
    that cannot share a table. There is no third mode, and asking for one says so."""
    with pytest.raises(ConfigError, match="no mode that decodes"):
        _with_yaml(config_root, reader_schema_mode="writer")


def test_pinning_the_reader_schema_requires_an_id(config_root):
    with pytest.raises(ConfigError, match="requires reader_schema_id"):
        _with_yaml(config_root, reader_schema_mode="pinned_id")


def test_the_failure_mode_values_are_the_ones_the_control_column_allows(config_root):
    """The setting and ingest_control.kafka_failure_mode are the same lever. Accepting a
    spelling the column's CHECK constraint rejects would mean support setting a value that
    looks legal and does nothing."""
    with pytest.raises(ConfigError, match="FAILFAST"):
        make_kafka_cfg(config_root, failure_mode="quarantine")
    assert make_kafka_cfg(config_root, failure_mode="QUARANTINE").quarantine_on_error is True


def test_a_checkpoint_root_that_is_not_volume_backed_is_refused(config_root):
    with pytest.raises(ConfigError, match="Volume-backed"):
        _with_yaml(config_root, checkpoint_root="/tmp/checkpoints")


@pytest.mark.parametrize("value", [0, -1])
def test_an_unusable_batch_cap_is_refused_rather_than_treated_as_no_limit(config_root, value):
    """Zero is not "unlimited" - it is a value nothing can act on. Unset would mean one
    unbounded microbatch, which is exactly what this setting exists to prevent."""
    with pytest.raises(ConfigError, match="max_offsets_per_trigger"):
        make_kafka_cfg(config_root, max_offsets_per_trigger=value)


def test_a_non_numeric_batch_cap_is_refused_too(config_root):
    """Separate from the case above because it arrives by a different route: a job
    parameter is coerced to the type of the value it replaces, so a non-numeric one fails
    in the framework's coercion rather than in this module's own check. Both paths must
    refuse it; only the message differs."""
    with pytest.raises((ConfigError, ValueError)):
        _with_yaml(config_root, max_offsets_per_trigger="not-a-number")


def test_min_partitions_must_be_a_real_number_too(config_root):
    with pytest.raises(ConfigError, match="min_partitions"):
        _with_yaml(config_root, min_partitions=0)


# --------------------------------------------------------------------------------------
# Replay controls
# --------------------------------------------------------------------------------------


def test_a_replay_without_a_rerun_id_is_refused(config_root):
    with pytest.raises(ConfigError, match="rerun_id"):
        make_kafka_cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, replay_starting_offsets=OFFSETS)


def test_a_kafka_replay_needs_a_start_position(config_root):
    with pytest.raises(ConfigError, match="exactly one of"):
        make_kafka_cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1")


def test_offsets_and_timestamp_are_mutually_exclusive(config_root):
    """Rejected by the SPEC, not by this module - the pair is declared data, so the
    framework catches it the same way it catches an Oracle one."""
    with pytest.raises(ConfigError, match="mutually exclusive"):
        make_kafka_cfg(
            config_root,
            run_type=RUN_TYPE_KAFKA_REPLAY,
            rerun_id="INC1",
            replay_starting_offsets=OFFSETS,
            replay_starting_timestamp="2026-08-11T09:00:00Z",
        )


@pytest.mark.parametrize(
    "raw, message",
    [
        ("not json at all", "not valid JSON"),
        ("[]", "non-empty JSON object"),
        ('{"t": 5}', "object of partition"),
        ('{"t": {"0": "abc"}}', "not an int offset"),
    ],
)
def test_malformed_replay_offsets_are_rejected_before_the_broker_sees_them(config_root, raw, message):
    """Spark's own error for a malformed offsets string names neither the option nor the
    value, which during an incident reads as "the replay is broken"."""
    with pytest.raises(ConfigError, match=message):
        make_kafka_cfg(config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", replay_starting_offsets=raw)


def test_a_rerun_id_must_be_path_safe_because_it_becomes_a_path(config_root):
    with pytest.raises(ConfigError, match="path segment"):
        make_kafka_cfg(
            config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="../../etc", replay_starting_offsets=OFFSETS
        )


def test_a_curated_replay_will_not_re_parse_all_of_history_by_accident(config_root):
    with pytest.raises(ConfigError, match="refusing to re-parse"):
        make_kafka_cfg(config_root, run_type=RUN_TYPE_CURATED_REPLAY, rerun_id="FIX1")


def test_a_bounded_replay_is_one_with_an_explicit_end(config_root):
    """The streaming Kafka source has no ending-offset option, so an explicit end is what
    drops the run to a batch read."""
    unbounded = make_kafka_cfg(
        config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", replay_starting_offsets=OFFSETS
    )
    assert unbounded.replay.is_bounded is False
    bounded = make_kafka_cfg(
        config_root,
        run_type=RUN_TYPE_KAFKA_REPLAY,
        rerun_id="INC1",
        replay_starting_offsets=OFFSETS,
        replay_ending_offsets='{"demo.events.v1": {"0": 900}}',
    )
    assert bounded.replay.is_bounded is True


def test_an_incident_lever_cannot_be_checked_into_yaml(config_root):
    """checkpoint_reset_id is operational-ONLY: a value in Git would silently re-apply on
    every future deploy, bypassing the guard against silent data loss forever."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  checkpoint_reset_id: INC-1042\n")
    with pytest.raises(ConfigError, match="operational-only"):
        make_kafka_cfg(config_root)


# --------------------------------------------------------------------------------------
# The derived properties that keep a replay off the primary lineage
# --------------------------------------------------------------------------------------


def _replay(config_root, rerun_id):
    return make_kafka_cfg(
        config_root, run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id=rerun_id, replay_starting_offsets=OFFSETS
    )


def test_primary_and_replay_checkpoints_can_never_collide(config_root):
    primary = make_kafka_cfg(config_root)
    replay = _replay(config_root, "INC1")
    assert primary.checkpoint_path.endswith("/demo_topic/primary")
    assert replay.checkpoint_path.endswith("/demo_topic/replay/INC1")
    assert primary.checkpoint_path != replay.checkpoint_path


def test_two_replays_of_one_source_are_isolated_from_each_other(config_root):
    assert _replay(config_root, "INC1").checkpoint_path != _replay(config_root, "INC2").checkpoint_path
    assert _replay(config_root, "INC1").txn_app_id != _replay(config_root, "INC2").txn_app_id


def test_the_txn_app_id_is_stable_across_calls(config_root):
    """Delta dedups a retried batch against this id. Anything per-run in it would make
    every restart a new writer and defeat the mechanism entirely."""
    assert make_kafka_cfg(config_root).txn_app_id == make_kafka_cfg(config_root).txn_app_id


def test_a_replay_never_shares_the_primary_transaction_identity(config_root):
    assert make_kafka_cfg(config_root).txn_app_id != _replay(config_root, "INC1").txn_app_id


def test_a_checkpoint_reset_forks_the_primary_transaction_identity(config_root):
    """This is what makes the reset SAFE rather than merely silencing the guard: a fresh
    identity has no committed versions for Delta to skip writes against."""
    plain = make_kafka_cfg(config_root)
    reset = make_kafka_cfg(config_root, checkpoint_reset_id="INC-1042")
    assert reset.txn_app_id != plain.txn_app_id
    assert "INC-1042" in reset.txn_app_id
    # The checkpoint PATH is unchanged - a reset restarts the same stream, it does not
    # move it somewhere new.
    assert reset.checkpoint_path == plain.checkpoint_path


def test_a_reset_id_must_be_path_safe_because_it_becomes_part_of_that_identity(config_root):
    with pytest.raises(ConfigError, match="transaction identity"):
        make_kafka_cfg(config_root, checkpoint_reset_id="INC 1042; DROP")


def test_a_replay_is_distinguishable_on_the_broker(config_root):
    """Spark overrides kafka.group.id, so groupIdPrefix is the only knob - and a replay
    sharing the primary's prefix would blend the two in broker-side consumer metrics."""
    assert _replay(config_root, "INC1").group_id_prefix == "dbx-demo-kafka_replay-INC1"
    assert make_kafka_cfg(config_root).group_id_prefix == "dbx-demo"


def test_a_primary_run_carries_no_replay_metadata(config_root):
    cfg = make_kafka_cfg(config_root)
    assert cfg.is_replay is False
    assert cfg.replay.rerun_id is None
    assert cfg.ingested_via == "primary"


def test_an_unknown_run_type_is_refused(config_root):
    ctx = make_kafka_ctx(config_root)
    with pytest.raises(ConfigError, match="run_type"):
        kafka_config.build(ctx.cfg, "sideways_replay", ctx.tables)


# --------------------------------------------------------------------------------------
# What the audit row carries about this source
# --------------------------------------------------------------------------------------


def test_source_detail_answers_the_questions_support_asks_from_the_audit_table(config_root):
    """Each of these is otherwise answerable only by reading a Git branch that may since
    have moved on - and fail_on_data_loss is the one a standing health check queries."""
    import json

    detail = json.loads(make_kafka_cfg(config_root).source_detail())
    assert detail["topic"] == "demo.events.v1"
    assert detail["fail_on_data_loss"] is True
    assert detail["checkpoint_path"].endswith("/demo_topic/primary")
    assert detail["cluster"] == "cc_shared"
    assert detail["failure_mode"] == "FAILFAST"


def test_source_detail_never_carries_a_credential(config_root):
    """It is a JSON string on a table support can read. Endpoints and profile NAMES are the
    point of it; a secret scope, a resolved secret or a JAAS string are not.

    Note what IS allowed through: the subject name `demo.events.v1-value` contains the word
    "value" and is exactly the kind of thing this column exists to carry. A test that
    grepped for suspicious words rather than for the actual leak would have to be weakened
    the first time a topic was named badly, which is how a check like this dies."""
    detail = make_kafka_cfg(config_root).source_detail()
    assert "kv-dev" not in detail, "a secret SCOPE name reached the audit table"
    assert "/value" not in detail, "a resolved secret reached the audit table"
    for leaked in ("password", "jaas", "secret"):
        assert leaked not in detail.lower()
