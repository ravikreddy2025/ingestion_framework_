"""sources/kafka/spec.py - the declaration the framework validates every layer against.

SOURCE_SPEC is data, and the framework reads it instead of branching on source type. That
makes it powerful and makes it dangerous in exactly one way: a key declared here that
nothing reads is a setting that silently does nothing, which CORE section 2 rule 2 ranks as
the worst possible output of this project. So the first test in this file walks the real
package and asserts the declaration and the code agree, in both directions.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from conftest import make_kafka_ctx
from kafka_ingest.framework.config import FRAMEWORK_STRUCTURAL_KEYS, known_keys
from kafka_ingest.framework.control import read_control
from kafka_ingest.sources import kafka
from kafka_ingest.sources.kafka import spec as kafka_spec

SPEC = kafka.SOURCE_SPEC
PACKAGE = pathlib.Path(kafka.__file__).parent


def _package_source() -> str:
    """Every .py file in the source package, concatenated. Read once, searched many."""
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(PACKAGE.glob("*.py")))


@pytest.mark.parametrize("key", sorted(SPEC.structural_keys | SPEC.operational_keys))
def test_every_declared_key_is_actually_read_by_this_package(key):
    """The half that matters most. A key nobody reads is a lever a support engineer will
    pull during an incident, expecting something to happen."""
    source = _package_source()
    assert re.search(rf'["\']{re.escape(key)}["\']|\b{re.escape(key)}\b', source), (
        f"'{key}' is declared in SOURCE_SPEC but appears nowhere in {PACKAGE.name}/"
    )


def test_the_layers_are_the_three_tables_this_source_writes():
    """framework/tables.py derives `<layer>_table` from this, so the layer list IS the set
    of target settings the framework will resolve, validate and create."""
    assert SPEC.layers == ("landing", "curated", "quarantine")
    assert {"landing_table", "curated_table", "quarantine_table"} <= known_keys(SPEC)


def test_the_framework_owned_keys_are_not_redeclared_here():
    """audit_table, state_table, control_table, domain, enabled and table_properties are
    read by the framework and by no source. A source that declared them could misspell one
    into silence."""
    assert not (SPEC.structural_keys & FRAMEWORK_STRUCTURAL_KEYS)


# --------------------------------------------------------------------------------------
# Structural vs operational
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "landing_partition_by",
        "curated_partition_by",
        "curated_dedup_keys",
        "topic",
        "table_name",
        "cluster",
        "registry",
        "subject",
        "checkpoint_root",
    ],
)
def test_structural_fields_are_not_operationally_overridable(key):
    """Partitioning and dedup keys describe what is already ON DISK; the topic, the cluster
    and the target name describe WHICH FEED THIS IS. Neither is a runtime toggle, and an
    override of one is ignored rather than applied - framework/config.py logs it."""
    assert key in SPEC.structural_keys
    assert key not in SPEC.operational_keys


@pytest.mark.parametrize(
    "key",
    [
        kafka_spec.CHECKPOINT_RESET_ID,
        kafka_spec.REPLAY_STARTING_OFFSETS,
        kafka_spec.REPLAY_STARTING_TIMESTAMP,
        kafka_spec.REPLAY_ENDING_OFFSETS,
        kafka_spec.REPLAY_ENDING_TIMESTAMP,
        kafka_spec.REPLAY_LANDING_FILTER,
    ],
)
def test_incident_scoped_levers_are_operational_only(key):
    """Each is scoped to ONE incident. A value checked into Git would silently re-apply on
    every future deploy - which for the reset id means bypassing the guard against silent
    data loss forever, long after the incident that justified it."""
    assert key in SPEC.operational_keys
    assert key not in SPEC.structural_keys


@pytest.mark.parametrize("key", ["failure_mode", "max_offsets_per_trigger"])
def test_the_two_standing_levers_are_settable_in_both_places(key):
    """A reviewed platform default in Git AND an incident override with no deploy. These are
    the only two keys that need both, and they are the two support actually pulls."""
    assert key in SPEC.structural_keys
    assert key in SPEC.operational_keys


def test_the_replay_bound_pairs_are_declared_mutually_exclusive():
    """Setting both an offset and a timestamp start is a contradiction the SPEC catches, so
    the error reads the same as it would for any other source type."""
    pairs = {frozenset(group) for group in SPEC.mutually_exclusive}
    assert frozenset({kafka_spec.REPLAY_STARTING_OFFSETS, kafka_spec.REPLAY_STARTING_TIMESTAMP}) in pairs
    assert frozenset({kafka_spec.REPLAY_ENDING_OFFSETS, kafka_spec.REPLAY_ENDING_TIMESTAMP}) in pairs


# --------------------------------------------------------------------------------------
# The control table (docs/build_log/DECISIONS.md D-01)
# --------------------------------------------------------------------------------------


def test_every_control_column_maps_to_a_setting_that_is_operationally_overridable():
    """THE POINT OF THIS FILE'S EXISTENCE, in one test.

    A column mapping to a setting that is not in operational_keys fails at run time with an
    unknown-key error - honest, but only discovered by a support engineer mid-incident. Both
    halves have to agree here, and until Stage 3 they deliberately did not.
    """
    for column, setting in SPEC.control_columns.items():
        assert column.startswith("kafka_"), f"'{column}' does not carry its source type's prefix"
        assert setting in SPEC.operational_keys, f"control column '{column}' maps to '{setting}', which nothing reads"


def test_the_control_columns_are_the_three_levers_support_has():
    """Named explicitly so that adding a fourth is a deliberate edit here AND an ALTER TABLE
    in sql/01 - there is no free-form JSON escape hatch any more."""
    assert SPEC.control_columns == {
        "kafka_failure_mode": "failure_mode",
        "kafka_max_offsets_per_trigger": "max_offsets_per_trigger",
        "kafka_checkpoint_reset_id": "checkpoint_reset_id",
    }


def test_a_control_row_setting_all_three_columns_resolves_end_to_end(config_root):
    """Through framework/control.py and framework/config.py, into the source's own config.

    This is the whole chain a support UPDATE travels, and every link in it was in place
    before the settings on the far end existed.
    """
    from conftest import FakeSpark
    from kafka_ingest.framework import tables
    from kafka_ingest.framework.config import resolve_config
    from kafka_ingest.sources.kafka import config as kafka_config

    row = {
        "source_key": "demo_topic",
        "source_type": "kafka",
        "kafka_failure_mode": "QUARANTINE",
        "kafka_max_offsets_per_trigger": 50000,
        "kafka_checkpoint_reset_id": "INC-1042",
    }
    table = "ops.ingestion.ingest_control"
    overrides = read_control(FakeSpark([row], existing_tables=(table,)), table, "demo_topic", SPEC)
    resolved = resolve_config(config_root, "demo_topic", "dev", SPEC, control=overrides)
    cfg = kafka_config.build(resolved, "primary", tables)

    assert cfg.failure_mode == "QUARANTINE"
    assert cfg.max_offsets_per_trigger == 50000
    assert cfg.checkpoint_reset_id == "INC-1042"
    # And the reset id reached the transaction identity, which is what makes it do anything.
    assert "INC-1042" in cfg.txn_app_id


def test_the_shipped_source_exposes_exactly_the_contract():
    """SOURCE_SPEC and run(), and nothing else. A source that grew a read()/parse()/write()
    surface would have broken the one abstraction this design is built on."""
    assert set(kafka.__all__) == {"SOURCE_SPEC", "run"}
    assert not {"read", "parse", "write", "validate"} & set(dir(kafka))


def test_the_context_is_the_whole_interface(config_root):
    """A source is handed a RunContext and nothing else. If this ever needs more, the honest
    answer is usually that the source should build it itself - as it does for secrets."""
    ctx = make_kafka_ctx(config_root)
    assert ctx.cfg.source_type == "kafka"
    assert ctx.run_type == "primary"
    assert ctx.run_sequence == 1
