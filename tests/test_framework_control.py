"""The operational control table: layer 4, and the hole it must not leave in validation.

Three rules, each with a test, and the third is the one most likely to be skipped:

  * a MISSING row is not an error - it means "no overrides", so a newly onboarded source
    runs the moment its YAML merges;
  * DUPLICATE rows are an error naming the source_key, because two rows means two answers
    to "is this source enabled?";
  * every override is VALIDATED against the source's SOURCE_SPEC, so a typo in the
    `replay_controls` JSON fails exactly the way a YAML typo does instead of parsing,
    merging and doing nothing at all.

Also covers docs/build_log/DECISIONS.md D-01: prefixed, per-source-type control columns
replacing the old shared `failure_mode` / `batch_limit` columns and the JSON
`source_overrides` escape hatch, and the wrong-source-type error that catches a column set
on a row it does not belong to.

Needs no Spark: framework/control.py takes the session as an argument and imports no
PySpark.
"""

from __future__ import annotations

import pytest

from conftest import FakeSpark
from kafka_ingest.framework import control
from kafka_ingest.framework.config import ConfigError

CONTROL_TABLE = "ops_prod.ingestion.ingest_control"


def _spark(rows, exists=True):
    return FakeSpark(
        existing_tables={CONTROL_TABLE} if exists else set(),
        rows_by_table={CONTROL_TABLE: rows},
    )


def _read(rows, spec, exists=True):
    return control.read_control(_spark(rows, exists), CONTROL_TABLE, "demo_source", spec)


# --------------------------------------------------------------------------------------
# Rule 1: a missing row is not an error
# --------------------------------------------------------------------------------------


def test_no_row_means_no_overrides(demo_spec):
    """A source with no control row runs on its YAML. Requiring an INSERT before a new
    source could run would make onboarding a two-step operation nobody remembers."""
    assert _read([], demo_spec) == {}


def test_no_control_table_at_all_still_runs(demo_spec, caplog):
    """A fresh environment where the support table has not been provisioned yet. The
    control table exists to CHANGE behaviour; its absence means nobody has changed any."""
    with caplog.at_level("WARNING"):
        assert _read([], demo_spec, exists=False) == {}
    assert "does not exist" in caplog.text


# --------------------------------------------------------------------------------------
# Rule 2: duplicate rows are an error
# --------------------------------------------------------------------------------------


def test_duplicate_rows_raise_and_name_the_source_key(demo_spec):
    rows = [{"source_key": "demo_source"}, {"source_key": "demo_source"}]
    with pytest.raises(ConfigError, match="rows for source_key 'demo_source'"):
        _read(rows, demo_spec)


# --------------------------------------------------------------------------------------
# Rule 3: everything is validated against the spec
# --------------------------------------------------------------------------------------


def test_an_unknown_key_in_replay_controls_is_rejected(demo_spec):
    """The rule most likely to be skipped, and skipping it puts a hole in the middle of the
    validation everything else is careful about."""
    rows = [{"source_key": "demo_source", "replay_controls": '{"btach_limit": 100}'}]
    with pytest.raises(ConfigError, match=r"unknown keys \['btach_limit'\]"):
        _read(rows, demo_spec)


def test_the_unknown_key_error_reads_the_same_as_a_yaml_typo(demo_spec, demo_config_root):
    """Same message shape, so a support engineer who has seen one recognises the other."""
    from kafka_ingest.framework.config import resolve_config

    rows = [{"source_key": "demo_source", "replay_controls": '{"nonsense": 1}'}]
    with pytest.raises(ConfigError) as from_control:
        _read(rows, demo_spec)
    with pytest.raises(ConfigError) as from_yaml:
        resolve_config(demo_config_root, "demo_source", "prod", demo_spec, job_parameters={"nonsense": 1})

    for error in (from_control, from_yaml):
        assert "contains unknown keys ['nonsense']" in str(error.value)
        assert "Known keys:" in str(error.value)


def test_malformed_json_says_which_column_and_which_source(demo_spec):
    rows = [{"source_key": "demo_source", "replay_controls": "{not json"}]
    with pytest.raises(ConfigError, match="replay_controls for source_key 'demo_source' is not valid JSON"):
        _read(rows, demo_spec)


def test_json_that_is_not_an_object_is_rejected(demo_spec):
    rows = [{"source_key": "demo_source", "replay_controls": "[1, 2]"}]
    with pytest.raises(ConfigError, match="parsed as list"):
        _read(rows, demo_spec)


# --------------------------------------------------------------------------------------
# What the row turns into
# --------------------------------------------------------------------------------------


def test_the_named_columns_become_settings(demo_spec):
    """`enabled` / `replay_rerun_id` are framework-owned and stay bare; `failure_mode` /
    `batch_limit` are this source type's OWN levers and are read from ITS prefixed columns
    (docs/build_log/DECISIONS.md D-01), not from a column called the same as the setting."""
    rows = [
        {
            "source_key": "demo_source",
            "source_type": "demo",
            "enabled": False,
            "demo_failure_mode": "QUARANTINE",
            "demo_batch_limit": 500,
            "replay_rerun_id": "INC42",
        }
    ]
    assert _read(rows, demo_spec) == {
        "enabled": False,
        "failure_mode": "QUARANTINE",
        "batch_limit": 500,
        "rerun_id": "INC42",
    }


def test_null_columns_are_not_overrides(demo_spec):
    """A NULL means "not set here", not "set to nothing" - otherwise every unset column
    would blank out the YAML value beneath it."""
    rows = [{"source_key": "demo_source", "enabled": None, "demo_batch_limit": 500}]
    assert _read(rows, demo_spec) == {"batch_limit": 500}


def test_attribution_and_notes_are_ignored(demo_spec):
    """The job reads the columns it knows and ignores the rest, so an admin adding a column
    for their own use does not break every run."""
    rows = [
        {
            "source_key": "demo_source",
            "notes": "raised under INC42",
            "updated_by": "someone",
            "updated_at": "2026-08-11",
            "an_admin_added_this": "hello",
        }
    ]
    assert _read(rows, demo_spec) == {}


def test_an_operational_key_with_no_control_column_cannot_be_set_from_the_control_table(demo_spec):
    """`trigger` is operational on demo_spec but was deliberately given no prefixed column
    (mirroring Kafka's real shape) - not every operational key needs one, and one with none
    is simply invisible to this table, not a bug. A bare `trigger` column is just an
    unrecognised column, silently ignored like any other."""
    rows = [{"source_key": "demo_source", "trigger": "once"}]
    assert _read(rows, demo_spec) == {}


def test_replay_controls_json_is_merged_in(demo_spec):
    rows = [{"source_key": "demo_source", "replay_controls": '{"trigger": "once"}'}]
    assert _read(rows, demo_spec) == {"trigger": "once"}


def test_replay_controls_win_over_a_named_column(demo_spec):
    """An incident-scoped replay setting should beat a standing override parked in a named
    control column."""
    rows = [
        {
            "source_key": "demo_source",
            "demo_batch_limit": 100,
            "replay_controls": '{"batch_limit": 5}',
        }
    ]
    assert _read(rows, demo_spec) == {"batch_limit": 5}


def test_a_structural_key_is_carried_through_and_ignored_later(demo_spec, demo_config_root):
    """Structural fields are IGNORED, not rejected - exactly as the YAML layers behave.
    control.py carries the key; config.py drops it and says so in the log."""
    from kafka_ingest.framework.config import resolve_config

    rows = [{"source_key": "demo_source", "replay_controls": '{"partition_by": ["nonsense"]}'}]
    overrides = _read(rows, demo_spec)
    assert overrides == {"partition_by": ["nonsense"]}

    cfg = resolve_config(demo_config_root, "demo_source", "prod", demo_spec, control=overrides)
    assert cfg.get("partition_by") == ["ingest_date"]


# --------------------------------------------------------------------------------------
# D-01 point 4: a column belonging to a DIFFERENT source type is an error, not a silent
# ignore. `other_control_columns` is what framework/runner.py builds from every known
# SOURCE_SPEC.control_columns and passes in - these tests supply it directly.
# --------------------------------------------------------------------------------------


def test_a_column_belonging_to_a_different_source_type_is_rejected(demo_spec):
    """A copy-pasted row, or a typo in the prefix, must fail loudly rather than silently do
    nothing."""
    rows = [{"source_key": "demo_source", "source_type": "demo", "other_reset_id": "INC1"}]
    with pytest.raises(ConfigError, match=r"'other_reset_id' is set for source_key 'demo_source'.*belongs to"):
        control.read_control(_spark(rows), CONTROL_TABLE, "demo_source", demo_spec, {"other_reset_id": "other_type"})


def test_a_column_this_spec_also_declares_is_exempt_from_the_foreign_check(demo_spec):
    """Two source types happening to declare the identical column name is not, on its own,
    a mistake on THIS row - only a column neither this spec nor the row's real type owns
    is."""
    rows = [{"source_key": "demo_source", "demo_batch_limit": 7}]
    overrides = control.read_control(
        _spark(rows), CONTROL_TABLE, "demo_source", demo_spec, {"demo_batch_limit": "demo"}
    )
    assert overrides == {"batch_limit": 7}


def test_a_spec_with_no_control_columns_declared_defaults_to_empty():
    """A source type with no operational levers must not have to declare an empty mapping.

    Asserted against a SourceSpec built here rather than against a shipped stub: the two
    stubs this used to use are being filled in by Stages 4 and 5, and a test that fails
    when a source type acquires its first control column is testing the wrong thing.
    """
    from kafka_ingest.framework.contracts import SourceSpec

    spec = SourceSpec(
        source_type="leverless",
        required_keys=frozenset(),
        structural_keys=frozenset(),
        operational_keys=frozenset(),
        mutually_exclusive=(),
        layers=("landing",),
    )
    assert dict(spec.control_columns) == {}
    rows = [{"source_key": "demo_source", "enabled": False}]
    assert control.read_control(_spark(rows), CONTROL_TABLE, "demo_source", spec) == {"enabled": False}


def test_the_shipped_kafka_spec_declares_its_control_columns():
    """docs/build_log/DECISIONS.md D-01's settled column shape for Kafka's own levers."""
    from kafka_ingest.sources.kafka import SOURCE_SPEC

    assert dict(SOURCE_SPEC.control_columns) == {
        "kafka_failure_mode": "failure_mode",
        "kafka_max_offsets_per_trigger": "max_offsets_per_trigger",
        "kafka_checkpoint_reset_id": "checkpoint_reset_id",
    }


def test_a_row_declaring_the_wrong_source_type_is_an_error(demo_spec):
    """The column exists so support can query the table by feed type. Letting it disagree
    with reality would make every such query quietly wrong."""
    rows = [{"source_key": "demo_source", "source_type": "kafka"}]
    with pytest.raises(ConfigError, match="declares source_type 'kafka'"):
        _read(rows, demo_spec)


def test_a_source_key_that_did_not_come_from_a_filename_is_rejected(demo_spec):
    """source_key is interpolated into the WHERE clause. It is a deployed YAML filename
    stem, never free-form input."""
    with pytest.raises(ConfigError, match="not allowed"):
        control.read_control(_spark([]), CONTROL_TABLE, "x'; DROP TABLE y --", demo_spec)
