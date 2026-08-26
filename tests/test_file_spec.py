"""sources/file/spec.py - the declaration the framework validates every layer against.

SOURCE_SPEC is data, and the framework reads it instead of branching on source type. That
makes it powerful and makes it dangerous in exactly one way: a key declared here that
nothing reads is a setting that silently does nothing, which CORE section 2 rule 2 ranks as
the worst possible output of this project. So the first test in this file walks the real
package and asserts the declaration and the code agree.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from conftest import make_file_cfg, write_file_source
from kafka_ingest.framework.config import FRAMEWORK_STRUCTURAL_KEYS, known_keys, resolve_config
from kafka_ingest.framework.control import read_control
from kafka_ingest.sources import file as file_source
from kafka_ingest.sources.file import spec as file_spec

SPEC = file_source.SOURCE_SPEC
PACKAGE = pathlib.Path(file_source.__file__).parent


def _package_source() -> str:
    """Every .py file in the source package, concatenated. Read once, searched many."""
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(PACKAGE.glob("*.py")))


@pytest.mark.parametrize("key", sorted(SPEC.structural_keys | SPEC.operational_keys))
def test_every_declared_key_is_actually_read_by_this_package(key):
    """The half that matters most. A key nobody reads is a lever a support engineer will
    pull during an incident, expecting something to happen."""
    assert re.search(rf'["\']{re.escape(key)}["\']|\b{re.escape(key)}\b', _package_source()), (
        f"'{key}' is declared in SOURCE_SPEC but appears nowhere in {PACKAGE.name}/"
    )


def test_files_land_only():
    """CORE section 10: no curated layer for Files, and no separate quarantine TABLE either
    - Auto Loader's rescuedDataColumn is this source's quarantine, landed as a column."""
    assert SPEC.layers == ("landing",)
    assert "landing_table" in known_keys(SPEC)
    assert "curated_table" not in known_keys(SPEC)


def test_the_framework_owned_keys_are_not_redeclared_here():
    """audit_table, state_table, control_table, domain, enabled and table_properties are
    read by the framework and by no source. A source that declared them could misspell one
    into silence."""
    assert not (SPEC.structural_keys & FRAMEWORK_STRUCTURAL_KEYS)


def test_the_shipped_source_exposes_exactly_the_contract():
    """SOURCE_SPEC and run(), and nothing else. A source that grew a read()/parse()/write()
    surface would have broken the one abstraction this design is built on."""
    assert set(file_source.__all__) == {"SOURCE_SPEC", "run"}
    assert not {"read", "parse", "write", "validate"} & set(dir(file_source))


# --------------------------------------------------------------------------------------
# Structural vs operational - mirrors Kafka's shape (STAGE_5 brief: reuse the design)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "access_mode",
        "volume_path",
        "storage_ref",
        "source_path",
        "target_schema",
        "target_table",
        "landing_partition_by",
    ],
)
def test_what_is_read_and_where_it_lands_is_never_operationally_overridable(key):
    """CORE section 5.2: where this source reads from and where it writes describe what is
    already on disk or which feed this is, and changing either needs a PR."""
    assert key in SPEC.structural_keys
    assert key not in SPEC.operational_keys


@pytest.mark.parametrize("key", ["failure_mode", "max_files_per_trigger"])
def test_the_two_standing_levers_are_settable_in_both_places(key):
    """A reviewed platform default in Git AND an incident override with no deploy - exactly
    Kafka's two standing levers, mirrored by design."""
    assert key in SPEC.structural_keys
    assert key in SPEC.operational_keys


def test_the_reset_id_is_operational_only():
    """Incident-scoped and single-use, same as Kafka's checkpoint_reset_id - a value
    checked into Git would silently re-apply on every future deploy."""
    assert file_spec.CHECKPOINT_RESET_ID in SPEC.operational_keys
    assert file_spec.CHECKPOINT_RESET_ID not in SPEC.structural_keys


# --------------------------------------------------------------------------------------
# The control table (docs/build_log/DECISIONS.md D-01)
# --------------------------------------------------------------------------------------


def test_every_control_column_maps_to_a_setting_that_is_operationally_overridable():
    for column, setting in SPEC.control_columns.items():
        assert column.startswith("file_"), f"'{column}' does not carry its source type's prefix"
        assert setting in SPEC.operational_keys, f"control column '{column}' maps to '{setting}', which nothing reads"


def test_the_control_columns_are_the_three_kafka_style_levers():
    """Named explicitly, mirroring D-01's own table, so adding a fourth is a deliberate
    edit here AND an ALTER TABLE in sql/01."""
    assert SPEC.control_columns == {
        "file_failure_mode": "failure_mode",
        "file_max_files_per_trigger": "max_files_per_trigger",
        "file_checkpoint_reset_id": file_spec.CHECKPOINT_RESET_ID,
    }


def test_a_control_row_setting_both_standing_levers_resolves_end_to_end(file_config_root):
    """Through framework/control.py and framework/config.py, into the source's own config -
    the whole chain a support UPDATE travels."""
    from conftest import FakeSpark
    from kafka_ingest.framework import tables
    from kafka_ingest.sources.file import config as file_config_module

    write_file_source(file_config_root)
    row = {
        "source_key": "demo_file",
        "source_type": "file",
        "file_failure_mode": "QUARANTINE",
        "file_max_files_per_trigger": 50,
    }
    table = "ops.ingestion.ingest_control"
    overrides = read_control(FakeSpark([row], existing_tables=(table,)), table, "demo_file", SPEC)
    resolved = resolve_config(file_config_root, "demo_file", "dev", SPEC, control=overrides)
    cfg = file_config_module.build(resolved, "primary", tables)

    assert cfg.failure_mode == "QUARANTINE"
    assert cfg.max_files_per_trigger == 50


def test_a_control_row_cannot_change_where_this_source_reads_from(file_config_root):
    """The structural half of the same chain: an override of a structural key is IGNORED,
    not applied and not rejected (framework/config.py `apply_overrides`)."""
    cfg = make_file_cfg(file_config_root, source_path="somewhere/else/")
    assert cfg.source_path == "claims/inbound/"
