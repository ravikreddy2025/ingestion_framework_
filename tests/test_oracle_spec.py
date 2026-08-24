"""sources/oracle/spec.py - the declaration the framework validates every layer against.

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

from conftest import make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import FRAMEWORK_STRUCTURAL_KEYS, known_keys, resolve_config
from kafka_ingest.framework.control import read_control
from kafka_ingest.sources import oracle

SPEC = oracle.SOURCE_SPEC
PACKAGE = pathlib.Path(oracle.__file__).parent


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


def test_oracle_lands_only():
    """CORE section 10: no curated layer for Oracle, and no quarantine layer either - a
    JDBC read returns typed rows or it fails. `layers` IS the set of `<layer>_table`
    settings framework/tables.py will resolve, validate and create."""
    assert SPEC.layers == ("landing",)
    assert "landing_table" in known_keys(SPEC)
    assert "curated_table" not in known_keys(SPEC)


def test_the_framework_owned_keys_are_not_redeclared_here():
    """audit_table, state_table, control_table, domain, enabled and table_properties are
    read by the framework and by no source. A source that declared them could misspell one
    into silence."""
    assert not (SPEC.structural_keys & FRAMEWORK_STRUCTURAL_KEYS)


def test_source_table_is_the_only_key_a_source_file_must_carry():
    """The other three required keys have platform defaults in conf/defaults/oracle.yaml.
    They are required so that DELETING a default is an error rather than a silent fallback
    - `fetch_size` unset means the Oracle driver's own default of ten rows per round
    trip, which fails as slowness and never as an error."""
    assert SPEC.required_keys == {"source_table", "incremental_mode", "fetch_size", "num_partitions"}


# --------------------------------------------------------------------------------------
# Structural vs operational
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    ["source_schema", "source_table", "filter_criteria", "filter_column", "merge_keys", "cursor_column", "sql_query"],
)
def test_what_is_extracted_is_never_operationally_overridable(key):
    """CORE section 5.2, and for `filter_criteria` it is a security boundary as well as a
    correctness one: it is a SQL fragment pasted into the extraction query, so leaving it
    out of operational_keys is what stops support reaching Oracle's parser through a table
    they can UPDATE."""
    assert key in SPEC.structural_keys
    assert key not in SPEC.operational_keys


@pytest.mark.parametrize("key", ["fetch_size", "num_partitions"])
def test_the_two_tuning_knobs_are_settable_in_both_places(key):
    """A reviewed platform default in Git AND an incident override with no deploy. Neither
    changes WHICH rows are extracted, which is exactly what makes them safe to turn."""
    assert key in SPEC.structural_keys
    assert key in SPEC.operational_keys


def test_a_hand_written_query_excludes_every_generated_clause():
    """`sql_query` and the column/filter set are two ways to say the same thing. Silently
    preferring one would make the other look ignored, so the spec rejects the pair."""
    pairs = {frozenset(group) for group in SPEC.mutually_exclusive}
    for other in ("columns", "filter_column", "filter_criteria", "dynamic_date_filter"):
        assert frozenset({"sql_query", other}) in pairs, f"sql_query + {other} is not declared exclusive"


def test_the_generated_clauses_are_not_exclusive_of_each_other():
    """`columns` + `filter_column` + `dynamic_date_filter` together is the ordinary case -
    declaring one group of five would have banned it."""
    for group in SPEC.mutually_exclusive:
        assert "sql_query" in group, f"group {group} bans a combination that is legal"


# --------------------------------------------------------------------------------------
# The control table (docs/build_log/DECISIONS.md D-01)
# --------------------------------------------------------------------------------------


def test_every_control_column_maps_to_a_setting_that_is_operationally_overridable():
    """A column mapping to a setting that is not in operational_keys fails at run time with
    an unknown-key error - honest, but only discovered by a support engineer mid-incident."""
    for column, setting in SPEC.control_columns.items():
        assert column.startswith("oracle_"), f"'{column}' does not carry its source type's prefix"
        assert setting in SPEC.operational_keys, f"control column '{column}' maps to '{setting}', which nothing reads"


def test_the_control_columns_are_the_two_levers_support_has():
    """Named explicitly so that adding a third is a deliberate edit here AND an ALTER TABLE
    in sql/01 - there is no free-form JSON escape hatch any more."""
    assert SPEC.control_columns == {
        "oracle_fetch_size": "fetch_size",
        "oracle_num_partitions": "num_partitions",
    }


def test_a_control_row_setting_both_columns_resolves_end_to_end(oracle_config_root):
    """Through framework/control.py and framework/config.py, into the source's own config.

    This is the whole chain a support UPDATE travels, and every link in it exists before
    the run does.
    """
    from conftest import FakeSpark
    from kafka_ingest.framework import tables
    from kafka_ingest.sources.oracle import config as oracle_config

    write_oracle_source(oracle_config_root, partition_column="CLAIM_ID")
    row = {
        "source_key": "demo_oracle",
        "source_type": "oracle",
        "oracle_fetch_size": 500,
        "oracle_num_partitions": 4,
    }
    table = "ops.ingestion.ingest_control"
    overrides = read_control(FakeSpark([row], existing_tables=(table,)), table, "demo_oracle", SPEC)
    resolved = resolve_config(oracle_config_root, "demo_oracle", "dev", SPEC, control=overrides)
    cfg = oracle_config.build(resolved, tables)

    assert cfg.fetch_size == 500
    assert cfg.num_partitions == 4


def test_a_control_row_cannot_change_what_is_extracted(oracle_config_root):
    """The structural half of the same chain: an override of a structural key is IGNORED,
    not applied and not rejected (framework/config.py `apply_overrides`). Here that is the
    difference between a knob and an injection point."""
    write_oracle_source(oracle_config_root, filter_column="STATUS", filter_criteria="IN ('A')")
    cfg = make_oracle_cfg(
        oracle_config_root,
        filter_criteria="IN ('A') OR 1=1",
        source_table="OTHER_TABLE",
    )
    assert cfg.filter_criteria == "IN ('A')"
    assert cfg.source_table == "CLAIM_HEADER"


def test_the_shipped_source_exposes_exactly_the_contract():
    """SOURCE_SPEC and run(), and nothing else. A source that grew a read()/parse()/write()
    surface would have broken the one abstraction this design is built on."""
    assert set(oracle.__all__) == {"SOURCE_SPEC", "run"}
    assert not {"read", "parse", "write", "validate"} & set(dir(oracle))
