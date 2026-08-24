"""sources/oracle/landing.py and tables.py - the projection and the DDL, and their agreement.

Two definitions of landing's provenance columns exist: the SQL expressions that produce
them and the DDL that creates them. A drift between the two surfaces as a confusing Delta
schema error on the first append and nowhere earlier, so the first test here is that they
agree - column for column, in order.
"""

from __future__ import annotations

import pytest

from conftest import FakeSchema, LoadedFrame, make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.oracle import landing, tables

SCHEMA = FakeSchema({"CLAIM_ID": "decimal(38,0)", "LAST_UPDATE_DT": "timestamp"})


def _cfg(oracle_config_root, **settings):
    write_oracle_source(oracle_config_root, **settings)
    return make_oracle_cfg(oracle_config_root)


def _projected(cfg, frame=None):
    frame = frame if frame is not None else LoadedFrame(schema=SCHEMA)
    return landing.project(frame, cfg, txn_version=7, run_id="demo_oracle-primary-abc").projections[0]


# --------------------------------------------------------------------------------------
# The two definitions agree
# --------------------------------------------------------------------------------------


def test_the_projection_and_the_ddl_name_the_same_columns_in_the_same_order(oracle_cfg):
    """THE POINT OF THIS FILE. The alias in each expression, against the DDL column list."""
    aliases = [expression.rsplit(" AS ", 1)[1] for expression in _projected(oracle_cfg)[1:]]
    ddl_columns = [line.strip().split()[0] for line in tables.METADATA_DDL.strip().splitlines()]

    assert aliases == list(landing.METADATA_COLUMNS)
    assert ddl_columns == list(landing.METADATA_COLUMNS)


def test_the_source_s_own_columns_come_first_and_in_order(oracle_cfg):
    """`*` first, so Oracle's column order survives into the landing table - which is the
    order the table is CREATED in on the first run and cannot be changed afterwards."""
    assert _projected(oracle_cfg)[0] == "*"
    ddl = tables.landing_columns(SCHEMA)
    assert ddl.index("CLAIM_ID") < ddl.index("LAST_UPDATE_DT") < ddl.index("source_key")


def test_the_ddl_carries_the_source_s_own_types(oracle_cfg):
    ddl = tables.landing_columns(SCHEMA)
    assert "CLAIM_ID decimal(38,0)" in ddl
    assert "LAST_UPDATE_DT timestamp" in ddl


def test_an_extract_that_returned_no_columns_creates_nothing(oracle_cfg):
    """An empty landing table would be created once and then never match anything."""
    with pytest.raises(ConfigError, match="no columns"):
        tables.landing_columns(FakeSchema({}))


# --------------------------------------------------------------------------------------
# What the provenance columns say
# --------------------------------------------------------------------------------------


def test_the_run_s_identity_reaches_every_row(oracle_cfg):
    expressions = _projected(oracle_cfg)
    assert "'demo_oracle' AS source_key" in expressions
    assert "'demo_oracle-primary-abc' AS run_id" in expressions
    assert "'primary' AS ingested_via" in expressions
    assert "CAST(7 AS BIGINT) AS txn_version" in expressions


def test_a_scheduled_run_records_no_replay_id(oracle_cfg):
    """NULL rather than a placeholder string: `replay_run_id IS NULL` is how a support
    query separates what a replay wrote from what the schedule did."""
    assert "CAST(NULL AS STRING) AS replay_run_id" in _projected(oracle_cfg)


def test_a_replay_stamps_its_rerun_id_on_every_row_it_writes(oracle_config_root):
    write_oracle_source(
        oracle_config_root,
        incremental_mode="cursor",
        cursor_column="LAST_UPDATE_DT",
        cursor_type="timestamp",
        merge_keys=["CLAIM_ID"],
    )
    cfg = make_oracle_cfg(
        oracle_config_root,
        run_type="oracle_replay",
        rerun_id="INC-1042",
        replay_cursor_start="2026-07-01 00:00:00",
    )
    assert "'INC-1042' AS replay_run_id" in _projected(cfg)


def test_the_ingest_date_is_a_date_and_the_timestamp_is_beside_it(oracle_cfg):
    """`ingest_date` is landing's PARTITION column, so it is a DATE rather than a
    truncation of the timestamp next to it - and both say when the row was WRITTEN here,
    never when it changed in Oracle."""
    expressions = _projected(oracle_cfg)
    assert "current_date() AS ingest_date" in expressions
    assert "current_timestamp() AS ingest_ts" in expressions


# --------------------------------------------------------------------------------------
# What the projection refuses
# --------------------------------------------------------------------------------------


def test_a_source_column_that_would_shadow_a_provenance_column_is_refused(oracle_cfg):
    """The error Delta gives for a duplicate column names neither this module nor the
    source table, so it is caught here with the column name and the fix."""
    frame = LoadedFrame(schema=FakeSchema({"CLAIM_ID": "decimal(38,0)", "run_id": "string"}))
    with pytest.raises(ConfigError, match="run_id"):
        landing.project(frame, oracle_cfg, 1, "run-1")


def test_a_run_id_that_is_not_an_identifier_never_reaches_the_projection(oracle_cfg):
    """Every literal here is framework-generated. A quote in one would rewrite the
    projection rather than being stored, so the shape is checked rather than escaped."""
    with pytest.raises(ConfigError):
        landing.project(LoadedFrame(schema=SCHEMA), oracle_cfg, 1, "run-1' AS x, 'y")
