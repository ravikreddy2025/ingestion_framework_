"""The shipped SQL against the DDL that creates the tables it queries.

`sql/03_support_queries.sql` went stale the moment Stage 2 renamed the operational tables,
and nothing caught it - every other "keep these in step" claim in this repository has a
test behind it, and this one did not. These are that test.

They are static checks, not executions: no warehouse here, and VB-17 covers actually
running the provisioning scripts. What they catch is the failure that actually happened -
a rename applied to the code and the DDL but not to the runbook queries a support engineer
pastes into a warehouse at 3am, where the error they get is "table not found" and the
conclusion they draw is "the pipeline is broken".
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from conftest import sql_table_columns

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

OPERATIONAL_SQL = SQL_DIR / "01_operational_config.sql"
LAYER_SQL = SQL_DIR / "02_layer_tables.sql"
SUPPORT_SQL = SQL_DIR / "03_support_queries.sql"
MAINTENANCE_SQL = SQL_DIR / "04_maintenance.sql"

CONTROL_TABLE = "{ops_catalog}.ingestion.ingest_control"
STATE_TABLE = "{ops_catalog}.ingestion.ingest_state"
AUDIT_TABLE = "{catalog}.audit.ingest_audit"

# Identifiers Stage 2 retired, and what replaced each. A rename that reaches the code and
# the DDL but not the runbook is the exact failure these files have already had once.
RETIRED = {
    "ingestion_topic_control": "ingest_control",
    "stream_audit": "ingest_audit",
    "topic_key": "source_key",
    "on_deser_error": "failure_mode",
    "change_reason": "notes",
    "rerun_starting_offsets": "replay_controls JSON",
    "rerun_starting_timestamp": "replay_controls JSON",
    "rerun_ending_offsets": "replay_controls JSON",
    "rerun_ending_timestamp": "replay_controls JSON",
    "curated_replay_landing_filter": "replay_controls JSON",
    "max_offsets_per_trigger": "batch_limit, or source_overrides JSON",
    "starting_offsets      STRING": "position_start",
    "ending_offsets        STRING": "position_end",
}


def _read(path: Path) -> str:
    assert path.is_file(), f"{path} is missing"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def support_sql() -> str:
    return _read(SUPPORT_SQL)


@pytest.fixture(scope="module")
def provisioning_sql() -> str:
    """Both provisioning files, which between them create every table support queries."""
    return _read(OPERATIONAL_SQL) + "\n" + _read(LAYER_SQL)


# --------------------------------------------------------------------------------------
# Nothing references a name that no longer exists
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("retired, replacement", sorted(RETIRED.items()))
def test_no_sql_file_references_a_retired_identifier(retired, replacement):
    """Every .sql file, not just the support queries - a half-applied rename anywhere in
    this directory produces a script that fails on the statement nobody tested."""
    offenders = [path.name for path in sorted(SQL_DIR.glob("*.sql")) if retired in path.read_text(encoding="utf-8")]
    assert not offenders, (
        f"{offenders} still reference '{retired}', which Stage 2 replaced with "
        f"'{replacement}'. A support query naming a table or column that no longer exists "
        "fails during an incident, and reads as a broken pipeline rather than a stale file."
    )


# --------------------------------------------------------------------------------------
# Every table the support queries name is a table something creates
# --------------------------------------------------------------------------------------


def test_every_operational_table_queried_is_provisioned(support_sql, provisioning_sql):
    """A query against {ops_catalog}.ingestion.<something nobody creates> is a typo that
    only shows up when someone runs it."""
    referenced = set(re.findall(r"\{ops_catalog\}\.ingestion\.(\w+)", support_sql))
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS \{ops_catalog\}\.ingestion\.(\w+)", provisioning_sql))
    assert referenced <= created, f"queried but never created: {sorted(referenced - created)}"


def test_the_audit_table_queried_is_the_one_that_is_provisioned(support_sql, provisioning_sql):
    referenced = set(re.findall(r"\{catalog\}\.audit\.(\w+)", support_sql))
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS \{catalog\}\.audit\.(\w+)", provisioning_sql))
    assert referenced == created == {"ingest_audit"}


def test_the_maintenance_script_optimises_the_audit_table_that_exists():
    """04 is otherwise source-agnostic VACUUM/OPTIMIZE, but it names the audit table twice,
    and a VACUUM against a table that does not exist is a maintenance job that fails every
    night until somebody looks at it."""
    maintenance = _read(MAINTENANCE_SQL)
    assert "audit.ingest_audit" in maintenance


# --------------------------------------------------------------------------------------
# Every column the support queries WRITE is a column the control table has
#
# Reads are not checked: they range over joins, aliases and JSON paths, and a regex that
# tried would be wrong more often than the file. Writes are where a wrong column name is
# both easy to make and worst to discover - an UPDATE naming a column that does not exist
# fails mid-incident, after the engineer has already decided this is the fix.
# --------------------------------------------------------------------------------------


def _update_targets(sql: str, table: str) -> set:
    """Column names assigned by every UPDATE against `table`."""
    pattern = re.compile(
        r"UPDATE\s+" + re.escape(table) + r"(?:\s+\w+)?\s*\nSET\s(.*?)\nWHERE",
        re.DOTALL | re.IGNORECASE,
    )
    columns = set()
    for block in pattern.findall(sql):
        for assignment in re.findall(r"(?:^|,)\s*(\w+)\s*=", block, re.MULTILINE):
            columns.add(assignment)
    return columns


def test_the_support_updates_only_set_columns_the_control_table_has(support_sql, provisioning_sql):
    written = _update_targets(support_sql, CONTROL_TABLE)
    assert written, "no UPDATE against the control table found - the regex has gone stale"
    declared = set(sql_table_columns(provisioning_sql, "ingestion.ingest_control"))
    assert written <= declared, f"UPDATE sets columns the table does not have: {sorted(written - declared)}"


def test_support_never_updates_the_state_table(support_sql):
    """Support has SELECT on ingest_state and nothing more, by grant (sql/01). A hand-moved
    watermark is a silent data-loss incident, so the runbook must not even show how."""
    assert not _update_targets(support_sql, STATE_TABLE)
    assert f"UPDATE {STATE_TABLE}" not in support_sql
    assert f"DELETE FROM {STATE_TABLE}" not in support_sql


def test_the_support_queries_read_all_three_framework_tables(support_sql):
    """Triage that only looks at the audit table cannot tell "read nothing" from "did not
    run", and cannot see where a source actually got to. All three or it is not a runbook."""
    for table in (CONTROL_TABLE, STATE_TABLE, AUDIT_TABLE):
        assert table in support_sql, f"{table} is never queried"


def test_the_audit_columns_the_queries_are_built_on_exist(provisioning_sql):
    """Spot-check the columns every triage query in the file depends on. Not exhaustive by
    design - see the note above - and it is the SHARED, source-agnostic ones that matter,
    because those are what make one audit table serve three source types."""
    declared = set(sql_table_columns(provisioning_sql, "audit.ingest_audit"))
    for column in (
        "source_type",
        "source_key",
        "source_ref",
        "layer",
        "status",
        "record_count",
        "position_start",
        "position_end",
        "source_detail",
        "rerun_id",
        "run_type",
        "audit_date",
        "domain",
    ):
        assert column in declared, f"support queries select '{column}', which is not an audit column"
