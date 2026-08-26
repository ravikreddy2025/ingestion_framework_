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

CONTROL_TABLE = "{ops_catalog}.{control_schema}.ingest_control"
STATE_TABLE = "{ops_catalog}.{control_schema}.ingest_state"
AUDIT_TABLE = "{ops_catalog}.{audit_schema}.ingest_audit"

# Identifiers Stage 2 (and the decisions-and-stage2-followup pass) retired, and what
# replaced each. A rename that reaches the code and the DDL but not the runbook is the
# exact failure these files have already had once.
#
# NOTE: `failure_mode`, `batch_limit`, `max_offsets_per_trigger` and `source_overrides`
# are deliberately NOT here even though D-01 retired them as bare control-table columns -
# `failure_mode` and `max_offsets_per_trigger` are now SUBSTRINGS of their own replacements
# (`kafka_failure_mode`, `kafka_max_offsets_per_trigger`), and `source_overrides` is a name
# every explanatory comment about the rename legitimately still uses, here and in the SQL
# itself. This test's `retired in text` check is a plain substring search, so adding any of
# them would fail on the very column, or the very comment, that replaced them.
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
    """A query against {ops_catalog}.{control_schema}.<something nobody creates> is a typo
    that only shows up when someone runs it."""
    referenced = set(re.findall(r"\{ops_catalog\}\.\{control_schema\}\.(\w+)", support_sql))
    created = set(
        re.findall(r"CREATE TABLE IF NOT EXISTS \{ops_catalog\}\.\{control_schema\}\.(\w+)", provisioning_sql)
    )
    assert referenced <= created, f"queried but never created: {sorted(referenced - created)}"


def test_the_audit_table_queried_is_the_one_that_is_provisioned(support_sql, provisioning_sql):
    referenced = set(re.findall(r"\{ops_catalog\}\.\{audit_schema\}\.(\w+)", support_sql))
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS \{ops_catalog\}\.\{audit_schema\}\.(\w+)", provisioning_sql))
    assert referenced == created == {"ingest_audit"}


def test_the_maintenance_script_optimises_the_audit_table_that_exists():
    """04 is otherwise source-agnostic VACUUM/OPTIMIZE, but it names the audit table twice,
    and a VACUUM against a table that does not exist is a maintenance job that fails every
    night until somebody looks at it."""
    maintenance = _read(MAINTENANCE_SQL)
    assert "audit.ingest_audit" in maintenance


def test_ingest_state_is_partitioned_with_deletion_vectors_in_the_provisioning_sql(provisioning_sql):
    """docs/build_log/DECISIONS.md D-04, kept in step with framework/state.py's own
    `ensure_state_table` call the same way the audit table's columns are kept in step with
    framework/audit.py - the job creates this table itself if provisioning has not run, so
    the two must agree on more than just column names."""
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS \{ops_catalog\}\.\{control_schema\}\.ingest_state.*?;",
        re.DOTALL,
    )
    match = pattern.search(provisioning_sql)
    assert match, "no CREATE TABLE for ingest_state found"
    statement = match.group(0)
    assert "PARTITIONED BY (source_key)" in statement
    assert re.search(r"'delta\.enableDeletionVectors'\s*=\s*'true'", statement)


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
    declared = set(sql_table_columns(provisioning_sql, "ingest_control"))
    assert written <= declared, f"UPDATE sets columns the table does not have: {sorted(written - declared)}"


def test_support_never_updates_the_state_table(support_sql):
    """Support has SELECT on ingest_state and nothing more (docs/ARCHITECTURE_OVERVIEW.md's
    Unity Catalog privileges table). A hand-moved watermark is a silent data-loss incident,
    so the runbook must not even show how."""
    assert not _update_targets(support_sql, STATE_TABLE)
    assert f"UPDATE {STATE_TABLE}" not in support_sql
    assert f"DELETE FROM {STATE_TABLE}" not in support_sql


def test_no_sql_file_grants_anything(provisioning_sql):
    """docs/build_log/DECISIONS.md D-02: grants are Terraform-owned, outside this
    repository - a job that can grant privileges is a job that can grant itself more. See
    the "Unity Catalog privileges" table in docs/ARCHITECTURE_OVERVIEW.md for the specification
    this replaced the GRANT statements with."""
    assert "GRANT " not in provisioning_sql
    assert "GRANT " not in _read(SUPPORT_SQL)
    assert "GRANT " not in _read(MAINTENANCE_SQL)


def test_the_support_queries_read_all_three_framework_tables(support_sql):
    """Triage that only looks at the audit table cannot tell "read nothing" from "did not
    run", and cannot see where a source actually got to. All three or it is not a runbook."""
    for table in (CONTROL_TABLE, STATE_TABLE, AUDIT_TABLE):
        assert table in support_sql, f"{table} is never queried"


def test_the_audit_columns_the_queries_are_built_on_exist(provisioning_sql):
    """Spot-check the columns every triage query in the file depends on. Not exhaustive by
    design - see the note above - and it is the SHARED, source-agnostic ones that matter,
    because those are what make one audit table serve three source types."""
    declared = set(sql_table_columns(provisioning_sql, "ingest_audit"))
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
