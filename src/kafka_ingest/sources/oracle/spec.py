"""What the framework needs to know about an oracle source. NO PySpark import.

Every key below is READ by code in this package. That is the rule the file exists to
keep: a key listed here that nothing reads is a setting that silently does nothing, which
CORE section 2 rule 2 ranks as the worst possible output of this project. If you delete
the code that reads a key, delete the key.

WHAT IS DELIBERATELY NOT HERE YET
---------------------------------
Sub-step 4a builds configuration, validation and the query builder. The JDBC read
(`query_timeout`, `session_init`, `column_types`) and the replay controls arrive with the
code that reads them, in 4b and 4c. An empty slot is honest; a declared key with no reader
is not.

STRUCTURAL vs OPERATIONAL, and the three interesting cases
----------------------------------------------------------
  in BOTH sets      settable in YAML and overridable at run time without a deploy.
                    `fetch_size` and `num_partitions` - a reviewed default plus the two
                    knobs support turns when an extract is too slow or too heavy for the
                    source database.
  structural ONLY   an operational override is IGNORED. `source_schema`, `source_table`,
                    `filter_criteria`, `merge_keys`, `partition_column`: they decide WHAT
                    IS EXTRACTED and how the target is keyed, and CORE section 5.2 puts
                    them behind a PR. `filter_criteria` most of all - it is a SQL
                    fragment, and support must not be able to reach the source database's
                    parser through the control table.
  operational ONLY  setting it in YAML is an ERROR. None yet; the replay controls in 4c
                    are the first.
"""

from __future__ import annotations

from ...framework.contracts import SourceSpec

# Extraction shape: either a hand-written query, or the column/filter set. Never both.
SQL_QUERY = "sql_query"
COLUMNS = "columns"
FILTER_COLUMN = "filter_column"
FILTER_CRITERIA = "filter_criteria"
DYNAMIC_DATE_FILTER = "dynamic_date_filter"

# Settable in conf/ (layers 1-3). `landing_table` is not here: framework/config.py derives
# `<layer>_table` from SOURCE_SPEC.layers, because the framework - not this source -
# resolves, validates and creates it.
_STRUCTURAL = frozenset(
    {
        "jdbc_ref",
        "source_schema",
        "source_table",
        SQL_QUERY,
        COLUMNS,
        FILTER_COLUMN,
        FILTER_CRITERIA,
        DYNAMIC_DATE_FILTER,
        "incremental_mode",
        "cursor_column",
        "cursor_type",
        "merge_keys",
        "partition_column",
        "num_partitions",
        "fetch_size",
    }
)

SOURCE_SPEC = SourceSpec(
    source_type="oracle",
    # `source_table` is the only key a source file MUST carry - everything else has a
    # platform default. The other three are here because their defaults live in
    # conf/defaults/oracle.yaml and none of them has a safe fallback in code: an unset
    # `fetch_size` means the Oracle JDBC driver's own default of TEN ROWS per round trip,
    # which is the difference between minutes and hours, and it fails as slowness rather
    # than as an error.
    required_keys=frozenset({"source_table", "incremental_mode", "fetch_size", "num_partitions"}),
    structural_keys=_STRUCTURAL,
    # The two knobs that are safe to turn during an incident: neither changes WHICH rows
    # are extracted, only how hard the extract leans on the source database.
    operational_keys=frozenset({"fetch_size", "num_partitions"}),
    # A hand-written query and the generated one are two ways to say the same thing, and
    # silently preferring one would make the other look ignored. Declared as pairs rather
    # than one group because `columns` + `filter_column` + `dynamic_date_filter` together
    # are perfectly legal - it is only their combination WITH sql_query that is not.
    mutually_exclusive=(
        (SQL_QUERY, COLUMNS),
        (SQL_QUERY, FILTER_COLUMN),
        (SQL_QUERY, FILTER_CRITERIA),
        (SQL_QUERY, DYNAMIC_DATE_FILTER),
    ),
    # CORE section 10: Oracle lands only. A curated layer for Oracle is out of scope.
    layers=("landing",),
    # The landing target is named after the Oracle object it mirrors, so only this source
    # can fill the pattern in conf/defaults/oracle.yaml. Config load leaves both tokens
    # alone; sources/oracle/config.py lower-cases them and framework/tables.py validates
    # the result before anything connects.
    target_tokens=frozenset({"source_schema", "source_table"}),
    # This source type's own columns on the one shared control table
    # (docs/build_log/DECISIONS.md D-01): column name -> the setting it overrides.
    control_columns={
        "oracle_fetch_size": "fetch_size",
        "oracle_num_partitions": "num_partitions",
    },
)
