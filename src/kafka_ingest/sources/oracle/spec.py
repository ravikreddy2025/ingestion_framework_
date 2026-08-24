"""What the framework needs to know about an oracle source. NO PySpark import.

Every key below is READ by code in this package. That is the rule the file exists to
keep: a key listed here that nothing reads is a setting that silently does nothing, which
CORE section 2 rule 2 ranks as the worst possible output of this project. If you delete
the code that reads a key, delete the key.

WHAT IS DELIBERATELY NOT HERE YET
---------------------------------
Nothing that the write path needs. `landing_partition_by` and anything else sub-step 4c
reads arrives with the code that reads it. An empty slot is honest; a declared key with no
reader is not.

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
  operational ONLY  setting it in YAML is an ERROR. The two replay cursor bounds: each is
                    scoped to ONE re-extraction, and a bound checked into Git would silently
                    re-apply on every future deploy.

`incremental_mode` IS OPERATIONALLY OVERRIDABLE, and it is the one exception to "structural
keys decide what is extracted" (docs/build_log/DECISIONS.md D-09). Support can move a source
between a full and a delta load with no deploy, because that is a recovery action - a delta
load that has been skipping rows is fixed by one full load, and waiting for a PR to merge is
the wrong shape of answer at 3am. What it is NOT allowed to do is change what a column
MEANS: `cursor_column`, `cursor_type`, `merge_keys` and `filter_criteria` all stay
structural, so the mode can change while the definition of the increment cannot.
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
        "query_timeout",
        "session_init",
        "column_types",
    }
)

# Replay bounds, operational-ONLY. They replace the stored watermark FOR ONE RUN, which is
# what makes a bounded re-extraction possible without touching the durable state a
# scheduled run depends on (D-09). Named `replay_*` rather than reusing the cursor keys
# because those already mean something permanent: which column the cursor IS.
REPLAY_CURSOR_START = "replay_cursor_start"
REPLAY_CURSOR_END = "replay_cursor_end"

_REPLAY_KEYS = frozenset({REPLAY_CURSOR_START, REPLAY_CURSOR_END})

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
    # The two tuning knobs, plus the mode switch and the replay bounds (D-09). The tuning
    # knobs change only how hard the extract leans on the source database; the mode switch
    # and the bounds change WHICH ROWS, deliberately, because both are recovery actions.
    operational_keys=frozenset({"fetch_size", "num_partitions", "incremental_mode"}) | _REPLAY_KEYS,
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
        # The full-vs-delta switch. The replay bounds are NOT columns of their own: they
        # ride in the framework-owned `replay_controls` JSON, exactly as Kafka's replay
        # offsets do, because they are per-incident parameters rather than standing state.
        "oracle_incremental_mode": "incremental_mode",
    },
)
