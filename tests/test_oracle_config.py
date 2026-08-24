"""sources/oracle/config.py - the VALUES half of Oracle's configuration.

The framework validates KEYS against SOURCE_SPEC (tests/test_oracle_spec.py); this file
covers everything the spec cannot express: the enumerations, the cross-field pairs, the
identifier rules, and the two rules that must fail at CONFIG LOAD rather than after an
hour-long read - a name Unity Catalog cannot hold, and a SQL fragment that is not a
predicate.

Every case here resolves through the REAL five-layer path against the REAL
conf/defaults/oracle.yaml (see the fixture in conftest.py). Testing a shortcut would prove
the shortcut works.
"""

from __future__ import annotations

import pytest

from conftest import make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.oracle import config as oracle_config


def _cfg(config_root, **settings):
    write_oracle_source(config_root, **settings)
    return make_oracle_cfg(config_root)


def _error(config_root, **settings):
    write_oracle_source(config_root, **settings)
    with pytest.raises(ConfigError) as excinfo:
        make_oracle_cfg(config_root)
    return str(excinfo.value)


# --------------------------------------------------------------------------------------
# The shipped platform defaults
# --------------------------------------------------------------------------------------


def test_the_shipped_defaults_are_the_ones_the_code_needs(oracle_cfg):
    """conf/defaults/oracle.yaml, resolved. `fetch_size` is the one that fails invisibly:
    the Oracle JDBC driver's own default is TEN rows per round trip, so an unset value is
    not 'the driver decides', it is an extract that takes hours instead of minutes."""
    assert oracle_cfg.fetch_size == 10000
    assert oracle_cfg.num_partitions == 1
    assert oracle_cfg.incremental_mode == oracle_config.MODE_FULL


def test_the_landing_target_is_derived_from_the_oracle_name(oracle_cfg):
    """`CLAIMS.CLAIM_HEADER` -> `{catalog}.oracle_claims.claim_header`. Oracle folds
    unquoted identifiers to upper case and Unity Catalog folds to lower, so the two names
    for one table differ by more than a prefix."""
    assert oracle_cfg.landing_table == "cat_dev.oracle_claims.claim_header"
    assert oracle_cfg.source_ref == "CLAIMS.CLAIM_HEADER"


def test_the_configured_case_does_not_change_the_target(oracle_config_root):
    """A source file written in lower case names the same Oracle object and the same UC
    table. If these two disagreed, renaming a source file's case would silently start
    writing to a second table."""
    lower = _cfg(oracle_config_root, source_schema="claims", source_table="claim_header")
    assert lower.landing_table == "cat_dev.oracle_claims.claim_header"
    assert lower.source_ref == "CLAIMS.CLAIM_HEADER"


def test_each_environment_writes_into_its_own_catalog(oracle_config_root):
    """dev must not be able to write into prod's tables."""
    assert make_oracle_cfg(oracle_config_root, environment="prod").landing_table.startswith("cat_prod.")


# --------------------------------------------------------------------------------------
# Names: legal in Oracle is not the same as legal in Unity Catalog
# --------------------------------------------------------------------------------------


def test_a_name_legal_in_oracle_and_illegal_in_unity_catalog_fails_at_config_load(oracle_config_root):
    """`$` and `#` are legal in an unquoted Oracle identifier and illegal in an unquoted UC
    one. Discovering that at write time means discovering it after the read has cost an
    hour."""
    message = _error(oracle_config_root, source_table="CLAIM$HEADER")
    assert "claim$header" in message
    assert "Unity Catalog" in message
    # And it names the SETTING at fault. framework/tables.py would reject the assembled
    # name too, but its message can only name `landing_table` - the pattern - which sends
    # an onboarder to the wrong file.
    assert "source_table" in message


@pytest.mark.parametrize("name", ["CLAIM HEADER", "1_CLAIMS", "CLAIMS;DROP", "CLAIMS.CLAIM_HEADER", ""])
def test_an_identifier_this_framework_will_not_paste_into_sql_is_refused(oracle_config_root, name):
    """Every one of these ends up in generated SQL. A space or a separator would either
    break the query or extend it, and a schema-qualified value in `source_table` would
    produce `CLAIMS.CLAIMS.CLAIM_HEADER`."""
    assert _error(oracle_config_root, source_table=name)


def test_a_source_file_with_no_table_names_nothing(oracle_config_root):
    """`source_table` is the one key with no platform default - required by the spec, so
    the error names the key and the source type rather than surfacing as an empty FROM."""
    assert "source_table" in _error(oracle_config_root, source_table=None)


def test_an_unknown_key_is_a_startup_error(oracle_config_root):
    """A typo'd setting is silent misconfiguration - the failure this whole layer exists
    to prevent."""
    assert "fetchsize" in _error(oracle_config_root, fetchsize=5000)


def test_an_unknown_jdbc_profile_fails_before_anything_connects(oracle_config_root):
    """The register is the single answer to 'which databases do we extract from?'. A typo
    must not quietly create a new profile."""
    message = _error(oracle_config_root, jdbc_ref="oracle_typo")
    assert "oracle_typo" in message and "oracle_demo" in message


# --------------------------------------------------------------------------------------
# Extraction shape
# --------------------------------------------------------------------------------------


def test_a_hand_written_query_and_a_generated_one_are_mutually_exclusive(oracle_config_root):
    """Named both ways round in the error, because the fix could be either."""
    message = _error(oracle_config_root, sql_query="SELECT 1 FROM DUAL", columns=["CLAIM_ID"])
    assert "columns" in message and "sql_query" in message


@pytest.mark.parametrize("query", ["UPDATE CLAIMS SET X = 1", "DELETE FROM CLAIMS", "BEGIN NULL; END;"])
def test_sql_query_must_be_a_read(oracle_config_root, query):
    """This framework extracts; it does not modify the source. A statement that is not a
    SELECT is refused by shape rather than by keyword, so the check cannot be evaded by
    spelling."""
    assert _error(oracle_config_root, sql_query=query)


@pytest.mark.parametrize("query", ["SELECT * FROM CLAIMS; DROP TABLE CLAIMS", "SELECT * FROM CLAIMS -- x", "SELECT 1;"])
def test_sql_query_may_not_hide_a_second_statement(oracle_config_root, query):
    """The query is wrapped in an inline view, so a separator or a comment marker either
    breaks the read or hides something inside it. A trailing semicolon is refused rather
    than stripped - stripping it would mean deciding which half was meant."""
    assert _error(oracle_config_root, sql_query=query)


def test_a_common_table_expression_is_still_one_read(oracle_config_root):
    """`WITH ... SELECT` is a single statement and a read, so it is allowed."""
    cfg = _cfg(oracle_config_root, sql_query="WITH c AS (SELECT 1 X FROM DUAL) SELECT X FROM c")
    assert cfg.sql_query.startswith("WITH")


# --------------------------------------------------------------------------------------
# filter_criteria - the one SQL fragment in the configuration
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "criteria",
    [
        "IN ('A'); DROP TABLE CLAIMS",
        "IN ('A') -- rest",
        "IN (SELECT ID FROM OTHER)",
        "IN ('A') UNION SELECT 1 FROM DUAL",
        "= (DELETE FROM CLAIMS)",
        "IN ('A') /* comment */",
    ],
)
def test_a_filter_fragment_that_is_more_than_a_predicate_is_refused(oracle_config_root, criteria):
    """`filter_criteria` reaches Oracle's parser verbatim. A predicate needs no statement
    separator, no comment marker and no keyword from the DML list; anything that does
    belongs in `sql_query`, which is reviewed in a PR."""
    assert _error(oracle_config_root, filter_column="STATUS", filter_criteria=criteria)


@pytest.mark.parametrize("criteria", ["IN ('A','P')", "= 'A'", "BETWEEN 1 AND 9", "IS NOT NULL", "LIKE 'A%'"])
def test_an_ordinary_predicate_is_accepted(oracle_config_root, criteria):
    """The allowlist has to admit the fragments people actually write, or it will be
    worked around."""
    assert _cfg(oracle_config_root, filter_column="STATUS", filter_criteria=criteria).filter_criteria == criteria


def test_a_column_called_update_dt_is_not_mistaken_for_a_keyword(oracle_config_root):
    """The keyword check is whole-word, deliberately: `LAST_UPDATE_DT` and `CREATE_DT` are
    ordinary Oracle column names and a substring check would reject both."""
    cfg = _cfg(oracle_config_root, filter_column="LAST_UPDATE_DT", filter_criteria="IS NOT NULL")
    assert cfg.filter_column == "LAST_UPDATE_DT"


@pytest.mark.parametrize("settings", [{"filter_column": "STATUS"}, {"filter_criteria": "IN ('A')"}])
def test_half_a_static_filter_is_a_configuration_error(oracle_config_root, settings):
    """Neither half means anything alone: a column with no criteria filters nothing, and
    criteria with no column has nothing to attach to."""
    assert _error(oracle_config_root, **settings)


# --------------------------------------------------------------------------------------
# Incremental modes
# --------------------------------------------------------------------------------------


def test_an_unknown_incremental_mode_is_refused(oracle_config_root):
    assert "incremental" in _error(oracle_config_root, incremental_mode="delta")


def test_cursor_mode_needs_a_cursor(oracle_config_root):
    assert _error(oracle_config_root, incremental_mode="cursor", merge_keys=["CLAIM_ID"])


def test_cursor_type_is_an_enumeration(oracle_config_root):
    """It decides how a stored watermark is rendered back into SQL, so 'date' - which
    looks plausible - would produce a comparison against the wrong type."""
    message = _error(
        oracle_config_root,
        incremental_mode="cursor",
        cursor_column="LAST_UPDATE_DT",
        cursor_type="date",
        merge_keys=["CLAIM_ID"],
    )
    assert "cursor_type" in message


def test_filter_mode_needs_the_filter_that_is_its_increment(oracle_config_root):
    """In `filter` mode the predicate IS the increment. Without it the run would extract
    the whole table while reporting itself as incremental."""
    assert _error(oracle_config_root, incremental_mode="filter")


def test_merge_keys_must_be_a_decision_not_a_default(oracle_config_root):
    """CORE section 10 records the recommendation as 'require unless explicitly waived'.
    Silence is not a waiver: a cursor extract with no merge keys appends, and rows sharing
    the boundary cursor value can be LOST. The error says what both options are."""
    message = _error(
        oracle_config_root, incremental_mode="cursor", cursor_column="LAST_UPDATE_DT", cursor_type="timestamp"
    )
    assert "merge_keys" in message


def test_the_waiver_is_an_empty_list_and_it_is_accepted(oracle_config_root):
    """`merge_keys: []` is a different configuration from no merge_keys at all, and the
    difference is load-bearing: this one was chosen."""
    cfg = _cfg(
        oracle_config_root,
        incremental_mode="cursor",
        cursor_column="LAST_UPDATE_DT",
        cursor_type="timestamp",
        merge_keys=[],
    )
    assert cfg.merge_keys == ()
    assert cfg.merge_on_write is False


def test_merge_keys_set_makes_the_write_a_merge(oracle_config_root):
    cfg = _cfg(
        oracle_config_root,
        incremental_mode="cursor",
        cursor_column="LAST_UPDATE_DT",
        cursor_type="timestamp",
        merge_keys=["CLAIM_ID"],
    )
    assert cfg.merge_on_write is True
    assert cfg.is_cursor is True


# --------------------------------------------------------------------------------------
# The dynamic date window
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("window, amount, unit", [("P7D", 7, "DAY"), ("PT12H", 12, "HOUR"), ("P1D", 1, "DAY")])
def test_a_supported_window_parses_into_an_oracle_interval(oracle_config_root, window, amount, unit):
    parsed = _cfg(oracle_config_root, dynamic_date_filter={"column": "LAST_UPDATE_DT", "window": window})
    assert parsed.dynamic_date_filter == oracle_config.DateWindow("LAST_UPDATE_DT", amount, unit)


@pytest.mark.parametrize("window", ["P1M", "P1Y", "7 days", "PT30M", "", "P"])
def test_a_window_whose_length_depends_on_when_you_ask_is_refused(oracle_config_root, window):
    """A month is not thirty days to everyone, and an approximated window silently changes
    how much history each run re-reads."""
    assert _error(oracle_config_root, dynamic_date_filter={"column": "LAST_UPDATE_DT", "window": window})


@pytest.mark.parametrize(
    "block", [{"column": "LAST_UPDATE_DT"}, {"window": "P7D"}, {"column": "X", "window": "P7D", "unit": "days"}, "P7D"]
)
def test_a_malformed_date_filter_block_is_refused(oracle_config_root, block):
    """Both keys, neither more nor fewer. A third key here would be a setting that
    silently does nothing."""
    assert _error(oracle_config_root, dynamic_date_filter=block)


# --------------------------------------------------------------------------------------
# Read parallelism
# --------------------------------------------------------------------------------------


def test_a_partition_count_without_a_partition_column_is_refused(oracle_config_root):
    """Spark needs a column and bounds to generate per-partition WHERE clauses. Given only
    a count it issues ONE query on ONE executor - so this fails as slowness, at scale,
    rather than as an error. The message names both halves of the fix."""
    message = _error(oracle_config_root, num_partitions=8)
    assert "partition_column" in message


def test_a_partitioned_read_is_accepted_when_both_halves_are_present(oracle_config_root):
    cfg = _cfg(oracle_config_root, num_partitions=8, partition_column="CLAIM_ID")
    assert (cfg.num_partitions, cfg.partition_column) == (8, "CLAIM_ID")


@pytest.mark.parametrize("key", ["fetch_size", "num_partitions"])
def test_zero_is_not_a_default(oracle_config_root, key):
    """Neither key treats zero or a missing value as 'let the library decide' - for
    fetch_size the library decides ten rows per round trip."""
    assert _error(oracle_config_root, **{key: 0})


def test_the_tuning_knobs_are_operationally_overridable(oracle_config_root):
    """The other half of test_oracle_spec.py's control-column chain: these two reach the
    resolved config from layer 4/5 without a deploy."""
    write_oracle_source(oracle_config_root, partition_column="CLAIM_ID")
    cfg = make_oracle_cfg(oracle_config_root, fetch_size="2000", num_partitions="4")
    assert (cfg.fetch_size, cfg.num_partitions) == (2000, 4)
