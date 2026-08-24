"""sources/oracle/query.py - the extraction query, exhaustively.

The query builder is pure string construction over an already-validated configuration, so
every input it will ever take can be enumerated here and every output asserted. That is
the point of keeping it a single function: the part of the Oracle source that decides WHAT
IS EXTRACTED is fully testable on a laptop, with no driver, no cluster and no database.

THE CLOSED INTERVAL is the reason this file is long. `cursor > :last AND cursor <= :high`
is the difference between an extract that is correct against a live table and one that
only looks correct against a static one, and the failure mode of getting it wrong is
missing rows rather than an error.
"""

from __future__ import annotations

import pytest

from conftest import make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.oracle.query import build_query

WATERMARK = "2026-08-01 00:00:00"
HIGH_WATER = "2026-08-24 06:30:00"


def _cfg(config_root, **settings):
    write_oracle_source(config_root, **settings)
    return make_oracle_cfg(config_root)


def _cursor_cfg(config_root, merge_keys=("CLAIM_ID",), cursor_type="timestamp", **settings):
    cursor = {"incremental_mode": "cursor", "cursor_column": "LAST_UPDATE_DT", "cursor_type": cursor_type}
    return _cfg(config_root, **{**cursor, **settings}, merge_keys=list(merge_keys))


# --------------------------------------------------------------------------------------
# 1. The base
# --------------------------------------------------------------------------------------


def test_the_default_extract_is_the_whole_table(oracle_cfg):
    assert build_query(oracle_cfg) == "SELECT * FROM CLAIMS.CLAIM_HEADER"


def test_a_column_list_is_projected_in_the_order_it_was_configured(oracle_config_root):
    """Column ORDER matters: it is the order the landing table's columns are created in on
    the first run, and a later reordering of the source file would not move them back."""
    cfg = _cfg(oracle_config_root, columns=["CLAIM_ID", "MEMBER_ID", "STATUS"])
    assert build_query(cfg) == "SELECT CLAIM_ID, MEMBER_ID, STATUS FROM CLAIMS.CLAIM_HEADER"


def test_a_hand_written_query_with_nothing_to_append_is_passed_through_verbatim(oracle_config_root):
    """What the audit row then shows is character-for-character what the source file asked
    for, which is what the person reading it during an incident is comparing against."""
    query = "SELECT c.CLAIM_ID, m.NAME FROM CLAIMS.CLAIM_HEADER c JOIN CLAIMS.MEMBER m ON m.ID = c.MEMBER_ID"
    assert build_query(_cfg(oracle_config_root, sql_query=query)) == query


def test_a_hand_written_query_becomes_an_inline_view_when_a_predicate_has_to_attach(oracle_config_root):
    """A cursor bound cannot be appended to an arbitrary query - it may already have its
    own WHERE, GROUP BY or ORDER BY - so the query becomes the FROM of a wrapper."""
    cfg = _cursor_cfg(oracle_config_root, sql_query="SELECT CLAIM_ID, LAST_UPDATE_DT FROM CLAIMS.CLAIM_HEADER")
    built = build_query(cfg, None, HIGH_WATER)
    assert built.startswith("SELECT * FROM (SELECT CLAIM_ID, LAST_UPDATE_DT FROM CLAIMS.CLAIM_HEADER) src WHERE ")


# --------------------------------------------------------------------------------------
# 2. and 3. The static filter and the dynamic window
# --------------------------------------------------------------------------------------


def test_the_static_filter_is_the_column_and_the_fragment(oracle_config_root):
    cfg = _cfg(oracle_config_root, filter_column="STATUS", filter_criteria="IN ('A','P')")
    assert build_query(cfg) == "SELECT * FROM CLAIMS.CLAIM_HEADER WHERE STATUS IN ('A','P')"


@pytest.mark.parametrize("window, interval", [("P7D", "INTERVAL '7' DAY"), ("PT12H", "INTERVAL '12' HOUR")])
def test_the_dynamic_window_is_evaluated_by_oracle_not_by_the_driver(oracle_config_root, window, interval):
    """SYSTIMESTAMP is the SOURCE database's clock. Computing the boundary here instead
    would compare Databricks' clock against Oracle's data, and the two disagree by however
    much clock skew and time-zone configuration disagree."""
    cfg = _cfg(oracle_config_root, dynamic_date_filter={"column": "LAST_UPDATE_DT", "window": window})
    assert build_query(cfg).endswith(f"WHERE LAST_UPDATE_DT >= SYSTIMESTAMP - {interval}")


def test_the_filters_are_appended_in_the_documented_order(oracle_config_root):
    """Static, then dynamic, then incremental - the order sources/oracle/query.py's
    docstring gives, so a query in an audit row reads the same way every time."""
    cfg = _cursor_cfg(
        oracle_config_root,
        filter_column="STATUS",
        filter_criteria="IN ('A')",
        dynamic_date_filter={"column": "CREATED_DT", "window": "P7D"},
    )
    built = build_query(cfg, WATERMARK, HIGH_WATER)
    positions = [
        built.index("STATUS IN ('A')"),
        built.index("CREATED_DT >= SYSTIMESTAMP"),
        built.index("LAST_UPDATE_DT >="),
        built.index("LAST_UPDATE_DT <="),
    ]
    assert positions == sorted(positions)
    assert built.count(" WHERE ") == 1
    assert built.count(" AND ") == 3


# --------------------------------------------------------------------------------------
# 4. The incremental predicate - the closed interval
# --------------------------------------------------------------------------------------


def test_a_cursor_run_always_has_an_upper_bound(oracle_config_root):
    """THE BOUNDARY BUG, asserted. With an open upper bound, rows committed in Oracle
    WHILE the extract runs may or may not be read depending on when each JDBC partition
    reaches them - and the watermark afterwards advances past all of them regardless."""
    built = build_query(_cursor_cfg(oracle_config_root), WATERMARK, HIGH_WATER)
    assert "LAST_UPDATE_DT <= TO_TIMESTAMP('2026-08-24 06:30:00', 'YYYY-MM-DD HH24:MI:SS')" in built


def test_a_cursor_run_without_an_upper_bound_is_refused(oracle_config_root):
    """Refused rather than defaulted. A caller that has not captured the high-water mark
    has not finished implementing the run."""
    with pytest.raises(ConfigError, match="upper bound"):
        build_query(_cursor_cfg(oracle_config_root), WATERMARK, None)


def test_the_first_cursor_run_reads_everything_up_to_the_upper_bound(oracle_config_root):
    """No watermark yet, so there is no lower bound - but the upper bound still applies,
    which is what makes the first run's own watermark trustworthy."""
    built = build_query(_cursor_cfg(oracle_config_root), None, HIGH_WATER)
    assert built.count("LAST_UPDATE_DT") == 1
    assert "<=" in built and ">" not in built


def test_merge_keys_make_the_lower_bound_inclusive(oracle_config_root):
    """TIE-SAFE. A second-granularity cursor is not unique, so the run re-reads its own
    boundary and the MERGE de-duplicates what it already has."""
    built = build_query(_cursor_cfg(oracle_config_root, merge_keys=("CLAIM_ID",)), WATERMARK, HIGH_WATER)
    assert "LAST_UPDATE_DT >= TO_TIMESTAMP('2026-08-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" in built


def test_waiving_merge_keys_excludes_the_boundary_and_that_loses_ties(oracle_config_root):
    """THE DOCUMENTED LOSS, asserted so that it is a known property rather than a surprise.

    With `merge_keys: []` the predicate is `>`, so a row committed in Oracle with EXACTLY
    the previous run's watermark value - after that run had already read past it - is
    never seen by any run. That is the trade `merge_keys: []` buys, and CORE section 10
    says to make it deliberate; this test is what makes it visible.
    """
    built = build_query(_cursor_cfg(oracle_config_root, merge_keys=()), WATERMARK, HIGH_WATER)
    assert "LAST_UPDATE_DT > TO_TIMESTAMP('2026-08-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" in built
    assert ">=" not in built


# --------------------------------------------------------------------------------------
# Watermark literals - the one place text becomes SQL
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, rendered",
    [
        ("2026-08-24 06:30:00", "TO_TIMESTAMP('2026-08-24 06:30:00', 'YYYY-MM-DD HH24:MI:SS')"),
        ("2026-08-24T06:30:00", "TO_TIMESTAMP('2026-08-24 06:30:00', 'YYYY-MM-DD HH24:MI:SS')"),
        ("2026-08-24 06:30:00.123456", "TO_TIMESTAMP('2026-08-24 06:30:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF')"),
    ],
)
def test_a_timestamp_watermark_is_rendered_with_an_explicit_format_model(oracle_config_root, value, rendered):
    """Never a bare string literal: an implicit conversion would depend on the session's
    NLS_DATE_FORMAT, which is set by whoever configured the database rather than by us."""
    assert rendered in build_query(_cursor_cfg(oracle_config_root), None, value)


def test_a_number_cursor_renders_as_a_bare_numeric_literal(oracle_config_root):
    cfg = _cursor_cfg(oracle_config_root, cursor_type="number", cursor_column="ROW_VERSION")
    assert build_query(cfg, "1000", "2000").endswith("ROW_VERSION >= 1000 AND ROW_VERSION <= 2000")


@pytest.mark.parametrize("watermark", ["2026-08-01' OR '1'='1", "SYSDATE", "yesterday", "2026-08-01", ""])
def test_a_watermark_that_is_not_a_timestamp_never_reaches_the_query(oracle_config_root, watermark):
    """`ingest_state` is a table a support engineer can UPDATE, so 'only this framework
    writes it' is a claim about a table, not a guarantee. The rendering is the boundary."""
    with pytest.raises(ConfigError):
        build_query(_cursor_cfg(oracle_config_root), None, watermark or "x")


@pytest.mark.parametrize("watermark", ["1000; DROP TABLE CLAIMS", "1e9", "0x10", "NULL"])
def test_a_number_cursor_refuses_anything_that_is_not_a_number(oracle_config_root, watermark):
    cfg = _cursor_cfg(oracle_config_root, cursor_type="number", cursor_column="ROW_VERSION")
    with pytest.raises(ConfigError):
        build_query(cfg, None, watermark)


@pytest.mark.parametrize("mode", ["full", "filter"])
def test_a_watermark_handed_to_a_mode_that_has_no_cursor_is_refused(oracle_config_root, mode):
    """Refused rather than ignored: a caller doing this has misunderstood something, and
    silently extracting the whole table would hide it behind a plausible row count."""
    cfg = _cfg(oracle_config_root, incremental_mode=mode, filter_column="STATUS", filter_criteria="IN ('A')")
    with pytest.raises(ConfigError, match="no cursor"):
        build_query(cfg, WATERMARK, HIGH_WATER)


# --------------------------------------------------------------------------------------
# The cross product - every combination of the four parts
# --------------------------------------------------------------------------------------

BASES = {
    "table": ({}, "FROM CLAIMS.CLAIM_HEADER"),
    "columns": ({"columns": ["CLAIM_ID"]}, "SELECT CLAIM_ID FROM CLAIMS.CLAIM_HEADER"),
    "query": (
        {"sql_query": "SELECT CLAIM_ID, LAST_UPDATE_DT FROM CLAIMS.CLAIM_HEADER"},
        "SELECT CLAIM_ID, LAST_UPDATE_DT FROM CLAIMS.CLAIM_HEADER",
    ),
}
FILTERS = {
    "none": ({}, None),
    "static": ({"filter_column": "STATUS", "filter_criteria": "IN ('A')"}, "STATUS IN ('A')"),
    "dynamic": (
        {"dynamic_date_filter": {"column": "CREATED_DT", "window": "P7D"}},
        "CREATED_DT >= SYSTIMESTAMP - INTERVAL '7' DAY",
    ),
}


@pytest.mark.parametrize("base_name", sorted(BASES))
@pytest.mark.parametrize("filter_name", sorted(FILTERS))
@pytest.mark.parametrize("cursor", [None, "merge", "append"])
def test_every_combination_of_the_four_parts_builds(oracle_config_root, base_name, filter_name, cursor):
    """The exhaustive pass 4a's exit gate asks for.

    A hand-written query excludes every generated clause, so those combinations are not
    buildable configurations at all - the spec rejects them (tests/test_oracle_spec.py) and
    they are skipped here rather than asserted twice.
    """
    base_settings, base_fragment = BASES[base_name]
    filter_settings, filter_fragment = FILTERS[filter_name]
    if base_name == "query" and filter_settings:
        pytest.skip("sql_query is mutually exclusive with every generated clause")

    settings = {**base_settings, **filter_settings}
    if cursor is None:
        cfg = _cfg(oracle_config_root, **settings)
        built = build_query(cfg)
    else:
        cfg = _cursor_cfg(oracle_config_root, merge_keys=("CLAIM_ID",) if cursor == "merge" else (), **settings)
        built = build_query(cfg, WATERMARK, HIGH_WATER)

    assert base_fragment in built
    if filter_fragment:
        assert filter_fragment in built
    if cursor is None:
        assert "LAST_UPDATE_DT <=" not in built
    else:
        assert f"LAST_UPDATE_DT {'>=' if cursor == 'merge' else '>'} TO_TIMESTAMP" in built
        assert "LAST_UPDATE_DT <= TO_TIMESTAMP" in built
    assert built.count("WHERE") == (0 if cursor is None and not filter_fragment else 1)
