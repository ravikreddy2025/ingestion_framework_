"""sources/oracle/reader.py - the JDBC options, and the partition bounds.

NOTHING HERE EXECUTES A READ. A JDBC extract is entirely "hand the right options to
spark.read", so recording the options map tests the part that has to be right - and it is
the only part that CAN be tested without a driver, a database and a JVM. What the options
then do at the far end is VB-01, VB-21 and VB-12; none of it is asserted here.
"""

from __future__ import annotations

import pytest

from conftest import FakeJdbcSpark, FakeSecrets, LoadedFrame, make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.framework.logs import MASK, redact
from kafka_ingest.sources.oracle import reader
from kafka_ingest.sources.oracle.query import build_query

QUERY = "SELECT * FROM CLAIMS.CLAIM_HEADER"


def _cfg(config_root, **settings):
    write_oracle_source(config_root, **settings)
    return make_oracle_cfg(config_root)


def _options(config_root, bounds=None, **settings):
    return reader.read_options(_cfg(config_root, **settings), FakeSecrets(), QUERY, bounds)


# --------------------------------------------------------------------------------------
# The three non-negotiable options
# --------------------------------------------------------------------------------------


def test_the_query_is_passed_as_a_parenthesised_subquery_in_dbtable(oracle_config_root):
    """Never the `query` option: the two have historically been mutually exclusive with
    `partitionColumn`, and the failure mode is a SILENT fall back to one partition. This
    form works under either answer to VB-01."""
    options = _options(oracle_config_root)
    assert options["dbtable"] == f"({QUERY}) src"
    assert "query" not in options


def test_the_fetch_size_is_always_set(oracle_config_root):
    """The Oracle JDBC driver's own default is TEN ROWS per round trip. Unset is not 'the
    driver decides', it is an extract that takes hours - and it fails as slowness rather
    than as an error, which is why it is not left to a default anywhere."""
    assert _options(oracle_config_root)["fetchsize"] == "10000"


def test_the_driver_class_is_named(oracle_config_root):
    assert _options(oracle_config_root)["driver"] == "oracle.jdbc.OracleDriver"


def test_the_query_timeout_is_passed_even_when_it_is_zero(oracle_config_root):
    """Zero is the JDBC option's own 'no timeout' and is what the platform ships. Passing
    it explicitly means the audited options map answers the question rather than leaving a
    reader to wonder which default applied."""
    assert _options(oracle_config_root)["queryTimeout"] == "0"
    assert _options(oracle_config_root, query_timeout=1800)["queryTimeout"] == "1800"


# --------------------------------------------------------------------------------------
# Optional options appear only when configured
# --------------------------------------------------------------------------------------


def test_a_session_statement_is_passed_only_when_set(oracle_config_root):
    """It runs once per CONNECTION, i.e. once per partition - which is why it is opt-in and
    why config.py checks its shape."""
    assert "sessionInitStatement" not in _options(oracle_config_root)
    options = _options(oracle_config_root, session_init="ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
    assert options["sessionInitStatement"].startswith("ALTER SESSION")


def test_custom_schema_is_passed_only_when_a_type_is_overridden(oracle_config_root):
    assert "customSchema" not in _options(oracle_config_root)
    options = _options(oracle_config_root, column_types={"AMOUNT": "DECIMAL(38,10)"})
    assert options["customSchema"] == "AMOUNT DECIMAL(38,10)"


# --------------------------------------------------------------------------------------
# Parallelism: all four options, or none
# --------------------------------------------------------------------------------------


def test_a_serial_read_sets_none_of_the_partition_options(oracle_config_root):
    """`numPartitions` alone does not split anything - Spark needs the column and both
    bounds to generate the per-partition WHERE clauses."""
    options = _options(oracle_config_root)
    for option in ("partitionColumn", "lowerBound", "upperBound", "numPartitions"):
        assert option not in options


def test_a_partitioned_read_sets_all_four(oracle_config_root):
    options = _options(oracle_config_root, bounds=(1, 5000), num_partitions=8, partition_column="CLAIM_ID")
    assert options["partitionColumn"] == "CLAIM_ID"
    assert (options["lowerBound"], options["upperBound"]) == ("1", "5000")
    assert options["numPartitions"] == "8"


def test_the_bounds_come_from_the_query_the_run_will_extract(oracle_config_root):
    """Over the QUERY, not the table: bounds taken from the whole table would slice a
    filtered extract into partitions that are mostly empty, and the last one would do all
    the work while the run still succeeded."""
    cfg = _cfg(
        oracle_config_root,
        num_partitions=4,
        partition_column="CLAIM_ID",
        filter_column="STATUS",
        filter_criteria="IN ('A')",
    )
    spark = FakeJdbcSpark(frames=[LoadedFrame([{"lower_bound": 10, "upper_bound": 99}])])
    assert reader.partition_bounds(spark, cfg, FakeSecrets(), build_query(cfg)) == (10, 99)
    assert "STATUS IN ('A')" in spark.options_for(0)["dbtable"]
    assert "MIN(CLAIM_ID)" in spark.options_for(0)["dbtable"]


def test_the_bounds_probe_never_opens_the_extract_s_connections(oracle_config_root):
    """A probe returns one row. Handing it the extract's parallelism would open
    `num_partitions` connections to fetch one value."""
    cfg = _cfg(oracle_config_root, num_partitions=8, partition_column="CLAIM_ID")
    spark = FakeJdbcSpark(frames=[LoadedFrame([{"lower_bound": 1, "upper_bound": 2}])])
    reader.partition_bounds(spark, cfg, FakeSecrets(), QUERY)
    assert "numPartitions" not in spark.options_for(0)


def test_no_partition_column_means_no_probe_at_all(oracle_config_root):
    """A serial read has nothing to bound, so it must not pay for a round trip to find that
    out."""
    spark = FakeJdbcSpark()
    assert reader.partition_bounds(spark, _cfg(oracle_config_root), FakeSecrets(), QUERY) is None
    assert spark.reads == []


def test_an_empty_extract_falls_back_to_a_serial_read(oracle_config_root):
    """`lowerBound=None` is not an empty read - it is an error four options deep in a stack
    trace. A query matching no rows is an ordinary Tuesday."""
    cfg = _cfg(oracle_config_root, num_partitions=4, partition_column="CLAIM_ID")
    spark = FakeJdbcSpark(frames=[LoadedFrame([{"lower_bound": None, "upper_bound": None}])])
    assert reader.partition_bounds(spark, cfg, FakeSecrets(), QUERY) is None


def test_a_partition_count_of_one_needs_no_bounds(oracle_config_root):
    cfg = _cfg(oracle_config_root, num_partitions=1, partition_column="CLAIM_ID")
    spark = FakeJdbcSpark()
    assert reader.partition_bounds(spark, cfg, FakeSecrets(), QUERY) is None
    assert spark.reads == []


def test_bounds_cannot_be_asked_for_without_a_partition_column(oracle_config_root):
    from kafka_ingest.sources.oracle.query import bounds_query

    with pytest.raises(ConfigError, match="partition_column"):
        bounds_query(_cfg(oracle_config_root), QUERY)


# --------------------------------------------------------------------------------------
# The read itself, and what it must never leak
# --------------------------------------------------------------------------------------


def test_the_read_is_the_jdbc_source_with_exactly_those_options(oracle_config_root):
    cfg = _cfg(oracle_config_root)
    spark = FakeJdbcSpark(frames=[LoadedFrame([{"CLAIM_ID": 1}])])
    frame = reader.read(spark, cfg, FakeSecrets(), QUERY, None)

    assert spark.reads[0].format_used == "jdbc"
    assert spark.options_for(0)["dbtable"] == f"({QUERY}) src"
    assert frame.count() == 1


def test_no_credential_appears_in_a_redacted_read_options_map(oracle_config_root):
    """The options map is logged by this module and reaches the audit row's source_detail
    through run.py. Both go through the framework's redactor - this is that assertion on
    the FULL read map, not just the connection half."""
    secrets = FakeSecrets({("kv-oracle-dev", "oracle-password"): "s3cret"})
    options = reader.read_options(_cfg(oracle_config_root), secrets, QUERY, None)
    redacted = redact(options)

    assert redacted["password"] == MASK
    assert "s3cret" not in str(redacted)
    assert redacted["dbtable"] == f"({QUERY}) src"
