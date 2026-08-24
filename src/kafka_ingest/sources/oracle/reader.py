"""Building the JDBC read. One options map, two shapes of read.

  the extract     the configured query, optionally split across `num_partitions` JDBC
                  connections when a `partition_column` is configured
  a scalar probe  one row, one connection: the partition bounds, and (in 4c) the run's
                  high-water mark

THREE OPTIONS ARE NOT NEGOTIABLE
--------------------------------
  fetchsize        the Oracle JDBC driver's own default is TEN ROWS per network round
                   trip. Unset is not "the driver decides", it is an extract that takes
                   hours instead of minutes, and it fails as slowness rather than as an
                   error.
  dbtable          the query is passed as a parenthesised subquery in `dbtable`, never as
                   the `query` option: the two have historically been mutually exclusive
                   with `partitionColumn`, and the failure mode is a SILENT fall back to a
                   single partition. VB-01 is the check; this code takes the form that
                   works under both answers.
  driver           named explicitly. Left out, the JVM's driver auto-discovery decides,
                   and on a cluster with two Oracle drivers on the classpath it decides
                   differently than it did in dev.

PARTITION BOUNDS ARE READ, NOT CONFIGURED
------------------------------------------
`lowerBound` / `upperBound` DO NOT FILTER - they only decide where Spark cuts the range
into `numPartitions` slices. Configuring them would mean a value that goes stale silently:
an id range grows, every new row lands in the last slice, and one task does all the work
while the run still succeeds. So they are read with a `SELECT MIN(col), MAX(col)` over the
same query the extract will run - one cheap round trip against a column that is almost
always indexed, in exchange for bounds that cannot go stale. VB-21.

NO PYSPARK IMPORT: the session arrives as an argument, so every option this module builds
can be asserted with a recording stand-in and no JVM.
"""

from __future__ import annotations

import logging
from typing import Any

from ...framework.config import ConfigError
from ...framework.security import SecretResolver, redact
from . import types
from .config import OracleConfig
from .query import bounds_query
from .security import build_connection_options

LOG = logging.getLogger(__name__)


def read_options(
    cfg: OracleConfig, secrets: SecretResolver, query: str, bounds: tuple[Any, Any] | None
) -> dict[str, str]:
    """Every option the JDBC read needs: connection, extraction, parallelism.

    `bounds` is the (lower, upper) pair from `partition_bounds()`, or None for a serial
    read. Passing it in rather than reading it here keeps this function pure - the map it
    returns is exactly what a test can assert on.
    """
    options = build_connection_options(cfg.jdbc, secrets)

    # A parenthesised subquery, not the `query` option - see the module docstring.
    options["dbtable"] = f"({query}) src"
    options["fetchsize"] = str(cfg.fetch_size)
    options["queryTimeout"] = str(cfg.query_timeout)

    if cfg.session_init:
        # Runs once per CONNECTION, i.e. once per partition. config.py has already checked
        # that it is an ALTER SESSION or a PL/SQL block and nothing else.
        options["sessionInitStatement"] = cfg.session_init

    custom = types.custom_schema(cfg.column_types, cfg.source_key)
    if custom:
        options["customSchema"] = custom

    if bounds is not None:
        lower, upper = bounds
        options["partitionColumn"] = _require_partition_column(cfg)
        options["lowerBound"] = str(lower)
        options["upperBound"] = str(upper)
        options["numPartitions"] = str(cfg.num_partitions)

    LOG.info("JDBC read options for '%s': %s", cfg.source_ref, redact(options))
    return options


def read(spark: Any, cfg: OracleConfig, secrets: SecretResolver, query: str, bounds: tuple[Any, Any] | None) -> Any:
    """The extract itself. Returns a DataFrame; reads nothing until something needs it."""
    return spark.read.format("jdbc").options(**read_options(cfg, secrets, query, bounds)).load()


def partition_bounds(spark: Any, cfg: OracleConfig, secrets: SecretResolver, query: str) -> tuple[Any, Any] | None:
    """MIN and MAX of the partition column over the query this run will extract.

    None means "read serially", and there are two ways to get it, both legitimate: no
    partition column is configured, or the query matches no rows at all. The second is why
    the NULL check is here rather than left to Spark - `lowerBound=None` is not an empty
    read, it is an error four options deep in a stack trace.
    """
    if not cfg.partition_column or cfg.num_partitions <= 1:
        return None
    row = read_scalar_row(spark, cfg, secrets, bounds_query(cfg, query))
    lower, upper = (row["lower_bound"], row["upper_bound"]) if row else (None, None)
    if lower is None or upper is None:
        LOG.info(
            "No partition bounds for '%s' (the extract matches no rows); reading serially.",
            cfg.source_ref,
        )
        return None
    return (lower, upper)


def read_scalar_row(spark: Any, cfg: OracleConfig, secrets: SecretResolver, query: str) -> Any:
    """One row from a probe query, on ONE connection.

    Never partitioned and never given a fetch size worth tuning: a probe returns a single
    row, and handing it the extract's parallelism would open `num_partitions` connections
    to fetch one value.
    """
    options = build_connection_options(cfg.jdbc, secrets)
    options["dbtable"] = f"({query}) probe"
    if cfg.session_init:
        options["sessionInitStatement"] = cfg.session_init
    rows = spark.read.format("jdbc").options(**options).load().collect()
    return rows[0] if rows else None


def _require_partition_column(cfg: OracleConfig) -> str:
    """Belt and braces: config.py refuses `num_partitions > 1` without one."""
    if not cfg.partition_column:
        raise ConfigError(f"source '{cfg.source_key}': a partitioned read needs a partition_column.")
    return cfg.partition_column
