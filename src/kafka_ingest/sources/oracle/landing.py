"""The landing projection: the source's own columns, plus metadata ABOUT the extract.

Nothing here interprets a value. Oracle has already typed every column by the time the
frame arrives, so landing's job is only to record WHERE each row came from and WHEN - and
to do it with column names that match Kafka's landing table, so an operator moving between
two source types does not relearn them.

NO PYSPARK IMPORT, and the projection is built as SQL EXPRESSION STRINGS rather than
Column objects to keep it that way. `selectExpr` takes exactly the strings below, which
means the whole projection is assertable in the fast test suite - and the alternative
(`F.lit(...)`) would need an active SparkContext just to construct.

THE SOURCE'S COLUMNS COME FIRST, verbatim and in the order Oracle returned them, and the
metadata is appended. A source column that collided with one of these names would shadow
it; `_check_no_reserved_collisions` refuses that at run time rather than letting the write
fail with a duplicate-column error nobody can trace back to here.
"""

from __future__ import annotations

import re
from typing import Any

from ...framework.config import ConfigError
from .config import OracleConfig

# The metadata columns, in the order they are appended. `ingest_date` is landing's
# partition column, so it is a DATE and not a truncation of the timestamp beside it.
METADATA_COLUMNS = (
    "source_key",
    "ingest_ts",
    "ingest_date",
    "ingested_via",
    "replay_run_id",
    "txn_version",
    "run_id",
)

# A value this module is willing to embed in a SQL expression as a literal. Every one of
# them is framework-generated - a config filename stem, a run id, a run type - and this
# refuses anything that is not, because a quote here would rewrite the projection.
_SAFE_LITERAL = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def project(df: Any, cfg: OracleConfig, txn_version: int, run_id: str) -> Any:
    """Append the metadata columns to whatever Oracle returned."""
    _check_no_reserved_collisions(df, cfg)
    return df.selectExpr(
        "*",
        f"{_literal(cfg.source_key)} AS source_key",
        "current_timestamp() AS ingest_ts",
        "current_date() AS ingest_date",
        f"{_literal(cfg.run_type)} AS ingested_via",
        f"{_literal(cfg.replay.rerun_id)} AS replay_run_id",
        f"CAST({int(txn_version)} AS BIGINT) AS txn_version",
        f"{_literal(run_id)} AS run_id",
    )


def _check_no_reserved_collisions(df: Any, cfg: OracleConfig) -> None:
    """An Oracle column called `run_id` would shadow this run's own provenance.

    Refused with the column name and the fix, because the error it prevents - a duplicate
    column on write - names neither this module nor the source table.
    """
    collisions = sorted(set(df.columns) & set(METADATA_COLUMNS))
    if collisions:
        raise ConfigError(
            f"source '{cfg.source_key}': the extract returns {collisions}, which landing uses "
            "for its own provenance columns. Rename them in a `sql_query:` projection "
            f"(SELECT ..., X AS SRC_X) - landing's metadata columns are {list(METADATA_COLUMNS)}."
        )


def _literal(value: Any) -> str:
    """A framework-generated string as a SQL literal, or NULL. Never source data."""
    if value is None:
        return "CAST(NULL AS STRING)"
    text = str(value)
    if not _SAFE_LITERAL.match(text):
        raise ConfigError(
            f"'{text}' cannot be embedded in the landing projection - it is expected to be a "
            "framework-generated identifier (a source key, a run id, a run type)."
        )
    return f"'{text}'"
