"""The landing projection: the source's own columns, plus metadata ABOUT the file they
came from, nothing else interpreted.

Every column Auto Loader returns for the row is kept as-is - this layer does not reshape
the payload, exactly as Kafka's landing.py keeps `value` verbatim and Oracle's keeps every
extracted column. What is added here is metadata about WHERE the row came from: the four
`_metadata` fields Databricks documents (file path, name, size, modification time - see
VB-06), any `filename_columns` the source configured, the rescued-data column Auto Loader
already produced, and the framework's own provenance columns.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import FileConfig

RESCUED_DATA_COLUMN = "_rescued_data"

# Order matches sources/file/tables.py FIXED_METADATA_DDL exactly - a test asserts it, the
# same drift check Oracle and Kafka run against their own landing.py/tables.py pair.
METADATA_COLUMNS = (
    "_file_path",
    "_file_name",
    "_file_size",
    "_file_modification_time",
    RESCUED_DATA_COLUMN,
    "source_key",
    "ingest_ts",
    "ingest_date",
    "ingested_via",
    "replay_run_id",
    "txn_version",
    "run_id",
)


def _filename_column(pattern: str, path_col: Column) -> Column:
    """Extract one `filename_columns` value from the file path via its configured regex.

    `regexp_extract` returns an empty string, never NULL, when the pattern does not match -
    which is the honest answer for a file that does not fit the naming convention a
    `filename_columns` entry describes, rather than failing the whole batch over it.
    """
    return F.regexp_extract(path_col, pattern, 1)


def project(batch_df: DataFrame, cfg: FileConfig, txn_version: int, run_id: str) -> DataFrame:
    """Shape one Auto Loader microbatch into the landing table's schema.

    Column order matches tables.FIXED_METADATA_DDL exactly, appended after the source's own
    columns (in the order Auto Loader returned them) and any `filename_columns`.
    """
    path_col = F.col("_metadata.file_path")
    df = batch_df
    if RESCUED_DATA_COLUMN not in df.columns:
        # Only reachable when rescuedDataColumn was disabled upstream - kept so landing has
        # one stable schema regardless, exactly as Kafka's landing keeps `headers` present
        # even when a caller read with includeHeaders off.
        df = df.withColumn(RESCUED_DATA_COLUMN, F.lit(None).cast("string"))

    for filename_column in cfg.filename_columns:
        df = df.withColumn(filename_column.column, _filename_column(filename_column.pattern, path_col))

    return (
        df.withColumn("_file_path", path_col)
        .withColumn("_file_name", F.col("_metadata.file_name"))
        .withColumn("_file_size", F.col("_metadata.file_size"))
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("source_key", F.lit(cfg.source_key))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.current_date())
        .withColumn("ingested_via", F.lit(cfg.run_type))
        .withColumn("replay_run_id", F.lit(None).cast("string"))
        .withColumn("txn_version", F.lit(txn_version).cast("bigint"))
        .withColumn("run_id", F.lit(run_id))
    )


def rescued_count(df: DataFrame) -> int:
    """How many rows in this (already-written) batch carry a non-NULL rescued-data column.

    This source's quarantine signal - see the module docstring in run.py. Counted from the
    SAME cached frame the write used, never a second read.
    """
    return df.where(F.col(RESCUED_DATA_COLUMN).isNotNull()).count()
