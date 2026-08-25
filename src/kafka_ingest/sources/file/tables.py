"""The landing table: its column list, and its creation. NO PySpark import.

ONE Delta table per file source, PARTITIONED BY the configured `landing_partition_by`.
There is no curated layer and no quarantine layer for Files (CORE section 10): Auto
Loader's own `rescuedDataColumn` is this source's quarantine, landed as a column rather
than routed to a second table - see the module docstring in run.py.

WHY THE TABLE IS CREATED EXPLICITLY RATHER THAN BY THE FIRST WRITE
--------------------------------------------------------------------
Exactly sources/oracle/tables.py's reasoning: an implicitly created Delta table gets no
TBLPROPERTIES and no partitioning, and the column list cannot be a static constant because
it depends on the source's own columns (from `schema:`, from `cloudFiles.schemaHints`, or
from inference) - but that shape IS known on the driver once the batch's schema resolves,
before a single row is written. So the DDL is built from that schema, and creation looks
the same as everywhere else in the framework.
"""

from __future__ import annotations

from typing import Any

from ...framework.config import ConfigError
from .config import FileConfig

# The columns landing.py derives from `_metadata` and `filename_columns`, plus Auto
# Loader's rescued-data column and the framework's own provenance columns. Matches
# sources/file/landing.py's projection ORDER exactly - tests/test_file_tables.py asserts
# the two agree, the same drift check Oracle and Kafka run against their own landing.py.
FIXED_METADATA_DDL = """
    _file_path               STRING    COMMENT 'From _metadata.file_path - see VB-06',
    _file_name               STRING    COMMENT 'From _metadata.file_name',
    _file_size                BIGINT    COMMENT 'From _metadata.file_size, bytes',
    _file_modification_time   TIMESTAMP COMMENT 'From _metadata.file_modification_time',
    _rescued_data              STRING    COMMENT 'cloudFiles.rescuedDataColumn - non-NULL is this source''s quarantine',
    source_key                 STRING    COMMENT 'Matches conf/sources/<source_key>.yaml',
    ingest_ts                  TIMESTAMP COMMENT 'When this row was WRITTEN here',
    ingest_date                DATE      COMMENT 'Partition column candidate. The date of ingest_ts',
    ingested_via               STRING    COMMENT 'primary - no other run type exists for this source yet',
    replay_run_id              STRING    COMMENT 'Reserved for a future file replay; always NULL today',
    txn_version                 BIGINT    COMMENT 'Delta txnVersion: this run''s Spark microbatch id, or -1',
    run_id                      STRING    COMMENT 'Joins to ingest_audit.run_id'
"""


def filename_column_ddl(cfg: FileConfig) -> str:
    """One STRING column per `filename_columns` entry - the value is always text extracted
    by a regex, never re-typed."""
    return ",\n".join(
        f"    {column.column} STRING COMMENT 'Derived from the file name via filename_columns'"
        for column in cfg.filename_columns
    )


def landing_columns(schema: Any, cfg: FileConfig) -> str:
    """The DDL column list: the source's own columns, in order, then the derived and
    provenance columns.

    `_rescued_data` is EXCLUDED from the source's own columns even though Auto Loader adds
    it to the real read schema (unlike `_metadata`, which is virtual and never appears in
    `schema.fields` at all) - it is declared exactly once, in FIXED_METADATA_DDL, rather
    than trusted to land wherever the raw schema happens to put it.
    """
    reserved = {"_rescued_data", *(column.column for column in cfg.filename_columns)}
    source_columns = [
        f"    {field.name} {field.dataType.simpleString()}" for field in schema.fields if field.name not in reserved
    ]
    if not source_columns:
        raise ConfigError("the read returned no columns; refusing to create an empty landing table")
    parts = [",\n".join(source_columns)]
    filename_ddl = filename_column_ddl(cfg)
    if filename_ddl:
        parts.append(filename_ddl)
    parts.append(FIXED_METADATA_DDL.strip("\n"))
    return ",\n".join(parts)


def ensure_landing_table(ctx: Any, cfg: FileConfig, schema: Any) -> None:
    """CREATE TABLE IF NOT EXISTS for this source's landing table. A no-op once it exists.

    Called AFTER the batch's schema resolves and BEFORE the write, which is the only window
    in which both facts are known: what the columns are, and that nothing has been written.
    """
    ctx.tables.ensure_table(
        ctx.spark,
        cfg.landing_table,
        landing_columns(schema, cfg),
        f"Landing mirror of {cfg.full_source_path}. Written by the ingestion job only.",
        properties=cfg.table_properties,
        partition_by=list(cfg.landing_partition_by),
    )
