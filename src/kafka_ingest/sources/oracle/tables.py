"""The landing table: its column list, and its creation. NO PySpark import.

ONE Delta table per Oracle table, PARTITIONED BY (ingest_date). There is no curated layer
and no quarantine layer for Oracle (CORE section 10): a JDBC read returns typed rows or it
fails, so there is nothing to quarantine and nothing to parse.

WHY THE TABLE IS CREATED EXPLICITLY RATHER THAN BY THE FIRST WRITE
------------------------------------------------------------------
An implicitly created Delta table gets no TBLPROPERTIES and no partitioning - so the table
people actually query would be the only one in the platform without auto-compaction, and it
would be partitioned by nothing until somebody noticed. The column list cannot be a
constant the way Kafka's landing table is, because it is whatever the Oracle table holds;
but it IS known on the driver once the read's schema resolves, before a single row is
written. So the DDL is built from that schema, and creation looks the same as everywhere
else in the framework.

The schema is read through `.fields`, `.name` and `.dataType.simpleString()` by duck
typing, exactly as sources/oracle/types.py does, which is what keeps this module free of a
PySpark import and testable with no JVM.
"""

from __future__ import annotations

from typing import Any

from ...framework.config import ConfigError
from .config import OracleConfig

# The provenance columns, matching sources/oracle/landing.py's projection ORDER exactly. A
# drift between the two surfaces as a confusing Delta schema error on the first append and
# nowhere earlier, so tests/test_oracle_tables.py asserts they agree.
METADATA_DDL = """
    source_key       STRING    COMMENT 'Matches conf/sources/<source_key>.yaml',
    ingest_ts        TIMESTAMP COMMENT 'When this row was WRITTEN here, not when it changed in Oracle',
    ingest_date      DATE      COMMENT 'Partition column. The date of ingest_ts',
    ingested_via     STRING    COMMENT 'primary | oracle_replay - which kind of run wrote this row',
    replay_run_id    STRING    COMMENT 'rerun_id when a replay wrote this row, NULL otherwise',
    txn_version      BIGINT    COMMENT 'Delta txnVersion: the run_sequence from ingest_state',
    run_id           STRING    COMMENT 'Joins to ingest_audit.run_id'
"""


def landing_columns(schema: Any) -> str:
    """The DDL column list: Oracle's own columns, in order, then the metadata columns."""
    source_columns = [f"    {field.name} {field.dataType.simpleString()}" for field in schema.fields]
    if not source_columns:
        raise ConfigError("the extract returned no columns; refusing to create an empty landing table")
    return ",\n".join(source_columns) + ",\n" + METADATA_DDL.strip("\n")


def ensure_landing_table(ctx: Any, cfg: OracleConfig, schema: Any) -> None:
    """CREATE TABLE IF NOT EXISTS for this source's landing table. A no-op once it exists.

    Called AFTER the read's schema resolves and BEFORE the write, which is the only window
    in which both facts are known: what the columns are, and that nothing has been written.
    """
    ctx.tables.ensure_table(
        ctx.spark,
        cfg.landing_table,
        landing_columns(schema),
        f"Landing mirror of Oracle {cfg.source_ref}. Written by the ingestion job only.",
        properties=cfg.table_properties,
        partition_by=list(cfg.landing_partition_by),
    )
