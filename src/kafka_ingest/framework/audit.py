"""Audit rows: the first thing support queries during an incident.

ONE table for every source of every type. ONE row per (run, layer, status) transition. A
healthy run of a three-layer source writes:

    layer=run        status=STARTED
    layer=landing    status=COMPLETED   record_count=N
    layer=curated    status=COMPLETED   record_count=M
    layer=run        status=COMPLETED   record_count=N  position_start/end

Rows are written as each layer finishes, not buffered to the end. If the driver dies
between two of them, the earlier row is already durable - which is what makes "which layer
was it on when it died?" a lookup instead of a guess.

AUDIT WRITES MUST NEVER RAISE
-----------------------------
Every write is wrapped; a failure is logged loudly and swallowed. Auditing must never be
the reason a good batch fails. This is the deliberate opposite of framework/state.py, whose
writes are mandatory and must raise - extraction correctness depends on state and must not
depend on best-effort audit. Do not "make these consistent".

The consequence is that the audit table is EVIDENCE, not a source of truth. Never derive a
watermark, an offset or a run sequence from it.

THREE COLUMNS THAT MEAN THREE THINGS
------------------------------------
`position_start` / `position_end` are text, and what they hold depends on `source_type`:
a Kafka offsets JSON, a database cursor value, a file boundary. `source_detail` is a JSON
STRING - not a MAP - so a new source type can record whatever it needs without an ALTER
TABLE on the one table every source shares. Both facts are repeated as column COMMENTs,
because a support engineer reads the column before they read any document.

KNOWN CAVEAT - read before trusting record_count
------------------------------------------------
`record_count` is the number of rows PRESENTED to the write, not the number the target
actually inserted. If Delta skips a write as a duplicate (see the txnAppId/txnVersion
markers in framework/writers.py), this column still reports the presented count.

`txn_version` (docs/build_log/DECISIONS.md D-03) CARRIES THE DELTA txnVersion
-------------------------------------------------------------------------------
Whatever produced it: a streaming source's microbatch id, a batch source's `run_sequence`,
or -1 when neither applies. One column, one name that says what it is, for every source
type - the column used to be called `batch_id`, which read as Kafka vocabulary sitting in a
table every source shares.
"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import tables

LOG = logging.getLogger(__name__)

# The framework's own layer. Sources use their own layer names, which come from their
# SOURCE_SPEC - this one covers the run as a whole, which is the only thing the runner
# knows about.
LAYER_RUN = "run"

STATUS_STARTED = "STARTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
STATUS_NO_DATA = "NO_DATA"

# A run with no microbatch id. Kafka's foreachBatch supplies a real one; a bounded batch
# read supplies its run_sequence; anything else records that it had neither.
NO_TXN_VERSION = -1

# Error text is truncated before it reaches the table: a Spark stack trace can be
# megabytes, and an audit table is not a log aggregator.
_ERROR_MESSAGE_LIMIT = 4000

# The physical table. AUDIT_SCHEMA below builds the in-memory DataFrame, this is the table
# on disk, and sql/02_layer_tables.sql provisions the same columns ahead of the first run.
# A test asserts all three agree, because drift surfaces as a confusing Delta schema error
# on the first append and nowhere earlier.
AUDIT_DDL_COLUMNS = """
    audit_id              STRING    COMMENT 'run_id::txn_version::layer::status - unique per row',
    run_id                STRING    COMMENT 'One value per job execution; also stamped on data rows',
    txn_version           BIGINT    COMMENT 'Delta txnVersion: a microbatch id, a batch source run_sequence, or -1',
    source_type           STRING    COMMENT 'Which source implementation ran',
    source_key            STRING    COMMENT 'Matches conf/sources/<source_key>.yaml and ingest_control.source_key',
    source_ref            STRING    COMMENT 'Source-side identifier: topic name, SCHEMA.TABLE, or path glob',
    domain                STRING    COMMENT 'Owning team',
    layer                 STRING    COMMENT 'run (this file), or a source''s own - e.g. Kafka''s LAYER_STREAM',
    status                STRING    COMMENT 'STARTED | COMPLETED | FAILED | SKIPPED | NO_DATA',
    record_count          BIGINT    COMMENT 'Rows PRESENTED to the write - see the caveat in framework/audit.py',
    quarantined_count     BIGINT,
    event_ts              TIMESTAMP COMMENT 'When this transition was recorded',
    duration_ms           BIGINT    COMMENT 'Time spent in this layer',
    run_type              STRING    COMMENT 'primary, or a source-specific replay type',
    rerun_id              STRING    COMMENT 'A replay id; on a primary run, a reset id that forked the identity',
    job_run_id            STRING    COMMENT 'The Databricks Workflows run id, when there is one',
    position_start        STRING    COMMENT 'THREE MEANINGS by source_type: Kafka offsets, a cursor, a file boundary',
    position_end          STRING    COMMENT 'Upper read boundary. Same three meanings as position_start',
    source_detail         STRING    COMMENT 'JSON STRING, not a map - a new source type forces no ALTER TABLE',
    pending_work          BIGINT    COMMENT 'Outstanding work at end of run. NULL means the source cannot know',
    error_class           STRING,
    error_message         STRING    COMMENT 'Truncated to 4000 characters',
    audit_date            DATE      COMMENT 'Partition key: date of event_ts'
"""

# Must stay aligned with AUDIT_DDL_COLUMNS above. The DDL owns the physical table; this
# owns the DataFrame built in memory.
AUDIT_SCHEMA = StructType(
    [
        StructField("audit_id", StringType()),
        StructField("run_id", StringType()),
        StructField("txn_version", LongType()),
        StructField("source_type", StringType()),
        StructField("source_key", StringType()),
        StructField("source_ref", StringType()),
        StructField("domain", StringType()),
        StructField("layer", StringType()),
        StructField("status", StringType()),
        StructField("record_count", LongType()),
        StructField("quarantined_count", LongType()),
        StructField("event_ts", TimestampType()),
        StructField("duration_ms", LongType()),
        StructField("run_type", StringType()),
        StructField("rerun_id", StringType()),
        StructField("job_run_id", StringType()),
        StructField("position_start", StringType()),
        StructField("position_end", StringType()),
        StructField("source_detail", StringType()),
        StructField("pending_work", LongType()),
        StructField("error_class", StringType()),
        StructField("error_message", StringType()),
        StructField("audit_date", DateType()),
    ]
)


def ensure_audit_table(spark: Any, cfg: Any, table: str) -> None:
    """Create the shared audit table if it does not exist yet. A no-op afterwards."""
    tables.ensure_table(
        spark,
        table,
        AUDIT_DDL_COLUMNS,
        "Per-run, per-layer ingestion status for every source. First stop for incident triage.",
        properties=cfg.get("table_properties"),
        partition_by=["audit_date"],
    )


class AuditWriter:
    """Emits status rows for one run. Built once by the runner and carried on RunContext.

    One row per write rather than a buffered flush: during an incident support needs to see
    the landing row while the curated write is still running, and a buffer lost when the
    driver dies is worse than useless. Small-file pressure is handled by the table's own
    auto-compaction properties.

    `source_ref` is a public attribute rather than a constructor argument because only the
    source knows its own source-side identifier - the topic name, the SCHEMA.TABLE, the
    path glob - and the framework must not learn the config key it lives under. A source
    sets it once at the top of run(); rows written before that carry NULL.
    """

    def __init__(self, spark: Any, cfg: Any, table: str, run_id: str, run_type: str, job_run_id: str | None = None):
        self._spark = spark
        self._cfg = cfg
        self.table = table
        self.run_id = run_id
        self.run_type = run_type
        self.job_run_id = job_run_id
        self.source_ref: str | None = None
        # Seeded from configuration, then writable for the same reason source_ref is: a
        # replay's id IS configuration, but a checkpoint-based source that deliberately
        # forks its write identity mid-lifecycle knows an id the configuration layer never
        # sees as `rerun_id`. Both end up in one column because `run_type` tells them
        # apart - a primary run with a non-NULL rerun_id is a reset, by construction.
        self.rerun_id: str | None = cfg.get("rerun_id")

    def emit(self, layer: str, status: str, txn_version: int = NO_TXN_VERSION, **details: Any) -> None:
        """Write one audit row. Never raises.

        Accepted `details` keys are AUDIT_SCHEMA field names: record_count,
        quarantined_count, duration_ms, position_start, position_end, source_detail,
        error_class, error_message.
        """
        try:
            row = self.build_row(layer, status, txn_version, **details)
            # Positional tuple in AUDIT_SCHEMA order, not the dict: createDataFrame with an
            # explicit StructType does not reorder dict keys, and a KeyError here would be
            # a better failure than a value landing silently in the wrong column.
            values = tuple(row[field.name] for field in AUDIT_SCHEMA.fields)
            if self._spark is None:
                # Only reachable from a caller that supplied no SparkSession - the disabled
                # short-circuit in a test. Stated rather than thrown, so a real audit
                # failure is not hidden among AttributeErrors.
                LOG.warning("No SparkSession; audit row not written: %s/%s %s", layer, status, self.run_id)
                return
            (
                self._spark.createDataFrame([values], schema=AUDIT_SCHEMA)
                .write.format("delta")
                .mode("append")
                .saveAsTable(self.table)
            )
        except Exception:  # noqa: BLE001 - auditing must never fail a good run
            LOG.error(
                "Failed to write audit row (%s/%s run %s): %s", layer, status, self.run_id, traceback.format_exc()
            )

    def build_row(self, layer: str, status: str, txn_version: int = NO_TXN_VERSION, **kw: Any) -> dict[str, Any]:
        cfg, now = self._cfg, datetime.now(timezone.utc)
        message = kw.get("error_message")
        detail = kw.get("source_detail")
        return {
            "audit_id": f"{self.run_id}::{txn_version}::{layer}::{status}",
            "run_id": self.run_id,
            "txn_version": int(txn_version),
            "source_type": cfg.source_type,
            "source_key": cfg.source_key,
            "source_ref": self.source_ref,
            "domain": cfg.get("domain"),
            "layer": layer,
            "status": status,
            "record_count": _as_long(kw.get("record_count")),
            "quarantined_count": _as_long(kw.get("quarantined_count")),
            "event_ts": now,
            "duration_ms": _as_long(kw.get("duration_ms")),
            "run_type": self.run_type,
            "rerun_id": self.rerun_id,
            "job_run_id": self.job_run_id,
            "position_start": kw.get("position_start"),
            "position_end": kw.get("position_end"),
            "source_detail": detail if isinstance(detail, (str, type(None))) else json.dumps(detail),
            "pending_work": _as_long(kw.get("pending_work")),
            "error_class": kw.get("error_class"),
            "error_message": (message or "")[:_ERROR_MESSAGE_LIMIT] or None,
            "audit_date": now.date(),
        }


def _as_long(value: Any) -> int | None:
    return int(value) if value is not None else None
