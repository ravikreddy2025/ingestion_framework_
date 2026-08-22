"""Audit rows: the first thing support queries during an incident.

ONE table. ONE row per (batch, layer, status) transition. A healthy microbatch writes:

    layer=landing  status=STARTED
    layer=landing  status=COMPLETED   record_count=N
    layer=curated  status=STARTED
    layer=curated  status=COMPLETED   record_count=M  quarantined_count=Q
    layer=stream   status=COMPLETED   + Kafka start/end offsets, from Spark's own metrics

WHY PER-LAYER ROWS, WRITTEN INSIDE THE MICROBATCH
-------------------------------------------------
The landing and curated rows are written immediately after each layer finishes, not
buffered to the end. If the driver dies between the two writes, the landing COMPLETED row
is already durable - which is what makes "which layer was it on when it died?" a lookup
instead of a guess.

The layer=stream rows come from a StreamingQueryListener, because batch id, input row
count and per-partition Kafka offsets are Spark's numbers, not ours.

The two sources run on different threads (foreachBatch vs the listener thread). They need
no coordination because they write DIFFERENT rows - an earlier design joined them on batch
id, which needed a lock and bought nothing.

Auditing must never be the reason a good batch fails: every write is wrapped, and a
failure to record is logged loudly rather than raised.

KNOWN CAVEAT - read before trusting record_count
------------------------------------------------
`record_count` is the number of rows PRESENTED to the write, not the number Delta actually
inserted. If Delta skips a write as a duplicate (see docs/DESIGN.md, "Re-runs and
duplicates"), this column still reports the presented count. It cannot be cheaply
corrected: reading the table's last operation metrics would race with a concurrent replay
of the same topic. The startup guard in pipeline.py catches the situation that causes the
discrepancy, which is the proportionate fix.
"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import TopicConfig

LOG = logging.getLogger(__name__)

LAYER_STREAM = "stream"
LAYER_LANDING = "landing"
LAYER_CURATED = "curated"

STATUS_STARTED = "STARTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
STATUS_NO_DATA = "NO_DATA"

# Must stay aligned with tables.AUDIT_DDL_COLUMNS. The DDL owns the physical table; this
# owns the DataFrame built in memory. A test asserts they match, because drift would
# otherwise surface as a confusing Delta schema error on the first append.
AUDIT_SCHEMA = StructType([
    StructField("audit_id", StringType()),
    StructField("run_id", StringType()),
    StructField("batch_id", LongType()),
    StructField("topic_key", StringType()),
    StructField("topic", StringType()),
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
    StructField("starting_offsets", StringType()),
    StructField("ending_offsets", StringType()),
    StructField("writer_schema_ids", ArrayType(IntegerType())),
    StructField("reader_schema_id", IntegerType()),
    StructField("checkpoint_path", StringType()),
    StructField("error_class", StringType()),
    StructField("error_message", StringType()),
    StructField("spark_progress_json", StringType()),
    StructField("audit_date", DateType()),
])


class AuditWriter:
    """Emits status rows. Constructed once per run, shared by the pipeline and listener.

    One row per append rather than a buffered flush: during an incident support needs to
    see batch N's landing row while batch N+1 is still running, and a buffer lost when the
    driver dies is worse than useless. Small-file pressure is handled by autoOptimize on
    the table (see tables.py).
    """

    def __init__(self, spark: SparkSession, cfg: TopicConfig, run_id: str):
        self._spark = spark
        self._cfg = cfg
        self.run_id = run_id

    def emit(self, layer: str, status: str, batch_id: int, **details: Any) -> None:
        """Write one audit row. Accepted `details` keys are the AUDIT_SCHEMA field names:
        record_count, quarantined_count, duration_ms, starting_offsets, ending_offsets,
        writer_schema_ids, reader_schema_id, error_class, error_message,
        spark_progress_json.
        """
        row = self.build_row(layer, status, batch_id, **details)
        try:
            # Positional tuple in AUDIT_SCHEMA order, not the dict: createDataFrame with an
            # explicit StructType does not reorder dict keys, and a KeyError here is a
            # better failure than a column silently landing in the wrong slot.
            values = tuple(row[f.name] for f in AUDIT_SCHEMA.fields)
            (
                self._spark.createDataFrame([values], schema=AUDIT_SCHEMA)
                .write.format("delta").mode("append")
                .saveAsTable(self._cfg.audit_table)
            )
        except Exception:  # noqa: BLE001 - never fail a good batch over an audit row
            LOG.error("Failed to write audit row (%s/%s batch %s): %s",
                      layer, status, batch_id, traceback.format_exc())

    def build_row(self, layer: str, status: str, batch_id: int, **kw: Any) -> Dict[str, Any]:
        cfg, now = self._cfg, datetime.now(timezone.utc)
        message = kw.get("error_message")
        schema_ids: Optional[List[int]] = kw.get("writer_schema_ids")
        return {
            "audit_id": f"{self.run_id}::{batch_id}::{layer}::{status}",
            "run_id": self.run_id,
            "batch_id": int(batch_id),
            "topic_key": cfg.topic_key,
            "topic": cfg.topic,
            "domain": cfg.domain,
            "layer": layer,
            "status": status,
            "record_count": _as_long(kw.get("record_count")),
            "quarantined_count": _as_long(kw.get("quarantined_count")),
            "event_ts": now,
            "duration_ms": _as_long(kw.get("duration_ms")),
            "run_type": cfg.run.run_type,
            "rerun_id": cfg.run.rerun_id,
            "job_run_id": cfg.run.job_run_id,
            "starting_offsets": kw.get("starting_offsets"),
            "ending_offsets": kw.get("ending_offsets"),
            "writer_schema_ids": [int(i) for i in schema_ids] if schema_ids else None,
            "reader_schema_id": kw.get("reader_schema_id"),
            "checkpoint_path": cfg.checkpoint_path,
            "error_class": kw.get("error_class"),
            "error_message": (message or "")[:4000] or None,
            "spark_progress_json": kw.get("spark_progress_json"),
            "audit_date": now.date(),
        }


def _as_long(value: Any) -> Optional[int]:
    return int(value) if value is not None else None


class StreamAuditListener(StreamingQueryListener):
    """Emits layer='stream' rows from Spark's own batch metrics.

    Registered for the duration of one query and removed afterwards - leaving listeners
    attached across runs on a shared cluster would write rows for other topics' queries.
    """

    def __init__(self, audit: AuditWriter):
        self._audit = audit

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark-defined name
        try:
            LOG.info("Query started: run_id=%s sparkQueryId=%s", self._audit.run_id, event.id)
            self._audit.emit(LAYER_STREAM, STATUS_STARTED, batch_id=-1)
        except Exception:  # noqa: BLE001 - Spark swallows listener exceptions
            LOG.error("onQueryStarted audit failed: %s", traceback.format_exc())

    def onQueryProgress(self, event) -> None:  # noqa: N802
        try:
            progress = _progress_dict(event)
            source = (progress.get("sources") or [{}])[0]
            records_read = int(progress.get("numInputRows") or 0)
            self._audit.emit(
                LAYER_STREAM,
                STATUS_COMPLETED if records_read else STATUS_NO_DATA,
                batch_id=int(progress.get("batchId", -1)),
                record_count=records_read,
                duration_ms=_trigger_duration(progress.get("durationMs") or {}),
                starting_offsets=_as_json(source.get("startOffset")),
                ending_offsets=_as_json(source.get("endOffset")),
                spark_progress_json=json.dumps(progress)[:100000],
            )
        except Exception:  # noqa: BLE001
            LOG.error("onQueryProgress audit failed: %s", traceback.format_exc())

    def onQueryIdle(self, event) -> None:  # noqa: N802
        # Required by the Spark 3.5 listener interface. Nothing to record: an idle query
        # under availableNow just means the run is finished.
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        try:
            if event.exception:
                self._audit.emit(LAYER_STREAM, STATUS_FAILED, batch_id=-1,
                                 error_class="StreamingQueryException",
                                 error_message=str(event.exception))
        except Exception:  # noqa: BLE001
            LOG.error("onQueryTerminated audit failed: %s", traceback.format_exc())


# --------------------------------------------------------------------------------------
# Progress parsing helpers
# --------------------------------------------------------------------------------------


def _progress_dict(event) -> Dict[str, Any]:
    """Normalise the progress payload to a plain dict.

    Classic and Spark Connect expose StreamingQueryProgress differently across DBR
    versions; every variant can produce JSON, so that is the common denominator.
    """
    progress = event.progress
    raw = getattr(progress, "json", None)
    if isinstance(raw, str):
        return json.loads(raw)
    if callable(raw):
        return json.loads(raw())
    if isinstance(progress, dict):
        return progress
    return json.loads(str(progress))


def _as_json(value: Any) -> Optional[str]:
    """Kafka offsets arrive as a nested dict or as a pre-serialised string."""
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value)


def _trigger_duration(duration_ms: Dict[str, Any]) -> Optional[int]:
    """durationMs breaks the batch into phases; triggerExecution is the whole batch."""
    if not duration_ms:
        return None
    if "triggerExecution" in duration_ms:
        return int(duration_ms["triggerExecution"])
    return int(sum(int(v) for v in duration_ms.values()))
