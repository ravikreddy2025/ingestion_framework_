"""Audit rows built from SPARK'S OWN batch metrics, not from hand-rolled counters.

Batch id, input row count and per-partition offsets are Spark's numbers. Reading them from
StreamingQueryProgress rather than counting alongside the write is the difference between
"what the source told us it read" and "what we think we saw".

The listener and the foreachBatch body run on DIFFERENT THREADS. They need no coordination
because they write DIFFERENT ROWS - an earlier design joined them on batch id, which needed
a lock and bought nothing.

THREE THINGS THIS FILE EXISTS TO GET RIGHT
------------------------------------------
1. Exceptions thrown inside a listener callback are SWALLOWED by Spark. Every callback
   therefore catches and logs its own, or a silent listener looks identical to a healthy one.
2. The progress object's SHAPE differs across Spark versions and between classic and Spark
   Connect. Every variant can produce JSON, so JSON is the common denominator.
3. `latestOffset` is what turns "we read N records" into "we read N and M are still
   waiting". Whether it is populated under availableNow is VB-05, so it is read
   defensively and a missing value yields NULL rather than a confident zero.
"""

from __future__ import annotations

import json
import logging
import traceback
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

LOG = logging.getLogger(__name__)

# The layer name for rows that describe the STREAM rather than a data layer. Not one of
# SOURCE_SPEC.layers: nothing is written to a table called "stream" - these rows record
# what the query itself did.
LAYER_STREAM = "stream"


class StreamAuditListener(StreamingQueryListener):
    """Emits layer='stream' rows for one query. Registered and removed around that query.

    Leaving listeners attached across runs on a shared session would write rows for other
    sources' queries, attributed to this one.
    """

    def __init__(self, audit: Any, statuses: Any):
        self._audit = audit
        # The framework's status vocabulary, passed in rather than imported, so this file
        # holds no opinion about what a status is called.
        self._statuses = statuses
        self.pending_work: int | None = None

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark-defined name
        try:
            LOG.info("Query started: run_id=%s sparkQueryId=%s", self._audit.run_id, event.id)
            self._audit.emit(LAYER_STREAM, self._statuses.STATUS_STARTED)
        except Exception:  # noqa: BLE001 - Spark swallows listener exceptions
            LOG.error("onQueryStarted audit failed: %s", traceback.format_exc())

    def onQueryProgress(self, event) -> None:  # noqa: N802
        try:
            self.record_progress(progress_dict(event.progress))
        except Exception:  # noqa: BLE001
            LOG.error("onQueryProgress audit failed: %s", traceback.format_exc())

    def onQueryIdle(self, event) -> None:  # noqa: N802
        # Required by the listener interface on recent Spark versions. Nothing to record:
        # an idle query under availableNow just means the run has finished.
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        try:
            if event.exception:
                self._audit.emit(
                    LAYER_STREAM,
                    self._statuses.STATUS_FAILED,
                    error_class="StreamingQueryException",
                    error_message=str(event.exception),
                )
        except Exception:  # noqa: BLE001
            LOG.error("onQueryTerminated audit failed: %s", traceback.format_exc())

    def record_progress(self, progress: dict) -> None:
        """One stream audit row from one progress payload.

        Public, and called from two places: the listener callback during the run, and the
        drain over `query.recentProgress` after awaitTermination() returns. The drain is
        what replaced a sleep: the listener thread may not have delivered the final event
        before the session tears down, and a fixed sleep is a guess that is either too
        short (the row is lost) or wasted time on every single run. The audit_id is
        deterministic per (run, batch, layer, status), so a row delivered twice is a
        duplicate that is trivially identifiable rather than a second, different story.
        """
        source = (progress.get("sources") or [{}])[0]
        records_read = int(progress.get("numInputRows") or 0)
        pending = _pending(source)
        if pending is not None:
            self.pending_work = pending
        self._audit.emit(
            LAYER_STREAM,
            self._statuses.STATUS_COMPLETED if records_read else self._statuses.STATUS_NO_DATA,
            int(progress.get("batchId", -1)),
            record_count=records_read,
            duration_ms=_trigger_duration(progress.get("durationMs") or {}),
            position_start=as_json(source.get("startOffset")),
            position_end=as_json(source.get("endOffset")),
            pending_work=pending,
            source_detail=json.dumps({"latest_offset": source.get("latestOffset")})
            if source.get("latestOffset") is not None
            else None,
        )


def drain(query: Any, listener: StreamAuditListener) -> int | None:
    """Emit a stream row for the LAST progress the query recorded, and return its lag.

    Called after awaitTermination(). `recentProgress` is a bounded in-memory list the query
    keeps itself, so this needs no broker contact and no sleep. Returns the pending-work
    figure so the run can put it on its own audit row.
    """
    try:
        recent = list(query.recentProgress or [])
        if recent:
            listener.record_progress(progress_dict(recent[-1]))
    except Exception:  # noqa: BLE001 - a missing final audit row must never fail a good run
        LOG.error("Could not drain the query's final progress: %s", traceback.format_exc())
    return listener.pending_work


# --------------------------------------------------------------------------------------
# Progress parsing
# --------------------------------------------------------------------------------------


def progress_dict(progress: Any) -> dict:
    """Normalise a progress payload to a plain dict.

    Classic and Spark Connect expose StreamingQueryProgress differently across versions;
    every variant can produce JSON, so that is what this leans on.
    """
    raw = getattr(progress, "json", None)
    if isinstance(raw, str):
        return json.loads(raw)
    if callable(raw):
        return json.loads(raw())
    if isinstance(progress, dict):
        return progress
    return json.loads(str(progress))


def _pending(source: dict) -> int | None:
    """Records read by the broker but not by this run: sum(latestOffset - endOffset).

    NULL-tolerant on purpose, in both directions. If `latestOffset` is absent from the
    progress JSON on this runtime (VB-05), or either side is unparseable, this returns None
    - and None on the audit column reads as "not known", which is the truth. Returning 0
    would read as "fully caught up", which is precisely the false claim a permanently
    lagging feed would then be able to make.
    """
    latest = _offsets(source.get("latestOffset"))
    end = _offsets(source.get("endOffset"))
    if not latest or not end:
        return None
    total = 0
    for topic, partitions in latest.items():
        for partition, offset in (partitions or {}).items():
            consumed = (end.get(topic) or {}).get(partition)
            if consumed is None:
                return None
            total += max(0, int(offset) - int(consumed))
    return total


def _offsets(value: Any) -> dict | None:
    """A Kafka offsets payload as {topic: {partition: offset}}, or None if it is not that."""
    if value is None:
        return None
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def as_json(value: Any) -> str | None:
    """Kafka offsets arrive as a nested dict or as a pre-serialised string."""
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value)


def _trigger_duration(duration_ms: dict) -> int | None:
    """durationMs breaks the batch into phases; triggerExecution is the whole batch."""
    if not duration_ms:
        return None
    if "triggerExecution" in duration_ms:
        return int(duration_ms["triggerExecution"])
    return int(sum(int(v) for v in duration_ms.values()))
