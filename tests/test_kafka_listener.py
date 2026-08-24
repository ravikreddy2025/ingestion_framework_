"""sources/kafka/listener.py - audit rows built from Spark's own batch metrics.

No Spark needed: every function here takes a progress payload and produces an audit row,
and a progress payload is JSON. That is deliberate - the progress object's SHAPE differs
across Spark versions and between classic and Spark Connect, so the code leans on the JSON
form as the common denominator, and testing it means feeding it that JSON.

The pending-work arithmetic is the part worth reading twice. `latestOffset` is what turns
"we read N records" into "we read N and M are still waiting", and whether it is populated
under availableNow is VB-05 - so every path that cannot compute it must return None rather
than 0, because 0 reads as "fully caught up" and that is the false claim a permanently
lagging feed would otherwise be able to make.
"""

from __future__ import annotations

import json

import pytest

from conftest import RecordingAudit
from kafka_ingest.framework import audit as audit_module
from kafka_ingest.sources.kafka import listener as listener_module
from kafka_ingest.sources.kafka.listener import LAYER_STREAM, StreamAuditListener, drain, progress_dict

TOPIC = "demo.events.v1"


def progress(batch_id=3, rows=120, start=None, end=None, latest=None, duration=None):
    source = {"description": f"KafkaV2[Subscribe[{TOPIC}]]"}
    if start is not None:
        source["startOffset"] = start
    if end is not None:
        source["endOffset"] = end
    if latest is not None:
        source["latestOffset"] = latest
    return {
        "batchId": batch_id,
        "numInputRows": rows,
        "durationMs": duration if duration is not None else {"triggerExecution": 4200, "addBatch": 3000},
        "sources": [source],
    }


def _listener():
    audit = RecordingAudit()
    return StreamAuditListener(audit, audit_module), audit


# --------------------------------------------------------------------------------------
# The stream audit row
# --------------------------------------------------------------------------------------


def test_a_progress_event_becomes_one_stream_row_with_spark_s_own_numbers():
    """Batch id, input row count and per-partition offsets are Spark's numbers. Counting
    them alongside the write instead would record what we THINK we saw."""
    listener, audit = _listener()
    listener.record_progress(progress(start={TOPIC: {"0": 100}}, end={TOPIC: {"0": 220}}))

    row = audit.rows[0]
    assert row["layer"] == LAYER_STREAM
    assert row["status"] == audit_module.STATUS_COMPLETED
    assert row["txn_version"] == 3
    assert row["record_count"] == 120
    assert json.loads(row["position_start"]) == {TOPIC: {"0": 100}}
    assert json.loads(row["position_end"]) == {TOPIC: {"0": 220}}


def test_a_batch_that_read_nothing_is_recorded_as_no_data_not_completed():
    """Trigger.AvailableNow emits an empty batch at the tail of every run, so a run that
    ends on one is healthy - and must not look like a run that read 0 rows because it broke."""
    listener, audit = _listener()
    listener.record_progress(progress(rows=0))
    assert audit.rows[0]["status"] == audit_module.STATUS_NO_DATA


def test_the_whole_batch_duration_is_recorded_not_one_phase_of_it():
    listener, audit = _listener()
    listener.record_progress(progress())
    assert audit.rows[0]["duration_ms"] == 4200


def test_a_progress_payload_with_no_trigger_phase_still_yields_a_duration():
    """Not every version reports triggerExecution; the sum of the phases is the honest
    fallback, and a NULL duration would lose the only timing evidence a run leaves."""
    listener, audit = _listener()
    listener.record_progress(progress(duration={"addBatch": 300, "walCommit": 40}))
    assert audit.rows[0]["duration_ms"] == 340


# --------------------------------------------------------------------------------------
# Pending work - VB-05
# --------------------------------------------------------------------------------------


def test_pending_work_is_the_lag_between_what_the_broker_has_and_what_the_run_read():
    listener, audit = _listener()
    listener.record_progress(progress(end={TOPIC: {"0": 220, "1": 100}}, latest={TOPIC: {"0": 500, "1": 100}}))
    assert audit.rows[0]["pending_work"] == 280
    assert listener.pending_work == 280


def test_a_fully_drained_run_reports_zero_pending_and_means_it():
    listener, audit = _listener()
    listener.record_progress(progress(end={TOPIC: {"0": 500}}, latest={TOPIC: {"0": 500}}))
    assert audit.rows[0]["pending_work"] == 0


@pytest.mark.parametrize(
    "end, latest",
    [
        ({TOPIC: {"0": 220}}, None),  # the runtime reported no latest offset at all - VB-05
        (None, {TOPIC: {"0": 500}}),  # no end offset to compare against
        ({TOPIC: {"0": 220}}, {TOPIC: {"0": 500}, "other.topic": {"0": 9}}),  # a partition we never read
        ("not json", {TOPIC: {"0": 500}}),  # unparseable payload
    ],
)
def test_pending_work_is_null_whenever_it_cannot_be_known(end, latest):
    """NULL is not zero, and the difference is the whole point of the column.

    Zero reads as "fully caught up". Reporting it when the figure is actually unknown would
    let a permanently lagging feed make exactly the claim this column exists to disprove.
    """
    listener, audit = _listener()
    listener.record_progress(progress(end=end, latest=latest))
    assert audit.rows[0]["pending_work"] is None


def test_a_partition_that_went_backwards_never_reports_negative_pending():
    """An end offset ahead of the reported latest is a race, not a negative backlog."""
    listener, audit = _listener()
    listener.record_progress(progress(end={TOPIC: {"0": 500}}, latest={TOPIC: {"0": 400}}))
    assert audit.rows[0]["pending_work"] == 0


def test_the_latest_offset_is_kept_in_source_detail_for_the_partition_level_answer():
    """The column is one number so a standing query can threshold on it; which PARTITION is
    behind is a different question, and it goes in the JSON column that costs no schema."""
    listener, audit = _listener()
    listener.record_progress(progress(end={TOPIC: {"0": 220}}, latest={TOPIC: {"0": 500}}))
    assert json.loads(audit.rows[0]["source_detail"])["latest_offset"] == {TOPIC: {"0": 500}}


# --------------------------------------------------------------------------------------
# Draining the final progress, instead of sleeping and hoping
# --------------------------------------------------------------------------------------


class _Query:
    def __init__(self, recent=(), fail=False):
        self._recent = list(recent)
        self.fail = fail

    @property
    def recentProgress(self):  # noqa: N802 - mirrors the Spark API
        if self.fail:
            raise RuntimeError("the query has already been torn down")
        return self._recent


def test_the_final_progress_is_read_from_the_query_rather_than_waited_for():
    """This replaced a fixed sleep. The listener thread may not have delivered the last
    event before the session tears down, and a sleep is a guess that is either too short -
    the row is lost - or wasted time on every single run."""
    listener, audit = _listener()
    recent = [
        progress(batch_id=1, rows=10),
        progress(batch_id=2, rows=5, end={TOPIC: {"0": 9}}, latest={TOPIC: {"0": 19}}),
    ]
    pending = drain(_Query(recent), listener)
    assert len(audit.rows) == 1, "only the LAST progress is drained"
    assert audit.rows[0]["txn_version"] == 2
    assert pending == 10


def test_draining_a_query_that_produced_no_progress_writes_nothing():
    listener, audit = _listener()
    assert drain(_Query([]), listener) is None
    assert audit.rows == []


def test_a_failure_to_drain_never_fails_a_run_that_already_wrote_its_data(caplog):
    """The data is committed by this point. Losing the final audit row is a gap in the
    evidence, not a reason to fail the run that produced it."""
    listener, audit = _listener()
    with caplog.at_level("ERROR"):
        assert drain(_Query(fail=True), listener) is None
    assert "drain" in caplog.text.lower()


# --------------------------------------------------------------------------------------
# Listener callbacks - Spark swallows anything they raise
# --------------------------------------------------------------------------------------


class _Event:
    def __init__(self, progress=None, exception=None, event_id="q-1"):
        self.progress = progress
        self.exception = exception
        self.id = event_id


def test_a_broken_progress_payload_is_logged_rather_than_swallowed_silently(caplog):
    """Exceptions thrown inside a listener callback are swallowed by Spark, so a listener
    that failed on every batch would look identical to a healthy one."""
    listener, audit = _listener()
    with caplog.at_level("ERROR"):
        listener.onQueryProgress(_Event(progress="{not json"))
    assert audit.rows == []
    assert "onQueryProgress" in caplog.text


def test_a_terminated_query_records_its_exception():
    listener, audit = _listener()
    listener.onQueryTerminated(_Event(exception="Connection to broker lost"))
    assert audit.rows[0]["status"] == audit_module.STATUS_FAILED
    assert "broker lost" in audit.rows[0]["error_message"]


def test_a_clean_termination_records_nothing():
    """A query that ended because it finished is not an event worth a row - the run-level
    COMPLETED row already says so."""
    listener, audit = _listener()
    listener.onQueryTerminated(_Event(exception=None))
    assert audit.rows == []


def test_an_idle_callback_exists_because_the_interface_requires_one():
    """Added to the listener interface in a later Spark version; absent from it, the
    listener fails to register at all on a runtime that declares it abstract."""
    listener, audit = _listener()
    listener.onQueryIdle(_Event())
    assert audit.rows == []


@pytest.mark.parametrize(
    "payload",
    [
        {"batchId": 1, "numInputRows": 2, "sources": []},  # already a dict
        json.dumps({"batchId": 1, "numInputRows": 2, "sources": []}),  # a JSON string
    ],
)
def test_a_progress_payload_is_normalised_however_this_runtime_expresses_it(payload):
    """Classic and Spark Connect expose StreamingQueryProgress differently across versions.
    Every variant can produce JSON, so JSON is what this leans on."""

    class _WithJsonMethod:
        def json(self):
            return json.dumps({"batchId": 1, "numInputRows": 2, "sources": []})

    assert progress_dict(payload)["batchId"] == 1
    assert progress_dict(_WithJsonMethod())["batchId"] == 1


def test_offsets_are_rendered_as_text_whether_they_arrive_as_a_dict_or_a_string():
    assert listener_module.as_json({TOPIC: {"0": 1}}) == json.dumps({TOPIC: {"0": 1}})
    assert listener_module.as_json('{"already": "text"}') == '{"already": "text"}'
    assert listener_module.as_json(None) is None
