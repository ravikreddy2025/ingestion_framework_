"""What the writers actually hand to Delta.

These cover the three decisions that determine whether data is correct on a re-run:

  * the Delta idempotent-write markers (txnAppId / txnVersion), which is what makes a
    retried batch a no-op instead of a duplicate;
  * append versus MERGE, which is what stops a replay duplicating everything it overlaps;
  * the PARTITION PREDICATE on every MERGE, which is what stops one day's replay rewriting
    three years of history.

All three are plain option and method calls, so the recording stand-ins in conftest test
them exactly - no Delta, no JVM, no cluster. The stand-in does not simulate merge semantics
on purpose: these assert which branch was taken and what it was given, not what Delta would
do with it.
"""

from __future__ import annotations

import inspect

import pytest

from conftest import FakeSpark, RecordingDataFrame
from kafka_ingest.framework import writers

TABLE = "cat_prod.landing.demo_source"
TXN_APP_ID = "demo_source::primary"
PARTITION_PREDICATE = "t.ingest_date = '2026-08-11'"
KEYS = ("source_key", "record_id")


# --------------------------------------------------------------------------------------
# Append
# --------------------------------------------------------------------------------------


def test_an_append_carries_the_idempotency_markers():
    """The mechanism the whole no-duplicates story rests on. Without these two options a
    retried batch appends its rows a second time."""
    df = RecordingDataFrame()
    writers.append(df, TABLE, txn_app_id=TXN_APP_ID, txn_version=7)

    assert df.write.format_used == "delta"
    assert df.write.mode_used == "append"
    assert df.write.options["txnAppId"] == TXN_APP_ID
    assert df.write.options["txnVersion"] == 7
    assert df.write.saved_as == TABLE


def test_version_zero_still_gets_markers():
    """Guards the boundary: batch 0 is a real microbatch, and `if txn_version` rather than
    `if txn_version >= 0` would silently skip deduplication for the first batch of a run."""
    df = RecordingDataFrame()
    writers.append(df, TABLE, txn_app_id=TXN_APP_ID, txn_version=0)
    assert df.write.options["txnVersion"] == 0


def test_a_negative_version_gets_no_markers():
    """A bounded read outside a microbatch has no batch identity. txnVersion must be a real
    one, so the markers are skipped and idempotency comes from the MERGE key instead."""
    df = RecordingDataFrame()
    writers.append(df, TABLE, txn_app_id=TXN_APP_ID, txn_version=-1)
    assert "txnAppId" not in df.write.options


@pytest.mark.parametrize(
    "kwargs",
    [
        {"txn_app_id": TXN_APP_ID},
        {"txn_version": 3},
        {},
    ],
)
def test_half_an_identity_records_nothing(kwargs):
    """An app id with no version, or a version with no id, records nothing in the Delta log
    and would leave a retried batch free to duplicate. Both or neither."""
    df = RecordingDataFrame()
    writers.append(df, TABLE, **kwargs)
    assert "txnAppId" not in df.write.options
    assert "txnVersion" not in df.write.options


def test_an_append_can_widen_the_schema_and_partition_on_first_write():
    df = RecordingDataFrame()
    writers.append(df, TABLE, merge_schema=True, partition_by=["ingest_date"])
    assert df.write.options["mergeSchema"] == "true"
    assert df.write.partition_by == ["ingest_date"]


def test_an_append_asks_for_nothing_it_was_not_given():
    """mergeSchema on by default would invite an unexpected column to silently widen the
    system of record."""
    df = RecordingDataFrame()
    writers.append(df, TABLE)
    assert df.write.options == {}
    assert df.write.partition_by is None


# --------------------------------------------------------------------------------------
# MERGE - and the partition predicate nobody can forget
# --------------------------------------------------------------------------------------


def test_merge_cannot_be_called_without_a_partition_predicate():
    """Required positional argument, no default. A MERGE whose ON clause names only the
    merge keys rewrites every partition it might match; making the predicate impossible to
    omit is the only version of this rule that survives a 3am incident."""
    with pytest.raises(TypeError):
        writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS)


def test_the_partition_predicate_has_no_default_in_the_signature():
    """Belt and braces on the test above: a default added later would make that TypeError
    quietly stop happening."""
    parameter = inspect.signature(writers.merge).parameters["partition_predicate"]
    assert parameter.default is inspect.Parameter.empty
    assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


def test_an_empty_partition_predicate_is_rejected_too(fake_delta):
    """`merge(..., "")` would satisfy the signature and prune nothing."""
    with pytest.raises(ValueError, match="needs a partition predicate"):
        writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS, "   ")


def test_no_keys_is_rejected(fake_delta):
    with pytest.raises(ValueError, match="at least one key column"):
        writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, (), PARTITION_PREDICATE)


def test_the_predicate_is_anded_ahead_of_the_key_match(fake_delta):
    """Ahead, so the plan prunes before it matches."""
    writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE)
    assert fake_delta.last.merge_op.condition == (
        "(t.ingest_date = '2026-08-11') AND t.source_key = s.source_key AND t.record_id = s.record_id"
    )
    assert fake_delta.last.name == TABLE


def test_a_merge_inserts_if_absent_and_leaves_matched_rows_alone(fake_delta):
    """The safe default. A landing row records what arrived; rewriting it with a replay's
    provenance destroys that record."""
    writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE)
    assert fake_delta.last.merge_op.clauses == ["whenNotMatchedInsertAll"]
    assert fake_delta.last.merge_op.executed


def test_a_merge_can_replace_matched_rows_when_asked(fake_delta):
    """A layer whose whole purpose is to REPLACE a bad parse with a good one passes True."""
    writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE, update_matched=True)
    assert fake_delta.last.merge_op.clauses == ["whenMatchedUpdateAll", "whenNotMatchedInsertAll"]


def test_a_merge_asks_for_no_schema_evolution_unless_it_is_asked_for(fake_delta):
    writers.merge(FakeSpark(), RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE)
    assert "withSchemaEvolution" not in fake_delta.last.merge_op.clauses


# --------------------------------------------------------------------------------------
# Schema evolution: two mechanisms, because the runtime floor is not pinned (VB-09)
# --------------------------------------------------------------------------------------


def test_schema_evolution_uses_the_operation_scoped_method_where_it_exists(fake_delta):
    spark = FakeSpark()
    writers.merge(spark, RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE, schema_evolution=True)
    assert "withSchemaEvolution" in fake_delta.last.merge_op.clauses
    # The operation-scoped API needs no session flag at all.
    assert spark.conf.history == []


def test_an_older_runtime_falls_back_to_the_session_flag_and_restores_it(fake_delta, monkeypatch):
    """On a runtime without withSchemaEvolution() the legacy flag is the only mechanism. It
    must be scoped to this one merge: leaving it on would silently change how every other
    write in the session handles an unexpected column."""
    from conftest import RecordingMerge

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")
    spark = FakeSpark()
    writers.merge(spark, RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE, schema_evolution=True)

    assert fake_delta.last.merge_op.executed
    assert spark.conf.history == [
        ("set", writers.AUTO_MERGE_CONF, "true"),
        ("unset", writers.AUTO_MERGE_CONF, None),
    ]
    assert writers.AUTO_MERGE_CONF not in spark.conf.values


def test_the_session_flag_is_restored_not_cleared_when_it_was_already_set(fake_delta, monkeypatch):
    """Another job on a shared session may have set it deliberately; clearing it would be a
    surprising side effect of running a replay."""
    from conftest import RecordingMerge

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")
    spark = FakeSpark(conf={writers.AUTO_MERGE_CONF: "false"})
    writers.merge(spark, RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE, schema_evolution=True)
    assert spark.conf.values[writers.AUTO_MERGE_CONF] == "false"


def test_the_session_flag_is_restored_even_if_the_merge_fails(fake_delta, monkeypatch):
    """A failed replay must not leave schema auto-merge switched on for the whole session."""
    from conftest import RecordingMerge

    monkeypatch.delattr(RecordingMerge, "withSchemaEvolution")

    def boom(self):
        raise RuntimeError("merge failed")

    monkeypatch.setattr(RecordingMerge, "execute", boom)
    spark = FakeSpark()
    with pytest.raises(RuntimeError, match="merge failed"):
        writers.merge(spark, RecordingDataFrame(), TABLE, KEYS, PARTITION_PREDICATE, schema_evolution=True)
    assert writers.AUTO_MERGE_CONF not in spark.conf.values


# --------------------------------------------------------------------------------------
# Quarantine split
# --------------------------------------------------------------------------------------


def test_the_split_is_total_so_no_row_is_dropped_by_a_null():
    """A three-valued predicate that evaluates to NULL belongs to neither `cond` nor
    `NOT cond`, so the plain form would drop those rows from BOTH sides - silently losing
    exactly the records that are most likely to be malformed."""
    from conftest import FakeDataFrame

    frame = FakeDataFrame([])
    valid, quarantined = writers.split_quarantine(frame, "wire_format_valid")
    assert frame.filters == [
        "coalesce(wire_format_valid, false)",
        "NOT coalesce(wire_format_valid, false)",
    ]
    assert valid is not quarantined
