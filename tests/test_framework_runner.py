"""The run lifecycle: resolve, short-circuit, dispatch, audit.

Needs no Spark. `runner.run` only builds a SparkSession when it is about to dispatch, and
these tests pass a stand-in - which is exactly why framework/runner.py has no module-level
PySpark import.
"""

from __future__ import annotations

import dataclasses
import types

import pytest

from conftest import FakeSpark
from kafka_ingest.framework import runner, tables, writers
from kafka_ingest.framework.audit import AUDIT_SCHEMA
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.framework.contracts import RunContext, RunResult

EMPTY_RESULT = RunResult(
    rows_read=0, rows_written={}, rows_quarantined=0, position_start=None, position_end=None, source_detail=None
)


def _fake_source(spec, result=EMPTY_RESULT, calls=None):
    """A source module is a module with two attributes. Nothing else is required of it -
    which is the contract, stated as a test."""
    module = types.SimpleNamespace(SOURCE_SPEC=spec)

    def run(ctx: RunContext) -> RunResult:
        if calls is not None:
            calls.append(ctx)
        return result

    module.run = run
    return module


@pytest.fixture
def fake_spark(fake_delta):
    """A recording SparkSession stand-in, with a stand-in delta.tables behind it.

    The runner writes before it dispatches: it ensures the audit and state tables exist and
    allocates a run sequence. None of that needs a cluster, but all of it needs something
    to call - and asserting on what it called is the point of the recording stand-ins.
    """
    return FakeSpark()


@pytest.fixture
def dispatch_demo(monkeypatch, demo_spec):
    """Register the synthetic source type in the dispatch dict for the duration of a test."""
    calls: list[RunContext] = []
    monkeypatch.setitem(runner._SOURCES, "demo", _fake_source(demo_spec, calls=calls))
    return calls


# --------------------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------------------


def test_the_declared_source_type_selects_the_implementation(demo_config_root, dispatch_demo, fake_spark):
    result = runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    assert result is EMPTY_RESULT
    assert len(dispatch_demo) == 1


def test_the_source_receives_a_fully_resolved_context(demo_config_root, dispatch_demo, fake_spark):
    runner.run("demo_source", "dev", config_root=demo_config_root, spark=fake_spark)
    ctx = dispatch_demo[0]
    assert ctx.cfg.source_key == "demo_source"
    assert ctx.cfg.source_type == "demo"
    assert ctx.cfg.environment == "dev"
    assert ctx.cfg.get("batch_limit") == 10  # the dev per-type default, resolved
    assert ctx.spark is fake_spark
    assert ctx.run_type == runner.RUN_TYPE_PRIMARY
    assert ctx.log.source_key == "demo_source"
    assert ctx.run_id == ctx.log.run_id


def test_the_run_context_cannot_be_mutated_by_a_source(demo_config_root, dispatch_demo, fake_spark):
    """A source that swapped its own config or run_id mid-run would make every audit row
    for that run a guess."""
    runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    with pytest.raises(dataclasses.FrozenInstanceError):
        dispatch_demo[0].run_id = "something-else"


def test_an_unimplemented_source_type_is_a_config_error(demo_config_root, monkeypatch, fake_spark):
    """The dispatch dict is the whole answer to 'which source types exist' - so a
    source_type nothing implements must say so, and list the ones that do."""
    monkeypatch.delitem(runner._SOURCES, "demo", raising=False)
    with pytest.raises(ConfigError, match=r"declares source_type 'demo', which no source package"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)


@pytest.mark.parametrize("source_type", sorted(runner._SOURCES))
def test_every_dispatchable_source_exposes_exactly_the_contract(source_type):
    """SOURCE_SPEC and run(). A source that grew a read()/parse()/write() surface would
    have broken the one abstraction this design is built on."""
    module = runner._SOURCES[source_type]
    assert module.SOURCE_SPEC.source_type == source_type
    assert callable(module.run)
    assert not {"read", "parse", "write", "validate"} & set(dir(module))


# Kafka (Stage 3), Oracle (Stage 4) and file (Stage 5) are all implemented now. The list is
# derived from the dispatch dict rather than hardcoded, so it emptied itself the moment the
# last source landed rather than this file needing an edit to notice.
_IMPLEMENTED = {"kafka", "oracle", "file"}
_UNIMPLEMENTED = sorted(t for t in runner._SOURCES if t not in _IMPLEMENTED)


@pytest.mark.parametrize("source_type", _UNIMPLEMENTED)
def test_the_shipped_source_stubs_refuse_to_pretend(source_type):
    """Stages 4-5 implement the two remaining sources. Until then they must RAISE: a stub
    that returned an empty result would report a successful run that ingested nothing,
    which is the one failure mode in this design that looks like success."""
    with pytest.raises(NotImplementedError, match="Stage"):
        runner._SOURCES[source_type].run(None)


# --------------------------------------------------------------------------------------
# The lifecycle around the dispatch
# --------------------------------------------------------------------------------------


def test_a_disabled_source_does_not_run_and_needs_no_spark(demo_config_root, dispatch_demo):
    """Disabling is the emergency stop. It must take effect before anything connects -
    note no `spark` is passed here, so building one would fail the test."""
    result = runner.run("demo_source", "prod", config_root=demo_config_root, control={"enabled": False})
    assert dispatch_demo == []
    assert result.rows_read == 0
    assert result.rows_written == {}


def test_a_disabled_source_still_leaves_a_trace(demo_config_root, dispatch_demo, caplog):
    """Silence would be indistinguishable from a broken scheduler."""
    with caplog.at_level("INFO"):
        runner.run("demo_source", "prod", config_root=demo_config_root, control={"enabled": False})
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "status=SKIPPED" in messages
    assert "source_key=demo_source" in messages


def test_a_completed_run_is_audited_with_what_it_read_and_wrote(
    demo_config_root, monkeypatch, demo_spec, caplog, fake_spark
):
    result = RunResult(
        rows_read=7,
        rows_written={"landing": 7},
        rows_quarantined=0,
        position_start="100",
        position_end="107",
        source_detail=None,
    )
    monkeypatch.setitem(runner._SOURCES, "demo", _fake_source(demo_spec, result=result))
    with caplog.at_level("INFO"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "status=STARTED" in messages
    assert "status=COMPLETED" in messages
    assert "rows_read=7" in messages
    assert "position_end=107" in messages


def test_a_failing_source_is_audited_and_the_error_still_propagates(
    demo_config_root, monkeypatch, demo_spec, caplog, fake_spark
):
    """Swallowing the exception would turn a failed extraction into a green job."""
    module = _fake_source(demo_spec)

    def explode(ctx):
        raise RuntimeError("the broker said no")

    module.run = explode
    monkeypatch.setitem(runner._SOURCES, "demo", module)

    with caplog.at_level("INFO"), pytest.raises(RuntimeError, match="the broker said no"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "status=FAILED" in messages
    assert "error_class=RuntimeError" in messages


# --------------------------------------------------------------------------------------
# Run identity
# --------------------------------------------------------------------------------------


def test_the_run_id_prefers_the_job_run_id(demo_config_root, dispatch_demo, fake_spark):
    """A support engineer looking at a Workflows run must be able to find its audit rows
    without joining on timestamps."""
    runner.run("demo_source", "prod", config_root=demo_config_root, job_run_id="4417", spark=fake_spark)
    assert dispatch_demo[0].run_id == "demo_source-primary-4417"


def test_the_job_run_id_is_not_a_config_setting(demo_config_root, dispatch_demo, fake_spark):
    """It identifies the run; it is not something any source reads. Passing it through the
    config layer would force every SOURCE_SPEC to declare a key nothing uses."""
    runner.run("demo_source", "prod", config_root=demo_config_root, job_run_id="4417", spark=fake_spark)
    assert "job_run_id" not in dispatch_demo[0].cfg.settings


def test_run_ids_are_unique_without_a_job_run_id(demo_config_root, dispatch_demo, fake_spark):
    for _ in range(2):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    first, second = (ctx.run_id for ctx in dispatch_demo)
    assert first != second
    assert first.startswith("demo_source-primary-")


def test_the_run_type_reaches_the_run_id_and_the_context(demo_config_root, dispatch_demo, fake_spark):
    """The framework carries the run type; it does not enumerate the replay shapes, because
    only the source knows how to execute one."""
    runner.run("demo_source", "prod", config_root=demo_config_root, run_type="demo_replay", spark=fake_spark)
    assert dispatch_demo[0].run_type == "demo_replay"
    assert dispatch_demo[0].run_id.startswith("demo_source-demo_replay-")


# --------------------------------------------------------------------------------------
# What the runner puts on the context, and what it does before dispatching
#
# Stage 2 filled in the four RunContext slots Stage 1 left as None. A source written
# against them in Stage 3-5 has to find them populated, and a run has to have set up its
# own two tables and taken its run sequence before the source is ever called.
# --------------------------------------------------------------------------------------


def test_the_context_carries_audit_state_writers_and_tables(demo_config_root, dispatch_demo, fake_spark):
    ctx = _run_and_capture(demo_config_root, dispatch_demo, fake_spark)
    assert ctx.audit.table == "cat_prod.audit.ingest_audit"
    assert ctx.state.table == "ops_prod.ingestion.ingest_state"
    assert ctx.writers is writers
    assert ctx.tables is tables


def test_a_run_sequence_is_allocated_before_the_source_runs(demo_config_root, dispatch_demo, fake_spark):
    """It is the Delta txnVersion for any source with no microbatch id, so it has to be
    durable and it has to exist by the time the source writes anything."""
    ctx = _run_and_capture(demo_config_root, dispatch_demo, fake_spark)
    assert ctx.run_sequence == 1


def test_the_run_sequence_is_allocated_for_every_source_type(demo_config_root, dispatch_demo, fake_spark):
    """Unconditionally, including for sources that do not need it. The alternative is a
    branch on source type inside framework/, which is the one thing this design forbids."""
    ctx = _run_and_capture(demo_config_root, dispatch_demo, fake_spark)
    written = [values[0] for values, _schema in fake_spark.created_frames]
    assert any(row[1] == "run_sequence" for row in written)
    assert ctx.run_sequence == 1


def test_the_frameworks_own_tables_are_created_before_dispatch(demo_config_root, dispatch_demo, fake_spark):
    """Both are CREATE TABLE IF NOT EXISTS and a metadata no-op afterwards. Layer tables are
    NOT here: a curated-style layer has a schema the source resolves, not the framework."""
    _run_and_capture(demo_config_root, dispatch_demo, fake_spark)
    created = " ".join(fake_spark.sql_statements)
    assert "CREATE TABLE IF NOT EXISTS cat_prod.audit.ingest_audit" in created
    assert "CREATE TABLE IF NOT EXISTS ops_prod.ingestion.ingest_state" in created
    assert "landing" not in created


def test_an_illegal_target_name_fails_before_anything_connects(demo_config_root, dispatch_demo, fake_spark):
    """A name legal in the source system and illegal in Unity Catalog has to fail at
    configuration load. Failing at write time means failing after the read has run."""
    layer_defaults = f"{demo_config_root}/defaults/demo.yaml"
    with open(layer_defaults, "r", encoding="utf-8") as handle:
        text = handle.read()
    with open(layer_defaults, "w", encoding="utf-8") as handle:
        handle.write(text.replace("{catalog}.landing.{source_key}", "{catalog}.landing.ORDER$"))

    with pytest.raises(ConfigError, match="not a legal"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)
    assert dispatch_demo == []


def test_a_completed_run_writes_one_audit_row_per_layer_and_one_for_the_run(
    demo_config_root, monkeypatch, demo_spec, fake_spark
):
    result = RunResult(
        rows_read=7,
        rows_written={"landing": 7},
        rows_quarantined=0,
        position_start="100",
        position_end="107",
        source_detail=None,
    )
    monkeypatch.setitem(runner._SOURCES, "demo", _fake_source(demo_spec, result=result))
    runner.run("demo_source", "prod", config_root=demo_config_root, spark=fake_spark)

    rows = [values[0] for values, schema in fake_spark.created_frames if schema is AUDIT_SCHEMA]
    by_layer_status = {(row[7], row[8]) for row in rows}
    assert ("run", "STARTED") in by_layer_status
    assert ("landing", "COMPLETED") in by_layer_status
    assert ("run", "COMPLETED") in by_layer_status


def test_the_control_table_is_read_when_one_is_configured(demo_config_root, dispatch_demo, fake_delta):
    """`control` was passed in directly by every test above. In production nobody passes it:
    the runner reads the table named by the `control_table` setting."""
    _add_control_table(demo_config_root)
    spark = FakeSpark(
        existing_tables={CONTROL_TABLE},
        rows_by_table={CONTROL_TABLE: [{"source_key": "demo_source", "demo_batch_limit": 77}]},
    )
    runner.run("demo_source", "prod", config_root=demo_config_root, spark=spark)
    assert dispatch_demo[0].cfg.get("batch_limit") == 77


def test_the_control_column_owner_registry_is_built_from_every_known_source():
    """framework/runner.py is the one place allowed to know source types by name; this is
    what it hands framework/control.py so a column belonging to a different source type is
    caught rather than silently ignored (docs/build_log/DECISIONS.md D-01 point 4)."""
    assert runner._CONTROL_COLUMN_OWNERS["kafka_checkpoint_reset_id"] == "kafka"
    assert runner._CONTROL_COLUMN_OWNERS["kafka_failure_mode"] == "kafka"


def test_a_column_for_a_different_source_type_is_rejected_end_to_end(demo_config_root, dispatch_demo, fake_delta):
    """The same check, exercised through the full runner.run() path rather than calling
    framework/control.py directly."""
    _add_control_table(demo_config_root)
    spark = FakeSpark(
        existing_tables={CONTROL_TABLE},
        rows_by_table={CONTROL_TABLE: [{"source_key": "demo_source", "kafka_checkpoint_reset_id": "INC1"}]},
    )
    with pytest.raises(ConfigError, match=r"'kafka_checkpoint_reset_id' is set for source_key 'demo_source'"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark=spark)


def test_an_explicit_control_dict_is_used_instead_of_reading_the_table(demo_config_root, dispatch_demo, fake_delta):
    """Which is what lets the whole lifecycle be exercised with no table at all."""
    _add_control_table(demo_config_root)
    runner.run("demo_source", "prod", config_root=demo_config_root, control={"enabled": False})
    assert dispatch_demo == []


CONTROL_TABLE = "ops_prod.ingestion.ingest_control"


def _add_control_table(config_root):
    """Point the demo configuration at a control table. The fixture leaves it out so every
    other test in this module runs without one."""
    defaults = f"{config_root}/defaults.yaml"
    with open(defaults, "a", encoding="utf-8") as handle:
        handle.write(f'\n  control_table: "{CONTROL_TABLE}"\n')


def _run_and_capture(config_root, dispatch_demo, spark):
    runner.run("demo_source", "prod", config_root=config_root, spark=spark)
    return dispatch_demo[0]
