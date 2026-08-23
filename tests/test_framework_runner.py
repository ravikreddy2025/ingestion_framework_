"""The run lifecycle: resolve, short-circuit, dispatch, audit.

Needs no Spark. `runner.run` only builds a SparkSession when it is about to dispatch, and
these tests pass a stand-in - which is exactly why framework/runner.py has no module-level
PySpark import.
"""

from __future__ import annotations

import dataclasses
import types

import pytest

from kafka_ingest.framework import runner
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
def dispatch_demo(monkeypatch, demo_spec):
    """Register the synthetic source type in the dispatch dict for the duration of a test."""
    calls: list[RunContext] = []
    monkeypatch.setitem(runner._SOURCES, "demo", _fake_source(demo_spec, calls=calls))
    return calls


# --------------------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------------------


def test_the_declared_source_type_selects_the_implementation(demo_config_root, dispatch_demo):
    result = runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")
    assert result is EMPTY_RESULT
    assert len(dispatch_demo) == 1


def test_the_source_receives_a_fully_resolved_context(demo_config_root, dispatch_demo):
    runner.run("demo_source", "dev", config_root=demo_config_root, spark="fake-spark")
    ctx = dispatch_demo[0]
    assert ctx.cfg.source_key == "demo_source"
    assert ctx.cfg.source_type == "demo"
    assert ctx.cfg.environment == "dev"
    assert ctx.cfg.get("batch_limit") == 10  # the dev per-type default, resolved
    assert ctx.spark == "fake-spark"
    assert ctx.run_type == runner.RUN_TYPE_PRIMARY
    assert ctx.log.source_key == "demo_source"
    assert ctx.run_id == ctx.log.run_id


def test_the_run_context_cannot_be_mutated_by_a_source(demo_config_root, dispatch_demo):
    """A source that swapped its own config or run_id mid-run would make every audit row
    for that run a guess."""
    runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")
    with pytest.raises(dataclasses.FrozenInstanceError):
        dispatch_demo[0].run_id = "something-else"


def test_an_unimplemented_source_type_is_a_config_error(demo_config_root, monkeypatch):
    """The dispatch dict is the whole answer to 'which source types exist' - so a
    source_type nothing implements must say so, and list the ones that do."""
    monkeypatch.delitem(runner._SOURCES, "demo", raising=False)
    with pytest.raises(ConfigError, match=r"declares source_type 'demo', which no source package"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")


@pytest.mark.parametrize("source_type", sorted(runner._SOURCES))
def test_every_dispatchable_source_exposes_exactly_the_contract(source_type):
    """SOURCE_SPEC and run(). A source that grew a read()/parse()/write() surface would
    have broken the one abstraction this design is built on."""
    module = runner._SOURCES[source_type]
    assert module.SOURCE_SPEC.source_type == source_type
    assert callable(module.run)
    assert not {"read", "parse", "write", "validate"} & set(dir(module))


@pytest.mark.parametrize("source_type", sorted(runner._SOURCES))
def test_the_shipped_source_stubs_refuse_to_pretend(source_type):
    """Stages 3-5 implement the three real sources. Until then they must raise: a stub that
    returned an empty result would report a successful run that ingested nothing, which is
    the one failure mode in this design that looks like success."""
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


def test_a_completed_run_is_audited_with_what_it_read_and_wrote(demo_config_root, monkeypatch, demo_spec, caplog):
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
        runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "status=STARTED" in messages
    assert "status=COMPLETED" in messages
    assert "rows_read=7" in messages
    assert "position_end=107" in messages


def test_a_failing_source_is_audited_and_the_error_still_propagates(demo_config_root, monkeypatch, demo_spec, caplog):
    """Swallowing the exception would turn a failed extraction into a green job."""
    module = _fake_source(demo_spec)

    def explode(ctx):
        raise RuntimeError("the broker said no")

    module.run = explode
    monkeypatch.setitem(runner._SOURCES, "demo", module)

    with caplog.at_level("INFO"), pytest.raises(RuntimeError, match="the broker said no"):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")
    messages = " ".join(record.getMessage() for record in caplog.records)
    assert "status=FAILED" in messages
    assert "error_class=RuntimeError" in messages


# --------------------------------------------------------------------------------------
# Run identity
# --------------------------------------------------------------------------------------


def test_the_run_id_prefers_the_job_run_id(demo_config_root, dispatch_demo):
    """A support engineer looking at a Workflows run must be able to find its audit rows
    without joining on timestamps."""
    runner.run("demo_source", "prod", config_root=demo_config_root, job_run_id="4417", spark="fake-spark")
    assert dispatch_demo[0].run_id == "demo_source-primary-4417"


def test_the_job_run_id_is_not_a_config_setting(demo_config_root, dispatch_demo):
    """It identifies the run; it is not something any source reads. Passing it through the
    config layer would force every SOURCE_SPEC to declare a key nothing uses."""
    runner.run("demo_source", "prod", config_root=demo_config_root, job_run_id="4417", spark="fake-spark")
    assert "job_run_id" not in dispatch_demo[0].cfg.settings


def test_run_ids_are_unique_without_a_job_run_id(demo_config_root, dispatch_demo):
    for _ in range(2):
        runner.run("demo_source", "prod", config_root=demo_config_root, spark="fake-spark")
    first, second = (ctx.run_id for ctx in dispatch_demo)
    assert first != second
    assert first.startswith("demo_source-primary-")


def test_the_run_type_reaches_the_run_id_and_the_context(demo_config_root, dispatch_demo):
    """The framework carries the run type; it does not enumerate the replay shapes, because
    only the source knows how to execute one."""
    runner.run("demo_source", "prod", config_root=demo_config_root, run_type="demo_replay", spark="fake-spark")
    assert dispatch_demo[0].run_type == "demo_replay"
    assert dispatch_demo[0].run_id.startswith("demo_source-demo_replay-")
