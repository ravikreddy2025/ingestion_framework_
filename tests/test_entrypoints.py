"""The two entrypoints: their argument surface, and what they hand the runner.

Thin files, so these are thin tests - but the argument NAMES are a deployment contract.
Every one of them appears in resources/*.yml as a `named_parameters` key, and a rename that
reaches the Python and not the job definition produces a task that fails at startup with an
argparse error, on a schedule, in an environment nobody is watching.
"""

from __future__ import annotations

import pytest

from kafka_ingest.entrypoints import run_ingest, run_replay


def _parse(parser, argv):
    return parser.parse_args(argv)


BASE = ["--config-root", "/Workspace/conf", "--source-key", "demo_topic", "--environment", "dev"]


# --------------------------------------------------------------------------------------
# run_ingest
# --------------------------------------------------------------------------------------


def test_the_primary_entrypoint_takes_a_source_key_not_a_topic_key():
    """One job definition, every source of every TYPE. The parameter names what the config
    file is called, not what kind of system is behind it."""
    args = _parse(run_ingest.build_parser(), BASE)
    assert args.source_key == "demo_topic"
    assert args.environment == "dev"
    assert args.job_run_id is None


@pytest.mark.parametrize("missing", ["--config-root", "--source-key", "--environment"])
def test_the_primary_entrypoint_requires_the_three_things_it_cannot_guess(missing):
    argv = [a for i, a in enumerate(BASE) if BASE[i - 1] != missing and a != missing]
    with pytest.raises(SystemExit):
        _parse(run_ingest.build_parser(), argv)


def test_the_primary_entrypoint_names_no_source_type():
    """Adding a source type must not touch this file. If it ever names one, dispatch has
    leaked out of framework/runner.py's one dict."""
    help_text = run_ingest.build_parser().format_help()
    # "file" is not checked: it is an ordinary English word and appears in "filename", which
    # is a description of the parameter and not a source type. A check that had to be
    # weakened later is worse than one that was honest about its scope now.
    for source_type in ("kafka", "oracle", "bigquery"):
        assert source_type not in help_text.lower()


# --------------------------------------------------------------------------------------
# run_replay
# --------------------------------------------------------------------------------------

REPLAY = BASE + ["--run-type", "kafka_replay", "--rerun-id", "INC1"]


def test_a_replay_must_state_its_shape_and_its_rerun_id():
    """--run-type has no default on purpose: the replay shapes fix different problems, and
    an operator picking the wrong one mid-incident is the mistake two jobs exist to prevent.
    --rerun-id is what isolates the checkpoint AND the Delta transaction identity."""
    args = _parse(run_replay.build_parser(), REPLAY)
    assert args.run_type == "kafka_replay"
    assert args.rerun_id == "INC1"


@pytest.mark.parametrize("missing", ["--run-type", "--rerun-id"])
def test_a_replay_without_a_shape_or_an_id_will_not_start(missing):
    argv = [a for i, a in enumerate(REPLAY) if REPLAY[i - 1] != missing and a != missing]
    with pytest.raises(SystemExit):
        _parse(run_replay.build_parser(), argv)


def test_every_replay_control_has_a_matching_flag():
    """The flags are generated from one list, so adding a control is one line - and this
    asserts the generation, because a hand-written flag list is what drifts."""
    parser = run_replay.build_parser()
    for name in run_replay._REPLAY_PARAMETERS:
        args = _parse(parser, REPLAY + [f"--{name.replace('_', '-')}", "value"])
        assert getattr(args, name) == "value"


def test_every_replay_control_any_shipped_source_declares_has_a_flag():
    """THE CROSSING TEST. The list above is what a support engineer can actually pass; a
    SOURCE_SPEC declaring a `replay_*` key with no flag here is a lever nobody can pull -
    the setting exists, the code reads it, and there is no way to get a value to it.

    Derived from the dispatch dict rather than hardcoded, so a source type added in a later
    stage is covered the day it declares its first replay control.
    """
    from kafka_ingest.framework import runner

    declared = {
        key
        for module in runner._SOURCES.values()
        for key in module.SOURCE_SPEC.operational_keys
        if key.startswith("replay_")
    }
    missing = sorted(declared - set(run_replay._REPLAY_PARAMETERS))
    assert not missing, f"replay controls with no --flag in run_replay.py: {missing}"


def test_replay_controls_reach_the_runner_as_job_parameters(monkeypatch):
    """Which means they are validated against the source type's own SOURCE_SPEC exactly like
    a YAML key: a misspelled control is an unknown-key error naming the key, not a silently
    ignored one. That is why they are not passed as a side channel."""
    captured = {}

    monkeypatch.setattr(run_replay.runner, "run", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr(
        "sys.argv",
        ["run-replay"] + REPLAY + ["--replay-starting-offsets", '{"demo.events.v1": {"0": 5}}'],
    )
    run_replay.main()

    assert captured["run_type"] == "kafka_replay"
    assert captured["job_parameters"]["rerun_id"] == "INC1"
    assert captured["job_parameters"]["replay_starting_offsets"] == '{"demo.events.v1": {"0": 5}}'


def test_a_blank_workflows_parameter_is_dropped_rather_than_passed_on(monkeypatch):
    """A Workflows parameter left empty arrives as "", which would otherwise look like a
    deliberately empty setting and override whatever the layer below it set."""
    captured = {}
    monkeypatch.setattr(run_replay.runner, "run", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr("sys.argv", ["run-replay"] + REPLAY + ["--replay-starting-timestamp", ""])
    run_replay.main()

    assert "replay_starting_timestamp" not in captured["job_parameters"]


def test_the_replay_entrypoint_names_no_source_type_either():
    """Which replay shapes exist is each source type's business. This file carries the
    string through and enumerates nothing."""
    help_text = run_replay.build_parser().format_help()
    assert "kafka" not in help_text.lower().replace("kafka_replay", "").replace("e.g. kafka", "")
