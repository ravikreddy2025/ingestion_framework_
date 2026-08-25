"""The shipped job definitions in resources/, against the code and config they invoke.

These are static checks, not deployments: there is no workspace here and
`databricks bundle validate` cannot run (VB-13). What they catch is the class of mistake
that survives every other test in this repository - a job definition that is internally
plausible and refers to something that does not exist. A task naming a source-key with no
YAML file behind it fails at 03:30 in an environment nobody is watching, with an argparse
error nobody reads.

Stage 6 generalises this over every job template; today it covers the Oracle job, which is
the one Stage 4 added.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
RESOURCES = REPO / "resources"
CONF = REPO / "conf"

ORACLE_JOB = RESOURCES / "job_ingest_oracle.yml"

# The wheel entry points declared in pyproject.toml. A task naming anything else installs
# fine and fails on the first run.
ENTRY_POINTS = {"run-ingest", "run-replay"}


def _job(path: Path, name: str) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return document["resources"]["jobs"][name]


@pytest.fixture(scope="module")
def oracle_job() -> dict:
    assert ORACLE_JOB.is_file(), f"{ORACLE_JOB} is missing"
    return _job(ORACLE_JOB, "ingest_oracle")


def test_the_oracle_job_runs_one_extract_at_a_time(oracle_job):
    """Two concurrent runs of one table would read overlapping intervals and race to
    advance a single watermark - and the loser's rows would be attributed to a window the
    winner has already declared complete."""
    assert oracle_job["max_concurrent_runs"] == 1


def test_a_run_that_arrives_while_one_is_in_flight_is_dropped(oracle_job):
    """Queueing would start a second extract against the source database the moment the
    first finished, which is precisely the load pattern a DBA asks us not to produce. The
    watermark means the next scheduled run covers the interval anyway."""
    assert oracle_job["queue"]["enabled"] is False


def test_every_task_calls_the_shipped_entrypoint_with_a_source_key(oracle_job):
    for task in oracle_job["tasks"]:
        wheel = task["python_wheel_task"]
        assert wheel["package_name"] == "kafka_ingest"
        assert wheel["entry_point"] in ENTRY_POINTS
        assert wheel["named_parameters"]["source-key"], f"{task['task_key']}: no source-key"


def test_every_task_names_a_source_that_actually_exists(oracle_job):
    """The failure this file exists for. A source-key with no conf/sources/<key>.yaml is a
    task that fails at startup, on a schedule, in an environment nobody is watching."""
    for task in oracle_job["tasks"]:
        source_key = task["python_wheel_task"]["named_parameters"]["source-key"]
        assert (CONF / "sources" / f"{source_key}.yaml").is_file(), f"{source_key}: no source file"


def test_every_task_names_an_oracle_source(oracle_job):
    """The job is separate from the Kafka one for operational reasons - a different
    schedule negotiation and a different blast radius - so a Kafka topic appearing in it
    would defeat the separation without failing anything."""
    for task in oracle_job["tasks"]:
        source_key = task["python_wheel_task"]["named_parameters"]["source-key"]
        declared = yaml.safe_load((CONF / "sources" / f"{source_key}.yaml").read_text(encoding="utf-8"))
        assert declared["source_type"] == "oracle", f"{source_key} is not an Oracle source"


def test_the_environment_is_taken_from_the_bundle_target(oracle_job):
    """Declared once in databricks.yml and never typed again - a task hardcoding an
    environment would read prod's config from the dev deployment."""
    parameters = {p["name"]: p["default"] for p in oracle_job["parameters"]}
    assert parameters["environment"] == "${bundle.target}"
    for task in oracle_job["tasks"]:
        assert task["python_wheel_task"]["named_parameters"]["environment"] == "{{job.parameters.environment}}"


def test_retry_settings_are_stated_on_every_task(oracle_job):
    """Retries are safe by construction here - the watermark advances only after a
    committed write, so a retry re-extracts the identical interval - but the settings had
    already drifted apart once on the Kafka job, so they are asserted rather than trusted."""
    for task in oracle_job["tasks"]:
        assert task["max_retries"] >= 1
        assert task["min_retry_interval_millis"] >= 300000
        assert task["retry_on_timeout"] is False
        assert task["timeout_seconds"] > 0


def test_the_oracle_onboarding_template_is_not_a_deployable_job(oracle_job):
    """The template is inert by the underscore convention; this is the other half - no job
    task may reference it."""
    for task in oracle_job["tasks"]:
        assert not task["python_wheel_task"]["named_parameters"]["source-key"].startswith("_")


# --------------------------------------------------------------------------------------
# The runbook's queries have to exist
# --------------------------------------------------------------------------------------


def test_every_oracle_query_the_runbook_cites_exists(oracle_job):
    """docs/RUNBOOK_SUPPORT.md section 8 sends a support engineer to Q17-Q24 during an
    incident. A citation pointing at a query that does not exist is discovered at exactly
    the wrong moment - this is the same failure sql/03 already had once with a rename."""
    support_sql = (REPO / "sql" / "03_support_queries.sql").read_text(encoding="utf-8")
    runbook = (REPO / "docs" / "RUNBOOK_SUPPORT.md").read_text(encoding="utf-8")

    oracle_section = runbook.split("## 8. Oracle incident playbooks", 1)
    assert len(oracle_section) == 2, "the Oracle playbook section is missing from the runbook"

    for number in range(17, 25):
        assert f"-- Q{number}." in support_sql, f"sql/03 has no Q{number}"
    for cited in ("Q17", "Q18", "Q21", "Q23", "Q24"):
        assert cited in oracle_section[1], f"the Oracle playbook never cites {cited}"
