"""Validate the ACTUAL shipped configuration in conf/, not a synthetic fixture.

Run this in CI on every PR. It catches, before anything reaches a cluster:
  * a typo'd or non-3-tier table name
  * a source pointing at a cluster or registry that does not exist
  * a cert or checkpoint path that is not on a Unity Catalog Volume
  * an unknown key in a source file (i.e. a silently ignored setting)
  * two sources sharing a checkpoint, a landing table or a curated table
  * pinned_id without a reader_schema_id

Needs no Spark, no secrets and no network - pure structural validation, so it is safe and
fast as a pre-merge gate.

Everything here goes through the REAL resolution path a job takes at startup: the
framework's five-layer loader validating against the source type's own SOURCE_SPEC, then
that source type building and validating its own frozen config. Testing a shortcut would
prove the shortcut works.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from kafka_ingest.framework import tables
from kafka_ingest.framework.config import ConfigError, available_environments, load_structural, resolve_config
from kafka_ingest.sources import kafka
from kafka_ingest.sources.kafka import config as kafka_config
from kafka_ingest.sources.kafka.config import (
    RUN_TYPE_PRIMARY,
    VALID_KAFKA_AUTH,
    VALID_READER_MODES,
    VALID_REGISTRY_AUTH,
)

CONF_ROOT = Path(__file__).resolve().parent.parent / "conf"

# Files starting with "_" are templates, not deployable sources. _TEMPLATE.yaml is full of
# <ANGLE_BRACKET> placeholders by design and must NOT be validated as a real source.
SOURCE_FILES = sorted(p for p in (CONF_ROOT / "sources").glob("*.yaml") if not p.name.startswith("_"))
SOURCE_KEYS = [p.stem for p in SOURCE_FILES]

# Every environment we ship. Most checks run against ALL of them, because the failure this
# suite exists to prevent is a value that is valid in prod and broken in dev.
ENVIRONMENTS = available_environments(str(CONF_ROOT))

# The full cross product - one test case per (source, environment).
SOURCE_ENVS = [(s, e) for s in SOURCE_KEYS for e in ENVIRONMENTS]


def _settings(source_key, environment="prod"):
    """Layers 1-3 only, exactly as framework/runner.py reads them to find the control table."""
    return load_structural(str(CONF_ROOT), source_key, environment, kafka.SOURCE_SPEC.target_tokens)


def _resolve(source_key, environment="prod", run_type=RUN_TYPE_PRIMARY, **job_parameters):
    """The full startup path: framework resolution, then the source's own config build."""
    resolved = resolve_config(str(CONF_ROOT), source_key, environment, kafka.SOURCE_SPEC, job_parameters=job_parameters)
    return kafka_config.build(resolved, run_type, tables)


@pytest.fixture
def source_key_any():
    """Any one shipped source: these settings come from conf/defaults.yaml, so every source
    resolves the same value and testing all of them would assert the same thing N times."""
    return SOURCE_KEYS[0]


def test_conf_directory_is_present_and_populated():
    assert CONF_ROOT.is_dir(), f"conf/ not found at {CONF_ROOT}"
    assert (CONF_ROOT / "defaults.yaml").is_file()
    assert (CONF_ROOT / "clusters.yaml").is_file()
    assert (CONF_ROOT / "registries.yaml").is_file()
    assert SOURCE_KEYS, "no deployable source files found in conf/sources/"
    assert ENVIRONMENTS, "no environment files found in conf/environments/"


def test_every_expected_environment_ships():
    """The bundle targets in databricks.yml pass ${bundle.target} as the environment, so a
    missing file here means that target fails at startup with 'unknown environment'."""
    assert set(ENVIRONMENTS) >= {"dev", "preprod", "prod"}, (
        f"expected dev/preprod/prod environment files, found {ENVIRONMENTS}"
    )


@pytest.mark.parametrize("source_key, environment", SOURCE_ENVS)
def test_shipped_source_resolves(source_key, environment):
    """The full resolution path every job takes at startup, for EVERY environment.

    Running the cross product is the point: it catches a value that is valid in prod and
    broken in dev, which is exactly what a per-environment override layer makes possible.
    """
    cfg = _resolve(source_key, environment)

    assert cfg.source_key == source_key
    assert cfg.topic, f"{source_key}: empty Kafka topic name"
    assert cfg.domain, f"{source_key}: empty domain"
    assert cfg.subject, f"{source_key}: empty Schema Registry subject"

    for label in ("landing_table", "curated_table", "quarantine_table"):
        assert len(getattr(cfg, label).split(".")) == 3, f"{source_key}: {label} is not 3-tier"

    assert cfg.checkpoint_root.startswith("/Volumes/")
    assert cfg.cluster.auth_mode in VALID_KAFKA_AUTH
    assert cfg.registry.auth_mode in VALID_REGISTRY_AUTH
    assert cfg.reader_schema_mode in VALID_READER_MODES
    assert cfg.environment == environment

    # Every environment must supply an endpoint and a registry URL for the profiles its
    # sources reference - a profile with no bootstrap_servers in one environment would only
    # fail when that environment is deployed.
    assert cfg.cluster.bootstrap_servers, f"{source_key}/{environment}: no bootstrap_servers"
    assert cfg.registry.url.startswith("http"), f"{source_key}/{environment}: bad registry URL"

    # No placeholder survived substitution anywhere it matters.
    for value in (
        cfg.landing_table,
        cfg.curated_table,
        cfg.checkpoint_root,
        cfg.cluster.bootstrap_servers,
        cfg.registry.url,
    ):
        assert "{" not in value, f"{source_key}/{environment}: unsubstituted placeholder in '{value}'"


@pytest.mark.parametrize("source_key, environment", SOURCE_ENVS)
def test_the_four_reader_options_are_configured_in_every_environment(source_key, environment):
    """The four options sources/kafka/reader.py always sets need values to set them FROM.

    includeHeaders is not configurable and needs nothing here. The other three do, and two
    of them fail SILENTLY when wrong rather than loudly: an unset max_offsets_per_trigger
    would mean one unbounded microbatch, and an unset min_partitions would cap read
    parallelism at the topic's partition count.
    """
    cfg = _resolve(source_key, environment)
    assert cfg.max_offsets_per_trigger > 0
    assert cfg.min_partitions > 0
    assert isinstance(cfg.fail_on_data_loss, bool)


@pytest.mark.parametrize("source_key, environment", SOURCE_ENVS)
def test_environments_never_share_a_catalog_or_a_checkpoint(source_key, environment):
    """dev must not be able to write into prod's tables or advance prod's offsets."""
    cfg = _resolve(source_key, environment)
    for other in [e for e in ENVIRONMENTS if e != environment]:
        rival = _resolve(source_key, other)
        assert cfg.landing_table != rival.landing_table, (
            f"{environment} and {other} share the landing table {cfg.landing_table}"
        )
        assert cfg.curated_table != rival.curated_table
        assert cfg.checkpoint_path != rival.checkpoint_path


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_partitioning_matches_the_agreed_layout(source_key):
    cfg = _resolve(source_key)
    # `topic` is constant inside a per-topic table, so partitioning on it would create a
    # single-value partition directory and prune nothing.
    assert "topic" not in cfg.landing_partition_by, (
        f"{source_key}: landing is one table per topic, so `topic` is a useless partition key"
    )
    assert cfg.landing_partition_by, f"{source_key}: landing needs a partition column"
    assert "event_date" in cfg.curated_partition_by, (
        f"{source_key}: curated must be partitioned by event_date - it is what bounds a "
        "curated replay's MERGE to the days being replayed"
    )


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_shipped_checkpoint_paths_are_unique_per_source(source_key):
    assert _resolve(source_key).checkpoint_path.endswith(f"/{source_key}/primary")


def test_no_two_sources_share_a_landing_table():
    """Landing is ONE TABLE PER TOPIC. Two sources sharing one would interleave their raw
    bytes, and a per-topic replay or retention drop would take the other one with it."""
    landing = [_resolve(k).landing_table for k in SOURCE_KEYS]
    assert len(set(landing)) == len(landing), f"sources share a landing table: {sorted(landing)}"


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_shipped_table_names_are_legal_unquoted_identifiers(source_key):
    """Kafka topic names carry dots; Unity Catalog identifiers cannot. If this fails, the
    table would need backtick quoting everywhere it is referenced."""
    cfg = _resolve(source_key)
    for label in ("landing_table", "curated_table", "quarantine_table"):
        name = getattr(cfg, label).split(".")[-1]
        assert re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name), f"{source_key}: {label} '{name}'"


def test_no_two_sources_share_a_checkpoint_or_a_curated_table():
    """A shared checkpoint silently corrupts both sources' offset state."""
    checkpoints, curated = {}, {}
    for source_key in SOURCE_KEYS:
        cfg = _resolve(source_key)
        assert cfg.checkpoint_path not in checkpoints, (
            f"{source_key} and {checkpoints.get(cfg.checkpoint_path)} share a checkpoint path"
        )
        assert cfg.curated_table not in curated, (
            f"{source_key} and {curated.get(cfg.curated_table)} share a curated table"
        )
        checkpoints[cfg.checkpoint_path] = source_key
        curated[cfg.curated_table] = source_key


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_pinned_reader_schema_always_has_an_id(source_key):
    cfg = _resolve(source_key)
    if cfg.reader_schema_mode == "pinned_id":
        assert cfg.reader_schema_id, f"{source_key}: pinned_id without reader_schema_id"


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_dedup_keys_reference_the_payload_struct(source_key):
    """Business fields live inside the nested payload struct, so a bare column name is a
    config mistake that would only surface at runtime."""
    cfg = _resolve(source_key)
    for key in cfg.curated_dedup_keys:
        assert key.startswith("payload."), (
            f"{source_key}: dedup key '{key}' should be 'payload.{key}' - business fields are "
            "nested inside the payload struct"
        )


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_every_referenced_profile_exists(source_key):
    """Resolution raises and lists the valid names if a reference is wrong."""
    cfg = _resolve(source_key)
    assert cfg.cluster.bootstrap_servers
    assert cfg.registry.url.startswith("http")


@pytest.mark.parametrize("source_key", SOURCE_KEYS)
def test_no_shipped_source_checks_an_incident_lever_into_git(source_key):
    """checkpoint_reset_id and every replay control are operational-ONLY.

    A value here would silently re-apply on every future deploy, long after the incident
    that justified it - which for the reset id specifically means bypassing the guard
    against silent data loss forever.
    """
    settings = _settings(source_key)
    for key in sorted(kafka.SOURCE_SPEC.operational_keys - kafka.SOURCE_SPEC.structural_keys):
        assert key not in settings, f"{source_key}: '{key}' is operational-only and must not be in YAML"


def test_template_is_not_mistaken_for_a_deployable_source():
    """The template must stay inert - and must fail loudly if someone tries to deploy it."""
    assert (CONF_ROOT / "sources" / "_TEMPLATE.yaml").is_file(), "onboarding template missing"
    assert "_TEMPLATE" not in SOURCE_KEYS
    with pytest.raises(ConfigError):
        _resolve("_TEMPLATE")


# --------------------------------------------------------------------------------------
# databricks.yml <-> conf/environments agreement
#
# The data catalog is declared twice: in conf/environments/<env>.yaml (which the Python
# reads) and as the `data_catalog` bundle variable (which the SQL maintenance job reads,
# because SQL cannot read conf/). Duplication that nothing checks is duplication that
# drifts, and a mismatch would silently point maintenance at a catalog that does not exist.
# --------------------------------------------------------------------------------------

BUNDLE_PATH = CONF_ROOT.parent / "databricks.yml"


def _bundle() -> dict:
    import yaml

    return yaml.safe_load(BUNDLE_PATH.read_text(encoding="utf-8"))


def _environment_vars(environment: str) -> dict:
    import yaml

    doc = yaml.safe_load((CONF_ROOT / "environments" / f"{environment}.yaml").read_text(encoding="utf-8"))
    return doc["vars"]


def _declared_variable(bundle: dict, target: str, name: str) -> str:
    """A bundle variable's value for one target: the per-target override, else the default."""
    override = (bundle["targets"][target].get("variables") or {}).get(name)
    return override or bundle["variables"][name]["default"]


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_bundle_data_catalog_matches_the_environment_file(environment):
    bundle = _bundle()
    assert environment in bundle["targets"], (
        f"conf/environments/{environment}.yaml has no matching bundle target - every job "
        f"passes environment=${{bundle.target}}, so it would fail at startup"
    )
    assert _declared_variable(bundle, environment, "data_catalog") == _environment_vars(environment)["catalog"], (
        f"{environment}: databricks.yml data_catalog and conf/environments/{environment}.yaml "
        f"vars.catalog disagree. The maintenance job would run against the wrong catalog."
    )


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_bundle_ops_catalog_matches_the_environment_file(environment):
    """Same duplication, same risk, one layer down: `vars.ops_catalog` names the catalog
    holding the control and state tables, and databricks.yml declares it too because
    sql/01_operational_config.sql is rendered from the bundle AND the maintenance job binds
    it as a parameter. A mismatch points the job at a control table nobody edits."""
    assert _declared_variable(_bundle(), environment, "ops_catalog") == _environment_vars(environment)["ops_catalog"], (
        f"{environment}: databricks.yml ops_catalog and conf/environments/{environment}.yaml vars.ops_catalog disagree."
    )


def test_every_bundle_target_has_an_environment_file():
    """The reverse direction: a target with no environment file fails at job startup."""
    missing = set(_bundle()["targets"]) - set(ENVIRONMENTS)
    assert not missing, f"bundle targets with no conf/environments file: {sorted(missing)}"


# --------------------------------------------------------------------------------------
# The working source+environment override example - vector_patient_events.yaml is the
# repository's canonical, referenced-by-name worked example (docs/CONFIGURATION.md points
# here rather than keeping a second, independently-driftable copy). This test IS the
# guarantee that the file and the documentation agree: if either drifts, this fails.
# --------------------------------------------------------------------------------------


def test_vector_patient_events_environment_override_resolves_as_documented():
    """dev falls through to its own environment default; preprod and prod each carry a
    value set only for them via `environments:` inside the source file."""
    assert _resolve("vector_patient_events", "dev").max_offsets_per_trigger == 100000
    assert _resolve("vector_patient_events", "preprod").max_offsets_per_trigger == 2000000
    assert _resolve("vector_patient_events", "prod").max_offsets_per_trigger == 5000000


# --------------------------------------------------------------------------------------
# The framework's own three tables
#
# audit, state and control are named ONCE, in conf/defaults.yaml, from vars.ops_catalog and
# the three schema vars. They used to be named twice - once there and once as a bundle
# variable passed in as a job parameter - and the copy was still pointing at a table that
# had been renamed. These two tests are what stops that coming back.
# --------------------------------------------------------------------------------------

FRAMEWORK_TABLES = ("audit_table", "state_table", "control_table")


@pytest.mark.parametrize("environment", ENVIRONMENTS)
@pytest.mark.parametrize("setting", FRAMEWORK_TABLES)
def test_the_framework_tables_resolve_to_legal_names(source_key_any, environment, setting):
    """Resolved through the real conf/, in every environment. A placeholder that does not
    resolve, or a two-part name, fails here rather than on a cluster."""
    tables.validate_name(_settings(source_key_any, environment)[setting], f"conf/defaults.yaml {setting}")


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_the_frameworks_own_tables_all_live_under_the_ops_catalog(source_key_any, environment):
    """docs/build_log/DECISIONS.md D-06: audit, control and state all moved under
    {ops_catalog}, split by schema. A name that is syntactically legal but points at the
    wrong catalog would pass the test above without this one - `{catalog}.audit.
    ingest_audit` is just as legal a name as `{ops_catalog}.{audit_schema}.ingest_audit`."""
    ops_catalog = _environment_vars(environment)["ops_catalog"]
    settings = _settings(source_key_any, environment)
    for setting in FRAMEWORK_TABLES:
        assert settings[setting].startswith(f"{ops_catalog}."), (
            f"{setting} in environment '{environment}' does not live under the ops catalog "
            f"'{ops_catalog}': {settings[setting]}"
        )


def test_the_bundle_does_not_also_name_the_control_table():
    """One name in one place. A bundle variable holding a table name the code derives is a
    variable that drifts - and the one this replaced was pointing at a table Stage 2
    renamed, which nothing would have caught."""
    bundle = _bundle()
    declared = set(bundle["variables"])
    for target in bundle["targets"].values():
        declared |= set(target.get("variables") or {})
    assert "control_table" not in declared, (
        "databricks.yml declares control_table again - it is named in conf/defaults.yaml, "
        "and two names for one table is how the two stop agreeing"
    )
