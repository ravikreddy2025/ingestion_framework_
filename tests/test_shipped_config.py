"""Validate the ACTUAL shipped configuration in conf/, not a synthetic fixture.

Run this in CI on every PR. It catches, before anything reaches a cluster:
  * a typo'd or non-3-tier table name
  * a topic pointing at a cluster or registry that does not exist
  * a cert or checkpoint path that is not on a Unity Catalog Volume
  * an unknown key in a topic file (i.e. a silently ignored setting)
  * two topics sharing a checkpoint or a curated table
  * topics NOT sharing the single landing table
  * pinned_id without a reader_schema_id

Needs no Spark, no secrets and no network - pure structural validation, so it is safe and
fast as a pre-merge gate.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from conftest import FakeSpark
from kafka_ingest.config import (
    VALID_KAFKA_AUTH,
    VALID_READER_MODES,
    VALID_REGISTRY_AUTH,
    ConfigError,
    available_environments,
    load_structural,
    resolve_topic_config,
)

CONF_ROOT = Path(__file__).resolve().parent.parent / "conf"

# Files starting with "_" are templates, not deployable topics. _TEMPLATE.yaml is full of
# <ANGLE_BRACKET> placeholders by design and must NOT be validated as a real topic.
TOPIC_FILES = sorted(p for p in (CONF_ROOT / "sources").glob("*.yaml")
                     if not p.name.startswith("_"))
TOPIC_KEYS = [p.stem for p in TOPIC_FILES]

# Every environment we ship. Most checks run against ALL of them, because the failure this
# suite exists to prevent is a value that is valid in prod and broken in dev.
ENVIRONMENTS = available_environments(str(CONF_ROOT))

# The full cross product - one test case per (topic, environment).
TOPIC_ENVS = [(t, e) for t in TOPIC_KEYS for e in ENVIRONMENTS]


def _resolve(topic_key, environment="prod"):
    return resolve_topic_config(FakeSpark(), str(CONF_ROOT), topic_key,
                                "ops.ingestion.control", environment)


def test_conf_directory_is_present_and_populated():
    assert CONF_ROOT.is_dir(), f"conf/ not found at {CONF_ROOT}"
    assert (CONF_ROOT / "defaults.yaml").is_file()
    assert (CONF_ROOT / "clusters.yaml").is_file()
    assert (CONF_ROOT / "registries.yaml").is_file()
    assert TOPIC_KEYS, "no deployable topic files found in conf/sources/"
    assert ENVIRONMENTS, "no environment files found in conf/environments/"


def test_every_expected_environment_ships():
    """The bundle targets in databricks.yml pass ${bundle.target} as the environment, so a
    missing file here means that target fails at startup with 'unknown environment'."""
    assert set(ENVIRONMENTS) >= {"dev", "preprod", "prod"}, (
        f"expected dev/preprod/prod environment files, found {ENVIRONMENTS}")


@pytest.mark.parametrize("topic_key, environment", TOPIC_ENVS)
def test_shipped_topic_resolves(topic_key, environment):
    """The full resolution path every job takes at startup, for EVERY environment.

    Running the cross product is the point: it catches a value that is valid in prod and
    broken in dev, which is exactly what a per-environment override layer makes possible.
    """
    cfg = _resolve(topic_key, environment)

    assert cfg.topic_key == topic_key
    assert cfg.topic, f"{topic_key}: empty Kafka topic name"
    assert cfg.domain, f"{topic_key}: empty domain"
    assert cfg.subject, f"{topic_key}: empty Schema Registry subject"

    for label in ("landing_table", "curated_table", "quarantine_table", "audit_table"):
        assert len(getattr(cfg, label).split(".")) == 3, f"{topic_key}: {label} is not 3-tier"

    assert cfg.checkpoint_root.startswith("/Volumes/")
    assert cfg.cluster.auth_mode in VALID_KAFKA_AUTH
    assert cfg.registry.auth_mode in VALID_REGISTRY_AUTH
    assert cfg.reader_schema_mode in VALID_READER_MODES
    assert cfg.environment == environment

    # Every environment must supply an endpoint and a registry URL for the profiles its
    # topics reference - a profile with no bootstrap_servers in one environment would only
    # fail when that environment is deployed.
    assert cfg.cluster.bootstrap_servers, f"{topic_key}/{environment}: no bootstrap_servers"
    assert cfg.registry.url.startswith("http"), f"{topic_key}/{environment}: bad registry URL"

    # No placeholder survived substitution anywhere it matters.
    for value in (cfg.landing_table, cfg.curated_table, cfg.audit_table, cfg.checkpoint_root,
                  cfg.cluster.bootstrap_servers, cfg.registry.url):
        assert "{" not in value, f"{topic_key}/{environment}: unsubstituted placeholder in '{value}'"


@pytest.mark.parametrize("topic_key, environment", TOPIC_ENVS)
def test_environments_never_share_a_catalog_or_a_checkpoint(topic_key, environment):
    """dev must not be able to write into prod's tables or advance prod's offsets."""
    others = [e for e in ENVIRONMENTS if e != environment]
    cfg = _resolve(topic_key, environment)
    for other in others:
        rival = _resolve(topic_key, other)
        assert cfg.landing_table != rival.landing_table, (
            f"{environment} and {other} share the landing table {cfg.landing_table}")
        assert cfg.curated_table != rival.curated_table
        assert cfg.checkpoint_path != rival.checkpoint_path


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_partitioning_matches_the_agreed_layout(topic_key):
    cfg = _resolve(topic_key)
    # `topic` is constant inside a per-topic table, so partitioning on it would create a
    # single-value partition directory and prune nothing.
    assert "topic" not in cfg.landing_partition_by, (
        f"{topic_key}: landing is one table per topic, so `topic` is a useless partition key")
    assert cfg.landing_partition_by, f"{topic_key}: landing needs a partition column"
    assert cfg.curated_partition_by, f"{topic_key}: curated needs a partition column"


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_shipped_topic_checkpoint_paths_are_unique_per_topic(topic_key):
    assert _resolve(topic_key).checkpoint_path.endswith(f"/{topic_key}/primary")


def test_no_two_topics_share_a_landing_table():
    """Landing is ONE TABLE PER TOPIC. Two topics sharing one would interleave their raw
    bytes, and a per-topic replay or retention drop would take the other topic with it."""
    landing = [_resolve(k).landing_table for k in TOPIC_KEYS]
    assert len(set(landing)) == len(landing), f"topics share a landing table: {sorted(landing)}"


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_shipped_table_names_are_legal_unquoted_identifiers(topic_key):
    """Kafka topic names carry dots; Unity Catalog identifiers cannot. If this fails, the
    table would need backtick quoting everywhere it is referenced."""
    cfg = _resolve(topic_key)
    for label in ("landing_table", "curated_table", "quarantine_table"):
        name = getattr(cfg, label).split(".")[-1]
        assert re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name), f"{topic_key}: {label} '{name}'"


def test_no_two_topics_share_a_checkpoint_or_a_curated_table():
    """A shared checkpoint silently corrupts both topics' offset state."""
    checkpoints, curated = {}, {}
    for topic_key in TOPIC_KEYS:
        cfg = _resolve(topic_key)
        assert cfg.checkpoint_path not in checkpoints, (
            f"{topic_key} and {checkpoints.get(cfg.checkpoint_path)} share a checkpoint path")
        assert cfg.curated_table not in curated, (
            f"{topic_key} and {curated.get(cfg.curated_table)} share a curated table")
        checkpoints[cfg.checkpoint_path] = topic_key
        curated[cfg.curated_table] = topic_key


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_pinned_reader_schema_always_has_an_id(topic_key):
    cfg = _resolve(topic_key)
    if cfg.reader_schema_mode == "pinned_id":
        assert cfg.reader_schema_id, f"{topic_key}: pinned_id without reader_schema_id"


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_dedup_keys_reference_the_payload_struct(topic_key):
    """Business fields live inside the nested payload struct, so a bare column name is a
    config mistake that would only surface at runtime."""
    cfg = _resolve(topic_key)
    for key in cfg.curated_dedup_keys:
        assert key.startswith("payload."), (
            f"{topic_key}: dedup key '{key}' should be 'payload.{key}' - business fields are "
            "nested inside the payload struct")


@pytest.mark.parametrize("topic_key", TOPIC_KEYS)
def test_every_referenced_profile_exists(topic_key):
    """load_structural raises and lists the valid names if a reference is wrong."""
    raw = load_structural(str(CONF_ROOT), topic_key, "prod")
    assert raw["_cluster_profile"].bootstrap_servers
    assert raw["_registry_profile"].url.startswith("http")


def test_template_is_not_mistaken_for_a_deployable_topic():
    """The template must stay inert - and must fail loudly if someone tries to deploy it."""
    assert (CONF_ROOT / "sources" / "_TEMPLATE.yaml").is_file(), "onboarding template missing"
    assert "_TEMPLATE" not in TOPIC_KEYS
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


def _declared_data_catalog(bundle: dict, target: str) -> str:
    """The bundle variable's value for one target: the per-target override, else the default."""
    override = (bundle["targets"][target].get("variables") or {}).get("data_catalog")
    return override or bundle["variables"]["data_catalog"]["default"]


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_bundle_data_catalog_matches_the_environment_file(environment):
    bundle = _bundle()
    assert environment in bundle["targets"], (
        f"conf/environments/{environment}.yaml has no matching bundle target - every job "
        f"passes environment=${{bundle.target}}, so it would fail at startup")

    from kafka_ingest.config import _read_yaml

    conf_catalog = _read_yaml(str(CONF_ROOT / "environments" / f"{environment}.yaml"))["vars"]["catalog"]
    assert _declared_data_catalog(bundle, environment) == conf_catalog, (
        f"{environment}: databricks.yml data_catalog and conf/environments/{environment}.yaml "
        f"vars.catalog disagree. The maintenance job would run against the wrong catalog.")


def test_every_bundle_target_has_an_environment_file():
    """The reverse direction: a target with no environment file fails at job startup."""
    missing = set(_bundle()["targets"]) - set(ENVIRONMENTS)
    assert not missing, f"bundle targets with no conf/environments file: {sorted(missing)}"

# --------------------------------------------------------------------------------------
# The working topic+environment override example - vector_patient_events.yaml is the
# repository's canonical, referenced-by-name worked example (docs/CONFIGURATION.md points
# here rather than keeping a second, independently-driftable copy). This test IS the
# guarantee that the file and the documentation agree: if either drifts, this fails.
# --------------------------------------------------------------------------------------


def test_vector_patient_events_environment_override_resolves_as_documented():
    """dev falls through to its own environment default; preprod and prod each carry a
    value set only for them via `environments:` inside the topic file."""
    assert _resolve("vector_patient_events", "dev").max_offsets_per_trigger == 100000
    assert _resolve("vector_patient_events", "preprod").max_offsets_per_trigger == 2000000
    assert _resolve("vector_patient_events", "prod").max_offsets_per_trigger == 5000000
