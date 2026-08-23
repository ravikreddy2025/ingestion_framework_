"""Config resolution, validation and the replay-checkpoint isolation rule."""

from __future__ import annotations

import pytest

from conftest import FakeSpark  # tests/ is on sys.path (no __init__.py, pytest prepend mode)
from kafka_ingest.config import (
    RUN_TYPE_CURATED_REPLAY,
    RUN_TYPE_KAFKA_REPLAY,
    RUN_TYPE_PRIMARY,
    ConfigError,
    KafkaClusterProfile,
    RunContext,
    SchemaRegistryProfile,
    resolve_topic_config,
)

# --------------------------------------------------------------------------------------
# Structural profile validation
# --------------------------------------------------------------------------------------


def test_certs_must_live_on_a_uc_volume():
    with pytest.raises(ConfigError, match="Unity Catalog Volume"):
        KafkaClusterProfile(
            name="bad", bootstrap_servers="b:9094", auth_mode="mtls",
            keystore_path="dbfs:/mnt/certs/keystore.jks",
            truststore_path="/Volumes/c/s/truststore.jks",
        )


def test_sasl_requires_secret_references():
    with pytest.raises(ConfigError, match="requires secret_scope"):
        KafkaClusterProfile(name="bad", bootstrap_servers="b:9092", auth_mode="sasl_plain")


def test_mtls_requires_both_stores():
    with pytest.raises(ConfigError, match="requires both keystore_path and truststore_path"):
        KafkaClusterProfile(name="bad", bootstrap_servers="b:9094", auth_mode="mtls",
                            keystore_path="/Volumes/c/s/ks.jks")


def test_registry_basic_auth_requires_keys():
    with pytest.raises(ConfigError, match="basic auth requires"):
        SchemaRegistryProfile(name="bad", url="https://sr", auth_mode="basic")


# --------------------------------------------------------------------------------------
# Replay control validation - the rules that stop a replay silently doing nothing
# --------------------------------------------------------------------------------------


def test_kafka_replay_without_rerun_id_is_rejected():
    with pytest.raises(ConfigError, match="isolates the replay checkpoint"):
        RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, starting_timestamp="2026-08-11T09:00:00Z").validate()


def test_kafka_replay_needs_a_start_position():
    with pytest.raises(ConfigError, match="requires exactly one of"):
        RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1").validate()


def test_offsets_and_timestamp_are_mutually_exclusive():
    ctx = RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1",
                     starting_offsets='{"t":{"0":1}}', starting_timestamp="2026-08-11T09:00:00Z")
    with pytest.raises(ConfigError, match="mutually exclusive"):
        ctx.validate()


@pytest.mark.parametrize("raw, message", [
    ("not json", "not valid JSON"),
    ("[]", "non-empty JSON object"),
    ('{"t": 5}', "must be an object of partition->offset"),
    ('{"t": {"0": "123"}}', "not an int offset"),
])
def test_malformed_starting_offsets_are_rejected(raw, message):
    ctx = RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="INC1", starting_offsets=raw)
    with pytest.raises(ConfigError, match=message):
        ctx.validate()


def test_rerun_id_must_be_path_safe():
    ctx = RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="../../etc", starting_timestamp="0")
    with pytest.raises(ConfigError, match="must match"):
        ctx.validate()


def test_curated_replay_requires_an_explicit_landing_filter():
    with pytest.raises(ConfigError, match="refusing to"):
        RunContext(run_type=RUN_TYPE_CURATED_REPLAY, rerun_id="FIX1").validate()


def test_bounded_only_when_an_end_is_given():
    assert not RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="a",
                          starting_timestamp="0").is_bounded
    assert RunContext(run_type=RUN_TYPE_KAFKA_REPLAY, rerun_id="a",
                      starting_timestamp="0", ending_timestamp="1").is_bounded


# --------------------------------------------------------------------------------------
# Resolution and merge precedence
# --------------------------------------------------------------------------------------


def _resolve(config_root, environment="prod", **kwargs):
    return resolve_topic_config(FakeSpark(), config_root, "demo_topic",
                                "ops.ingestion.control", environment, **kwargs)


def test_yaml_only_resolution(config_root):
    cfg = _resolve(config_root)
    assert cfg.topic == "demo.events.v1"
    assert cfg.cluster.name == "cc_shared"
    assert cfg.registry.url == "https://sr-prod.example.com"
    assert cfg.enabled is True
    assert cfg.on_deser_error == "fail"


def test_layer_defaults_match_the_agreed_layout(config_root):
    """Landing is one table per topic, partitioned by ingest_date; curated is also one per
    topic, partitioned by event_date."""
    cfg = _resolve(config_root)
    assert cfg.landing_partition_by == ["ingest_date"]
    assert cfg.curated_partition_by == ["event_date"]


# --------------------------------------------------------------------------------------
# The five-layer merge: defaults -> environment -> topic -> control table -> job params
# --------------------------------------------------------------------------------------


def test_defaults_apply_when_no_layer_overrides_them(config_root):
    """Layer 1 only. The topic file says nothing about triggers or failure modes."""
    cfg = _resolve(config_root)
    assert cfg.trigger == "availableNow"
    assert cfg.fail_on_data_loss is True
    assert cfg.reader_schema_mode == "registry_latest"


def test_environment_overrides_defaults(config_root):
    """Layer 2 beats layer 1. dev sets small batches and starts at latest."""
    dev = _resolve(config_root, "dev")
    prod = _resolve(config_root, "prod")
    assert dev.max_offsets_per_trigger == 100
    assert prod.max_offsets_per_trigger is None      # not set anywhere for prod
    assert dev.starting_offsets == "latest"          # dev override
    assert prod.starting_offsets == "earliest"       # falls through to defaults.yaml


def test_topic_overrides_environment_and_defaults(config_root):
    """Layer 3 beats layers 2 and 1."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  starting_offsets: earliest\n  on_deser_error: quarantine\n")
    dev = _resolve(config_root, "dev")
    assert dev.starting_offsets == "earliest"        # beats the dev environment override
    assert dev.on_deser_error == "quarantine"        # beats the defaults.yaml value


def test_checkpoint_reset_id_is_rejected_in_topic_yaml(config_root):
    """Control-table-only: a value checked into Git would silently re-apply the bypass on
    every future deploy, long after the incident that justified it - see config.py's
    load_structural rejection and RUNBOOK_SUPPORT.md 5.4a."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  checkpoint_reset_id: INC12345\n")
    with pytest.raises(ConfigError, match="checkpoint_reset_id.*control-table-only"):
        _resolve(config_root, "dev")


# --------------------------------------------------------------------------------------
# Topic-level environment overrides: topic:.environments.<env>
#
# A fourth structural sub-layer, still inside sources/<key>.yaml, for the topic whose
# tuning genuinely needs to differ in exactly one environment. Precedence:
#     defaults -> environment topic_defaults -> topic -> topic.environments.<env>
# --------------------------------------------------------------------------------------


def test_topic_environment_override_beats_the_bare_topic_value(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write(
            "\n  max_offsets_per_trigger: 500000\n"
            "  environments:\n"
            "    prod:\n"
            "      max_offsets_per_trigger: 5000000\n"
        )
    assert _resolve(config_root, "prod").max_offsets_per_trigger == 5000000


def test_topic_environment_override_applies_only_to_that_environment(config_root):
    """The whole point: dev must be untouched by a setting written for prod."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write(
            "\n  max_offsets_per_trigger: 500000\n"
            "  environments:\n"
            "    prod:\n"
            "      max_offsets_per_trigger: 5000000\n"
        )
    # dev falls through to the bare topic-level value, exactly as if no override existed.
    assert _resolve(config_root, "dev").max_offsets_per_trigger == 500000


def test_topic_environment_override_still_beats_the_environment_layer(config_root):
    """Confirms the full order for the environment that HAS an override: env-defaults is
    beaten by the bare topic value, which is in turn beaten by the topic+env value."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments:\n    dev:\n      max_offsets_per_trigger: 7\n")
    # dev/environments.yaml sets max_offsets_per_trigger: 100 at the environment layer.
    assert _resolve(config_root, "dev").max_offsets_per_trigger == 7


def test_topic_environment_override_can_use_placeholders(config_root):
    """{catalog} etc. must resolve inside an override exactly as it does everywhere else."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write(
            "\n  environments:\n"
            "    prod:\n"
            "      curated_table: \"{catalog}.curated.demo_special\"\n"
        )
    assert _resolve(config_root, "prod").curated_table == "cat_prod.curated.demo_special"


def test_topic_environment_block_naming_an_unknown_environment_is_rejected(config_root):
    """A typo here would otherwise be silently unused - it only takes effect when THAT
    environment happens to be the one being resolved, so nothing would ever catch it."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments:\n    staging:\n      max_offsets_per_trigger: 1\n")
    with pytest.raises(ConfigError, match=r"environments block names \['staging'\]"):
        _resolve(config_root, "dev")


def test_topic_environment_block_must_be_a_mapping(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments: not-a-mapping\n")
    with pytest.raises(ConfigError, match="must be a mapping"):
        _resolve(config_root, "dev")


def test_one_topic_environment_value_must_itself_be_a_mapping(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments:\n    dev: not-a-mapping\n")
    with pytest.raises(ConfigError, match=r"environments\.dev must be a mapping"):
        _resolve(config_root, "dev")


def test_topic_environment_override_never_leaks_as_an_unknown_key(config_root):
    """The `environments:` key itself must not reach the "unknown top-level key" check -
    only the settings written INSIDE it should be validated as topic settings."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments:\n    prod:\n      on_deser_error: quarantine\n")
    cfg = _resolve(config_root, "dev")   # dev has no override; must still resolve cleanly
    assert cfg.on_deser_error == "fail"


def test_an_unknown_key_inside_a_topic_environment_override_is_still_rejected(config_root):
    """The override is real topic config, so it goes through the same typo protection as
    every other topic setting - just one layer later."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  environments:\n    prod:\n      on_deser_eror: quarantine\n")
    with pytest.raises(ConfigError, match="unknown keys"):
        _resolve(config_root, "prod")


def test_a_topic_with_no_environments_block_is_unaffected(config_root):
    """Most topics never use this. Confirms it is purely additive."""
    cfg = _resolve(config_root, "prod")
    assert cfg.on_deser_error == "fail"


def test_same_topic_resolves_differently_per_environment(config_root):
    """The whole point: one topic file, one catalog per environment, no duplication."""
    dev, prod = _resolve(config_root, "dev"), _resolve(config_root, "prod")

    # Both layers are named from the Kafka topic, not the config key: demo.events.v1.
    assert dev.landing_table == "cat_dev.landing.demo_events_v1"
    assert prod.landing_table == "cat_prod.landing.demo_events_v1"
    assert dev.curated_table == "cat_dev.curated.demo_events_v1"
    assert prod.curated_table == "cat_prod.curated.demo_events_v1"
    assert dev.checkpoint_root == "/Volumes/cat_dev/ingestion/checkpoints"
    assert prod.checkpoint_root == "/Volumes/cat_prod/ingestion/checkpoints"
    # Nothing in the topic file changed - only the environment layer did.
    assert dev.topic == prod.topic == "demo.events.v1"


def test_environment_overrides_cluster_and_registry_endpoints(config_root):
    """dev Kafka is not prod Kafka, and the secret scope differs with it."""
    dev, prod = _resolve(config_root, "dev"), _resolve(config_root, "prod")
    assert dev.cluster.bootstrap_servers == "dev-broker:9092"
    assert prod.cluster.bootstrap_servers == "prod-broker:9092"
    assert dev.cluster.secret_scope == "kv-dev"
    assert prod.cluster.secret_scope == "kv-prod"
    assert dev.registry.url == "https://sr-dev.example.com"
    # Auth mode and key NAMES are env-neutral and come from the register file.
    assert dev.cluster.auth_mode == prod.cluster.auth_mode == "sasl_plain"
    assert dev.cluster.sasl_username_key == prod.cluster.sasl_username_key == "api-key"


def test_placeholders_resolve_in_certificate_paths_too(config_root):
    """Cert paths contain {catalog}, so they must be substituted per environment."""
    content = open(f"{config_root}/sources/demo_topic.yaml", encoding="utf-8").read()
    open(f"{config_root}/sources/demo_topic.yaml", "w", encoding="utf-8").write(
        content.replace("cluster: cc_shared", "cluster: onprem_mtls")
               .replace("registry: sr_shared", "registry: sr_mtls"))
    cfg = _resolve(config_root, "prod")
    assert cfg.cluster.truststore_path == "/Volumes/cat_prod/certs/truststore.jks"
    assert cfg.registry.client_cert_path == "/Volumes/cat_prod/certs/sr.pem"


def test_unknown_environment_lists_the_valid_ones(config_root):
    with pytest.raises(ConfigError, match=r"unknown environment 'uat'.*\['dev', 'prod'\]"):
        _resolve(config_root, "uat")


def test_unresolved_placeholder_is_a_hard_error(config_root):
    """A {token} with no matching var would otherwise create a table literally named
    '{region}.landing...' and fail much later, much less clearly."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write('\n  curated_table: "{region}.curated.demo"\n')
    with pytest.raises(ConfigError, match=r"uses \{region\}, which is not defined"):
        _resolve(config_root)


def test_environment_cannot_invent_a_cluster_profile(config_root, tmp_path):
    """clusters.yaml stays the single register of which clusters exist, so a typo in an
    environment file is an error rather than a silently-unused new profile."""
    import textwrap
    (tmp_path / "environments" / "typo.yaml").write_text(
        textwrap.dedent(
            """
            vars:
              catalog: cat_typo
            defaults: {}
            defaults_by_type: {}
            clusters:
              cc_shard:                      # typo: should be cc_shared
                bootstrap_servers: "nope:9092"
            """
        ).strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match=r"'cc_shard'.*not defined in clusters\.yaml"):
        _resolve(config_root, "typo")


def test_landing_partitioning_cannot_be_emptied(config_root):
    """Landing is created by explicit DDL, so an empty partition list would silently produce
    an unpartitioned table - and make retention a full rewrite instead of a partition drop.

    Note this is edited into the YAML, not passed as an override: partitioning is
    STRUCTURAL, so it is deliberately not in the set of fields the control table or a job
    parameter can change. Changing a table's physical layout needs a PR.
    """
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  landing_partition_by: []\n")
    with pytest.raises(ConfigError, match="must name at least one"):
        resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")


# --------------------------------------------------------------------------------------
# Table naming - one table per topic in both layers, derived from the Kafka topic name
# --------------------------------------------------------------------------------------


def test_table_name_is_derived_from_the_kafka_topic(config_root):
    """Dots and hyphens are conventional in Kafka topic names and illegal in an unquoted
    Unity Catalog identifier, so both become underscores."""
    cfg = _resolve(config_root)
    assert cfg.table_name == "demo_events_v1"
    assert cfg.landing_table.endswith(".landing.demo_events_v1")
    assert cfg.curated_table.endswith(".curated.demo_events_v1")
    assert cfg.quarantine_table.endswith(".landing.demo_events_v1_quarantine")


def test_both_layers_use_the_same_derived_name(config_root):
    """A landing table and its curated table should be recognisably the same feed."""
    cfg = _resolve(config_root)
    assert cfg.landing_table.split(".")[-1] == cfg.curated_table.split(".")[-1]


def test_a_topic_file_can_override_the_derived_name(config_root):
    """The escape hatch for a topic whose derived name collides or reads badly. It overrides
    the NAME only - catalog and schema still come from defaults.yaml, so a topic file still
    never contains a catalog."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  table_name: patient_events\n")
    cfg = _resolve(config_root)
    assert cfg.table_name == "patient_events"
    assert cfg.landing_table == "cat_prod.landing.patient_events"
    assert cfg.curated_table == "cat_prod.curated.patient_events"
    assert cfg.quarantine_table == "cat_prod.landing.patient_events_quarantine"


def test_an_override_applies_in_every_environment(config_root):
    """An override that only took effect in one environment would be worse than none."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  table_name: patient_events\n")
    assert _resolve(config_root, "dev").landing_table == "cat_dev.landing.patient_events"
    assert _resolve(config_root, "prod").landing_table == "cat_prod.landing.patient_events"


def test_an_underivable_topic_name_fails_loudly(config_root):
    """A name that cannot become a legal identifier must say so, not produce a mangled table
    that fails much later with a confusing Delta error."""
    path = f"{config_root}/sources/demo_topic.yaml"
    content = open(path, encoding="utf-8").read().replace("topic: demo.events.v1", "topic: 9demo.events")
    open(path, "w", encoding="utf-8").write(content)
    with pytest.raises(ConfigError, match="cannot derive a table name"):
        _resolve(config_root)


def test_missing_control_row_is_not_an_error(config_root):
    """A newly onboarded topic must run the moment its YAML merges."""
    spark = FakeSpark(control_rows=[], existing_tables={"ops.ingestion.control"})
    assert resolve_topic_config(spark, config_root, "demo_topic", "ops.ingestion.control", "prod").enabled


def test_operational_row_overrides_yaml(config_root):
    spark = FakeSpark(
        control_rows=[{"topic_key": "demo_topic", "enabled": False, "on_deser_error": "quarantine"}],
        existing_tables={"ops.ingestion.control"},
    )
    cfg = resolve_topic_config(spark, config_root, "demo_topic", "ops.ingestion.control", "prod")
    assert cfg.enabled is False
    assert cfg.on_deser_error == "quarantine"


def test_checkpoint_reset_id_flows_from_the_control_table(config_root):
    """The control table is the ONLY legal source for this override - unlike
    checkpoint_reset_id in topic YAML, which load_structural rejects outright."""
    spark = FakeSpark(
        control_rows=[{"topic_key": "demo_topic", "checkpoint_reset_id": "INC12345"}],
        existing_tables={"ops.ingestion.control"},
    )
    cfg = resolve_topic_config(spark, config_root, "demo_topic", "ops.ingestion.control", "prod")
    assert cfg.checkpoint_reset_id == "INC12345"


def test_job_parameters_beat_the_control_table(config_root):
    """An urgent override typed into Workflows must not need an UPDATE first."""
    spark = FakeSpark(control_rows=[{"topic_key": "demo_topic", "on_deser_error": "quarantine"}],
                      existing_tables={"ops.ingestion.control"})
    cfg = resolve_topic_config(spark, config_root, "demo_topic", "ops.ingestion.control", "prod",
                               overrides={"on_deser_error": "fail"})
    assert cfg.on_deser_error == "fail"


def test_duplicate_control_rows_are_rejected(config_root):
    spark = FakeSpark(control_rows=[{"topic_key": "demo_topic"}, {"topic_key": "demo_topic"}],
                      existing_tables={"ops.ingestion.control"})
    with pytest.raises(ConfigError, match="exactly one row per topic"):
        resolve_topic_config(spark, config_root, "demo_topic", "ops.ingestion.control", "prod")


def test_unknown_yaml_key_is_rejected(config_root):
    """A typo in structural config is silent misconfiguration - fail at load."""
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  curated_tabel: cat.curated.typo\n")
    with pytest.raises(ConfigError, match="unknown keys"):
        resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")


def test_unknown_cluster_reference_names_the_known_ones(config_root):
    path = f"{config_root}/sources/demo_topic.yaml"
    content = open(path, encoding="utf-8").read().replace("cluster: cc_shared", "cluster: nope")
    open(path, "w", encoding="utf-8").write(content)
    with pytest.raises(ConfigError, match="known: \\['cc_shared', 'onprem_mtls'\\]"):
        resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")


def test_three_tier_table_names_are_enforced(config_root):
    with open(f"{config_root}/sources/demo_topic.yaml", "a", encoding="utf-8") as handle:
        handle.write("\n  curated_table: curated.demo\n")   # missing the catalog
    with pytest.raises(ConfigError, match="3-tier UC name"):
        _resolve(config_root)


def test_reader_schema_writer_mode_no_longer_exists(config_root):
    """Removed on purpose: curated stores payload as ONE struct column, and two writer
    versions would produce two incompatible struct types that cannot be unioned."""
    with pytest.raises(ConfigError, match="reader_schema_mode"):
        resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
                             overrides={"reader_schema_mode": "writer"})


def test_pinned_reader_schema_requires_an_id(config_root):
    with pytest.raises(ConfigError, match="requires reader_schema_id"):
        resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
                             overrides={"reader_schema_mode": "pinned_id"})


# --------------------------------------------------------------------------------------
# Checkpoint isolation - the most important derived value in the framework
# --------------------------------------------------------------------------------------


def test_primary_and_replay_checkpoints_never_collide(config_root):
    primary = resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")
    replay = resolve_topic_config(
        FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
        run_type=RUN_TYPE_KAFKA_REPLAY,
        overrides={"rerun_id": "INC12345", "starting_timestamp": "2026-08-11T09:00:00Z"},
    )
    assert primary.checkpoint_path.endswith("/demo_topic/primary")
    assert replay.checkpoint_path.endswith("/demo_topic/replay/INC12345")
    assert primary.checkpoint_path != replay.checkpoint_path
    # A replay must never be nested inside the primary checkpoint directory either.
    assert not replay.checkpoint_path.startswith(primary.checkpoint_path + "/")


def test_two_replays_of_the_same_topic_are_isolated_from_each_other(config_root):
    def replay(rerun_id):
        return resolve_topic_config(
            FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
            run_type=RUN_TYPE_KAFKA_REPLAY,
            overrides={"rerun_id": rerun_id, "starting_timestamp": "0"},
        ).checkpoint_path

    assert replay("INC1") != replay("INC2")


def test_replay_group_id_is_distinguishable_on_the_broker(config_root):
    cfg = resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
                               run_type=RUN_TYPE_KAFKA_REPLAY,
                               overrides={"rerun_id": "INC1", "starting_timestamp": "0"})
    assert cfg.group_id_prefix == "dbx-demo-kafka_replay-INC1"
    assert cfg.ingested_via == RUN_TYPE_KAFKA_REPLAY


def test_primary_run_carries_no_replay_metadata(config_root):
    cfg = resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod")
    assert cfg.run.run_type == RUN_TYPE_PRIMARY
    assert cfg.run.rerun_id is None
    assert cfg.group_id_prefix == "dbx-demo"
