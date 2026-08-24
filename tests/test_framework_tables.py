"""Target names and table creation.

Two failures this module exists to prevent, both of which are only visible on a cluster
otherwise:

  * a table name that is legal in the system being read and illegal in Unity Catalog. It
    has to fail at configuration load, because failing at write time means failing after
    the read has already run.
  * a CREATE TABLE that asks for PARTITIONED BY and CLUSTER BY at once. Delta accepts one
    or the other, and its error names neither the table nor the setting.

Needs no Spark: framework/tables.py builds a SQL string and hands it to a session, so a
recording stand-in tests exactly the thing that matters.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from conftest import FakeSpark
from kafka_ingest.framework import tables
from kafka_ingest.framework.config import ConfigError, resolve_config


@pytest.fixture
def cfg(demo_config_root, demo_spec):
    return resolve_config(demo_config_root, "demo_source", "prod", demo_spec)


# --------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------


def test_a_three_part_name_is_accepted():
    assert tables.validate_name("cat.landing.orders", "where") == "cat.landing.orders"


@pytest.mark.parametrize("name", ["landing.orders", "orders", "cat.a.b.c"])
def test_a_name_that_is_not_three_part_is_rejected(name):
    """A two-part name resolves against whatever the session default happens to be, which
    is how a dev run writes into prod."""
    with pytest.raises(ConfigError, match="three-part Unity Catalog name"):
        tables.validate_name(name, "sources/demo.yaml setting 'landing_table'")


@pytest.mark.parametrize(
    "name",
    [
        "cat.landing.ORDER$",  # legal in Oracle, illegal unquoted in UC
        "cat.landing.order#items",
        "cat.landing.2024_orders",  # leading digit
        "cat.land-ing.orders",  # hyphen, as a Kafka topic would supply
    ],
)
def test_a_name_illegal_in_unity_catalog_is_rejected_naming_the_part(name):
    with pytest.raises(ConfigError, match="not a legal"):
        tables.validate_name(name, "where")


def test_a_missing_name_is_rejected_rather_than_stringified():
    """`None` reaching a saveAsTable() would produce a table literally called 'None'."""
    with pytest.raises(ConfigError, match="expected a table name"):
        tables.validate_name(None, "sources/demo.yaml setting 'landing_table'")


def test_a_pattern_is_filled_from_tokens_the_source_supplies():
    """This is what lets Oracle name its target from SCHEMA and TABLE without the framework
    ever learning what a schema is."""
    rendered = tables.render(
        "{catalog}.landing.{source_schema}_{source_table}",
        {"catalog": "cat", "source_schema": "hr", "source_table": "employees"},
        "conf/defaults/demo.yaml",
    )
    assert rendered == "cat.landing.hr_employees"


def test_an_unsupplied_token_names_itself_rather_than_reaching_a_cluster():
    with pytest.raises(ConfigError, match=r"uses \{source_table\}"):
        tables.render("{catalog}.landing.{source_table}", {"catalog": "cat"}, "where")


def test_targets_resolve_one_table_per_layer_from_the_spec(cfg):
    """The framework asks the spec which layers exist and reads `<layer>_table` for each.
    It never enumerates layer names, which is what keeps a fourth source type free."""
    assert tables.targets(cfg) == {"landing": "cat_prod.landing.demo_source"}


def test_validate_targets_covers_the_frameworks_own_tables_too(cfg):
    names = tables.validate_targets(cfg)
    assert names["landing"] == "cat_prod.landing.demo_source"
    assert names["audit_table"] == "cat_prod.audit.ingest_audit"
    assert names["state_table"] == "ops_prod.ingestion.ingest_state"


def test_a_configuration_with_no_state_table_is_rejected(demo_config_root, demo_spec, tmp_path):
    """A run with nowhere to keep its run sequence is a run whose re-run behaviour nobody
    can reason about, so this is required rather than defaulted."""
    defaults = f"{demo_config_root}/defaults.yaml"
    with open(defaults, "r", encoding="utf-8") as handle:
        text = handle.read()
    with open(defaults, "w", encoding="utf-8") as handle:
        handle.write(text.replace('  state_table: "{ops_catalog}.ingestion.ingest_state"\n', ""))

    cfg = resolve_config(demo_config_root, "demo_source", "prod", demo_spec)
    with pytest.raises(ConfigError, match="state_table"):
        tables.validate_targets(cfg)


# --------------------------------------------------------------------------------------
# Creation
# --------------------------------------------------------------------------------------


def test_create_table_is_if_not_exists_and_carries_the_configured_properties(cfg):
    spark = FakeSpark()
    tables.ensure_table(
        spark,
        "cat.landing.orders",
        "a INT, b STRING",
        "a comment",
        properties={"delta.autoOptimize.autoCompact": "true"},
        partition_by=["ingest_date"],
    )

    statement = spark.sql_statements[0]
    assert "CREATE TABLE IF NOT EXISTS cat.landing.orders" in statement
    assert "PARTITIONED BY (ingest_date)" in statement
    assert "'delta.autoOptimize.autoCompact' = 'true'" in statement
    assert "COMMENT 'a comment'" in statement


def test_a_table_with_no_layout_gets_neither_clause():
    """The state table is a handful of rows; any physical layout on it costs more to
    maintain than it could save."""
    spark = FakeSpark()
    tables.ensure_table(spark, "ops.ingestion.ingest_state", "a INT", "state")
    statement = spark.sql_statements[0]
    assert "PARTITIONED BY" not in statement
    assert "CLUSTER BY" not in statement


def test_clustering_is_available_instead_of_partitioning():
    spark = FakeSpark()
    tables.ensure_table(spark, "cat.landing.orders", "a INT", "c", cluster_by=["a"])
    assert "CLUSTER BY (a)" in spark.sql_statements[0]


def test_partitioning_and_clustering_together_is_rejected():
    """Delta accepts one or the other. Its own error names neither the table nor the
    setting that caused it, so this is caught here."""
    with pytest.raises(ConfigError, match="one or the other"):
        tables.ensure_table(FakeSpark(), "cat.landing.orders", "a INT", "c", partition_by=["d"], cluster_by=["a"])


def test_creating_a_table_validates_its_name_first():
    with pytest.raises(ConfigError, match="three-part"):
        tables.ensure_table(FakeSpark(), "landing.orders", "a INT", "c")


def test_no_properties_at_all_still_gets_auto_compaction():
    """An implicitly created Delta table gets no TBLPROPERTIES, so the table people query
    ends up the only one without compaction. The fallback stops that."""
    clause = tables.properties_clause(None)
    assert "'delta.autoOptimize.optimizeWrite' = 'true'" in clause
    assert "'delta.autoOptimize.autoCompact' = 'true'" in clause


def test_a_quote_in_a_property_is_rejected_rather_than_escaped():
    """A Delta property name containing a single quote is a typo, not a use case - and
    escaping it would silently produce a broken TBLPROPERTIES clause."""
    with pytest.raises(ConfigError, match="single quote"):
        tables.properties_clause({"delta.it's": "true"})


def test_a_quote_in_the_comment_is_rejected_too():
    with pytest.raises(ConfigError, match="single quote"):
        tables.ensure_table(FakeSpark(), "cat.a.b", "a INT", "don't")


# --------------------------------------------------------------------------------------
# Tokens only the source can fill
#
# A target pattern like `{catalog}.landing.{topic_table}` names one thing configuration
# knows and one thing it cannot: the second comes from a Kafka topic name, or an Oracle
# SCHEMA and TABLE. The source type declares those in SOURCE_SPEC.target_tokens, config.py
# leaves them alone, and the source renders them at the top of its run.
# --------------------------------------------------------------------------------------


@pytest.fixture
def deferred_spec(demo_spec):
    """The demo source, with a target pattern only the source can finish."""
    return dataclasses.replace(demo_spec, target_tokens=frozenset({"object_table"}))


@pytest.fixture
def deferred_config_root(demo_config_root):
    layer_defaults = f"{demo_config_root}/defaults/demo.yaml"
    with open(layer_defaults, "r", encoding="utf-8") as handle:
        text = handle.read()
    with open(layer_defaults, "w", encoding="utf-8") as handle:
        handle.write(text.replace("{catalog}.landing.{source_key}", "{catalog}.landing.{object_table}"))
    return demo_config_root


def test_a_declared_token_survives_configuration_load(deferred_config_root, deferred_spec):
    """Without this the whole pattern is unusable: config.py's unresolved-placeholder error
    would fire on a token that is not missing, only not known yet."""
    cfg = resolve_config(deferred_config_root, "demo_source", "prod", deferred_spec)
    assert cfg.get("landing_table") == "cat_prod.landing.{object_table}"
    assert tables.is_deferred(cfg.get("landing_table"))


def test_an_undeclared_token_is_still_a_hard_error(deferred_config_root, demo_spec):
    """The declaration is what separates "the source fills this" from "somebody typo'd a
    var name". Same file, same pattern, spec that does not declare it."""
    with pytest.raises(ConfigError, match=r"uses \{object_table\}"):
        resolve_config(deferred_config_root, "demo_source", "prod", demo_spec)


def test_the_source_renders_and_the_result_is_validated(deferred_config_root, deferred_spec):
    cfg = resolve_config(deferred_config_root, "demo_source", "prod", deferred_spec)
    assert tables.target(cfg, "landing", {"object_table": "widget_events"}) == ("cat_prod.landing.widget_events")


def test_a_token_the_source_forgot_to_supply_names_itself(deferred_config_root, deferred_spec):
    cfg = resolve_config(deferred_config_root, "demo_source", "prod", deferred_spec)
    with pytest.raises(ConfigError, match=r"uses \{object_table\}"):
        tables.target(cfg, "landing", {})


def test_a_rendered_name_that_is_illegal_in_unity_catalog_is_rejected(deferred_config_root, deferred_spec):
    """The whole point of deferring: the token's value comes from the source system, where
    `ORDER$` is a perfectly ordinary table name."""
    cfg = resolve_config(deferred_config_root, "demo_source", "prod", deferred_spec)
    with pytest.raises(ConfigError, match="not a legal"):
        tables.target(cfg, "landing", {"object_table": "ORDER$"})


def test_the_runner_skips_a_deferred_name_and_says_so(deferred_config_root, deferred_spec, caplog):
    """It cannot validate a name it cannot yet know. Skipping silently would be the same
    mistake as validating a string with a brace in it."""
    cfg = resolve_config(deferred_config_root, "demo_source", "prod", deferred_spec)
    with caplog.at_level("INFO"):
        names = tables.validate_targets(cfg)
    assert "landing" not in names
    assert "audit_table" in names
    assert "the source renders and validates it" in caplog.text


def test_the_shipped_kafka_spec_declares_the_token_its_defaults_use():
    """conf/defaults/kafka.yaml names all three targets `{catalog}.<layer>.{topic_table}`.
    If the spec stopped declaring it, every Kafka source would fail to resolve."""
    from kafka_ingest.sources.kafka import SOURCE_SPEC

    conf = Path(__file__).resolve().parent.parent / "conf" / "defaults" / "kafka.yaml"
    text = conf.read_text(encoding="utf-8")
    for layer in SOURCE_SPEC.layers:
        assert "{topic_table}" in text, f"{layer}_table no longer uses the declared token"
    assert "topic_table" in SOURCE_SPEC.target_tokens
