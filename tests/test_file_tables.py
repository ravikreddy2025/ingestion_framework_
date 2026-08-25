"""sources/file/tables.py - the landing DDL, built from a resolved read schema.

Unlike Kafka's and Oracle's, this source's projection (sources/file/landing.py) uses real
PySpark column expressions (`F.col`, `F.regexp_extract`), so - matching the precedent that
neither Kafka's landing.py nor Oracle's write path with real Spark calls is unit-tested
without a cluster - this file exercises only the DDL builder, which is pure Python over a
duck-typed schema (see conftest.FakeSchema).
"""

from __future__ import annotations

import pytest

from conftest import FakeSchema, write_file_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.file import tables

SCHEMA = FakeSchema({"claim_id": "string", "amount": "decimal(18,2)"})


def _cfg(file_config_root, **settings):
    from conftest import make_file_cfg

    write_file_source(file_config_root, **settings)
    return make_file_cfg(file_config_root)


def test_the_source_s_own_columns_come_first_and_keep_their_types(file_cfg):
    ddl = tables.landing_columns(SCHEMA, file_cfg)
    assert "claim_id string" in ddl
    assert "amount decimal(18,2)" in ddl
    assert ddl.index("claim_id") < ddl.index("_file_path")


def test_the_fixed_metadata_columns_are_all_present(file_cfg):
    ddl = tables.landing_columns(SCHEMA, file_cfg)
    for column in (
        "_file_path",
        "_file_name",
        "_file_size",
        "_file_modification_time",
        "_rescued_data",
        "source_key",
        "ingest_ts",
        "ingest_date",
        "ingested_via",
        "replay_run_id",
        "txn_version",
        "run_id",
    ):
        assert column in ddl, f"{column} missing from the landing DDL"


def test_filename_columns_are_declared_as_string(file_config_root):
    cfg = _cfg(file_config_root, filename_columns={"business_date": r"claims_(\d{8})\.csv"})
    ddl = tables.landing_columns(SCHEMA, cfg)
    assert "business_date STRING" in ddl


def test_rescued_data_in_the_raw_schema_is_not_declared_twice(file_cfg):
    """Auto Loader adds `_rescued_data` to the REAL read schema when the option is set - see
    tables.py's docstring. If the raw schema already carries it, the DDL must still declare
    it exactly once, from FIXED_METADATA_DDL, not from both."""
    schema_with_rescued = FakeSchema({"claim_id": "string", "_rescued_data": "string"})
    ddl = tables.landing_columns(schema_with_rescued, file_cfg)
    assert ddl.count("_rescued_data") == 1


def test_an_extract_that_returned_no_columns_creates_nothing(file_cfg):
    with pytest.raises(ConfigError, match="no columns"):
        tables.landing_columns(FakeSchema({}), file_cfg)


def test_ensure_landing_table_partitions_by_the_configured_columns(file_config_root):
    cfg = _cfg(file_config_root, landing_partition_by=["ingest_date"])

    class _Ctx:
        class tables:  # noqa: N801 - mirrors ctx.tables as a module-shaped stand-in
            created = []

            @classmethod
            def ensure_table(cls, spark, name, columns, comment, properties=None, partition_by=None, cluster_by=None):
                cls.created.append({"name": name, "partition_by": partition_by})

        spark = None

    tables.ensure_landing_table(_Ctx(), cfg, SCHEMA)
    assert _Ctx.tables.created[0]["name"] == cfg.landing_table
    assert _Ctx.tables.created[0]["partition_by"] == ["ingest_date"]
