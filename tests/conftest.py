"""Shared fixtures.

The config and security layers deliberately have no PySpark import, so their tests run in
plain CI without a Spark install. Tests that genuinely need Spark call
pytest.importorskip("pyspark") at module level.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest


class FakeSecrets:
    """Stands in for SecretResolver. Returns a predictable value per (scope, key)."""

    def __init__(self, values=None):
        self.values = values or {}
        self.requested = []

    def get(self, scope, key):
        self.requested.append((scope, key))
        return self.values.get((scope, key), f"{scope}/{key}/value")


class FakeCatalog:
    def __init__(self, existing=()):
        self._existing = set(existing)

    def tableExists(self, name):  # noqa: N802 - mirrors the Spark API
        return name in self._existing


class FakeConf:
    """Stands in for SparkSession.conf, and records every set/unset in order.

    The order matters: curated_writer sets the legacy schema-evolution flag around a single
    merge and must put it back afterwards.
    """

    def __init__(self, initial=None):
        self.values = dict(initial or {})
        self.history = []

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value
        self.history.append(("set", key, value))

    def unset(self, key):
        self.values.pop(key, None)
        self.history.append(("unset", key, None))


class FakeSpark:
    """Minimal SparkSession stand-in for config resolution tests."""

    def __init__(self, control_rows=None, existing_tables=(), conf=None):
        self.catalog = FakeCatalog(existing_tables)
        self._control_rows = control_rows or []
        self.conf = FakeConf(conf)

    def table(self, name):
        return FakeDataFrame(self._control_rows)


class FakeRow:
    def __init__(self, data):
        self._data = data

    def asDict(self):  # noqa: N802 - mirrors the Spark API
        return dict(self._data)


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def where(self, _condition):
        return self

    def limit(self, _n):
        return self

    def collect(self):
        return [FakeRow(r) for r in self._rows]

    def count(self):
        # pipeline.guard_against_checkpoint_reset counts landing rows for one topic.
        return len(self._rows)


# --------------------------------------------------------------------------------------
# Recording write path - lets the writers be tested without Delta, Spark or a cluster.
#
# The writers' whole job is to hand Delta the right options: txnAppId/txnVersion decide
# whether a retried batch is deduplicated, and append-vs-MERGE decides whether a replay
# duplicates data. Both are plain method calls, so recording the call chain tests exactly
# the thing that matters.
# --------------------------------------------------------------------------------------


class RecordingWriter:
    """Stands in for DataFrameWriter, capturing the chain instead of writing."""

    def __init__(self):
        self.format_used = None
        self.mode_used = None
        self.options = {}
        self.partition_by = None
        self.saved_as = None

    def format(self, source):
        self.format_used = source
        return self

    def mode(self, how):
        self.mode_used = how
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def partitionBy(self, *columns):  # noqa: N802 - mirrors the Spark API
        self.partition_by = list(columns)
        return self

    def saveAsTable(self, name):  # noqa: N802 - mirrors the Spark API
        self.saved_as = name


class RecordingDataFrame:
    """A DataFrame stand-in that exposes a RecordingWriter as `.write`."""

    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.write = RecordingWriter()
        self.alias_used = None

    def alias(self, name):
        self.alias_used = name
        return self

    def count(self):
        return len(self._rows)


class RecordingMerge:
    def __init__(self, source, condition):
        self.source = source
        self.condition = condition
        self.clauses = []
        self.executed = False

    def withSchemaEvolution(self):  # noqa: N802 - mirrors the Delta API (DBR 15.4 LTS+)
        self.clauses.append("withSchemaEvolution")
        return self

    def whenMatchedUpdateAll(self):  # noqa: N802 - mirrors the Delta API
        self.clauses.append("whenMatchedUpdateAll")
        return self

    def whenNotMatchedInsertAll(self):  # noqa: N802 - mirrors the Delta API
        self.clauses.append("whenNotMatchedInsertAll")
        return self

    def execute(self):
        self.executed = True


class RecordingDeltaTable:
    """Stands in for delta.tables.DeltaTable. Captures the most recent merge for assertion.

    Deliberately does NOT simulate merge semantics - the tests assert which branch was taken
    and what condition it was given, not what Delta would do with it.
    """

    last = None

    def __init__(self, name):
        self.name = name
        self.alias_used = None
        self.merge_op = None

    @classmethod
    def forName(cls, _spark, name):  # noqa: N802 - mirrors the Delta API
        table = cls(name)
        cls.last = table
        return table

    def alias(self, name):
        self.alias_used = name
        return self

    def merge(self, source, condition):
        self.merge_op = RecordingMerge(source, condition)
        return self.merge_op


@pytest.fixture
def fake_delta(monkeypatch):
    """Install a stand-in `delta.tables` module.

    The writers import DeltaTable lazily, inside the function, precisely so the module can
    be imported without Delta present - which is what makes this fixture possible.
    """
    import sys
    import types

    RecordingDeltaTable.last = None
    tables_module = types.ModuleType("delta.tables")
    tables_module.DeltaTable = RecordingDeltaTable
    package = types.ModuleType("delta")
    package.tables = tables_module
    monkeypatch.setitem(sys.modules, "delta", package)
    monkeypatch.setitem(sys.modules, "delta.tables", tables_module)
    return RecordingDeltaTable


@pytest.fixture
def reset_from_avro_selfcheck():
    """Clear curated_writer's once-per-process self-check latch.

    assert_from_avro_semantics() latches on success so it costs nothing after the first
    microbatch - correct in production. In a test session it means whichever test touches
    the parser first consumes the check, and the test that exists to VERIFY the from_avro
    writer/reader mapping on this runtime then passes without running anything.

    Imported lazily so conftest itself stays importable without PySpark.
    """
    from kafka_ingest import curated_writer

    previous = curated_writer._SEMANTICS_CHECKED
    curated_writer._SEMANTICS_CHECKED = False
    yield
    curated_writer._SEMANTICS_CHECKED = previous


@pytest.fixture
def secrets():
    return FakeSecrets()


@pytest.fixture
def config_root(tmp_path: Path) -> str:
    """A minimal but valid structural config tree with all five layers represented.

    Mirrors the shipped shape: defaults.yaml + environments/{dev,prod}.yaml +
    clusters/registries registers + one topic file. `dev` deliberately overrides several
    defaults so the layering tests have something to assert against.
    """
    (tmp_path / "topics").mkdir()
    (tmp_path / "environments").mkdir()

    (tmp_path / "defaults.yaml").write_text(
        textwrap.dedent(
            """
            topic_defaults:
              landing_table: "{catalog}.landing.{topic_table}"
              curated_table: "{catalog}.curated.{topic_table}"
              quarantine_table: "{catalog}.landing.{topic_table}_quarantine"
              audit_table: "{catalog}.audit.stream_audit"
              checkpoint_root: "/Volumes/{catalog}/ingestion/checkpoints"
              consumer_group_prefix: "dbx-{topic_key}"
              starting_offsets: earliest
              trigger: availableNow
              fail_on_data_loss: true
              include_headers: true
              landing_partition_by: [ingest_date]
              curated_partition_by: [event_date]
              curated_dedup_order_by: kafka_timestamp
              reader_schema_mode: registry_latest
              on_deser_error: fail
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "environments" / "dev.yaml").write_text(
        textwrap.dedent(
            """
            vars:
              catalog: cat_dev
            topic_defaults:
              max_offsets_per_trigger: 100
              starting_offsets: latest
            clusters:
              cc_shared:
                bootstrap_servers: "dev-broker:9092"
                secret_scope: kv-dev
            registries:
              sr_shared:
                url: "https://sr-dev.example.com"
                secret_scope: kv-dev
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "environments" / "prod.yaml").write_text(
        textwrap.dedent(
            """
            vars:
              catalog: cat_prod
            topic_defaults: {}
            clusters:
              cc_shared:
                bootstrap_servers: "prod-broker:9092"
                secret_scope: kv-prod
              onprem_mtls:
                bootstrap_servers: "prod-mtls:9094"
                secret_scope: kv-prod
            registries:
              sr_shared:
                url: "https://sr-prod.example.com"
                secret_scope: kv-prod
              sr_mtls:
                url: "https://sr-mtls-prod.example.com"
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "clusters.yaml").write_text(
        textwrap.dedent(
            """
            clusters:
              cc_shared:
                auth_mode: sasl_plain
                sasl_username_key: api-key
                sasl_password_key: api-secret
              onprem_mtls:
                auth_mode: mtls
                truststore_path: "/Volumes/{catalog}/certs/truststore.jks"
                truststore_password_key: ts-pw
                keystore_path: "/Volumes/{catalog}/certs/keystore.jks"
                keystore_password_key: ks-pw
                key_password_key: key-pw
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "registries.yaml").write_text(
        textwrap.dedent(
            """
            registries:
              sr_shared:
                auth_mode: basic
                username_key: sr-key
                password_key: sr-secret
              sr_mtls:
                auth_mode: mtls
                client_cert_path: "/Volumes/{catalog}/certs/sr.pem"
                client_key_path: "/Volumes/{catalog}/certs/sr-key.pem"
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "topics" / "demo_topic.yaml").write_text(
        textwrap.dedent(
            """
            topic:
              topic: demo.events.v1
              domain: demo
              cluster: cc_shared
              registry: sr_shared
              subject: demo.events.v1-value
              consumer_group_prefix: dbx-demo
            """
        ).strip(),
        encoding="utf-8",
    )
    return str(tmp_path)
