"""Shared fixtures.

The config and security layers deliberately have no PySpark import, so their tests run in
plain CI without a Spark install. Tests that genuinely need Spark call
pytest.importorskip("pyspark") at module level.
"""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

# --------------------------------------------------------------------------------------
# DDL readers - shared by every test that asserts a table's columns.
#
# Three definitions of every framework table exist and must agree: the DDL string that
# creates it, the StructType that builds the DataFrame written into it, and the CREATE
# TABLE in sql/ that provisions it ahead of the first run. These two helpers are how that
# agreement is asserted rather than left to a comment saying "keep these in step".
# --------------------------------------------------------------------------------------


def ddl_column_names(ddl: str) -> list:
    """Pull column names out of a DDL column list, ignoring types, comments and nesting."""
    names, depth = [], 0
    for raw_line in ddl.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Skip continuation lines inside a nested type such as ARRAY<STRUCT<...>>.
        if depth == 0:
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+\S", line)
            if match:
                names.append(match.group(1))
        depth += line.count("<") - line.count(">")
    return names


def sql_table_columns(sql: str, table_suffix: str) -> list:
    """Column names, in order, from the CREATE TABLE whose name ends with `table_suffix`."""
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+\S*" + re.escape(table_suffix) + r"\s*\((.*?)\n\)",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(sql)
    assert match, f"no CREATE TABLE ending in '{table_suffix}' found"
    return ddl_column_names(match.group(1))


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
    """Minimal SparkSession stand-in for config, control, state and table-DDL tests.

    Records rather than executes: `sql_statements` and `created_frames` are what the
    framework's DDL and state writes are asserted against. Nothing here simulates Spark
    semantics - the assertions are about what the framework HANDS to Spark, which is
    exactly the part that has to be right.
    """

    def __init__(self, control_rows=None, existing_tables=(), conf=None, rows_by_table=None):
        self.catalog = FakeCatalog(existing_tables)
        self._control_rows = control_rows or []
        self._rows_by_table = dict(rows_by_table or {})
        self.conf = FakeConf(conf)
        self.sql_statements = []
        self.created_frames = []

    def table(self, name):
        return FakeDataFrame(self._rows_by_table.get(name, self._control_rows))

    def sql(self, statement):
        self.sql_statements.append(statement)
        return FakeDataFrame([])

    def createDataFrame(self, data, schema=None):  # noqa: N802 - mirrors the Spark API
        rows = list(data)
        self.created_frames.append((rows, schema))
        return RecordingDataFrame(rows)


class FakeRow:
    def __init__(self, data):
        self._data = data

    def asDict(self):  # noqa: N802 - mirrors the Spark API
        return dict(self._data)


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows
        self.filters = []

    def where(self, _condition):
        return self

    def filter(self, condition):
        """Records the predicate instead of applying it - see FakeSpark's docstring."""
        self.filters.append(condition)
        return FakeDataFrame(self._rows)

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

    Mirrors the shipped shape: defaults.yaml + defaults/kafka.yaml +
    environments/{dev,prod}.yaml + clusters/registries registers + one source file. `dev`
    deliberately overrides several defaults so the layering tests have something to assert
    against.
    """
    (tmp_path / "sources").mkdir()
    (tmp_path / "defaults").mkdir()
    (tmp_path / "environments").mkdir()

    # Layer 1: common to every source of every type.
    (tmp_path / "defaults.yaml").write_text(
        textwrap.dedent(
            """
            defaults:
              audit_table: "{catalog}.audit.ingest_audit"
              control_table: "{ops_catalog}.ingestion.ingest_control"
            """
        ).strip(),
        encoding="utf-8",
    )

    # Layer 1b: common to every Kafka source. Everything Kafka-shaped lives here, so an
    # Oracle or file source in the same tree never sees it.
    (tmp_path / "defaults" / "kafka.yaml").write_text(
        textwrap.dedent(
            """
            defaults:
              landing_table: "{catalog}.landing.{topic_table}"
              curated_table: "{catalog}.curated.{topic_table}"
              quarantine_table: "{catalog}.landing.{topic_table}_quarantine"
              checkpoint_root: "/Volumes/{catalog}/ingestion/checkpoints"
              consumer_group_prefix: "dbx-{source_key}"
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
              ops_catalog: ops_dev
            defaults: {}
            defaults_by_type:
              kafka:
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
              ops_catalog: ops_prod
            defaults: {}
            defaults_by_type: {}
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

    (tmp_path / "sources" / "demo_topic.yaml").write_text(
        textwrap.dedent(
            """
            source_type: kafka

            source:
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


# --------------------------------------------------------------------------------------
# Framework fixtures - a SYNTHETIC source type.
#
# The framework's config tests deliberately use a made-up source type rather than kafka,
# oracle or file. Two reasons, and both matter:
#   * it proves the loader is spec-driven, because nothing about "demo" exists anywhere in
#     framework/ - if a test passes here it passed on the spec alone;
#   * it keeps these tests stable while Stages 3-5 fill the real specs in.
# --------------------------------------------------------------------------------------


def _demo_spec():
    """Imported lazily so conftest itself stays importable with nothing installed."""
    from kafka_ingest.framework.contracts import SourceSpec

    return SourceSpec(
        source_type="demo",
        required_keys=frozenset({"object_name", "widget"}),
        structural_keys=frozenset(
            {
                "object_name",
                "widget",
                "audit_table",
                "landing_table",
                "partition_by",
                "trigger",
                "batch_limit",
                "failure_mode",
                "cursor_column",
                "full_refresh",
            }
        ),
        # `trigger`, `batch_limit` and `failure_mode` are settable in BOTH places - the
        # normal case. `reset_id` is operational ONLY (a YAML value would re-apply an
        # incident bypass on every future deploy), and `partition_by` / `landing_table` /
        # `object_name` are structural ONLY (they describe what is already on disk).
        operational_keys=frozenset({"trigger", "batch_limit", "failure_mode", "reset_id"}),
        mutually_exclusive=(("cursor_column", "full_refresh"),),
        layers=("landing",),
        # Mirrors sources/kafka/spec.py's shape (docs/build_log/DECISIONS.md D-01): three
        # of the four operational keys get their own prefixed control-table column.
        # `trigger` deliberately does NOT - not every operational key needs one, and a test
        # exercises that it simply cannot be set from the control table without one.
        control_columns={
            "demo_failure_mode": "failure_mode",
            "demo_batch_limit": "batch_limit",
            "demo_reset_id": "reset_id",
        },
    )


@pytest.fixture
def demo_spec():
    return _demo_spec()


@pytest.fixture
def demo_config_root(tmp_path: Path) -> str:
    """A complete conf/ tree for one synthetic source type, with every layer represented."""
    (tmp_path / "sources").mkdir()
    (tmp_path / "defaults").mkdir()
    (tmp_path / "environments").mkdir()

    # Layer 1: every source of every type. failure_mode is set here AND in the per-type
    # file, so the tests can prove which wins.
    (tmp_path / "defaults.yaml").write_text(
        textwrap.dedent(
            """
            defaults:
              audit_table: "{catalog}.audit.ingest_audit"
              state_table: "{ops_catalog}.ingestion.ingest_state"
              failure_mode: QUARANTINE
            """
        ).strip(),
        encoding="utf-8",
    )

    # Layer 1b: every source of THIS type.
    (tmp_path / "defaults" / "demo.yaml").write_text(
        textwrap.dedent(
            """
            defaults:
              landing_table: "{catalog}.landing.{source_key}"
              partition_by: [ingest_date]
              trigger: availableNow
              batch_limit: 1000
              failure_mode: FAILFAST
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "environments" / "dev.yaml").write_text(
        textwrap.dedent(
            """
            vars:
              catalog: cat_dev
              ops_catalog: ops_dev
            defaults:
              audit_table: "{catalog}.audit.dev_audit"
            defaults_by_type:
              demo:
                batch_limit: 10
            widgets:
              main:
                endpoint: "dev-endpoint:1521"
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "environments" / "prod.yaml").write_text(
        textwrap.dedent(
            """
            vars:
              catalog: cat_prod
              ops_catalog: ops_prod
            defaults: {}
            defaults_by_type: {}
            widgets:
              main:
                endpoint: "prod-endpoint:1521"
            """
        ).strip(),
        encoding="utf-8",
    )

    # A register: what exists, plus the secret KEY names. Never a secret value.
    (tmp_path / "widgets.yaml").write_text(
        textwrap.dedent(
            """
            widgets:
              main:
                auth_mode: basic
                password_key: widget-pw
                wallet_path: "/Volumes/{catalog}/certs/wallet"
              spare:
                auth_mode: none
            """
        ).strip(),
        encoding="utf-8",
    )

    (tmp_path / "sources" / "demo_source.yaml").write_text(
        textwrap.dedent(
            """
            source_type: demo

            source:
              domain: demo
              object_name: WIDGET_EVENTS
              widget: main
            """
        ).strip(),
        encoding="utf-8",
    )
    return str(tmp_path)
