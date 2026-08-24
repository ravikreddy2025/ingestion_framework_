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
        self._frames = {}

    def table(self, name):
        """One frame per table name, kept, so a test can read back the predicates a query
        was built with. A fresh frame per call would discard exactly that."""
        if name not in self._frames:
            self._frames[name] = FakeDataFrame(self._rows_by_table.get(name, self._control_rows))
        return self._frames[name]

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

    def __getitem__(self, key):
        """By column name, as pyspark.sql.Row does - a probe row is read that way."""
        return self._data[key]


class FakeDataFrame:
    """Records predicates instead of applying them.

    Nothing here simulates Spark semantics - the assertions are about what the framework
    HANDS to Spark, which is exactly the part that has to be right. `conditions` is how a
    test asserts on a WHERE clause whose correctness is the point (a query that must
    exclude the current run, say) without building a query engine to prove it.
    """

    def __init__(self, rows):
        self._rows = rows
        self.filters = []
        self.conditions = []

    def where(self, condition):
        self.conditions.append(condition)
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
    from kafka_ingest.sources.kafka import curated

    previous = curated._SEMANTICS_CHECKED
    curated._SEMANTICS_CHECKED = False
    yield
    curated._SEMANTICS_CHECKED = previous


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
              min_partitions: 32
              max_offsets_per_trigger: 1000000
              landing_partition_by: [ingest_date]
              curated_partition_by: [event_date]
              curated_dedup_order_by: kafka_timestamp
              reader_schema_mode: registry_latest
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
# Kafka fixtures.
#
# A source is handed a RunContext and nothing else, so the way to test one without a
# cluster is to hand it a RunContext of stand-ins and assert on what it did with them.
# `kafka_ctx` is that context; `kafka_cfg` resolves the source's own frozen config through
# the REAL five-layer path, because a hand-built config would prove only that the
# hand-built config works.
# --------------------------------------------------------------------------------------


class RecordingAudit:
    """Stands in for framework/audit.py's AuditWriter. Records rows instead of writing."""

    def __init__(self, run_id="run-1", run_type="primary"):
        self.run_id = run_id
        self.run_type = run_type
        self.source_ref = None
        self.rerun_id = None
        self.rows = []

    def emit(self, layer, status, txn_version=-1, **details):
        self.rows.append({"layer": layer, "status": status, "txn_version": txn_version, **details})

    def statuses(self, layer):
        return [row["status"] for row in self.rows if row["layer"] == layer]


class RecordingLog:
    """Stands in for framework/logs.py's RunLog."""

    def __init__(self):
        self.lines = []

    def _record(self, level, event, **fields):
        self.lines.append((level, event, fields))

    def info(self, event, **fields):
        self._record("INFO", event, **fields)

    def warning(self, event, **fields):
        self._record("WARNING", event, **fields)

    def error(self, event, **fields):
        self._record("ERROR", event, **fields)

    def events(self, level=None):
        return [event for lvl, event, _ in self.lines if level is None or lvl == level]

    def fields(self, event):
        return next(fields for _, name, fields in self.lines if name == event)


class RecordingWriters:
    """Stands in for framework/writers.py, recording every append and merge.

    Deliberately does NOT simulate Delta. What has to be right is WHICH call the source
    makes and WHAT it passes - append vs merge decides whether a replay duplicates, the
    txn markers decide whether a retry does, and the partition predicate decides how much
    of the table a replay rewrites. All three are visible from the call alone.
    """

    def __init__(self):
        self.appends = []
        self.merges = []

    def append(self, df, table, **kwargs):
        self.appends.append({"df": df, "table": table, **kwargs})

    def merge(self, spark, df, table, keys, partition_predicate, **kwargs):
        self.merges.append(
            {
                "df": df,
                "table": table,
                "keys": tuple(keys),
                "partition_predicate": partition_predicate,
                **kwargs,
            }
        )

    def merge_into(self, table):
        return next(m for m in self.merges if m["table"] == table)

    def append_into(self, table):
        return next(a for a in self.appends if a["table"] == table)


class RecordingTables:
    """Stands in for framework/tables.py. Real naming, recorded creation.

    `target`/`targets`/`validate_name` delegate to the real module: rendering a target
    pattern is exactly the behaviour a Kafka test wants exercised, not stubbed.
    """

    def __init__(self, existing=()):
        self.created = []
        self.existing = set(existing)

    def ensure_table(self, spark, name, columns, comment, properties=None, partition_by=None, cluster_by=None):
        self.created.append({"name": name, "columns": columns, "partition_by": partition_by})

    def table_exists(self, spark, name):
        return name in self.existing

    def target(self, cfg, layer, tokens=None):
        from kafka_ingest.framework import tables

        return tables.target(cfg, layer, tokens)

    def targets(self, cfg, tokens=None):
        from kafka_ingest.framework import tables

        return tables.targets(cfg, tokens)

    def validate_name(self, name, where):
        from kafka_ingest.framework import tables

        return tables.validate_name(name, where)

    def created_names(self):
        return [entry["name"] for entry in self.created]


def make_kafka_ctx(
    config_root,
    source_key="demo_topic",
    environment="dev",
    run_type="primary",
    spark=None,
    existing_tables=(),
    **job_parameters,
):
    """A RunContext carrying stand-ins, built through the real config resolution path."""
    from kafka_ingest.framework.config import resolve_config
    from kafka_ingest.framework.contracts import RunContext
    from kafka_ingest.sources import kafka

    cfg = resolve_config(config_root, source_key, environment, kafka.SOURCE_SPEC, job_parameters=job_parameters)
    return RunContext(
        cfg=cfg,
        spark=spark if spark is not None else FakeSpark(existing_tables=existing_tables),
        audit=RecordingAudit(run_type=run_type),
        state=None,
        writers=RecordingWriters(),
        tables=RecordingTables(existing_tables),
        log=RecordingLog(),
        run_id=f"{source_key}-{run_type}-test",
        run_type=run_type,
        run_sequence=1,
    )


def make_kafka_cfg(config_root, source_key="demo_topic", environment="dev", run_type="primary", **job_parameters):
    """The source's own frozen config, resolved exactly as run() resolves it."""
    from kafka_ingest.sources.kafka import config as kafka_config

    ctx = make_kafka_ctx(config_root, source_key, environment, run_type, **job_parameters)
    return kafka_config.build(ctx.cfg, run_type, ctx.tables)


@pytest.fixture
def kafka_ctx(config_root):
    return make_kafka_ctx(config_root)


@pytest.fixture
def kafka_cfg(config_root):
    return make_kafka_cfg(config_root)


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


# --------------------------------------------------------------------------------------
# Oracle fixtures.
#
# The tree is synthetic EXCEPT for conf/defaults/oracle.yaml, which is copied from the
# repository. That file carries the landing-table pattern, the fetch size and the
# partition count - the three shipped values whose absence or mis-spelling would be a real
# outage - so the tests resolve the same layer 1b a job resolves, not a paraphrase of it.
# --------------------------------------------------------------------------------------

REPO_CONF = Path(__file__).resolve().parent.parent / "conf"

# The source file every Oracle test starts from. `write_oracle_source` replaces or extends
# it, so a test states only the setting it is about.
ORACLE_SOURCE_DEFAULTS = {
    "jdbc_ref": "oracle_demo",
    "domain": "claims",
    "source_schema": "CLAIMS",
    "source_table": "CLAIM_HEADER",
}


@pytest.fixture
def oracle_config_root(tmp_path: Path) -> str:
    """A minimal but valid conf/ tree for one Oracle source, with every layer represented."""
    (tmp_path / "sources").mkdir()
    (tmp_path / "defaults").mkdir()
    (tmp_path / "environments").mkdir()

    (tmp_path / "defaults.yaml").write_text(
        textwrap.dedent(
            """
            defaults:
              audit_table: "{ops_catalog}.audit.ingest_audit"
              state_table: "{ops_catalog}.ingestion.ingest_state"
              control_table: "{ops_catalog}.ingestion.ingest_control"
            """
        ).strip(),
        encoding="utf-8",
    )

    # Layer 1b, verbatim from the repository - see the note above.
    (tmp_path / "defaults" / "oracle.yaml").write_text(
        (REPO_CONF / "defaults" / "oracle.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )

    for environment, catalog in (("dev", "cat_dev"), ("prod", "cat_prod")):
        (tmp_path / "environments" / f"{environment}.yaml").write_text(
            textwrap.dedent(
                f"""
                vars:
                  catalog: {catalog}
                  ops_catalog: ops_{environment}
                defaults: {{}}
                defaults_by_type: {{}}
                jdbc:
                  oracle_demo:
                    host: "oracle-{environment}.corp.internal"
                    secret_scope: kv-oracle-{environment}
                """
            ).strip(),
            encoding="utf-8",
        )

    # A register of one. Its CONTENTS are not read until sub-step 4b builds a connection
    # from them; what 4a asserts is that a jdbc_ref naming something absent from it fails.
    (tmp_path / "jdbc.yaml").write_text(
        textwrap.dedent(
            """
            jdbc:
              oracle_demo:
                port: 1521
                service_name: CLAIMSPDB
                username_key: oracle-user
                password_key: oracle-password
            """
        ).strip(),
        encoding="utf-8",
    )

    write_oracle_source(str(tmp_path))
    return str(tmp_path)


def write_oracle_source(config_root, source_key="demo_oracle", **settings) -> str:
    """Write conf/sources/<source_key>.yaml with the defaults plus whatever a test states.

    A setting passed as None is REMOVED rather than written as a null, so a test can say
    "this source does not set merge_keys at all" - which is a different configuration from
    setting it to an empty list, and the difference is load-bearing.
    """
    import yaml

    merged = {**ORACLE_SOURCE_DEFAULTS, **settings}
    document = {"source_type": "oracle", "source": {k: v for k, v in merged.items() if v is not None}}
    path = Path(config_root) / "sources" / f"{source_key}.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return source_key


def make_oracle_cfg(config_root, source_key="demo_oracle", environment="dev", run_type="primary", **job_parameters):
    """The source's own frozen config, resolved through the REAL five-layer path.

    Uses framework/tables.py itself rather than a stand-in: rendering the landing pattern
    and rejecting a name Unity Catalog cannot hold is part of what is being tested.
    """
    from kafka_ingest.framework import tables
    from kafka_ingest.framework.config import resolve_config
    from kafka_ingest.sources import oracle
    from kafka_ingest.sources.oracle import config as oracle_config

    resolved = resolve_config(config_root, source_key, environment, oracle.SOURCE_SPEC, job_parameters=job_parameters)
    return oracle_config.build(resolved, run_type, tables)


class RecordingState:
    """Stands in for framework/state.py's StateStore, recording every read and write.

    The Oracle source's correctness is an ORDERING - read, write, commit, then advance -
    so what a test needs is not a state table but a record of when state was written
    relative to everything else. `writes` is that record, and it is empty for every case
    where the watermark must NOT move.
    """

    def __init__(self, values=None, on_write=None):
        self.values = dict(values or {})
        self.reads = []
        self.writes = []
        self._on_write = on_write

    def read_state(self, source_key, state_key):
        self.reads.append((source_key, state_key))
        return self.values.get((source_key, state_key))

    def write_state(self, source_key, state_key, value, value_type, run_id):
        if self._on_write is not None:
            self._on_write()
        self.writes.append(
            {
                "source_key": source_key,
                "state_key": state_key,
                "value": value,
                "value_type": value_type,
                "run_id": run_id,
            }
        )
        self.values[(source_key, state_key)] = value

    def next_run_sequence(self, source_key):
        return 1

    @property
    def watermark(self):
        """The value the LAST write left, or None if the watermark never moved."""
        writes = [w for w in self.writes if w["state_key"] == "watermark"]
        return writes[-1]["value"] if writes else None


def make_oracle_ctx(
    config_root,
    source_key="demo_oracle",
    environment="dev",
    run_type="primary",
    spark=None,
    state=None,
    existing_tables=(),
    run_sequence=1,
    **job_parameters,
):
    """A RunContext of stand-ins, built through the real config resolution path.

    A source is handed a RunContext and nothing else, so this is how one is tested without
    a database: recording writers, a recording audit writer and a FakeSpark whose reader
    captures the options rather than connecting.
    """
    from kafka_ingest.framework.config import resolve_config
    from kafka_ingest.framework.contracts import RunContext
    from kafka_ingest.sources import oracle

    cfg = resolve_config(config_root, source_key, environment, oracle.SOURCE_SPEC, job_parameters=job_parameters)
    return RunContext(
        cfg=cfg,
        spark=spark if spark is not None else FakeJdbcSpark(),
        audit=RecordingAudit(run_type=run_type),
        state=state if state is not None else RecordingState(),
        writers=RecordingWriters(),
        tables=RecordingTables(existing_tables),
        log=RecordingLog(),
        run_id=f"{source_key}-{run_type}-test",
        run_type=run_type,
        run_sequence=run_sequence,
    )


@pytest.fixture
def oracle_cfg(oracle_config_root):
    """The default source: a full extract of CLAIMS.CLAIM_HEADER."""
    return make_oracle_cfg(oracle_config_root)


# --------------------------------------------------------------------------------------
# A Spark schema, without Spark. sources/oracle/types.py reads `.fields`, `.name` and
# `.dataType.simpleString()` by duck typing precisely so this is possible.
# --------------------------------------------------------------------------------------


class FakeType:
    def __init__(self, name):
        self._name = name

    def simpleString(self):  # noqa: N802 - mirrors the Spark API
        return self._name


class FakeField:
    def __init__(self, name, type_name):
        self.name = name
        self.dataType = FakeType(type_name)


class FakeSchema:
    def __init__(self, columns):
        self.fields = [FakeField(name, type_name) for name, type_name in columns.items()]


# --------------------------------------------------------------------------------------
# Recording JDBC read path.
#
# A JDBC read is entirely "hand the right options to spark.read". Recording the options
# map tests exactly the part that has to be right, and does it with no driver, no
# database and no JVM - which is the only way it can be tested at all here.
# --------------------------------------------------------------------------------------


class RecordingJdbcReader:
    """Stands in for spark.read, capturing format/options instead of connecting."""

    def __init__(self, spark):
        self._spark = spark
        self.format_used = None
        self.options_used = {}

    def format(self, source):
        self.format_used = source
        return self

    def options(self, **options):
        self.options_used.update(options)
        return self

    def option(self, key, value):
        self.options_used[key] = value
        return self

    def load(self):
        self._spark.reads.append(self)
        # Kept so a test can assert on the frame this read returned - specifically that it
        # was cached and released, which is what stops a JDBC extract running twice.
        self.load_result = self._spark.next_frame()
        return self.load_result


class LoadedFrame:
    """What a recorded read returns: rows and a schema, both supplied by the test.

    Records the projection and the cache lifecycle instead of performing either. Between
    them those are what an Oracle run does to a frame, and both matter: the projection is
    landing's provenance columns, and an extract that is not cached is read from the source
    TWICE (once to count, once to write).
    """

    def __init__(self, rows=(), schema=None):
        self.rows = [FakeRow(row) if isinstance(row, dict) else row for row in rows]
        self.schema = schema if schema is not None else FakeSchema({})
        self.projections = []
        self.persisted = 0
        self.unpersisted = 0

    @property
    def columns(self):
        return [field.name for field in self.schema.fields]

    def selectExpr(self, *expressions):  # noqa: N802 - mirrors the Spark API
        self.projections.append(list(expressions))
        return self

    def persist(self, _level=None):
        self.persisted += 1
        return self

    def unpersist(self):
        self.unpersisted += 1
        return self

    def collect(self):
        return list(self.rows)

    def count(self):
        return len(self.rows)


class FakeJdbcSpark(FakeSpark):
    """A FakeSpark whose `.read` records. `frames` are returned one per load(), in order.

    A list rather than one frame because an Oracle run reads more than once: a probe for
    the partition bounds, then the extract itself, and the interesting assertions are
    about how the two DIFFER.
    """

    def __init__(self, frames=None, frames_by_table=None, **kwargs):
        super().__init__(**kwargs)
        self.reads = []
        # `_load_frames`, not `_frames`: FakeSpark already uses that name for the frames it
        # hands back from `table()`, and one name for two things is how a fake starts lying.
        self._load_frames = list(frames or [])
        # What `spark.table(name).schema` returns - i.e. what the landing table already
        # holds, which is the other half of the schema-drift check.
        self._frames_by_table = dict(frames_by_table or {})

    @property
    def read(self):
        return RecordingJdbcReader(self)

    def next_frame(self):
        return self._load_frames.pop(0) if self._load_frames else LoadedFrame()

    def table(self, name):
        """An existing Delta table, for the schema-drift comparison.

        A table a test did not describe gets an EMPTY schema, which reads as "nothing to
        compare against" rather than as drift - so a test about the write path does not
        have to restate the schema it is not testing.
        """
        return self._frames_by_table.setdefault(name, LoadedFrame())

    def options_for(self, index):
        return self.reads[index].options_used
