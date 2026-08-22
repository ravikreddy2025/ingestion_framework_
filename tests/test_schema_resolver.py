"""Schema Registry lookup behaviour and Confluent wire-format parsing.

The registry tests run without Spark data but need the module to import, so pyspark must
be importable. The wire-format tests need a real SparkSession and are marked `spark`;
run them with `pytest -m spark` on a machine that has one.
"""

from __future__ import annotations

import json
import struct

import pytest

pytest.importorskip("pyspark", reason="schema_resolver imports pyspark.sql")

from kafka_ingest.config import SchemaRegistryProfile
from kafka_ingest.schema_resolver import (
    SchemaRegistryClient,
    SchemaResolutionError,
)
from kafka_ingest.security import RegistryAuth

PROFILE = SchemaRegistryProfile(name="sr", url="https://sr.example.com", max_retries=0)

RECORD_SCHEMA = json.dumps(
    {"type": "record", "name": "Demo", "fields": [{"name": "id", "type": "int"}]}
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or json.dumps(payload or {})

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._payload is None:
            raise json.JSONDecodeError("no json", "", 0)
        return self._payload


def make_client(responses_by_path):
    client = SchemaRegistryClient(PROFILE, RegistryAuth())
    calls = []

    def fake_get(url, **_kwargs):
        path = url.replace(PROFILE.url, "")
        calls.append(path)
        response = responses_by_path.get(path)
        if response is None:
            return FakeResponse(status_code=404, payload={}, text="not found")
        return response

    client._session.get = fake_get  # deliberate seam for testing
    client.calls = calls
    return client


def test_schema_is_fetched_by_id():
    client = make_client({"/schemas/ids/4711": FakeResponse(payload={"schema": RECORD_SCHEMA})})
    assert json.loads(client.get_schema_by_id(4711))["name"] == "Demo"


def test_schema_lookups_are_cached_for_the_run():
    """Schema ids are immutable, so one HTTP call per id per run is correct and safe."""
    client = make_client({"/schemas/ids/4711": FakeResponse(payload={"schema": RECORD_SCHEMA})})
    client.get_schema_by_id(4711)
    client.get_schema_by_id(4711)
    client.get_schema_by_id(4711)
    assert client.calls == ["/schemas/ids/4711"]


def test_unknown_schema_id_explains_the_likely_cause():
    client = make_client({})
    with pytest.raises(SchemaResolutionError, match="different registry instance"):
        client.get_schema_by_id(9999)


def test_auth_failure_points_at_the_secret_config():
    client = make_client({"/schemas/ids/1": FakeResponse(status_code=403, payload={})})
    with pytest.raises(SchemaResolutionError, match="registries.yaml"):
        client.get_schema_by_id(1)


def test_non_avro_schema_type_is_refused_not_mis_decoded():
    client = make_client(
        {"/schemas/ids/1": FakeResponse(payload={"schema": "{}", "schemaType": "PROTOBUF"})}
    )
    with pytest.raises(SchemaResolutionError, match="only AVRO is supported"):
        client.get_schema_by_id(1)


def test_latest_subject_version_returns_id_and_schema():
    client = make_client(
        {"/subjects/demo-value/versions/latest": FakeResponse(payload={"id": 88, "schema": RECORD_SCHEMA})}
    )
    schema_id, schema_json = client.get_latest("demo-value")
    assert schema_id == 88
    assert json.loads(schema_json)["name"] == "Demo"


def test_connection_failure_mentions_private_connectivity():
    import requests

    client = SchemaRegistryClient(PROFILE, RegistryAuth())

    def boom(*_args, **_kwargs):
        raise requests.ConnectionError("no route to host")

    client._session.get = boom
    with pytest.raises(SchemaResolutionError, match="NCC private endpoint"):
        client.get_schema_by_id(1)


# --------------------------------------------------------------------------------------
# Wire format - needs a SparkSession
# --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    """A local SparkSession, or a clean skip.

    Needs a JVM on PATH.

    Deliberately does NOT set spark.jars.packages: Ivy resolution shells out through
    Hadoop's Shell class, which on Windows requires winutils.exe and fails the whole
    SparkContext. `from_avro` needs the spark-avro connector, which the PySpark pip
    package does not bundle (it ships the Avro Java library only) - drop that jar into
    pyspark/jars/ instead, and requires_spark_avro() skips cleanly when it is absent.
    """
    pyspark_sql = pytest.importorskip("pyspark.sql")

    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("kafka-ingest-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - no JVM, or package resolution failed
        pytest.skip(f"no local Spark available ({type(exc).__name__}: {exc})")
    yield session
    session.stop()


def requires_spark_avro(spark):
    """Skip when the spark-avro connector is absent - its absence is an environment fact,
    not a defect in the code under test."""
    try:
        spark._jvm.java.lang.Class.forName(
            "org.apache.spark.sql.avro.AvroDataToCatalyst"
        )
    except Exception:  # noqa: BLE001
        pytest.skip("spark-avro connector not available in this local Spark install")


def confluent_bytes(schema_id: int, payload: bytes = b"\x02") -> bytes:
    """magic byte 0x00 + big-endian int32 schema id + Avro payload."""
    return b"\x00" + struct.pack(">i", schema_id) + payload


@pytest.mark.spark
def test_writer_schema_id_is_parsed_from_the_header(spark):
    from kafka_ingest.schema_resolver import add_wire_format_columns

    rows = [
        (confluent_bytes(1),),        # smallest realistic id
        (confluent_bytes(4711),),
        (confluent_bytes(2147483647),),  # max int32 - guards the conv() overflow path
    ]
    df = spark.createDataFrame(rows, "value BINARY")
    parsed = [r["writer_schema_id"] for r in add_wire_format_columns(df).collect()]
    assert parsed == [1, 4711, 2147483647]


@pytest.mark.spark
def test_malformed_records_yield_null_schema_id_not_a_wrong_one(spark):
    from kafka_ingest.schema_resolver import add_wire_format_columns

    rows = [
        (None,),                       # tombstone / key-only record
        (b"\x00\x01\x02",),            # shorter than the 5-byte header
        (b"\x01\x00\x00\x12\x67x",),   # wrong magic byte - plain Avro or another framing
        (b"",),                        # empty value
    ]
    df = spark.createDataFrame(rows, "value BINARY")
    result = add_wire_format_columns(df).collect()
    assert all(r["writer_schema_id"] is None for r in result)
    assert all(r["wire_format_valid"] is False for r in result)


@pytest.mark.spark
def test_payload_strips_exactly_the_five_header_bytes(spark):
    from kafka_ingest.schema_resolver import avro_payload_col

    df = spark.createDataFrame([(confluent_bytes(4711, b"\x02\x04\x06"),)], "value BINARY")
    payload = df.select(avro_payload_col("value").alias("p")).collect()[0]["p"]
    assert bytes(payload) == b"\x02\x04\x06"


@pytest.mark.spark
def test_header_only_record_yields_an_empty_payload(spark):
    """No Avro schema can be satisfied by zero bytes - from_avro reports it as corrupt."""
    from kafka_ingest.schema_resolver import avro_payload_col

    df = spark.createDataFrame([(confluent_bytes(4711, b""),)], "value BINARY")
    payload = df.select(avro_payload_col("value").alias("p")).collect()[0]["p"]
    assert bytes(payload) == b""


@pytest.mark.spark
def test_from_avro_reader_writer_semantics_hold_on_this_runtime(spark, reset_from_avro_selfcheck):
    """Guards the assumption that fixes the curated payload struct. See curated_writer.

    The fixture clears the once-per-process latch first - without it, any earlier test that
    parsed a batch would have consumed the check and this would assert nothing.
    """
    requires_spark_avro(spark)
    from kafka_ingest.curated_writer import assert_from_avro_semantics

    assert_from_avro_semantics(spark)
