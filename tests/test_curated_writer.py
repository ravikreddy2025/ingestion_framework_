"""End-to-end: Kafka rows -> landing projection -> curated + quarantine.

These are the tests for the framework's central claim: a single microbatch containing
records written under DIFFERENT schema versions parses correctly, because each record is
resolved by the writer_schema_id in its own wire header.

They also pin the two shape decisions that the developer team is most likely to want to
change later, so a change is a deliberate test edit rather than a silent regression:
  * the payload stays NESTED in one struct column
  * CloudEvent attributes are promoted from Kafka headers to typed columns

Avro payloads are hand-encoded rather than pulled from a library. The encoding is small and
stable (zigzag varint ints, length-prefixed strings) and hand-encoding keeps the test honest
about the exact bytes, which is the thing under test.
"""

from __future__ import annotations

import json
import struct
from dataclasses import replace
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="needs pyspark")

from conftest import FakeSpark
from kafka_ingest.config import resolve_topic_config
from kafka_ingest.schema_resolver import SchemaResolutionError

# --------------------------------------------------------------------------------------
# Minimal Avro binary encoder
# --------------------------------------------------------------------------------------


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        if value:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def avro_long(value: int) -> bytes:
    """Avro int/long are zigzag-encoded varints."""
    return _varint((value << 1) ^ (value >> 63))


def avro_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return avro_long(len(encoded)) + encoded


def wire(schema_id: int, payload: bytes) -> bytes:
    """Confluent framing: 0x00 magic + big-endian int32 schema id + Avro payload."""
    return b"\x00" + struct.pack(">i", schema_id) + payload


# --------------------------------------------------------------------------------------
# Two registered schema versions of the same subject
# --------------------------------------------------------------------------------------

SCHEMA_V1_ID = 101
SCHEMA_V2_ID = 202

SCHEMA_V1 = json.dumps({
    "type": "record", "name": "Demo", "namespace": "kafka_ingest.test",
    "fields": [{"name": "event_id", "type": "long"}],
})

# v2 adds a field WITH a default - the FORWARD_TRANSITIVE-compatible change that makes a
# mixed-version microbatch legal in the first place.
SCHEMA_V2 = json.dumps({
    "type": "record", "name": "Demo", "namespace": "kafka_ingest.test",
    "fields": [
        {"name": "event_id", "type": "long"},
        {"name": "source_system", "type": "string", "default": "unknown"},
    ],
})

V1_RECORD = avro_long(7)                                # {event_id: 7}
V2_RECORD = avro_long(8) + avro_string("rcm-gateway")   # {event_id: 8, source_system: ...}


class FakeRegistryClient:
    """Stands in for SchemaRegistryClient. Records what was asked for."""

    def __init__(self, schemas=None):
        self.schemas = schemas or {SCHEMA_V1_ID: SCHEMA_V1, SCHEMA_V2_ID: SCHEMA_V2}
        self.requested = []

    def get_schema_by_id(self, schema_id):
        self.requested.append(schema_id)
        if schema_id not in self.schemas:
            raise SchemaResolutionError(f"no schema registered under id {schema_id}")
        return self.schemas[schema_id]

    def get_latest(self, subject):
        return SCHEMA_V2_ID, SCHEMA_V2


# --------------------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("kafka-ingest-curated-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no local Spark available ({type(exc).__name__}: {exc})")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _needs_spark_avro(spark):
    try:
        spark._jvm.java.lang.Class.forName("org.apache.spark.sql.avro.AvroDataToCatalyst")
    except Exception:  # noqa: BLE001
        pytest.skip("spark-avro connector not available in this local Spark install")


KAFKA_SCHEMA = (
    "topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, timestampType INT, "
    "key BINARY, value BINARY, headers ARRAY<STRUCT<key: STRING, value: BINARY>>"
)


def ce_headers(**attributes):
    """Build Kafka headers the way a CloudEvents binary-mode producer would."""
    return [(k, v.encode("utf-8")) for k, v in attributes.items()]


def kafka_rows(spark, records):
    """records: list of (value_bytes, headers) tuples."""
    rows = [
        ("demo.events.v1", 0, 1000 + i, datetime(2026, 8, 11, 9, i % 60), 0, b"k", value, headers)
        for i, (value, headers) in enumerate(records)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def make_cfg(config_root, **overrides):
    """Resolve the demo topic, applying operational-tier overrides (on_deser_error etc.)."""
    return resolve_topic_config(FakeSpark(), config_root, "demo_topic", "ops.ingestion.control", "prod",
                                overrides=overrides)


def with_structural(cfg, **fields):
    """Set STRUCTURAL fields directly.

    Fields like curated_dedup_keys and the partition lists are deliberately NOT in the
    operational override set - changing them is a PR, not a runtime toggle - so a test that
    needs them has to set them on the resolved config rather than pass them as overrides.
    """
    return replace(cfg, **fields)


def run_parse(spark, cfg, records, client=None, reader=(SCHEMA_V2_ID, SCHEMA_V2)):
    from kafka_ingest.curated_writer import parse_batch
    from kafka_ingest.landing_writer import project_landing

    landing = project_landing(kafka_rows(spark, records), cfg, batch_id=0, run_id="run-1")
    result = parse_batch(spark, landing, cfg, client or FakeRegistryClient(), 0, "run-1",
                         reader_schema=reader)
    return landing, result


# --------------------------------------------------------------------------------------
# The central test
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_one_microbatch_with_two_writer_schema_versions_parses_correctly(spark, config_root):
    """A batch holding v1 and v2 records must parse BOTH, each with its own writer schema.

    The v1 record has no source_system field on the wire at all; Avro resolution against the
    v2 reader schema fills it from the declared default. A framework that resolved one
    'latest' schema for the whole stream would either fail on the v1 bytes or mis-read them.
    """
    cfg = make_cfg(config_root)
    _, result = run_parse(spark, cfg, [
        (wire(SCHEMA_V1_ID, V1_RECORD), []),
        (wire(SCHEMA_V2_ID, V2_RECORD), []),
    ])

    assert result.quarantine_df is None
    assert sorted(result.writer_schema_ids) == [SCHEMA_V1_ID, SCHEMA_V2_ID]

    rows = {r["payload"]["event_id"]: r for r in result.curated_df.collect()}
    assert set(rows) == {7, 8}
    # v1 record: field absent from the bytes, filled from the reader schema's default.
    assert rows[7]["payload"]["source_system"] == "unknown"
    assert rows[7]["writer_schema_id"] == SCHEMA_V1_ID
    # v2 record: value read from the wire.
    assert rows[8]["payload"]["source_system"] == "rcm-gateway"
    assert rows[8]["writer_schema_id"] == SCHEMA_V2_ID
    # Both share one payload struct type, pinned by the reader schema.
    assert rows[7]["reader_schema_id"] == rows[8]["reader_schema_id"] == SCHEMA_V2_ID


@pytest.mark.spark
def test_each_writer_schema_is_fetched_once_per_batch(spark, config_root):
    """Registry lookups are per distinct schema id, not per record."""
    client = FakeRegistryClient()
    run_parse(spark, make_cfg(config_root),
              [(wire(SCHEMA_V1_ID, V1_RECORD), [])] * 5 + [(wire(SCHEMA_V2_ID, V2_RECORD), [])] * 5,
              client=client)
    assert sorted(client.requested) == [SCHEMA_V1_ID, SCHEMA_V2_ID]


# --------------------------------------------------------------------------------------
# Shape decisions
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_payload_stays_nested_and_is_not_exploded(spark, config_root):
    """Curated is 1:1 with Kafka records and keeps the payload in one struct column.

    Exploding would break the (topic, kafka_partition, kafka_offset) merge key that makes
    replay idempotent. Fan-out belongs downstream, not here.
    """
    cfg = make_cfg(config_root)
    _, result = run_parse(spark, cfg, [(wire(SCHEMA_V2_ID, V2_RECORD), [])])
    row = result.curated_df.collect()[0]

    assert result.curated_df.count() == 1
    assert dict(row["payload"].asDict()) == {"event_id": 8, "source_system": "rcm-gateway"}
    # Business fields are NOT top-level; framework columns are, and are plainly named.
    assert "event_id" not in result.curated_df.columns
    assert {"topic", "kafka_offset", "ingest_ts"} <= set(result.curated_df.columns)


@pytest.mark.spark
def test_curated_column_order_matches_the_ddl(spark, config_root):
    """Projection and the documented DDL must agree - a test, not a comment."""
    from kafka_ingest.tables import CURATED_FIXED_COLUMNS
    from test_audit_and_tables import ddl_column_names

    _, result = run_parse(spark, make_cfg(config_root), [(wire(SCHEMA_V2_ID, V2_RECORD), [])])
    assert result.curated_df.columns == ddl_column_names(CURATED_FIXED_COLUMNS) + ["payload"]


# --------------------------------------------------------------------------------------
# CloudEvents
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_cloudevent_attributes_are_promoted_from_kafka_headers(spark, config_root):
    cfg = make_cfg(config_root)
    headers = ce_headers(
        ce_id="evt-123", ce_source="/rcm/gateway", ce_type="com.acme.claim.updated",
        ce_subject="claim/987", ce_time="2026-08-11T09:30:00Z", ce_specversion="1.0",
    ) + [("content-type", b"application/avro")]

    landing, result = run_parse(spark, cfg, [(wire(SCHEMA_V2_ID, V2_RECORD), headers)])

    for frame in (landing, result.curated_df):
        row = frame.collect()[0]
        assert row["ce_id"] == "evt-123"
        assert row["ce_source"] == "/rcm/gateway"
        assert row["ce_type"] == "com.acme.claim.updated"
        assert row["ce_subject"] == "claim/987"
        assert row["ce_specversion"] == "1.0"
        # The Kafka binding maps datacontenttype onto the standard content-type header.
        assert row["ce_datacontenttype"] == "application/avro"
        # Stored verbatim, not parsed - RFC3339 UTC sorts correctly as a string.
        assert row["ce_time"] == "2026-08-11T09:30:00Z"


@pytest.mark.spark
def test_header_matching_is_case_insensitive(spark, config_root):
    """Brokers and client libraries disagree about header casing."""
    _, result = run_parse(spark, make_cfg(config_root),
                          [(wire(SCHEMA_V2_ID, V2_RECORD), ce_headers(CE_ID="evt-9"))])
    assert result.curated_df.collect()[0]["ce_id"] == "evt-9"


@pytest.mark.spark
def test_topic_without_cloudevents_gets_nulls_not_an_error(spark, config_root):
    """A NULL ce_id is the signal that a producer is not emitting CloudEvents."""
    _, result = run_parse(spark, make_cfg(config_root), [(wire(SCHEMA_V2_ID, V2_RECORD), [])])
    row = result.curated_df.collect()[0]
    assert row["ce_id"] is None and row["ce_type"] is None


@pytest.mark.spark
def test_event_date_prefers_ce_time_and_falls_back_to_kafka_timestamp(spark, config_root):
    """event_date is the curated partition key, so it must never be NULL and never throw."""
    cfg = make_cfg(config_root)
    _, result = run_parse(spark, cfg, [
        (wire(SCHEMA_V2_ID, V2_RECORD), ce_headers(ce_time="2026-07-04T23:00:00Z")),
        (wire(SCHEMA_V2_ID, V2_RECORD), []),                                  # no ce_time
        (wire(SCHEMA_V2_ID, V2_RECORD), ce_headers(ce_time="not-a-timestamp")),  # malformed
    ])
    by_offset = {r["kafka_offset"]: r["event_date"] for r in result.curated_df.collect()}
    assert str(by_offset[1000]) == "2026-07-04"          # from ce_time
    assert str(by_offset[1001]) == "2026-08-11"          # fell back to kafka_timestamp
    assert str(by_offset[1002]) == "2026-08-11"          # malformed ce_time, no exception


# --------------------------------------------------------------------------------------
# Landing
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_landing_keeps_the_original_bytes_verbatim(spark, config_root):
    """Landing must be byte-identical to the wire, header included - that is what makes a
    curated replay possible after Kafka retention has expired."""
    original = wire(SCHEMA_V2_ID, V2_RECORD)
    landing, _ = run_parse(spark, make_cfg(config_root), [(original, [])])
    row = landing.collect()[0]
    assert bytes(row["value"]) == original
    assert row["writer_schema_id"] == SCHEMA_V2_ID
    assert row["wire_format_valid"] is True
    assert row["payload_bytes"] == len(original)
    assert row["kafka_key_string"] == "k"


@pytest.mark.spark
def test_landing_column_order_matches_the_ddl(spark, config_root):
    from kafka_ingest.tables import LANDING_DDL_COLUMNS
    from test_audit_and_tables import ddl_column_names

    landing, _ = run_parse(spark, make_cfg(config_root), [(wire(SCHEMA_V2_ID, V2_RECORD), [])])
    assert landing.columns == ddl_column_names(LANDING_DDL_COLUMNS)


# --------------------------------------------------------------------------------------
# Failure handling
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_failfast_raises_on_a_malformed_record_and_names_the_offsets(spark, config_root):
    cfg = make_cfg(config_root, on_deser_error="fail")
    with pytest.raises(SchemaResolutionError, match="wire-format header"):
        run_parse(spark, cfg, [(wire(SCHEMA_V2_ID, V2_RECORD), []), (b"\x99not-confluent", [])])


@pytest.mark.spark
def test_quarantine_splits_bad_records_and_keeps_the_good_ones(spark, config_root):
    cfg = make_cfg(config_root, on_deser_error="quarantine")
    _, result = run_parse(spark, cfg, [
        (wire(SCHEMA_V2_ID, V2_RECORD), []),
        (b"\x99not-confluent", []),
        (wire(SCHEMA_V1_ID, V1_RECORD), []),
    ])
    assert result.curated_df.count() == 2
    quarantined = result.quarantine_df.collect()
    assert len(quarantined) == 1
    assert quarantined[0]["quarantine_reason"] == "malformed_wire_format"
    # Raw bytes retained, so the record is recoverable once the cause is fixed.
    assert bytes(quarantined[0]["value"]) == b"\x99not-confluent"
    assert quarantined[0]["writer_schema_id"] is None


@pytest.mark.spark
def test_unregistered_schema_id_quarantines_that_group_only(spark, config_root):
    """The registry cannot say how those bytes were written, but the rest of the batch is
    perfectly parseable and must not be held hostage."""
    cfg = make_cfg(config_root, on_deser_error="quarantine")
    client = FakeRegistryClient(schemas={SCHEMA_V2_ID: SCHEMA_V2})  # v1 not registered
    _, result = run_parse(spark, cfg, [
        (wire(SCHEMA_V1_ID, V1_RECORD), []),
        (wire(SCHEMA_V2_ID, V2_RECORD), []),
    ], client=client)

    assert result.curated_df.count() == 1
    assert result.writer_schema_ids == [SCHEMA_V2_ID]
    assert SCHEMA_V1_ID in result.unresolvable_schema_ids
    assert result.quarantine_df.collect()[0]["quarantine_reason"] == "schema_resolution_failed"


@pytest.mark.spark
def test_unregistered_schema_id_fails_the_batch_under_failfast(spark, config_root):
    cfg = make_cfg(config_root, on_deser_error="fail")
    client = FakeRegistryClient(schemas={SCHEMA_V2_ID: SCHEMA_V2})
    with pytest.raises(SchemaResolutionError, match="no schema registered under id"):
        run_parse(spark, cfg, [(wire(SCHEMA_V1_ID, V1_RECORD), [])], client=client)


@pytest.mark.spark
def test_empty_batch_produces_neither_frame(spark, config_root):
    """Trigger.AvailableNow ends every run with an empty batch."""
    _, result = run_parse(spark, make_cfg(config_root), [])
    assert result.curated_df is None and result.quarantine_df is None
    assert result.writer_schema_ids == []


@pytest.mark.spark
def test_tombstone_record_is_treated_as_malformed_not_parsed(spark, config_root):
    """A NULL value has no schema id and cannot be parsed; it must not become a null row."""
    cfg = make_cfg(config_root, on_deser_error="quarantine")
    _, result = run_parse(spark, cfg, [(None, []), (wire(SCHEMA_V2_ID, V2_RECORD), [])])
    assert result.curated_df.count() == 1
    assert result.quarantine_df.count() == 1


@pytest.mark.spark
def test_dedup_key_must_reference_the_payload_struct(spark, config_root):
    """A bare business-field name is a common mistake now that payload is nested - the error
    has to say so plainly."""
    cfg = with_structural(make_cfg(config_root), curated_dedup_keys=["event_id"])
    with pytest.raises(ValueError, match="payload.<field>"):
        run_parse(spark, cfg, [(wire(SCHEMA_V2_ID, V2_RECORD), [])])


@pytest.mark.spark
def test_dedup_keeps_the_newest_record_per_business_key(spark, config_root):
    cfg = with_structural(make_cfg(config_root), curated_dedup_keys=["payload.event_id"])
    _, result = run_parse(spark, cfg, [
        (wire(SCHEMA_V2_ID, V2_RECORD), []),
        (wire(SCHEMA_V2_ID, V2_RECORD), []),
    ])
    rows = result.curated_df.collect()
    assert len(rows) == 1
    assert rows[0]["kafka_offset"] == 1001   # highest offset wins the tie


# --------------------------------------------------------------------------------------
# Curated table schema, derived before any data is read
# --------------------------------------------------------------------------------------


@pytest.mark.spark
def test_curated_schema_matches_what_the_writer_produces(spark, config_root):
    """Derived from an EMPTY frame, so onboarding needs no manual DDL - and the table cannot
    disagree with the projection, because it IS the projection."""
    from kafka_ingest.curated_writer import curated_schema
    from kafka_ingest.tables import CURATED_FIXED_COLUMNS
    from test_audit_and_tables import ddl_column_names

    cfg = make_cfg(config_root)
    schema = curated_schema(spark, cfg, (SCHEMA_V2_ID, SCHEMA_V2))

    assert [f.name for f in schema.fields] == ddl_column_names(CURATED_FIXED_COLUMNS) + ["payload"]

    # And it agrees with a real parsed batch, column for column.
    _, result = run_parse(spark, cfg, [(wire(SCHEMA_V2_ID, V2_RECORD), [])])
    assert [f.name for f in schema.fields] == result.curated_df.columns


@pytest.mark.spark
def test_curated_schema_keeps_the_payload_nested(spark, config_root):
    from kafka_ingest.curated_writer import curated_schema

    schema = curated_schema(spark, make_cfg(config_root), (SCHEMA_V2_ID, SCHEMA_V2))
    payload = schema["payload"].dataType
    assert [f.name for f in payload.fields] == ["event_id", "source_system"]


@pytest.mark.spark
def test_curated_schema_makes_every_field_nullable(spark, config_root):
    """Avro declares non-optional fields and from_avro reports them NOT NULL. Baking that
    into the table would turn one bad record into a failed batch instead of a quarantined
    row - the Avro decode already enforces the contract at parse time."""
    from kafka_ingest.curated_writer import curated_schema

    schema = curated_schema(spark, make_cfg(config_root), (SCHEMA_V2_ID, SCHEMA_V2))
    assert all(f.nullable for f in schema.fields)
    assert all(f.nullable for f in schema["payload"].dataType.fields)
    assert "NOT NULL" not in schema.toDDL()


@pytest.mark.spark
def test_curated_schema_needs_no_data(spark, config_root):
    """It must work on a brand new topic that has never received a message."""
    from kafka_ingest.curated_writer import curated_schema

    schema = curated_schema(spark, make_cfg(config_root), (SCHEMA_V1_ID, SCHEMA_V1))
    assert schema["payload"].dataType.fields, "payload struct should still be derived"
    assert [f.name for f in schema["payload"].dataType.fields] == ["event_id"]
