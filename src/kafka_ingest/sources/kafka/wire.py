"""The Confluent wire format, as Spark column expressions.

A Confluent-serialised Kafka value is:

    byte 0        magic byte, always 0x00
    bytes 1..4    writer schema id, big-endian signed int32
    bytes 5..n    Avro binary payload (no embedded schema)

Landing stores the WHOLE thing verbatim and additionally surfaces the parsed schema id as
a column, so landing alone is enough to reason about schema drift without re-parsing bytes.

TWO API TRAPS ARE ENCODED HERE, NOT COMMENTED ABOUT
---------------------------------------------------
`substring()` on BINARY is 1-indexed over BYTES, and `conv(hex(...), 16, 10)` returns an
UNSIGNED decimal string - so the id is cast through BIGINT before INT, or a schema id with
the high bit set overflows into a wrong number rather than an error.

WHY MALFORMED RECORDS GET A REASON, NOT JUST A NULL ID
-------------------------------------------------------
Three different inputs produce a NULL writer schema id: a NULL value (a tombstone), a
value shorter than the 5-byte header, and a value whose first byte is not 0x00. They are
three different operational problems - a compacted topic, a truncated producer, and a
producer that is not using this framing at all - and reporting all three as "malformed
wire format" sends the first responder to the wrong producing team. `malformed_reason_col`
separates them; NULL means the record is fine.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MAGIC_BYTE_HEX = "00"
HEADER_BYTES = 5

# The three ways a value can fail before a schema id is even meaningful. These strings
# reach the quarantine table's `quarantine_reason` column and appear in support queries,
# so they are constants rather than inline literals.
REASON_NULL_VALUE = "NULL_VALUE_TOMBSTONE"
REASON_TRUNCATED = "TRUNCATED_PAYLOAD"
REASON_BAD_MAGIC = "BAD_MAGIC_BYTE"

MALFORMED_REASONS = (REASON_NULL_VALUE, REASON_TRUNCATED, REASON_BAD_MAGIC)

# What each reason means, verbatim, in the quarantine row's `quarantine_detail`. Written
# for whoever opens the quarantine table months later with no context.
MALFORMED_DETAIL = {
    REASON_NULL_VALUE: (
        "the Kafka value was NULL. On a log-compacted topic this is a tombstone and is expected; "
        "on any other topic it means a producer published a key with no payload."
    ),
    REASON_TRUNCATED: (
        f"the Kafka value was shorter than the {HEADER_BYTES}-byte Confluent header, so it cannot "
        "carry a schema id at all. Usually a producer serialising without the registry serialiser."
    ),
    REASON_BAD_MAGIC: (
        "the Kafka value did not start with the 0x00 magic byte, so it is not Confluent-framed "
        "Avro. Plain Avro, JSON or Protobuf on this topic would all look like this."
    ),
}


def malformed_reason_col(value_col: str = "value") -> Column:
    """Why this record cannot be decoded, or NULL when it can be.

    Ordered from cheapest and most specific outward: a NULL value cannot be measured, and
    a value under five bytes has no byte 0 worth comparing. Each branch is only reached
    when the ones above it did not match, so `length()` and `substring()` never see input
    they cannot handle.

    The magic-byte branch compares HEX TEXT rather than bytes. Comparing a BINARY column
    against a Python bytes literal is the obvious form and its behaviour on BINARY is not
    something this project can verify without a cluster - VB-18 - whereas `hex(substring(
    value, 1, 1)) = '00'` is a string comparison whose semantics are not in question.
    """
    return (
        F.when(F.col(value_col).isNull(), F.lit(REASON_NULL_VALUE))
        .when(F.length(F.col(value_col)) < F.lit(HEADER_BYTES), F.lit(REASON_TRUNCATED))
        .when(F.hex(F.substring(F.col(value_col), 1, 1)) != F.lit(MAGIC_BYTE_HEX), F.lit(REASON_BAD_MAGIC))
        .otherwise(F.lit(None).cast("string"))
    )


def writer_schema_id_col(value_col: str = "value") -> Column:
    """Bytes 1..4 as a big-endian int32, or NULL when the record is not wire-format.

    NULL here is a SIGNAL, not a silent default: it is what routes a record to quarantine
    or fails the batch, depending on the source's failure_mode. `malformed_reason_col`
    above says which of the three NULL cases it was.
    """
    return F.when(
        F.col(value_col).isNotNull()
        & (F.length(F.col(value_col)) >= F.lit(HEADER_BYTES))
        & (F.hex(F.substring(F.col(value_col), 1, 1)) == F.lit(MAGIC_BYTE_HEX)),
        F.conv(F.hex(F.substring(F.col(value_col), 2, 4)), 16, 10).cast("bigint").cast("int"),
    ).otherwise(F.lit(None).cast("int"))


def avro_payload_col(value_col: str = "value") -> Column:
    """Strip the 5-byte header, leaving the raw Avro binary for from_avro().

    Derived at read time, never stored - landing keeps the full original bytes so a future
    change to this parsing logic can be re-applied to historical data. A header-only record
    yields an empty binary, which from_avro reports as corrupt: the correct outcome, since
    an empty payload cannot satisfy any schema.
    """
    return F.expr(f"substring({value_col}, {HEADER_BYTES + 1}, length({value_col}) - {HEADER_BYTES})")


def add_wire_format_columns(df: DataFrame, value_col: str = "value") -> DataFrame:
    """Attach the parsed header columns that landing stores and curated reads."""
    return (
        df.withColumn("writer_schema_id", writer_schema_id_col(value_col))
        .withColumn("malformed_reason", malformed_reason_col(value_col))
        # An explicit boolean so a landing consumer can count malformed arrivals without
        # re-deriving the rule from the two columns above it.
        .withColumn("wire_format_valid", F.col("writer_schema_id").isNotNull())
        .withColumn("payload_bytes", F.length(F.col(value_col)))
    )


def distinct_schema_ids(batch_df: DataFrame) -> list:
    """The distinct writer schema ids in one microbatch.

    A driver-side collect of METADATA CARDINALITY, not of data: a batch normally holds 1-3
    distinct ids and is bounded by the number of schema versions the topic has ever had.
    NULL is included when malformed records are present, so the caller handles that group
    explicitly rather than by its absence.
    """
    rows = batch_df.select("writer_schema_id").distinct().collect()
    return [row["writer_schema_id"] for row in rows]
