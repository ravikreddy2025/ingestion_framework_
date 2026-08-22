"""Confluent wire-format handling and Schema Registry resolution.

Two closely-coupled concerns live here:

1. Wire format decomposition (Spark column expressions).
   A Confluent-serialised Kafka value is:
       byte 0        magic byte, always 0x00
       bytes 1..4    writer schema id, big-endian signed int32
       bytes 5..n    Avro binary payload (no embedded schema)
   Landing stores the WHOLE thing verbatim and additionally surfaces the parsed
   schema id as a column, so landing alone is enough to reason about schema drift
   without re-parsing bytes.

2. Registry lookup by schema id (driver-side HTTP, cached).
   Resolution is by the id found on each record, never a single "latest" pinned at
   query start. That is what makes a mixed-version microbatch decode correctly and
   what makes a landing re-read months later decode exactly as it did originally.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import SchemaRegistryProfile
from .security import RegistryAuth

LOG = logging.getLogger(__name__)

MAGIC_BYTE_HEX = "00"
HEADER_BYTES = 5


class SchemaResolutionError(RuntimeError):
    """Registry could not return a usable schema for a writer schema id."""


# --------------------------------------------------------------------------------------
# Wire-format column expressions
# --------------------------------------------------------------------------------------


def writer_schema_id_col(value_col: str = "value") -> Column:
    """Parse bytes 1..4 as a big-endian int32, or NULL if the record isn't wire-format.

    Edge cases folded into the one expression:
      * value IS NULL           -> NULL (a tombstone / key-only record)
      * length(value) < 5       -> NULL (truncated or non-Confluent producer)
      * byte 0 != 0x00          -> NULL (plain Avro, JSON, or a different framing)
    NULL here is a signal, not a silent default: the deserializer routes NULL-id rows to
    quarantine or fails the batch depending on the topic's on_deser_error setting.

    substring() on BINARY is 1-indexed over bytes; conv(hex(...), 16, 10) reassembles the
    big-endian id. It is cast through BIGINT first because conv returns an unsigned
    decimal string - a schema id with the high bit set would overflow a direct INT cast.
    """
    return (
        F.when(
            F.col(value_col).isNotNull()
            & (F.length(F.col(value_col)) >= F.lit(HEADER_BYTES))
            & (F.hex(F.substring(F.col(value_col), 1, 1)) == F.lit(MAGIC_BYTE_HEX)),
            F.conv(F.hex(F.substring(F.col(value_col), 2, 4)), 16, 10).cast("bigint").cast("int"),
        ).otherwise(F.lit(None).cast("int"))
    )


def avro_payload_col(value_col: str = "value") -> Column:
    """Strip the 5-byte header, leaving the raw Avro binary for from_avro().

    Derived at read time, never stored - landing keeps the full original bytes so that a
    future change to this parsing logic can be re-applied to historical data.
    A zero-length payload (header only) yields an empty binary, which from_avro reports
    as a corrupt record - the correct outcome, since it cannot satisfy any schema.
    """
    return F.expr(f"substring({value_col}, {HEADER_BYTES + 1}, length({value_col}) - {HEADER_BYTES})")


def add_wire_format_columns(df: DataFrame, value_col: str = "value") -> DataFrame:
    """Attach the parsed header columns used by both landing_writer and curated_writer."""
    return (
        df.withColumn("writer_schema_id", writer_schema_id_col(value_col))
        .withColumn(
            "wire_format_valid",
            # Explicit boolean so a landing consumer can count malformed arrivals without
            # re-deriving the rule.
            F.col("writer_schema_id").isNotNull(),
        )
        .withColumn("payload_bytes", F.length(F.col(value_col)))
    )


def distinct_schema_ids(batch_df: DataFrame) -> List[Optional[int]]:
    """Collect the distinct writer schema ids present in one microbatch.

    This is a driver-side collect of *metadata cardinality*, not of data: a batch
    normally contains 1-3 distinct ids, and is bounded by the number of schema versions
    a topic has ever had. NULL is included in the result when malformed records exist so
    the caller can handle that group explicitly.
    """
    rows = batch_df.select("writer_schema_id").distinct().collect()
    return [row["writer_schema_id"] for row in rows]


# --------------------------------------------------------------------------------------
# Registry client
# --------------------------------------------------------------------------------------


class SchemaRegistryClient:
    """Minimal Confluent Schema Registry REST client.

    Deliberately not the `confluent-kafka` client: this needs exactly two endpoints, and
    a plain requests.Session keeps the dependency surface (and the DBR install time) small.

    Instances are driver-only. They are never referenced inside a Spark UDF or closure -
    schema JSON is resolved on the driver, then passed to from_avro as a literal string,
    which is what keeps the decode fully vectorised on the executors.
    """

    def __init__(self, profile: SchemaRegistryProfile, auth: RegistryAuth):
        self.profile = profile
        self._auth = auth
        self._by_id: Dict[int, str] = {}
        self._latest: Dict[str, tuple] = {}   # subject -> (schema_id, schema_json)
        self._session = requests.Session()
        retry = Retry(
            total=profile.max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))

    # -- public --------------------------------------------------------------------

    def get_schema_by_id(self, schema_id: int) -> str:
        """Return the Avro schema JSON string registered under `schema_id`.

        Cached for the life of the run. Schema ids are immutable in Confluent Schema
        Registry, so an unbounded per-run cache is safe and there is no invalidation
        story to get wrong.
        """
        if schema_id not in self._by_id:
            body = self._get(f"/schemas/ids/{schema_id}")
            schema = body.get("schema")
            if not schema:
                raise SchemaResolutionError(
                    f"registry '{self.profile.name}' returned no 'schema' field for id {schema_id}"
                )
            schema_type = body.get("schemaType", "AVRO")
            if schema_type != "AVRO":
                # Protobuf/JSON-Schema subjects would need a different decoder. Out of
                # scope today; fail clearly rather than mis-decoding.
                raise SchemaResolutionError(
                    f"schema id {schema_id} is {schema_type}, but only AVRO is supported."
                )
            self._by_id[schema_id] = schema
            LOG.info("Resolved writer schema id %s from registry '%s'", schema_id, self.profile.name)
        return self._by_id[schema_id]

    def get_latest(self, subject: str) -> tuple:
        """Return (schema_id, schema_json) for the subject's latest version.

        Used only to pick the *reader* schema. Resolved once per run, not per record -
        a mid-run registration would otherwise change the curated payload struct halfway
        through a batch sequence.
        """
        if subject not in self._latest:
            body = self._get(f"/subjects/{subject}/versions/latest")
            self._latest[subject] = (int(body["id"]), body["schema"])
            LOG.info("Reader schema for subject '%s' pinned to id %s", subject, body["id"])
        return self._latest[subject]

    # -- internal ------------------------------------------------------------------

    def _get(self, path: str) -> Dict[str, Any]:
        url = f"{self.profile.url.rstrip('/')}{path}"
        try:
            response = self._session.get(
                url,
                auth=self._auth.auth,
                cert=self._auth.cert,
                verify=self._auth.verify,
                timeout=self.profile.timeout_seconds,
                headers={"Accept": "application/vnd.schemaregistry.v1+json, application/json"},
            )
        except requests.RequestException as exc:
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' unreachable at {url}. On serverless compute this "
                f"is usually a missing NCC private endpoint / firewall rule. Cause: {exc}"
            ) from exc

        if response.status_code == 404:
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' has no entry for {path} (HTTP 404). "
                "For a schema id this means the record was produced against a different "
                "registry instance than the one this topic is configured to use."
            )
        if response.status_code in (401, 403):
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' rejected credentials for {path} "
                f"(HTTP {response.status_code}). Check the secret scope/keys in registries.yaml."
            )
        if not response.ok:
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' returned HTTP {response.status_code} for {path}: "
                f"{response.text[:500]}"
            )
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' returned non-JSON for {path}: {response.text[:200]}"
            ) from exc
