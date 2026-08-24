"""Schema Registry lookup by id, over HTTP, on the driver. NO PySpark import.

Resolution is BY THE ID FOUND ON EACH RECORD, never a single "latest" pinned at query
start. That is what makes a mixed-version microbatch decode correctly, and what makes a
landing re-read months later decode exactly as it did originally.

Instances are DRIVER-ONLY and are never referenced inside a Spark closure. Schema JSON is
resolved here and passed to from_avro as a literal string, which is what keeps the decode
fully vectorised on the executors - see curated.py.

Deliberately not the vendor client library: this needs exactly two endpoints, and a plain
requests.Session keeps the runtime dependency surface at PyYAML + requests.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import RegistryProfile
from .security import RegistryAuth

LOG = logging.getLogger(__name__)


class SchemaResolutionError(RuntimeError):
    """The registry could not return a usable schema for a writer schema id."""


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

    def __init__(self, profile: RegistryProfile, auth: RegistryAuth):
        self.profile = profile
        self._auth = auth
        self._by_id: dict[int, str] = {}
        self._latest: dict[str, tuple] = {}  # subject -> (schema_id, schema_json)
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
                raise SchemaResolutionError(f"schema id {schema_id} is {schema_type}, but only AVRO is supported.")
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

    def _get(self, path: str) -> dict[str, Any]:
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
                f"registry '{self.profile.name}' returned HTTP {response.status_code} for {path}: {response.text[:500]}"
            )
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise SchemaResolutionError(
                f"registry '{self.profile.name}' returned non-JSON for {path}: {response.text[:200]}"
            ) from exc
