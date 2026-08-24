"""framework/security.py - the source-agnostic half of the security split.

Everything here would read the same for a database or a storage account: resolving a secret
by (scope, key) and never letting a credential-bearing value reach a log line. The
Kafka-shaped half - JAAS strings, truststores, registry auth - is
tests/test_kafka_security.py, because an options map is shaped by the system it connects to.
"""

from __future__ import annotations

import pytest

from kafka_ingest.framework.config import ConfigError
from kafka_ingest.framework.security import MASK, SecretResolver, redact


class _RecordingDbutils:
    """Stands in for dbutils. Counts calls, so the cache can be asserted rather than assumed."""

    def __init__(self, values=None, fail=False):
        self.values = values or {}
        self.calls = []
        self.fail = fail
        self.secrets = self

    def get(self, scope, key):
        self.calls.append((scope, key))
        if self.fail:
            raise RuntimeError("PERMISSION_DENIED: no READ on scope")
        return self.values.get((scope, key), f"{scope}/{key}/value")


def test_a_secret_is_resolved_by_scope_and_key():
    dbutils = _RecordingDbutils({("kv-prod", "api-key"): "AKIA-not-real"})
    assert SecretResolver(dbutils).get("kv-prod", "api-key") == "AKIA-not-real"
    assert dbutils.calls == [("kv-prod", "api-key")]


def test_the_same_secret_is_fetched_once_per_run():
    """Each dbutils call is a control-plane round trip, and one run resolves the same
    scope/key from more than one place."""
    dbutils = _RecordingDbutils()
    secrets = SecretResolver(dbutils)
    secrets.get("kv", "user")
    secrets.get("kv", "user")
    secrets.get("kv", "password")
    assert dbutils.calls == [("kv", "user"), ("kv", "password")]


@pytest.mark.parametrize("scope, key", [("", "k"), ("s", ""), (None, "k"), ("s", None)])
def test_an_incomplete_secret_reference_is_refused_before_it_reaches_dbutils(scope, key):
    dbutils = _RecordingDbutils()
    with pytest.raises(ConfigError, match="requires both scope and key"):
        SecretResolver(dbutils).get(scope, key)
    assert dbutils.calls == []


def test_a_failed_lookup_names_the_scope_and_key_but_never_the_value():
    """The most common cause is a missing privilege on the scope, and the message has to be
    enough to fix that without anyone pasting a secret into a ticket."""
    with pytest.raises(ConfigError) as exc:
        SecretResolver(_RecordingDbutils(fail=True)).get("kv-prod", "api-secret")
    message = str(exc.value)
    assert "api-secret" in message and "kv-prod" in message
    assert "READ" in message


def test_redaction_is_available_from_the_module_a_reader_looks_in():
    """One implementation, re-exported. Two lists of credential-bearing key names would be
    two lists to keep in step, and the one that fell behind would be the one leaking."""
    masked = redact({"kafka.sasl.jaas.config": "...password=hunter2...", "kafka.bootstrap.servers": "broker:9092"})
    assert masked["kafka.sasl.jaas.config"] == MASK
    assert masked["kafka.bootstrap.servers"] == "broker:9092"
