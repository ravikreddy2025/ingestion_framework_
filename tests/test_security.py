"""Auth option construction, secret indirection and redaction."""

from __future__ import annotations

import pytest

from kafka_ingest.config import ConfigError, KafkaClusterProfile, SchemaRegistryProfile
from kafka_ingest.security import build_kafka_options, build_registry_auth, redact

SASL = KafkaClusterProfile(
    name="cc",
    bootstrap_servers="broker:9092",
    auth_mode="sasl_scram_sha_512",
    secret_scope="kv-test",
    sasl_username_key="user-key",
    sasl_password_key="pass-key",
)

MTLS = KafkaClusterProfile(
    name="onprem",
    bootstrap_servers="broker:9094",
    auth_mode="mtls",
    secret_scope="kv-test",
    truststore_path="/Volumes/c/s/truststore.jks",
    truststore_password_key="ts-pw",
    keystore_path="/Volumes/c/s/keystore.jks",
    keystore_password_key="ks-pw",
    key_password_key="key-pw",
)


def test_sasl_options_use_the_right_login_module(secrets):
    options = build_kafka_options(SASL, secrets)
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert "ScramLoginModule" in options["kafka.sasl.jaas.config"]
    # Credentials came from the configured scope/keys, not from anything hardcoded.
    assert ("kv-test", "user-key") in secrets.requested
    assert ("kv-test", "pass-key") in secrets.requested


def test_plain_and_scram_use_different_login_modules(secrets):
    plain = KafkaClusterProfile(
        name="cc", bootstrap_servers="b:9092", auth_mode="sasl_plain",
        secret_scope="kv", sasl_username_key="u", sasl_password_key="p",
    )
    assert "PlainLoginModule" in build_kafka_options(plain, secrets)["kafka.sasl.jaas.config"]
    assert "ScramLoginModule" in build_kafka_options(SASL, secrets)["kafka.sasl.jaas.config"]


def test_mtls_options_reference_volume_paths_and_never_inline_certs(secrets):
    options = build_kafka_options(MTLS, secrets)
    assert options["kafka.security.protocol"] == "SSL"
    assert options["kafka.ssl.keystore.location"].startswith("/Volumes/")
    assert options["kafka.ssl.truststore.location"].startswith("/Volumes/")
    assert "kafka.sasl.jaas.config" not in options
    # Passwords are resolved, the files themselves are not read into the options map.
    assert options["kafka.ssl.key.password"] == "kv-test/key-pw/value"


def test_sasl_cluster_can_still_carry_a_private_ca_truststore(secrets):
    """SASL_SSL against a self-managed cluster needs a truststore; Confluent Cloud does not."""
    profile = KafkaClusterProfile(
        name="cp", bootstrap_servers="b:9093", auth_mode="sasl_scram_sha_512",
        secret_scope="kv", sasl_username_key="u", sasl_password_key="p",
        truststore_path="/Volumes/c/s/ca.jks", truststore_password_key="ts",
    )
    options = build_kafka_options(profile, secrets)
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.ssl.truststore.location"] == "/Volumes/c/s/ca.jks"


def test_extra_options_are_prefixed(secrets):
    profile = KafkaClusterProfile(
        name="cc", bootstrap_servers="b:9092", auth_mode="sasl_plain",
        secret_scope="kv", sasl_username_key="u", sasl_password_key="p",
        extra_options={"session.timeout.ms": "45000"},
    )
    assert build_kafka_options(profile, secrets)["kafka.session.timeout.ms"] == "45000"


def test_credential_that_would_break_the_jaas_string_fails_loudly(secrets):
    """A quote in a password silently truncates the JAAS config - refuse it instead."""
    secrets.values[("kv-test", "pass-key")] = 'has"quote'
    with pytest.raises(ConfigError, match="double quote"):
        build_kafka_options(SASL, secrets)


def test_registry_auth_is_independent_of_kafka_auth(secrets):
    basic = SchemaRegistryProfile(
        name="sr", url="https://sr", auth_mode="basic",
        secret_scope="kv-sr", username_key="u", password_key="p",
    )
    auth = build_registry_auth(basic, secrets)
    assert auth.auth == ("kv-sr/u/value", "kv-sr/p/value")
    assert auth.cert is None
    # Registry credentials came from their own scope, not the Kafka one.
    assert ("kv-sr", "u") in secrets.requested


def test_registry_mtls_uses_client_cert_pair(secrets):
    profile = SchemaRegistryProfile(
        name="sr", url="https://sr", auth_mode="mtls",
        client_cert_path="/Volumes/c/s/sr.pem", client_key_path="/Volumes/c/s/sr-key.pem",
        ca_bundle_path="/Volumes/c/s/ca.pem",
    )
    auth = build_registry_auth(profile, secrets)
    assert auth.cert == ("/Volumes/c/s/sr.pem", "/Volumes/c/s/sr-key.pem")
    assert auth.verify == "/Volumes/c/s/ca.pem"
    assert auth.auth is None


def test_registry_auth_repr_never_leaks_credentials(secrets):
    basic = SchemaRegistryProfile(
        name="sr", url="https://sr", auth_mode="basic",
        secret_scope="kv", username_key="u", password_key="p",
    )
    rendered = repr(build_registry_auth(basic, secrets))
    assert "value" not in rendered
    assert "basic=yes" in rendered


def test_redaction_masks_every_credential_bearing_option(secrets):
    masked = redact(build_kafka_options(MTLS, secrets))
    assert masked["kafka.ssl.keystore.location"] == "/Volumes/c/s/keystore.jks"
    for key, value in masked.items():
        if "password" in key or "jaas" in key:
            assert value == "***REDACTED***", key
