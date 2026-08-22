"""Resolve secrets and certificates into ready-to-use connection options.

Two responsibilities, deliberately kept together because they answer one question:
"given a profile, what do I hand to the Kafka source / to `requests`?"

  * Secrets come from Databricks secret scopes backed by Azure Key Vault. Scope and key
    names arrive from config; nothing is hardcoded here.
  * Certificates come from Unity Catalog Volumes. They are referenced by path - the
    files are NOT copied, staged or read into memory by the driver.

EXECUTOR VISIBILITY (important prerequisite, not a code concern):
    `ssl.truststore.location` / `ssl.keystore.location` are opened by the Kafka client
    running on the *executors*, not the driver. UC Volume FUSE paths must therefore be
    readable from executors, which requires a compute configuration where that holds
    (dedicated access mode, or standard access mode on a recent DBR). This is verified
    once per cluster at onboarding - see README "mTLS prerequisites". If a future
    compute profile cannot read Volumes from executors, the documented fallback is
    `SparkContext.addFile()` + `SparkFiles.get()` staging. That fallback is NOT built
    here because no in-scope cluster needs it today.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from .config import (
    AUTH_MTLS,
    AUTH_SASL_PLAIN,
    AUTH_SASL_SCRAM_256,
    AUTH_SASL_SCRAM_512,
    REGISTRY_AUTH_BASIC,
    REGISTRY_AUTH_MTLS,
    ConfigError,
    KafkaClusterProfile,
    SchemaRegistryProfile,
)

LOG = logging.getLogger(__name__)

# Substrings that mark an option value as sensitive. Used by redact() before anything
# reaches a log line or the audit table.
_SENSITIVE_HINTS = ("password", "secret", "jaas", "credential", "token", "key.pem")

_LOGIN_MODULES = {
    AUTH_SASL_PLAIN: "org.apache.kafka.common.security.plain.PlainLoginModule",
    AUTH_SASL_SCRAM_256: "org.apache.kafka.common.security.scram.ScramLoginModule",
    AUTH_SASL_SCRAM_512: "org.apache.kafka.common.security.scram.ScramLoginModule",
}

_SASL_MECHANISMS = {
    AUTH_SASL_PLAIN: "PLAIN",
    AUTH_SASL_SCRAM_256: "SCRAM-SHA-256",
    AUTH_SASL_SCRAM_512: "SCRAM-SHA-512",
}


class SecretResolver:
    """Thin wrapper over dbutils.secrets so unit tests can inject a fake.

    Caches within a run: the same scope/key is often needed by both the Kafka options
    builder and the registry client, and each dbutils call is a control-plane round trip.
    """

    def __init__(self, dbutils: Any = None):
        self._dbutils = dbutils or _get_dbutils()
        self._cache: Dict[tuple, str] = {}

    def get(self, scope: str, key: str) -> str:
        if not scope or not key:
            raise ConfigError(f"secret lookup requires both scope and key (got scope={scope!r} key={key!r})")
        cache_key = (scope, key)
        if cache_key not in self._cache:
            try:
                self._cache[cache_key] = self._dbutils.secrets.get(scope=scope, key=key)
            except Exception as exc:  # blind catch: surface the scope/key, never the value
                raise ConfigError(
                    f"could not read secret '{key}' from scope '{scope}'. Check that the scope is "
                    f"backed by the right Key Vault and that the job's service principal has READ "
                    f"on it. Underlying error: {type(exc).__name__}: {exc}"
                ) from exc
        return self._cache[cache_key]


def _get_dbutils() -> Any:
    """Obtain dbutils in both notebook and wheel-task contexts."""
    try:  # notebook / %run context injects it as a global
        import IPython

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:  # noqa: BLE001
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate())
    except Exception as exc:
        raise ConfigError(
            "dbutils is not available in this context; secrets cannot be resolved. "
            "This code must run on Databricks compute."
        ) from exc


# --------------------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------------------


def build_kafka_options(cluster: KafkaClusterProfile, secrets: SecretResolver) -> Dict[str, str]:
    """Return the `kafka.*`-prefixed options for spark.read/readStream.

    Note every returned key is already prefixed with "kafka." - Spark forwards those
    verbatim to the underlying consumer and rejects unknown non-prefixed keys.
    """
    options: Dict[str, str] = {"kafka.bootstrap.servers": cluster.bootstrap_servers}

    if cluster.auth_mode == AUTH_MTLS:
        options["kafka.security.protocol"] = "SSL"
        options.update(_truststore_options(cluster, secrets))
        options["kafka.ssl.keystore.location"] = _require(cluster.keystore_path, "keystore_path", cluster.name)
        options["kafka.ssl.keystore.type"] = cluster.keystore_type
        if cluster.keystore_password_key:
            options["kafka.ssl.keystore.password"] = secrets.get(
                cluster.secret_scope, cluster.keystore_password_key
            )
        # Private key password is often distinct from the keystore password. When the
        # key is not separately encrypted the config simply omits key_password_key.
        if cluster.key_password_key:
            options["kafka.ssl.key.password"] = secrets.get(cluster.secret_scope, cluster.key_password_key)
    else:
        options["kafka.security.protocol"] = "SASL_SSL"
        options["kafka.sasl.mechanism"] = _SASL_MECHANISMS[cluster.auth_mode]
        username = secrets.get(cluster.secret_scope, cluster.sasl_username_key)
        password = secrets.get(cluster.secret_scope, cluster.sasl_password_key)
        options["kafka.sasl.jaas.config"] = _jaas_config(cluster.auth_mode, username, password)
        # A self-managed Confluent Platform cluster fronted by a private CA still needs a
        # truststore even under SASL_SSL; Confluent Cloud does not (public CA).
        if cluster.truststore_path:
            options.update(_truststore_options(cluster, secrets))

    for key, value in cluster.extra_options.items():
        options[f"kafka.{key}"] = str(value)

    LOG.info("Kafka options for cluster '%s': %s", cluster.name, redact(options))
    return options


def _truststore_options(cluster: KafkaClusterProfile, secrets: SecretResolver) -> Dict[str, str]:
    opts = {
        "kafka.ssl.truststore.location": _require(cluster.truststore_path, "truststore_path", cluster.name),
        "kafka.ssl.truststore.type": cluster.truststore_type,
    }
    if cluster.truststore_password_key:
        opts["kafka.ssl.truststore.password"] = secrets.get(
            cluster.secret_scope, cluster.truststore_password_key
        )
    return opts


def _jaas_config(auth_mode: str, username: str, password: str) -> str:
    """Build the JAAS login string.

    The value is a Java config snippet, so embedded double quotes in a credential would
    terminate the string early and produce a confusing broker-side auth failure rather
    than a clear error. Fail loudly instead - rotating the secret is the correct fix.
    """
    for label, value in (("username", username), ("password", password)):
        if '"' in value or "\\" in value:
            raise ConfigError(
                f"Kafka {label} contains a double quote or backslash, which cannot be safely "
                "embedded in sasl.jaas.config. Rotate the credential to an alphanumeric value."
            )
    module = _LOGIN_MODULES[auth_mode]
    return f'{module} required username="{username}" password="{password}";'


def _require(value: Optional[str], label: str, cluster_name: str) -> str:
    if not value:
        raise ConfigError(f"cluster '{cluster_name}': {label} is required for this auth mode")
    return value


# --------------------------------------------------------------------------------------
# Schema Registry
# --------------------------------------------------------------------------------------


class RegistryAuth:
    """Everything `requests` needs to talk to one registry instance.

    Kept as a small object rather than a dict so the registry client cannot accidentally
    log it - __repr__ is redacted.
    """

    def __init__(
        self,
        auth: Optional[tuple] = None,
        cert: Optional[tuple] = None,
        verify: Any = True,
    ):
        self.auth = auth      # (user, password) for basic auth
        self.cert = cert      # (client_cert_pem, client_key_pem) for mTLS
        self.verify = verify  # True, or a CA bundle path

    def __repr__(self) -> str:  # pragma: no cover - defensive, keeps creds out of tracebacks
        return (
            f"RegistryAuth(basic={'yes' if self.auth else 'no'}, "
            f"mtls={'yes' if self.cert else 'no'}, verify={self.verify})"
        )


def build_registry_auth(registry: SchemaRegistryProfile, secrets: SecretResolver) -> RegistryAuth:
    """Registry auth is resolved independently of Kafka auth by design - the same domain
    may use an API key for the broker and a different credential for the registry."""
    auth = None
    cert = None
    verify: Any = registry.ca_bundle_path or True

    if registry.auth_mode == REGISTRY_AUTH_BASIC:
        auth = (
            secrets.get(registry.secret_scope, registry.username_key),
            secrets.get(registry.secret_scope, registry.password_key),
        )
    elif registry.auth_mode == REGISTRY_AUTH_MTLS:
        # requests reads these PEM files from the driver only - the registry is never
        # contacted from executors, so Volume FUSE access on the driver is sufficient.
        cert = (registry.client_cert_path, registry.client_key_path)

    LOG.info("Schema Registry '%s' auth resolved: %s", registry.name, RegistryAuth(auth, cert, verify))
    return RegistryAuth(auth=auth, cert=cert, verify=verify)


# --------------------------------------------------------------------------------------
# Redaction
# --------------------------------------------------------------------------------------


def redact(options: Dict[str, str]) -> Dict[str, str]:
    """Mask credential-bearing values before logging or auditing an options map.

    Spark's own log redaction (spark.redaction.regex) covers driver logs, but the audit
    table and our INFO lines are ours to protect.
    """
    masked = {}
    for key, value in options.items():
        lowered = key.lower()
        masked[key] = "***REDACTED***" if any(hint in lowered for hint in _SENSITIVE_HINTS) else value
    return masked
