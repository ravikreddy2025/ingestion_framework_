"""Profiles + resolved secrets -> ready-to-use connection options. NO PySpark import.

This is the Kafka half of the security split. framework/security.py answers "what is the
value of this secret?" for every source type; this module answers the question only a
Kafka source asks: "given this cluster profile, what exactly do I hand to the Kafka source,
and given this registry profile, what do I hand to `requests`?"

The split is by SHAPE, not by sensitivity. An options map is shaped by the system being
connected to - `kafka.*` keys here, JDBC properties for a database, `fs.*` keys for a
storage account - so it belongs to that source's package. Keeping it in framework/ would
mean framework/ knowing what a JAAS config is, which is the leak CORE section 7 forbids.

EXECUTOR VISIBILITY (a prerequisite, not something this code can influence)
--------------------------------------------------------------------------
`ssl.truststore.location` / `ssl.keystore.location` are opened by the Kafka client running
on the EXECUTORS, not the driver. UC Volume FUSE paths must therefore be readable from
executors, which is a property of the compute access mode and runtime - VB-11. The
registry PEMs below are different: `requests` runs on the driver only, so driver-side
Volume access is sufficient for those.

If a future compute profile cannot read Volumes from executors, the documented fallback is
`SparkContext.addFile()` + `SparkFiles.get()` staging. It is NOT built here, because no
in-scope cluster needs it today and an untested fallback is worse than a documented one.
"""

from __future__ import annotations

import logging
from typing import Any

from ...framework.config import ConfigError
from ...framework.security import SecretResolver, redact
from .config import (
    AUTH_MTLS,
    AUTH_SASL_PLAIN,
    AUTH_SASL_SCRAM_256,
    AUTH_SASL_SCRAM_512,
    REGISTRY_AUTH_BASIC,
    REGISTRY_AUTH_MTLS,
    ClusterProfile,
    RegistryProfile,
)

LOG = logging.getLogger(__name__)

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


def build_kafka_options(cluster: ClusterProfile, secrets: SecretResolver) -> dict[str, str]:
    """The `kafka.*`-prefixed options for spark.read / spark.readStream.

    Every returned key is already prefixed with "kafka." - Spark forwards those verbatim to
    the underlying consumer and rejects unknown non-prefixed keys, so the prefix is not
    decoration.
    """
    options: dict[str, str] = {"kafka.bootstrap.servers": cluster.bootstrap_servers}

    if cluster.auth_mode == AUTH_MTLS:
        options["kafka.security.protocol"] = "SSL"
        options.update(_truststore_options(cluster, secrets))
        options["kafka.ssl.keystore.location"] = _require(cluster.keystore_path, "keystore_path", cluster.name)
        options["kafka.ssl.keystore.type"] = cluster.keystore_type
        if cluster.keystore_password_key:
            options["kafka.ssl.keystore.password"] = secrets.get(cluster.secret_scope, cluster.keystore_password_key)
        # The private key password is often distinct from the keystore password. When the
        # key is not separately encrypted the profile simply omits key_password_key.
        if cluster.key_password_key:
            options["kafka.ssl.key.password"] = secrets.get(cluster.secret_scope, cluster.key_password_key)
    else:
        options["kafka.security.protocol"] = "SASL_SSL"
        options["kafka.sasl.mechanism"] = _SASL_MECHANISMS[cluster.auth_mode]
        username = secrets.get(cluster.secret_scope, cluster.sasl_username_key)
        password = secrets.get(cluster.secret_scope, cluster.sasl_password_key)
        options["kafka.sasl.jaas.config"] = _jaas_config(cluster.auth_mode, username, password)
        # A self-managed cluster fronted by a private CA still needs a truststore under
        # SASL_SSL; a managed cloud cluster on a public CA does not.
        if cluster.truststore_path:
            options.update(_truststore_options(cluster, secrets))

    for key, value in (cluster.extra_options or {}).items():
        options[f"kafka.{key}"] = str(value)

    LOG.info("Kafka options for cluster '%s': %s", cluster.name, redact(options))
    return options


def _truststore_options(cluster: ClusterProfile, secrets: SecretResolver) -> dict[str, str]:
    options = {
        "kafka.ssl.truststore.location": _require(cluster.truststore_path, "truststore_path", cluster.name),
        "kafka.ssl.truststore.type": cluster.truststore_type,
    }
    if cluster.truststore_password_key:
        options["kafka.ssl.truststore.password"] = secrets.get(cluster.secret_scope, cluster.truststore_password_key)
    return options


def _jaas_config(auth_mode: str, username: str, password: str) -> str:
    """Build the JAAS login string.

    The value is a Java config snippet, so an embedded double quote in a credential would
    terminate the string early and produce a confusing broker-side auth failure rather than
    a clear error here. Fail loudly instead - rotating the secret is the correct fix, and
    it is a one-minute one.
    """
    for label, value in (("username", username), ("password", password)):
        if '"' in value or "\\" in value:
            raise ConfigError(
                f"Kafka {label} contains a double quote or backslash, which cannot be safely "
                "embedded in sasl.jaas.config. Rotate the credential to an alphanumeric value."
            )
    return f'{_LOGIN_MODULES[auth_mode]} required username="{username}" password="{password}";'


def _require(value: str | None, label: str, cluster_name: str) -> str:
    if not value:
        raise ConfigError(f"cluster '{cluster_name}': {label} is required for this auth mode")
    return value


# --------------------------------------------------------------------------------------
# Schema Registry
# --------------------------------------------------------------------------------------


class RegistryAuth:
    """Everything `requests` needs to talk to one registry instance.

    A small object rather than a dict so the registry client cannot accidentally log it -
    __repr__ says whether each credential is present and never what it is.
    """

    def __init__(self, auth: tuple | None = None, cert: tuple | None = None, verify: Any = True):
        self.auth = auth  # (user, password) for basic auth
        self.cert = cert  # (client_cert_pem, client_key_pem) for mTLS
        self.verify = verify  # True, or a CA bundle path

    def __repr__(self) -> str:
        return (
            f"RegistryAuth(basic={'yes' if self.auth else 'no'}, "
            f"mtls={'yes' if self.cert else 'no'}, verify={self.verify})"
        )


def build_registry_auth(registry: RegistryProfile, secrets: SecretResolver) -> RegistryAuth:
    """Registry auth is resolved INDEPENDENTLY of Kafka auth, by design."""
    auth = None
    cert = None
    verify: Any = registry.ca_bundle_path or True

    if registry.auth_mode == REGISTRY_AUTH_BASIC:
        auth = (
            secrets.get(registry.secret_scope, registry.username_key),
            secrets.get(registry.secret_scope, registry.password_key),
        )
    elif registry.auth_mode == REGISTRY_AUTH_MTLS:
        # requests reads these PEM files on the DRIVER only - the registry is never
        # contacted from executors, so driver-side Volume access is sufficient here.
        cert = (registry.client_cert_path, registry.client_key_path)

    LOG.info("Schema Registry '%s' auth resolved: %s", registry.name, RegistryAuth(auth, cert, verify))
    return RegistryAuth(auth=auth, cert=cert, verify=verify)
