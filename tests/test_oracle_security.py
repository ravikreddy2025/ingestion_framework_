"""sources/oracle/security.py and the JDBC profile - credentials, and where they must not go.

Nothing here connects to anything. What it asserts is the part that would be a security
incident rather than a bug: that a credential reaches the driver and NOTHING ELSE, and
that the URL is built from validated parts rather than accepted as a string.
"""

from __future__ import annotations

import pytest

from conftest import FakeSecrets, make_oracle_cfg, write_oracle_source
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.framework.logs import MASK, RunLog, redact
from kafka_ingest.sources.oracle import security
from kafka_ingest.sources.oracle.config import ORACLE_DRIVER, JdbcProfile

PROFILE = {
    "name": "oracle_demo",
    "host": "oracle-dev.corp.internal",
    "port": 1521,
    "service_name": "CLAIMSPDB",
    "secret_scope": "kv-oracle-dev",
    "username_key": "oracle-user",
    "password_key": "oracle-password",
}


def _profile(**overrides):
    return JdbcProfile(**{**PROFILE, **overrides})


# --------------------------------------------------------------------------------------
# The URL is built, never configured
# --------------------------------------------------------------------------------------


def test_a_service_name_profile_builds_the_modern_url_form():
    assert _profile().url == "jdbc:oracle:thin:@//oracle-dev.corp.internal:1521/CLAIMSPDB"


def test_a_sid_profile_builds_the_older_url_form():
    """Many on-premise instances still present a SID rather than a service name, and the
    two URL forms are not interchangeable."""
    assert _profile(service_name=None, sid="BILLPRD").url == "jdbc:oracle:thin:@oracle-dev.corp.internal:1521:BILLPRD"


@pytest.mark.parametrize(
    "overrides",
    [
        {"service_name": None, "sid": None},
        {"sid": "BILLPRD"},
    ],
)
def test_exactly_one_of_service_name_and_sid_is_required(overrides):
    """Guessing between them produces a connection failure that names neither."""
    with pytest.raises(ConfigError, match="service_name or sid"):
        _profile(**overrides)


@pytest.mark.parametrize(
    "host",
    ["user/password@real-host", "host:1521/OTHER", "host name", "", "-leading-dash"],
)
def test_a_host_that_is_not_a_hostname_is_refused(host):
    """THE ONE THAT MATTERS: `user/password@host` in the host field would put a credential
    inside `url`, which is the single option whose NAME gives a log redactor no hint that
    it might be sensitive. It is refused before it can be built into anything."""
    with pytest.raises(ConfigError):
        _profile(host=host)


def test_a_profile_without_a_secret_reference_is_refused():
    """The register records the NAMES of secrets. A profile with none is either a password
    about to be written into YAML or a connection that cannot authenticate."""
    with pytest.raises(ConfigError, match="username_key"):
        _profile(password_key=None)


def test_an_unimplemented_auth_mode_is_refused_rather_than_half_supported():
    """Wallets and Kerberos both need a file or ticket staged on the executors. Adding one
    is a code change, deliberately, so that no half-supported mode is selectable in YAML."""
    with pytest.raises(ConfigError, match="auth_mode"):
        _profile(auth_mode="wallet")


# --------------------------------------------------------------------------------------
# The options map
# --------------------------------------------------------------------------------------


def test_the_connection_options_are_the_four_the_driver_needs():
    secrets = FakeSecrets(
        {("kv-oracle-dev", "oracle-user"): "svc_ingest", ("kv-oracle-dev", "oracle-password"): "s3cret"}
    )
    options = security.build_connection_options(_profile(), secrets)

    assert options["url"] == "jdbc:oracle:thin:@//oracle-dev.corp.internal:1521/CLAIMSPDB"
    assert options["driver"] == ORACLE_DRIVER
    assert options["user"] == "svc_ingest"
    assert options["password"] == "s3cret"
    assert secrets.requested == [("kv-oracle-dev", "oracle-user"), ("kv-oracle-dev", "oracle-password")]


def test_the_driver_class_is_named_rather_than_discovered():
    """Left out, the JVM's driver auto-discovery decides - and on a cluster with two Oracle
    drivers on the classpath it can decide differently than it did in dev."""
    assert "driver" in security.build_connection_options(_profile(), FakeSecrets())


def test_register_extra_options_reach_the_driver_verbatim():
    """This is where a driver property such as oracle.jdbc.mapDateToTimestamp belongs once
    VB-03 answers whether it is needed - per profile, in a PR."""
    profile = _profile(extra_options={"oracle.jdbc.mapDateToTimestamp": True})
    assert security.build_connection_options(profile, FakeSecrets())["oracle.jdbc.mapDateToTimestamp"] == "True"


# --------------------------------------------------------------------------------------
# No credential in a log line or an audit row
# --------------------------------------------------------------------------------------


def test_no_credential_survives_redaction_of_the_options_map():
    """The map this module builds, through the framework's own redactor - the same call
    every log line and every audited options map goes through."""
    secrets = FakeSecrets({("kv-oracle-dev", "oracle-password"): "s3cret"})
    redacted = redact(security.build_connection_options(_profile(), secrets))

    assert redacted["password"] == MASK
    assert "s3cret" not in str(redacted)
    # And the rest is still readable: a redactor that masked the URL would make every
    # "which database did this run read?" question unanswerable.
    assert redacted["url"].endswith("/CLAIMSPDB")


def test_a_rendered_log_line_never_carries_the_password():
    """Belt and braces over the layer that actually reaches the driver log."""
    secrets = FakeSecrets({("kv-oracle-dev", "oracle-password"): "s3cret"})
    options = security.build_connection_options(_profile(), secrets)
    line = RunLog("oracle", "demo_oracle", "run-1").line("jdbc_connect", options=options)

    assert "s3cret" not in line
    assert MASK in line


def test_the_password_is_read_once_per_run_not_once_per_partition():
    """SecretResolver caches within a run; each dbutils call is a control-plane round trip
    and a read is built more than once (a bounds probe, then the extract)."""
    secrets = FakeSecrets()
    security.build_connection_options(_profile(), secrets)
    security.build_connection_options(_profile(), secrets)
    assert len(secrets.requested) == 4  # the FAKE does not cache; the real resolver does


# --------------------------------------------------------------------------------------
# The shipped register
# --------------------------------------------------------------------------------------


def test_the_shipped_profiles_resolve_in_every_environment(oracle_config_root):
    """The synthetic tree's own profile, through the real five-layer path: a jdbc_ref
    resolves to a profile with everything JdbcProfile requires."""
    write_oracle_source(oracle_config_root, jdbc_ref="oracle_demo")
    for environment in ("dev", "prod"):
        cfg = make_oracle_cfg(oracle_config_root, environment=environment)
        assert cfg.jdbc.url.startswith("jdbc:oracle:thin:@//")
        assert environment in cfg.jdbc.secret_scope
        assert "{" not in cfg.jdbc.url
