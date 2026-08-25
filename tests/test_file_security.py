"""sources/file/security.py - profile + secrets -> ADLS session options, and that no
credential ever reaches a log line.
"""

from __future__ import annotations

from conftest import FakeSecrets
from kafka_ingest.framework.logs import MASK
from kafka_ingest.sources.file.config import StorageProfile
from kafka_ingest.sources.file.security import build_storage_options


def test_account_key_auth_builds_the_one_scoped_option():
    profile = StorageProfile(
        name="adls_demo",
        account="acct1",
        container="landing",
        auth_mode="account_key",
        secret_scope="kv-demo",
        account_key_secret_key="the-key",
    )
    options = build_storage_options(profile, FakeSecrets({("kv-demo", "the-key"): "SECRET_VALUE"}))
    assert options == {"fs.azure.account.key.acct1.dfs.core.windows.net": "SECRET_VALUE"}


def test_service_principal_auth_builds_the_full_oauth_option_set():
    profile = StorageProfile(
        name="adls_demo",
        account="acct1",
        container="landing",
        auth_mode="service_principal",
        secret_scope="kv-demo",
        client_id_secret_key="client-id-key",
        client_secret_secret_key="client-secret-key",
        tenant_id="tenant-123",
    )
    secrets = FakeSecrets(
        {("kv-demo", "client-id-key"): "CLIENT_ID", ("kv-demo", "client-secret-key"): "CLIENT_SECRET"}
    )
    options = build_storage_options(profile, secrets)

    endpoint = "acct1.dfs.core.windows.net"
    assert options[f"fs.azure.account.auth.type.{endpoint}"] == "OAuth"
    assert options[f"fs.azure.account.oauth2.client.id.{endpoint}"] == "CLIENT_ID"
    assert options[f"fs.azure.account.oauth2.client.secret.{endpoint}"] == "CLIENT_SECRET"
    assert "tenant-123" in options[f"fs.azure.account.oauth2.client.endpoint.{endpoint}"]


def test_no_credential_reaches_the_log(monkeypatch, caplog):
    """redact() masks by KEY NAME - `account.key` and `client.secret` are both already in
    framework/logs.py's hint list, so a file-shaped options map is covered with no
    widening required."""
    profile = StorageProfile(
        name="adls_demo",
        account="acct1",
        container="landing",
        auth_mode="account_key",
        secret_scope="kv-demo",
        account_key_secret_key="the-key",
    )
    import logging

    caplog.set_level(logging.INFO)
    build_storage_options(profile, FakeSecrets({("kv-demo", "the-key"): "TOP_SECRET_VALUE"}))
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "TOP_SECRET_VALUE" not in logged
    assert MASK in logged
