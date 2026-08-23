"""Structured log lines and redaction.

Redaction is tested against every shape of option map the framework will ever log, not
just the one that exists today - a hint list that only covers one source type's option
names is a hint list that leaks the moment a second source is added.
"""

from __future__ import annotations

import logging

import pytest

from kafka_ingest.framework.logs import MASK, RunLog, redact

# The three option maps this framework produces. Values are deliberately recognisable so a
# leak shows up as a literal string in the assertion, not as a missing mask.
BROKER_OPTIONS = {
    "kafka.bootstrap.servers": "broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": 'ScramLoginModule required username="u" password="LEAK";',
    "kafka.ssl.truststore.location": "/Volumes/c/s/truststore.jks",
    "kafka.ssl.truststore.password": "LEAK",
    "kafka.ssl.keystore.location": "/Volumes/c/s/keystore.jks",
    "kafka.ssl.key.password": "LEAK",
}

DATABASE_OPTIONS = {
    "url": "jdbc:oracle:thin:@//host:1521/SVC",
    "user": "ingest_svc",
    "password": "LEAK",
    "driver": "oracle.jdbc.OracleDriver",
    "fetchsize": "10000",
    "oracle.net.wallet_password": "LEAK",
    "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'",
}

STORAGE_OPTIONS = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth2.client.id": "a-client-id",
    "fs.azure.account.oauth2.client.secret": "LEAK",
    "fs.azure.account.key.acct.dfs.core.windows.net": "LEAK",
    "sas_token": "LEAK",
    "cloudFiles.format": "parquet",
}

ALL_OPTIONS = [
    pytest.param(BROKER_OPTIONS, id="broker"),
    pytest.param(DATABASE_OPTIONS, id="database"),
    pytest.param(STORAGE_OPTIONS, id="storage"),
]


@pytest.mark.parametrize("options", ALL_OPTIONS)
def test_no_credential_value_survives_redaction(options):
    """Every value spelled LEAK in the maps above is a credential. None may appear."""
    masked = redact(options)
    assert "LEAK" not in " ".join(str(value) for value in masked.values())


@pytest.mark.parametrize("options", ALL_OPTIONS)
def test_redaction_leaves_everything_diagnosable(options):
    """Over-eager is right; masking everything is not. An endpoint, a driver name and a
    fetch size are what a support engineer actually reads these lines for."""
    masked = redact(options)
    for key, value in options.items():
        if value != "LEAK" and "LEAK" not in str(value):
            assert masked[key] == value, key


def test_masked_keys_are_replaced_not_removed():
    """A missing key reads as 'not configured', which is a different and misleading fact."""
    masked = redact(DATABASE_OPTIONS)
    assert set(masked) == set(DATABASE_OPTIONS)
    assert masked["password"] == MASK


def test_a_bare_key_suffix_does_not_trigger_redaction():
    """`keystore.location` and `partitionColumn` must stay readable - masking them would
    make the lines useless without protecting anything."""
    masked = redact({"kafka.ssl.keystore.location": "/Volumes/c/s/ks.jks", "partitionColumn": "ID"})
    assert masked["kafka.ssl.keystore.location"] == "/Volumes/c/s/ks.jks"
    assert masked["partitionColumn"] == "ID"


# --------------------------------------------------------------------------------------
# The structured line
# --------------------------------------------------------------------------------------


def test_every_line_carries_source_type_source_key_and_run_id():
    """The first question in every incident is 'which feed, which run?'."""
    line = RunLog("demo", "demo_source", "demo_source-primary-abc").line("run_started", rows=0)
    assert "event=run_started" in line
    assert "source_type=demo" in line
    assert "source_key=demo_source" in line
    assert "run_id=demo_source-primary-abc" in line
    assert "rows=0" in line


def test_a_sensitive_field_is_redacted_in_a_log_line():
    line = RunLog("demo", "demo_source").line("connected", password="LEAK")
    assert "LEAK" not in line
    assert f"password={MASK}" in line


def test_an_options_map_logged_as_a_field_is_redacted():
    """The likeliest accident: logging the whole connection map to debug a timeout."""
    line = RunLog("demo", "demo_source").line("options", connection=DATABASE_OPTIONS)
    assert "LEAK" not in line
    assert "jdbc:oracle:thin:@//host:1521/SVC" in line


def test_a_value_containing_spaces_is_quoted():
    """Otherwise one field with a space silently becomes two fields to whatever parses it."""
    assert 'error="something went wrong"' in RunLog("demo", "k").line("failed", error="something went wrong")


def test_lines_reach_the_logger_at_the_right_level(caplog):
    log = RunLog("demo", "demo_source", "run-1")
    with caplog.at_level(logging.INFO):
        log.info("started")
        log.warning("slow")
        log.error("failed")
    levels = [record.levelno for record in caplog.records]
    assert levels == [logging.INFO, logging.WARNING, logging.ERROR]
    assert all("source_key=demo_source" in record.getMessage() for record in caplog.records)
