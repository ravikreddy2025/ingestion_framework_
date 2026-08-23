"""Structured log lines, and redaction of anything credential-bearing.

Every line a run emits carries the same three fields - source_type, source_key, run_id -
because the first question in every incident is "which feed, which run?". A driver log
that answers that with a grep is worth more than one that reads better in isolation:

    2026-08-23 09:14:02 INFO  ...framework.runner | event=run_started
        source_type=<type> source_key=patient_events run_id=patient_events-primary-9f3c
        environment=prod run_type=primary

REDACTION. Spark's own log redaction (spark.redaction.regex) covers driver logs, but our
INFO lines and the audit table are ours to protect. `redact()` masks by KEY NAME, not by
value, because a connection option's name is the only reliable signal available before the
value is resolved. The hint list below covers the option-name conventions of every
connection map this framework builds - broker, database and cloud storage alike - because
a list tuned to one of them leaks the moment a second source type is added. The tests in
tests/test_framework_logs.py hold real option maps of all three shapes.

Redaction is deliberately over-eager. Masking a harmless value costs a support engineer one
question; leaking a credential into a log aggregator costs a rotation.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Mapping

MASK = "***REDACTED***"

# Substrings that mark an option KEY as carrying a credential. Matched case-insensitively
# against the whole key, so "fs.azure.account.key.x.dfs.core.windows.net" is caught by
# "account.key" and "spark...oauth2.client.secret" by "secret".
#
# Note what is NOT here: a bare "key". Half the option names in a Kafka or JDBC map contain
# it ("sasl.mechanism" aside, think "keystore.location", "partitionColumn"), and masking
# locations and column names would make these lines useless for triage.
_SENSITIVE_HINTS = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "credential",
    "token",
    "jaas",
    "key.pem",
    "account.key",
    "account_key",
    "accountkey",
    "sharedkey",
    "shared_key",
    "connection_string",
    "connectionstring",
    "private_key",
    "privatekey",
    "api_key",
    "apikey",
    "authorization",
)


def is_sensitive(key: str) -> bool:
    lowered = str(key).lower()
    return any(hint in lowered for hint in _SENSITIVE_HINTS)


def redact(options: Mapping[str, Any]) -> dict[str, Any]:
    """Mask credential-bearing values before an options map reaches a log or the audit table."""
    return {key: (MASK if is_sensitive(key) else value) for key, value in options.items()}


def configure(level: int = logging.INFO) -> None:
    """One logging setup for every entrypoint. Stdout, because that is what the driver log
    captures on Databricks."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        stream=sys.stdout,
        force=True,
    )


class RunLog:
    """A logger bound to one run. Built once by the runner and carried on RunContext.

    Not a logging.Logger subclass and not a LoggerAdapter: a source author should be able
    to read this class end to end in under a minute and know exactly what reaches the log.
    """

    def __init__(self, source_type: str, source_key: str, run_id: str = "", logger: Any = None):
        self.source_type = source_type
        self.source_key = source_key
        self.run_id = run_id
        self._logger = logger or logging.getLogger("kafka_ingest.run")

    def info(self, event: str, **fields: Any) -> None:
        self._logger.info(self.line(event, **fields))

    def warning(self, event: str, **fields: Any) -> None:
        self._logger.warning(self.line(event, **fields))

    def error(self, event: str, **fields: Any) -> None:
        self._logger.error(self.line(event, **fields))

    def line(self, event: str, **fields: Any) -> str:
        """Render one line. Public so tests can assert on it without capturing logging."""
        parts = [
            f"event={event}",
            f"source_type={self.source_type}",
            f"source_key={self.source_key}",
            f"run_id={self.run_id}",
        ]
        parts.extend(f"{key}={_render(key, value)}" for key, value in fields.items())
        return " ".join(parts)


def _render(key: str, value: Any) -> str:
    """One field. Redacted by key name, and recursively for an options map."""
    if is_sensitive(key):
        return MASK
    if isinstance(value, Mapping):
        value = redact(value)
    text = str(value)
    return f'"{text}"' if (" " in text or text == "") else text
