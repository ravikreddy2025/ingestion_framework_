"""Secrets, and the one rule about what must never reach a log line.

TWO THINGS LIVE HERE, AND NEITHER KNOWS WHAT IT IS CONNECTING TO.

  SecretResolver   scope + key -> the secret value, via Databricks secret scopes backed
                   by a cloud key vault. Scope and key NAMES come from configuration;
                   nothing is hardcoded, and no secret VALUE ever appears in this
                   repository or in a log line.
  redact()         mask credential-bearing entries in an options map before it is logged
                   or audited. Re-exported from framework/logs.py - see below.

Turning a resolved secret into a connection options map is NOT here, and that is the
whole point of the split: an options map is shaped by the system being connected to - its
own option prefixes, its own property names, its own idea of what a credential looks like
in a config string - so it belongs to that source type's own package. What every source
shares is "read a secret" and "never log one", which is exactly what this module is.

CERTIFICATES are referenced by path and are never read, copied or staged by the driver.
Whether the process that opens one can SEE that path is a compute-profile property, not
something this module can influence - a store opened by a client running on the executors
has a different answer from a PEM opened by an HTTP client on the driver. Each source
documents which of the two it needs; VB-11 is the check.

WHY redact() IS RE-EXPORTED RATHER THAN REIMPLEMENTED
-----------------------------------------------------
framework/logs.py already masks by option-name, with a hint list deliberately widened past
any one system's naming conventions, and every log line it renders goes through it. A
second implementation here would be a second list to keep in step, and the one that fell
behind would be the one leaking. So there is exactly one implementation, and this module -
where a reader looks for it - names it.
"""

from __future__ import annotations

from typing import Any

from .config import ConfigError
from .logs import MASK, is_sensitive, redact

__all__ = ["MASK", "ConfigError", "SecretResolver", "get_dbutils", "is_sensitive", "redact"]


class SecretResolver:
    """Thin wrapper over dbutils.secrets so unit tests can inject a fake.

    Caches within a run: the same scope/key is often needed by more than one connection a
    single run builds, and each dbutils call is a control-plane round trip.

    The cache lives as long as the resolver, which is one run. Nothing invalidates it - a
    secret rotated mid-run would not be picked up, which is correct: a run that started
    with one credential should finish with it rather than change identity halfway.
    """

    def __init__(self, dbutils: Any = None):
        self._dbutils = dbutils or get_dbutils()
        self._cache: dict[tuple[str, str], str] = {}

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
                    f"backed by the right key vault and that the job's service principal has READ "
                    f"on it. Underlying error: {type(exc).__name__}: {exc}"
                ) from exc
        return self._cache[cache_key]


def get_dbutils() -> Any:
    """Obtain dbutils in both notebook and wheel-task contexts.

    Two lookups because the two contexts inject it differently, and a job that runs fine
    interactively and fails as a wheel task is a bad way to find that out.
    """
    try:  # notebook / %run context injects it as a global
        import IPython

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:  # noqa: BLE001 - absence of IPython is not an error, just the other context
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
