"""Secrets, and the one rule about what must never reach a log line.

THREE THINGS LIVE HERE, AND NONE OF THEM KNOWS WHAT IT IS CONNECTING TO.

  SecretResolver        scope + key -> the secret value, via Databricks secret scopes
                         backed by a cloud key vault. Scope and key NAMES come from
                         configuration; nothing is hardcoded, and no secret VALUE ever
                         appears in this repository or in a log line.
  redact()               mask credential-bearing entries in an options map before it is
                         logged or audited. Re-exported from framework/logs.py - see below.
  apply_session_options() set Spark/Hadoop session configuration for the duration of one
                         call, then restore whatever was there before. Added in Stage 5 for
                         a source whose credentials are NOT read via `.option()` calls on
                         the reader the way most connection maps in this framework are -
                         they are read by the underlying Hadoop FileSystem from SESSION
                         configuration instead (an `fs.azure.account.key...`-style entry,
                         set with `spark.conf.set()`). This is the generic "set for one
                         call, then put back" mechanism framework/writers.py already uses
                         for the schema-evolution flag, pulled up here because a second
                         source needing session-scoped auth would otherwise duplicate it.
                         See VB-26.

Turning a resolved secret into a connection options map is NOT here, and that is the
whole point of the split: an options map is shaped by the system being connected to - its
own option prefixes, its own property names, its own idea of what a credential looks like
in a config string - so it belongs to that source type's own package. What every source
shares is "read a secret", "never log one", and now "apply it to the session and put it
back" - which is exactly what this module is.

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

from typing import Any, Callable, Mapping

from .config import ConfigError
from .logs import MASK, is_sensitive, redact

__all__ = [
    "MASK",
    "ConfigError",
    "SecretResolver",
    "apply_session_options",
    "get_dbutils",
    "is_sensitive",
    "redact",
]


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


def apply_session_options(spark: Any, options: Mapping[str, str]) -> Callable[[], None]:
    """Set session/Hadoop configuration, and return a callable that restores it.

    Mirrors framework/writers.py's handling of the schema-evolution session flag: capture
    whatever was there before setting anything, so the restore puts back a PRIOR VALUE
    rather than always unsetting - another job sharing this session may have set one of
    these deliberately, and silently clearing it would be a surprising side effect.

    Restoring is the caller's responsibility, in a `finally` around whatever needs the
    options applied - this function does not know how long that is. No secret VALUE is
    logged here; the caller has already resolved them, and this only sets them on the
    session.
    """
    previous = {key: spark.conf.get(key, None) for key in options}

    def _restore() -> None:
        for key, prior_value in previous.items():
            if prior_value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, prior_value)

    for key, value in options.items():
        spark.conf.set(key, value)
    return _restore
