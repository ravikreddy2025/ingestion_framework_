"""Profile + resolved secrets -> ready-to-use JDBC options. NO PySpark import.

This is the Oracle half of the security split. framework/security.py answers "what is the
value of this secret?" for every source type; this module answers the question only a
database source asks: "given this connection profile, what exactly do I hand to the JDBC
data source?"

The split is by SHAPE, not by sensitivity. An options map is shaped by the system being
connected to, so it belongs to that source's package - keeping it in framework/ would mean
framework/ knowing what a JDBC URL is, which is the leak CORE section 7 forbids.

WHAT MUST NEVER HAPPEN HERE
---------------------------
A credential must not reach a log line or an audit row. Two things enforce that, and both
matter:

  * the option holding it is called `password`, which framework/logs.py masks by NAME -
    `redact()` is applied to every map this module logs;
  * the URL is BUILT from validated parts in config.py rather than configured as a string,
    so `user/password@host` cannot be smuggled into the one option whose name carries no
    hint of being sensitive.

CONNECTIONS ARE OPENED ON THE EXECUTORS, one per JDBC partition. That is why
`sessionInitStatement` has to be cheap and idempotent (config.py checks its shape), and it
is also why there is no connection pool here: Spark opens and closes its own connections
per partition, and a pool in the driver process would pool nothing.
"""

from __future__ import annotations

import logging

from ...framework.security import SecretResolver, redact
from .config import ORACLE_DRIVER, JdbcProfile

LOG = logging.getLogger(__name__)


def build_connection_options(profile: JdbcProfile, secrets: SecretResolver) -> dict[str, str]:
    """The connection half of the JDBC options: where, who, and with which driver.

    Deliberately NOT the read half - `dbtable`, `fetchsize` and the partitioning options
    are assembled in reader.py, because those are decided by the extraction rather than by
    the connection. A reader can therefore be built and asserted without a secret in sight.
    """
    options = {
        "url": profile.url,
        "driver": ORACLE_DRIVER,
        "user": secrets.get(profile.secret_scope, profile.username_key),
        "password": secrets.get(profile.secret_scope, profile.password_key),
    }
    # Driver properties from the register, verbatim. Passed through rather than validated
    # against a list: the useful ones (oracle.jdbc.mapDateToTimestamp and friends) are
    # driver-version-specific, and a list here would go stale silently - see VB-02/VB-03.
    options.update({str(key): str(value) for key, value in (profile.extra_options or {}).items()})

    LOG.info("JDBC connection options for profile '%s': %s", profile.name, redact(options))
    return options
