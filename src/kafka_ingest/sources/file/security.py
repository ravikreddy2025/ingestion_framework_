"""Profile + resolved secrets -> ready-to-use ADLS Gen2 session options. NO PySpark import.

This is the file source's half of the security split. framework/security.py answers "what
is the value of this secret?" for every source type, and now also "how do I apply session-
scoped options and put them back?"; this module answers the question only a file source
asks: "given this storage profile, which `fs.azure.*` Hadoop options authenticate against
it?"

WHY THESE READ AS SESSION OPTIONS, NOT READER OPTIONS
-------------------------------------------------------
`abfss://` paths are opened by Hadoop's Azure Filesystem (ABFS) driver, which reads
credentials from the SparkContext's Hadoop configuration - not from `.option()` on the
DataFrameReader the way `kafka.*` or JDBC properties do. sources/file/run.py applies the
map this module builds with framework/security.py's `apply_session_options()`, around the
read, and restores whatever was there before. See VB-26: this is the documented mechanism
for direct (non-Unity-Catalog-external-location) ADLS Gen2 access, not measured against a
real workspace.

TWO AUTH MODES, DELIBERATELY. Everything else Azure/ADLS supports (SAS tokens, managed
identity, Unity Catalog credential passthrough) needs either a token-provider class this
project cannot verify exists on the target runtime, or workspace-level UC wiring outside
this repository's control - so, as with Oracle's JDBC auth (sources/oracle/config.py
JDBC_AUTH_BASIC), adding either is a deliberate code change, not a config guess.
"""

from __future__ import annotations

import logging

from ...framework.security import SecretResolver, redact
from .config import StorageProfile

LOG = logging.getLogger(__name__)

OAUTH_LOGIN_ENDPOINT = "https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# The Hadoop-Azure (ABFS driver) token-provider class for OAuth 2.0 client-credentials
# auth. A well-documented, stable class name - not something this project invented.
CLIENT_CREDS_TOKEN_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"


def build_storage_options(storage: StorageProfile, secrets: SecretResolver) -> dict[str, str]:
    """Every `fs.azure.*` session option this profile's auth mode needs.

    Keyed on `storage.endpoint` (`<account>.dfs.core.windows.net`), which is how the ABFS
    driver scopes a Hadoop config entry to one storage account among however many a
    session touches.
    """
    endpoint = storage.endpoint
    if storage.auth_mode == "account_key":
        options = {
            f"fs.azure.account.key.{endpoint}": secrets.get(storage.secret_scope, storage.account_key_secret_key),
        }
    else:
        options = {
            f"fs.azure.account.auth.type.{endpoint}": "OAuth",
            f"fs.azure.account.oauth.provider.type.{endpoint}": CLIENT_CREDS_TOKEN_PROVIDER,
            f"fs.azure.account.oauth2.client.id.{endpoint}": secrets.get(
                storage.secret_scope, storage.client_id_secret_key
            ),
            f"fs.azure.account.oauth2.client.secret.{endpoint}": secrets.get(
                storage.secret_scope, storage.client_secret_secret_key
            ),
            f"fs.azure.account.oauth2.client.endpoint.{endpoint}": OAUTH_LOGIN_ENDPOINT.format(
                tenant_id=storage.tenant_id
            ),
        }

    LOG.info("ADLS session options for storage profile '%s': %s", storage.name, redact(options))
    return options
