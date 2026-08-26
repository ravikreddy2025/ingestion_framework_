"""The resolved settings this source actually runs on. NO PySpark import.

framework/config.py hands every source a `ResolvedConfig`: identity, the declared layers,
a read-only settings mapping and the registers. This module turns that generic mapping
into ONE frozen dataclass with names, types and validation - so the rest of the package
reads `cfg.source_path` rather than `ctx.cfg.get("source_path")`, and a typo is an
AttributeError rather than a None that reaches a reader.

The framework validates KEYS against SOURCE_SPEC; this module validates VALUES - the
enumerations, the cross-field rules (`schema` required only when `schema_mode: provided`),
and the two things that must fail at CONFIG LOAD rather than at read time: a
`filename_columns` regex that does not compile or does not have exactly one capture group,
and a `format_options` key that is not known for the configured `file_format`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from ...framework.config import ConfigError
from .spec import CHECKPOINT_RESET_ID

RUN_TYPE_PRIMARY = "primary"
# No file replay job exists yet (STAGE_5 brief lists no run_replay.py / job_replay.yml
# change), so this is the only run type this source accepts today. Declared as a tuple
# rather than a single constant so a future file_replay type is a one-line change here.
VALID_RUN_TYPES = (RUN_TYPE_PRIMARY,)

SCHEMA_PROVIDED = "provided"
SCHEMA_HINTS = "hints"
SCHEMA_INFER = "infer"
VALID_SCHEMA_MODES = (SCHEMA_PROVIDED, SCHEMA_HINTS, SCHEMA_INFER)

LISTING_DIRECTORY = "directory"
LISTING_NOTIFICATION = "notification"
VALID_LISTING_MODES = (LISTING_DIRECTORY, LISTING_NOTIFICATION)

FORMAT_CSV = "csv"
FORMAT_JSON = "json"
FORMAT_PARQUET = "parquet"
FORMAT_AVRO = "avro"
VALID_FILE_FORMATS = (FORMAT_CSV, FORMAT_JSON, FORMAT_PARQUET, FORMAT_AVRO)

# Values match the CHECK constraint on ingest_control.file_failure_mode (sql/01), the same
# two values Kafka's failure_mode uses - see run.py for what each means with no separate
# quarantine table.
FAILURE_FAILFAST = "FAILFAST"
FAILURE_QUARANTINE = "QUARANTINE"
VALID_FAILURE_MODES = (FAILURE_FAILFAST, FAILURE_QUARANTINE)

# Reader options known to be genuine Spark options for each format. Deliberately a
# conservative subset - CORE section 2 rule 2 forbids inventing one, so this lists only
# options this project is confident exist rather than a guess at Auto Loader's full surface.
# An option not in the set for the configured format is rejected: Spark silently ignores an
# unknown reader option, so without this a typo'd `delimeter` produces a table full of
# one-column rows and no error anywhere.
_KNOWN_FORMAT_OPTIONS = {
    FORMAT_CSV: frozenset(
        {
            "header",
            "delimiter",
            "encoding",
            "quote",
            "escape",
            "comment",
            "nullValue",
            "emptyValue",
            "dateFormat",
            "timestampFormat",
            "multiLine",
            "ignoreLeadingWhiteSpace",
            "ignoreTrailingWhiteSpace",
        }
    ),
    FORMAT_JSON: frozenset(
        {
            "multiLine",
            "encoding",
            "dateFormat",
            "timestampFormat",
            "allowComments",
            "primitivesAsString",
        }
    ),
    FORMAT_PARQUET: frozenset({"mergeSchema", "datetimeRebaseMode"}),
    FORMAT_AVRO: frozenset({"avroSchema", "datetimeRebaseMode", "ignoreExtension"}),
}

# A Unity Catalog identifier that needs no backtick quoting - the same rule
# framework/tables.py applies to the assembled three-part name, checked here on the two
# parts this source derives so a bad one fails with this source's own vocabulary rather
# than the framework's generic one.
_UC_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# docs/build_log/DECISIONS.md D-15: access_mode is an EXPLICIT choice between the two ways
# this source can read, not inferred from source_path's shape (D-13, superseded). "volume"
# takes volume_path and no storage credentials at all - UC governs it directly. "adls"
# takes storage_ref + source_path, exactly as this source worked before D-13 existed.
ACCESS_MODE_VOLUME = "volume"
ACCESS_MODE_ADLS = "adls"
VALID_ACCESS_MODES = (ACCESS_MODE_VOLUME, ACCESS_MODE_ADLS)

# /Volumes/<catalog>/<schema>/<volume>/... - three non-empty segments after /Volumes/, then
# whatever path is left. {catalog} is already substituted by framework/config.py by the
# time this module sees the value (it is an ordinary `vars:` placeholder, not a deferred
# target_token), so this checks only the SHAPE of the resolved string.
_VOLUME_PATH_SHAPE = re.compile(r"^/Volumes/[^/]+/[^/]+/[^/]+/.+$")

# A rerun/reset id becomes part of a Delta app id and a path segment.
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


@dataclass(frozen=True)
class StorageProfile:
    """One entry from conf/storage.yaml, with this environment's overlay applied.

    Holds no credential and never will: `secret_scope` plus the KEY NAMES are what is
    written down, and sources/file/security.py resolves them at run time - the same split
    as sources/kafka/config.py's ClusterProfile and sources/oracle/config.py's JdbcProfile.
    """

    name: str
    account: str
    container: str
    auth_mode: str
    secret_scope: str | None = None
    account_key_secret_key: str | None = None
    client_id_secret_key: str | None = None
    client_secret_secret_key: str | None = None
    tenant_id: str | None = None

    def __post_init__(self) -> None:
        where = f"storage profile '{self.name}'"
        if self.auth_mode not in ("account_key", "service_principal"):
            raise ConfigError(f"{where}: auth_mode '{self.auth_mode}' not in ['account_key', 'service_principal']")
        if not (self.account and self.container):
            raise ConfigError(f"{where}: account and container are both required")
        if self.auth_mode == "account_key":
            if not (self.secret_scope and self.account_key_secret_key):
                raise ConfigError(f"{where}: account_key auth requires secret_scope and account_key_secret_key")
        elif not (self.secret_scope and self.client_id_secret_key and self.client_secret_secret_key and self.tenant_id):
            raise ConfigError(
                f"{where}: service_principal auth requires secret_scope, client_id_secret_key, "
                "client_secret_secret_key and tenant_id"
            )

    @property
    def endpoint(self) -> str:
        """The `dfs.core.windows.net` host this account's Hadoop options are keyed under."""
        return f"{self.account}.dfs.core.windows.net"


@dataclass(frozen=True)
class FilenameColumn:
    """One `filename_columns` entry: a derived column, and the regex that produces it."""

    column: str
    pattern: str


@dataclass(frozen=True)
class FileConfig:
    """One directory of files, one environment, one execution. Frozen: nothing mutates it
    mid-run."""

    source_key: str
    environment: str
    run_type: str
    domain: str

    access_mode: str  # "volume" | "adls" (docs/build_log/DECISIONS.md D-15)
    volume_path: str | None  # set only for access_mode "volume"
    storage_ref: str | None  # set only for access_mode "adls"
    storage: StorageProfile | None  # set only for access_mode "adls"
    source_path: str | None  # path WITHIN the container - set only for access_mode "adls"
    path_glob: str

    file_format: str
    format_options: Mapping[str, str]
    schema_mode: str
    schema: str | None

    landing_table: str
    landing_partition_by: tuple[str, ...]
    filename_columns: tuple[FilenameColumn, ...]

    checkpoint_root: str
    schema_location_root: str
    listing_mode: str
    max_files_per_trigger: int
    failure_mode: str

    table_properties: Mapping[str, Any]
    checkpoint_reset_id: str | None

    # -- derived ---------------------------------------------------------------------

    @property
    def is_replay(self) -> bool:
        return self.run_type != RUN_TYPE_PRIMARY

    @property
    def quarantine_on_error(self) -> bool:
        """FAILFAST refuses a batch that contains a rescued row; QUARANTINE lands it and
        only reports the count. There is no separate quarantine TABLE for this source - see
        the module docstring in run.py."""
        return self.failure_mode == FAILURE_QUARANTINE

    @property
    def checkpoint_path(self) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{self.source_key}/primary"

    @property
    def schema_location_path(self) -> str:
        return f"{self.schema_location_root.rstrip('/')}/{self.source_key}"

    @property
    def full_source_path(self) -> str:
        """The path Auto Loader actually reads.

        `access_mode: volume` (docs/build_log/DECISIONS.md D-15) reads `volume_path`
        directly - it takes no account or container, because Unity Catalog governs it
        directly. `access_mode: adls` never carries the account or container in
        `source_path` itself (CORE section 6, extended to storage the way it already
        applies to a catalog name): both come from `storage_ref`, which is the one thing
        that differs per environment - so the same source file resolves to a different
        account in dev and prod, exactly as an Oracle source's `jdbc_ref` does.
        """
        if self.access_mode == ACCESS_MODE_VOLUME:
            return self.volume_path
        return f"abfss://{self.storage.container}@{self.storage.endpoint}/{self.source_path.lstrip('/')}"

    @property
    def txn_app_id(self) -> str:
        """The identity Delta dedups retried batches against. STABLE across restarts.

        `checkpoint_reset_id` forks the lineage exactly as Kafka's does - see run.py's
        checkpoint-reset guard, which mirrors sources/kafka/run.py's by design
        (STAGE_5 brief: "do not write a second guard").
        """
        lineage = self.checkpoint_reset_id or "primary"
        return f"file_ingest::{self.source_key}::{self.run_type}::{lineage}"

    def source_detail(self) -> str:
        import json

        return json.dumps(
            {
                "access_mode": self.access_mode,
                "source_path": self.full_source_path,
                "path_glob": self.path_glob,
                "file_format": self.file_format,
                "schema_mode": self.schema_mode,
                "listing_mode": self.listing_mode,
                "checkpoint_path": self.checkpoint_path,
                "schema_location_path": self.schema_location_path,
                "txn_app_id": self.txn_app_id,
                "max_files_per_trigger": self.max_files_per_trigger,
                "failure_mode": self.failure_mode,
                "checkpoint_reset_id": self.checkpoint_reset_id,
            },
            sort_keys=True,
        )


# --------------------------------------------------------------------------------------
# Building one from the framework's ResolvedConfig
# --------------------------------------------------------------------------------------


def build(cfg: Any, run_type: str, tables: Any) -> FileConfig:
    """Turn a framework ResolvedConfig into a validated FileConfig.

    `tables` is framework/tables.py, carried on the RunContext. It renders the landing
    pattern - which still holds {target_schema} and {target_table}, because only this
    source can supply them - and validates the resulting name before anything connects.
    """
    if run_type not in VALID_RUN_TYPES:
        raise ConfigError(f"run_type '{run_type}' not in {sorted(VALID_RUN_TYPES)}")

    target_schema = _uc_identifier(_required_text(cfg, "target_schema"), "target_schema", cfg.source_key)
    target_table = _uc_identifier(_required_text(cfg, "target_table"), "target_table", cfg.source_key)
    access_mode, volume_path, storage_ref, storage, source_path = _access(cfg)

    reset_id = cfg.get(CHECKPOINT_RESET_ID)
    if reset_id and not _SAFE_ID.match(str(reset_id)):
        raise ConfigError(
            f"{CHECKPOINT_RESET_ID} '{reset_id}' must match [A-Za-z0-9_.-]{{1,64}} - it becomes "
            "part of this source's Delta transaction identity."
        )

    resolved = FileConfig(
        source_key=cfg.source_key,
        environment=cfg.environment,
        run_type=run_type,
        domain=str(cfg.get("domain") or ""),
        access_mode=access_mode,
        volume_path=volume_path,
        storage_ref=storage_ref,
        storage=storage,
        source_path=source_path,
        path_glob=_required_text(cfg, "path_glob"),
        file_format=str(cfg.get("file_format") or "").strip().lower(),
        format_options=_format_options(cfg),
        schema_mode=str(cfg.get("schema_mode") or "").strip().lower(),
        schema=_text(cfg.get("schema")),
        landing_table=tables.target(cfg, "landing", {"target_schema": target_schema, "target_table": target_table}),
        landing_partition_by=_identifiers(cfg, "landing_partition_by"),
        filename_columns=_filename_columns(cfg),
        checkpoint_root=_required_text(cfg, "checkpoint_root"),
        schema_location_root=_required_text(cfg, "schema_location_root"),
        listing_mode=str(cfg.get("listing_mode") or "").strip().lower(),
        max_files_per_trigger=_positive_int(cfg.get("max_files_per_trigger"), "max_files_per_trigger", cfg.source_key),
        failure_mode=str(cfg.get("failure_mode") or "").strip().upper(),
        table_properties=cfg.get("table_properties") or {},
        checkpoint_reset_id=str(reset_id) if reset_id else None,
    )
    _validate(resolved)
    return resolved


def _validate(cfg: FileConfig) -> None:
    """Value-level rules the spec cannot express: enumerations and cross-field pairs."""
    where = f"source '{cfg.source_key}'"
    if cfg.file_format not in VALID_FILE_FORMATS:
        raise ConfigError(f"{where}: file_format '{cfg.file_format}' not in {sorted(VALID_FILE_FORMATS)}")
    if cfg.schema_mode not in VALID_SCHEMA_MODES:
        raise ConfigError(f"{where}: schema_mode '{cfg.schema_mode}' not in {sorted(VALID_SCHEMA_MODES)}")
    if cfg.listing_mode not in VALID_LISTING_MODES:
        raise ConfigError(
            f"{where}: listing_mode '{cfg.listing_mode}' not in {sorted(VALID_LISTING_MODES)}. See VB-07."
        )
    if cfg.failure_mode not in VALID_FAILURE_MODES:
        raise ConfigError(
            f"{where}: failure_mode '{cfg.failure_mode}' not in {sorted(VALID_FAILURE_MODES)}. The same two "
            "values are the CHECK constraint on ingest_control.file_failure_mode."
        )
    if cfg.schema_mode == SCHEMA_PROVIDED and not cfg.schema:
        raise ConfigError(f"{where}: schema_mode 'provided' requires `schema:` - a DDL column-list string.")
    if cfg.schema_mode != SCHEMA_PROVIDED and cfg.schema:
        raise ConfigError(
            f"{where}: `schema:` is set but schema_mode is '{cfg.schema_mode}'. A provided schema only "
            "applies under schema_mode: provided - set that, or remove `schema:`."
        )
    if not cfg.checkpoint_root.startswith("/Volumes/"):
        raise ConfigError(f"{where}: checkpoint_root must be Volume-backed, got '{cfg.checkpoint_root}'")
    if not cfg.schema_location_root.startswith("/Volumes/"):
        raise ConfigError(f"{where}: schema_location_root must be Volume-backed, got '{cfg.schema_location_root}'")
    if not cfg.landing_partition_by:
        raise ConfigError(f"{where}: landing_partition_by must name at least one column (normally ['ingest_date'])")
    if cfg.source_path is not None and "://" in cfg.source_path:
        raise ConfigError(
            f"{where}: source_path '{cfg.source_path}' looks like a full URL. It must be the path "
            "WITHIN the container only (e.g. 'claims/inbound/') - the account and container come "
            "from storage_ref, which is what makes the same source file resolve to a different "
            "account per environment."
        )


def _access(cfg: Any) -> tuple[str, str | None, str | None, StorageProfile | None, str | None]:
    """`(access_mode, volume_path, storage_ref, storage, source_path)` for this source.

    docs/build_log/DECISIONS.md D-15: access_mode is an EXPLICIT choice, not inferred from
    source_path's shape (D-13's mechanism, now superseded). "volume" takes volume_path and
    REJECTS storage_ref/source_path; "adls" takes storage_ref + source_path and REJECTS
    volume_path. Checked here, not via SourceSpec.mutually_exclusive: that field only
    expresses "at most one of these two keys may be set," and cannot express "this key is
    required in mode X and forbidden in mode Y" with a message naming the mode - the same
    reason sources/oracle/config.py checks incremental_mode's cursor/filter requirements in
    its own module rather than in SourceSpec.
    """
    where = f"source '{cfg.source_key}'"
    access_mode = str(cfg.get("access_mode") or "").strip().lower()
    if access_mode not in VALID_ACCESS_MODES:
        raise ConfigError(f"{where}: access_mode '{access_mode}' not in {sorted(VALID_ACCESS_MODES)}")

    volume_path = _text(cfg.get("volume_path"))
    storage_ref = _text(cfg.get("storage_ref"))
    source_path = _text(cfg.get("source_path"))

    if access_mode == ACCESS_MODE_VOLUME:
        for key, value in (("storage_ref", storage_ref), ("source_path", source_path)):
            if value is not None:
                raise ConfigError(
                    f"{where}: '{key}' is set, but access_mode is 'volume'. '{key}' is REJECTED in "
                    "volume mode - a Unity Catalog Volume path is governed by Unity Catalog grants "
                    "directly and takes no storage credentials of this framework's own. Remove it, "
                    "or set access_mode to 'adls'."
                )
        if volume_path is None:
            raise ConfigError(
                f"{where}: access_mode is 'volume', which requires `volume_path:` - the "
                "/Volumes/<catalog>/<schema>/<volume>/... path this source reads."
            )
        if not _VOLUME_PATH_SHAPE.match(volume_path):
            raise ConfigError(
                f"{where}: volume_path '{volume_path}' does not match "
                "/Volumes/<catalog>/<schema>/<volume>/... - a Unity Catalog Volume path needs all "
                "three segments plus a trailing path."
            )
        return access_mode, volume_path, None, None, None

    # access_mode == ACCESS_MODE_ADLS
    if volume_path is not None:
        raise ConfigError(
            f"{where}: 'volume_path' is set, but access_mode is 'adls'. 'volume_path' is REJECTED "
            "in adls mode - set storage_ref and source_path instead, or set access_mode to 'volume'."
        )
    missing = [key for key, value in (("storage_ref", storage_ref), ("source_path", source_path)) if value is None]
    if missing:
        raise ConfigError(
            f"{where}: access_mode is 'adls', which requires {missing} - together they name where "
            "an ADLS-governed read comes from."
        )
    storage = StorageProfile(name=storage_ref, **dict(cfg.profile("storage", storage_ref)))
    return access_mode, None, storage_ref, storage, source_path


def _format_options(cfg: Any) -> dict[str, str]:
    """`format_options`, validated against the known set for the configured `file_format`.

    Validated here rather than in the spec because the legal KEY SET depends on the VALUE of
    another key (`file_format`) - exactly the kind of cross-field rule SourceSpec cannot
    express, and the same reason sources/oracle/config.py checks `dynamic_date_filter`'s
    shape here rather than there.
    """
    raw = cfg.get("format_options")
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConfigError(f"source '{cfg.source_key}': format_options must be a mapping, got {type(raw).__name__}.")
    file_format = str(cfg.get("file_format") or "").strip().lower()
    known = _KNOWN_FORMAT_OPTIONS.get(file_format, frozenset())
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ConfigError(
            f"source '{cfg.source_key}': format_options {unknown} not known for file_format "
            f"'{file_format}'. Known options: {sorted(known)}. Spark silently ignores an unknown "
            "reader option, so a typo here would otherwise produce a table full of wrong columns "
            "with no error."
        )
    return {str(key): str(value) for key, value in raw.items()}


def _filename_columns(cfg: Any) -> tuple[FilenameColumn, ...]:
    """`filename_columns`, with every regex compiled and checked for exactly one capture
    group - at config load, not at the first file that reaches it."""
    raw = cfg.get("filename_columns")
    if raw is None:
        return ()
    if not isinstance(raw, Mapping):
        raise ConfigError(f"source '{cfg.source_key}': filename_columns must be a mapping, got {type(raw).__name__}.")
    result = []
    for column, pattern in raw.items():
        where = f"source '{cfg.source_key}': filename_columns.{column}"
        try:
            compiled = re.compile(str(pattern))
        except re.error as exc:
            raise ConfigError(f"{where}: '{pattern}' does not compile as a regex: {exc}") from exc
        if compiled.groups != 1:
            raise ConfigError(
                f"{where}: '{pattern}' has {compiled.groups} capture groups - exactly one is required, "
                "so the derived column has an unambiguous value."
            )
        column_name = _uc_identifier(str(column), "filename_columns", cfg.source_key)
        result.append(FilenameColumn(column=column_name, pattern=str(pattern)))
    return tuple(result)


def _uc_identifier(value: str, key: str, source_key: str) -> str:
    if not _UC_IDENTIFIER.match(value):
        raise ConfigError(
            f"source '{source_key}': '{key}' is '{value}', which is not a legal unquoted Unity "
            "Catalog identifier - letters, digits and underscore, not starting with a digit."
        )
    return value


def _identifiers(cfg: Any, key: str) -> tuple[str, ...]:
    value = cfg.get(key)
    if value is None:
        return ()
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        raise ConfigError(f"source '{cfg.source_key}': '{key}' must be a list of column names, got {value!r}.")
    return tuple(str(item).strip() for item in value)


def _required_text(cfg: Any, key: str) -> str:
    value = cfg.get(key)
    if not value or not str(value).strip():
        raise ConfigError(
            f"source '{cfg.source_key}': '{key}' is required and is empty. Set it in the source "
            "file, or in conf/defaults/file.yaml if every file source shares it."
        )
    return str(value).strip()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_int(value: Any, key: str, source_key: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"source '{source_key}': '{key}' must be an integer, got {value!r}") from exc
    if number <= 0:
        raise ConfigError(
            f"source '{source_key}': '{key}' must be greater than zero, got {number}. Unset or zero is "
            "not 'no limit' - an unbounded first microbatch can swallow an entire backlog in one batch "
            "whose failure costs the whole run."
        )
    return number
