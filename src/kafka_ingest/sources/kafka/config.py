"""The resolved settings this source actually runs on. NO PySpark import.

framework/config.py hands every source a `ResolvedConfig`: identity, the declared layers,
a read-only settings mapping and the registers. This module turns that generic mapping
into ONE frozen dataclass with names, types and validation - so the rest of the package
reads `cfg.topic` rather than `ctx.cfg.get("topic")`, and a typo is an AttributeError
rather than a None that reaches a broker.

WHY THE SOURCE BUILDS ITS OWN DATACLASS
---------------------------------------
The framework cannot name it. Doing so would mean framework/ importing a source module by
name, which is the one thing CORE section 7 forbids. So the contract is: the framework
validates KEYS against SOURCE_SPEC, and this module validates VALUES - the enumerations,
the cross-field rules, and the two path rules that must fail before anything connects.

Both halves matter and neither substitutes for the other: the spec catches
`reader_schema_mod: pinned_id` (a typo), and this catches `reader_schema_mode: writer` (a
mode that does not exist).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from ...framework.config import ConfigError
from .spec import (
    CHECKPOINT_RESET_ID,
    REPLAY_ENDING_OFFSETS,
    REPLAY_ENDING_TIMESTAMP,
    REPLAY_LANDING_FILTER,
    REPLAY_STARTING_OFFSETS,
    REPLAY_STARTING_TIMESTAMP,
)

RUN_TYPE_PRIMARY = "primary"
RUN_TYPE_KAFKA_REPLAY = "kafka_replay"
RUN_TYPE_CURATED_REPLAY = "curated_replay"
VALID_RUN_TYPES = (RUN_TYPE_PRIMARY, RUN_TYPE_KAFKA_REPLAY, RUN_TYPE_CURATED_REPLAY)

AUTH_SASL_PLAIN = "sasl_plain"
AUTH_SASL_SCRAM_256 = "sasl_scram_sha_256"
AUTH_SASL_SCRAM_512 = "sasl_scram_sha_512"
AUTH_MTLS = "mtls"
VALID_KAFKA_AUTH = (AUTH_SASL_PLAIN, AUTH_SASL_SCRAM_256, AUTH_SASL_SCRAM_512, AUTH_MTLS)

REGISTRY_AUTH_NONE = "none"
REGISTRY_AUTH_BASIC = "basic"
REGISTRY_AUTH_MTLS = "mtls"
VALID_REGISTRY_AUTH = (REGISTRY_AUTH_NONE, REGISTRY_AUTH_BASIC, REGISTRY_AUTH_MTLS)

# THERE IS DELIBERATELY NO "use each writer schema as-is" MODE. Curated keeps the payload
# as ONE struct column, and two writer versions decoded without a common reader schema
# produce two incompatible struct types that cannot share a table. A reader schema is what
# makes a mixed-version microbatch land in one table - see curated.py.
READER_LATEST = "registry_latest"
READER_PINNED = "pinned_id"
VALID_READER_MODES = (READER_LATEST, READER_PINNED)

# Values match the CHECK constraint on ingest_control.kafka_failure_mode (sql/01), because
# the column and the setting are the same lever and disagreeing spellings would mean
# support setting a legal-looking value this code does not recognise.
FAILURE_FAILFAST = "FAILFAST"
FAILURE_QUARANTINE = "QUARANTINE"
VALID_FAILURE_MODES = (FAILURE_FAILFAST, FAILURE_QUARANTINE)

# A rerun_id and a reset id each become a path segment and part of a Delta app id.
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# A Unity Catalog identifier that needs no backtick quoting.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Kafka topic separators that are not legal in an unquoted UC identifier.
_TOPIC_NAME_SEPARATORS = str.maketrans({".": "_", "-": "_"})

_VOLUME_PREFIX = "/Volumes/"


def table_name_for(topic: str) -> str:
    """Derive a table name from a Kafka topic name.

    Kafka topics conventionally use dots and hyphens (`vector.patient.events.v1`); neither
    is legal in an unquoted Unity Catalog identifier, so both become underscores:

        vector.patient.events.v1  ->  vector_patient_events_v1
        rcm-claim-status.v2       ->  rcm_claim_status_v2

    Case is left alone. UC folds identifiers to lower case itself, and changing it here
    would make the configured name and the catalogued name differ for no benefit.

    A topic whose name cannot survive this (a leading digit, other punctuation) is a hard
    error rather than a silently mangled table - set `table_name:` in the source file.
    """
    candidate = topic.translate(_TOPIC_NAME_SEPARATORS)
    if not _SAFE_TABLE_NAME.match(candidate):
        raise ConfigError(
            f"cannot derive a table name from topic '{topic}': '{candidate}' is not a valid "
            "Unity Catalog identifier. Set `table_name:` explicitly in the source file."
        )
    return candidate


# --------------------------------------------------------------------------------------
# Connection profiles - one per register entry, validated on construction
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ClusterProfile:
    """One entry from conf/clusters.yaml, with this environment's overlay applied."""

    name: str
    bootstrap_servers: str
    auth_mode: str
    secret_scope: str | None = None

    sasl_username_key: str | None = None
    sasl_password_key: str | None = None

    truststore_path: str | None = None
    truststore_password_key: str | None = None
    truststore_type: str = "JKS"
    keystore_path: str | None = None
    keystore_password_key: str | None = None
    key_password_key: str | None = None
    keystore_type: str = "JKS"

    extra_options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.auth_mode not in VALID_KAFKA_AUTH:
            raise ConfigError(f"cluster '{self.name}': auth_mode '{self.auth_mode}' not in {sorted(VALID_KAFKA_AUTH)}")
        if self.auth_mode == AUTH_MTLS:
            if not (self.keystore_path and self.truststore_path):
                raise ConfigError(f"cluster '{self.name}': mtls requires keystore_path and truststore_path")
        elif not (self.secret_scope and self.sasl_username_key and self.sasl_password_key):
            raise ConfigError(f"cluster '{self.name}': SASL auth requires secret_scope + username/password key names")
        for label in ("truststore_path", "keystore_path"):
            _require_volume_path(getattr(self, label), f"cluster '{self.name}': {label}")


@dataclass(frozen=True)
class RegistryProfile:
    """One entry from conf/registries.yaml. Auth is INDEPENDENT of the cluster's.

    The same domain routinely uses an API key for the broker and a different credential
    for the registry, so nothing here is derived from the cluster profile.
    """

    name: str
    url: str
    auth_mode: str = REGISTRY_AUTH_NONE
    secret_scope: str | None = None
    username_key: str | None = None
    password_key: str | None = None
    client_cert_path: str | None = None
    client_key_path: str | None = None
    ca_bundle_path: str | None = None
    timeout_seconds: int = 20
    max_retries: int = 3

    def __post_init__(self) -> None:
        if self.auth_mode not in VALID_REGISTRY_AUTH:
            raise ConfigError(
                f"registry '{self.name}': auth_mode '{self.auth_mode}' not in {sorted(VALID_REGISTRY_AUTH)}"
            )
        if self.auth_mode == REGISTRY_AUTH_BASIC and not (
            self.secret_scope and self.username_key and self.password_key
        ):
            raise ConfigError(f"registry '{self.name}': basic auth requires scope + username/password key names")
        if self.auth_mode == REGISTRY_AUTH_MTLS and not (self.client_cert_path and self.client_key_path):
            raise ConfigError(f"registry '{self.name}': mtls requires client_cert_path and client_key_path")
        for label in ("client_cert_path", "client_key_path", "ca_bundle_path"):
            _require_volume_path(getattr(self, label), f"registry '{self.name}': {label}")


def _require_volume_path(path: str | None, where: str) -> None:
    """A certificate that is not on a UC Volume is REJECTED, not merely discouraged.

    A workspace-file or DBFS path is either unreadable from where the client opens it or
    ungoverned, and both failures show up as an opaque SSL error on a cluster rather than
    as a configuration mistake here.
    """
    if path and not str(path).startswith(_VOLUME_PREFIX):
        raise ConfigError(f"{where} must be a Unity Catalog Volume path starting {_VOLUME_PREFIX}, got '{path}'")


# --------------------------------------------------------------------------------------
# Replay controls
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplayControls:
    """What THIS execution is replaying, if anything. Every field is operational-only.

    `rerun_id` is the load-bearing one: it isolates the checkpoint path AND the Delta app
    id. An existing checkpoint always beats `startingOffsets`, so a replay without that
    isolation would silently ignore the offsets it was given and advance production state
    at the same time.
    """

    rerun_id: str | None = None
    starting_offsets: str | None = None
    starting_timestamp: str | None = None
    ending_offsets: str | None = None
    ending_timestamp: str | None = None
    landing_filter: str | None = None

    @property
    def is_bounded(self) -> bool:
        """A replay with an explicit end runs as a bounded BATCH read.

        The streaming Kafka source has no ending-offset option - it always reads to
        latest - so "just the bad two hours" has to drop to spark.read.
        """
        return bool(self.ending_offsets or self.ending_timestamp)


@dataclass(frozen=True)
class KafkaConfig:
    """One topic, one environment, one execution. Frozen: nothing mutates it mid-run."""

    source_key: str
    environment: str
    run_type: str

    topic: str
    domain: str
    subject: str
    cluster: ClusterProfile
    registry: RegistryProfile

    landing_table: str
    curated_table: str
    quarantine_table: str
    table_name: str

    checkpoint_root: str
    consumer_group_prefix: str
    starting_offsets: str
    trigger: str
    fail_on_data_loss: bool
    min_partitions: int
    max_offsets_per_trigger: int

    reader_schema_mode: str
    reader_schema_id: int | None
    failure_mode: str

    landing_partition_by: tuple[str, ...]
    curated_partition_by: tuple[str, ...]
    curated_dedup_keys: tuple[str, ...]
    curated_dedup_order_by: str

    table_properties: Mapping[str, Any]
    checkpoint_reset_id: str | None
    replay: ReplayControls

    # -- derived ---------------------------------------------------------------------

    @property
    def is_replay(self) -> bool:
        return self.run_type != RUN_TYPE_PRIMARY

    @property
    def quarantine_on_error(self) -> bool:
        return self.failure_mode == FAILURE_QUARANTINE

    @property
    def checkpoint_path(self) -> str:
        """Primary and replay checkpoints are siblings, never nested in one another.

        Keying the replay location on rerun_id makes replays repeatable (the same rerun_id
        resumes the same replay) and makes two replays of one topic incapable of colliding.
        """
        base = f"{self.checkpoint_root.rstrip('/')}/{self.source_key}"
        if self.run_type == RUN_TYPE_KAFKA_REPLAY:
            return f"{base}/replay/{self.replay.rerun_id}"
        return f"{base}/primary"

    @property
    def txn_app_id(self) -> str:
        """The identity Delta dedups retried batches against. STABLE across restarts.

        Derived from the source key and the run lineage, never from anything per-run: a
        random component would make every restart a new writer and defeat the whole
        idempotent-write mechanism.

        checkpoint_reset_id forks the lineage exactly as a replay's rerun_id does. A
        deliberately RESET primary is, for dedup purposes, a writer that has never
        committed anything - so it must not inherit the old lineage's committed versions.
        That fork is what makes a reset safe, and it is why REUSING a reset id is a refusal
        in run.py rather than a warning.
        """
        lineage = self.replay.rerun_id or self.checkpoint_reset_id or "primary"
        return f"kafka_ingest::{self.source_key}::{self.run_type}::{lineage}"

    @property
    def group_id_prefix(self) -> str:
        """Spark's Kafka source manages its own consumer group and OVERRIDES
        kafka.group.id; groupIdPrefix is the only supported knob. Replays get their own
        prefix so broker-side consumer metrics do not blend replay with primary."""
        if self.is_replay:
            return f"{self.consumer_group_prefix}-{self.run_type}-{self.replay.rerun_id}"
        return self.consumer_group_prefix

    @property
    def ingested_via(self) -> str:
        return self.run_type

    def source_detail(self) -> str:
        """The JSON blob the audit row carries for this source type.

        Everything here is a question a support engineer asks during an incident and would
        otherwise answer by reading YAML in a Git branch that may since have moved on:
        which broker, which registry, which checkpoint, and - the one that matters most -
        whether this run was allowed to skip aged-out records.
        """
        return json.dumps(
            {
                "topic": self.topic,
                "subject": self.subject,
                "cluster": self.cluster.name,
                "registry": self.registry.name,
                "checkpoint_path": self.checkpoint_path,
                "txn_app_id": self.txn_app_id,
                "fail_on_data_loss": self.fail_on_data_loss,
                "failure_mode": self.failure_mode,
                "reader_schema_mode": self.reader_schema_mode,
                "max_offsets_per_trigger": self.max_offsets_per_trigger,
                "min_partitions": self.min_partitions,
                "checkpoint_reset_id": self.checkpoint_reset_id,
            },
            sort_keys=True,
        )


# --------------------------------------------------------------------------------------
# Building one from the framework's ResolvedConfig
# --------------------------------------------------------------------------------------


def build(cfg: Any, run_type: str, tables: Any) -> KafkaConfig:
    """Turn a framework ResolvedConfig into a validated KafkaConfig.

    `tables` is framework/tables.py, carried on the RunContext. It renders the three target
    patterns - which still hold {topic_table}, because only this source can compute it -
    and validates the resulting names before anything connects.
    """
    if run_type not in VALID_RUN_TYPES:
        raise ConfigError(f"run_type '{run_type}' not in {sorted(VALID_RUN_TYPES)}")

    topic = _required_text(cfg, "topic")
    table_name = cfg.get("table_name") or table_name_for(topic)
    targets = tables.targets(cfg, {"topic_table": table_name})

    reset_id = cfg.get(CHECKPOINT_RESET_ID)
    if reset_id and not _SAFE_ID.match(str(reset_id)):
        raise ConfigError(
            f"{CHECKPOINT_RESET_ID} '{reset_id}' must match [A-Za-z0-9_.-]{{1,64}} - it becomes "
            "part of this source's Delta transaction identity."
        )

    resolved = KafkaConfig(
        source_key=cfg.source_key,
        environment=cfg.environment,
        run_type=run_type,
        topic=topic,
        domain=str(cfg.get("domain") or ""),
        subject=_required_text(cfg, "subject"),
        cluster=_cluster(cfg),
        registry=_registry(cfg),
        landing_table=targets["landing"],
        curated_table=targets["curated"],
        quarantine_table=targets["quarantine"],
        table_name=table_name,
        checkpoint_root=_required_text(cfg, "checkpoint_root"),
        consumer_group_prefix=_required_text(cfg, "consumer_group_prefix"),
        starting_offsets=str(cfg.get("starting_offsets")),
        trigger=str(cfg.get("trigger")),
        fail_on_data_loss=_as_bool(cfg.get("fail_on_data_loss")),
        min_partitions=_positive_int(cfg.get("min_partitions"), "min_partitions", cfg.source_key),
        max_offsets_per_trigger=_positive_int(
            cfg.get("max_offsets_per_trigger"), "max_offsets_per_trigger", cfg.source_key
        ),
        reader_schema_mode=str(cfg.get("reader_schema_mode")),
        reader_schema_id=_optional_int(cfg.get("reader_schema_id")),
        failure_mode=str(cfg.get("failure_mode")),
        landing_partition_by=_columns(cfg, "landing_partition_by"),
        curated_partition_by=_columns(cfg, "curated_partition_by"),
        curated_dedup_keys=tuple(cfg.get("curated_dedup_keys") or ()),
        curated_dedup_order_by=str(cfg.get("curated_dedup_order_by") or "kafka_timestamp"),
        table_properties=cfg.get("table_properties") or {},
        checkpoint_reset_id=str(reset_id) if reset_id else None,
        replay=_replay_controls(cfg, run_type),
    )
    _validate(resolved)
    return resolved


def _validate(cfg: KafkaConfig) -> None:
    """Value-level rules the spec cannot express: enumerations and cross-field pairs."""
    where = f"source '{cfg.source_key}'"
    if cfg.reader_schema_mode not in VALID_READER_MODES:
        raise ConfigError(
            f"{where}: reader_schema_mode '{cfg.reader_schema_mode}' not in {sorted(VALID_READER_MODES)}. "
            "There is deliberately no mode that decodes with each record's own writer schema "
            "as-is - see the note in sources/kafka/config.py."
        )
    if cfg.reader_schema_mode == READER_PINNED and not cfg.reader_schema_id:
        raise ConfigError(f"{where}: reader_schema_mode=pinned_id requires reader_schema_id")
    if cfg.failure_mode not in VALID_FAILURE_MODES:
        raise ConfigError(
            f"{where}: failure_mode '{cfg.failure_mode}' not in {sorted(VALID_FAILURE_MODES)}. "
            "The same two values are the CHECK constraint on ingest_control.kafka_failure_mode."
        )
    if not cfg.checkpoint_root.startswith(_VOLUME_PREFIX):
        raise ConfigError(f"{where}: checkpoint_root must be Volume-backed, got '{cfg.checkpoint_root}'")
    if not cfg.landing_partition_by:
        raise ConfigError(f"{where}: landing_partition_by must name at least one column (normally ['ingest_date'])")
    if not cfg.curated_partition_by:
        raise ConfigError(f"{where}: curated_partition_by must name at least one column (normally ['event_date'])")


def _replay_controls(cfg: Any, run_type: str) -> ReplayControls:
    """Assemble and validate the replay bounds for this run type.

    The mutually-exclusive PAIRS are already rejected by the spec. What is left is the
    rules that depend on the run type, which the spec has no way to express.
    """
    controls = ReplayControls(
        rerun_id=_text(cfg.get("rerun_id")),
        starting_offsets=_text(cfg.get(REPLAY_STARTING_OFFSETS)),
        starting_timestamp=_text(cfg.get(REPLAY_STARTING_TIMESTAMP)),
        ending_offsets=_text(cfg.get(REPLAY_ENDING_OFFSETS)),
        ending_timestamp=_text(cfg.get(REPLAY_ENDING_TIMESTAMP)),
        landing_filter=_text(cfg.get(REPLAY_LANDING_FILTER)),
    )
    if run_type == RUN_TYPE_PRIMARY:
        return controls

    if not controls.rerun_id:
        raise ConfigError(
            f"{run_type} requires a rerun_id - it is what isolates the replay's checkpoint AND "
            "its Delta transaction identity from the primary lineage. Without it the replay "
            "would resume the primary checkpoint, silently ignore the offsets it was given, and "
            "advance production state."
        )
    if not _SAFE_ID.match(controls.rerun_id):
        raise ConfigError(
            f"rerun_id '{controls.rerun_id}' must match [A-Za-z0-9_.-]{{1,64}} - it becomes a path segment."
        )

    if run_type == RUN_TYPE_KAFKA_REPLAY:
        if not (controls.starting_offsets or controls.starting_timestamp):
            raise ConfigError(
                f"kafka_replay requires exactly one of {REPLAY_STARTING_OFFSETS} or {REPLAY_STARTING_TIMESTAMP}."
            )
        for label, raw in (
            (REPLAY_STARTING_OFFSETS, controls.starting_offsets),
            (REPLAY_ENDING_OFFSETS, controls.ending_offsets),
        ):
            if raw:
                _require_offsets_json(label, raw)
    if run_type == RUN_TYPE_CURATED_REPLAY and not controls.landing_filter:
        raise ConfigError(
            f"curated_replay requires a {REPLAY_LANDING_FILTER} predicate - refusing to re-parse "
            "the entire landing history implicitly."
        )
    return controls


def _require_offsets_json(label: str, raw: str) -> None:
    """Offsets JSON must parse AND look like {"topic": {"partition": offset}}.

    Rejected here rather than at the broker because Spark's own error for a malformed
    offsets string names neither the option nor the value.
    """
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{label} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ConfigError(f"{label} must be a non-empty JSON object keyed by topic name.")
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            raise ConfigError(f"{label}: value for topic '{topic}' must be an object of partition -> offset.")
        for part, offset in partitions.items():
            if not str(part).lstrip("-").isdigit() or not isinstance(offset, int):
                raise ConfigError(
                    f"{label}: topic '{topic}' partition '{part}' -> '{offset}' is not an int offset "
                    "(-1 = latest, -2 = earliest)."
                )


def _cluster(cfg: Any) -> ClusterProfile:
    name = _required_text(cfg, "cluster")
    return ClusterProfile(name=name, **dict(cfg.profile("clusters", name)))


def _registry(cfg: Any) -> RegistryProfile:
    name = _required_text(cfg, "registry")
    return RegistryProfile(name=name, **dict(cfg.profile("registries", name)))


def _columns(cfg: Any, key: str) -> tuple[str, ...]:
    value = cfg.get(key)
    if isinstance(value, str):
        return (value,)
    return tuple(value or ())


def _required_text(cfg: Any, key: str) -> str:
    value = cfg.get(key)
    if not value or not str(value).strip():
        raise ConfigError(f"source '{cfg.source_key}': '{key}' is required and is empty.")
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
            f"source '{source_key}': '{key}' must be greater than zero, got {number}. Leaving it "
            "unset or zero is not the same as 'no limit' - see conf/defaults/kafka.yaml."
        )
    return number


def _optional_int(value: Any) -> int | None:
    return None if value is None or value == "" else int(value)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
