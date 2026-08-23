"""Two-tier configuration loading and merging.

FIVE LAYERS. Later always wins on a per-key basis; absent keys fall through.

  STRUCTURAL (YAML in Git, PR-reviewed, deployed by DAB)
    1. conf/defaults.yaml            defaults common to every source of every type
       conf/defaults/kafka.yaml      defaults common to every Kafka source
    2. conf/environments/<env>.yaml  vars (e.g. catalog), per-environment defaults
                                     (`defaults:` and `defaults_by_type: kafka:`), and
                                     per-environment cluster/registry overrides
    3. conf/sources/<key>.yaml        only what is unique to this topic
         3a. topic:.environments.<env>  OPTIONAL, within the same file - what is unique to
                                     this topic IN ONE environment. Rare: most topics never
                                     need it. See below.

  OPERATIONAL (no deploy required)
    4. <ops_catalog>.ingestion.ingestion_topic_control   support-team runtime overrides
    5. job parameters                                    one-off overrides from Workflows

  Supporting registers, merged into layer 2:
    conf/clusters.yaml    which Kafka clusters exist (auth mode, secret KEY names, certs)
    conf/registries.yaml  which Schema Registries exist

{placeholder} tokens in layers 1-3 resolve from the environment's `vars:`, plus topic_key
and domain for topic settings. An unresolved placeholder is a hard error.

FULL PRECEDENCE, LATER ALWAYS WINS:
    defaults.yaml -> environments/<env>.yaml -> sources/<key>.yaml
        -> sources/<key>.yaml: environments.<env> -> control table -> job parameters

topic:.environments.<env> is still layer 3 - still Git, still PR-reviewed, still deployed by
DAB. It exists for the topic that needs ONE setting to differ in ONE environment (a batch
size that only needs tuning in prod, say) without either duplicating the whole topic file per
environment or promoting a topic-specific value into environments/<env>.yaml, where it would
silently apply to every OTHER topic in that environment too.

It is genuinely rare. Reach for conf/environments/<env>.yaml first (a setting that is the
same for every topic in an environment) and for the operational control table second (a
setting that changes at runtime, no deploy). Use topic:.environments.<env> only when a value
is specific to BOTH one topic AND one environment, structurally, and does not belong in
either of those places.

LAYER NAMING - the framework's scope is Kafka -> landing -> curated. Nothing further.
    landing   ONE Delta table per topic, PARTITIONED BY (ingest_date).
              Raw wire bytes, zero interpretation of the payload.
    curated   ONE Delta table per topic, PARTITIONED BY (event_date).
              Parsed payload, kept NESTED in a single struct column.

TABLE NAMING - both layers are named from the Kafka topic, with dots and hyphens turned
into underscores because neither is legal in an unquoted Unity Catalog identifier:
    vector.patient.events.v1  ->  <catalog>.landing.vector_patient_events_v1
                                  <catalog>.curated.vector_patient_events_v1
The pattern lives in defaults/kafka.yaml as {catalog}.<layer>.{topic_table}. A topic that needs a
different name sets `table_name:` in its own file; everything else derives. See
table_name_for().

Design notes / edge cases handled here:
  * A missing operational row is NOT an error - it means "no overrides, run as coded".
    That keeps a newly onboarded topic runnable the moment its YAML merges.
  * Replay controls are validated as a *set*: offsets XOR timestamp, and a rerun_id is
    mandatory for any Kafka replay because rerun_id is what isolates the checkpoint.
  * Everything here is plain Python on driver-side metadata. No Spark data is touched,
    and nothing in this module imports PySpark - that is what keeps it testable in CI.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional

import yaml

# --------------------------------------------------------------------------------------
# Enumerated values. Kept as module constants rather than Enum classes so that the YAML
# and the Delta control table can hold plain strings that read the same in both places.
# --------------------------------------------------------------------------------------

RUN_TYPE_PRIMARY = "primary"
RUN_TYPE_KAFKA_REPLAY = "kafka_replay"
RUN_TYPE_CURATED_REPLAY = "curated_replay"
VALID_RUN_TYPES = {RUN_TYPE_PRIMARY, RUN_TYPE_KAFKA_REPLAY, RUN_TYPE_CURATED_REPLAY}

AUTH_SASL_PLAIN = "sasl_plain"
AUTH_SASL_SCRAM_256 = "sasl_scram_sha_256"
AUTH_SASL_SCRAM_512 = "sasl_scram_sha_512"
AUTH_MTLS = "mtls"
VALID_KAFKA_AUTH = {AUTH_SASL_PLAIN, AUTH_SASL_SCRAM_256, AUTH_SASL_SCRAM_512, AUTH_MTLS}

REGISTRY_AUTH_NONE = "none"
REGISTRY_AUTH_BASIC = "basic"
REGISTRY_AUTH_MTLS = "mtls"
VALID_REGISTRY_AUTH = {REGISTRY_AUTH_NONE, REGISTRY_AUTH_BASIC, REGISTRY_AUTH_MTLS}

# How the *reader* schema is chosen. The reader schema fixes the shape of the curated
# `payload` struct; the writer schema (always per-record, from the wire header) decides how
# the bytes are decoded.
#
# There is deliberately NO "use each writer schema as-is" mode. Curated keeps the payload
# as a single STRUCT column, and two writer schema versions produce two different struct
# types, which cannot be unioned into one table. A reader schema is what makes a
# mixed-version microbatch land in one consistent table. See docs/DESIGN.md.
READER_LATEST = "registry_latest"   # resolve subject's latest version once per run (default)
READER_PINNED = "pinned_id"         # freeze on an explicit schema id - contract freeze
VALID_READER_MODES = {READER_LATEST, READER_PINNED}

ON_ERROR_FAIL = "fail"              # platform default - loud failure
ON_ERROR_QUARANTINE = "quarantine"  # route bad records to a dead-letter table
VALID_ON_ERROR = {ON_ERROR_FAIL, ON_ERROR_QUARANTINE}

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# A Unity Catalog identifier that needs no backtick quoting.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Kafka topic separators that are not legal in an unquoted UC identifier.
_TOPIC_NAME_SEPARATORS = str.maketrans({".": "_", "-": "_"})


def table_name_for(topic: str) -> str:
    """Derive a table name from a Kafka topic name.

    Kafka topics conventionally use dots and hyphens (`vector.patient.events.v1`); neither is
    legal in an unquoted Unity Catalog identifier, so both become underscores:

        vector.patient.events.v1  ->  vector_patient_events_v1
        rcm-claim-status.v2       ->  rcm_claim_status_v2

    Case is left alone. UC folds identifiers to lower case itself, and changing it here would
    make the configured name and the catalogued name differ for no benefit.

    A topic whose name cannot survive this (leading digit, other punctuation) is a hard error
    rather than a silently mangled table - set `table_name:` in the topic file instead.
    """
    candidate = topic.translate(_TOPIC_NAME_SEPARATORS)
    if not _SAFE_TABLE_NAME.match(candidate):
        raise ConfigError(
            f"cannot derive a table name from topic '{topic}': '{candidate}' is not a valid "
            "Unity Catalog identifier. Set `table_name:` explicitly in the topic file."
        )
    return candidate


class ConfigError(ValueError):
    """Raised for any structurally invalid or internally inconsistent configuration."""


# --------------------------------------------------------------------------------------
# Structural profiles
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class KafkaClusterProfile:
    """One physical Kafka cluster. Multiple topics may share a profile; different domains
    may point at entirely different clusters with different auth postures."""

    name: str
    bootstrap_servers: str
    auth_mode: str
    secret_scope: Optional[str] = None          # Databricks secret scope backed by Azure Key Vault

    # SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512)
    sasl_username_key: Optional[str] = None
    sasl_password_key: Optional[str] = None

    # TLS material. Paths MUST be Unity Catalog Volume paths (/Volumes/<cat>/<sch>/<vol>/...).
    truststore_path: Optional[str] = None
    truststore_password_key: Optional[str] = None
    truststore_type: str = "JKS"
    keystore_path: Optional[str] = None
    keystore_password_key: Optional[str] = None
    key_password_key: Optional[str] = None
    keystore_type: str = "JKS"

    # Escape hatch for cluster-specific tuning (e.g. request.timeout.ms).
    # Keys are given WITHOUT the "kafka." prefix; security.py adds it.
    extra_options: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.auth_mode not in VALID_KAFKA_AUTH:
            raise ConfigError(
                f"cluster '{self.name}': auth_mode '{self.auth_mode}' not in {sorted(VALID_KAFKA_AUTH)}"
            )
        if self.auth_mode == AUTH_MTLS:
            if not (self.keystore_path and self.truststore_path):
                raise ConfigError(
                    f"cluster '{self.name}': mtls requires both keystore_path and truststore_path"
                )
        else:
            if not (self.secret_scope and self.sasl_username_key and self.sasl_password_key):
                raise ConfigError(
                    f"cluster '{self.name}': {self.auth_mode} requires secret_scope, "
                    "sasl_username_key and sasl_password_key"
                )
        for label, path in (("truststore_path", self.truststore_path),
                            ("keystore_path", self.keystore_path)):
            # UC-first governance: certs live in Volumes, never DBFS or /Workspace.
            if path and not path.startswith("/Volumes/"):
                raise ConfigError(
                    f"cluster '{self.name}': {label} must be a Unity Catalog Volume path "
                    f"(/Volumes/...), got '{path}'"
                )


@dataclass(frozen=True)
class SchemaRegistryProfile:
    """One Confluent Schema Registry instance. Auth here is deliberately independent of
    Kafka auth - a cluster using mTLS may still front a registry using basic auth."""

    name: str
    url: str
    auth_mode: str = REGISTRY_AUTH_NONE
    secret_scope: Optional[str] = None
    username_key: Optional[str] = None          # basic auth: "user" half of user-info
    password_key: Optional[str] = None          # basic auth: "secret" half
    client_cert_path: Optional[str] = None      # mTLS: PEM on a UC Volume
    client_key_path: Optional[str] = None
    ca_bundle_path: Optional[str] = None        # custom CA for a private registry
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
            raise ConfigError(f"registry '{self.name}': basic auth requires scope + username/password keys")
        if self.auth_mode == REGISTRY_AUTH_MTLS and not (self.client_cert_path and self.client_key_path):
            raise ConfigError(f"registry '{self.name}': mtls requires client_cert_path and client_key_path")
        for label, path in (("client_cert_path", self.client_cert_path),
                            ("client_key_path", self.client_key_path),
                            ("ca_bundle_path", self.ca_bundle_path)):
            if path and not path.startswith("/Volumes/"):
                raise ConfigError(f"registry '{self.name}': {label} must be a UC Volume path, got '{path}'")


# --------------------------------------------------------------------------------------
# Run context - what this particular execution is doing
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RunContext:
    """Distinguishes a normal daily run from a replay, and carries the replay bounds.

    `rerun_id` is the single most important field: it derives an isolated checkpoint
    sub-path, and it is stamped onto every landing/curated row written by the replay so
    downstream consumers can tell reprocessed data from primary-stream data.
    """

    run_type: str = RUN_TYPE_PRIMARY
    rerun_id: Optional[str] = None
    job_run_id: Optional[str] = None            # Databricks job run id, for monitor correlation

    # Kafka replay bounds
    starting_offsets: Optional[str] = None      # Spark startingOffsets JSON: {"topic":{"0":123}}
    starting_timestamp: Optional[str] = None    # ISO-8601 or epoch millis as string
    ending_offsets: Optional[str] = None        # bounded replay -> batch read, not streaming
    ending_timestamp: Optional[str] = None

    # Curated-only replay bounds (over the landing table, no broker involved)
    landing_filter: Optional[str] = None        # SQL predicate, e.g. "ingest_date = '2026-08-01'"

    @property
    def is_replay(self) -> bool:
        return self.run_type != RUN_TYPE_PRIMARY

    @property
    def is_bounded(self) -> bool:
        """A replay with an explicit end is executed as a bounded batch read.

        Rationale: Spark's Kafka *streaming* source has no endingOffsets - it always
        reads to latest. A support engineer replaying "just the bad two hours" needs a
        hard upper bound, so that case drops to spark.read instead of readStream.
        """
        return bool(self.ending_offsets or self.ending_timestamp)

    def validate(self) -> None:
        if self.run_type not in VALID_RUN_TYPES:
            raise ConfigError(f"run_type '{self.run_type}' not in {sorted(VALID_RUN_TYPES)}")
        if self.run_type == RUN_TYPE_KAFKA_REPLAY:
            if not self.rerun_id:
                raise ConfigError(
                    "kafka_replay requires a rerun_id - it is what isolates the replay "
                    "checkpoint from the primary checkpoint lineage."
                )
            if not (self.starting_offsets or self.starting_timestamp):
                raise ConfigError(
                    "kafka_replay requires exactly one of rerun_starting_offsets or "
                    "rerun_starting_timestamp."
                )
            if self.starting_offsets and self.starting_timestamp:
                raise ConfigError(
                    "kafka_replay: rerun_starting_offsets and rerun_starting_timestamp are "
                    "mutually exclusive - pick one."
                )
            if self.ending_offsets and self.ending_timestamp:
                raise ConfigError("kafka_replay: ending offsets and ending timestamp are mutually exclusive.")
            if self.starting_offsets:
                _require_offsets_json("rerun_starting_offsets", self.starting_offsets)
            if self.ending_offsets:
                _require_offsets_json("rerun_ending_offsets", self.ending_offsets)
        if self.run_type == RUN_TYPE_CURATED_REPLAY:
            if not self.rerun_id:
                raise ConfigError("curated_replay requires a rerun_id for monitoring and row provenance.")
            if not self.landing_filter:
                raise ConfigError(
                    "curated_replay requires a landing_filter predicate - refusing to "
                    "re-parse the entire landing history implicitly."
                )
        if self.rerun_id and not _SAFE_ID.match(self.rerun_id):
            raise ConfigError(
                f"rerun_id '{self.rerun_id}' must match [A-Za-z0-9_.-]{{1,64}} - it becomes a path segment."
            )


def _require_offsets_json(label: str, raw: str) -> None:
    """startingOffsets JSON must parse and look like {"topic": {"partition": offset}}."""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{label} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ConfigError(f"{label} must be a non-empty JSON object keyed by topic name.")
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            raise ConfigError(f"{label}: value for topic '{topic}' must be an object of partition->offset.")
        for part, off in partitions.items():
            if not str(part).lstrip("-").isdigit() or not isinstance(off, int):
                raise ConfigError(
                    f"{label}: topic '{topic}' partition '{part}' -> '{off}' is not an int offset "
                    "(-1 = latest, -2 = earliest)."
                )


# --------------------------------------------------------------------------------------
# The resolved, per-topic configuration the rest of the framework consumes
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class TopicConfig:
    # Identity
    topic_key: str                               # config key, e.g. "vector_patient_events"
    topic: str                                   # actual Kafka topic name
    domain: str                                  # vector | rcm | gma | antifraud | dwh_pes
    cluster: KafkaClusterProfile
    registry: SchemaRegistryProfile
    subject: str                                 # Schema Registry subject for the value

    # Targets (Unity Catalog 3-tier).
    # ONE TABLE PER TOPIC in both layers. The names come from defaults.yaml, which builds
    # them from {catalog} and {topic_table} - so a topic file never needs to name a table.
    landing_table: str
    curated_table: str
    quarantine_table: str
    audit_table: str

    # Streaming
    checkpoint_root: str                         # /Volumes/<cat>/<sch>/checkpoints
    consumer_group_prefix: str
    starting_offsets: str = "earliest"           # first-run position only; checkpoint wins after
    fail_on_data_loss: bool = True
    max_offsets_per_trigger: Optional[int] = None
    trigger: str = "availableNow"                # availableNow | processingTime=<interval> | once
    include_headers: bool = True                 # required for CloudEvent columns to populate

    # Deserialization
    reader_schema_mode: str = READER_LATEST
    reader_schema_id: Optional[int] = None
    on_deser_error: str = ON_ERROR_FAIL

    # Table layout. Both layers use Delta PARTITIONING (Delta allows PARTITIONED BY or
    # CLUSTER BY, never both - see docs/DESIGN.md).
    #   landing  (ingest_date) - one table per topic, so `topic` is constant within it and
    #            worthless as a key. ingest_date stops a partition growing without bound
    #            over years of daily runs, and makes retention a partition drop.
    #   curated  (event_date) - also one per topic; queries filter on when the event
    #            happened, not when we ingested it.
    landing_partition_by: List[str] = field(default_factory=lambda: ["ingest_date"])
    curated_partition_by: List[str] = field(default_factory=lambda: ["event_date"])
    curated_dedup_keys: List[str] = field(default_factory=list)
    curated_dedup_order_by: str = "kafka_timestamp"

    # Delta TBLPROPERTIES applied to every table this framework creates. Defaults come
    # from conf/defaults.yaml; a topic overrides only what it needs to differ on. Applied at
    # CREATE time only - changing this does NOT alter an existing table.
    table_properties: Dict[str, str] = field(default_factory=dict)

    # The table name both layers derive from, e.g. "vector.patient.events.v1" ->
    # "vector_patient_events_v1". Resolved by the loader: normally derived from the Kafka
    # topic name, or taken verbatim from `table_name:` in the topic file when one is set.
    # Carried on the config so it is visible in 00_validate_config and assertable in tests.
    table_name: str = ""

    # Operational
    enabled: bool = True
    environment: str = ""            # dev | preprod | prod - set by the loader, not by YAML

    # Deliberate override of guard_against_checkpoint_reset (pipeline.py). Control-table
    # ONLY - see the rejection in load_structural() - because it is a one-way safety bypass,
    # not a tunable, and a value checked into sources/<key>.yaml would apply on every future
    # deploy with no incident behind it. Setting it also changes the primary run's Delta
    # txnAppId (see _make_txn_app_id), which is what makes the restart actually safe rather
    # than just silencing the guard: a fresh identity has no prior committed versions to
    # collide with. See docs/RUNBOOK_SUPPORT.md 5.4a.
    checkpoint_reset_id: Optional[str] = None

    run: RunContext = field(default_factory=RunContext)

    def __post_init__(self) -> None:
        if self.reader_schema_mode not in VALID_READER_MODES:
            raise ConfigError(
                f"topic '{self.topic_key}': reader_schema_mode '{self.reader_schema_mode}' "
                f"not in {sorted(VALID_READER_MODES)}"
            )
        if self.reader_schema_mode == READER_PINNED and not self.reader_schema_id:
            raise ConfigError(
                f"topic '{self.topic_key}': reader_schema_mode=pinned_id requires reader_schema_id"
            )
        if self.on_deser_error not in VALID_ON_ERROR:
            raise ConfigError(f"topic '{self.topic_key}': on_deser_error invalid")
        for label, name in (("landing_table", self.landing_table), ("curated_table", self.curated_table),
                            ("quarantine_table", self.quarantine_table),
                            ("audit_table", self.audit_table)):
            if len(name.split(".")) != 3:
                raise ConfigError(
                    f"topic '{self.topic_key}': {label} must be a 3-tier UC name "
                    f"catalog.schema.table, got '{name}'"
                )
        if not self.checkpoint_root.startswith("/Volumes/"):
            raise ConfigError(
                f"topic '{self.topic_key}': checkpoint_root must be Volume-backed, got '{self.checkpoint_root}'"
            )
        if not self.landing_partition_by:
            # Landing is created by explicit DDL, so an empty list would produce an
            # unpartitioned table and make retention a full rewrite instead of a drop.
            raise ConfigError(
                f"topic '{self.topic_key}': landing_partition_by must name at least one "
                "column (normally ['ingest_date'])"
            )
        if not self.curated_partition_by:
            # A partition column is required: curated_writer partitions at write time, and
            # an empty list would silently produce an unpartitioned table.
            raise ConfigError(
                f"topic '{self.topic_key}': curated_partition_by must name at least one "
                "column (normally ['event_date'])"
            )
        self.run.validate()

    # -- derived -----------------------------------------------------------------------

    @property
    def checkpoint_path(self) -> str:
        """Primary and replay checkpoints are siblings, never nested inside one another.

        Resuming an existing checkpoint always beats any startingOffsets setting in
        Spark, so a replay MUST get a fresh location or the offset override is silently
        ignored. Keying that location on rerun_id makes replays repeatable (same
        rerun_id resumes the same replay) and collision-free.
        """
        base = f"{self.checkpoint_root.rstrip('/')}/{self.topic_key}"
        if self.run.run_type == RUN_TYPE_KAFKA_REPLAY:
            return f"{base}/replay/{self.run.rerun_id}"
        return f"{base}/primary"

    @property
    def group_id_prefix(self) -> str:
        # Spark's Kafka source manages its own consumer group; `groupIdPrefix` is the
        # only supported knob (`kafka.group.id` is overridden). Replays get their own
        # prefix so broker-side consumer metrics don't blend replay with primary.
        if self.run.is_replay:
            return f"{self.consumer_group_prefix}-{self.run.run_type}-{self.run.rerun_id}"
        return self.consumer_group_prefix

    @property
    def ingested_via(self) -> str:
        return self.run.run_type

    def with_run(self, run: RunContext) -> "TopicConfig":
        return replace(self, run=run)


# --------------------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------------------


def _read_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(f"structural config file not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a YAML mapping at the top level")
    return data


# Placeholders look like {catalog}. Only lower_snake_case names are recognised, so a
# stray brace in a value (e.g. a JSON fragment) is left alone rather than half-substituted.
_PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")


def _substitute(value: Any, scope: Dict[str, Any], where: str) -> Any:
    """Resolve {placeholder} tokens in strings, recursing into lists and dicts.

    An unresolved placeholder is a hard error naming the setting and the offending token.
    Letting it through would produce a table literally called `{catalog}.landing...`, which
    fails much later and much less clearly.
    """
    if isinstance(value, str):
        def _replace(match: "re.Match") -> str:
            name = match.group(1)
            if name not in scope:
                raise ConfigError(
                    f"{where}: '{value}' uses {{{name}}}, which is not defined. Available: "
                    f"{sorted(scope)}. Add it under `vars:` in the environment file."
                )
            return str(scope[name])
        return _PLACEHOLDER.sub(_replace, value)
    if isinstance(value, list):
        return [_substitute(item, scope, where) for item in value]
    if isinstance(value, dict):
        return {k: _substitute(v, scope, where) for k, v in value.items()}
    return value


def _overlay_profiles(base: Dict[str, Any], overrides: Dict[str, Any], register_file: str,
                      environment: str) -> Dict[str, Any]:
    """Merge per-environment profile overrides over the base register, key by key.

    A profile named in the environment file must already exist in the base file. That keeps
    clusters.yaml / registries.yaml the single answer to "which clusters do we consume
    from?", and turns a typo into an error instead of a silently-unused new profile.
    """
    merged = {name: dict(profile) for name, profile in base.items()}
    for name, override in (overrides or {}).items():
        if name not in merged:
            raise ConfigError(
                f"environments/{environment}.yaml overrides the profile '{name}', which is not "
                f"defined in {register_file} (known: {sorted(merged)}). Add it there first."
            )
        merged[name].update(override)
    return merged


def available_environments(config_root: str) -> List[str]:
    directory = os.path.join(config_root, "environments")
    if not os.path.isdir(directory):
        return []
    return sorted(f[:-5] for f in os.listdir(directory) if f.endswith(".yaml"))


def load_structural(config_root: str, topic_key: str, environment: str) -> Dict[str, Any]:
    """Merge the structural layers for one topic in one environment.

        1. defaults.yaml + defaults/kafka.yaml   common to everything / to Kafka
        2. environments/<env>.yaml          vars + defaults + cluster/registry overrides
        3. sources/<key>.yaml                what is unique to this topic
        3a. sources/<key>.yaml: environments.<env>   what is unique to this topic in ONE
                                             environment - optional, most topics omit it

    Later layers win per key; absent keys fall through. {placeholder} tokens are then
    resolved from the environment's `vars:` (plus topic_key and domain for topic settings) -
    AFTER 3a is folded in, so an environment-specific override can use them too.

    config_root is wherever DAB deployed conf/ - a /Workspace path or a UC Volume path.
    Both are readable with plain open() from the driver.
    """
    env_path = os.path.join(config_root, "environments", f"{environment}.yaml")
    if not os.path.exists(env_path):
        raise ConfigError(
            f"unknown environment '{environment}' - no {env_path}. "
            f"Available: {available_environments(config_root)}"
        )

    # Layer 1 is a pair: what is common to every source type, then what is common to every
    # Kafka source. This loader is Kafka-only, so it reads defaults/kafka.yaml by name; the
    # generic, spec-driven equivalent is framework/config.py, which resolves the file from
    # the source's declared source_type.
    defaults = {
        **(_read_yaml(os.path.join(config_root, "defaults.yaml")).get("defaults", {}) or {}),
        **(_read_yaml(os.path.join(config_root, "defaults", "kafka.yaml")).get("defaults", {}) or {}),
    }
    env_doc = _read_yaml(env_path)
    env_vars = env_doc.get("vars", {}) or {}
    env_defaults = {
        **(env_doc.get("defaults") or {}),
        **((env_doc.get("defaults_by_type") or {}).get("kafka") or {}),
    }
    topic_doc = _read_yaml(os.path.join(config_root, "sources", f"{topic_key}.yaml"))

    topic_raw = topic_doc.get("source")
    if not isinstance(topic_raw, dict):
        raise ConfigError(f"sources/{topic_key}.yaml: expected a top-level 'source:' mapping")

    # 3a. Optional per-environment override, nested inside the same topic file. Popped out
    # before the main merge so it never reaches the "unknown top-level key" check as a key
    # in its own right - only the settings inside it do, exactly like every other topic key.
    topic_env_overrides = topic_raw.pop("environments", None) or {}
    if not isinstance(topic_env_overrides, dict):
        raise ConfigError(
            f"sources/{topic_key}.yaml: 'environments:' must be a mapping of environment name "
            "-> override settings, e.g. 'environments: {prod: {max_offsets_per_trigger: ...}}'"
        )
    # Every key must name a real environment - this is the one place a typo here would
    # otherwise be silently unused rather than a startup error, because it only takes effect
    # when THAT environment happens to be the one being resolved.
    known_envs = set(available_environments(config_root))
    unknown_envs = set(topic_env_overrides) - known_envs
    if unknown_envs:
        raise ConfigError(
            f"sources/{topic_key}.yaml: environments block names {sorted(unknown_envs)}, which "
            f"{'is' if len(unknown_envs) == 1 else 'are'} not in Available: {sorted(known_envs)}"
        )
    this_env_override = topic_env_overrides.get(environment) or {}
    if not isinstance(this_env_override, dict):
        raise ConfigError(
            f"sources/{topic_key}.yaml: environments.{environment} must be a mapping of "
            "settings to override, not a scalar"
        )

    # Layers 1 -> 2 -> 3 -> 3a. Shallow per key: a list or scalar replaces wholesale rather
    # than merging, which is what makes an override predictable to read.
    merged: Dict[str, Any] = {
        **defaults, **env_defaults, **topic_raw, **this_env_override,
    }
    merged.setdefault("topic_key", topic_key)

    if merged.get("checkpoint_reset_id"):
        # A support-only, incident-scoped safety override belongs in the control table (layer
        # 4), never in Git: a value here would silently re-apply on every future deploy long
        # after the incident that justified it, defeating the guard for good.
        raise ConfigError(
            f"sources/{topic_key}.yaml: 'checkpoint_reset_id' is a control-table-only override "
            "and must not be set in topic YAML - see docs/RUNBOOK_SUPPORT.md 5.4a"
        )

    # Topic settings may use vars plus these two derived values.
    # {topic_table} is what defaults.yaml builds both layers' table names from. A topic file
    # may set `table_name:` to override the derivation; anything else derives from the Kafka
    # topic name. Resolved before substitution so the override reaches every derived name.
    topic_table = merged.get("table_name") or table_name_for(str(merged.get("topic", "")))
    merged["table_name"] = topic_table
    topic_scope = {
        **env_vars,
        "source_key": topic_key,
        "domain": merged.get("domain", ""),
        "topic_table": topic_table,
    }
    merged = _substitute(merged, topic_scope, f"sources/{topic_key}.yaml [{environment}]")

    clusters = _overlay_profiles(
        _read_yaml(os.path.join(config_root, "clusters.yaml")).get("clusters", {}),
        env_doc.get("clusters"), "clusters.yaml", environment,
    )
    registries = _overlay_profiles(
        _read_yaml(os.path.join(config_root, "registries.yaml")).get("registries", {}),
        env_doc.get("registries"), "registries.yaml", environment,
    )

    cluster_ref = merged.get("cluster")
    registry_ref = merged.get("registry")
    if cluster_ref not in clusters:
        raise ConfigError(
            f"topic '{topic_key}' references cluster '{cluster_ref}' which is not in clusters.yaml "
            f"(known: {sorted(clusters)})"
        )
    if registry_ref not in registries:
        raise ConfigError(
            f"topic '{topic_key}' references registry '{registry_ref}' which is not in registries.yaml "
            f"(known: {sorted(registries)})"
        )

    # Profiles are shared by many topics, so they get vars ONLY - substituting topic_key
    # into a cluster's cert path would silently produce a per-topic path, which is wrong.
    cluster_raw = _substitute(clusters[cluster_ref], env_vars, f"clusters.yaml/{cluster_ref}")
    registry_raw = _substitute(registries[registry_ref], env_vars, f"registries.yaml/{registry_ref}")

    merged["_cluster_profile"] = KafkaClusterProfile(name=cluster_ref, **cluster_raw)
    merged["_registry_profile"] = SchemaRegistryProfile(name=registry_ref, **registry_raw)
    merged["environment"] = environment
    return merged


# Columns the job reads from the operational control table. Anything else on that table
# (audit columns, free-text notes) is deliberately ignored by the job.
OPERATIONAL_OVERRIDE_FIELDS = (
    "enabled",
    "trigger",
    "max_offsets_per_trigger",
    "on_deser_error",
    "fail_on_data_loss",
    "reader_schema_mode",
    "reader_schema_id",
    "checkpoint_reset_id",
)

OPERATIONAL_RUN_FIELDS = {
    "rerun_starting_offsets": "starting_offsets",
    "rerun_starting_timestamp": "starting_timestamp",
    "rerun_ending_offsets": "ending_offsets",
    "rerun_ending_timestamp": "ending_timestamp",
    "rerun_id": "rerun_id",
}


def load_operational(spark, control_table: str, topic_key: str) -> Dict[str, Any]:
    """Read the single control row for this topic, if one exists.

    Returns {} when the table has no row for the topic - see module docstring for why
    that is a valid, non-error state.
    """
    if not spark.catalog.tableExists(control_table):
        # A fresh environment where the support table has not been created yet still runs.
        return {}
    # topic_key is validated against a strict charset before interpolation - it is a
    # config key from a deployed YAML filename, never free-form user input.
    rows = (
        spark.table(control_table)
        .where(f"topic_key = '{_require_safe_identifier(topic_key)}'")
        .limit(2)
        .collect()
    )
    if not rows:
        return {}
    if len(rows) > 1:
        raise ConfigError(
            f"{control_table} contains {len(rows)}+ rows for topic_key '{topic_key}'. "
            "The control table must hold exactly one row per topic - deduplicate it before rerunning."
        )
    return {k: v for k, v in rows[0].asDict().items() if v is not None}


def _require_safe_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z0-9_.-]+$", value):
        raise ConfigError(f"topic_key '{value}' contains characters that are not allowed in a config key")
    return value


def resolve_topic_config(
    spark,
    config_root: str,
    topic_key: str,
    control_table: str,
    environment: str,
    run_type: str = RUN_TYPE_PRIMARY,
    overrides: Optional[Dict[str, Any]] = None,
) -> TopicConfig:
    """Full five-layer resolution.

        defaults.yaml -> environments/<env>.yaml -> sources/<key>.yaml  (structural, PR)
            -> operational control table row -> job parameters          (operational)

    `overrides` are the job parameters a support engineer typed into the Workflows UI.
    They win over the control table so an urgent one-off replay does not require an UPDATE
    statement first - but the control table remains the durable place to park a setting
    that should persist across runs.
    """
    overrides = {k: v for k, v in (overrides or {}).items() if v not in (None, "")}
    structural = load_structural(config_root, topic_key, environment)
    operational = load_operational(spark, control_table, topic_key)

    cluster: KafkaClusterProfile = structural.pop("_cluster_profile")
    registry: SchemaRegistryProfile = structural.pop("_registry_profile")
    structural.pop("cluster", None)
    structural.pop("registry", None)

    merged: Dict[str, Any] = dict(structural)
    for key in OPERATIONAL_OVERRIDE_FIELDS:
        if key in operational:
            merged[key] = operational[key]
        if key in overrides:
            merged[key] = _coerce(key, overrides[key])

    # Replay controls: control table first, job parameters win.
    run_kwargs: Dict[str, Any] = {"run_type": run_type, "job_run_id": overrides.get("job_run_id")}
    for src_name, dest_name in OPERATIONAL_RUN_FIELDS.items():
        if src_name in operational:
            run_kwargs[dest_name] = operational[src_name]
        if src_name in overrides:
            run_kwargs[dest_name] = overrides[src_name]
        if dest_name in overrides:  # allow the shorter job-parameter spelling too
            run_kwargs[dest_name] = overrides[dest_name]
    if "landing_filter" in overrides:
        run_kwargs["landing_filter"] = overrides["landing_filter"]
    elif "curated_replay_landing_filter" in operational:
        run_kwargs["landing_filter"] = operational["curated_replay_landing_filter"]

    run = RunContext(**{k: v for k, v in run_kwargs.items() if v is not None or k == "run_type"})

    # Normalise scalar types that arrive as strings from job parameters.
    for bool_field in ("enabled", "fail_on_data_loss", "include_headers"):
        if bool_field in merged:
            merged[bool_field] = _as_bool(merged[bool_field])
    for int_field in ("max_offsets_per_trigger", "reader_schema_id"):
        if merged.get(int_field) is not None:
            merged[int_field] = int(merged[int_field])

    known = set(TopicConfig.__dataclass_fields__)  # type: ignore[attr-defined]
    unknown = set(merged) - known
    if unknown:
        raise ConfigError(
            f"sources/{topic_key}.yaml contains unknown keys {sorted(unknown)}. "
            "Typos here are silent misconfiguration - fix the YAML or extend TopicConfig."
        )

    return TopicConfig(cluster=cluster, registry=registry, run=run, **merged)


def _coerce(key: str, value: Any) -> Any:
    if key in {"enabled", "fail_on_data_loss", "include_headers"}:
        return _as_bool(value)
    if key in {"max_offsets_per_trigger", "reader_schema_id"}:
        return int(value)
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
