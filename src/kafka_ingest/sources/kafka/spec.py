"""What the framework needs to know about a Kafka source. NO PySpark import.

Every key below is READ by code in this package. That is the rule the file exists to
keep: a key listed here that nothing reads is a setting that silently does nothing, which
CORE section 2 rule 2 ranks as the worst possible output of this project. If you delete
the code that reads a key, delete the key.

STRUCTURAL vs OPERATIONAL, and the three interesting cases
----------------------------------------------------------
  in BOTH sets      settable in YAML and overridable at run time without a deploy.
                    `failure_mode` and `max_offsets_per_trigger` - a platform default that
                    support can move during an incident.
  structural ONLY   an operational override is IGNORED. Partitioning, dedup keys, target
                    names, the topic itself: they describe what is already on disk or
                    which feed this is, and changing either needs a PR.
  operational ONLY  setting it in YAML is an ERROR. `checkpoint_reset_id` and every
                    `replay_*` control: each is incident-scoped, and a value checked into
                    Git would silently re-apply on every future deploy, long after the
                    incident that justified it.
"""

from __future__ import annotations

from ...framework.contracts import SourceSpec

# Replay bounds and the reset id, all operational-only. Named `replay_*` rather than
# reusing `starting_offsets` because that key already means something else and permanently:
# the FIRST-RUN position of the primary stream. One name, one meaning.
REPLAY_STARTING_OFFSETS = "replay_starting_offsets"
REPLAY_STARTING_TIMESTAMP = "replay_starting_timestamp"
REPLAY_ENDING_OFFSETS = "replay_ending_offsets"
REPLAY_ENDING_TIMESTAMP = "replay_ending_timestamp"
REPLAY_LANDING_FILTER = "replay_landing_filter"
CHECKPOINT_RESET_ID = "checkpoint_reset_id"

_REPLAY_KEYS = frozenset(
    {
        REPLAY_STARTING_OFFSETS,
        REPLAY_STARTING_TIMESTAMP,
        REPLAY_ENDING_OFFSETS,
        REPLAY_ENDING_TIMESTAMP,
        REPLAY_LANDING_FILTER,
    }
)

# Settable in conf/ (layers 1-3). `<layer>_table` is not here: framework/config.py derives
# those from SOURCE_SPEC.layers, because the framework - not this source - resolves,
# validates and creates them.
_STRUCTURAL = frozenset(
    {
        "topic",
        "cluster",
        "registry",
        "subject",
        "checkpoint_root",
        "consumer_group_prefix",
        "starting_offsets",
        "trigger",
        "fail_on_data_loss",
        "min_partitions",
        "max_offsets_per_trigger",
        "reader_schema_mode",
        "reader_schema_id",
        "failure_mode",
        "landing_partition_by",
        "curated_partition_by",
        "curated_dedup_keys",
        "curated_dedup_order_by",
        "table_name",
    }
)

SOURCE_SPEC = SourceSpec(
    source_type="kafka",
    # Present in SOME layer, not necessarily the topic file - most come from
    # conf/defaults/kafka.yaml. This set is the safety net for a default someone deleted:
    # every one of them has no safe fallback in code, so a missing value must stop the run
    # rather than be guessed at. Note `min_partitions` and `max_offsets_per_trigger` are
    # here deliberately - an unset maxOffsetsPerTrigger means one unbounded microbatch.
    required_keys=frozenset(
        {
            "topic",
            "cluster",
            "registry",
            "subject",
            "checkpoint_root",
            "consumer_group_prefix",
            "starting_offsets",
            "trigger",
            "fail_on_data_loss",
            "min_partitions",
            "max_offsets_per_trigger",
            "reader_schema_mode",
            "failure_mode",
            "landing_partition_by",
            "curated_partition_by",
        }
    ),
    structural_keys=_STRUCTURAL,
    # Three standing levers plus the replay controls. The first two are also structural, so
    # they have a reviewed default AND an incident override; the rest are operational-only.
    operational_keys=frozenset({"failure_mode", "max_offsets_per_trigger", CHECKPOINT_RESET_ID}) | _REPLAY_KEYS,
    mutually_exclusive=(
        (REPLAY_STARTING_OFFSETS, REPLAY_STARTING_TIMESTAMP),
        (REPLAY_ENDING_OFFSETS, REPLAY_ENDING_TIMESTAMP),
    ),
    # Kafka is the only source with all three layers: raw wire bytes land, the payload is
    # parsed into curated, and records that cannot be parsed go to quarantine.
    layers=("landing", "curated", "quarantine"),
    # {topic_table} is the topic name with dots and hyphens turned into underscores - a
    # value only this source can compute, so config load leaves it alone and
    # framework/tables.py renders it when run() supplies the token.
    target_tokens=frozenset({"topic_table"}),
    # This source type's own columns on the one shared control table
    # (docs/build_log/DECISIONS.md D-01): column name -> the setting it overrides. Every
    # setting on the right is in `operational_keys` above, which is what makes the column
    # do something rather than fail with an unknown-key error.
    control_columns={
        "kafka_failure_mode": "failure_mode",
        "kafka_max_offsets_per_trigger": "max_offsets_per_trigger",
        "kafka_checkpoint_reset_id": CHECKPOINT_RESET_ID,
    },
)
