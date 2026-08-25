"""What the framework needs to know about a file source. NO PySpark import.

Every key below is READ by code in this package. That is the rule the file exists to
keep: a key listed here that nothing reads is a setting that silently does nothing, which
CORE section 2 rule 2 ranks as the worst possible output of this project. If you delete
the code that reads a key, delete the key.

STRUCTURAL vs OPERATIONAL, and the three interesting cases
----------------------------------------------------------
  in BOTH sets      settable in YAML and overridable at run time without a deploy.
                    `failure_mode` and `max_files_per_trigger` - a platform default that
                    support can move during an incident, exactly as Kafka's are.
  structural ONLY   an operational override is IGNORED. `storage_ref`, `source_path`,
                    `target_schema` / `target_table`, `landing_partition_by`: they decide
                    where this source reads from and where it writes, and CORE section 5.2
                    puts them behind a PR.
  operational ONLY  setting it in YAML is an ERROR. `checkpoint_reset_id`: incident-scoped,
                    and a value checked into Git would silently re-apply on every future
                    deploy, long after the incident that justified it - the same reasoning
                    as Kafka's.

THE FILE SOURCE IS CHECKPOINT-BASED (STAGE_5 brief, "Decision, already made"). It reuses
the SAME checkpoint-reset-id mechanism Kafka uses, not a second design - see the guard in
run.py. `checkpoint_root` is therefore required with no safe fallback, exactly like Kafka's.

`storage_ref` IS NOT IN `required_keys` (docs/build_log/DECISIONS.md D-13), even though it
IS in `structural_keys` and every shipped source today sets it. A `source_path` under
`/Volumes/<catalog>/<schema>/<volume>/...` is Unity-Catalog-governed and takes no storage
credentials at all, so it has nothing for `storage_ref` to name - a blanket `required_keys`
entry would make that legal, credential-free form fail with a missing-key error for the
wrong reason. `sources/file/config.py`'s `build()` enforces the real rule instead: exactly
one of `storage_ref` or a Volume-shaped `source_path`, depending on `source_path`'s own
shape - the same kind of cross-field rule that already lives there for `schema_mode` /
`schema`, because SourceSpec has no way to express "required only when a sibling key looks
like X."
"""

from __future__ import annotations

from ...framework.contracts import SourceSpec

# Operational-only: incident-scoped, single-use. See run.py's checkpoint-reset guard.
CHECKPOINT_RESET_ID = "checkpoint_reset_id"

# Settable in conf/ (layers 1-3). `landing_table` is not here: framework/config.py derives
# `<layer>_table` from SOURCE_SPEC.layers, because the framework - not this source -
# resolves, validates and creates it.
_STRUCTURAL = frozenset(
    {
        "storage_ref",
        "source_path",
        "path_glob",
        "file_format",
        "format_options",
        "schema_mode",
        "schema",
        "target_schema",
        "target_table",
        "landing_partition_by",
        "filename_columns",
        "checkpoint_root",
        "schema_location_root",
        "listing_mode",
        "max_files_per_trigger",
        "failure_mode",
    }
)

SOURCE_SPEC = SourceSpec(
    source_type="file",
    # Present in SOME layer, not necessarily the source's own file - most come from
    # conf/defaults/file.yaml. Every one of them has no safe fallback in code: an unset
    # `max_files_per_trigger` means one unbounded microbatch on the first run, exactly the
    # Kafka `max_offsets_per_trigger` hazard; an unset `schema_mode` would leave Auto Loader
    # to decide between inference and a provided schema with no explicit instruction at all.
    required_keys=frozenset(
        {
            "source_path",
            "path_glob",
            "file_format",
            "schema_mode",
            "target_schema",
            "target_table",
            "checkpoint_root",
            "schema_location_root",
            "listing_mode",
            "max_files_per_trigger",
            "failure_mode",
        }
    ),
    structural_keys=_STRUCTURAL,
    # The two standing levers, mirroring Kafka's shape exactly (docs/build_log/DECISIONS.md
    # D-01 names both control columns up front). Neither changes WHICH files are read - only
    # how hard the read leans on a batch, or what happens to a row Auto Loader could not fit
    # the schema - which is what makes them safe to turn without a PR.
    operational_keys=frozenset({"failure_mode", "max_files_per_trigger", CHECKPOINT_RESET_ID}),
    mutually_exclusive=(),
    # CORE section 10: files land only. A curated layer for Files is out of scope.
    layers=("landing",),
    # The landing target is named from the operator's own choice of schema/table - a file
    # feed has no source-side schema/table the way Oracle's does, so these are literal
    # settings rather than a derivation, but they still keep {catalog} out of the source
    # file: only sources/file/config.py fills them into the pattern in
    # conf/defaults/file.yaml, exactly as sources/oracle/config.py fills {source_schema} /
    # {source_table}.
    target_tokens=frozenset({"target_schema", "target_table"}),
    # This source type's own columns on the one shared control table
    # (docs/build_log/DECISIONS.md D-01): column name -> the setting it overrides.
    control_columns={
        "file_failure_mode": "failure_mode",
        "file_max_files_per_trigger": "max_files_per_trigger",
        "file_checkpoint_reset_id": CHECKPOINT_RESET_ID,
    },
)
