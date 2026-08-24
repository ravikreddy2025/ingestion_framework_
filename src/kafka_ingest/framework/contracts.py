"""The three dataclasses every source and every framework module agrees on.

Nothing else belongs in this file. It is deliberately the smallest module in the
framework: no helpers, no factories, no base classes, and NO PySpark import - a source's
`spec.py` imports `SourceSpec` from here, and `spec.py` must stay importable without a
cluster.

A source's ENTIRE public surface is:

    SOURCE_SPEC            data - what keys this source type accepts, and where
    run(ctx) -> RunResult  one function

There is deliberately no `read()` / `parse()` / `write()` / `validate()` in that contract.
A Kafka `foreachBatch` body and a bounded JDBC read share governance, not steps; any
step-level contract across them leaks the moment the second source is written. See
docs/build_log/STAGE_1_REPORT.md for the full argument.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True)
class SourceSpec:
    """What the framework needs to know about a source type to validate its config.

    This is DATA. The framework reads it; it never branches on `source_type`. Every
    guarantee config.py offers - unknown key, missing required key, mutually exclusive
    keys, structural-vs-operational separation - is driven entirely by these fields.

    structural_keys   settable in YAML layers 1-3 (Git, PR-reviewed)
    operational_keys  settable in the control table / job parameters (no deploy)

    The two sets overlap freely: a key in both can be set in YAML and overridden at
    runtime. The interesting cases are the keys in exactly one:

      structural only   an operational override of it is IGNORED (partitioning, merge
                        keys, target names - changing a table's physical layout needs a PR)
      operational only  setting it in YAML is an ERROR (an incident-scoped safety bypass
                        checked into Git would silently re-apply on every future deploy)

    target_tokens is the one field that is not about validation. It names the
    {placeholders} in this source type's configuration that only the SOURCE can fill -
    `{topic_table}` from a topic name, `{source_table}` from a database table - so
    config.py leaves them alone instead of failing on them, and tables.render() fills them
    at run time. Everything NOT named here still has to resolve at load, which is what
    keeps a genuine typo a startup error. See framework/tables.py.

    control_columns is the same idea applied to the operational control table
    (framework/control.py): the one shared `ingest_control` table holds every source
    type's operational levers side by side, so a column meaning ONE thing for every type
    stays unprefixed and framework-owned (`enabled`, `replay_rerun_id`), while a column
    specific to this source type is named `<source_type>_<setting>` and declared here,
    mapping the COLUMN NAME to the SETTING NAME it overrides. framework/control.py reads
    whatever a spec declares and never names a source type itself - see
    docs/build_log/DECISIONS.md D-01.
    """

    source_type: str
    required_keys: frozenset[str]
    structural_keys: frozenset[str]
    operational_keys: frozenset[str]
    mutually_exclusive: tuple[tuple[str, ...], ...]
    layers: tuple[str, ...]  # ("landing",) / ("landing", "curated", "quarantine")
    target_tokens: frozenset[str] = frozenset()
    control_columns: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True)
class RunContext:
    """Everything one run of one source needs, assembled once in framework/runner.py.

    Built in exactly one place. This is not a dependency-injection container and must not
    grow into one: if a source needs something that is not here, the honest fix is usually
    that the source should build it itself.

    cfg    the resolved configuration for this source. See framework/config.py.
    spark  the SparkSession. Typed Any so this module stays PySpark-free.
    """

    cfg: Any
    spark: Any
    audit: Any
    state: Any
    writers: Any
    tables: Any
    log: Any
    run_id: str
    run_type: str  # primary, or a source-specific replay type
    run_sequence: int


@dataclass(frozen=True)
class RunResult:
    """What a source reports back. The runner audits it; nothing else interprets it.

    position_start / position_end carry THREE different meanings depending on the source
    type - Kafka offsets JSON, an Oracle watermark, a file boundary - which is why they
    are text and why the audit table documents that on the column itself.

    source_detail is a JSON string, not a map: a new source type must never force an
    ALTER TABLE on the shared audit table.

    pending_work is what was STILL OUTSTANDING when the run ended - records not yet read,
    files not yet processed. It defaults to None and None is a legitimate answer: a source
    that cannot learn it without a second round trip to the system it just read must say
    so rather than report a confident zero, because a zero here reads as "fully caught up"
    and that is exactly the claim a permanently-lagging feed makes falsely.
    """

    rows_read: int
    rows_written: dict[str, int]  # layer -> count
    rows_quarantined: int
    position_start: str | None  # JSON or scalar, as text
    position_end: str | None
    source_detail: str | None  # JSON
    pending_work: int | None = None
