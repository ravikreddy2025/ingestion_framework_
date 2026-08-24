"""The run lifecycle, and the only place in framework/ that knows a source type's name.

    read the control table -> resolve config -> validate target names -> ensure the
    framework's own tables -> allocate a run sequence -> build RunContext -> dispatch to
    the source -> audit -> return

That is the whole file. Everything a source does is behind `module.run(ctx)`; everything
the framework guarantees - config validation, audit rows, the disabled short-circuit -
happens here and nowhere else, so a support engineer reading one screen can tell what any
run did before and after the source-specific part.

The dispatch is a module-level dict literal. Not a registry class, not entry-point
discovery, not a dynamic import by string: three sources do not justify any of them, and
the dict is the one artefact that makes "which source types exist?" answerable by reading
rather than by tracing (CORE section 7).
"""

from __future__ import annotations

import uuid
from typing import Any, Mapping

from ..sources import file as file_source  # imported only to build _SOURCES, below
from ..sources import kafka, oracle  # imported only to build _SOURCES, below
from . import audit as audit_module
from . import control as control_module
from . import state as state_module
from . import tables, writers
from .config import ConfigError, ResolvedConfig, load_structural, read_source_type, resolve_config
from .contracts import RunContext, RunResult, SourceSpec
from .logs import RunLog

_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}

# Every source type's own control-table columns, keyed by column name -> the source_type
# that owns it. Built once, here, because this is the one place in framework/ allowed to
# know source types by name; framework/control.py receives this as plain data and uses it
# only to catch a column set for the wrong source type (docs/build_log/DECISIONS.md D-01).
_CONTROL_COLUMN_OWNERS = {
    column: source_type for source_type, module in _SOURCES.items() for column in module.SOURCE_SPEC.control_columns
}

# The framework interprets exactly one run_type. Everything else is a source-specific
# replay shape (a Kafka offset replay, a database window re-extract, a file re-read), which
# only that source knows how to execute - so the framework carries the string and does not
# enumerate the values.
RUN_TYPE_PRIMARY = "primary"


def run(
    source_key: str,
    environment: str,
    config_root: str,
    run_type: str = RUN_TYPE_PRIMARY,
    job_parameters: Mapping[str, Any] | None = None,
    control: Mapping[str, Any] | None = None,
    job_run_id: str | None = None,
    spark: Any = None,
) -> RunResult:
    """Resolve, dispatch, audit. Every entrypoint calls exactly this.

    `control` is the layer-4 override dict. Pass it to supply the overrides directly (a
    notebook, a test); leave it None and the control table named by the `control_table`
    setting is read instead. Either way config.py never touches a table itself.

    `job_parameters` is layer 5, and every key in it is a CONFIG SETTING validated against
    the source's spec. `job_run_id` is deliberately a separate argument rather than one
    more key in that dict: it identifies the run, it is not a setting, and letting it ride
    along would mean the spec had to declare a key no source ever reads.
    """
    spec = _spec_for(config_root, source_key)
    if control is None:
        spark = _session(spark)
        control = _read_control(spark, config_root, source_key, environment, spec)

    cfg = resolve_config(
        config_root,
        source_key,
        environment,
        spec,
        control=control,
        job_parameters=job_parameters or {},
    )
    run_id = _make_run_id(source_key, run_type, job_run_id)
    log = RunLog(cfg.source_type, source_key, run_id)

    if not cfg.enabled:
        # Silence would be indistinguishable from a broken scheduler, so a disabled source
        # still leaves a trace. Disabling is the emergency stop: it stops replays too, and
        # it takes effect before this run builds a session of its own.
        log.warning("run_skipped", reason="disabled in configuration", environment=environment)
        _audit(
            log, _audit_writer(spark, cfg, run_id, run_type, job_run_id), audit_module.STATUS_SKIPPED, run_type=run_type
        )
        return RunResult(0, {}, 0, None, None, None)

    # Names are validated BEFORE anything connects: the failure this catches is a name that
    # is legal in the system being read and illegal in Unity Catalog, and finding that out
    # after an hour-long read is finding it out too late.
    tables.validate_targets(cfg)

    spark = _session(spark)
    audit = _audit_writer(spark, cfg, run_id, run_type, job_run_id)
    state = state_module.StateStore(spark, cfg.get("state_table"), run_id)
    _ensure_framework_tables(spark, cfg)

    ctx = RunContext(
        cfg=cfg,
        spark=spark,
        audit=audit,
        state=state,
        # The framework's write and DDL helpers, as modules. There is no wrapper object
        # because there is nothing to wrap: every function in them takes what it needs as
        # an argument. Carrying them on the context is what makes `ctx` the one thing a
        # source author has to understand.
        writers=writers,
        tables=tables,
        log=log,
        run_id=run_id,
        run_type=run_type,
        # Allocated for every run, not just the sources that need it. The alternative is a
        # branch on source type in framework/, which is the one thing this design forbids -
        # and the cost is one upsert on a table with one row per source.
        run_sequence=state.next_run_sequence(source_key),
    )

    _audit(
        log, audit, audit_module.STATUS_STARTED, run_type=run_type, environment=environment, layers=",".join(cfg.layers)
    )
    try:
        result = _SOURCES[cfg.source_type].run(ctx)
    except Exception as exc:  # blind catch: record that the run failed, then re-raise
        _audit(log, audit, audit_module.STATUS_FAILED, error_class=type(exc).__name__, error_message=str(exc))
        raise
    _audit_result(log, audit, result)
    return result


def _audit_writer(
    spark: Any, cfg: ResolvedConfig, run_id: str, run_type: str, job_run_id: str | None
) -> audit_module.AuditWriter:
    """One construction, two call sites - the disabled short-circuit and the real run."""
    return audit_module.AuditWriter(spark, cfg, cfg.get("audit_table"), run_id, run_type, job_run_id=job_run_id)


def _spec_for(config_root: str, source_key: str) -> SourceSpec:
    """Read the declared source type and pick its spec.

    Done before anything else, because the spec is what decides which keys are valid - in
    the YAML layers AND in the control table. Resolving first and discovering the source
    type afterwards would mean validating before knowing what is valid.
    """
    source_type = read_source_type(config_root, source_key)
    module = _SOURCES.get(source_type)
    if module is None:
        raise ConfigError(
            f"sources/{source_key}.yaml declares source_type '{source_type}', which no source "
            f"package implements (known: {sorted(_SOURCES)}). Add a package under sources/ and "
            "one entry to _SOURCES in framework/runner.py."
        )
    return module.SOURCE_SPEC


def _read_control(
    spark: Any, config_root: str, source_key: str, environment: str, spec: SourceSpec
) -> Mapping[str, Any]:
    """Where is the control table, and what does it say about this source?

    The table's name is itself a structural setting, so layers 1-3 are merged once to find
    it before they are merged again with the overrides applied. Two passes over a few small
    YAML files is the price of having exactly one mechanism - conf/ - for naming things.
    """
    structural = load_structural(config_root, source_key, environment, spec.target_tokens)
    control_table = structural.get("control_table")
    if not control_table:
        return {}
    return control_module.read_control(spark, control_table, source_key, spec, _CONTROL_COLUMN_OWNERS)


def _ensure_framework_tables(spark: Any, cfg: ResolvedConfig) -> None:
    """Create the audit and state tables if they are not there. A no-op once they are.

    The framework's OWN two tables only. A source's layer tables are created by the source,
    because curated-style layers have a schema that is not known until the run has resolved
    it. Schemas and GRANTs are provisioning, and live in sql/ - see framework/tables.py.
    """
    audit_module.ensure_audit_table(spark, cfg, cfg.get("audit_table"))
    state_module.ensure_state_table(spark, cfg, cfg.get("state_table"))


def _make_run_id(source_key: str, run_type: str, job_run_id: Any = None) -> str:
    """Unique per execution. Stamped on every data row and every audit row.

    Prefers the Databricks job run id when there is one, so a support engineer can get from
    a Workflows run straight to its audit rows without a join through timestamps.
    """
    suffix = str(job_run_id) if job_run_id else uuid.uuid4().hex[:12]
    return f"{source_key}-{run_type}-{suffix}"


def _audit(log: RunLog, audit: Any, status: str, **fields: Any) -> None:
    """The four run-level audit call sites, in one place: log the line, write the row.

    Both, not either: the log line is what a driver-log grep finds during the incident, and
    the row is what a SQL query finds afterwards. `audit.emit` never raises, so this
    function returns no matter what - which is what lets it sit on the failure path.
    """
    log.info("run_status", status=status, **fields)
    audit.emit(audit_module.LAYER_RUN, status, **_audit_fields(fields))


def _audit_result(log: RunLog, audit: Any, result: RunResult) -> None:
    """One row per layer the source wrote, then the run-level row.

    Per-layer first: if the driver dies between them, what is already durable is the
    detail, and the run-level row's absence is itself the signal that the run did not
    finish.
    """
    for layer, count in (result.rows_written or {}).items():
        audit.emit(layer, audit_module.STATUS_COMPLETED, record_count=count)
    _audit(
        log,
        audit,
        audit_module.STATUS_COMPLETED,
        rows_read=result.rows_read,
        rows_written=result.rows_written,
        rows_quarantined=result.rows_quarantined,
        position_start=result.position_start,
        position_end=result.position_end,
        source_detail=result.source_detail,
        pending_work=result.pending_work,
    )


# Fields the log line and the audit row spell differently. The log line describes the run
# in the run's own words; the audit table has one column name per concept across every
# source type. Anything not named here is for the log only.
_AUDIT_FIELD_NAMES = {
    "rows_read": "record_count",
    "rows_quarantined": "quarantined_count",
    "position_start": "position_start",
    "position_end": "position_end",
    "source_detail": "source_detail",
    "pending_work": "pending_work",
    "error_class": "error_class",
    "error_message": "error_message",
}


def _audit_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    return {column: fields[name] for name, column in _AUDIT_FIELD_NAMES.items() if name in fields}


def _session(spark: Any) -> Any:
    return spark if spark is not None else _active_spark()


def _active_spark() -> Any:
    """Imported inside the function on purpose.

    framework/runner.py has no module-level PySpark import, so the whole lifecycle -
    dispatch, the disabled short-circuit, run-id derivation - is testable in the fast suite
    with a stand-in session.
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
