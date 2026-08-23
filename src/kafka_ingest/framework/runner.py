"""The run lifecycle, and the only place in framework/ that knows a source type's name.

    resolve config -> build RunContext -> dispatch to the source -> audit -> return

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
from .config import ConfigError, ResolvedConfig, read_source_type, resolve_config
from .contracts import RunContext, RunResult
from .logs import RunLog

_SOURCES = {"kafka": kafka, "oracle": oracle, "file": file_source}

# The framework interprets exactly one run_type. Everything else is a source-specific
# replay shape (a Kafka offset replay, an Oracle window re-extract, a file re-read), which
# only that source knows how to execute - so the framework carries the string and does not
# enumerate the values.
RUN_TYPE_PRIMARY = "primary"

# Audit and state are Stage 2. Until then a run still has a run_sequence field, because
# RunContext is frozen and sources are written against it now.
_RUN_SEQUENCE_UNSET = 0


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

    `control` is the layer-4 override dict. Stage 2's framework/control.py reads it from
    {ops_catalog}.ingest_control; passing it in keeps this function - and config.py -
    free of any table read, and makes the whole lifecycle testable with no Spark.

    `job_parameters` is layer 5, and every key in it is a CONFIG SETTING validated against
    the source's spec. `job_run_id` is deliberately a separate argument rather than one
    more key in that dict: it identifies the run, it is not a setting, and letting it ride
    along would mean the spec had to declare a key no source ever reads.
    """
    cfg = _resolve(source_key, environment, config_root, control, job_parameters or {})
    run_id = _make_run_id(source_key, run_type, job_run_id)
    log = RunLog(cfg.source_type, source_key, run_id)

    if not cfg.enabled:
        # Silence would be indistinguishable from a broken scheduler, so a disabled source
        # still leaves a trace. Disabling is the emergency stop: it stops replays too.
        log.warning("run_skipped", reason="disabled in configuration", environment=environment)
        _audit(log, "SKIPPED", run_type=run_type)
        return RunResult(0, {}, 0, None, None, None)

    ctx = RunContext(
        cfg=cfg,
        spark=spark if spark is not None else _active_spark(),
        # Stage 2 fills these four in. The call sites below already exist, so that stage is
        # a fill-in rather than a restructure.
        audit=None,
        state=None,
        writers=None,
        tables=None,
        log=log,
        run_id=run_id,
        run_type=run_type,
        run_sequence=_RUN_SEQUENCE_UNSET,
    )

    _audit(log, "STARTED", run_type=run_type, environment=environment, layers=",".join(cfg.layers))
    try:
        result = _SOURCES[cfg.source_type].run(ctx)
    except Exception as exc:  # blind catch: record that the run failed, then re-raise
        _audit(log, "FAILED", error_class=type(exc).__name__, error_message=str(exc))
        raise
    _audit(
        log,
        "COMPLETED",
        rows_read=result.rows_read,
        rows_written=result.rows_written,
        rows_quarantined=result.rows_quarantined,
        position_start=result.position_start,
        position_end=result.position_end,
    )
    return result


def _resolve(
    source_key: str,
    environment: str,
    config_root: str,
    control: Mapping[str, Any] | None,
    job_parameters: Mapping[str, Any],
) -> ResolvedConfig:
    """Read the declared source type, pick its spec, then validate against it.

    Two reads of one small YAML file. The alternative - resolving first and discovering the
    source type afterwards - would mean validating before knowing what is valid.
    """
    source_type = read_source_type(config_root, source_key)
    module = _SOURCES.get(source_type)
    if module is None:
        raise ConfigError(
            f"sources/{source_key}.yaml declares source_type '{source_type}', which no source "
            f"package implements (known: {sorted(_SOURCES)}). Add a package under sources/ and "
            "one entry to _SOURCES in framework/runner.py."
        )
    return resolve_config(
        config_root,
        source_key,
        environment,
        module.SOURCE_SPEC,
        control=control,
        job_parameters=job_parameters,
    )


def _make_run_id(source_key: str, run_type: str, job_run_id: Any = None) -> str:
    """Unique per execution. Stamped on every data row and every audit row.

    Prefers the Databricks job run id when there is one, so a support engineer can get from
    a Workflows run straight to its audit rows without a join through timestamps.
    """
    suffix = str(job_run_id) if job_run_id else uuid.uuid4().hex[:12]
    return f"{source_key}-{run_type}-{suffix}"


def _audit(log: RunLog, status: str, **fields: Any) -> None:
    """The three audit call sites, in one place.

    STAGE 1: logs only. Stage 2 replaces this body with a framework/audit.py write - which
    must never raise, so this function's contract of "returns no matter what" is already
    the right one.
    """
    log.info("run_status", status=status, **fields)


def _active_spark() -> Any:
    """Imported inside the function on purpose.

    framework/runner.py has no module-level PySpark import, so the whole lifecycle -
    dispatch, the disabled short-circuit, run-id derivation - is testable in the fast suite
    with a stand-in session.
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
