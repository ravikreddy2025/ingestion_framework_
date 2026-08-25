"""The file source's one function, and everything one run of it does.

    readStream(cloudFiles)
        +-- foreachBatch(batch_df, batch_id)
              +-- audit landing STARTED
              +-- project + write LANDING     (source columns, verbatim, plus metadata)
              +-- audit landing COMPLETED, with the rescued-row count

There is no curated layer and no quarantine TABLE for this source (CORE section 10).
`cloudFiles.rescuedDataColumn` IS this source's quarantine: a row whose fields did not fit
the expected schema still lands, with whatever did not fit captured in `_rescued_data`
rather than dropped, and its non-NULL count is reported the way Kafka's quarantine count is
- a rising trend is the file equivalent of a rising quarantine trend. `failure_mode`
decides what a NON-ZERO rescued count means for the run itself: FAILFAST raises rather than
land a batch quietly holding malformed rows; QUARANTINE lands it and only reports the count.

THE FILE SOURCE IS CHECKPOINT-BASED (STAGE_5 brief, "Decision, already made"), so it takes
on Kafka's checkpoint-reset guard exactly - not a second design. `_guard_against_
checkpoint_reset` below mirrors sources/kafka/run.py's function of the same shape, adapted
to this source's own fields: no `topic` filter is needed on the "already landed" check,
because this source's landing table belongs to exactly one file source, unlike Kafka's
shared per-cluster checkpoint namespace.

`cloudFiles.schemaLocation` IS COVERED BY THE SAME GUARD AS THE STREAM CHECKPOINT
-----------------------------------------------------------------------------------
Both live under the same Volume root per source_key (`checkpoint_path` and
`schema_location_path` are siblings - see sources/file/config.py), so a checkpoint reset
that forks the stream checkpoint's parent directory also leaves the OLD schema location
in place, untouched, under the old `source_key` path - the reset does not delete it, and
does not need to: a fresh stream re-infers or re-applies the configured schema into
whatever is at `schema_location_path` for THIS `source_key`, which is unaffected by the
reset id. Recorded as a design decision in docs/DESIGN.md; see VB-06 for whether schema
inference itself behaves as expected on the target runtime, and VB-26 for the ADLS session-
option mechanism this run() applies before reading.
"""

from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any

from ...framework import audit as audit_module
from ...framework.contracts import RunContext, RunResult
from ...framework.security import SecretResolver, apply_session_options
from . import config as file_config
from . import landing, security, tables
from .config import RUN_TYPE_PRIMARY, FileConfig
from .reader import build_stream_reader

LAYER_LANDING = "landing"


def run(ctx: RunContext, secrets: Any = None) -> RunResult:
    """Resolve, guard, read, write, report. The WHOLE contract of this source.

    `secrets` is an optional injection point and not part of the contract: a run builds its
    own SecretResolver, and a test supplies a stand-in rather than a workspace.
    """
    cfg = file_config.build(ctx.cfg, ctx.run_type, ctx.tables)
    ctx.audit.source_ref = cfg.full_source_path
    ctx.log.info(
        "file_run_resolved",
        source_path=cfg.full_source_path,
        storage=cfg.storage_ref,
        landing=cfg.landing_table,
        checkpoint=cfg.checkpoint_path,
        schema_location=cfg.schema_location_path,
        txn_app_id=cfg.txn_app_id,
        failure_mode=cfg.failure_mode,
    )

    # BEFORE the reset id reaches the audit writer - see sources/kafka/run.py's guard for
    # why the ordering is belt and braces rather than the only thing keeping it honest.
    _guard_against_checkpoint_reset(ctx, cfg)
    if cfg.checkpoint_reset_id:
        ctx.audit.rerun_id = cfg.checkpoint_reset_id

    state = _RunState(cfg=cfg)
    session = _Session(ctx=ctx, cfg=cfg, state=state, secrets=secrets or SecretResolver())
    session.run_streaming()

    return RunResult(
        rows_read=state.rows_read,
        rows_written=dict(state.rows_written),
        rows_quarantined=state.rows_quarantined,
        position_start=state.position_start,
        position_end=state.position_end,
        source_detail=cfg.source_detail(),
        # Not derivable cheaply from Auto Loader's own progress payload without guessing at
        # a shape this project has not verified - see the module docstring's VB-26 pointer.
        # None reads as "not known"; a confident 0 would read as "fully caught up", which is
        # exactly the false claim a permanently-lagging feed would then be able to make.
        pending_work=None,
    )


@dataclass
class _RunState:
    """What this run has done so far. The only mutable thing in the package."""

    cfg: FileConfig
    rows_read: int = 0
    rows_written: dict = field(default_factory=dict)
    rows_quarantined: int = 0
    position_start: str | None = None
    position_end: str | None = None

    def record(self, layer: str, count: int) -> None:
        self.rows_written[layer] = self.rows_written.get(layer, 0) + count


@dataclass
class _Session:
    ctx: RunContext
    cfg: FileConfig
    state: _RunState
    secrets: Any

    def run_streaming(self) -> None:
        """Apply this source's ADLS session options for the duration of the query, then
        put them back - see framework/security.py `apply_session_options`."""
        storage_options = security.build_storage_options(self.cfg.storage, self.secrets)
        restore = apply_session_options(self.ctx.spark, storage_options)
        try:
            query = (
                build_stream_reader(self.ctx.spark, self.cfg)
                .writeStream.foreachBatch(lambda df, batch_id: self.process_microbatch(df, batch_id))
                .queryName(f"file_ingest::{self.cfg.source_key}::{self.cfg.run_type}")
                .option("checkpointLocation", self.cfg.checkpoint_path)
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()
            self._record_positions(query)
        finally:
            restore()

    def process_microbatch(self, batch_df: Any, txn_version: int) -> None:
        """Land one microbatch. Raises on any failure so Structured Streaming does not
        commit the batch - the FAILED audit row is what turns triage into a lookup."""
        from pyspark.storagelevel import StorageLevel

        started = time.time()
        landing_df = None
        try:
            self.ctx.audit.emit(LAYER_LANDING, audit_module.STATUS_STARTED, txn_version)

            landing_df = landing.project(batch_df, self.cfg, txn_version, self.ctx.run_id).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            landing_count = landing_df.count()

            if landing_count == 0:
                # Trigger.AvailableNow emits a final empty batch on every run.
                self.ctx.audit.emit(LAYER_LANDING, audit_module.STATUS_NO_DATA, txn_version, record_count=0)
                return

            rescued = landing.rescued_count(landing_df)
            if rescued and not self.cfg.quarantine_on_error:
                raise RuntimeError(
                    f"{rescued} row(s) in this batch did not fit the configured schema for "
                    f"'{self.cfg.source_key}' ({RESCUED_DATA_HINT}). failure_mode is FAILFAST, so the "
                    "batch is refused rather than landed with malformed rows silently mixed in. "
                    "Set file_failure_mode to QUARANTINE to land it and investigate the "
                    "_rescued_data column afterwards."
                )

            tables.ensure_landing_table(self.ctx, self.cfg, batch_df.schema)
            self.ctx.writers.append(
                landing_df,
                self.cfg.landing_table,
                txn_app_id=self.cfg.txn_app_id,
                txn_version=txn_version,
                partition_by=list(self.cfg.landing_partition_by),
            )

            self.state.rows_read += landing_count
            self.state.record(LAYER_LANDING, landing_count)
            if rescued:
                self.state.rows_quarantined += rescued
                self.ctx.log.warning(
                    "file_rows_rescued",
                    source_key=self.cfg.source_key,
                    txn_version=txn_version,
                    rescued=rescued,
                )
            self.ctx.audit.emit(
                LAYER_LANDING,
                audit_module.STATUS_COMPLETED,
                txn_version,
                record_count=landing_count,
                quarantined_count=rescued,
                duration_ms=_elapsed_ms(started),
            )
        except Exception as exc:  # blind catch: record the failure, then re-raise
            self.ctx.audit.emit(
                LAYER_LANDING,
                audit_module.STATUS_FAILED,
                txn_version,
                error_class=type(exc).__name__,
                error_message=f"{exc}\n{traceback.format_exc()}",
            )
            raise
        finally:
            # In a `finally`, not on the success path only: a batch that keeps failing is
            # exactly the one that would otherwise leak a cache on every retry.
            if landing_df is not None:
                landing_df.unpersist()

    def _record_positions(self, query: Any) -> None:
        """Where this run started and stopped, from the query's own progress records.

        Best-effort: this is for the audit row, and losing it must never fail a run that
        has already written its data. Position values are Auto Loader's own opaque offset
        JSON - this does not interpret their shape, only serialises what is there.
        """
        try:
            recent = [_progress_dict(p) for p in (query.recentProgress or [])]
            sources = [(p.get("sources") or [{}])[0] for p in recent]
            starts = [_as_json(s.get("startOffset")) for s in sources if s.get("startOffset") is not None]
            ends = [_as_json(s.get("endOffset")) for s in sources if s.get("endOffset") is not None]
            self.state.position_start = starts[0] if starts else None
            self.state.position_end = ends[-1] if ends else None
        except Exception:  # noqa: BLE001 - the run succeeded; only its provenance is missing
            self.ctx.log.warning("file_positions_unavailable", detail=traceback.format_exc())


RESCUED_DATA_HINT = "see the _rescued_data column"


def _elapsed_ms(since: float) -> int:
    return int((time.time() - since) * 1000)


def _progress_dict(progress: Any) -> dict:
    """Normalise a StreamingQueryProgress payload to a plain dict.

    Small and duplicated from sources/kafka/listener.py's function of the same name rather
    than imported: the two purely-generic JSON-normalisation helpers are a handful of
    lines, and importing them would couple this source's correctness to Kafka's module
    existing for no benefit - see the module docstring for why the riskier, offset-shape-
    dependent parts of Kafka's listener (`_pending`) are deliberately NOT reused here.
    """
    import json

    raw = getattr(progress, "json", None)
    if isinstance(raw, str):
        return json.loads(raw)
    if callable(raw):
        return json.loads(raw())
    if isinstance(progress, dict):
        return progress
    return json.loads(str(progress))


def _as_json(value: Any) -> str | None:
    import json

    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value)


# --------------------------------------------------------------------------------------
# The startup guard - mirrors sources/kafka/run.py's, by design (STAGE_5 brief: "do not
# write a second guard"). See that module for the full reasoning; this is the same shape
# applied to this source's own fields.
# --------------------------------------------------------------------------------------


def _checkpoint_offsets_exist(checkpoint_path: str) -> bool:
    """Is there an offsets directory under this checkpoint? Same probe as Kafka's, and the
    same reason it is `os.stat()` rather than `os.path.exists()` - see sources/kafka/run.py."""
    probe = os.path.join(checkpoint_path, "offsets")
    try:
        os.stat(probe)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RuntimeError(
            f"Could not determine whether the checkpoint exists at {probe}: {type(exc).__name__}: {exc}\n\n"
            "This is NOT the same as the checkpoint being missing, so this job refuses to guess: "
            "treating it as missing would block a healthy stream, and treating it as present would "
            "disable the guard against silent data loss. Confirm the driver on this compute profile "
            "can read the checkpoint Volume, then re-run."
        ) from exc
    return True


def _guard_against_checkpoint_reset(ctx: RunContext, cfg: FileConfig) -> None:
    """Refuse to run a primary stream in either state that would silently ingest nothing.

    No `topic`-style filter on the "already landed" check: unlike Kafka's shared-checkpoint-
    namespace-per-cluster shape, this source's landing table belongs to exactly this one
    file source, so "does the landing table hold any rows at all" is the whole question.
    """
    if cfg.is_replay:
        return
    if _checkpoint_offsets_exist(cfg.checkpoint_path):
        return

    if cfg.checkpoint_reset_id:
        if _reset_id_already_used(ctx, cfg):
            raise RuntimeError(
                f"REFUSING TO RUN: checkpoint_reset_id '{cfg.checkpoint_reset_id}' has ALREADY been "
                f"used by an earlier run of '{cfg.source_key}', and the checkpoint is missing again "
                f"({cfg.checkpoint_path}).\n\n"
                "A reset id is single-use. It works by forking this source's Delta transaction "
                "identity, so a restarted stream has no committed versions to collide with. Reusing "
                "one keeps the OLD identity - against which Delta already holds high versions - so "
                "batch ids would restart at 0 and EVERY WRITE WOULD BE SKIPPED AS A DUPLICATE. The "
                "run would report success and ingest nothing.\n\n"
                "Set file_checkpoint_reset_id to an UNUSED incident id and re-run. Do not clear the "
                "field afterwards."
            )
        ctx.log.warning(
            "file_checkpoint_reset_engaged",
            source_key=cfg.source_key,
            checkpoint_reset_id=cfg.checkpoint_reset_id,
            txn_app_id=cfg.txn_app_id,
            note=(
                "starting this stream from batch 0 under a NEW transaction identity; do NOT clear "
                "this field afterwards - reverting to the old identity would resurrect the exact "
                "collision it was set to avoid"
            ),
        )
        return

    if not ctx.tables.table_exists(ctx.spark, cfg.landing_table):
        return
    already_landed = ctx.spark.table(cfg.landing_table).limit(1).count() > 0
    if not already_landed:
        return
    raise RuntimeError(
        f"REFUSING TO RUN: the checkpoint for '{cfg.source_key}' is missing ({cfg.checkpoint_path}) "
        f"but {cfg.landing_table} already holds rows.\n\n"
        "Running now would restart batch ids at 0, and Delta would silently SKIP every write as a "
        "duplicate - the job would report success and ingest nothing.\n\n"
        "Set file_checkpoint_reset_id to an unused incident id in the control table to restart under "
        "a fresh identity - see docs/RUNBOOK_SUPPORT.md."
    )


def _reset_id_already_used(ctx: RunContext, cfg: FileConfig) -> bool:
    """Has this reset id appeared on a primary run of this source before? Same audit-table
    check as Kafka's, and the same reasoning: a missing row reads as "unused", which is the
    correct default when the audit table is itself best-effort."""
    audit_table = ctx.cfg.get("audit_table")
    if not audit_table or not ctx.tables.table_exists(ctx.spark, audit_table):
        return False
    rows = (
        ctx.spark.table(audit_table)
        .where(
            f"source_key = '{cfg.source_key}' AND run_type = '{RUN_TYPE_PRIMARY}' "
            f"AND rerun_id = '{cfg.checkpoint_reset_id}' AND run_id <> '{ctx.run_id}'"
        )
        .limit(1)
        .collect()
    )
    return bool(rows)
