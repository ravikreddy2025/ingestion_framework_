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
on Kafka's checkpoint-reset guard exactly - not a second design. Both sources now call the
one implementation in `framework/checkpoint.py::guard_against_checkpoint_reset`.

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

import time
import traceback
from dataclasses import dataclass, field
from typing import Any

from ...framework import audit as audit_module
from ...framework import checkpoint as checkpoint_guard
from ...framework.contracts import RunContext, RunResult
from ...framework.security import SecretResolver, apply_session_options
from . import config as file_config
from . import landing, security, tables
from .config import FileConfig
from .reader import build_stream_reader
from .spec import CHECKPOINT_RESET_ID, SOURCE_SPEC

LAYER_LANDING = "landing"

# This source's own control-table column for the reset id, reverse-looked-up once at
# import time from SOURCE_SPEC.control_columns - see framework/checkpoint.py.
_RESET_ID_CONTROL_COLUMN = checkpoint_guard.control_column_for(SOURCE_SPEC, CHECKPOINT_RESET_ID)


def run(ctx: RunContext, secrets: Any = None) -> RunResult:
    """Resolve, guard, read, write, report. The WHOLE contract of this source.

    `secrets` is an optional injection point and not part of the contract: a run builds its
    own SecretResolver, and a test supplies a stand-in rather than a workspace.
    """
    cfg = file_config.build(ctx.cfg, ctx.run_type, ctx.tables)
    ctx.audit.source_ref = cfg.full_source_path
    ctx.log.info(
        "file_run_resolved",
        access_mode=cfg.access_mode,
        source_path=cfg.full_source_path,
        storage=cfg.storage_ref,
        landing=cfg.landing_table,
        checkpoint=cfg.checkpoint_path,
        schema_location=cfg.schema_location_path,
        txn_app_id=cfg.txn_app_id,
        failure_mode=cfg.failure_mode,
    )

    # BEFORE the reset id reaches the audit writer - see framework/checkpoint.py for why
    # the ordering is belt and braces rather than the only thing keeping it honest.
    checkpoint_guard.guard_against_checkpoint_reset(
        ctx,
        checkpoint_path=cfg.checkpoint_path,
        landing_table=cfg.landing_table,
        checkpoint_reset_id=cfg.checkpoint_reset_id,
        is_replay=cfg.is_replay,
        control_column=_RESET_ID_CONTROL_COLUMN,
    )
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
        put them back - see framework/security.py `apply_session_options`.

        `access_mode: volume` (docs/build_log/DECISIONS.md D-15) applies NONE of this: it is
        governed by Unity Catalog grants on the Volume itself, and this framework has no
        credential of its own to set or restore - `cfg.storage` is None by construction for
        this mode (sources/file/config.py `_access`).
        """
        if self.cfg.access_mode == file_config.ACCESS_MODE_VOLUME:
            restore = _no_op_restore
        else:
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


def _no_op_restore() -> None:
    """The `restore` callable for a Unity Catalog Volume source (D-15): there were no
    session options to put back, because none were ever set."""
    return None


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
