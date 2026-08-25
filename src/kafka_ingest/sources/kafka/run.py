"""The Kafka source's one function, and everything one run of it does.

    readStream(kafka)
        +-- foreachBatch(batch_df, batch_id)
              +-- audit landing STARTED
              +-- project + write LANDING            (raw bytes, zero interpretation)
              +-- audit landing COMPLETED
              +-- audit curated STARTED
              +-- parse THE SAME in-memory batch
              +-- write CURATED (+ quarantine)
              +-- audit curated COMPLETED

Curated never re-reads Kafka and never re-reads the landing table. The batch is read once,
cached, and used twice.

RE-RUNS AND DUPLICATES - understand this before changing anything here
---------------------------------------------------------------------
Structured Streaming writes the batch's offset range to the checkpoint BEFORE running
foreachBatch, and the commit marker only AFTER it returns cleanly. A failed batch therefore
re-executes as the SAME batch id over the IDENTICAL offset range - a retry never pulls a
different set of records.

That makes duplicates preventable with Delta's idempotent writes (txnAppId + txnVersion):

  landing ok, curated fails  -> foreachBatch raises, the batch is NOT committed, the run
                                fails. On retry the same batch replays; Delta skips the
                                landing append because (txnAppId, batch_id) is already in
                                THAT table's log, and curated is written. Both end
                                consistent.
  landing fails              -> nothing written anywhere; the retry is clean.
  both ok, driver dies       -> the retry skips both appends, then commits. A no-op.

Delta tracks (appId, version) PER TABLE, which is why one shared txn_app_id is correct:
landing dedupes itself while curated still proceeds.

THE ONE FAILURE THAT LOOKS LIKE SUCCESS
---------------------------------------
Delete the primary checkpoint and batch ids restart at 0, but Delta has already recorded
higher versions for this app id - so every write is SKIPPED as a duplicate, the job reports
success, and nothing is ingested. `framework/checkpoint.py::guard_against_checkpoint_reset`
refuses to run in exactly that state, and `checkpoint_reset_id` is the deliberate,
single-use way through it. Read that function before touching either - it is shared with
sources/file/run.py, the other checkpoint-based source.
"""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass, field
from typing import Any

from ...framework import audit as audit_module
from ...framework import checkpoint as checkpoint_guard
from ...framework.contracts import RunContext, RunResult
from ...framework.security import SecretResolver
from . import config as kafka_config
from . import curated, landing, listener, tables
from .config import RUN_TYPE_CURATED_REPLAY, RUN_TYPE_KAFKA_REPLAY, KafkaConfig
from .reader import build_batch_reader, build_stream_reader, resolve_trigger
from .registry import SchemaRegistryClient
from .security import build_registry_auth
from .spec import CHECKPOINT_RESET_ID, SOURCE_SPEC

LOG = logging.getLogger(__name__)

LAYER_LANDING = "landing"
LAYER_CURATED = "curated"
LAYER_QUARANTINE = "quarantine"

# This source's own control-table column for the reset id, reverse-looked-up once at
# import time from SOURCE_SPEC.control_columns - see framework/checkpoint.py.
_RESET_ID_CONTROL_COLUMN = checkpoint_guard.control_column_for(SOURCE_SPEC, CHECKPOINT_RESET_ID)

# The txnVersion for an execution that is not a microbatch (a bounded batch replay, a
# curated replay). Negative so it can never collide with a real batch id, and so
# framework/writers.py skips the idempotency markers for it - a bounded replay's
# idempotency comes from its MERGE key instead.
TXN_VERSION_NON_STREAMING = -1


def run(ctx: RunContext, secrets: Any = None) -> RunResult:
    """Resolve, guard, read, write, report. The WHOLE contract of this source.

    `secrets` is an optional injection point and not part of the contract: a run builds its
    own SecretResolver, and a test supplies a stand-in rather than a workspace.
    """
    cfg = kafka_config.build(ctx.cfg, ctx.run_type, ctx.tables)
    ctx.audit.source_ref = cfg.topic
    ctx.log.info(
        "kafka_run_resolved",
        topic=cfg.topic,
        cluster=cfg.cluster.name,
        registry=cfg.registry.name,
        landing=cfg.landing_table,
        curated=cfg.curated_table,
        checkpoint=cfg.checkpoint_path,
        txn_app_id=cfg.txn_app_id,
        failure_mode=cfg.failure_mode,
    )

    # BEFORE the reset id reaches the audit writer, so the history check below cannot see
    # this run's own rows. It excludes the current run_id as well, so the ordering is
    # belt and braces rather than the only thing keeping the check honest.
    checkpoint_guard.guard_against_checkpoint_reset(
        ctx,
        checkpoint_path=cfg.checkpoint_path,
        landing_table=cfg.landing_table,
        checkpoint_reset_id=cfg.checkpoint_reset_id,
        is_replay=cfg.is_replay,
        control_column=_RESET_ID_CONTROL_COLUMN,
    )
    if cfg.checkpoint_reset_id:
        # One column, two meanings, told apart by run_type: a replay's id on a replay run,
        # and the reset id that forked the write identity on a primary one. This is what
        # makes the reuse refusal above answerable from the audit table alone.
        ctx.audit.rerun_id = cfg.checkpoint_reset_id
    elif cfg.replay.rerun_id:
        ctx.audit.rerun_id = cfg.replay.rerun_id

    state = _RunState(cfg=cfg)
    session = _Session(ctx=ctx, cfg=cfg, state=state, secrets=secrets or SecretResolver())
    session.prepare()

    if ctx.run_type == RUN_TYPE_CURATED_REPLAY:
        session.run_curated_replay()
    elif ctx.run_type == RUN_TYPE_KAFKA_REPLAY and cfg.replay.is_bounded:
        session.run_bounded_replay()
    else:
        session.run_streaming()

    return RunResult(
        rows_read=state.rows_read,
        rows_written=dict(state.rows_written),
        rows_quarantined=state.rows_quarantined,
        position_start=state.position_start,
        position_end=state.position_end,
        source_detail=cfg.source_detail(),
        pending_work=state.pending_work,
    )


@dataclass
class _RunState:
    """What this run has done so far. The only mutable thing in the package.

    Accumulated across microbatches, because a `RunResult` describes the RUN and one
    availableNow run is many batches. Per-batch detail is already durable in the audit
    table by the time this is read.
    """

    cfg: KafkaConfig
    rows_read: int = 0
    rows_written: dict = field(default_factory=dict)
    rows_quarantined: int = 0
    position_start: str | None = None
    position_end: str | None = None
    pending_work: int | None = None

    def record(self, layer: str, count: int) -> None:
        self.rows_written[layer] = self.rows_written.get(layer, 0) + count


@dataclass
class _Session:
    """One run's collaborators, assembled once on the driver.

    A plain dataclass rather than a set of arguments threaded through six functions: every
    method below needs most of it, and the alternative is a signature nobody reads.
    """

    ctx: RunContext
    cfg: KafkaConfig
    state: _RunState
    secrets: Any
    client: Any = None
    reader_schema: tuple = ()

    # -- setup -------------------------------------------------------------------------

    def prepare(self) -> None:
        """Everything that must be true before the first byte is read.

        The reader schema is resolved ONCE PER RUN, here: a registration landing mid-run
        would otherwise change the payload struct's shape partway through a sequence of
        microbatches, and two batches of one run would produce two different curated shapes.
        """
        self.client = SchemaRegistryClient(self.cfg.registry, build_registry_auth(self.cfg.registry, self.secrets))
        self.reader_schema = curated.resolve_reader_schema(self.cfg, self.client)

        tables.ensure_landing(self.ctx, self.cfg)
        if self.cfg.quarantine_on_error:
            tables.ensure_quarantine(self.ctx, self.cfg)
        # Curated cannot be created alongside the other two: its payload struct follows the
        # reader schema, which is only known now. It is created explicitly rather than left
        # for Spark to invent from whatever the first batch happens to contain.
        tables.ensure_curated(self.ctx, self.cfg, curated.curated_schema(self.ctx.spark, self.cfg, self.reader_schema))

    # -- run shapes --------------------------------------------------------------------

    def run_streaming(self) -> None:
        """A primary run, or an unbounded replay. Both are a streaming query with a
        checkpoint; only the checkpoint path and the starting position differ."""
        audit_listener = listener.StreamAuditListener(self.ctx.audit, audit_module)
        self.ctx.spark.streams.addListener(audit_listener)
        try:
            query = (
                build_stream_reader(self.ctx.spark, self.cfg, self.secrets)
                .writeStream.foreachBatch(lambda df, batch_id: self.process_microbatch(df, batch_id))
                .queryName(f"kafka_ingest::{self.cfg.source_key}::{self.cfg.run_type}")
                .option("checkpointLocation", self.cfg.checkpoint_path)
                .trigger(**resolve_trigger(self.cfg))
                .start()
            )
            query.awaitTermination()
            # Drain the final progress from the query itself rather than sleeping and
            # hoping the listener thread got there first - see listener.drain().
            self.state.pending_work = listener.drain(query, audit_listener)
            self._record_positions(query)
        finally:
            self.ctx.spark.streams.removeListener(audit_listener)

    def run_bounded_replay(self) -> None:
        """A replay with an explicit end, executed as a BATCH read.

        No checkpoint at all, which is the feature: the run is a pure function of
        (start, end), so re-running it has identical effect. Idempotency comes from the
        MERGE keys in the writes below.
        """
        self.ctx.log.warning(
            "kafka_bounded_replay",
            topic=self.cfg.topic,
            rerun_id=self.cfg.replay.rerun_id,
            starting=self.cfg.replay.starting_offsets or self.cfg.replay.starting_timestamp,
            ending=self.cfg.replay.ending_offsets or self.cfg.replay.ending_timestamp,
        )
        self.state.position_start = self.cfg.replay.starting_offsets or self.cfg.replay.starting_timestamp
        self.state.position_end = self.cfg.replay.ending_offsets or self.cfg.replay.ending_timestamp
        self.process_microbatch(build_batch_reader(self.ctx.spark, self.cfg, self.secrets), TXN_VERSION_NON_STREAMING)

    def run_curated_replay(self) -> None:
        """Re-parse existing landing rows into curated. The broker is never contacted.

        A fundamentally different operation from a Kafka replay, and deliberately a
        separate run type: it fixes PARSING problems (a schema registered late, a corrected
        reader schema, a bug in this code), not MISSING DATA problems. Because landing holds
        the original bytes, it works long after Kafka retention has expired.
        """
        self.ctx.log.warning(
            "kafka_curated_replay",
            topic=self.cfg.topic,
            rerun_id=self.cfg.replay.rerun_id,
            landing_table=self.cfg.landing_table,
            landing_filter=self.cfg.replay.landing_filter,
        )
        self.state.position_start = self.cfg.replay.landing_filter
        landing_rows = self.ctx.spark.table(self.cfg.landing_table).where(self.cfg.replay.landing_filter)
        # Belt and braces. Landing holds exactly one topic, so this matches everything - but
        # it costs nothing, and it makes a mistyped table name produce an empty replay
        # rather than another topic's rows in this curated table.
        landing_rows = landing_rows.where(f"topic = '{self.cfg.topic}'")

        try:
            self._curated_layer(landing_rows, TXN_VERSION_NON_STREAMING)
        except Exception as exc:  # blind catch: record which layer failed, then re-raise
            self.ctx.audit.emit(
                LAYER_CURATED,
                audit_module.STATUS_FAILED,
                TXN_VERSION_NON_STREAMING,
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            raise

    # -- the microbatch body -----------------------------------------------------------

    def process_microbatch(self, batch_df: Any, txn_version: int) -> None:
        """Landing then curated, from ONE cached in-memory batch.

        Raises on any failure so Structured Streaming does not commit the batch. The FAILED
        audit row names the layer that was in progress, which is what turns triage into a
        lookup rather than a guess.
        """
        from pyspark.storagelevel import StorageLevel

        layer = LAYER_LANDING
        landing_df = None
        started = time.time()

        try:
            self.ctx.audit.emit(LAYER_LANDING, audit_module.STATUS_STARTED, txn_version)

            # MEMORY_AND_DISK, not MEMORY_ONLY: a large first-run batch that does not fit in
            # memory must SPILL rather than silently recompute, because recomputing means a
            # second read from Kafka at offsets the source has already advanced past.
            landing_df = landing.project(batch_df, self.cfg, txn_version, self.ctx.run_id).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            landing_count = landing_df.count()

            if landing_count == 0:
                # Trigger.AvailableNow emits a final empty batch on every run.
                self.ctx.audit.emit(LAYER_LANDING, audit_module.STATUS_NO_DATA, txn_version, record_count=0)
                return

            self.state.rows_read += landing_count
            self._write_landing(landing_df, txn_version)
            self.state.record(LAYER_LANDING, landing_count)
            self.ctx.audit.emit(
                LAYER_LANDING,
                audit_module.STATUS_COMPLETED,
                txn_version,
                record_count=landing_count,
                duration_ms=_elapsed_ms(started),
            )

            layer = LAYER_CURATED
            self._curated_layer(landing_df, txn_version)

        except Exception as exc:  # blind catch: record which layer failed, then re-raise
            self.ctx.audit.emit(
                layer,
                audit_module.STATUS_FAILED,
                txn_version,
                error_class=type(exc).__name__,
                error_message=f"{exc}\n{traceback.format_exc()}",
            )
            raise
        finally:
            # In a `finally`, not on the success path only: the poison-batch path is the one
            # that runs over and over, and it is exactly where a leaked cache accumulates.
            if landing_df is not None:
                landing_df.unpersist()

    def _curated_layer(self, landing_df: Any, txn_version: int) -> None:
        """Parse the already-landed batch and persist curated + quarantine.

        Split out of process_microbatch purely so each function stays readable; it is
        called from there and from the curated-replay shape, and from nowhere else.
        """
        started = time.time()
        self.ctx.audit.emit(LAYER_CURATED, audit_module.STATUS_STARTED, txn_version)

        result = curated.parse_batch(
            self.ctx.spark, landing_df, self.cfg, self.client, txn_version, self.ctx.run_id, self.reader_schema
        )
        curated_count = self._write_and_count(result.curated_df, lambda df: self._write_curated(df, txn_version))
        quarantined = self._write_and_count(result.quarantine_df, lambda df: self._write_quarantine(df, txn_version))

        self.state.record(LAYER_CURATED, curated_count)
        if quarantined:
            self.state.record(LAYER_QUARANTINE, quarantined)
            self.state.rows_quarantined += quarantined
            self.ctx.log.warning(
                "kafka_records_quarantined",
                topic=self.cfg.topic,
                txn_version=txn_version,
                quarantined=quarantined,
                unresolvable_schema_ids=sorted(result.unresolvable_schema_ids) or "none",
            )

        self.ctx.audit.emit(
            LAYER_CURATED,
            audit_module.STATUS_COMPLETED,
            txn_version,
            record_count=curated_count,
            quarantined_count=quarantined,
            duration_ms=_elapsed_ms(started),
            source_detail={
                "writer_schema_ids": sorted(result.writer_schema_ids),
                "reader_schema_id": result.reader_schema_id,
                "unresolvable_schema_ids": sorted(result.unresolvable_schema_ids),
            },
        )

    def _write_and_count(self, df: Any, write) -> int:
        """Count then write, caching in between so the frame is computed once."""
        from pyspark.storagelevel import StorageLevel

        if df is None:
            return 0
        cached = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            count = cached.count()
            if count:
                write(cached)
            return count
        finally:
            cached.unpersist()

    # -- the writes --------------------------------------------------------------------

    def _write_landing(self, df: Any, txn_version: int) -> None:
        """Append with idempotency markers (primary), or insert-if-absent (replay).

        A replay overlaps existing data by definition, so an append would duplicate.
        Matched rows are left UNTOUCHED: the original landing row records what arrived on
        the primary stream, and rewriting its provenance columns with replay metadata would
        destroy that record.
        """
        if not self.cfg.is_replay:
            self.ctx.writers.append(df, self.cfg.landing_table, txn_app_id=self.cfg.txn_app_id, txn_version=txn_version)
            return
        if not self.ctx.tables.table_exists(self.ctx.spark, self.cfg.landing_table):
            # A replay into a landing table that does not exist yet is unusual but legal
            # (rebuilding a dropped table from the broker). Nothing to merge against.
            self.ctx.writers.append(df, self.cfg.landing_table)
            return
        self.ctx.writers.merge(
            self.ctx.spark,
            df,
            self.cfg.landing_table,
            keys=landing.RECORD_KEYS,
            # NO PARTITION BOUND IS DERIVABLE HERE, and this is the one place in the
            # codebase that says so out loud. Landing is partitioned by ingest_date - the
            # date the row was WRITTEN - so a replayed record carries TODAY's ingest_date
            # while its target twin carries the date it originally arrived. Bounding on the
            # source frame's ingest_date would therefore match nothing and INSERT
            # DUPLICATES, which is worse than the full scan it would have saved. Curated is
            # different and does get a bound: event_date is a property of the RECORD, so a
            # replayed row and its original share it. See _write_curated below.
            partition_predicate="true",
        )

    def _write_curated(self, df: Any, txn_version: int) -> None:
        """Append (primary) or upsert (replay) into the topic's curated table.

        Replay uses update-matched, unlike landing: the entire point of a curated replay is
        to REPLACE a bad parse with a good one. Landing deliberately does the opposite.
        """
        if self.cfg.is_replay and self.ctx.tables.table_exists(self.ctx.spark, self.cfg.curated_table):
            predicate = _event_date_predicate(df)
            if predicate is None:
                # An empty frame never reaches here (_write_and_count skips a zero count),
                # but a frame whose event_date is entirely NULL would - and formatting None
                # into the predicate string would produce a MERGE that silently matches
                # nothing.
                raise ValueError(
                    f"curated replay for '{self.cfg.source_key}' produced rows with no usable "
                    "event_date, so the MERGE has no partition bound. event_date falls back to "
                    "the Kafka timestamp and should never be NULL - investigate the landing rows "
                    "this replay selected before re-running."
                )
            self.ctx.writers.merge(
                self.ctx.spark,
                df,
                self.cfg.curated_table,
                keys=landing.RECORD_KEYS,
                partition_predicate=predicate,
                update_matched=True,
                # A curated replay is the very thing an operator runs AFTER a schema change,
                # re-parsing older rows with the corrected schema. Without evolution here,
                # that replay fails on the schema it was run to apply. Landing deliberately
                # does not get this: its schema is fixed DDL and never evolves.
                schema_evolution=True,
            )
            return
        self.ctx.writers.append(
            df,
            self.cfg.curated_table,
            txn_app_id=self.cfg.txn_app_id,
            txn_version=txn_version,
            # An additive reader-schema change (a new optional Avro field) lands without a
            # manual ALTER TABLE. Additive only - Delta still rejects type changes.
            merge_schema=True,
            partition_by=list(self.cfg.curated_partition_by),
        )

    def _write_quarantine(self, df: Any, txn_version: int) -> None:
        """Always append. A record can legitimately be quarantined twice - once by the
        primary run, once by a replay that still lacked the schema - and both attempts are
        evidence worth keeping.

        The app id carries a distinct suffix: Delta tracks (appId, version) per table, so
        reusing the landing app id would be correct but much harder to read in DESCRIBE
        HISTORY.
        """
        self.ctx.writers.append(
            df,
            self.cfg.quarantine_table,
            txn_app_id=None if self.cfg.is_replay else f"{self.cfg.txn_app_id}::quarantine",
            txn_version=txn_version,
        )

    # -- reporting ---------------------------------------------------------------------

    def _record_positions(self, query: Any) -> None:
        """Where this run started and stopped, from the query's own progress records.

        Best-effort: these are for the audit row, and losing them must never fail a run
        that has already written its data.
        """
        try:
            recent = [listener.progress_dict(p) for p in (query.recentProgress or [])]
            sources = [(p.get("sources") or [{}])[0] for p in recent]
            starts = [listener.as_json(s.get("startOffset")) for s in sources if s.get("startOffset") is not None]
            ends = [listener.as_json(s.get("endOffset")) for s in sources if s.get("endOffset") is not None]
            self.state.position_start = starts[0] if starts else None
            self.state.position_end = ends[-1] if ends else None
        except Exception:  # noqa: BLE001 - the run succeeded; only its provenance is missing
            LOG.error("Could not read start/end offsets from the query progress: %s", traceback.format_exc())


def _event_date_predicate(df: Any) -> str | None:
    """`t.event_date BETWEEN <min> AND <max>` for the batch about to be merged.

    WITHOUT THIS, ONE REPLAYED DAY REWRITES EVERY PARTITION IT MIGHT MATCH. The merge key
    is (topic, kafka_partition, kafka_offset), which says nothing about event_date, so
    Delta has no way to prune - and on a table with three years of daily partitions, a
    replay of two hours rewrites three years.

    The batch is already cached by the caller, so this aggregation is a scan of memory, not
    a second read. Returns None when the bounds are unusable, which the caller turns into a
    refusal rather than formatting "None" into a SQL string.

    selectExpr rather than agg(F.min(...)): building a Column object needs an active
    SparkSession, and this function is on the path of every curated replay, so keeping it
    to strings is what lets the predicate itself be asserted without a cluster.
    """
    bounds = df.selectExpr("min(event_date) AS lo", "max(event_date) AS hi").collect()
    if not bounds or bounds[0]["lo"] is None or bounds[0]["hi"] is None:
        return None
    return f"t.event_date BETWEEN '{bounds[0]['lo']}' AND '{bounds[0]['hi']}'"


def _elapsed_ms(since: float) -> int:
    return int((time.time() - since) * 1000)
