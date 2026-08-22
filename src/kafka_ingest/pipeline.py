"""Orchestration: one Kafka read, chained landing then curated, per microbatch.

    readStream(kafka)
        └── foreachBatch(batch_df, batch_id)
                ├── audit  landing STARTED
                ├── project + write LANDING      (raw bytes, zero interpretation)
                ├── audit  landing COMPLETED
                ├── audit  curated STARTED
                ├── parse the SAME in-memory batch_df
                ├── write CURATED (+ quarantine)
                └── audit  curated COMPLETED

Curated never re-reads Kafka and never re-reads the landing table. The batch is read once,
cached, and used twice.

RE-RUNS AND DUPLICATES  (the thing to understand before changing anything here)
------------------------------------------------------------------------------
Structured Streaming writes the batch's offset range to the checkpoint BEFORE running
foreachBatch, and the commit marker only AFTER it returns cleanly. So a failed batch
re-executes as the SAME batch_id over the IDENTICAL offset range - a retry never pulls a
different set of records from Kafka.

That makes duplicates preventable with Delta's idempotent writes (txnAppId + txnVersion):

  landing ok, curated fails  -> foreachBatch raises, batch NOT committed, run fails.
                                On retry the same batch replays; Delta skips the landing
                                append because (txnAppId, batch_id) is already in THAT
                                table's log, and curated is written. Both end consistent.
  landing fails              -> nothing written anywhere; retry is clean.
  both ok, driver dies       -> retry skips both appends, then commits. A no-op.

Delta tracks (appId, version) PER TABLE, which is why one shared txn_app_id is correct:
landing dedupes itself while curated still proceeds.

`txn_app_id` MUST be stable across restarts for this to work, so it is derived from the
topic key and run lineage - never from a random per-run value. It is scoped per replay
lineage so a replay's batch 0 is never mistaken for the primary stream's batch 0.

See docs/DESIGN.md for the full failure-scenario table and the no-code fixes.
"""

from __future__ import annotations

import logging
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from . import tables
from .audit import (
    LAYER_CURATED,
    LAYER_LANDING,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_NO_DATA,
    STATUS_SKIPPED,
    STATUS_STARTED,
    AuditWriter,
    StreamAuditListener,
)
from .config import RUN_TYPE_CURATED_REPLAY, RUN_TYPE_KAFKA_REPLAY, TopicConfig
from .curated_writer import (
    curated_schema,
    parse_batch,
    resolve_reader_schema,
    write_curated,
    write_quarantine,
)
from .kafka_source import build_batch_reader, build_stream_reader, resolve_trigger
from .landing_writer import project_landing, write_landing
from .schema_resolver import SchemaRegistryClient
from .security import SecretResolver, build_registry_auth

LOG = logging.getLogger(__name__)

# batch_id for non-streaming executions (bounded batch replay, curated replay). Negative so
# it can never collide with a real microbatch id, and so the writers skip txnVersion for it
# - a bounded replay is made idempotent by its MERGE key instead.
BATCH_ID_NON_STREAMING = -1


@dataclass
class PipelineContext:
    """Everything a microbatch needs, assembled once per run on the driver."""

    spark: SparkSession
    cfg: TopicConfig
    client: SchemaRegistryClient
    audit: AuditWriter
    run_id: str
    txn_app_id: str
    reader_schema: Tuple


def build_context(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> PipelineContext:
    client = SchemaRegistryClient(cfg.registry, build_registry_auth(cfg.registry, secrets))
    run_id = _make_run_id(cfg)
    return PipelineContext(
        spark=spark,
        cfg=cfg,
        client=client,
        audit=AuditWriter(spark, cfg, run_id),
        run_id=run_id,
        txn_app_id=_make_txn_app_id(cfg),
        # Resolved once per run: a registration landing mid-run would otherwise change the
        # payload struct's shape partway through a sequence of microbatches.
        reader_schema=resolve_reader_schema(cfg, client),
    )


def ensure_curated(spark: SparkSession, cfg: TopicConfig, ctx: PipelineContext) -> None:
    """Create the curated table explicitly, before the first write.

    Curated cannot be created in tables.ensure_all() because its payload struct follows the
    Avro reader schema, which is not resolved until the run context is built. It is created
    here instead - NOT left to Spark to invent from the first batch it happens to see.

    CREATE TABLE IF NOT EXISTS, so this is a metadata no-op on every run after the first.
    Onboarding a topic therefore needs no manual DDL step, and no table is ever created with
    a shape nobody chose.
    """
    tables.ensure_curated_table(spark, cfg, curated_schema(spark, cfg, ctx.reader_schema))


def _make_run_id(cfg: TopicConfig) -> str:
    """Unique per execution. Stamped on every data row and every audit row."""
    suffix = cfg.run.job_run_id or uuid.uuid4().hex[:12]
    return f"{cfg.topic_key}-{cfg.run.run_type}-{suffix}"


def _make_txn_app_id(cfg: TopicConfig) -> str:
    """Stable across restarts - this is the identity Delta dedups retried batches against.

    checkpoint_reset_id (control-table override, primary runs only) forks the lineage the
    same way a replay's rerun_id does: a deliberately RESET primary is, for dedup purposes,
    a new writer that has never committed anything, so it must not inherit the old lineage's
    already-committed versions. See guard_against_checkpoint_reset().
    """
    lineage = cfg.run.rerun_id or cfg.checkpoint_reset_id or "primary"
    return f"kafka_ingest::{cfg.topic_key}::{cfg.run.run_type}::{lineage}"


# --------------------------------------------------------------------------------------
# The microbatch body - shared by primary runs, streaming replays and batch replays
# --------------------------------------------------------------------------------------


def process_microbatch(batch_df: DataFrame, batch_id: int, ctx: PipelineContext) -> None:
    """Landing then curated, from one cached in-memory batch.

    Raises on any failure so Structured Streaming does not commit the batch. The FAILED
    audit row names the layer that was in progress, which is what turns triage into a
    lookup rather than a guess.
    """
    cfg, audit = ctx.cfg, ctx.audit
    layer = LAYER_LANDING
    landing_df: Optional[DataFrame] = None
    started = time.time()

    try:
        audit.emit(LAYER_LANDING, STATUS_STARTED, batch_id)

        # MEMORY_AND_DISK, not MEMORY_ONLY: a large first-run batch that does not fit in
        # memory must spill rather than silently recompute, because recomputing means a
        # second read from Kafka at offsets the source has already advanced past.
        landing_df = project_landing(batch_df, cfg, batch_id, ctx.run_id).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        landing_count = landing_df.count()

        if landing_count == 0:
            # Trigger.AvailableNow emits a final empty batch on every run.
            audit.emit(LAYER_LANDING, STATUS_NO_DATA, batch_id, record_count=0)
            return

        write_landing(ctx.spark, landing_df, cfg, batch_id, ctx.txn_app_id)
        audit.emit(LAYER_LANDING, STATUS_COMPLETED, batch_id, record_count=landing_count,
                   duration_ms=_elapsed_ms(started))

        layer = LAYER_CURATED
        _run_curated_layer(landing_df, batch_id, ctx)

    except Exception as exc:  # blind catch: record which layer failed, then re-raise
        audit.emit(layer, STATUS_FAILED, batch_id, error_class=type(exc).__name__,
                   error_message=f"{exc}\n{traceback.format_exc()}")
        raise
    finally:
        if landing_df is not None:
            landing_df.unpersist()


def _run_curated_layer(landing_df: DataFrame, batch_id: int, ctx: PipelineContext) -> None:
    """Parse the already-written landing batch and persist curated + quarantine.

    Split out of process_microbatch purely so each function stays readable; it is only
    ever called from there and from the curated-replay run shape.
    """
    cfg, audit = ctx.cfg, ctx.audit
    started = time.time()
    audit.emit(LAYER_CURATED, STATUS_STARTED, batch_id)

    result = parse_batch(ctx.spark, landing_df, cfg, ctx.client, batch_id, ctx.run_id,
                         ctx.reader_schema)
    curated_count = _write_and_count(
        result.curated_df,
        lambda df: write_curated(ctx.spark, df, cfg, batch_id, ctx.txn_app_id),
    )
    quarantined = _write_and_count(
        result.quarantine_df,
        lambda df: write_quarantine(df, cfg, batch_id, ctx.txn_app_id),
    )
    if quarantined:
        LOG.warning("Topic '%s' batch %s: %s record(s) quarantined. Unresolvable schema ids: %s",
                    cfg.topic, batch_id, quarantined,
                    sorted(result.unresolvable_schema_ids) or "none")

    audit.emit(LAYER_CURATED, STATUS_COMPLETED, batch_id, record_count=curated_count,
               quarantined_count=quarantined, writer_schema_ids=result.writer_schema_ids,
               reader_schema_id=result.reader_schema_id, duration_ms=_elapsed_ms(started))


def _write_and_count(df: Optional[DataFrame], write) -> int:
    """Count then write, caching in between so the frame is computed once."""
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


def _elapsed_ms(since: float) -> int:
    return int((time.time() - since) * 1000)


# --------------------------------------------------------------------------------------
# Startup guard
# --------------------------------------------------------------------------------------


def _checkpoint_offsets_exist(checkpoint_path: str) -> bool:
    """Is there an offsets directory under this checkpoint?

    Deliberately NOT os.path.exists(): that swallows every OSError and returns False, so a
    UC Volume the driver cannot reach right now would be indistinguishable from a checkpoint
    someone deleted. The guard below turns "absent" into a hard refusal, so a false "absent"
    blocks a perfectly healthy job - the failure mode is a stuck production stream, not a
    silent one.

    os.stat() raises instead, which lets us separate the three cases:
      FileNotFoundError -> genuinely absent, the case the guard exists for
      other OSError     -> we cannot tell; say so rather than guessing either way
      no exception      -> present

    Volume access from the driver is a compute-profile property (see the module docstring in
    security.py for the executor-side equivalent), so on serverless this is the branch that
    is most likely to surprise you.
    """
    probe = os.path.join(checkpoint_path, "offsets")
    try:
        os.stat(probe)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RuntimeError(
            f"Could not determine whether the checkpoint exists at {probe}: "
            f"{type(exc).__name__}: {exc}\n\n"
            "This is NOT the same as the checkpoint being missing, so this job refuses to "
            "guess: treating it as missing would block a healthy stream, and treating it as "
            "present would disable the guard against silent data loss. Confirm the driver on "
            "this compute profile can read the checkpoint Volume, then re-run."
        ) from exc
    return True


def guard_against_checkpoint_reset(spark: SparkSession, cfg: TopicConfig) -> None:
    """Refuse to run a primary stream whose checkpoint has been deleted under it.

    This is the only failure mode in the design that LOOKS LIKE SUCCESS. Delete the primary
    checkpoint and batch ids restart at 0, but Delta has already recorded high txnVersions
    for this txnAppId - so every append is skipped as a duplicate, the job reports success,
    and nothing is written.

    Detection is deliberately narrow: checkpoint absent AND this topic's landing table
    already has rows. A genuine first run has none, so it is not tripped - which also covers
    a migration that pre-loads CURATED from the legacy system before the stream ever starts:
    this guard never looks at curated, only landing.

    The correct way to reprocess a data gap is the replay job, which uses a different
    checkpoint AND a different txnAppId, so neither collides.

    For the rarer case - the checkpoint really is gone and the primary stream itself must
    restart - support sets checkpoint_reset_id in the control table (see
    docs/RUNBOOK_SUPPORT.md 5.4a). That is a deliberate, logged, one-way bypass: unlike
    silently deleting landing rows to fool this check, it also forks _make_txn_app_id's
    lineage, so the restarted primary gets a Delta identity with no committed history to
    collide with - closing the actual hole, not just the symptom.
    """
    if cfg.run.is_replay:
        return
    if cfg.checkpoint_reset_id:
        LOG.warning(
            "Checkpoint-reset override engaged for '%s' (checkpoint_reset_id=%s): starting "
            "the primary stream from batch 0 under a NEW transaction identity. Set via the "
            "control table - see docs/RUNBOOK_SUPPORT.md 5.4a. Do NOT clear this field back "
            "out afterwards; reverting to the old identity would resurrect the exact "
            "collision it was set to avoid.",
            cfg.topic_key, cfg.checkpoint_reset_id,
        )
        return
    if _checkpoint_offsets_exist(cfg.checkpoint_path):
        return
    if not tables.table_exists(spark, cfg.landing_table):
        return
    already_landed = (
        spark.table(cfg.landing_table).where(f"topic = '{cfg.topic}'").limit(1).count() > 0
    )
    if not already_landed:
        return
    raise RuntimeError(
        f"REFUSING TO RUN: the primary checkpoint for '{cfg.topic_key}' is missing "
        f"({cfg.checkpoint_path}) but {cfg.landing_table} already holds rows for topic "
        f"'{cfg.topic}'.\n\n"
        "Running now would restart batch ids at 0, and Delta would silently SKIP every "
        "write as a duplicate - the job would report success and ingest nothing.\n\n"
        "To reprocess data, use the kafka replay job with a new rerun_id (it uses its own "
        "checkpoint and its own txnAppId). To genuinely start this topic over, drop or "
        "rename the landing rows for this topic first, so this guard has nothing to catch."
    )


# --------------------------------------------------------------------------------------
# Run shapes
# --------------------------------------------------------------------------------------


def run_streaming(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> None:
    """Primary run, or an unbounded replay. Both are a streaming query with a checkpoint;
    only the checkpoint path and the starting position differ."""
    tables.ensure_all(spark, cfg)
    guard_against_checkpoint_reset(spark, cfg)

    ctx = build_context(spark, cfg, secrets)
    ensure_curated(spark, cfg, ctx)
    listener = StreamAuditListener(ctx.audit)
    spark.streams.addListener(listener)

    LOG.info("Starting %s run for '%s' | run_id=%s | checkpoint=%s | trigger=%s",
             cfg.run.run_type, cfg.topic, ctx.run_id, cfg.checkpoint_path, cfg.trigger)
    try:
        query = (
            build_stream_reader(spark, cfg, secrets).writeStream
            .foreachBatch(lambda df, bid: process_microbatch(df, bid, ctx))
            .queryName(f"kafka_ingest::{cfg.topic_key}::{cfg.run.run_type}")
            .option("checkpointLocation", cfg.checkpoint_path)
            .trigger(**resolve_trigger(cfg))
            .start()
        )
        query.awaitTermination()
    finally:
        # Give the listener thread a moment to drain the final progress event before the
        # session tears down; otherwise the last batch's stream audit row can be lost on a
        # fast availableNow run.
        time.sleep(2)
        spark.streams.removeListener(listener)


def run_bounded_replay(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> None:
    """Replay with an explicit end offset/timestamp, executed as a batch read.

    No checkpoint at all: the run is a pure function of (start, end), so re-running it has
    identical effect. Idempotency comes from the MERGE keys in the writers.
    """
    tables.ensure_all(spark, cfg)
    ctx = build_context(spark, cfg, secrets)
    ensure_curated(spark, cfg, ctx)
    LOG.info("Bounded replay for '%s' | rerun_id=%s | run_id=%s",
             cfg.topic, cfg.run.rerun_id, ctx.run_id)
    process_microbatch(build_batch_reader(spark, cfg, secrets), BATCH_ID_NON_STREAMING, ctx)


def run_curated_replay(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> None:
    """Re-parse existing landing rows into curated. The broker is never contacted.

    A fundamentally different operation from a Kafka replay, and deliberately a separate
    entrypoint: it fixes PARSING problems (a schema registered late, a corrected reader
    schema, a bug in this code), not MISSING DATA problems. Because landing holds the
    original bytes, it works long after Kafka retention has expired.
    """
    tables.ensure_all(spark, cfg)
    ctx = build_context(spark, cfg, secrets)
    ensure_curated(spark, cfg, ctx)
    LOG.info("Curated replay for '%s' | rerun_id=%s | landing filter: %s",
             cfg.topic, cfg.run.rerun_id, cfg.run.landing_filter)

    landing = spark.table(cfg.landing_table).where(cfg.run.landing_filter)
    # Belt and braces. The landing table holds exactly one topic, so this matches
    # everything - but it costs nothing, and it means a mistyped table name in config
    # produces an empty replay rather than another topic's rows in this curated table.
    landing = landing.where(landing["topic"] == cfg.topic)

    try:
        _run_curated_layer(landing, BATCH_ID_NON_STREAMING, ctx)
    except Exception as exc:
        ctx.audit.emit(LAYER_CURATED, STATUS_FAILED, BATCH_ID_NON_STREAMING,
                       error_class=type(exc).__name__, error_message=str(exc))
        raise


def run_disabled(spark: SparkSession, cfg: TopicConfig) -> None:
    """A topic turned off in the control table still leaves a trace.

    Silence would be indistinguishable from a broken scheduler; a SKIPPED row tells the
    support team the job ran and chose not to consume.
    """
    tables.ensure_audit_table(spark, cfg)
    LOG.warning("Topic '%s' is disabled in the control table - nothing consumed.", cfg.topic_key)
    AuditWriter(spark, cfg, _make_run_id(cfg)).emit(
        LAYER_LANDING, STATUS_SKIPPED, BATCH_ID_NON_STREAMING
    )


def run(spark: SparkSession, cfg: TopicConfig, secrets: SecretResolver) -> None:
    """Single dispatch point. Every entrypoint calls exactly this."""
    if not cfg.enabled:
        # Disabling is the emergency stop; it stops replays too. Re-enable first.
        run_disabled(spark, cfg)
        return
    if cfg.run.run_type == RUN_TYPE_CURATED_REPLAY:
        run_curated_replay(spark, cfg, secrets)
    elif cfg.run.run_type == RUN_TYPE_KAFKA_REPLAY and cfg.run.is_bounded:
        run_bounded_replay(spark, cfg, secrets)
    else:
        run_streaming(spark, cfg, secrets)
