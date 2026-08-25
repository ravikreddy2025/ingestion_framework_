"""The checkpoint-reset guard, shared by every checkpoint-based source.

Structured Streaming writes a batch's offset range to the checkpoint BEFORE running
foreachBatch, and restarts `batchId` at 0 the moment the checkpoint is gone. If the target
table already holds committed Delta versions for this run's txnAppId, every write from
batch 0 onward is then SKIPPED as a duplicate - the job reports success and ingests
nothing. That is true of any checkpoint-based source, not of one source type's plumbing, so
the guard against it lives here once rather than as near-identical copies in each source's
own run.py - see each checkpoint-based source's run() for how it calls this.

Read `guard_against_checkpoint_reset` in order; the three states below are IT.
"""

from __future__ import annotations

import os

from .contracts import RunContext, SourceSpec

# The framework's own vocabulary for a non-replay run - see contracts.RunContext.run_type
# and framework/runner.py's RUN_TYPE_PRIMARY. Not a source name, so naming it here does not
# trip the CORE section 7 grep gate; every source-specific replay type is still unknown to
# this module, which is exactly why a replay is never guarded (branch 1 below).
_RUN_TYPE_PRIMARY = "primary"


def control_column_for(spec: SourceSpec, setting: str) -> str:
    """The control-table column that overrides `setting`, from a source's own SOURCE_SPEC.

    A reverse lookup, not a second naming convention: `spec.control_columns` already maps
    column -> setting the other way round (framework/control.py reads it that way - see
    docs/build_log/DECISIONS.md D-01), and restating the mapping backwards here would be a
    second place for the two to disagree. Used to put THIS source's own column name (e.g.
    `kafka_checkpoint_reset_id`) into the guard's refusal messages without this module
    naming a source type to get it.
    """
    return next(column for column, name in spec.control_columns.items() if name == setting)


def guard_against_checkpoint_reset(
    ctx: RunContext,
    *,
    checkpoint_path: str,
    landing_table: str,
    checkpoint_reset_id: str | None,
    is_replay: bool,
    control_column: str,
) -> None:
    """Refuse to run a primary stream in either state that would silently ingest nothing.

    The order of the branches IS the logic, so read them in order:

    1. A REPLAY is never guarded. It has its own checkpoint and its own app id by
       construction, so neither collision is possible.
    2. THE CHECKPOINT EXISTS -> nothing to guard. This is also what makes a stale
       `checkpoint_reset_id` inert rather than a permanent alarm: once the stream has a
       checkpoint again, the field only forks the app id (which must stay forked) and no
       longer bypasses anything.
    3. THE CHECKPOINT IS GONE AND A RESET ID IS SET. This is the deliberate bypass - but
       ONLY if the id has never been used before. Reusing one is the third of the three
       silent-data-loss states, and the nastiest: the guard is bypassed, batch ids restart
       at 0, and the app id is UNCHANGED, so Delta still holds high versions against it and
       skips every write. The run reports success and ingests nothing. Hence the history
       check, and hence a raise rather than a warning.
    4. THE CHECKPOINT IS GONE WITH NO RESET ID -> the original silent-data-loss case.
       Narrow on purpose: a genuine first run has no rows yet in ITS OWN landing table and
       is not tripped, and the check never looks past landing, so pre-loading a downstream
       layer from a legacy system does not trip it either.

    `landing_table` is checked with no further filter: every source that calls this has one
    landing table per source (see `docs/DESIGN.md` for the Kafka case specifically - its
    landing table is one-per-topic, so a `topic` filter here would be redundant, not real
    isolation), so "does the table hold any row at all" is already the right question.

    `control_column` is THIS source's control-table column for the reset id, reverse-looked-
    up by the caller via `control_column_for` - so a refusal message names the exact column
    an operator can set, without this module ever naming a source type.
    """
    if is_replay:
        return
    if _checkpoint_offsets_exist(checkpoint_path):
        return

    if checkpoint_reset_id:
        if _reset_id_already_used(ctx, checkpoint_reset_id):
            raise RuntimeError(
                f"REFUSING TO RUN: checkpoint_reset_id '{checkpoint_reset_id}' has ALREADY been "
                f"used by an earlier run of '{ctx.cfg.source_key}', and the checkpoint is missing "
                f"again ({checkpoint_path}).\n\n"
                "A reset id is single-use. It works by forking this source's Delta transaction "
                "identity, so a restarted stream has no committed versions to collide with. Reusing "
                "one keeps the OLD identity - against which Delta already holds high versions - so "
                "batch ids would restart at 0 and EVERY WRITE WOULD BE SKIPPED AS A DUPLICATE. The "
                "run would report success and ingest nothing.\n\n"
                f"Set {control_column} to an UNUSED incident id (the current incident's, not the "
                "old one's) and re-run. Do not clear the field afterwards. Full procedure: "
                "docs/RUNBOOK_SUPPORT.md 5.4a."
            )
        ctx.log.warning(
            "checkpoint_reset_engaged",
            source_key=ctx.cfg.source_key,
            checkpoint_reset_id=checkpoint_reset_id,
            note=(
                "starting this stream from batch 0 under a NEW transaction identity; do NOT clear "
                "this field afterwards - reverting to the old identity would resurrect the exact "
                "collision it was set to avoid"
            ),
        )
        return

    if not ctx.tables.table_exists(ctx.spark, landing_table):
        return
    already_landed = ctx.spark.table(landing_table).limit(1).count() > 0
    if not already_landed:
        return
    raise RuntimeError(
        f"REFUSING TO RUN: the checkpoint for '{ctx.cfg.source_key}' is missing "
        f"({checkpoint_path}) but {landing_table} already holds rows.\n\n"
        "Running now would restart batch ids at 0, and Delta would silently SKIP every write as a "
        "duplicate - the job would report success and ingest nothing.\n\n"
        f"Set {control_column} to an unused incident id in the control table to restart under a "
        "fresh identity - see docs/RUNBOOK_SUPPORT.md 5.4a."
    )


def _checkpoint_offsets_exist(checkpoint_path: str) -> bool:
    """Is there an offsets directory under this checkpoint?

    Deliberately NOT os.path.exists(): that swallows every OSError and returns False, so a
    Volume the driver cannot reach right now would be indistinguishable from a checkpoint
    someone deleted. The guard above turns "absent" into a hard refusal, so a false "absent"
    blocks a perfectly healthy job.

    os.stat() raises instead, which separates the three cases:
      FileNotFoundError -> genuinely absent, the case the guard exists for
      other OSError     -> we cannot tell; say so rather than guessing either way
      no exception      -> present
    """
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


def _reset_id_already_used(ctx: RunContext, checkpoint_reset_id: str) -> bool:
    """Has this reset id appeared on a PRIMARY run of this source before?

    The audit table is best-effort evidence, which is normally a reason not to depend on it
    - but the failure direction here is safe. A missing row means this reads as "unused", so
    the run proceeds under a forked identity, which is the correct outcome for a genuine
    first use. Being wrong the other way (refusing a legitimate reset) is the more expensive
    mistake, and it is the one this cannot make.

    The current run is excluded explicitly, so this does not depend on being called before
    the reset id reaches the audit writer - though it is, and each source's run() says so.
    """
    audit_table = ctx.cfg.get("audit_table")
    if not audit_table or not ctx.tables.table_exists(ctx.spark, audit_table):
        return False
    rows = (
        ctx.spark.table(audit_table)
        .where(
            f"source_key = '{ctx.cfg.source_key}' AND run_type = '{_RUN_TYPE_PRIMARY}' "
            f"AND rerun_id = '{checkpoint_reset_id}' AND run_id <> '{ctx.run_id}'"
        )
        .limit(1)
        .collect()
    )
    return bool(rows)
