"""How a batch reaches Delta: append with idempotency markers, or MERGE.

These two functions are where re-run correctness is decided, so they are deliberately the
smallest, most boring code in the framework.

APPEND carries the Delta idempotent-write markers, txnAppId and txnVersion. Delta records
the pair in the table's transaction log and skips a write whose version it has already
seen, which is what makes a retried microbatch a no-op instead of a duplicate. The markers
apply to APPENDS ONLY - Delta does not honour them on a MERGE - so `merge()` does not take
them, and a replay's idempotency comes from its merge key instead.

The app id must be STABLE across runs for this to work, which is why a batch source's
txnVersion is the durable run_sequence from framework/state.py and not a counter in memory.

MERGE REQUIRES A PARTITION PREDICATE, as a positional argument with no default. A MERGE
whose ON clause names only the merge keys must scan and rewrite every partition it might
match, so on a table with three years of daily partitions one replay of one day rewrites
three years of history. Making the predicate impossible to omit is the only version of this
rule that survives contact with a 3am incident.

NO PYSPARK IMPORT: everything here is a method call on objects the caller supplies, and
delta.tables is imported inside the one function that needs it. That is what lets the whole
write path be tested against recording stand-ins with no Delta, no JVM and no cluster.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

LOG = logging.getLogger(__name__)

# The legacy, session-scoped way to let a MERGE widen the target schema. The
# operation-scoped builder method is preferred where the runtime has it; which mechanisms
# exist on the target runtime is VB-09.
AUTO_MERGE_CONF = "spark.databricks.delta.schema.autoMerge.enabled"

# MERGE aliases. Fixed rather than parameterised so every partition predicate in the
# codebase reads the same: `t.<partition_column> = ...`.
TARGET_ALIAS = "t"
SOURCE_ALIAS = "s"


def append(
    df: Any,
    table: str,
    *,
    txn_app_id: str | None = None,
    txn_version: int | None = None,
    merge_schema: bool = False,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Append a batch, with Delta's idempotent-write markers when the caller has an id.

    Both markers or neither: a txnAppId with no version, or a version with no id, records
    nothing and would leave a retried batch free to duplicate. A negative version means the
    caller has no real batch identity (a bounded read outside a microbatch), so the markers
    are skipped and idempotency has to come from somewhere else - normally `merge()`.
    """
    writer = df.write.format("delta").mode("append")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if txn_app_id is not None and txn_version is not None and txn_version >= 0:
        writer = writer.option("txnAppId", txn_app_id).option("txnVersion", int(txn_version))
    writer.saveAsTable(table)


def merge(
    spark: Any,
    df: Any,
    table: str,
    keys: Sequence[str],
    partition_predicate: str,
    *,
    update_matched: bool = False,
    schema_evolution: bool = False,
) -> None:
    """Upsert a batch. `partition_predicate` is required - see the module docstring.

    `keys` are the columns that identify a row; the predicate is a SQL expression over the
    TARGET table's partition columns, written against the `t` alias, e.g.
    `t.ingest_date = '2026-08-11'` or `t.ingest_date BETWEEN '...' AND '...'`. It is ANDed
    ahead of the key equality so the plan prunes before it matches.

    `update_matched` is False by default because the safe answer is insert-if-absent: a
    landing row records what arrived, and rewriting it with a replay's provenance destroys
    that record. A curated-style layer whose whole purpose is to REPLACE a bad parse with a
    good one passes True.
    """
    from delta.tables import DeltaTable

    if not keys:
        raise ValueError(f"merge into {table} needs at least one key column to match on")
    if not partition_predicate or not str(partition_predicate).strip():
        raise ValueError(
            f"merge into {table} needs a partition predicate over the target's partition "
            "columns. A MERGE without one rewrites every partition it might match. If the "
            "target genuinely has no partitions, pass 'true' and say so at the call site."
        )

    key_match = " AND ".join(f"{TARGET_ALIAS}.{key} = {SOURCE_ALIAS}.{key}" for key in keys)
    condition = f"({partition_predicate}) AND {key_match}"
    builder = DeltaTable.forName(spark, table).alias(TARGET_ALIAS).merge(df.alias(SOURCE_ALIAS), condition)

    if not schema_evolution:
        _clauses(builder, update_matched).execute()
        return

    # TWO MECHANISMS, BECAUSE THE RUNTIME FLOOR IS NOT PINNED. withSchemaEvolution() is the
    # documented, operation-scoped way and needs a recent runtime; older ones have only the
    # session flag, which Databricks describes as legacy. Use the method where it exists,
    # otherwise scope the flag to this one operation and put it back. Either way the blast
    # radius is one merge. Which of the two the target runtime has is VB-09.
    if hasattr(builder, "withSchemaEvolution"):
        _clauses(builder.withSchemaEvolution(), update_matched).execute()
        return

    LOG.info("This runtime has no withSchemaEvolution(); scoping %s to this merge instead.", AUTO_MERGE_CONF)
    previous = spark.conf.get(AUTO_MERGE_CONF, None)
    spark.conf.set(AUTO_MERGE_CONF, "true")
    try:
        _clauses(builder, update_matched).execute()
    finally:
        # Restore rather than always unset: another job on a shared session may have set it
        # deliberately, and silently clearing it would be a surprising side effect.
        if previous is None:
            spark.conf.unset(AUTO_MERGE_CONF)
        else:
            spark.conf.set(AUTO_MERGE_CONF, previous)


def _clauses(builder: Any, update_matched: bool) -> Any:
    if update_matched:
        builder = builder.whenMatchedUpdateAll()
    return builder.whenNotMatchedInsertAll()


def split_quarantine(df: Any, is_valid: str) -> tuple[Any, Any]:
    """Split one frame into (valid, quarantined) on a SQL expression. Total by construction.

    `coalesce(..., false)` is not decoration: a three-valued predicate that evaluates to
    NULL belongs to neither `cond` nor `NOT cond`, so the plain form would drop those rows
    from BOTH sides. Anything not provably valid is quarantined, which is the safe
    direction - a quarantined good record is recoverable, a dropped one is not.
    """
    valid = f"coalesce({is_valid}, false)"
    return df.filter(valid), df.filter(f"NOT {valid}")
