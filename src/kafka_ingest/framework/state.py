"""Durable run state: the watermark, and the run sequence.

**STATE WRITES ARE MANDATORY AND MUST RAISE ON FAILURE.** That is the whole reason this is
a separate table from the audit table, whose writes must never raise. Extraction
correctness depends on state - a watermark that silently failed to advance re-reads a
window, and one that silently advanced skips one - and it must not depend on best-effort
audit. Anyone reading this next to framework/audit.py will be tempted to make the two
consistent. Do not: the asymmetry is the design.

For the same reason, never derive a watermark from the audit table.

TWO KEYS, AND THE TABLE STAYS KEY/VALUE
---------------------------------------
    watermark      the last committed cursor value for a source that has no checkpoint.
                   Advanced ONLY after the write it covers has committed.
    run_sequence   a monotonically increasing integer per source_key, used as the Delta
                   txnVersion by sources with no Spark microbatch id. It gives a bounded
                   batch read the same idempotent-write protection a streaming source gets
                   for free from its batch id, which is why it is durable and not a
                   counter in memory.

Do not add a column per source concept. A third state key costs a row; a third column
costs an ALTER TABLE on a shared table and a migration for every environment.

NO PYSPARK IMPORT. The session arrives as an argument and delta.tables is imported inside
the one function that needs it, so this module is importable, and its SQL assertable,
without a cluster.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from . import tables
from .config import ConfigError

# The two state keys the framework itself uses. A source may write others.
STATE_WATERMARK = "watermark"
STATE_RUN_SEQUENCE = "run_sequence"

# Value types recorded alongside the value, so a reader knows how to interpret the text.
TYPE_INT = "int"
TYPE_STRING = "string"

STATE_DDL_COLUMNS = """
    source_key       STRING    NOT NULL COMMENT 'Matches conf/sources/<source_key>.yaml',
    state_key        STRING    NOT NULL COMMENT 'watermark | run_sequence',
    state_value      STRING    COMMENT 'The value, as text. value_type says how to read it.',
    value_type       STRING    COMMENT 'int | string | whatever the writing source recorded',
    updated_at       TIMESTAMP,
    updated_by_run   STRING    COMMENT 'run_id of the run that last wrote this row'
"""

# The DataFrame handed to the MERGE. A DDL string rather than a StructType keeps this
# module free of a PySpark import; the column list is the same one as above.
_STATE_ROW_SCHEMA = (
    "source_key STRING, state_key STRING, state_value STRING, "
    "value_type STRING, updated_at TIMESTAMP, updated_by_run STRING"
)

_KEY_COLUMNS = ("source_key", "state_key")

# Both key columns are interpolated into SQL. They are config-derived - a filename stem and
# a fixed vocabulary - never free-form input, and this refuses anything that is not.
_SAFE_KEY = re.compile(r"^[A-Za-z0-9_.-]+$")


class StateStore:
    """Read and write one source's durable state. Built once per run by the runner.

    ONE RUN PER source_key IS ASSUMED. `next_run_sequence` reads, adds one and writes; two
    concurrent runs of the same source would therefore both see N and both take N+1. That
    is acceptable because the scheduler runs one job per source_key - and if that ever
    stops being true, the txnVersion collision it causes is exactly what Delta's idempotent
    writes turn into a dropped duplicate rather than corruption.
    """

    def __init__(self, spark: Any, table: str | None, run_id: str = ""):
        self._spark = spark
        self.table = table
        self.run_id = run_id

    def read_state(self, source_key: str, state_key: str) -> str | None:
        """The stored value, or None when nothing has been written for this key yet.

        None is a legitimate answer - it is what a first run sees - and callers must treat
        it as "start from the beginning", never as a failure.
        """
        rows = (
            self._spark.table(self._table())
            .where(f"source_key = '{_safe(source_key)}' AND state_key = '{_safe(state_key)}'")
            .limit(2)
            .collect()
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise ConfigError(
                f"{self._table()} holds {len(rows)}+ rows for source_key '{source_key}' "
                f"state_key '{state_key}'. State must be one row per key - deduplicate it "
                "before rerunning, and check which value is correct rather than guessing."
            )
        value = rows[0].asDict().get("state_value")
        return None if value is None else str(value)

    def write_state(self, source_key: str, state_key: str, value: str, value_type: str, run_id: str) -> None:
        """Upsert one state row. Raises on any failure - see the module docstring.

        Deliberately not wrapped, not retried and not logged-and-swallowed. If this fails
        the run must fail, because whatever it was recording did not get recorded.
        """
        from delta.tables import DeltaTable

        row = (
            _safe(source_key),
            _safe(state_key),
            None if value is None else str(value),
            value_type,
            datetime.now(timezone.utc),
            run_id,
        )
        source = self._spark.createDataFrame([row], schema=_STATE_ROW_SCHEMA)
        condition = " AND ".join(f"t.{column} = s.{column}" for column in _KEY_COLUMNS)
        (
            DeltaTable.forName(self._spark, self._table())
            .alias("t")
            .merge(source.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def next_run_sequence(self, source_key: str) -> int:
        """Allocate this run's sequence number: read, add one, write, return.

        The write happens before the number is used, so a run that dies mid-way burns its
        number rather than letting the next run reuse it. Burning numbers is free; reusing
        one means two different runs writing under the same Delta txnVersion, where the
        second is silently dropped as a duplicate.
        """
        current = self.read_state(source_key, STATE_RUN_SEQUENCE)
        following = 1 if current is None else int(current) + 1
        self.write_state(source_key, STATE_RUN_SEQUENCE, str(following), TYPE_INT, self.run_id)
        return following

    def _table(self) -> str:
        if not self.table:
            raise ConfigError(
                "this source asked for durable state but no `state_table` is configured. "
                "Set it in conf/defaults.yaml - state is mandatory for any source that "
                "resumes from a watermark or needs a run sequence."
            )
        return self.table


_DELETION_VECTORS_PROPERTY = {"delta.enableDeletionVectors": "true"}


def ensure_state_table(spark: Any, cfg: Any, table: str) -> None:
    """Create the state table if it does not exist yet. A no-op afterwards.

    PARTITIONED BY (source_key) (docs/build_log/DECISIONS.md D-04). A run sequence is
    allocated on EVERY run of every source, so with many sources on the same schedule this
    table takes concurrent MERGEs from different sources. Delta detects conflicts at file
    granularity, so partitioning by source_key puts each source's rows in disjoint files and
    those concurrent MERGEs stop conflicting with each other. Deletion vectors are enabled
    for the same reason CLUSTER BY is not used here: this table's whole shape is small,
    frequent, single-row MERGE updates, which deletion vectors make cheaper than rewriting a
    file per update.

    Small-file growth from the partitioning is not a concern: a handful of rows per source,
    and the maintenance job already covers this framework's tables.
    """
    tables.ensure_table(
        spark,
        table,
        STATE_DDL_COLUMNS,
        "Durable ingestion state: watermarks and run sequences. Written by the ingestion job only.",
        properties={**tables.effective_properties(cfg.get("table_properties")), **_DELETION_VECTORS_PROPERTY},
        partition_by=["source_key"],
    )


def _safe(value: str) -> str:
    if not isinstance(value, str) or not _SAFE_KEY.match(value):
        raise ConfigError(
            f"state key component {value!r} contains characters that are not allowed - "
            "state keys come from configuration, not from data."
        )
    return value
