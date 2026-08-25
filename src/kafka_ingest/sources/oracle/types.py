"""Column types: overriding them going in, and noticing when they change. NO PySpark import.

Three jobs, and nothing else belongs here:

  GOING IN     `custom_schema()` renders the optional per-source `column_types` map into
               the JDBC `customSchema` option, which is how a column whose driver default
               mapping is wrong gets the type it should have had.
  COMING OUT   `describe()` turns the schema Spark actually resolved into plain data, so
               the run can record it in the audit row's `source_detail`. What the source
               looked like on the day is otherwise unreconstructable.
  BETWEEN RUNS `drift()` compares that schema against the landing table's and reports the
               NON-ADDITIVE differences - a dropped column, or a column whose type changed.

WHY DRIFT IS A RUN FAILURE AND NOT A WARNING
--------------------------------------------
A silent type change on an Oracle column is invisible until a consumer breaks. `NUMBER(9)`
widened to `NUMBER(18)`, a `VARCHAR2` that became a `CLOB` - the extract keeps succeeding,
the row counts stay plausible, and what changes is the values. So a non-additive change
stops the run and names the column. A NEW column is additive and is allowed: adding one is
the ordinary way a source table evolves, and the write path lets Delta widen the target.

WHAT THIS MODULE CANNOT DO, AND WHY IT SAYS SO
-----------------------------------------------
It cannot name the ORACLE type behind a column: by the time Spark hands over a schema, the
driver's mapping has already happened and the original type name is gone. Recovering it
needs a second query against `ALL_TAB_COLUMNS`, which is a round trip this framework does
not make. What it CAN catch is the driver telling us it mapped nothing - a `void` column -
and that is exactly the case the stage brief calls "unmapped". VB-04 is the standing entry
for the types that need a human to look at the source schema first.

NO PYSPARK IMPORT. A Spark `StructType` is read here through `.fields`, `.name` and
`.dataType.simpleString()`, which is duck typing on purpose: it keeps this module - and
every test of it - runnable with no JVM.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from ...framework.config import ConfigError

# A Spark type name as it may be written in `column_types`. Deliberately narrow: a bare
# type name, optionally with a precision/scale or a length. Anything nested (ARRAY<...>,
# STRUCT<...>) is refused, because a JDBC read of an Oracle table never produces one and a
# `customSchema` this framework cannot verify is worse than none.
_SPARK_TYPE = re.compile(r"^[A-Za-z]+(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$")

# What Spark calls a column it could not map to any type. The driver produced no mapping
# and the values would arrive as NULL - silently, on every row.
_UNMAPPED = ("void", "null")


def custom_schema(column_types: Mapping[str, str], source_key: str) -> str | None:
    """Render the JDBC `customSchema` option, or None when nothing is overridden.

    `customSchema` names only the columns being overridden; every other column keeps the
    driver's default mapping, which is the right default - see CORE section 11 and VB-02.
    """
    if not column_types:
        return None
    parts = []
    for column, spark_type in column_types.items():
        if not _SPARK_TYPE.match(str(spark_type).strip()):
            raise ConfigError(
                f"source '{source_key}': column_types['{column}'] is '{spark_type}', which is "
                "not a plain Spark type such as DECIMAL(38,10), STRING or TIMESTAMP. It is "
                "passed to the JDBC reader as customSchema, where an unparseable entry fails "
                "the read with an error that names neither the column nor this setting."
            )
        parts.append(f"{column} {str(spark_type).strip()}")
    return ", ".join(parts)


def describe(schema: Any) -> dict[str, str]:
    """Column name -> Spark type, as plain data for the audit row and for `drift()`."""
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def refuse_unmapped(schema: Any, source_key: str) -> None:
    """Stop the run when the driver mapped a column to nothing.

    The alternative - landing a column of NULLs - is the failure this framework exists to
    prevent: it succeeds, it reports a row count, and the data is not there. The fix is
    always at the source end (project the column away, or give it an explicit
    `column_types` entry), so the message says both.
    """
    unmapped = sorted(name for name, spark_type in describe(schema).items() if spark_type.lower() in _UNMAPPED)
    if unmapped:
        raise ConfigError(
            f"source '{source_key}': the JDBC driver could not map {unmapped} to a Spark type, "
            "so every value in them would land as NULL. Either exclude the column from the "
            "extract (`columns:` or `sql_query:`) or give it an explicit `column_types:` "
            "entry. See VB-04 for the Oracle types this usually means."
        )


def drift(existing: Mapping[str, str], incoming: Mapping[str, str]) -> list[str]:
    """Non-additive differences, as sentences. Empty means the read is safe to land.

    Additive changes - a column present in `incoming` and absent from `existing` - are
    deliberately not reported: a new column on the source table is the ordinary way a
    schema evolves, and the write path widens the target for it.
    """
    changes = []
    for column, was in existing.items():
        if column not in incoming:
            changes.append(f"column '{column}' ({was}) is no longer returned by the extract")
        elif incoming[column] != was:
            changes.append(f"column '{column}' changed type from {was} to {incoming[column]}")
    return changes


def assert_no_drift(existing: Mapping[str, str], incoming: Mapping[str, str], source_key: str, table: str) -> None:
    """`drift()`, raised. One call site, in run.py, before anything is written."""
    changes = drift(existing, incoming)
    if changes:
        raise ConfigError(
            f"source '{source_key}': the extract's schema no longer matches {table} - "
            + "; ".join(changes)
            + ". A type change is invisible to a row count and breaks the consumer rather "
            "than the pipeline, so the run stops here. Decide deliberately: ALTER the "
            "landing table, pin the old type with `column_types:`, or accept the change and "
            "recreate the table."
        )
