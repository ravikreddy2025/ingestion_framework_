"""sources/oracle/types.py - type overrides going in, and schema drift between runs.

The schemas here are stand-ins (conftest.FakeSchema): types.py reads `.fields`, `.name`
and `.dataType.simpleString()` by duck typing precisely so that the drift rules - the part
that decides whether a run stops - can be tested with no JVM.
"""

from __future__ import annotations

import pytest

from conftest import FakeSchema
from kafka_ingest.framework.config import ConfigError
from kafka_ingest.sources.oracle import types

BEFORE = {"CLAIM_ID": "decimal(38,0)", "AMOUNT": "decimal(18,2)", "STATUS": "string"}


# --------------------------------------------------------------------------------------
# customSchema
# --------------------------------------------------------------------------------------


def test_no_overrides_means_no_option_at_all():
    """The right default is the driver's own mapping. An empty customSchema string would
    be an option Spark has to interpret, for no reason."""
    assert types.custom_schema({}, "demo_oracle") is None


def test_overrides_render_in_the_option_s_own_grammar():
    rendered = types.custom_schema({"AMOUNT": "DECIMAL(38,10)", "NOTES": "STRING"}, "demo_oracle")
    assert rendered == "AMOUNT DECIMAL(38,10), NOTES STRING"


@pytest.mark.parametrize("spark_type", ["ARRAY<STRING>", "DECIMAL(38,10) NOT NULL", "; DROP", "", "STRUCT<a:INT>"])
def test_a_type_this_module_cannot_verify_is_refused(spark_type):
    """An unparseable customSchema fails the read with an error that names neither the
    column nor this setting, an hour in. A JDBC read of an Oracle table never produces a
    nested type, so refusing them costs nothing."""
    with pytest.raises(ConfigError, match="column_types"):
        types.custom_schema({"AMOUNT": spark_type}, "demo_oracle")


# --------------------------------------------------------------------------------------
# What the read actually returned
# --------------------------------------------------------------------------------------


def test_the_resolved_schema_becomes_plain_data_for_the_audit_row():
    """What the source looked like on the day is otherwise unreconstructable - the config
    says which columns were asked for, never which types came back."""
    assert types.describe(FakeSchema(BEFORE)) == BEFORE


def test_a_column_the_driver_could_not_map_stops_the_run():
    """A `void` column lands as NULL on every row. The run succeeds, the row count is
    plausible, and the data is not there - which is the exact failure this framework
    exists to prevent."""
    with pytest.raises(ConfigError, match="NOTES"):
        types.refuse_unmapped(FakeSchema({"CLAIM_ID": "decimal(38,0)", "NOTES": "void"}), "demo_oracle")


def test_a_fully_mapped_schema_passes_silently():
    types.refuse_unmapped(FakeSchema(BEFORE), "demo_oracle")


# --------------------------------------------------------------------------------------
# Drift
# --------------------------------------------------------------------------------------


def test_an_unchanged_schema_has_no_drift():
    assert types.drift(BEFORE, dict(BEFORE)) == []


def test_a_new_column_is_additive_and_allowed():
    """Adding a column is the ordinary way a source table evolves, and the write path lets
    Delta widen the target for it. Failing here would make every source-side release an
    ingestion incident."""
    assert types.drift(BEFORE, {**BEFORE, "SUBMITTED_BY": "string"}) == []


def test_a_type_change_is_reported_with_both_types():
    """The one that is invisible to a row count: `NUMBER(9)` widened to `NUMBER(18)` keeps
    extracting successfully and changes the values a consumer reads."""
    changes = types.drift(BEFORE, {**BEFORE, "AMOUNT": "double"})
    assert changes == ["column 'AMOUNT' changed type from decimal(18,2) to double"]


def test_a_column_that_stopped_being_returned_is_reported():
    """Either the source dropped it or somebody edited `columns:`. Both are worth stopping
    for: the landing table's history would silently gain a wall of NULLs."""
    remaining = {key: value for key, value in BEFORE.items() if key != "STATUS"}
    assert types.drift(BEFORE, remaining) == ["column 'STATUS' (string) is no longer returned by the extract"]


def test_assert_no_drift_names_the_table_and_every_change():
    """One message carrying everything a support engineer needs to decide between altering
    the table, pinning the type, and recreating."""
    with pytest.raises(ConfigError) as excinfo:
        types.assert_no_drift(BEFORE, {"CLAIM_ID": "string"}, "demo_oracle", "cat.oracle_claims.claim_header")
    message = str(excinfo.value)
    assert "cat.oracle_claims.claim_header" in message
    assert "AMOUNT" in message and "STATUS" in message and "CLAIM_ID" in message


def test_assert_no_drift_is_silent_when_nothing_changed():
    types.assert_no_drift(BEFORE, {**BEFORE, "NEW_COL": "string"}, "demo_oracle", "cat.oracle_claims.claim_header")
