"""What the framework needs to know about a Kafka source. NO PySpark import.

STAGE 1 STUB. The key sets below are deliberately incomplete: Stage 3 ports the Kafka
implementation and fills them in from what the code actually reads. They are left empty
rather than guessed at, because a key listed here that nothing reads is exactly the
"silently does nothing" outcome CORE section 2 rule 2 forbids.

Resolving a real conf/sources/*.yaml Kafka file against this stub therefore fails with an
unknown-key error, which is the honest answer until Stage 3. The legacy Kafka loader in
kafka_ingest/config.py is what still resolves those files today.
"""

from __future__ import annotations

from ...framework.contracts import SourceSpec

SOURCE_SPEC = SourceSpec(
    source_type="kafka",
    required_keys=frozenset(),
    structural_keys=frozenset(),
    operational_keys=frozenset(),
    mutually_exclusive=(),
    # Kafka is the only source with all three layers: raw wire bytes land, the payload is
    # parsed into curated, and records that cannot be parsed go to quarantine.
    layers=("landing", "curated", "quarantine"),
    # NOT a stub. conf/defaults/kafka.yaml already names all three targets
    # `{catalog}.<layer>.{topic_table}`, and {topic_table} is the topic name with dots and
    # hyphens turned into underscores - a value only this source can compute. Declaring it
    # here is what lets that pattern survive configuration load; framework/tables.py
    # renders it when Stage 3 supplies the token.
    target_tokens=frozenset({"topic_table"}),
)
