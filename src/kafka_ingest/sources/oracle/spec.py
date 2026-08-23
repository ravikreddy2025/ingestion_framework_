"""What the framework needs to know about an oracle source. NO PySpark import.

STAGE 1 STUB. The key sets below are deliberately empty: Stage 4 builds the oracle
source and fills them in from what the code actually reads. They are left empty rather
than guessed at, because a key listed here that nothing reads is exactly the "silently
does nothing" outcome CORE section 2 rule 2 forbids.
"""

from __future__ import annotations

from ...framework.contracts import SourceSpec

SOURCE_SPEC = SourceSpec(
    source_type="oracle",
    required_keys=frozenset(),
    structural_keys=frozenset(),
    operational_keys=frozenset(),
    mutually_exclusive=(),
    # CORE section 10: Oracle lands only. A curated layer for Oracle is out of scope.
    layers=("landing",),
)
