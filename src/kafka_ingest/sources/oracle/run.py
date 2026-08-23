"""The oracle source's one function. Stage 4 implements it.

`run(ctx)` is the WHOLE contract, alongside SOURCE_SPEC in spec.py. There is no `read()`,
`parse()`, `write()` or `validate()` here and none may be added - see
framework/contracts.py.
"""

from __future__ import annotations

from ...framework.contracts import RunContext, RunResult


def run(ctx: RunContext) -> RunResult:
    raise NotImplementedError(
        "the oracle source is implemented in Stage 4; framework/runner.py already dispatches to it"
    )
