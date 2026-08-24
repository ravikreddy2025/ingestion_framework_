"""The kafka source. Public surface: SOURCE_SPEC and run().

Everything else in this package is an implementation detail of run(), and framework/ never
imports any of it. See framework/contracts.py for why the contract is one function.
"""

from .run import run
from .spec import SOURCE_SPEC

__all__ = ["SOURCE_SPEC", "run"]
