"""The oracle source. Public surface: SOURCE_SPEC and run()."""

from .run import run
from .spec import SOURCE_SPEC

__all__ = ["SOURCE_SPEC", "run"]
