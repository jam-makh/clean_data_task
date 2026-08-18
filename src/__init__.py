"""Transaction cleaning pipeline.

The library half: the steps, the orchestrator and the report. The entry point
that composes them lives in the repo-root ``main.py``, so this package never
imports it -- a package that reaches back out to a root-level module cannot be
imported from anywhere but the repo root.
"""

from src.pipeline import TransactionCleaner
from src.utils.report import CleaningReport

__all__ = ["TransactionCleaner", "CleaningReport"]
__version__ = "0.1.0"
