"""The orchestrator: runs cleaning steps in dependency order."""

import pandas as pd

from cleaning_task.cleaners import (
    AmountNormalizer,
    CityNormalizer,
    CodeNormalizer,
    DateNormalizer,
    DuplicateCleaner,
    MerchantCleaner,
    MissingValueHandler,
)
from cleaning_task.utils.report import CleaningReport
from cleaning_task.validators import ConsistencyValidator, MccValidator

# Order is dictated by real dependencies, not preference. Dates precede
# DuplicateCleaner's ID sequencing because that orders by date, and sorting
# mixed-format date strings orders garbage. MccValidator follows
# MerchantCleaner because it groups by the cleaned merchant name.
DEFAULT_STEPS = [
    DateNormalizer,
    DuplicateCleaner,
    AmountNormalizer,
    CodeNormalizer,
    MissingValueHandler,
    MerchantCleaner,
    CityNormalizer,
    MccValidator,
    ConsistencyValidator,
]


class TransactionCleaner:
    """
    Runs a list of steps over a frame and collects one report.

    Steps are injected rather than hardcoded, so a caller can run a single
    cleaner while developing or skip steps that do not apply to their file.

    :param steps: Cleaner classes to run; defaults to the full pipeline.
    :param mcc_reference: MCC code to category, from the workbook.
    """

    def __init__(self, steps: list[type] | None = None, mcc_reference: dict | None = None, **config):
        self.steps = steps if steps is not None else list(DEFAULT_STEPS)
        self.mcc_reference = mcc_reference or {}
        self.config = config
        self.report = CleaningReport()
        self.result: pd.DataFrame | None = None
        self._instances: dict[str, object] = {}

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        :param df: Raw transactions.
        :returns: The cleaned frame, originals untouched.
        """
        self.report.record("pipeline", "input_rows", len(df))
        current = df.copy()

        for step_class in self.steps:
            step = step_class(self.report, **self.config)
            self._instances[step.name] = step
            if isinstance(step, CodeNormalizer):
                current = step.apply(current, mcc_reference=self.mcc_reference)
            else:
                current = step.apply(current)

        self.report.record("pipeline", "output_rows", len(current))
        self.result = current
        return current

    def step(self, name: str):
        """
        :param name: Step name, e.g. ``"mcc"``.
        :returns: The instance that ran, for access to its extra output.
        """
        return self._instances.get(name)
