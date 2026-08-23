"""The orchestrator: runs cleaning steps in dependency order."""

import pandas as pd

from src.cleaners import (
    AmountNormalizer,
    BalanceReconstructor,
    CityNormalizer,
    CodeNormalizer,
    DateNormalizer,
    DuplicateCleaner,
    MacroCleaner,
    MccResolver,
    MerchantCleaner,
    MissingValueHandler,
    TimestampNormalizer,
)
from src.config.policy import Policy
from src.config.policy import load as load_policy
from src.utils.report import CleaningReport
from src.validators import ConsistencyValidator

# Every step the pipeline can run, by the name a config file calls it. The
# indirection exists so a profile is a list of strings in YAML rather than a
# list of imports in Python -- which is what lets a new source be described
# instead of coded.
STEP_REGISTRY: dict[str, type] = {
    "dates": DateNormalizer,
    "timestamps": TimestampNormalizer,
    "macro": MacroCleaner,
    "duplicates": DuplicateCleaner,
    "codes": CodeNormalizer,
    "amounts": AmountNormalizer,
    "balance": BalanceReconstructor,
    "missing": MissingValueHandler,
    "merchant": MerchantCleaner,
    "geo": CityNormalizer,
    "mcc": MccResolver,
    "consistency": ConsistencyValidator,
}


def steps_for(names: list[str] | tuple[str, ...]) -> list[type]:
    """
    :param names: Step names in run order.
    :returns: The classes they name.
    :raises KeyError: If a name is not registered, listing what is -- a typo
        in a profile must fail at load, not by silently skipping a step.
    """
    unknown = [n for n in names if n not in STEP_REGISTRY]
    if unknown:
        raise KeyError(
            f"unknown pipeline step(s) {unknown}; "
            f"known steps: {sorted(STEP_REGISTRY)}"
        )
    return [STEP_REGISTRY[n] for n in names]


# Order is dictated by real dependencies, not preference. Dates precede
# DuplicateCleaner's ID sequencing because that orders by date, and sorting
# mixed-format date strings orders garbage. AmountNormalizer follows
# CodeNormalizer because it signs each amount by the transaction type that
# step resolves. MccResolver follows MerchantCleaner because it groups by the
# cleaned merchant name.
DEFAULT_STEPS = [
    DateNormalizer,
    DuplicateCleaner,
    CodeNormalizer,
    AmountNormalizer,
    MissingValueHandler,
    MerchantCleaner,
    CityNormalizer,
    MccResolver,
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

    def __init__(
        self,
        steps: list[type] | None = None,
        mcc_reference: dict | None = None,
        policy: Policy | None = None,
        **config,
    ):
        self.steps = steps if steps is not None else list(DEFAULT_STEPS)
        self.mcc_reference = mcc_reference or {}
        self.config = config
        # Resolved once, here, and handed to every step. Loading eagerly means
        # a malformed policy file fails before the first row is touched, and
        # it guarantees all nine steps read the same generation of the rules
        # -- which a lazy per-step load could not promise if the file changed
        # underneath a long run.
        self.policy = policy if policy is not None else load_policy()
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
            step = step_class(self.report, policy=self.policy, **self.config)
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
