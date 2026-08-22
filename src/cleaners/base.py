"""The contract every cleaning step implements."""

from abc import ABC, abstractmethod

import pandas as pd

from src.config import policy as policy_module
from src.utils.report import CleaningReport


class BaseCleaner(ABC):
    """
    One cleaning concern: take a frame, return a frame, record what happened.

    Every step has this same shape, which is what lets the pipeline hold them
    in a list and run them without knowing what any of them does.

    :param report: Shared report all steps write into.
    :param policy: Cleaning policy; loaded from the default file when absent.
    """

    name: str = "base"

    def __init__(
        self,
        report: CleaningReport,
        *,
        policy: policy_module.Policy | None = None,
        **config,
    ):
        self.report = report
        self.config = config
        self._policy = policy

    @property
    def policy(self) -> policy_module.Policy:
        """
        The policy this step reads its thresholds from.

        Injected by the orchestrator in a normal run, and resolved lazily from
        the default file otherwise, so a single cleaner is still usable on its
        own in a notebook or a test. The injection is the load-bearing half:
        in Stage 2 the orchestrator builds one policy on the Spark driver and
        hands it down, because an executor resolving its own would be reading
        a file that may not be there.

        :returns: The validated policy.
        """
        if self._policy is None:
            self._policy = policy_module.load()
        return self._policy

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        :param df: Frame to clean.
        :returns: A new frame; the input is not mutated.
        """

    def log(self, metric: str, value) -> None:
        """
        :param metric: What is being counted.
        :param value: The count.
        """
        self.report.record(self.name, metric, value)

    @staticmethod
    def text(value) -> str:
        """
        :param value: Any cell value.
        :returns: Stripped string, empty for nulls.
        """
        if value is None or value != value:
            return ""
        return str(value).strip()
