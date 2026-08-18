"""The contract every cleaning step implements."""

from abc import ABC, abstractmethod

import pandas as pd

from src.utils.report import CleaningReport


class BaseCleaner(ABC):
    """
    One cleaning concern: take a frame, return a frame, record what happened.

    Every step has this same shape, which is what lets the pipeline hold them
    in a list and run them without knowing what any of them does.

    :param report: Shared report all steps write into.
    """

    name: str = "base"

    def __init__(self, report: CleaningReport, **config):
        self.report = report
        self.config = config

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
