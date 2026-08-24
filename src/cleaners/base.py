"""The contract every cleaning step implements."""

from abc import ABC, abstractmethod
from collections.abc import Iterator

import pandas as pd

from src.config import policy as policy_module
from src.utils.report import CleaningReport


class BaseCleaner(ABC):
    """
    One cleaning concern: take a frame, return a frame that records on every
    row what was done to it.

    Every step has this same shape, which is what lets the pipeline hold them
    in a list and run them without knowing what any of them does.

    Two methods, and the split between them is the whole contract:

    ``apply`` **marks**. It writes a diagnostic column stating what happened
    to each row -- which date format was read, whether an amount had to be
    reformatted, whether a balance reconciled -- and computes no totals at
    all.

    ``metrics`` **counts**, once, after every step has run, reading only
    columns that are on the frame by then.

    The separation is not tidiness. A step that counted as it went had to keep
    a running total somewhere outside the rows, and the only places to keep
    one are a closure or an accumulating call like ``.sum()``. Both are
    single-process constructs: distributed across executors, a closure counter
    is filled in on each worker and read back empty on the driver, silently,
    with the report still rendering plausible-looking zeros. Marking a row is
    the operation that survives the move, because the mark travels with the
    row it describes.

    It also makes the audit trail a property of the data rather than of the
    run. "How many amounts were reformatted" and "was *this* amount
    reformatted" stop being two different questions answered in two different
    places.

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
        Cleans, and marks each row with what was done to it.

        :param df: Frame to clean.
        :returns: A new frame; the input is not mutated.
        """

    def metrics(self, df: pd.DataFrame) -> Iterator[tuple[str, object]]:
        """
        Derives this step's report rows from the columns it marked.

        Called once, on the finished frame, after every step has run. A step
        whose columns are absent -- because it was skipped, or because the
        source had nothing for it to do -- yields nothing, so column presence
        is the guard rather than a flag someone has to remember to set.

        :param df: The frame as it left the last step.
        :returns: ``(metric, value)`` pairs, in the order they should be read.
        """
        return iter(())

    def collect(self, df: pd.DataFrame) -> None:
        """
        Writes this step's metrics into the shared report.

        :param df: The frame as it left the last step.
        """
        for metric, value in self.metrics(df):
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
