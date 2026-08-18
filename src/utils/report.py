"""The shared audit trail every cleaner writes into."""

from dataclasses import dataclass, field


@dataclass
class CleaningReport:
    """
    Collects what each pipeline step did.

    One instance is passed to every cleaner so the run produces a single
    summary. Without it, a step that quietly nulls 400 rows is
    indistinguishable from one that changed nothing.
    """

    entries: list[tuple[str, str, object]] = field(default_factory=list)

    def record(self, step: str, metric: str, value) -> None:
        """
        :param step: Name of the cleaner reporting.
        :param metric: What is being counted.
        :param value: The count or value.
        """
        self.entries.append((step, metric, value))

    def to_frame(self):
        """:returns: The report as a DataFrame, ready to write as a sheet."""
        import pandas as pd

        return pd.DataFrame(self.entries, columns=["step", "metric", "value"])

    def __str__(self) -> str:
        if not self.entries:
            return "(nothing recorded)"
        width = max(len(s) for s, _, _ in self.entries)
        return "\n".join(
            f"  {s:<{width}}  {m:<34} {v}" for s, m, v in self.entries
        )
