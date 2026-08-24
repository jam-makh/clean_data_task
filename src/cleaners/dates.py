"""Date normalization across the five formats present in the source."""

import re
from datetime import datetime

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.utils import audit

DATE_COLUMNS = {
    "TXN_DATE_TIME": "TXN_DATE_TIME_CLEANED",
    "SETTLE_DATE": "SETTLE_DATE_CLEANED",
}

# What the ``*_FORMAT`` column says when no format rule produced the value.
#
# NULL_TOKEN     the source said "no date" -- blank, or a date-shaped sentinel
#                like 0000-00-00. Nothing was lost.
# UNPARSEABLE    the source said something, and no rule could read it. This is
#                a format we do not handle yet, which is a bug signal rather
#                than a missing value.
NULL_TOKEN = "NULL_TOKEN"
UNPARSEABLE = "UNPARSEABLE"


def format_column(source: str) -> str:
    """:returns: The name of the column recording how ``source`` was read."""
    return f"{source}_FORMAT"


class DateNormalizer(BaseCleaner):
    """
    Parses every date column to real datetimes, or ``NaT`` beside the reason.

    Formats are tried in a fixed order and nothing is ever guessed. Which rule
    won is written to a ``*_FORMAT`` column on the row itself, so the question
    "how was this date read" is answerable per transaction and not only as a
    total at the bottom of the report.
    """

    name = "dates"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        formats, null_tokens = loader.date_formats()
        compiled = [
            (re.compile(f["regex"]), f["strptime"], f["name"])
            for f in formats
        ]
        df = df.copy()

        for source, target in DATE_COLUMNS.items():
            if source not in df.columns:
                continue
            # One pure function of one cell, returning both the value and the
            # reason for it. Nothing accumulates: the reason lands in a column
            # beside the value rather than in a counter the caller holds.
            read = df[source].map(
                lambda v: self._read(v, compiled, null_tokens)
            )
            df[target] = pd.to_datetime(read.map(lambda r: r[0]))
            df[format_column(source)] = read.map(lambda r: r[1])

        return df

    def metrics(self, df: pd.DataFrame):
        for source in DATE_COLUMNS:
            column = format_column(source)
            if column not in df.columns:
                continue
            read = df[column]
            yield f"{source}.unparseable", audit.rows(read.eq(UNPARSEABLE))
            yield (
                f"{source}.missing_or_placeholder",
                audit.rows(read.eq(NULL_TOKEN)),
            )
            named = read[~read.isin((UNPARSEABLE, NULL_TOKEN))]
            for name, count in audit.ranked(named):
                yield f"{source}.format[{name}]", count

    @staticmethod
    def _read(value, compiled, null_tokens) -> tuple[datetime | None, str]:
        """
        :param value: Raw cell value.
        :param compiled: Ordered (regex, strptime, name) triples.
        :param null_tokens: Strings meaning "no date", including date-shaped
            sentinels such as ``0000-00-00`` and epoch zero.
        :returns: (the datetime or None, the name of the rule that read it).
        """
        text = "" if value is None or value != value else str(value).strip()
        if text in null_tokens:
            return None, NULL_TOKEN
        for regex, fmt, fmt_name in compiled:
            if not regex.match(text):
                continue
            try:
                if fmt == "EPOCH_MS":
                    result = pd.to_datetime(
                        int(text), unit="ms"
                    ).to_pydatetime()
                else:
                    result = datetime.strptime(text, fmt)
            except (ValueError, OverflowError):
                # The shape matched and the value did not. Reported as
                # unparseable rather than credited to the rule that nearly
                # read it.
                return None, UNPARSEABLE
            return result, fmt_name
        return None, UNPARSEABLE
