"""Date normalization across the five formats present in the source."""

import re
from datetime import datetime

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader

DATE_COLUMNS = {
    "TXN_DATE_TIME": "TXN_DATE_TIME_CLEANED",
    "SETTLE_DATE": "SETTLE_DATE_CLEANED",
}


class DateNormalizer(BaseCleaner):
    """
    Parses every date column to real datetimes, or ``NaT`` with a count.

    Formats are tried in a fixed order and nothing is ever guessed: an
    unrecognised value becomes ``NaT`` and is reported, because an unparsed
    date means a format we do not handle yet, which is a bug signal rather
    than a missing value.
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
            matched: dict[str, int] = {}
            parsed = df[source].map(
                lambda v: self._parse(v, compiled, null_tokens, matched)
            )
            df[target] = pd.to_datetime(parsed)

            nulls = int(df[target].isna().sum())
            blanks = int(
                df[source].map(lambda v: self.text(v) in null_tokens).sum()
            )
            self.log(f"{source}.unparseable", nulls - blanks)
            self.log(f"{source}.missing_or_placeholder", blanks)
            for fmt, count in sorted(matched.items(), key=lambda kv: -kv[1]):
                self.log(f"{source}.format[{fmt}]", count)

        return df

    @staticmethod
    def _parse(value, compiled, null_tokens, matched) -> datetime | None:
        """
        :param value: Raw cell value.
        :param compiled: Ordered (regex, strptime, name) triples.
        :param null_tokens: Strings meaning "no date", including date-shaped
            sentinels such as ``0000-00-00`` and epoch zero.
        :param matched: Mutable counter of which format won.
        :returns: A datetime, or None when nothing matches.
        """
        text = "" if value is None or value != value else str(value).strip()
        if text in null_tokens:
            return None
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
                return None
            matched[fmt_name] = matched.get(fmt_name, 0) + 1
            return result
        return None
