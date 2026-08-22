"""Wall-clock normalization for the forecast-balance source.

Distinct from ``DateNormalizer``, which serves the v4 workbook. Two things
here have no counterpart there: the ``/`` column mixes day-first and
month-first in the same file, and one format stores an instant rather than a
reading, so an offset has to be applied to it and to nothing else.
"""

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader

# What the timestamp is at its finest, which is not the same as how it is
# rendered. Every value is written to second resolution because the column has
# one dtype; DAY says the trailing 00:00:00 was supplied by the parser and
# means "no time of day was recorded", not "midnight".
PRECISIONS = ["SECOND", "MINUTE", "DAY"]

# Where the reading came from. AS_WRITTEN is the source's own clock, copied
# digit for digit. OFFSET_APPLIED means the source stored an instant and an
# offset was used to render it -- the one place in this step where a value
# moves.
SOURCES = ["AS_WRITTEN", "OFFSET_APPLIED"]

# What is actually known about the reading, in one word, so a reader who never
# looks at the precision or ambiguity flags still cannot mistake a supplied
# midnight for an observed one.
#
# OBSERVED       date and time of day both stated by the source.
# TIME_UNKNOWN   the date is stated, the time of day is not. The rendered
#                00:00:00 is a floor, not a reading.
# DATE_AMBIGUOUS the date could be read two ways and nothing settled it; the
#                majority reading is used and the row says so.
# UNKNOWN        nothing parseable. TXN_TS is null and stays null.
STATUSES = ["OBSERVED", "TIME_UNKNOWN", "DATE_AMBIGUOUS", "UNKNOWN"]

MONTH_FIRST = "%m/%d/%Y %H:%M"
DAY_FIRST = "%d/%m/%Y %H:%M"
MONTH_FIRST_DATE = "%m/%d/%Y"
DAY_FIRST_DATE = "%d/%m/%Y"

_SLASH_FIELDS = re.compile(r"^(\d{2})/(\d{2})/")

# Columns whose value names the month a row belongs to, tried in order with
# the context each one is keyed by. The rate is global, so it applies to every
# row and goes first; inflation is per country and only covers six of the
# twelve, so it is the follow-up that catches what the rate cannot separate --
# 5.331 is the rate in both 2023-03 and 2023-04, and the inflation index is
# not.
MACRO_ORACLES = [
    ("INTEREST_RATE_INDEX", ()),
    ("INFLATION_INDEX", ("MERCHANT_COUNTRY",)),
]


class TimestampNormalizer(BaseCleaner):
    """
    Resolves every ``TXN_DATE_TIME`` spelling to one wall clock plus its
    offset, without moving any reading the source already wrote as a clock.

    The ``/`` ambiguity is settled in three passes of decreasing confidence.

    A field above 12 can only be a day, which settles 55716 rows outright:
    44902 month-first, 10814 day-first. Both conventions really are present in
    this one column, so neither can simply be assumed.

    What is left is put to ``INTEREST_RATE_INDEX``, which is constant within a
    year-month across the whole file and therefore names the month a row
    belongs to independently of how its date is spelled. The two readings of
    ``NN/NN`` always fall in different months, so the rate separates them:
    33572 of 33708 settled, every one of them month-first.

    The residue is bracketed against the nearest already-placed neighbours in
    ``TXN_SEQ`` order within the account, which is chronological there at a
    median Spearman of 1.0. This pass is last because it is much the weakest:
    the median bracket is 41.6 hours wide, and on rows whose reading is
    already known it prefers the truth over a 2-hour shift by only 3%.
    Anything still open takes the majority reading and is flagged -- a coin
    flip recorded as a coin flip is recoverable, a coin flip recorded as a
    fact is not.
    """

    name = "timestamps"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.config.get("input_col", "TXN_DATE_TIME")
        if source not in df.columns:
            return df

        df = df.copy()
        rules = loader.timestamp_formats()
        zone = ZoneInfo(rules["zone"]["name"])
        raw = df[source].map(self.text)

        wall, precision, origin, ambiguous = self._parse_column(
            df, raw, rules["formats"], rules["null_tokens"], zone
        )

        df["TXN_TS"] = wall
        # Derived from the calendar date, so it labels the reading without
        # touching it. Stored as the offset rather than a tz-aware dtype
        # because 74 wall-clock values in this file fall in a DST gap or fold
        # and cannot be localised at all -- tz_localize raises on them, and a
        # column that cannot hold every row is not a column.
        df["TXN_TS_UTC_OFFSET"] = self._offsets(wall, zone)
        df["TXN_TS_PRECISION"] = pd.Categorical(
            precision, categories=PRECISIONS
        )
        df["TXN_TS_SOURCE"] = pd.Categorical(origin, categories=SOURCES)
        df["TXN_TS_AMBIGUOUS"] = ambiguous

        # Ordered weakest claim last: a row that is both date-only and
        # ambiguous is reported as ambiguous, because that is the one a reader
        # must not act on.
        status = pd.Series("OBSERVED", index=df.index, dtype=object)
        status[precision == "DAY"] = "TIME_UNKNOWN"
        status[ambiguous] = "DATE_AMBIGUOUS"
        status[wall.isna()] = "UNKNOWN"
        df["TXN_TS_STATUS"] = pd.Categorical(status, categories=STATUSES)
        for value in STATUSES:
            self.log(f"txn_ts.status[{value}]", int((status == value).sum()))

        self.log("txn_ts.unparseable", int(wall.isna().sum()))
        self.log(
            "txn_ts.offset_applied", int((origin == "OFFSET_APPLIED").sum())
        )
        self.log("txn_ts.ambiguous_day_month", int(ambiguous.sum()))
        for value in PRECISIONS:
            self.log(
                f"txn_ts.precision[{value}]", int((precision == value).sum())
            )

        if "SETTLE_DATE" in df.columns:
            settle = df["SETTLE_DATE"].map(self.text)
            df["SETTLE_DATE_CLEANED"] = self._parse_settle(
                settle, rules["settle_formats"], rules["null_tokens"]
            )
            blanks = int(settle.isin(rules["null_tokens"]).sum())
            self.log("settle_date.null_token", blanks)
            self.log(
                "settle_date.unparseable",
                int(df["SETTLE_DATE_CLEANED"].isna().sum()) - blanks,
            )
        return df

    def _parse_column(self, df, raw, formats, null_tokens, zone):
        """
        :param df: Frame, for the account and sequence columns.
        :param raw: Stripped source strings.
        :returns: (wall clock, precision, origin, ambiguity flag).
        """
        index = raw.index
        wall = pd.Series(pd.NaT, index=index, dtype="datetime64[ns]")
        precision = pd.Series("", index=index, dtype=object)
        origin = pd.Series("AS_WRITTEN", index=index, dtype=object)

        slash_rule, matched = None, raw.isin(null_tokens)
        for rule in formats:
            hit = raw.str.match(rule["regex"]).fillna(False) & ~matched
            if not hit.any():
                continue
            matched |= hit
            if rule["strptime"] == "AMBIGUOUS_SLASH":
                slash_rule = (rule, hit)
                continue
            if rule["strptime"] == "EPOCH_SECONDS":
                # The only branch that moves a value. The source stored an
                # instant, not a reading, so rendering it as a clock takes an
                # offset whichever offset is chosen; UTC is not the neutral
                # option here, it is a different assumption with a worse fit.
                local = (
                    pd.to_datetime(
                        raw[hit].astype("int64"), unit="s", utc=True
                    )
                    .dt.tz_convert(zone)
                    .dt.tz_localize(None)
                )
                wall[hit] = local
                origin[hit] = "OFFSET_APPLIED"
                # A local midnight is what a date-only value becomes on its
                # way through an epoch column; 8142 carry no time of day.
                precision[hit] = np.where(
                    (local.dt.hour == 0)
                    & (local.dt.minute == 0)
                    & (local.dt.second == 0),
                    "DAY",
                    rule["precision"],
                )
                continue
            wall[hit] = pd.to_datetime(
                raw[hit], format=rule["strptime"], errors="coerce"
            )
            precision[hit] = rule["precision"]

        ambiguous = pd.Series(False, index=index)
        if slash_rule is not None:
            rule, hit = slash_rule
            precision[hit] = rule["precision"]
            resolved, unresolved = self._resolve_slash(df, raw, hit)
            wall[hit] = resolved
            ambiguous[hit] = unresolved

        self.log("txn_ts.unrecognised_format", int((~matched).sum()))
        return wall, precision, origin, ambiguous

    def _resolve_slash(self, df, raw, hit):
        """
        Settles ``NN/NN`` by field range, then by macro month, then by
        sequence bracket.

        :param hit: Mask of the rows written in the ambiguous format.
        :returns: (parsed timestamps, mask of rows still unresolved).
        """
        subset = raw[hit]
        fields = subset.str.extract(_SLASH_FIELDS)
        first = pd.to_numeric(fields[0], errors="coerce")
        second = pd.to_numeric(fields[1], errors="coerce")

        month_first = pd.to_datetime(
            subset, format=MONTH_FIRST, errors="coerce"
        )
        day_first = pd.to_datetime(subset, format=DAY_FIRST, errors="coerce")

        forced_day = (first > 12) & day_first.notna()
        forced_month = (second > 12) & month_first.notna()
        out = pd.Series(pd.NaT, index=subset.index, dtype="datetime64[ns]")
        out[forced_month] = month_first[forced_month]
        out[forced_day] = day_first[forced_day]
        self.log("txn_ts.slash_forced_month_first", int(forced_month.sum()))
        self.log("txn_ts.slash_forced_day_first", int(forced_day.sum()))

        open_rows = subset.index[~forced_day & ~forced_month]
        if not len(open_rows):
            return out, pd.Series(False, index=subset.index)

        open_rows = self._settle_by_macro(
            df, out, open_rows, month_first, day_first
        )
        open_rows = self._settle_by_sequence(
            df, out, open_rows, month_first, day_first
        )

        # Month-first is the majority reading, 44902 forced rows to 10814, and
        # every row the rate could judge agreed with it. Recorded as a guess
        # regardless, which is the part that matters.
        out[open_rows] = month_first[open_rows]
        flag = pd.Series(False, index=subset.index)
        # Unresolved is not the same as ambiguous. 3081 of these are dates
        # like 01/01/2022 where the day and the month are the same number, so
        # both readings produce the same timestamp and nothing was ever at
        # stake. Flagging those would bury the 4 rows that are genuinely a
        # coin flip.
        flag[open_rows] = (
            month_first[open_rows] != day_first[open_rows]
        )
        return out, flag

    def _settle_by_macro(self, df, out, open_rows, month_first, day_first):
        """
        Uses the month a row's macro rate belongs to as an oracle for its date.

        Legitimate because the rate is constant within a year-month across the
        whole file, so it is evidence about the month that does not come from
        the date string being judged. The table is built only from rows
        already placed, so nothing circular enters it. A rate covering two
        months settles nothing and is left alone: 5.331 spans 2023-03 and
        2023-04, which is what the last 136 open rows are.

        :param out: Timestamps so far, written into in place.
        :param open_rows: Index of rows still unresolved.
        :returns: The subset of ``open_rows`` this pass could not settle.
        """
        for column, context in self.config.get("macro_oracles", MACRO_ORACLES):
            if not len(open_rows) or column not in df.columns:
                continue
            if any(name not in df.columns for name in context):
                continue

            keys = df[column].map(self.text)
            for name in context:
                keys = keys + "|" + df[name].map(self.text)

            # Built from rows already placed, which is what keeps the oracle
            # independent of the strings it is about to judge.
            placed = out.dropna()
            table: dict[str, set[str]] = {}
            for key, month in zip(
                keys.loc[placed.index], placed.dt.strftime("%Y-%m")
            ):
                table.setdefault(key, set()).add(month)

            settled = []
            for row in open_rows:
                months = table.get(keys.at[row])
                if not months:
                    continue
                candidates = (month_first.at[row], day_first.at[row])
                fits = [
                    pd.notna(value) and value.strftime("%Y-%m") in months
                    for value in candidates
                ]
                if fits[0] == fits[1]:
                    continue
                out.at[row] = candidates[0] if fits[0] else candidates[1]
                settled.append(row)

            self.log(f"txn_ts.slash_resolved_by[{column}]", len(settled))
            open_rows = open_rows.difference(pd.Index(settled))
        return open_rows

    def _settle_by_sequence(self, df, out, open_rows, month_first, day_first):
        """
        Brackets each remaining row between its nearest placed neighbours in
        transaction order and keeps the reading that fits.

        :returns: The subset of ``open_rows`` this pass could not settle.
        """
        order = self.config.get("sequence_col", "TXN_SEQ")
        group = self.config.get("group_col", "ACCOUNT_ID")
        if (
            not len(open_rows)
            or order not in df.columns
            or group not in df.columns
        ):
            return open_rows

        anchors = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        anchors.loc[out.index] = out
        sequence = df[order].astype("int64").sort_values().index
        anchors = anchors.loc[sequence]
        keys = df[group].loc[sequence]
        low = anchors.groupby(keys).ffill().loc[open_rows]
        high = anchors.groupby(keys).bfill().loc[open_rows]

        fits_month = month_first[open_rows].between(low, high)
        fits_day = day_first[open_rows].between(low, high)
        only_month = fits_month & ~fits_day
        only_day = fits_day & ~fits_month
        out[open_rows[only_month]] = month_first[open_rows[only_month]]
        out[open_rows[only_day]] = day_first[open_rows[only_day]]
        self.log("txn_ts.slash_bracket_month_first", int(only_month.sum()))
        self.log("txn_ts.slash_bracket_day_first", int(only_day.sum()))
        return open_rows[~(only_month | only_day)]

    def _parse_settle(self, raw, formats, null_tokens):
        """
        :returns: Settlement dates; date-only in every spelling, by design of
            the field rather than by loss, so no time or offset is attached.
        """
        out = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")
        done = raw.isin(null_tokens)
        for rule in formats:
            hit = raw.str.match(rule["regex"]).fillna(False) & ~done
            if not hit.any():
                continue
            done |= hit
            if rule["strptime"] == "AMBIGUOUS_SLASH":
                subset = raw[hit]
                first = pd.to_numeric(
                    subset.str.extract(_SLASH_FIELDS)[0], errors="coerce"
                )
                month_first = pd.to_datetime(
                    subset, format=MONTH_FIRST_DATE, errors="coerce"
                )
                day_first = pd.to_datetime(
                    subset, format=DAY_FIRST_DATE, errors="coerce"
                )
                out[hit] = month_first.where(
                    ~((first > 12) & day_first.notna()), day_first
                )
                continue
            out[hit] = pd.to_datetime(
                raw[hit], format=rule["strptime"], errors="coerce"
            )
        return out

    @staticmethod
    def _offsets(wall: pd.Series, zone: ZoneInfo) -> pd.Series:
        """
        :param wall: Naive wall-clock readings.
        :returns: The zone's offset on each reading's own date, as ``UTC+N``.
            A label, computed from the calendar: it describes the reading and
            never shifts it, which is what keeps the DST-gap rows expressible.
        """

        def label(value) -> str:
            if pd.isna(value):
                return ""
            naive = value.to_pydatetime().replace(tzinfo=None)
            hours = int(zone.utcoffset(naive).total_seconds() // 3600)
            return f"UTC{hours:+d}"

        return wall.map(label)
