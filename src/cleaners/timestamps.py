"""Wall-clock normalization for the forecast-balance source.

Distinct from ``DateNormalizer``, which serves the v4 workbook. Two things
here have no counterpart there: the ``/`` column mixes day-first and
month-first in the same file, and one format stores an instant rather than a
reading, so an offset has to be applied to it and to nothing else.
"""

import re
from typing import NamedTuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.utils import audit

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

# Which rule in timestamp_formats.json read the row, and the three answers
# that are not a rule.
#
# NULL_TOKEN    the source said "no timestamp".
# UNRECOGNISED  no rule's pattern matched at all. This is a format nobody has
#               described yet, which is a gap in the rule file.
# UNPARSEABLE   a pattern matched and the value behind it did not parse -- a
#               31st of February. The rule is not credited with a row it
#               could not read.
NULL_TOKEN = "NULL_TOKEN"
UNRECOGNISED = "UNRECOGNISED"
UNPARSEABLE = "UNPARSEABLE"

# How a ``NN/NN/YYYY`` date was settled, in decreasing order of confidence.
# Blank on every row not written in that format.
FORCED_MONTH_FIRST = "FORCED_MONTH_FIRST"
FORCED_DAY_FIRST = "FORCED_DAY_FIRST"
BRACKET_MONTH_FIRST = "BRACKET_MONTH_FIRST"
BRACKET_DAY_FIRST = "BRACKET_DAY_FIRST"
# Nothing settled it and the majority reading was taken. Whether that was a
# real coin flip is TXN_TS_AMBIGUOUS's question: most of these are dates whose
# two readings are the same day.
MAJORITY = "MAJORITY"

TS_FORMAT = "TXN_TS_FORMAT"
TS_SLASH = "TXN_TS_SLASH_RESOLUTION"
SETTLE_FORMAT = "SETTLE_DATE_FORMAT"

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


class Parsed(NamedTuple):
    """One column's worth of readings, and the provenance of each."""

    wall: pd.Series
    precision: pd.Series
    origin: pd.Series
    ambiguous: pd.Series
    fmt: pd.Series
    slash: pd.Series


class TimestampNormalizer(BaseCleaner):
    """
    Resolves every ``TXN_DATE_TIME`` spelling to one wall clock plus its
    offset, without moving any reading the source already wrote as a clock.

    The ``/`` ambiguity is settled in three passes of decreasing confidence,
    and which pass settled a given row is written onto that row.

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Which macro oracles this run was able to consult, in the order it
        # consulted them. An oracle whose column is absent, or that was never
        # reached because nothing was left open, did not run -- and a zero
        # reported against it would claim it looked and settled nothing. Set
        # from column presence, before any row is read.
        self.oracles: list[str] = []

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.config.get("input_col", "TXN_DATE_TIME")
        if source not in df.columns:
            return df

        df = df.copy()
        self.oracles = []
        rules = loader.timestamp_formats()
        zone = ZoneInfo(rules["zone"]["name"])
        raw = df[source].map(self.text)

        read = self._parse_column(
            df, raw, rules["formats"], rules["null_tokens"], zone
        )

        df["TXN_TS"] = read.wall
        # Derived from the calendar date, so it labels the reading without
        # touching it. Stored as the offset rather than a tz-aware dtype
        # because 74 wall-clock values in this file fall in a DST gap or fold
        # and cannot be localised at all -- tz_localize raises on them, and a
        # column that cannot hold every row is not a column.
        df["TXN_TS_UTC_OFFSET"] = self._offsets(read.wall, zone)
        df["TXN_TS_PRECISION"] = pd.Categorical(
            read.precision, categories=PRECISIONS
        )
        df["TXN_TS_SOURCE"] = pd.Categorical(read.origin, categories=SOURCES)
        df["TXN_TS_AMBIGUOUS"] = read.ambiguous
        df[TS_FORMAT] = read.fmt
        df[TS_SLASH] = read.slash

        # Ordered weakest claim last: a row that is both date-only and
        # ambiguous is reported as ambiguous, because that is the one a reader
        # must not act on.
        status = pd.Series("OBSERVED", index=df.index, dtype=object)
        status[read.precision == "DAY"] = "TIME_UNKNOWN"
        status[read.ambiguous] = "DATE_AMBIGUOUS"
        status[read.wall.isna()] = "UNKNOWN"
        df["TXN_TS_STATUS"] = pd.Categorical(status, categories=STATUSES)

        if "SETTLE_DATE" in df.columns:
            settle = df["SETTLE_DATE"].map(self.text)
            values, labels = self._parse_settle(
                settle, rules["settle_formats"], rules["null_tokens"]
            )
            df["SETTLE_DATE_CLEANED"] = values
            df[SETTLE_FORMAT] = labels
        return df

    def metrics(self, df: pd.DataFrame):
        if TS_SLASH in df.columns:
            route = df[TS_SLASH]
            yield (
                "txn_ts.slash_forced_month_first",
                audit.rows(route.eq(FORCED_MONTH_FIRST)),
            )
            yield (
                "txn_ts.slash_forced_day_first",
                audit.rows(route.eq(FORCED_DAY_FIRST)),
            )
            for column in self.oracles:
                yield (
                    f"txn_ts.slash_resolved_by[{column}]",
                    audit.rows(route.eq(f"MACRO[{column}]")),
                )
            yield (
                "txn_ts.slash_bracket_month_first",
                audit.rows(route.eq(BRACKET_MONTH_FIRST)),
            )
            yield (
                "txn_ts.slash_bracket_day_first",
                audit.rows(route.eq(BRACKET_DAY_FIRST)),
            )

        if TS_FORMAT in df.columns:
            yield (
                "txn_ts.unrecognised_format",
                audit.rows(df[TS_FORMAT].eq(UNRECOGNISED)),
            )

        if "TXN_TS_STATUS" in df.columns:
            status = df["TXN_TS_STATUS"].astype(str)
            for value in STATUSES:
                yield f"txn_ts.status[{value}]", audit.rows(status.eq(value))

            yield "txn_ts.unparseable", audit.rows(df["TXN_TS"].isna())
            yield (
                "txn_ts.offset_applied",
                audit.rows(df["TXN_TS_SOURCE"].astype(str).eq(
                    "OFFSET_APPLIED"
                )),
            )
            yield (
                "txn_ts.ambiguous_day_month",
                audit.rows(df["TXN_TS_AMBIGUOUS"]),
            )
            precision = df["TXN_TS_PRECISION"].astype(str)
            for value in PRECISIONS:
                yield (
                    f"txn_ts.precision[{value}]",
                    audit.rows(precision.eq(value)),
                )

        if SETTLE_FORMAT in df.columns:
            blanks = audit.rows(df[SETTLE_FORMAT].eq(NULL_TOKEN))
            yield "settle_date.null_token", blanks
            # Everything null that the source did not declare null. Taken
            # from the value column rather than the label, so a rule that
            # matched a shape and then failed on the value is counted here
            # rather than credited to the rule.
            yield (
                "settle_date.unparseable",
                audit.rows(df["SETTLE_DATE_CLEANED"].isna()) - blanks,
            )

    def _parse_column(self, df, raw, formats, null_tokens, zone) -> Parsed:
        """
        :param df: Frame, for the account and sequence columns.
        :param raw: Stripped source strings.
        :returns: The readings and the provenance of each.
        """
        index = raw.index
        wall = pd.Series(pd.NaT, index=index, dtype="datetime64[ns]")
        precision = pd.Series("", index=index, dtype=object)
        origin = pd.Series("AS_WRITTEN", index=index, dtype=object)
        fmt = pd.Series(UNRECOGNISED, index=index, dtype=object)
        slash = pd.Series("", index=index, dtype=object)

        blank = raw.isin(null_tokens)
        fmt[blank] = NULL_TOKEN

        slash_rule, matched = None, blank
        for rule in formats:
            hit = raw.str.match(rule["regex"]).fillna(False) & ~matched
            if not hit.any():
                continue
            matched |= hit
            fmt[hit] = rule["name"]
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
            resolved, unresolved, route = self._resolve_slash(df, raw, hit)
            wall[hit] = resolved
            ambiguous[hit] = unresolved
            slash[hit] = route

        # A pattern that matched a value it then could not read is not
        # evidence for that pattern. Applied after every rule has run, and
        # never to a row nothing matched, which stays UNRECOGNISED -- the two
        # say different things about the rule file.
        fmt[wall.isna() & ~blank & matched] = UNPARSEABLE
        return Parsed(wall, precision, origin, ambiguous, fmt, slash)

    def _resolve_slash(self, df, raw, hit):
        """
        Settles ``NN/NN`` by field range, then by macro month, then by
        sequence bracket.

        :param hit: Mask of the rows written in the ambiguous format.
        :returns: (parsed timestamps, mask of rows still unresolved, the pass
            that settled each row).
        """
        subset = raw[hit]
        fields = subset.str.extract(_SLASH_FIELDS)
        first = pd.to_numeric(fields[0], errors="coerce")
        second = pd.to_numeric(fields[1], errors="coerce")

        month_first = pd.to_datetime(
            subset, format=MONTH_FIRST, errors="coerce"
        )
        day_first = pd.to_datetime(subset, format=DAY_FIRST, errors="coerce")

        # The two cannot both hold: forcing month-first needs the second field
        # above 12 and the month-first reading to parse, which needs the first
        # field at or below 12 -- and forcing day-first needs the opposite of
        # both. So one column can carry the answer.
        forced_day = (first > 12) & day_first.notna()
        forced_month = (second > 12) & month_first.notna()
        out = pd.Series(pd.NaT, index=subset.index, dtype="datetime64[ns]")
        route = pd.Series("", index=subset.index, dtype=object)
        out[forced_month] = month_first[forced_month]
        out[forced_day] = day_first[forced_day]
        route[forced_month] = FORCED_MONTH_FIRST
        route[forced_day] = FORCED_DAY_FIRST

        open_rows = subset.index[~forced_day & ~forced_month]
        if not len(open_rows):
            return out, pd.Series(False, index=subset.index), route

        open_rows = self._settle_by_macro(
            df, out, route, open_rows, month_first, day_first
        )
        open_rows = self._settle_by_sequence(
            df, out, route, open_rows, month_first, day_first
        )

        # Month-first is the majority reading, 44902 forced rows to 10814, and
        # every row the rate could judge agreed with it. Recorded as a guess
        # regardless, which is the part that matters.
        out[open_rows] = month_first[open_rows]
        route[open_rows] = MAJORITY
        flag = pd.Series(False, index=subset.index)
        # Unresolved is not the same as ambiguous. 3081 of these are dates
        # like 01/01/2022 where the day and the month are the same number, so
        # both readings produce the same timestamp and nothing was ever at
        # stake. Flagging those would bury the 4 rows that are genuinely a
        # coin flip.
        flag[open_rows] = (
            month_first[open_rows] != day_first[open_rows]
        )
        return out, flag, route

    def _settle_by_macro(
        self, df, out, route, open_rows, month_first, day_first
    ):
        """
        Uses the month a row's macro rate belongs to as an oracle for its date.

        Legitimate because the rate is constant within a year-month across the
        whole file, so it is evidence about the month that does not come from
        the date string being judged. The table is built only from rows
        already placed, so nothing circular enters it. A rate covering two
        months settles nothing and is left alone: 5.331 spans 2023-03 and
        2023-04, which is what the last 136 open rows are.

        :param out: Timestamps so far, written into in place.
        :param route: Which pass settled each row, written into in place.
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
                route.at[row] = f"MACRO[{column}]"
                settled.append(row)

            self.oracles.append(column)
            open_rows = open_rows.difference(pd.Index(settled))
        return open_rows

    def _settle_by_sequence(
        self, df, out, route, open_rows, month_first, day_first
    ):
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
        route[open_rows[only_month]] = BRACKET_MONTH_FIRST
        route[open_rows[only_day]] = BRACKET_DAY_FIRST
        return open_rows[~(only_month | only_day)]

    def _parse_settle(self, raw, formats, null_tokens):
        """
        :returns: (settlement dates, the rule that read each one). Date-only
            in every spelling, by design of the field rather than by loss, so
            no time or offset is attached.
        """
        out = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")
        labels = pd.Series(UNRECOGNISED, index=raw.index, dtype=object)
        blank = raw.isin(null_tokens)
        labels[blank] = NULL_TOKEN

        done = blank
        for rule in formats:
            hit = raw.str.match(rule["regex"]).fillna(False) & ~done
            if not hit.any():
                continue
            done |= hit
            labels[hit] = rule["name"]
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

        labels[out.isna() & ~blank & done] = UNPARSEABLE
        return out, labels

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
