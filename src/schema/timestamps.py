"""
Wall-clock normalization for the forecast-balance source.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

import re
from typing import NamedTuple
from zoneinfo import ZoneInfo

import numpy as np

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


class TimestampNormalizer:
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
