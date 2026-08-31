"""
Sentinel handling: what is absent, unreadable, or not applicable.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

import re
from collections import Counter


# The cleaned transaction timestamp, under either name a profile gives it:
# DateNormalizer produces the first, TimestampNormalizer the second.
TIMESTAMP_COLUMNS = ("TXN_DATE_TIME_CLEANED", "TXN_TS")

TERMINAL_SENTINEL = re.compile(r"^0+$")
AUTH_SENTINEL = re.compile(r"^0+$")

# Whether this row's auth code is one that recurs across the file. Kept apart
# from AUTH_CODE_VALID, which is also false for blanks and all-zero sentinels:
# a planted code and an absent one are both unusable and are not the same
# finding, and only this one identifies a *value* worth chasing upstream.
REPEATED = "AUTH_CODE_REPEATED"

# The repeat threshold that decides a planted auth code from a chance
# collision is a judgement about this source, so it lives in
# config/policy.yaml with the probability argument that sets it.
