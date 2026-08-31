"""
Recovery of the macro columns broadcast onto every transaction row.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

from src.rules import loader

# How a macro value came to be in the row.
#
# OBSERVED   the source carried it.
# RECOVERED  the source did not, and the series for its month says what it is.
# OUT_OF_PANEL  there is no series for this row to be missing from. Six of the
#               twelve countries have no inflation figure in any month, so
#               17873 nulls are the absence of a series rather than the loss
#               of a value -- not a gap to fill, and not one that could be.
# UNRECOVERABLE the series exists but has no entry for this row's key, which
#               is a hole in the rule file rather than in the data.
COVERAGE = ["OBSERVED", "RECOVERED", "OUT_OF_PANEL", "UNRECOVERABLE"]

# Source column, output column, and the extra columns its key is built from.
# The rate is one global series; the other two are per country.
#
# The country here is the STATED MERCHANT_COUNTRY, deliberately, and not the
# corrected MERCHANT_COUNTRY_CLEANED that CityNormalizer derives from the
# city. The stated column is only 85.8% right -- BEIRUT appears against all
# twelve codes -- but it is the key the source itself used: inflation is
# constant within (month, stated country) in 252 of 252 groups, and within
# (month, corrected country) in only 180 of 324. Recovering on the corrected
# country would therefore contradict the 236045 values the file already
# states. The two columns answer different questions and both are kept: this
# one reproduces the source, MERCHANT_COUNTRY_CLEANED says where the
# transaction actually happened.
SERIES = [
    ("INTEREST_RATE_INDEX", "interest_rate_index", ()),
    ("INFLATION_INDEX", "inflation_index", ("MERCHANT_COUNTRY",)),
    ("IS_HOLIDAY_MONTH", "is_holiday_month", ("MERCHANT_COUNTRY",)),
]

TRUTHY = {"TRUE": True, "FALSE": False, "1": True, "0": False}
