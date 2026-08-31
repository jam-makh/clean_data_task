"""
City normalization, country resolution, and the card-not-present flag.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

from src.rules import loader

# One token for "we do not know", used for both city and country. A blank cell
# reads as an oversight; an explicit UNKNOWN reads as a fact that was checked
# and is genuinely absent.
UNKNOWN = "UNKNOWN"

# What kind of place the row happened in. UNKNOWN is reserved for a city the
# source did not state: a marker like INTERNAL or INTERNET is not a missing
# city, it is a positive statement that there was no merchant location, and
# collapsing the two would lose 24614 rows worth of that distinction.
#
# A working column rather than a presented one. It classifies
# MERCHANT_CITY_CLEANED, which sits beside it already spelling the marker
# out, and the reason a row carries no place -- that it is the bank moving
# the customer's own money -- is stated on MERCHANT_TYPE, where it is a fact
# about the counterparty rather than a gap in the geography. The counts stay
# in cleaning_report.
LOCATION_TYPES = ["PHYSICAL", "ECOMMERCE", "INTERNAL", "UNKNOWN"]
