"""City normalization, country resolution, and the card-not-present flag."""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.utils import audit

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


class CityNormalizer(BaseCleaner):
    """
    Collapses transliteration variants and e-commerce markers to one spelling,
    then resolves the country the city sits in.

    A city sits in exactly one country, so a known city settles the country and
    the stated one is only a candidate. Where the city is unknown the stated
    country still stands; where neither is known the pair is UNKNOWN rather
    than blank.
    """

    name = "geo"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if "MERCHANT_CITY" not in df.columns:
            return df

        df = df.copy()
        aliases, ecommerce = loader.city_aliases()

        raw = df["MERCHANT_CITY"].map(lambda v: self.text(v).upper())
        df["MERCHANT_CITY_CLEANED"] = raw.map(
            lambda c: aliases.get(c, c) if c else UNKNOWN
        )

        markers = loader.non_geographic_cities()
        kind = df["MERCHANT_CITY_CLEANED"].map(
            lambda c: UNKNOWN if c == UNKNOWN else markers.get(c, "PHYSICAL")
        )
        df["LOCATION_TYPE"] = pd.Categorical(kind, categories=LOCATION_TYPES)

        online = df["MERCHANT_CITY_CLEANED"].isin(ecommerce)
        if "HAS_TERMINAL" in df.columns:
            online = online | ~df["HAS_TERMINAL"]
        df["IS_ECOMMERCE"] = online

        # What the city implies, kept apart from what the file states so the
        # two can still be compared. Blank for e-commerce markers and unknown
        # cities: they carry no geography, and "no expectation" must never
        # read as "mismatch".
        countries = loader.city_countries()
        df["MERCHANT_COUNTRY_EXPECTED"] = [
            "" if place != "PHYSICAL" else countries.get(city, "")
            for city, place in zip(df["MERCHANT_CITY_CLEANED"], kind)
        ]

        # The single country column the reader sees: the city's country where
        # the city names one, otherwise the stated country, otherwise UNKNOWN.
        # The stated value is not lost -- raw_transactions still carries it,
        # and any row where the two disagreed is flagged.
        stated = (
            df["MERCHANT_COUNTRY"].map(self.text).str.upper()
            if "MERCHANT_COUNTRY" in df.columns
            else pd.Series("", index=df.index)
        )
        df["MERCHANT_COUNTRY_CLEANED"] = [
            expected or state or UNKNOWN
            for expected, state in zip(
                df["MERCHANT_COUNTRY_EXPECTED"], stated
            )
        ]

        return df

    def metrics(self, df: pd.DataFrame):
        # This step adds no diagnostic column of its own, because it already
        # had four. LOCATION_TYPE, IS_ECOMMERCE, MERCHANT_COUNTRY_EXPECTED and
        # the cleaned city each state per row what the totals below are totals
        # of, and MERCHANT_CITY is a raw column no step overwrites -- so the
        # count of distinct spellings before cleaning is still readable at the
        # end of the run.
        if "MERCHANT_CITY_CLEANED" not in df.columns:
            return

        yield (
            "cities_distinct_before",
            audit.distinct(
                df["MERCHANT_CITY"].map(lambda v: self.text(v).upper())
            ),
        )
        yield (
            "cities_distinct_after",
            audit.distinct(df["MERCHANT_CITY_CLEANED"]),
        )
        yield (
            "city.unknown",
            audit.rows(df["MERCHANT_CITY_CLEANED"].eq(UNKNOWN)),
        )
        yield "ecommerce_rows", audit.rows(df["IS_ECOMMERCE"])

        kind = df["LOCATION_TYPE"].astype(str)
        for value in LOCATION_TYPES:
            count = audit.rows(kind.eq(value))
            if count:
                yield f"location_type.{value.lower()}", count

        yield (
            "city.not_in_country_reference",
            audit.rows(
                kind.eq("PHYSICAL") & df["MERCHANT_COUNTRY_EXPECTED"].eq("")
            ),
        )
        yield (
            "country.unknown",
            audit.rows(df["MERCHANT_COUNTRY_CLEANED"].eq(UNKNOWN)),
        )
