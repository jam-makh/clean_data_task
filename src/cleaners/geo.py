"""City normalization, country resolution, and the card-not-present flag."""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader

# One token for "we do not know", used for both city and country. A blank cell
# reads as an oversight; an explicit UNKNOWN reads as a fact that was checked
# and is genuinely absent.
UNKNOWN = "UNKNOWN"


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
            "" if city in ecommerce else countries.get(city, "")
            for city in df["MERCHANT_CITY_CLEANED"]
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

        physical = ~online & df["MERCHANT_CITY_CLEANED"].ne(UNKNOWN)
        unresolved = physical & df["MERCHANT_COUNTRY_EXPECTED"].eq("")

        self.log("cities_distinct_before", int(raw.nunique()))
        self.log(
            "cities_distinct_after", int(df["MERCHANT_CITY_CLEANED"].nunique())
        )
        self.log(
            "city.unknown", int(df["MERCHANT_CITY_CLEANED"].eq(UNKNOWN).sum())
        )
        self.log("ecommerce_rows", int(online.sum()))
        self.log("city.not_in_country_reference", int(unresolved.sum()))
        self.log(
            "country.unknown",
            int(df["MERCHANT_COUNTRY_CLEANED"].eq(UNKNOWN).sum()),
        )
        return df
