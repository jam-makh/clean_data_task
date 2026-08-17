"""City normalization and the card-not-present flag."""

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner
from cleaning_task.rules import loader


class CityNormalizer(BaseCleaner):
    """
    Collapses transliteration variants and e-commerce markers to one spelling.

    Blank cities are left blank: country does not determine city, so filling
    them would fabricate location data.
    """

    name = "geo"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if "MERCHANT_CITY" not in df.columns:
            return df

        df = df.copy()
        aliases, ecommerce = loader.city_aliases()

        raw = df["MERCHANT_CITY"].map(lambda v: self.text(v).upper())
        df["MERCHANT_CITY_CLEAN"] = raw.map(lambda c: aliases.get(c, c))

        online = df["MERCHANT_CITY_CLEAN"].isin(ecommerce)
        if "HAS_TERMINAL" in df.columns:
            online = online | ~df["HAS_TERMINAL"]
        df["IS_ECOMMERCE"] = online

        # A city sits in exactly one country, so the city implies the country
        # and the pair is checkable. Blank cities and card-not-present markers
        # carry no geography and are left empty -- not knowing where a merchant
        # is differs from placing it in the wrong country.
        countries = loader.city_countries()
        df["MERCHANT_COUNTRY_EXPECTED"] = [
            "" if city in ecommerce else countries.get(city, "")
            for city in df["MERCHANT_CITY_CLEAN"]
        ]

        physical = ~df["MERCHANT_CITY_CLEAN"].isin(ecommerce) & df[
            "MERCHANT_CITY_CLEAN"
        ].ne("")
        unknown = physical & df["MERCHANT_COUNTRY_EXPECTED"].eq("")

        self.log("cities_distinct_before", int(raw.nunique()))
        self.log("cities_distinct_after", int(df["MERCHANT_CITY_CLEAN"].nunique()))
        self.log("ecommerce_rows", int(online.sum()))
        self.log("city.not_in_country_reference", int(unknown.sum()))
        return df
