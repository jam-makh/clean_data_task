"""Recovery of the macro columns broadcast onto every transaction row."""

import pandas as pd

from src.cleaners.base import BaseCleaner
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


class MacroCleaner(BaseCleaner):
    """
    Fills the macro columns by lookup, never by imputation.

    These three are not transaction attributes. Each is constant across a
    whole group in the source -- one interest rate per year-month across all
    12 countries, one inflation figure and one holiday flag per
    (year-month, country) -- so the value a missing row should carry is
    already stated by its neighbours, exactly and without a model. A per-user
    mode would be wrong for every user whose rows span more than one month,
    and a median would invent a number that no month ever had.

    All 11277 rows missing the rate and the holiday flag are the same rows:
    the ones whose ``TXN_DATE_TIME`` arrived as an epoch integer, which is one
    corruption recipe that blanked three fields and rewrote the date together.
    That is why this step runs after ``TimestampNormalizer`` -- the key it
    joins on is the very thing that was damaged.
    """

    name = "macro"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        timestamp = self.config.get("timestamp_col", "TXN_TS")
        if timestamp not in df.columns:
            return df

        df = df.copy()
        rules = loader.macro_series()
        month = df[timestamp].dt.strftime("%Y-%m")

        for source, key, context in SERIES:
            if source not in df.columns:
                continue
            series = rules[key]
            lookup = series["values"]
            uncovered = set(series.get("uncovered_countries", ()))

            keys = month.copy()
            for name in context:
                if name not in df.columns:
                    keys = None
                    break
                keys = keys + "|" + df[name].map(self.text)
            if keys is None:
                continue

            observed = df[source].notna()
            recovered = keys.map(lookup)
            df[f"{source}_CLEANED"] = self._coerce(
                source, df[source].where(observed, recovered)
            )

            status = pd.Series("OBSERVED", index=df.index, dtype=object)
            gap = ~observed
            status[gap & recovered.notna()] = "RECOVERED"
            status[gap & recovered.isna()] = "UNRECOVERABLE"
            if uncovered and context:
                # Named before the join rather than inferred from its failure:
                # a country outside the panel and a month missing from the
                # rule file both produce a null, and they mean opposite things.
                outside = df[context[0]].map(self.text).isin(uncovered)
                status[gap & outside] = "OUT_OF_PANEL"
            df[f"{source}_COVERAGE"] = pd.Categorical(
                status, categories=COVERAGE
            )

            for value in COVERAGE:
                count = int((status == value).sum())
                if count:
                    self.log(f"{source}.{value.lower()}", count)

        return df

    @staticmethod
    def _coerce(source: str, values: pd.Series) -> pd.Series:
        """
        :returns: The series as the type its column actually holds, so a
            recovered value and an observed one are indistinguishable
            downstream -- which is the point of recovering it.
        """
        if source == "IS_HOLIDAY_MONTH":
            return values.map(
                lambda v: TRUTHY.get(str(v).strip().upper(), v)
                if not isinstance(v, bool) and pd.notna(v)
                else v
            ).astype("boolean")
        return pd.to_numeric(values, errors="coerce")
