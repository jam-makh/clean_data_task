"""Cross-field checks. Changes no data; records what disagrees."""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.cleaners.codes import REFUND_LABEL
from src.rules import loader

# A null in any of these makes the row unusable for its purpose, and is the
# only condition under which dropping a row would be justified.
REQUIRED = [
    "TXN_ID", "ACCOUNT_ID", "TXN_DATE_TIME_CLEANED", "TXN_AMOUNT_CLEANED",
    "TXN_CCY",
]

# How far the two amounts may drift before they count as disagreeing. FX_RATE
# is stored to six decimals, so a large transaction reconciles to a few cents
# rather than exactly; a relative tolerance holds across the four orders of
# magnitude this file spans, where a fixed one would flag every large row or
# miss every small one. At 1% the ten rows that differ only by rate rounding
# fall inside it, and the 42 that are genuinely wrong stay outside.
FX_TOLERANCE = 0.01

# How far a row's own rate may sit from the reference before the rate itself is
# suspect. Far wider than FX_TOLERANCE, and for a different reason: that one
# allows for rounding, this one has to allow for a currency genuinely moving
# over the seven months the file covers. Every floating currency here stays
# within 4.3% of its own median, so 15% catches nothing on merit alone -- which
# is the point. It fires only where a rate is wrong by an order of magnitude,
# not where it is merely from a different Tuesday.
FX_REFERENCE_TOLERANCE = 0.15


class ConsistencyValidator(BaseCleaner):
    """
    Asserts that the redundant encodings still agree.

    The dataset states the same fact three ways — processing code, processing
    type, and the sign of the billing amount — so any disagreement means one
    of them is corrupt. Violations are written to ``VALIDATION_FLAGS``, never
    silently repaired.

    The amount is stated twice as well, in local currency and in billing
    currency with the rate between them, so the two must reconcile. Where they
    do not, this step stops at the flag: any one of the three values could be
    the wrong one and the arithmetic cannot say which, so repairing would mean
    inventing a number. That is the difference between this check and the sign
    restoration in ``AmountNormalizer``, where the transaction type says
    unambiguously which value is wrong.
    """

    name = "consistency"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Start from any flags an earlier step already raised, so validators
        # accumulate rather than the last one winning.
        existing = (
            df["VALIDATION_FLAGS"] if "VALIDATION_FLAGS" in df.columns
            else pd.Series([""] * len(df), index=df.index)
        )
        flags = existing.map(lambda v: [f for f in str(v).split(";") if f])

        def raise_flag(mask: pd.Series, code: str) -> None:
            n = int(mask.sum())
            self.log(code, n)
            if n:
                for idx in df.index[mask]:
                    flags[idx].append(code)

        for column in REQUIRED:
            if column in df.columns:
                raise_flag(df[column].isna(), f"REQUIRED_NULL[{column}]")

        dates = {"SETTLE_DATE_CLEANED", "TXN_DATE_TIME_CLEANED"}
        if dates.issubset(df.columns):
            both = (
                df["SETTLE_DATE_CLEANED"].notna()
                & df["TXN_DATE_TIME_CLEANED"].notna()
            )
            raise_flag(
                both
                & (
                    df["SETTLE_DATE_CLEANED"].dt.normalize()
                    < df["TXN_DATE_TIME_CLEANED"].dt.normalize()
                ),
                "SETTLE_BEFORE_TXN",
            )

        fx_cols = {"TXN_AMOUNT_CLEANED", "BILLING_AMOUNT", "FX_RATE"}
        if fx_cols.issubset(df.columns):
            # The amount is signed by transaction type and the billing amount
            # by its own convention, so the two are compared as magnitudes and
            # the direction is left to the sign checks below.
            rate = pd.to_numeric(df["FX_RATE"], errors="coerce")
            billed = pd.to_numeric(df["BILLING_AMOUNT"], errors="coerce").abs()
            expected = df["TXN_AMOUNT_CLEANED"].abs() * rate

            # Only rows where all three values are actually present and the
            # expectation is non-zero: a missing rate is a gap, not a
            # contradiction, and dividing by zero would invent one.
            comparable = expected.notna() & billed.notna() & (expected > 0)
            drift = (expected - billed).abs() / expected.where(comparable)
            raise_flag(
                comparable & (drift > FX_TOLERANCE), "FX_RECONCILE_MISMATCH"
            )

        if {"FX_RATE", "TXN_CCY"}.issubset(df.columns):
            # The reconciliation above proves a row is internally consistent,
            # which a row using a dead rate still is: a stale peg applied to
            # both the rate and the billing amount agrees with itself and is
            # wrong by a factor of twenty. Only an outside reference can see
            # that, which is what fx_rates.json is.
            reference = loader.fx_rates()
            expected_rate = df["TXN_CCY"].map(self.text).str.upper().map(
                reference
            )
            stated = pd.to_numeric(df["FX_RATE"], errors="coerce")
            known = expected_rate.notna() & stated.notna() & (stated > 0)
            off = (stated - expected_rate).abs() / expected_rate.where(known)
            raise_flag(
                known & (off > FX_REFERENCE_TOLERANCE), "FX_RATE_OFF_REFERENCE"
            )

        if {"PROCESSING_TYPE_CLEANED", "BILLING_AMOUNT"}.issubset(df.columns):
            amount = pd.to_numeric(df["BILLING_AMOUNT"], errors="coerce")
            is_refund = (
                df["PROCESSING_TYPE_CLEANED"].astype(str) == REFUND_LABEL
            )
            raise_flag(is_refund & (amount < 0), "REFUND_NEGATIVE")
            raise_flag(~is_refund & (amount > 0), "PURCHASE_POSITIVE")

        expected_cols = {"MERCHANT_COUNTRY_EXPECTED", "MERCHANT_COUNTRY"}
        if expected_cols.issubset(df.columns):
            # Only rows where the city actually implies a country: a blank
            # expectation means "no geography stated", never "mismatch".
            stated = df["MERCHANT_COUNTRY_EXPECTED"].ne("")
            raise_flag(
                stated
                & (
                    df["MERCHANT_COUNTRY"].map(self.text)
                    != df["MERCHANT_COUNTRY_EXPECTED"]
                ),
                "GEO_CITY_COUNTRY_MISMATCH",
            )

        if {"PROCESSING_TYPE", "PROCESSING_TYPE_CLEANED"}.issubset(df.columns):
            raise_flag(
                df["PROCESSING_TYPE"].map(self.text)
                != df["PROCESSING_TYPE_CLEANED"].astype(str),
                "CODE_TYPE_MISMATCH",
            )

        df["VALIDATION_FLAGS"] = flags.map(lambda f: ";".join(f))
        self.log(
            "rows_with_any_flag", int((df["VALIDATION_FLAGS"] != "").sum())
        )
        return df
