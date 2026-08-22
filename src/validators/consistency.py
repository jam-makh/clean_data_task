"""
Cross-field Validation Module.

Performs read-only validation across related columns to flag discrepancies.
Note: This module tags problematic rows; it does NOT mutate or drop data.
"""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.cleaners.codes import REFUND_LABEL
from src.rules import loader

# The required-column list and both FX tolerances are judgements about how
# strict this pipeline should be, not facts about the data, so they live in
# config/policy.yaml -- each beside the argument that sets it -- and reach
# this step through the injected policy.


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

        for column in self.policy.validation.required_columns:
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
                comparable & (drift > self.policy.fx.reconcile_tolerance),
                "FX_RECONCILE_MISMATCH",
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
                known & (off > self.policy.fx.reference_tolerance),
                "FX_RATE_OFF_REFERENCE",
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
