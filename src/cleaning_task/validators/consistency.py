"""Cross-field checks. Changes no data; records what disagrees."""

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner

# A null in any of these makes the row unusable for its purpose, and is the
# only condition under which dropping a row would be justified.
REQUIRED = ["TXN_ID", "ACCOUNT_ID", "TXN_DATE_TIME_CLEAN", "TXN_AMOUNT_CLEAN", "TXN_CCY"]

REFUND_LABEL = "Purchase Return/Refund"


class ConsistencyValidator(BaseCleaner):
    """
    Asserts that the redundant encodings still agree.

    The dataset states the same fact three ways — processing code, processing
    type, and the sign of the billing amount — so any disagreement means one
    of them is corrupt. Violations are written to ``VALIDATION_FLAGS``, never
    silently repaired.
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

        if {"SETTLE_DATE_CLEAN", "TXN_DATE_TIME_CLEAN"}.issubset(df.columns):
            both = df["SETTLE_DATE_CLEAN"].notna() & df["TXN_DATE_TIME_CLEAN"].notna()
            raise_flag(
                both
                & (
                    df["SETTLE_DATE_CLEAN"].dt.normalize()
                    < df["TXN_DATE_TIME_CLEAN"].dt.normalize()
                ),
                "SETTLE_BEFORE_TXN",
            )

        if {"PROCESSING_TYPE_CLEAN", "BILLING_AMOUNT"}.issubset(df.columns):
            amount = pd.to_numeric(df["BILLING_AMOUNT"], errors="coerce")
            is_refund = df["PROCESSING_TYPE_CLEAN"].astype(str) == REFUND_LABEL
            raise_flag(is_refund & (amount < 0), "REFUND_NEGATIVE")
            raise_flag(~is_refund & (amount > 0), "PURCHASE_POSITIVE")

        if {"MERCHANT_COUNTRY_EXPECTED", "MERCHANT_COUNTRY"}.issubset(df.columns):
            # Only rows where the city actually implies a country: a blank
            # expectation means "no geography stated", never "mismatch".
            stated = df["MERCHANT_COUNTRY_EXPECTED"].ne("")
            raise_flag(
                stated
                & (df["MERCHANT_COUNTRY"].map(self.text) != df["MERCHANT_COUNTRY_EXPECTED"]),
                "GEO_CITY_COUNTRY_MISMATCH",
            )

        if {"PROCESSING_TYPE", "PROCESSING_TYPE_CLEAN"}.issubset(df.columns):
            raise_flag(
                df["PROCESSING_TYPE"].map(self.text)
                != df["PROCESSING_TYPE_CLEAN"].astype(str),
                "CODE_TYPE_MISMATCH",
            )

        df["VALIDATION_FLAGS"] = flags.map(lambda f: ";".join(f))
        self.log("rows_with_any_flag", int((df["VALIDATION_FLAGS"] != "").sum()))
        return df
