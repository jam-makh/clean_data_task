"""Sentinel handling: what is absent, what is unreadable, what is not applicable."""

import re
from collections import Counter

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner

TERMINAL_SENTINEL = re.compile(r"^0+$")
AUTH_SENTINEL = re.compile(r"^0+$")

# A 6-character alphanumeric auth code has ~2.2 billion combinations, so across
# a few thousand rows the expected number of genuine collisions is ~0.001. Any
# repeat at all is therefore a planted value, not chance.
AUTH_REPEAT_THRESHOLD = 2


class MissingValueHandler(BaseCleaner):
    """
    Turns sentinel values into explicit flags without erasing them.

    The three kinds of missing are kept apart: values that are absent stay
    null, values that are unreadable stay null and are counted, and values
    that are legitimately not applicable keep their sentinel and gain a flag.
    """

    name = "missing"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if "TERMINAL_ID" in df.columns:
            # Not dirt: an ATM is itself a terminal, and 0 of 71 ATM rows carry
            # this sentinel, so it marks card-not-present rather than data loss.
            is_sentinel = df["TERMINAL_ID"].map(
                lambda v: bool(TERMINAL_SENTINEL.match(self.text(v)))
            )
            df["HAS_TERMINAL"] = ~is_sentinel
            self.log("terminal_id.sentinel_rows", int(is_sentinel.sum()))

        if "AUTH_CODE" in df.columns:
            codes = df["AUTH_CODE"].map(self.text)
            counts = Counter(c for c in codes if c)
            repeated = {c for c, n in counts.items() if n >= AUTH_REPEAT_THRESHOLD}
            invalid = codes.map(
                lambda c: not c or bool(AUTH_SENTINEL.match(c)) or c in repeated
            )
            df["AUTH_CODE_VALID"] = ~invalid
            self.log("auth_code.invalid_rows", int(invalid.sum()))
            self.log("auth_code.repeated_values", len(repeated))

        if "SETTLE_DATE_CLEAN" in df.columns:
            # UNKNOWN is expressed in its own column; the date column keeps a
            # true null. A literal "unknown" string would force the column to
            # text and break every sort and date calculation.
            status = pd.Series("OBSERVED", index=df.index, dtype=object)
            status[df["SETTLE_DATE_CLEAN"].isna()] = "UNKNOWN"
            if "TXN_DATE_TIME_CLEAN" in df.columns:
                impossible = (
                    df["SETTLE_DATE_CLEAN"].notna()
                    & df["TXN_DATE_TIME_CLEAN"].notna()
                    & (
                        df["SETTLE_DATE_CLEAN"].dt.normalize()
                        < df["TXN_DATE_TIME_CLEAN"].dt.normalize()
                    )
                )
                status[impossible] = "ANOMALOUS"
                self.log("settle_date.anomalous", int(impossible.sum()))
            df["SETTLE_DATE_STATUS"] = pd.Categorical(
                status, categories=["OBSERVED", "UNKNOWN", "ANOMALOUS"]
            )
            self.log("settle_date.unknown", int((status == "UNKNOWN").sum()))

        for column in ("MERCHANT_CITY", "MERCHANT_COUNTRY"):
            if column in df.columns:
                blanks = int(df[column].map(lambda v: self.text(v) == "").sum())
                if blanks:
                    self.log(f"{column}.blank", blanks)

        return df
