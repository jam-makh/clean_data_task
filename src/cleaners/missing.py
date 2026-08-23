"""Sentinel handling: what is absent, unreadable, or not applicable."""

import re
from collections import Counter

import pandas as pd

from src.cleaners.base import BaseCleaner

# The cleaned transaction timestamp, under either name a profile gives it:
# DateNormalizer produces the first, TimestampNormalizer the second.
TIMESTAMP_COLUMNS = ("TXN_DATE_TIME_CLEANED", "TXN_TS")

TERMINAL_SENTINEL = re.compile(r"^0+$")
AUTH_SENTINEL = re.compile(r"^0+$")

# The repeat threshold that decides a planted auth code from a chance
# collision is a judgement about this source, so it lives in
# config/policy.yaml with the probability argument that sets it.


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
            # this sentinel, so it marks card-not-present rather than data
            # loss.
            is_sentinel = df["TERMINAL_ID"].map(
                lambda v: bool(TERMINAL_SENTINEL.match(self.text(v)))
            )
            df["HAS_TERMINAL"] = ~is_sentinel
            self.log("terminal_id.sentinel_rows", int(is_sentinel.sum()))

        if "AUTH_CODE" in df.columns:
            codes = df["AUTH_CODE"].map(self.text)
            counts = Counter(c for c in codes if c)
            threshold = self.policy.missing.auth_repeat_threshold
            repeated = {c for c, n in counts.items() if n >= threshold}
            invalid = codes.map(
                lambda c: not c
                or bool(AUTH_SENTINEL.match(c))
                or c in repeated
            )
            df["AUTH_CODE_VALID"] = ~invalid
            self.log("auth_code.invalid_rows", int(invalid.sum()))
            self.log("auth_code.repeated_values", len(repeated))

        if "SETTLE_DATE_CLEANED" in df.columns:
            # MISSING says what the source did, which is the only thing this
            # step knows: no readable settlement date was supplied. The word
            # is deliberately not UNKNOWN, because the sheet writes UNKNOWN in
            # SETTLE_DATE_CLEANED itself for exactly these rows, and a status
            # that repeats the cell beside it tells a reader nothing.
            #
            # The frame keeps a true null throughout. The placeholder is a
            # rendering, applied by ``render_dates`` on the way out: holding a
            # literal string in the column here would force it to text and
            # break every sort and date calculation the pipeline does after
            # this step.
            status = pd.Series("OBSERVED", index=df.index, dtype=object)
            status[df["SETTLE_DATE_CLEANED"].isna()] = "MISSING"
            # Whichever name the profile's date step produced. Naming only one
            # of them would let the anomaly check silently never fire on the
            # other file, which reads exactly like a clean run.
            stamp = next(
                (c for c in TIMESTAMP_COLUMNS if c in df.columns), None
            )
            if stamp is not None:
                impossible = (
                    df["SETTLE_DATE_CLEANED"].notna()
                    & df[stamp].notna()
                    & (
                        df["SETTLE_DATE_CLEANED"].dt.normalize()
                        < df[stamp].dt.normalize()
                    )
                )
                status[impossible] = "ANOMALOUS"
                self.log("settle_date.anomalous", int(impossible.sum()))
            df["SETTLE_DATE_STATUS"] = pd.Categorical(
                status, categories=["OBSERVED", "MISSING", "ANOMALOUS"]
            )
            self.log("settle_date.missing", int((status == "MISSING").sum()))

        for column in ("MERCHANT_CITY", "MERCHANT_COUNTRY"):
            if column in df.columns:
                blanks = int(
                    df[column].map(lambda v: self.text(v) == "").sum()
                )
                if blanks:
                    self.log(f"{column}.blank", blanks)

        return df
