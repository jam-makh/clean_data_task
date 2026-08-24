"""Sentinel handling: what is absent, unreadable, or not applicable."""

import re
from collections import Counter

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.utils import audit

# The cleaned transaction timestamp, under either name a profile gives it:
# DateNormalizer produces the first, TimestampNormalizer the second.
TIMESTAMP_COLUMNS = ("TXN_DATE_TIME_CLEANED", "TXN_TS")

TERMINAL_SENTINEL = re.compile(r"^0+$")
AUTH_SENTINEL = re.compile(r"^0+$")

# Whether this row's auth code is one that recurs across the file. Kept apart
# from AUTH_CODE_VALID, which is also false for blanks and all-zero sentinels:
# a planted code and an absent one are both unusable and are not the same
# finding, and only this one identifies a *value* worth chasing upstream.
REPEATED = "AUTH_CODE_REPEATED"

# The repeat threshold that decides a planted auth code from a chance
# collision is a judgement about this source, so it lives in
# config/policy.yaml with the probability argument that sets it.


class MissingValueHandler(BaseCleaner):
    """
    Turns sentinel values into explicit flags without erasing them.

    The three kinds of missing are kept apart: values that are absent stay
    null, values that are unreadable stay null and are marked, and values
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

        if "AUTH_CODE" in df.columns:
            codes = df["AUTH_CODE"].map(self.text)
            counts = Counter(c for c in codes if c)
            threshold = self.policy.missing.auth_repeat_threshold
            repeated = {c for c, n in counts.items() if n >= threshold}
            df[REPEATED] = codes.isin(repeated)
            df["AUTH_CODE_VALID"] = ~codes.map(
                lambda c: not c or bool(AUTH_SENTINEL.match(c))
            ) & ~df[REPEATED]

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
            df["SETTLE_DATE_STATUS"] = pd.Categorical(
                status, categories=["OBSERVED", "MISSING", "ANOMALOUS"]
            )

        return df

    def metrics(self, df: pd.DataFrame):
        if "HAS_TERMINAL" in df.columns:
            yield (
                "terminal_id.sentinel_rows",
                audit.rows(~df["HAS_TERMINAL"]),
            )

        if "AUTH_CODE_VALID" in df.columns:
            yield (
                "auth_code.invalid_rows",
                audit.rows(~df["AUTH_CODE_VALID"]),
            )
            # How many *values* recur, not how many rows carry one. A single
            # code planted on four hundred rows is one thing to chase.
            yield (
                "auth_code.repeated_values",
                audit.distinct(
                    df.loc[df[REPEATED], "AUTH_CODE"].map(self.text)
                ),
            )

        if "SETTLE_DATE_STATUS" in df.columns:
            status = df["SETTLE_DATE_STATUS"].astype(str)
            # The anomaly needs a transaction date to be anomalous against.
            # Without one the check never ran, and reporting a zero would
            # claim it did and found nothing.
            if any(c in df.columns for c in TIMESTAMP_COLUMNS):
                yield "settle_date.anomalous", audit.rows(
                    status.eq("ANOMALOUS")
                )
            yield "settle_date.missing", audit.rows(status.eq("MISSING"))

        for column in ("MERCHANT_CITY", "MERCHANT_COUNTRY"):
            if column in df.columns:
                blanks = audit.rows(df[column].map(lambda v: self.text(v) == ""))
                if blanks:
                    yield f"{column}.blank", blanks
