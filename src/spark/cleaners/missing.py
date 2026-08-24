"""
Sentinel handling: what is absent, unreadable, or not applicable.

Three of the four columns here are a regex against one cell. The fourth --
whether a row's auth code is one that recurs across the file -- is the only
thing in this stage that is not a property of the row, and it is where the
pandas version keeps a ``Counter``.

A Counter is exactly the shape ``BaseCleaner`` warns about: it is filled in
wherever the rows are and read back on the driver, and distributed it would
be read back partial. The Spark answer is a count window partitioned by the
value itself -- ``count(*) over (partition by code)`` -- which puts the tally
on every row sharing a code, computed wherever those rows happen to live.
Same number, no accumulator, and it survives being run on a cluster.

The two sentinel patterns are imported from the pandas module rather than
retyped. ``^0+$`` means the same thing to both regex engines, and a stage
that spelled its own copy would be one edit away from the two disagreeing
about what an all-zero terminal ID is.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.cleaners.missing import (
    AUTH_SENTINEL,
    REPEATED,
    TERMINAL_SENTINEL,
    TIMESTAMP_COLUMNS,
)
from src.spark import audit
from src.spark.spark_utils import chain, text


def apply(frame, policy):
    """
    Turns sentinel values into explicit flags without erasing them.

    :param frame: Frame with the settlement date already parsed, where the
        profile parses one.
    :param policy: Read for the auth-code repeat threshold, which is a
        judgement about this source and lives in ``config/policy.yaml`` with
        the probability argument that sets it.
    :returns: The frame with the sentinel flags added.
    """
    if "TERMINAL_ID" in frame.columns:
        # Not dirt: an ATM is itself a terminal, and no ATM row carries this
        # sentinel, so it marks card-not-present rather than data loss.
        frame = frame.withColumn(
            "HAS_TERMINAL",
            ~text("TERMINAL_ID").rlike(TERMINAL_SENTINEL.pattern),
        )

    if "AUTH_CODE" in frame.columns:
        code = "_missing_code"
        frame = frame.withColumn(code, text("AUTH_CODE"))
        repeats = F.count("*").over(Window.partitionBy(code))
        frame = frame.withColumn(
            REPEATED,
            # The blank exclusion is the pandas ``Counter`` over non-empty
            # codes only. Without it every blank row would count every other
            # blank row as a repeat of itself, and a column of absent values
            # would be reported as the most planted value in the file.
            (F.col(code) != "")
            & (repeats >= policy.missing.auth_repeat_threshold),
        ).withColumn(
            "AUTH_CODE_VALID",
            (F.col(code) != "")
            & ~F.col(code).rlike(AUTH_SENTINEL.pattern)
            & ~F.col(REPEATED),
        ).drop(code)

    if "SETTLE_DATE_CLEANED" in frame.columns:
        settled = F.col("SETTLE_DATE_CLEANED")
        # Whichever name the profile's date step produced. Naming only one of
        # them would let the anomaly check silently never fire on the other
        # file, which reads exactly like a clean run.
        stamp = next((c for c in TIMESTAMP_COLUMNS if c in frame.columns), None)
        anomalous = (
            settled.isNotNull()
            & F.col(stamp).isNotNull()
            & (F.to_date(settled) < F.to_date(F.col(stamp)))
            if stamp is not None
            else F.lit(False)
        )
        frame = frame.withColumn(
            "SETTLE_DATE_STATUS",
            chain(
                [
                    (anomalous, F.lit("ANOMALOUS")),
                    (settled.isNull(), F.lit("MISSING")),
                ],
                otherwise=F.lit("OBSERVED"),
            ),
        )

    return frame


def metrics(frame, policy):
    """
    What was absent, unreadable, or planted.

    :param frame: The frame as the last stage left it.
    :param policy: Unused; the repeat threshold decided the flag, and the flag
        is what is counted.
    :returns: ``(metric, request)`` pairs in report order.
    """
    columns = set(frame.columns)
    out = []

    if "HAS_TERMINAL" in columns:
        out.append((
            "terminal_id.sentinel_rows", audit.rows(~F.col("HAS_TERMINAL"))
        ))

    if "AUTH_CODE_VALID" in columns:
        out.append((
            "auth_code.invalid_rows", audit.rows(~F.col("AUTH_CODE_VALID"))
        ))
        # How many *values* recur, not how many rows carry one. A single code
        # planted on four hundred rows is one thing to chase.
        out.append((
            "auth_code.repeated_values",
            audit.distinct(
                F.when(F.col(REPEATED), text("AUTH_CODE"))
            ),
        ))

    if "SETTLE_DATE_STATUS" in columns:
        status = F.col("SETTLE_DATE_STATUS")
        # The anomaly needs a transaction date to be anomalous against.
        # Without one the check never ran, and reporting a zero would claim
        # it did and found nothing.
        if any(c in columns for c in TIMESTAMP_COLUMNS):
            out.append((
                "settle_date.anomalous",
                audit.rows(status == F.lit("ANOMALOUS")),
            ))
        out.append((
            "settle_date.missing", audit.rows(status == F.lit("MISSING"))
        ))

    for column in ("MERCHANT_CITY", "MERCHANT_COUNTRY"):
        if column in columns:
            out.append((
                f"{column}.blank",
                audit.rows(text(column) == "", nonzero=True),
            ))

    return out
