"""
Cross-field validation: flag the disagreements, repair nothing.

The dataset states the same fact several ways -- processing code, processing
type, and the sign of the billing amount; the amount in local currency, in
billing currency, and the rate between them -- so any disagreement means one of
them is corrupt. Which one, the arithmetic cannot say, so this stage stops at
the flag.

The port is the most direct of the eleven: every check is already a boolean
mask over columns, which is a boolean column expression here with nothing to
reorganise. Three things are not direct, and all three are pandas
representation leaking into the answer.

``mask.fillna(False)`` in the original's ``raise_flag`` is doing real work. A
comparison against a null is False in pandas and null in Spark, and a null
would propagate through the concatenation and blank the whole flags string for
that row. Every mask below is therefore coalesced.

``PROCESSING_TYPE_CLEANED.astype(str)`` turns a null into the literal string
``"nan"``, because the column is a Categorical and an unclassified code is not
one of its categories. That is not a tidy detail: it decides CODE_TYPE_MISMATCH
for every row whose code nobody has classified -- pandas compares the stated
type against ``"nan"``, they differ, and the row is flagged. A Spark comparison
against null would yield null, coalesce to False, and quietly drop a flag the
pandas run raises. The literal is reproduced rather than corrected, because
this is a port and the two must agree; whether "unclassified code" ought to
read as a type mismatch is a question for the pandas side.

The flag order is the order the checks are written, and it is part of the
output -- the flags column is a ``;``-joined string, not a set.
"""

from pyspark.sql import functions as F

from src.schema.codes import REFUND_LABEL
from src.rules import loader
from src.spark import audit
from src.spark.spark_utils import lookup, text

FLAGS = "VALIDATION_FLAGS"

# What ``Categorical.astype(str)`` spells a null as. See the module docstring.
_NULL_AS_TEXT = "nan"


def _flag(condition):
    """
    :param condition: A boolean expression that may be null.
    :returns: The same, with null read as "did not fail this check" -- which is
        ``mask.fillna(False)`` on the pandas side.
    """
    return F.coalesce(condition, F.lit(False))


def checks(frame, policy) -> list[tuple[str, object]]:
    """
    The checks this run makes, in the order it makes them.

    Built here rather than inside ``apply`` so that the counting pass can ask
    the same question and get the same answer. The pandas original keeps the
    list on the instance and the report reads it back off there; the list is a
    pure function of which columns are present and which the policy requires,
    so deriving it twice is the same thing as remembering it once -- and it is
    the derivation that has to be shared, since a check missing from the
    report and a check that never ran look identical.

    :param frame: Frame as the earlier stages left it. Each check runs only
        where its columns are present -- a run over a source with no FX_RATE
        does not ask that question, and must not report a zero for it, which
        would claim it looked and found nothing.
    :param policy: Read for the required-column list and both FX tolerances,
        each of which is a judgement about how strict this pipeline should be
        rather than a fact about the data.
    :returns: ``(code, condition)`` in the order the checks are made, because
        the flags column is a joined string and the order is visible in it.
    """
    columns = set(frame.columns)
    checks: list[tuple[str, object]] = []

    for column in policy.validation.required_columns:
        if column in columns:
            checks.append(
                (f"REQUIRED_NULL[{column}]", F.col(column).isNull())
            )

    if {"SETTLE_DATE_CLEANED", "TXN_DATE_TIME_CLEANED"} <= columns:
        settle = F.col("SETTLE_DATE_CLEANED")
        txn = F.col("TXN_DATE_TIME_CLEANED")
        checks.append(
            (
                "SETTLE_BEFORE_TXN",
                settle.isNotNull()
                & txn.isNotNull()
                # ``.dt.normalize()`` on both sides: a settlement recorded
                # earlier in the day than the transaction is not a settlement
                # that precedes it.
                & (F.to_date(settle) < F.to_date(txn)),
            )
        )

    if {"TXN_AMOUNT_CLEANED", "BILLING_AMOUNT", "FX_RATE"} <= columns:
        rate = F.col("FX_RATE").cast("double")
        billed = F.abs(F.col("BILLING_AMOUNT").cast("double"))
        # The amount is signed by transaction type and the billing amount by
        # its own convention, so the two are compared as magnitudes and the
        # direction is left to the sign checks below.
        expected = F.abs(F.col("TXN_AMOUNT_CLEANED").cast("double")) * rate
        # Only rows where all three values are present and the expectation is
        # non-zero: a missing rate is a gap, not a contradiction, and dividing
        # by zero would invent one.
        comparable = (
            expected.isNotNull() & billed.isNotNull() & (expected > 0)
        )
        drift = F.abs(expected - billed) / F.when(comparable, expected)
        checks.append(
            (
                "FX_RECONCILE_MISMATCH",
                comparable & (drift > F.lit(policy.fx.reconcile_tolerance)),
            )
        )

    if {"FX_RATE", "TXN_CCY"} <= columns:
        # The reconciliation above proves a row is internally consistent, which
        # a row using a dead rate still is: a stale peg applied to both the
        # rate and the billing amount agrees with itself and is wrong by a
        # factor of twenty. Only an outside reference can see that.
        expected_rate = lookup(
            F.upper(text("TXN_CCY")), loader.fx_rates()
        )
        stated = F.col("FX_RATE").cast("double")
        known = (
            expected_rate.isNotNull() & stated.isNotNull() & (stated > 0)
        )
        off = F.abs(stated - expected_rate) / F.when(known, expected_rate)
        checks.append(
            (
                "FX_RATE_OFF_REFERENCE",
                known & (off > F.lit(policy.fx.reference_tolerance)),
            )
        )

    if {"PROCESSING_TYPE_CLEANED", "BILLING_AMOUNT"} <= columns:
        amount = F.col("BILLING_AMOUNT").cast("double")
        is_refund = _type_as_text() == F.lit(REFUND_LABEL)
        checks.append(("REFUND_NEGATIVE", is_refund & (amount < 0)))
        checks.append(("PURCHASE_POSITIVE", ~is_refund & (amount > 0)))

    if {"MERCHANT_COUNTRY_EXPECTED", "MERCHANT_COUNTRY"} <= columns:
        # Only rows where the city actually implies a country: a blank
        # expectation means "no geography stated", never "mismatch".
        expected_country = F.col("MERCHANT_COUNTRY_EXPECTED")
        checks.append(
            (
                "GEO_CITY_COUNTRY_MISMATCH",
                (expected_country != "")
                & (text("MERCHANT_COUNTRY") != expected_country),
            )
        )

    if {"PROCESSING_TYPE", "PROCESSING_TYPE_CLEANED"} <= columns:
        checks.append(
            (
                "CODE_TYPE_MISMATCH",
                text("PROCESSING_TYPE") != _type_as_text(),
            )
        )

    return checks


def apply(frame, policy):
    """
    Asserts that the redundant encodings still agree, and records where they
    do not.

    :param frame: Frame as the earlier stages left it.
    :param policy: Handed straight to ``checks``.
    :returns: The frame with ``VALIDATION_FLAGS`` written.
    """
    # One expression per check rather than one per flagged row. The leading
    # separator is trimmed at the end instead of being conditional per row,
    # which is the same string for a fraction of the work.
    joined = (
        F.coalesce(F.col(FLAGS), F.lit(""))
        if FLAGS in frame.columns
        else F.lit("")
    )
    for code, condition in checks(frame, policy):
        joined = F.concat(
            joined,
            F.when(_flag(condition), F.lit(f";{code}")).otherwise(F.lit("")),
        )

    return frame.withColumn(FLAGS, F.regexp_replace(joined, "^;+", ""))


def metrics(frame, policy):
    """
    One total per check, including the checks that found nothing.

    The flags column is the diagnostic column here, and it has been one all
    along -- this stage already wrote its findings per row. Only the counting
    moved.

    The check list is rebuilt from the finished frame rather than remembered
    from the run, which is exact for one reason worth stating: this stage runs
    last and adds exactly one column, ``VALIDATION_FLAGS``, which no check
    keys on. The columns it sees here are therefore the columns it saw then.

    :param frame: The frame as the last stage left it.
    :param policy: Handed straight to ``checks``.
    :returns: ``(metric, request)`` pairs in report order.
    """
    if FLAGS not in frame.columns:
        return []

    flags = F.col(FLAGS)
    out = [
        (code, audit.rows(audit.carries(flags, code)))
        for code, _ in checks(frame, policy)
    ]
    out.append(("rows_with_any_flag", audit.rows(flags != "")))
    return out


def _type_as_text():
    """
    :returns: ``PROCESSING_TYPE_CLEANED`` with a null spelled the way
        ``Categorical.astype(str)`` spells it. See the module docstring -- this
        is load-bearing for CODE_TYPE_MISMATCH, not cosmetic.
    """
    return F.coalesce(
        F.col("PROCESSING_TYPE_CLEANED"), F.lit(_NULL_AS_TEXT)
    )
