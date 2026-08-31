"""
How busy a user was in a month, and how many accounts they held by then.

The account count is the point-in-time one: accounts opened later are not
visible to a month that precedes them.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.rules import loader

TXN_COUNT = "txn_count"
DISTINCT_MERCHANTS = "distinct_merchants"
ACCOUNTS_HELD = "accounts_held"

# How many months since the user last transacted. A dormancy diagnostic, not
# a feature: it describes the pipeline's view of a quiet user rather than
# predicting anything Stage 4 asked for. Carried on the monthly facts so the
# run report can read it.
MONTHS_SINCE = "months_since_last_txn"

COUNTERPARTY = "counterparty"


def counterparties(frame=None):
    """
    The merchant name on each row, blanked where it names no counterparty.

    ``CARD SETTLEMENT``, ``STANDING ORDER`` and ``INTERNAL TRANSFER`` occupy
    the merchant column without being merchants -- they describe money moving
    inside the bank. Counting them would make a user with three sweep rules
    look like a user shopping at three merchants.

    :param frame: Unused; kept so the call reads like the other helpers.
    :returns: A column carrying the merchant name, null on internal movements.
    """
    internal = sorted(set(loader.internal_movement_labels().values()))
    names = F.col("merchant_name_cleaned")
    if not internal:
        return names
    return F.when(~names.isin(*internal), names)


def monthly(frame):
    """
    Transaction count and distinct counterparty count per user per month.

    :param frame: Cleaned transactions as ``source`` returned them.
    :returns: One row per ``(user_id, month)`` that had any transaction.
    """
    return frame.groupBy("user_id", "month").agg(
        # Every row counts, including the internal movements the distinct
        # count excludes -- they are transactions, they are just not shopping.
        F.count(F.lit(1)).cast("int").alias(TXN_COUNT),
        F.countDistinct(counterparties()).cast("int").alias(
            DISTINCT_MERCHANTS
        ),
    )


def accounts_held(frame, users):
    """
    How many accounts each user held going into each month.

    Counted as accounts whose first transaction falls strictly before the
    month, so a month cannot see an account that opens during or after it.
    ``COUNT(DISTINCT account_id)`` over the whole history is the leak this
    exists to avoid: it tells a model in month 3 that the user will open a
    fourth account in month 40.

    The running total is a window that ends one row before the current one.
    On a dense spine that frame is "every month strictly before this one",
    which is the point-in-time rule stated as a frame rather than enforced by
    a later shift.

    Accounts are never decremented. The source carries no closure signal, and
    inferring one from silence would confuse a dormant account with a closed
    one.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param users: The dense user spine.
    :returns: The spine with ``accounts_held``.
    """
    opened = (
        frame.filter(F.col("month").isNotNull())
        .groupBy("user_id", "account_id")
        .agg(F.min("month").alias("opened_month"))
    )

    # One row per month an account first appears, so the running total below
    # counts each account exactly once.
    per_month = opened.groupBy(
        "user_id", F.col("opened_month").alias("month")
    ).agg(F.count("*").alias("opened"))

    joined = users.join(per_month, on=["user_id", "month"], how="left")
    joined = joined.withColumn(
        "opened", F.coalesce(F.col("opened"), F.lit(0))
    )

    before = (
        Window.partitionBy("user_id")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    return joined.select(
        "user_id",
        "month",
        F.coalesce(F.sum("opened").over(before), F.lit(0))
        .cast("int")
        .alias(ACCOUNTS_HELD),
    )


def months_since_last_txn(users, active):
    """
    How stale a user's activity is at the end of each month.

    Zero in a month the user transacted, one in the month after, and so on.
    Null before the user's first transaction, where there is no last month to
    count from.

    :param users: The dense user spine.
    :param active: One row per user-month that had any transaction.
    :returns: The spine with ``months_since_last_txn``.
    """
    flags = users.join(
        active.select("user_id", "month").withColumn(
            "active", F.lit(True)
        ),
        on=["user_id", "month"],
        how="left",
    )

    # The month's own ordinal, and the last ordinal at which the user was
    # active -- held forward, so a quiet month keeps pointing back at the last
    # busy one.
    ordinal = F.year("month") * F.lit(12) + F.month("month")
    history = (
        Window.partitionBy("user_id")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    last_active = F.last(
        F.when(F.col("active"), ordinal), ignorenulls=True
    ).over(history)

    return flags.select(
        "user_id",
        "month",
        (ordinal - last_active).cast("int").alias(MONTHS_SINCE),
    )
