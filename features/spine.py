"""
The dense account-by-month and user-by-month timelines every fact hangs on.

Built before any lag is taken, so ``prev_1m`` means the immediately preceding
calendar month rather than the previous month that happened to have a
transaction.

On Spark the density is one expression: ``sequence`` generates the months
between an account's first and the end of the window, and ``explode`` turns
that array into rows. The pandas version needed a hand-built ragged index to
avoid a per-account loop; here the engine does it.
"""

from pyspark.sql import functions as F

from src.config_readers.errors import ConfigError

FIRST_MONTH = "first_month"


def account_owners(frame):
    """
    Maps each account to the user that holds it.

    :param frame: Cleaned transactions as ``source`` returned them.
    :returns: One row per account, with its ``user_id``.
    :raises ConfigError: If an account appears under two users. The rollup
        sums account balances into a user total, and an account counted under
        two users would be counted twice.
    """
    owners = frame.select("account_id", "user_id").distinct()

    shared = (
        owners.groupBy("account_id")
        .agg(F.count("*").alias("owners"))
        .filter(F.col("owners") > 1)
        .orderBy("account_id")
        .limit(5)
        .collect()
    )
    if shared:
        offenders = ", ".join(row["account_id"] for row in shared)
        raise ConfigError(
            f"account(s) appear under more than one user, e.g. {offenders}. "
            f"The user rollup cannot be trusted until that is resolved "
            f"upstream."
        )
    return owners


def window_end(frame, through=None):
    """
    The last month the table covers, and the first the source states.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param through: Last month the table covers, as a date or ``YYYY-MM-DD``
        string. Defaults to the last month seen in the source.
    :returns: ``(first_month, last_month)`` as Python dates.
    :raises ConfigError: If no row carries a usable month, or the window ends
        before the first transaction.
    """
    bounds = frame.agg(
        F.min("month").alias("first"), F.max("month").alias("last")
    ).first()

    if bounds["first"] is None:
        raise ConfigError(
            "no transaction carries a parseable txn_ts, so there is no "
            "timeline to build"
        )

    first = bounds["first"]
    last = bounds["last"]

    if through is not None:
        import datetime

        if isinstance(through, str):
            last = datetime.date.fromisoformat(through)
        elif isinstance(through, datetime.datetime):
            last = through.date()
        else:
            last = through
        # Normalised to the first of the month, because the spine's step is a
        # month and a mid-month bound would shift every generated date.
        last = last.replace(day=1)

    if last < first:
        raise ConfigError(
            f"the observation window ends at {last}, before the first "
            f"transaction at {first}"
        )
    return first, last


def account_months(frame, through=None):
    """
    The dense account timeline: every calendar month from an account's first
    transaction to the end of the observation window, active or not.

    The window ends at ``through`` for every account, not at each account's
    own last transaction. An account that goes quiet has not ceased to exist
    -- its balance persists and it should keep producing rows -- and ending
    each account where it fell silent would make the table's coverage depend
    on when a user stopped spending.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param through: Last month the table covers. Defaults to the last month
        seen in the source. Stated explicitly by the point-in-time tests,
        which rebuild over a truncated source and need the window to be the
        same one: a globally quiet final month is still a month, and the
        window is a property of the run rather than of the last transaction.
    :returns: One row per ``(account_id, user_id, month)``.
    :raises ConfigError: If no row carries a usable month, or the window ends
        before the first transaction.
    """
    _, last = window_end(frame, through)

    opened = (
        frame.filter(F.col("month").isNotNull())
        .groupBy("account_id")
        .agg(F.min("month").alias(FIRST_MONTH))
        # An account that opens after the window closes contributes no months.
        # Filtered rather than clipped: `sequence` refuses a range that runs
        # backwards, so an unfiltered account would fail the job rather than
        # produce nothing.
        .filter(F.col(FIRST_MONTH) <= F.lit(last))
    )

    spine = opened.select(
        "account_id",
        F.explode(
            F.sequence(
                F.col(FIRST_MONTH),
                F.lit(last).cast("date"),
                F.expr("INTERVAL 1 MONTH"),
            )
        ).alias("month"),
    )

    # Broadcast: one row per account against the dense spine, which is the
    # larger side by a factor of however many months the window spans.
    return spine.join(
        F.broadcast(account_owners(frame)), on="account_id", how="left"
    ).select("account_id", "user_id", "month")


def user_months(accounts):
    """
    The user timeline, derived from the account one so the two cannot disagree
    about which months exist.

    :param accounts: The dense account spine.
    :returns: One row per ``(user_id, month)``.
    """
    return accounts.select("user_id", "month").distinct()


def calendar_features(users):
    """
    The two columns that legitimately read the row's own month.

    Both are properties of the Gregorian calendar and are fixed before the
    month begins, which is why they are not lagged.

    :param users: The dense user spine.
    :returns: The spine with ``month_of_year`` and ``days_in_month``.
    """
    return users.select(
        "user_id",
        "month",
        F.month("month").alias("month_of_year"),
        F.dayofmonth(F.last_day("month")).alias("days_in_month"),
    )
