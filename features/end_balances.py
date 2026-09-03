"""
Account month-end balances, carried forward through quiet months, then rolled
up to the user.

Reads ``running_balance_normalized`` only, so the figures being summed across
accounts are all USD.

The three provenance columns this produces -- how many accounts contributed,
how many supplied a balance, and whether any figure was carried -- are
diagnostics. They travel on the internal frames so validation and the run
report can read them, and they are not feature columns.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from features.settings import FeatureSettings

# The USD balance this stage publishes, and the column every lag of it is
# taken from. The target reads the same column, so the label and its own
# history are on one scale by construction.
BALANCE = "closing_balance_usd"

# Per-account provenance, carried on the account spine.
ACCOUNT_BALANCE = "account_balance_usd"
OBSERVED = "balance_observed"
CARRIED = "balance_carried"

# Per-user provenance, carried on the monthly facts. Diagnostics, not
# features -- see the module docstring.
CONTRIBUTING = "accounts_contributing"
WITH_BALANCE = "accounts_with_balance"
IS_CARRIED = "balance_is_carried_forward"

DIAGNOSTICS = (CONTRIBUTING, WITH_BALANCE, IS_CARRIED)


def month_end_by_account(frame, config: FeatureSettings):
    """
    The last eligible balance each account states in each month ordered by txn_seq

    Taken as ``max`` over a struct rather than a sorted window. Both find the
    same row; the aggregate is a hash aggregate with no ordering stage behind
    it, and a window would sort every account-month to read one row of each.

    A row whose ``txn_seq`` is null cannot win the comparison, where the
    pandas implementation sorted it last and let it win. That is stricter,
    and it is the right way round: a row with no position in the chain is not
    evidence about where the chain ended.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param config: The build settings; supplies the eligible statuses.
    :returns: One row per ``(account_id, month)`` that stated one, with the
        balance in USD.
    """
    eligible = frame.filter(
        F.col("running_balance_status").isin(*config.eligible_statuses)
        & F.col("running_balance_normalized").isNotNull()
    )

    latest = eligible.groupBy("account_id", "month").agg(
        F.max(
            F.struct(
                F.col("txn_seq").alias("seq"),
                F.col("running_balance_normalized").alias("balance"),
            )
        ).alias("last")
    )

    return latest.select(
        "account_id",
        "month",
        F.col("last.balance").alias("observed_balance_usd"),
    )


def carry_forward(accounts, month_ends):
    """
    Fills the dense account spine, holding the last known balance through
    months the account was quiet.

    A balance persists whether or not it moves, so a month with no transaction
    does not lose one. Nothing is filled backwards: before an account's first
    eligible balance there is no figure to hold, and inventing one would put a
    number where the source has none. That is what the frame at the unbounded
    preceding edge expresses -- ``last(ignorenulls=True)`` over a window that
    ends at the current row sees only earlier months.

    :param accounts: The dense account spine.
    :param month_ends: Observed month-end balances per account-month.
    :returns: The spine with ``account_balance_usd``, whether it was observed
        in that month, and whether it was carried into it.
    """
    history = (
        Window.partitionBy("account_id")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    filled = accounts.join(month_ends, on=["account_id", "month"], how="left")
    filled = filled.withColumn(
        OBSERVED, F.col("observed_balance_usd").isNotNull()
    )
    filled = filled.withColumn(
        ACCOUNT_BALANCE,
        F.last("observed_balance_usd", ignorenulls=True).over(history),
    )

    # Carried, not observed: a figure is present in this month but was stated
    # in an earlier one. Months before the first observation are neither.
    filled = filled.withColumn(
        CARRIED, ~F.col(OBSERVED) & F.col(ACCOUNT_BALANCE).isNotNull()
    )

    return filled.drop("observed_balance_usd")


def roll_up(filled):
    """
    Sums account balances into one figure per user per month.

    Summing is only legitimate because every value is USD. The two count
    columns travel with it so a partial rollup -- a user whose second account
    has no reachable balance yet -- is visible rather than looking like a real
    decline.

    ``sum`` returns null when every input is null, which is exactly the
    ``min_count=1`` semantics the pandas version had to ask for: a user with
    no known balance is reported as unknown, not as holding zero.

    :param filled: The account spine with balances carried forward.
    :returns: One row per ``(user_id, month)`` with the closing balance and
        its provenance counts.
    """
    return filled.groupBy("user_id", "month").agg(
        F.sum(ACCOUNT_BALANCE).alias(BALANCE),
        F.countDistinct("account_id").cast("int").alias(CONTRIBUTING),
        F.count(ACCOUNT_BALANCE).cast("int").alias(WITH_BALANCE),
        F.max(F.col(CARRIED)).alias(IS_CARRIED),
    )


def monthly(frame, accounts, config: FeatureSettings):
    """
    The whole balance path: month-end per account, carried forward, rolled up.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param accounts: The dense account spine.
    :param config: The build settings; supplies the eligible statuses.
    :returns: One row per ``(user_id, month)``.
    """
    return roll_up(carry_forward(accounts, month_end_by_account(frame, config)))
