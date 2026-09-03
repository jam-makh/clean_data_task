"""
What happened to a user during a month, assembled on the dense spine.

Nothing here is a feature. Every column describes month M and only becomes
usable once ``windows`` has shifted it.

Some of these columns are diagnostics rather than features -- the balance
provenance counts, the undeclared-direction count, the dormancy gap. They are
lagged with everything else, because they describe the same month the features
do and leaving them unshifted would make them a back door to month M. They are
dropped at the final projection, in ``contract.select``, and read before that
by the run report.
"""

from pyspark.sql import functions as F

from features import activity, end_balances, flows
from features import spending
from features.settings import FeatureSettings
from features.windows import Lagged
from src.rules.store import Rules

# Facts that are zero in a month with no transactions. A quiet month did not
# credit, debit or spend anything, and that is an observation 
# which is why the dense spine exists and why these are filled and the
# balance is not.
ZERO_MONEY = (
    flows.CREDITED,
    flows.DEBITED,
    flows.NET,
    spending.TOTAL,
)

ZERO_COUNTS = (
    flows.UNDECLARED,
    activity.TXN_COUNT,
    activity.DISTINCT_MERCHANTS,
)

# Facts that exist so the run report can count them, and for no other reason.
DIAGNOSTIC_FACTS = end_balances.DIAGNOSTICS + (
    flows.UNDECLARED,
    activity.MONTHS_SINCE,
)


def build(frame, filled, users, rules: Rules, config: FeatureSettings):
    """
    Joins every per-month aggregate onto the dense user spine.

    A month with no transaction keeps its row. Flows and activity are zero
    there; the balance is whatever was last known, carried forward by
    ``balances``.

    Every join is a left join from an aggregate already keyed on
    ``(user_id, month)``, so none of them can fan the spine out -- the grain
    is preserved structurally rather than checked afterwards.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param filled: The dense account spine with balances carried forward, as
        ``balances.carry_forward`` left it. Passed in rather than built here
        so the builder can hold on to it: it is what the run report's
        account-month diagnostics are counted over.
    :param users: The dense user spine.
    :param rules: The vocabularies.
    :param config: The build settings.
    :returns: One row per ``(user_id, month)``, describing that month.
    """
    balance = end_balances.roll_up(filled)
    flow = flows.monthly(frame, rules)
    active = activity.monthly(frame)
    spend = spending.monthly(frame, rules)
    stale = activity.months_since_last_txn(users, active)

    facts = users
    for part in (balance, flow, active, spend, stale):
        facts = facts.join(part, on=["user_id", "month"], how="left")

    for column in ZERO_MONEY:
        facts = facts.withColumn(
            column, F.coalesce(F.col(column), F.lit(0.0))
        )
    for column in ZERO_COUNTS:
        facts = facts.withColumn(
            column, F.coalesce(F.col(column), F.lit(0)).cast("int")
        )
    for column in spending.amount_columns(rules):
        facts = facts.withColumn(
            column, F.coalesce(F.col(column), F.lit(0.0))
        )

    # Two diagnostics that are counts of accounts, not of transactions: a
    # month the user held no account on the spine contributes zero, not null.
    for column in (end_balances.CONTRIBUTING, end_balances.WITH_BALANCE):
        facts = facts.withColumn(
            column, F.coalesce(F.col(column), F.lit(0)).cast("int")
        )
    facts = facts.withColumn(
        end_balances.IS_CARRIED,
        F.coalesce(F.col(end_balances.IS_CARRIED), F.lit(False)),
    )

    return facts


def lag_plan(rules: Rules) -> tuple[Lagged, ...]:
    """
    Which facts become which point-in-time columns.

    The names this produces are checked against ``contract`` by the build, so
    a fact renamed here and not there fails before anything is written.

    :param rules: The vocabularies, which fix the spending columns.
    :returns: The plan ``windows.build`` applies.
    """
    plan = [
        # The balance history: three lags, both rolling statistics, and the
        # month-on-month change.
        Lagged(
            end_balances.BALANCE,
            lags=(1, 2, 3),
            rolling=True,
            rolling_std=True,
            delta=True,
        ),
        Lagged(flows.CREDITED, rolling=True),
        Lagged(flows.DEBITED, rolling=True),
        Lagged(flows.NET, rolling=True),
        Lagged(activity.TXN_COUNT),
        Lagged(activity.DISTINCT_MERCHANTS),
        Lagged(spending.TOTAL),
    ]

    plan += [
        Lagged(spending.amount_column(category))
        for category in rules.categories
    ]

    # The diagnostics. Lagged like everything else -- see the module
    # docstring -- and dropped by the projection rather than by never being
    # computed, so validation can still read them.
    plan += [Lagged(fact) for fact in DIAGNOSTIC_FACTS]

    return tuple(plan)
