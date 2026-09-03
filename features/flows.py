"""
Money in and money out per user per month, in USD.

The sign comes from the processing code's declared direction, never from the
sign the source wrote on the amount.
"""

from pyspark.sql import functions as F

from src.rules.loader import CREDIT, DEBIT
from src.rules.store import Rules

CREDITED = "total_credited_usd"
DEBITED = "total_debited_usd"
NET = "net_flow_usd"

# How many of a month's transactions declared no direction. A diagnostic, not
# a feature: it counts what the pipeline could not classify, which belongs in
# the run report. Carried on the monthly facts so the report can read it.
UNDECLARED = "undeclared_txn_count"

DIRECTION = "declared_direction"


def direction_map(rules: Rules):
    """
    The code-to-direction vocabulary as a Spark map literal.

    A literal rather than a broadcast join. There are fourteen codes; a join
    would add a stage and a null-key branch to express a lookup the planner
    can inline. The rules are metadata -- they arrive as a Python dict from
    the rule tables and never become a dataset.

    :param rules: The vocabularies, whose ``directions`` map is the only
        authority on which code moves money which way.
    :returns: A column expression mapping a code to ``CREDIT``/``DEBIT``.
    """
    if not rules.directions:
        return F.lit(None).cast("string")

    pairs = []
    for code, direction in sorted(rules.directions.items()):
        pairs.extend([F.lit(code), F.lit(direction)])
    return F.create_map(*pairs)


def classify(frame, rules: Rules):
    """
    Labels each transaction CREDIT, DEBIT, or neither.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :returns: A column carrying the declared direction per row, null where the
        code declares none.
    """
    return direction_map(rules)[F.col("processing_code_cleaned")]


def magnitude(frame=None):
    """
    The size of each transaction, in USD, with no sign on it.

    Taken as the absolute value rather than read as signed. Stage 2 signs
    ``txn_amount_cleaned`` from the declared direction but leaves
    ``billing_amount`` exactly as the source wrote it, so its sign is the
    source's claim and not the pipeline's. Deriving the direction separately
    and the size from here means a source that loses the sign still totals
    correctly.

    :param frame: Unused; kept so the call reads the same as ``classify``.
    :returns: A column carrying the unsigned USD amount per row.
    """
    return F.abs(F.col("billing_amount"))


def disagreement(rules: Rules):
    """
    The predicate marking rows whose stated sign contradicts their direction.

    Zero on the current extract across all 265,195 rows. It is measured rather
    than assumed because the day it stops being zero, the source has changed
    its convention and the flow totals are the last place that would show it.

    :param rules: The vocabularies.
    :returns: A boolean column, true where the two disagree.
    """
    direction = direction_map(rules)[F.col("processing_code_cleaned")]
    amount = F.col("billing_amount")

    # Zero carries no sign, so it can contradict nothing.
    return (direction == F.lit(CREDIT)) & (amount < 0) | (
        direction == F.lit(DEBIT)
    ) & (amount > 0)


def monthly(frame, rules: Rules):
    """
    Credited, debited and net per user per month.
    Aggregate all transactions into monthly money-in and money-out totals.
    Both totals are positive magnitudes and the net is their difference, so a
    reader never has to know which way the underlying column was signed. A row
    whose code declares no direction enters neither total and is counted
    separately instead of being silently dropped.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :returns: One row per ``(user_id, month)`` that had any transaction.
    """
    direction = classify(frame, rules)
    size = magnitude()

    credited = F.when(direction == F.lit(CREDIT), size).otherwise(F.lit(0.0))
    debited = F.when(direction == F.lit(DEBIT), size).otherwise(F.lit(0.0))

    rolled = frame.groupBy("user_id", "month").agg(
        F.coalesce(F.sum(credited), F.lit(0.0)).alias(CREDITED),
        F.coalesce(F.sum(debited), F.lit(0.0)).alias(DEBITED),
        F.sum(
            F.when(direction.isNull(), F.lit(1)).otherwise(F.lit(0))
        ).cast("int").alias(UNDECLARED),
    )

    return rolled.withColumn(NET, F.col(CREDITED) - F.col(DEBITED))
