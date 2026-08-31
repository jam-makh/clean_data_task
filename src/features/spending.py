"""
Monthly spend split across the Stage 3 categories, as USD amounts.

Amounts only. A category's share of the month's spend is that category's
amount divided by ``total_spend_usd``, and both are columns on the same row --
publishing the quotient as well would duplicate information and cost one
column per category to do it. Stage 4 derives a share if Stage 4 wants one.

Both the eligibility rule and the MCC-to-category map come from the rule
tables; nothing about either is named in this file.
"""

from pyspark.sql import functions as F

from src.features.flows import direction_map
from src.rules.loader import DEBIT
from src.rules.store import Rules

TOTAL = "total_spend_usd"

# How a category column is spelled, before the lag layer prefixes it.
AMOUNT = "spend_{category}_usd"

CATEGORY = "spend_category"


def amount_column(category: str) -> str:
    """
    :param category: A declared spending category.
    :returns: The name of its amount column.
    """
    return AMOUNT.format(category=category)


def amount_columns(rules: Rules) -> list[str]:
    """
    :param rules: The vocabularies.
    :returns: Every category amount column, in display order.
    """
    return [amount_column(category) for category in rules.categories]


def eligible(frame, rules: Rules):
    """
    Which transactions count as spending.

    Both conditions are required and they are different questions. The
    direction says money left the account; eligibility says it left as
    consumption. Transfer Out satisfies the first and fails the second: it is
    a real outflow, so it belongs in ``total_debited``, but it is money moving
    between the customer's own accounts and belongs in no category.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies, which own both facts.
    :returns: A boolean column over the rows.
    """
    codes = F.col("processing_code_cleaned")
    is_debit = direction_map(rules)[codes] == F.lit(DEBIT)

    if not rules.spend_eligible:
        return F.lit(False)
    return is_debit & codes.isin(*sorted(rules.spend_eligible))


def categorize(frame, rules: Rules):
    """
    The spending category of each row, from its MCC.

    An MCC with no mapping -- and a row with no MCC at all -- lands in the
    residual rather than being dropped, so the category amounts always sum to
    the total.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :returns: A column carrying the category per row.
    """
    if not rules.mcc_categories:
        return F.lit(rules.residual)

    pairs = []
    for mcc, category in sorted(rules.mcc_categories.items()):
        pairs.extend([F.lit(mcc), F.lit(category)])

    mapped = F.create_map(*pairs)[F.col("mcc_code_cleaned")]
    return F.coalesce(mapped, F.lit(rules.residual))


def monthly(frame, rules: Rules):
    """
    Spend per category per user per month, plus the total.

    A month with no spending gets zero in every amount column -- zero spend in
    a category is an observation, and the dense spine is where that gets
    filled.

    The pivot is given its value list explicitly. That fixes the table's width
    to the declared vocabulary rather than to whatever this extract happened
    to contain, and it saves Spark the distinct scan it would otherwise run to
    discover the columns.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :returns: One row per ``(user_id, month)`` that had eligible spending.
    """
    spend = frame.filter(eligible(frame, rules)).select(
        "user_id",
        "month",
        categorize(frame, rules).alias(CATEGORY),
        F.abs(F.col("billing_amount")).alias("amount"),
    )

    wide = (
        spend.groupBy("user_id", "month")
        .pivot(CATEGORY, list(rules.categories))
        .agg(F.sum("amount"))
    )

    amounts = amount_columns(rules)

    # A category nothing landed in is a zero, not an absence: the pivot leaves
    # null where the group had no rows for that value. Renamed in the same
    # projection, so the vocabulary's spelling never reaches a column name.
    filled = [
        F.coalesce(F.col(f"`{category}`"), F.lit(0.0)).alias(column)
        for category, column in zip(rules.categories, amounts)
    ]

    total = F.lit(0.0)
    for column in amounts:
        total = total + F.coalesce(F.col(f"`{column}`"), F.lit(0.0))

    return (
        wide.select("user_id", "month", *filled)
        .withColumn(TOTAL, total)
        .select("user_id", "month", TOTAL, *amounts)
    )
