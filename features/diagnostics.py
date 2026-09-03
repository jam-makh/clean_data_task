"""
The pipeline diagnostics, counted once at the end of the build.

These numbers describe the build rather than the customer -- how many accounts
contributed a balance, whether one was carried forward, how many transactions
declared no direction, which MCCs went unmapped. They are not features, and a
model handed them would have to be told to ignore them; they are here to answer
"why do this run's numbers look like that". ``builder.manifest`` counts them
over the finished frames and hands them to ``report``, which is the only reader.

Three entry points, one per grain: ``transactions`` over the cleaned
transactions, ``account_months`` over the dense account spine after
carry-forward, ``user_months`` over the monthly facts before they are lagged.
Each returns plain Python, ready to serialise.

The collection pattern is ``src.spark.audit``'s, for the reason that file
gives: a Spark frame is a recipe, not a result, so counting one metric at a
time costs one job per number. Every scalar over a frame is evaluated in a
single ``agg``, and each breakdown is one grouped pass. Adding a metric to an
existing group costs nothing.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from features import activity, end_balances, flows
from features import spending
from features.settings import FeatureSettings
from src.rules import loader
from src.rules.loader import CREDIT, DEBIT
from src.rules.store import Rules

# The two exclusions named separately in the report because they fail for
# different reasons: one has two answers and one has none.
CONTRADICTED, UNAVAILABLE = "CONTRADICTED", "UNAVAILABLE"

# How many rows a breakdown publishes. Enough to act on, short enough that the
# manifest stays readable.
TOP_N = 10


def _count(condition):
    """
    Count how many rows satisfy a condition.

    :param condition: A boolean column.
    :returns: An expression counting the rows it holds for.
    """
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("long")


def _tally(frame, column, limit: int = TOP_N) -> dict[str, int]:
    """
    Create a frequency breakdown of values.

    :param frame: The frame to group.
    :param column: The label column, as an expression or a name.
    :param limit: How many labels to keep, most frequent first.
    :returns: Label to count, as plain Python.
    """
    rows = (
        frame.select(F.col(column).alias("label") if isinstance(column, str)
                    else column.alias("label"))
        .filter(F.col("label").isNotNull())
        .groupBy("label")
        .agg(F.count("*").alias("rows"))
        .orderBy(F.desc("rows"), "label")
        .limit(limit)
        .collect()
    )
    return {str(row["label"]): int(row["rows"]) for row in rows}


def transactions(frame, rules: Rules, config: FeatureSettings) -> dict:
    """
    Everything countable over the cleaned transactions themselves.

    One ``agg`` for the scalars, three grouped passes for the breakdowns.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :param config: The build settings; supplies the eligible statuses.
    :returns: The transaction-level counts, as plain Python.
    """
    status = F.col("running_balance_status")
    eligible_status = status.isin(*config.eligible_statuses)
    direction = flows.classify(frame, rules)
    is_spend = spending.eligible(frame, rules)
    mcc = F.col("mcc_code_cleaned")

    mapped = (
        mcc.isin(*sorted(rules.mcc_categories))
        if rules.mcc_categories
        else F.lit(False)
    )
    internal = sorted(set(loader.internal_movement_labels().values()))

    row = frame.agg(
        F.count("*").alias("txns_total"),
        _count(F.col("month").isNull()).alias("rows_without_parseable_month"),

        # Balance eligibility.
        _count(status == F.lit(CONTRADICTED)).alias(
            "rows_excluded_contradicted"
        ),
        _count(status == F.lit(UNAVAILABLE)).alias(
            "rows_excluded_unavailable"
        ),
        _count(~eligible_status).alias("rows_excluded_by_status_total"),
        _count(
            eligible_status & F.col("running_balance_normalized").isNull()
        ).alias("rows_eligible_but_null"),

        # Declared direction.
        _count(direction == F.lit(CREDIT)).alias("txns_credit"),
        _count(direction == F.lit(DEBIT)).alias("txns_debit"),
        _count(direction.isNull()).alias("txns_undeclared_direction"),
        F.coalesce(
            F.sum(
                F.when(direction.isNull(), F.abs(F.col("billing_amount")))
            ),
            F.lit(0.0),
        ).alias("undeclared_amount_usd"),
        _count(flows.disagreement(rules)).alias("sign_disagreements"),

        # Spending scope.
        _count(is_spend).alias("txns_spend_eligible"),
        _count((direction == F.lit(DEBIT)) & ~is_spend).alias(
            "txns_debit_not_spend_eligible"
        ),
        _count(is_spend & mcc.isNotNull() & ~mapped).alias(
            "spend_rows_unmapped_mcc"
        ),
        _count(is_spend & mcc.isNull()).alias("spend_rows_null_mcc"),
        _count(
            F.col("merchant_name_cleaned").isin(*internal)
            if internal
            else F.lit(False)
        ).alias("internal_descriptor_rows"),
    ).first()

    counts = {key: _plain(value) for key, value in row.asDict().items()}

    counts["rows_by_balance_status"] = _tally(
        frame, "running_balance_status", limit=32
    )
    counts["undeclared_by_code"] = _tally(
        frame.filter(direction.isNull()), "processing_code_cleaned"
    )
    counts["unmapped_mcc_top"] = _tally(
        frame.filter(is_spend & mcc.isNotNull() & ~mapped), "mcc_code_cleaned"
    )
    return counts


def account_months(filled) -> dict:
    """
    Everything countable over the dense account spine, after carry-forward.

    :param filled: The account spine as ``balances.carry_forward`` left it.
    :returns: The account-month counts, as plain Python.
    """
    observed = F.col(end_balances.OBSERVED)
    carried = F.col(end_balances.CARRIED)
    has_balance = F.col(end_balances.ACCOUNT_BALANCE).isNotNull()

    row = filled.agg(
        F.count("*").alias("account_months_total"),
        _count(observed).alias("account_months_observed"),
        _count(carried).alias("account_months_carried_forward"),
        _count(~has_balance).alias("account_months_without_balance"),
        F.countDistinct("account_id").alias("accounts_total"),
    ).first()

    counts = {key: _plain(value) for key, value in row.asDict().items()}

    # An account that never once stated an eligible balance. Counted by
    # asking each account whether any of its months observed one, which is a
    # second grouped pass over the same frame rather than a second scan of
    # the transactions.
    never = (
        filled.groupBy("account_id")
        .agg(F.max(observed.cast("int")).alias("ever"))
        .agg(_count(F.col("ever") == 0).alias("accounts_never_with_balance"))
        .first()
    )
    counts["accounts_never_with_balance"] = _plain(
        never["accounts_never_with_balance"]
    )

    # The longest unbroken stretch a single figure was held. Each carried
    # month inherits the running count of observations before it, so every
    # stretch shares the id of the observation that started it and a group-by
    # on that id is the stretch.
    history = (
        Window.partitionBy("account_id")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    runs = filled.withColumn(
        "_run", F.sum(observed.cast("int")).over(history)
    )
    longest = (
        runs.filter(carried)
        .groupBy("account_id", "_run")
        .agg(F.count("*").alias("months"))
        .agg(F.max("months").alias("max_carry_forward_run_months"))
        .first()
    )
    counts["max_carry_forward_run_months"] = _plain(
        longest["max_carry_forward_run_months"]
    ) or 0

    return counts


def user_months(facts, rules: Rules) -> dict:
    """
    Everything countable over the monthly facts, before they are lagged.

    Read here rather than off the feature table, because the diagnostics are
    dropped from that table by design and because these describe month M --
    which is what a data-quality report should describe, and exactly what a
    feature must not.

    :param facts: Monthly facts on the dense user spine.
    :param rules: The vocabularies.
    :returns: The user-month counts, as plain Python.
    """
    contributing = F.col(end_balances.CONTRIBUTING)
    with_balance = F.col(end_balances.WITH_BALANCE)
    since = F.col(activity.MONTHS_SINCE)

    residual = spending.amount_column(rules.residual)

    row = facts.agg(
        F.count("*").alias("user_months_total"),
        F.countDistinct("user_id").alias("users"),
        _count(F.col(activity.TXN_COUNT) == 0).alias("user_months_inactive"),
        _count(F.col(end_balances.BALANCE).isNull()).alias(
            "user_months_without_balance"
        ),
        _count(with_balance < contributing).alias(
            "user_months_partial_rollup"
        ),
        _count(F.col(end_balances.IS_CARRIED)).alias(
            "user_months_carried_forward"
        ),
        _count(F.col(flows.UNDECLARED) > 0).alias(
            "user_months_with_undeclared"
        ),
        _count(F.col(spending.TOTAL) == 0).alias("user_months_zero_spend"),
        F.max(since).alias("max_months_since_last_txn"),
        F.percentile_approx(since, 0.5).alias("months_since_last_txn_p50"),
        F.percentile_approx(since, 0.9).alias("months_since_last_txn_p90"),
        F.coalesce(F.sum(F.col(residual)), F.lit(0.0)).alias(
            "_residual_spend_usd"
        ),
        F.coalesce(F.sum(F.col(spending.TOTAL)), F.lit(0.0)).alias(
            "_total_spend_usd"
        ),
        F.min("month").alias("_first_month"),
        F.max("month").alias("_last_month"),
        F.countDistinct("month").alias("months"),
    ).first()

    counts = {key: _plain(value) for key, value in row.asDict().items()}

    total = counts.pop("_total_spend_usd") or 0.0
    residual_spend = counts.pop("_residual_spend_usd") or 0.0
    counts["total_spend_usd"] = round(float(total), 2)
    counts["residual_spend_usd"] = round(float(residual_spend), 2)
    # The one share worth publishing, and it belongs in a report rather than
    # in a column repeated on every row.
    counts["residual_share_of_spend"] = (
        round(float(residual_spend) / float(total), 6) if total else None
    )

    counts["first_month"] = str(counts.pop("_first_month"))
    counts["last_month"] = str(counts.pop("_last_month"))
    return counts


def _plain(value):
    """
    :param value: A value off a collected Spark row.
    :returns: The same value as a plain Python int, float or None -- so
        ``json.dumps`` does not meet a numpy or Decimal type it cannot
        serialise.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return round(float(value), 4)
    return value
