"""
The point-in-time rule, checked the two ways it can fail.

Truncation catches a leak of any width; shift-invariance catches the
off-by-one that truncation alone can miss.
"""

import datetime

import pytest
from pyspark.sql import functions as F

from src.features import balances, builder, contract, monthly_facts
from src.features import spine, windows
from src.features import settings as feature_settings
from src.rules import store
from tests.harness import features


@pytest.fixture(scope="module")
def rules():
    """:returns: The vocabularies, read from the rule files."""
    return store.from_json()


@pytest.fixture(scope="module")
def config():
    """:returns: The feature build settings."""
    return feature_settings.load()


@pytest.fixture(scope="module")
def source_frame(spark):
    """:returns: The synthetic cleaned transactions."""
    return features.simple(spark)


@pytest.fixture(scope="module")
def build(source_frame, rules, config):
    """:returns: The whole build over the synthetic source."""
    return builder.assemble(source_frame, rules, config)


@pytest.fixture(scope="module")
def facts(source_frame, rules, config):
    """:returns: The monthly facts the point-in-time layer reads."""
    accounts = spine.account_months(source_frame)
    users = spine.user_months(accounts)
    filled = balances.carry_forward(
        accounts, balances.month_end_by_account(source_frame, config.balance)
    )
    return monthly_facts.build(source_frame, filled, users, rules, config)


def _rows_for_month(table, month: datetime.date) -> dict:
    """
    :param table: A built feature table.
    :param month: The month to keep.
    :returns: That month's rows, keyed by user.
    """
    return features.rows_by_key(
        table.filter(F.col("month") == F.lit(month)), "user_id"
    )


@pytest.mark.parametrize(
    "cutoff", ["2022-02-01", "2022-03-01", "2022-04-01", "2022-05-01"]
)
def test_truncating_after_a_month_leaves_that_month_identical(
    source_frame, rules, config, build, cutoff
):
    """
    The test the whole architecture is arranged around.

    Every row after month M is deleted and the table rebuilt. If any feature
    on M read a month at or after M, the rebuilt row differs. If none did, the
    row is unchanged to the bit.
    """
    month = datetime.date.fromisoformat(cutoff)
    truncated = source_frame.filter(F.col("month") <= F.lit(month))

    # The window is stated, not inferred: month M is still month M even when
    # nobody transacted in it, and letting the rebuild end at the last
    # surviving transaction would test a shorter table rather than the same
    # row.
    rebuilt = builder.assemble(truncated, rules, config, through=month)

    # The target legitimately reads month M, and truncation keeps M, so it
    # must survive too -- comparing every column including the label.
    assert _rows_for_month(build.table, month) == _rows_for_month(
        rebuilt.table, month
    )


def test_a_lag_of_one_is_the_previous_months_fact(build, facts):
    """
    ``prev_1m_X`` on month M must equal the monthly fact ``X`` on M minus one.

    Off by one in either direction still passes the truncation test on some
    months, which is why this is checked separately and directly against the
    facts frame.
    """
    published = features.rows_by_key(build.table, "user_id", "month")
    stated = features.rows_by_key(facts, "user_id", "month")

    def previous(user: str, month: datetime.date):
        """:returns: The month before ``month``, as the spine spells it."""
        year, number = month.year, month.month - 1
        if number == 0:
            year, number = year - 1, 12
        return (user, datetime.date(year, number, 1))

    checked = 0
    for (user, month), row in published.items():
        expected = stated.get(previous(user, month))
        assert row["prev_1m_txn_count"] == (
            expected["txn_count"] if expected else None
        )
        checked += 1

    assert checked == len(published)


def test_a_rolling_window_never_includes_the_rows_own_month(facts, config):
    """
    Rolling first and shifting after would put month M in its own window.

    Checked against the facts directly: each row's rolling mean must equal the
    mean of the three months before it, and be null where fewer than three
    exist.
    """
    rolled = features.rows_by_key(
        facts.select(
            "user_id",
            "month",
            windows.rolling("txn_count", config.windows, "mean").alias(
                "rolled"
            ),
        ),
        "user_id",
        "month",
    )

    by_user: dict[str, list] = {}
    for row in sorted(
        facts.collect(), key=lambda r: (r["user_id"], r["month"])
    ):
        by_user.setdefault(row["user_id"], []).append(row)

    for user, months in by_user.items():
        for position, row in enumerate(months):
            window = months[max(0, position - config.windows.rolling_months):
                            position]
            values = [
                month["txn_count"]
                for month in window
                if month["txn_count"] is not None
            ]
            actual = rolled[(user, row["month"])]["rolled"]

            if len(values) < config.windows.min_periods:
                assert actual is None
            else:
                assert actual == pytest.approx(sum(values) / len(values))


def test_a_zero_lag_is_refused():
    """A lag of zero is the row's own month and must not be expressible."""
    with pytest.raises(Exception, match="at least 1 month"):
        windows.shift("txn_count", 0)


def test_a_repeated_month_is_refused(spark):
    """
    Lags are read off an ordered window, so a user-month appearing twice would
    make ``lag(1)`` return a duplicate of the current month rather than the
    previous one -- a leak that produces plausible numbers and no error.
    """
    doubled = spark.createDataFrame(
        [
            ("u1", datetime.date(2022, 1, 1), 1),
            ("u1", datetime.date(2022, 1, 1), 2),
        ],
        schema="user_id string, month date, txn_count int",
    )
    with pytest.raises(Exception, match="duplicate"):
        windows.verify_grain(doubled)


def test_every_column_declares_when_it_is_known(build, rules):
    """
    A column with no declared ``known_at`` is exactly the state the rule
    forbids, so the contract refuses the frame rather than writing it.
    """
    declared = {
        column.name: column.known_at
        for column in contract.columns(rules.categories)
    }
    assert set(build.table.columns) == set(declared)

    # Exactly one column may read month M, and it is the label.
    targets = [
        name
        for name, known in declared.items()
        if known == contract.TARGET
    ]
    assert targets == [builder.TARGET]


def test_an_undeclared_column_is_refused(build, rules):
    """The contract is a gate, not a comment. This is what stops a diagnostic
    from drifting back into the feature table."""
    smuggled = build.table.withColumn("leaked_future_balance", F.lit(1.0))
    with pytest.raises(Exception, match="no declared known_at"):
        contract.verify(smuggled, rules.categories)


def test_the_diagnostics_are_lagged_like_everything_else(rules):
    """
    They are dropped from the output, not from the plan. Leaving them
    unlagged on the internal frames would make them a back door to month M for
    anything that reads those frames.
    """
    plan = {item.fact: item for item in monthly_facts.lag_plan(rules)}

    for fact in monthly_facts.DIAGNOSTIC_FACTS:
        assert fact in plan
        assert plan[fact].lags == (1,)
