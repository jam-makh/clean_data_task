"""
The aggregation: the dense spine, the balance rollup, and the credit/debit and
spending rules applied to it.

Every expected number here is small enough to check by hand against
``tests/harness/features.py``.

The build runs on Spark; the assertions read a dozen rows back with
``collect``. That boundary is the assertion, not the pipeline --
``test_features_engine.py`` is what keeps the pipeline itself clear of it.
"""

import pytest

from features import activity, end_balances, flows, spending
from features import spine
from features import builder
from features import settings as feature_settings
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
def table(build):
    """:returns: The projected feature table, as a dict keyed by user-month."""
    return features.rows_by_key(build.table, "user_id", "month")


@pytest.fixture(scope="module")
def facts(build):
    """
    :returns: The monthly facts, keyed the same way. This is where the
        diagnostics live now -- they describe month M and are deliberately not
        columns on the feature table.
    """
    return features.rows_by_key(build.facts, "user_id", "month")


def month_of(table: dict, user: str, month: str) -> dict:
    """
    :param table: A frame collected by ``rows_by_key``.
    :param user: Which user, as a fixture handle -- the frame holds the uuid
        ``features.handle`` maps it to, and translating here is what keeps the
        tests below readable.
    :param month: Which month, as ``YYYY-MM-DD``.
    :returns: That single row.
    """
    import datetime

    key = (features.handle(user), datetime.date.fromisoformat(month))
    assert key in table, f"expected a row for {user} {month}"
    return table[key]


def test_a_month_with_no_transactions_still_gets_a_row(source_frame):
    """
    The dense spine is what makes ``prev_1m`` mean the previous calendar
    month. Nobody transacts in April; both users must still have an April row.
    """
    accounts = spine.account_months(source_frame)
    users = spine.user_months(accounts)

    april = users.filter(users["month"] == features.QUIET_MONTH).collect()
    assert sorted(row["user_id"] for row in april) == sorted(
        [features.handle("u1"), features.handle("u2")]
    )

    # And the spine is contiguous, not just present: six months each.
    sizes = {
        row["user_id"]: row["months"]
        for row in users.groupBy("user_id")
        .count()
        .withColumnRenamed("count", "months")
        .collect()
    }
    assert sizes == {features.handle("u1"): 6, features.handle("u2"): 6}


def test_a_quiet_month_is_zero_flow_and_a_carried_balance(table, facts):
    """
    Nothing happened in April, so its flows are zero and its balance is
    March's. Read on the May row, where April is the preceding month.
    """
    may = month_of(table, "u1", "2022-05-01")

    assert may["prev_1m_txn_count"] == 0
    assert may["prev_1m_total_credited_usd"] == 0.0
    assert may["prev_1m_total_debited_usd"] == 0.0
    assert may["prev_1m_net_flow_usd"] == 0.0
    # March closed at 650 and April moved nothing.
    assert may["prev_1m_closing_balance_usd"] == 650.0

    # The provenance of that figure is a diagnostic, so it is on the facts and
    # not on the table.
    april = month_of(facts, "u1", "2022-04-01")
    assert april[end_balances.IS_CARRIED] is True


def test_the_lag_skips_no_month(table):
    """
    ``prev_1m`` on May must be April, not March -- which is the whole reason
    the spine is dense. March closed at 650 as well, so the two are told apart
    by the transaction count rather than the balance.
    """
    may = month_of(table, "u1", "2022-05-01")
    june = month_of(table, "u1", "2022-06-01")

    # April: silent. May: one ATM withdrawal.
    assert may["prev_1m_txn_count"] == 0
    assert june["prev_1m_txn_count"] == 1


def test_balances_from_several_accounts_are_summed(table, facts):
    """
    ``u2`` holds two accounts from March. The March closing balance is the sum
    of both, and it is only summable because every figure is USD.
    """
    april = month_of(table, "u2", "2022-04-01")

    # u2a closed March at 1600, u2b at 500.
    assert april["prev_1m_closing_balance_usd"] == 2100.0

    # How many accounts went into that sum is a diagnostic. It is still
    # computed -- the run report counts partial rollups from it -- but it is
    # not a feature column.
    march = month_of(facts, "u2", "2022-03-01")
    assert march[end_balances.CONTRIBUTING] == 2
    assert march[end_balances.WITH_BALANCE] == 2


def test_accounts_held_cannot_see_an_account_that_opens_later(table):
    """
    ``u2`` opens a second account in March. A row for February must still say
    one, and a row for March must say one too -- an account opened during M is
    not held before M begins.
    """
    held = {
        month: month_of(table, "u2", month)["accounts_held"]
        for month in (
            "2022-01-01",
            "2022-02-01",
            "2022-03-01",
            "2022-04-01",
            "2022-06-01",
        )
    }
    assert held == {
        "2022-01-01": 0,
        "2022-02-01": 1,
        "2022-03-01": 1,
        "2022-04-01": 2,
        "2022-06-01": 2,
    }


def test_transfer_out_is_debited_but_is_not_spending(table):
    """
    The decision this build was designed around. In February ``u1`` makes a
    200 purchase and a 100 transfer out: both leave the account, only one is
    consumption.
    """
    march = month_of(table, "u1", "2022-03-01")

    assert march["prev_1m_total_debited_usd"] == 300.0
    assert march["prev_1m_net_flow_usd"] == -300.0
    # The transfer is absent from every category, so the total is the purchase
    # alone and groceries holds all of it.
    assert march["prev_1m_total_spend_usd"] == 200.0
    assert march["prev_1m_spend_groceries_usd"] == 200.0
    assert march["prev_1m_spend_other_usd"] == 0.0


def test_a_fee_is_spending(source_frame, rules):
    """Fee is a debit and is spend-eligible, which is the other half of the
    same decision."""
    assert "24" in rules.spend_eligible
    assert "23" not in rules.spend_eligible

    eligible = source_frame.filter(spending.eligible(source_frame, rules))
    codes = {row["processing_code_cleaned"] for row in eligible.collect()}
    assert "24" in codes
    assert "23" not in codes


def test_a_code_with_no_declared_direction_enters_neither_total(table, facts):
    """
    ``u2`` makes one transaction in May under a code the rule file has never
    classified. It is counted, and it moves neither total.
    """
    june = month_of(table, "u2", "2022-06-01")

    assert june["prev_1m_txn_count"] == 1
    assert june["prev_1m_total_credited_usd"] == 0.0
    assert june["prev_1m_total_debited_usd"] == 0.0

    # How many rows were unclassifiable is a diagnostic, reported rather than
    # published as a column.
    may = month_of(facts, "u2", "2022-05-01")
    assert may[flows.UNDECLARED] == 1


def test_an_unmapped_mcc_lands_in_the_residual(table):
    """
    ``u2`` spends 100 in March at an MCC no rule maps. It must land in the
    residual rather than being dropped, so the categories still sum to the
    total.
    """
    april = month_of(table, "u2", "2022-04-01")

    assert april["prev_1m_spend_other_usd"] == 100.0
    assert april["prev_1m_total_spend_usd"] == 100.0


def test_category_amounts_sum_to_the_total(table, rules):
    """
    The residual is what guarantees this. A category list that dropped
    anything would make the parts sum to less than the whole.

    It is also what makes the removed share columns derivable: a share is an
    amount over this total, and both are on the row.
    """
    amounts = [
        f"prev_1m_spend_{category}_usd" for category in rules.categories
    ]

    checked = 0
    for row in table.values():
        stated = row["prev_1m_total_spend_usd"]
        if stated is None:
            continue
        summed = sum(row[name] or 0.0 for name in amounts)
        assert abs(summed - stated) < 1e-9
        checked += 1

    assert checked > 0


def test_the_table_carries_no_share_columns(build, rules):
    """
    Shares are derivable from the amounts and the total, so publishing them
    would duplicate information at one column per category.
    """
    shares = [
        name for name in build.table.columns if name.endswith("_share")
    ]
    assert shares == []


def test_a_share_is_recoverable_from_what_is_published(table):
    """
    The point of keeping the total. In February ``u1`` spent 200, all of it
    groceries, so the groceries share of that month is one -- computed, not
    stored.
    """
    march = month_of(table, "u1", "2022-03-01")

    share = (
        march["prev_1m_spend_groceries_usd"]
        / march["prev_1m_total_spend_usd"]
    )
    assert share == 1.0


def test_totals_are_positive_magnitudes(table):
    """
    Both totals are magnitudes and the net is their difference, so a reader
    never has to know how the underlying column was signed.
    """
    checked = 0
    for row in table.values():
        credited = row["prev_1m_total_credited_usd"]
        debited = row["prev_1m_total_debited_usd"]
        if credited is None or debited is None:
            continue

        assert credited >= 0
        assert debited >= 0
        assert abs(row["prev_1m_net_flow_usd"] - (credited - debited)) < 1e-9
        checked += 1

    assert checked > 0


def test_the_sign_the_source_wrote_is_not_what_decides_direction(
    source_frame, rules
):
    """
    Direction comes from the processing code. Flipping every stated sign must
    not change either total, because the magnitude is taken absolute.
    """
    from pyspark.sql import functions as F

    flipped = source_frame.withColumn(
        "billing_amount", -F.col("billing_amount")
    )

    original = features.rows_by_key(
        flows.monthly(source_frame, rules), "user_id", "month"
    )
    inverted = features.rows_by_key(
        flows.monthly(flipped, rules), "user_id", "month"
    )

    assert original == inverted


def test_internal_descriptors_are_not_counted_as_merchants(source_frame):
    """
    ``CARD SETTLEMENT`` and ``INTERNAL TRANSFER`` occupy the merchant column
    without naming a counterparty.
    """
    named = source_frame.select(
        activity.counterparties().alias("counterparty")
    ).filter("counterparty IS NOT NULL")
    counted = {row["counterparty"] for row in named.collect()}

    assert "CARREFOUR" in counted
    assert "INTERNAL TRANSFER" not in counted
    assert "CARD SETTLEMENT" not in counted


def test_a_contradicted_balance_does_not_supply_a_month_end(spark, config):
    """
    CONTRADICTED is excluded: the two reconstructions of that row disagree, so
    the published figure is one of two answers rather than the answer.
    """
    rows = [
        features.transaction("u1", "u1a", "2022-01", "salary", 100, 100, 1),
        features.transaction(
            "u1", "u1a", "2022-02", "purchase", 50, 999.0, 2,
            status="CONTRADICTED",
        ),
    ]
    frame = features.frame(spark, rows)

    month_ends = end_balances.month_end_by_account(frame, config).collect()
    assert len(month_ends) == 1
    assert month_ends[0]["observed_balance_usd"] == 100.0

    # And February inherits January's figure rather than the disputed one.
    accounts = spine.account_months(frame)
    rolled = features.rows_by_key(
        end_balances.monthly(frame, accounts, config), "user_id", "month"
    )
    february = month_of(rolled, "u1", "2022-02-01")
    assert february[end_balances.BALANCE] == 100.0
    assert february[end_balances.IS_CARRIED] is True
