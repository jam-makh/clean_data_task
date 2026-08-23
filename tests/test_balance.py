"""
The running balance: what gets published, and what gets refused.

Every test here is about the same question -- can this value be checked? --
because that is the only thing the step decides. A balance nobody can check is
withheld whether it was blank in the source or stated in it, and the tests
that matter most are the ones asserting a refusal.
"""

import pandas as pd
import pytest

from src.cleaners.balance import (
    ADJUSTED,
    ADJUSTED_STATUS,
    BalanceReconstructor,
)
from src.utils.report import CleaningReport


@pytest.fixture
def report():
    return CleaningReport()


def frame(amounts, balances, account="A"):
    """
    :param amounts: Signed transaction amounts, in sequence order.
    :param balances: Stated running balances, ``None`` where the source left
        the cell blank.
    :returns: The minimal frame this step reads.
    """
    return pd.DataFrame(
        {
            "TXN_SEQ": range(1, len(amounts) + 1),
            "ACCOUNT_ID": [account] * len(amounts),
            "TXN_AMOUNT_CLEANED": amounts,
            "RUNNING_BALANCE": balances,
        }
    )


def run(df, report):
    return BalanceReconstructor(report).apply(df)


def states(out):
    return list(out["RUNNING_BALANCE_STATUS"].astype(str))


def values(out):
    """:returns: The published balances, with every flavour of null as None."""
    series = pd.to_numeric(out["RUNNING_BALANCE_CLEANED"])
    return [None if pd.isna(v) else float(v) for v in series]


# --- filling a gap the arithmetic can close -------------------------------


def test_a_gap_between_two_agreeing_balances_is_filled(report):
    """
    The ordinary case: the source states 100, then leaves two rows blank,
    then states 70. The two purchases between them account for the whole
    drop, so the blanks are the intermediate balances and nothing is guessed.
    """
    out = run(frame([0, -10, -20, 0], [100, None, None, 70]), report)
    assert states(out) == ["OBSERVED", "DERIVED", "DERIVED", "OBSERVED"]
    assert values(out) == [100, 90, 70, 70]


def test_a_stated_balance_is_never_replaced_by_a_computed_one(report):
    """
    Where the source states a value and it checks out, that value is what
    goes out -- not the arithmetic's own answer to the same question. The two
    agree here by definition, and the source is still the one that gets to
    say it.
    """
    out = run(frame([0, -10], [100, 90]), report)
    assert values(out) == [100, 90]
    assert states(out) == ["OBSERVED", "OBSERVED"]


# --- refusing what cannot be checked ---------------------------------------


def test_a_balance_the_transactions_contradict_is_withheld(report):
    """
    The defect this step exists for. The source says the balance fell by 5
    while the transaction says 500 left the account, so the stated figure
    cannot be what it claims to be. It is dropped rather than published or
    quietly corrected: which of the two is wrong is not something the
    arithmetic can say.
    """
    out = run(frame([0, -500, -500], [1000, 995, 990]), report)
    assert states(out) == ["CONTRADICTED"] * 3
    assert pd.isna(out["RUNNING_BALANCE_CLEANED"]).all()


def test_rows_before_the_first_stated_balance_are_unknown(report):
    """
    Nothing to count from. This is the case that started the whole design --
    an account whose opening balance the source never gave.
    """
    out = run(frame([-10, -20, 0, -5], [None, None, 100, 95]), report)
    assert states(out) == ["UNKNOWN", "UNKNOWN", "OBSERVED", "OBSERVED"]
    assert values(out)[:2] == [None, None]


def test_rows_after_the_last_stated_balance_are_unverified(report):
    """
    Calculable, but nothing confirms them. A trailing run has no closing
    balance to land on, so the arithmetic could produce a number and no
    evidence that the number is right -- which is exactly the thing this step
    will not publish.
    """
    out = run(frame([0, -10, -20, -30], [100, 90, None, None]), report)
    assert states(out) == ["OBSERVED", "OBSERVED", "UNVERIFIED", "UNVERIFIED"]
    assert values(out)[2:] == [None, None]


def test_a_lone_stated_balance_is_not_reported_as_contradicted(report):
    """
    One anchor and nothing to compare it against. Untested is not the same as
    failed, and calling it CONTRADICTED would accuse the source of an error
    the data never demonstrated.
    """
    out = run(frame([0, -10], [100, None]), report)
    assert states(out) == ["UNVERIFIED", "UNVERIFIED"]


def test_an_unreadable_amount_breaks_the_chain_but_not_the_account(report):
    """
    A gap the arithmetic cannot cross stops the two balances either side of
    it from confirming each other. It must not cost the account every row
    after it, though -- the next pair of stated balances starts a fresh
    chain and is judged on its own.
    """
    out = run(
        frame([0, None, 0, -10, -20], [100, None, 50, 40, 20]), report
    )
    assert states(out)[:3] == ["UNVERIFIED", "UNVERIFIED", "OBSERVED"]
    assert states(out)[3:] == ["OBSERVED", "OBSERVED"]
    assert values(out)[2:] == [50, 40, 20]


# --- the boundaries the step must respect ----------------------------------


def test_accounts_are_never_mixed(report):
    """
    Two accounts interleaved in one file. B's balance must not be reached by
    adding A's transactions, which is what a global cumulative sum would do.
    """
    df = pd.DataFrame(
        {
            "TXN_SEQ": [1, 2, 3, 4],
            "ACCOUNT_ID": ["A", "B", "A", "B"],
            "TXN_AMOUNT_CLEANED": [0, 0, -10, -500],
            "RUNNING_BALANCE": [100, 20, 90, -480],
        }
    )
    out = run(df, report)
    assert states(out) == ["OBSERVED"] * 4
    assert values(out) == [100, 20, 90, -480]


def test_the_frame_leaves_in_the_order_it_arrived(report):
    """
    The step sorts by TXN_SEQ to do its arithmetic. A caller's row order is
    not the step's to change, and a later step joining positionally would
    silently pair the wrong rows if it were.
    """
    df = frame([0, -10, -20], [100, 90, 70])
    shuffled = df.iloc[[2, 0, 1]]
    out = run(shuffled, report)
    assert list(out.index) == [2, 0, 1]
    assert list(out["TXN_SEQ"]) == [3, 1, 2]
    assert values(out) == [70, 100, 90]


def test_the_step_is_skipped_when_its_columns_are_absent(report):
    """A profile without a balance column runs the rest of the pipeline."""
    df = pd.DataFrame({"TXN_ID": ["a"]})
    out = run(df, report)
    assert "RUNNING_BALANCE_CLEANED" not in out.columns


def test_billing_amount_is_never_used_to_move_the_balance(report):
    """
    The unit error this source actually contains. The balance is in the
    account's own currency and BILLING_AMOUNT is in USD, so a step built from
    it is off by the exchange rate -- catastrophically so on a currency like
    LBP. Here the balance moves by the local amount and the billing figure
    beside it is ignored, not preferred because it is tidier.
    """
    opening = -6669698.88
    df = frame([0, -2369474.06], [opening, opening - 2369474.06])
    # The USD figure for the same purchase. Stepping by this instead would
    # land on -6669750.39, which is where the source's own back block goes
    # wrong -- so if it were ever preferred, these rows would not reconcile.
    df["BILLING_AMOUNT"] = [0, -51.51]
    out = run(df, report)
    assert states(out) == ["OBSERVED", "OBSERVED"]


# --- what the report has to say --------------------------------------------


def test_the_report_names_where_the_source_stops_reconciling(report):
    """
    The step never configures a boundary; it finds one. When part of a file
    stops reconciling, the sequence number where that starts is the finding
    someone upstream needs, so it goes in the report rather than staying
    implicit in a column of statuses.
    """
    run(frame([0, -10, -20, -1000], [100, 90, 500, 400]), report)
    logged = {m: v for step, m, v in report.entries if step == "balance"}
    assert logged["contradicted.first_seq"] == 3
    assert logged["status[CONTRADICTED]"] == 2


# --- the adjusted column ----------------------------------------------------
#
# A second balance, stated wherever there is a trusted one to count from. It
# answers "what would the balance be if only this account's own transactions
# moved it", which is a weaker claim than the published column makes and is
# kept in its own column for exactly that reason.

def adjusted(out):
    return [
        None if pd.isna(v) else round(float(v), 2) for v in out[ADJUSTED]
    ]


def adjusted_states(out):
    return list(out[ADJUSTED_STATUS].astype(str))


def test_the_adjusted_column_fills_rows_the_published_one_withholds(report):
    """
    Nothing follows the last stated balance, so the published column refuses
    every row after it. The adjusted column carries on counting.
    """
    out = run(frame([100, 50, 25, 30], [100, 150, None, None]), report)
    assert values(out) == [100.0, 150.0, None, None]
    assert adjusted(out) == [100.0, 150.0, 175.0, 205.0]
    assert adjusted_states(out) == [
        "VERIFIED", "VERIFIED", "UNTESTED", "UNTESTED",
    ]


def test_a_row_the_published_column_already_filled_is_not_a_projection(report):
    """
    Where the bracket closes the published column derives the value itself,
    so the adjusted column has nothing to add and says so. VERIFIED wins over
    every other state for exactly this reason: a row carrying a proven
    balance is not a projection, whatever the rows around it are doing.
    """
    out = run(frame([100, 50, 25], [100, None, 175]), report)
    assert states(out) == ["OBSERVED", "DERIVED", "OBSERVED"]
    assert adjusted(out) == [100.0, 150.0, 175.0]
    assert adjusted_states(out) == ["VERIFIED"] * 3


def test_a_projection_a_later_balance_refutes_still_states_a_value(report):
    """
    The whole point of the second column, and the case the real source is
    full of. Two pairs of stated balances each check out against their own
    neighbour, but 1000 is unreachable from 150 by the transactions between
    them -- money moved that the file does not record as a transaction. The
    published column withholds the rows in between; this one states what the
    arithmetic gives and marks it as refuted, so the number is available and
    nobody can mistake it for a verified one.
    """
    out = run(
        frame([100, 50, 25, 30, 40, 20], [100, 150, None, None, 1000, 1020]),
        report,
    )
    assert values(out)[2:4] == [None, None]
    assert adjusted(out)[2:4] == [175.0, 205.0]
    assert adjusted_states(out)[2:4] == ["CONTRADICTED", "CONTRADICTED"]


def test_rows_before_the_first_trusted_balance_count_backwards(report):
    """
    The same arithmetic with the sign reversed. The published column calls
    these UNKNOWN because nothing precedes them; the adjusted column reaches
    them from the balance that follows.
    """
    out = run(frame([40, 60, 30], [None, 100, 130]), report)
    assert states(out)[0] == "UNKNOWN"
    assert adjusted(out) == [40.0, 100.0, 130.0]


def test_an_account_with_no_trusted_balance_gets_no_projection(report):
    """
    A projection needs somewhere to count from. A lone stated balance is
    never verified, so it cannot anchor one, and inventing an origin of zero
    would state a balance the source never supported.
    """
    out = run(frame([100, 50, 25], [100, None, None]), report)
    assert states(out) == ["UNVERIFIED"] * 3
    assert adjusted(out) == [None, None, None]
    assert adjusted_states(out) == ["NO_ANCHOR"] * 3


def test_the_projection_never_crosses_an_unreadable_amount(report):
    """
    An unreadable amount is a hole in the running total. Counting across it
    would produce a figure short by an amount nobody can name, which is worse
    than leaving the row unanswered.
    """
    out = run(frame([100, 50, None, 25], [100, 150, None, None]), report)
    assert adjusted(out) == [100.0, 150.0, None, None]
    assert adjusted_states(out)[2:] == ["NO_ANCHOR", "NO_ANCHOR"]


def test_the_adjusted_column_never_uses_billing_amount(report):
    """
    The same guarantee the published column carries, and for the same reason:
    BILLING_AMOUNT is USD on every row whatever the account transacts in, so
    it cannot move a balance denominated in anything else.
    """
    df = frame([100, 50, 25], [100, 150, None])
    df["BILLING_AMOUNT"] = [1.0, 2.0, 3.0]
    df["TXN_CCY"] = ["LBP"] * 3
    out = run(df, report)
    assert adjusted(out) == [100.0, 150.0, 175.0]
