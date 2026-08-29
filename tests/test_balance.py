"""
The running balance: what gets stated, and what the status admits about it.

Every test here is about the same pair of questions -- what number does this
row get, and how much is it worth trusting? The step states a figure wherever
it can reach an anchor in either direction, so almost nothing is refused; what
the tests assert instead is that the figure never travels without a status
honest enough to stop a consumer using it wrongly.

The one refusal left is UNAVAILABLE, and it has its own section below.
"""

import pandas as pd
import pytest

from src.cleaners.balance import (
    BASIS,
    CURRENCY,
    DISCREPANCY,
    FILLED,
    NORMALIZED,
    PROVEN,
    STATUS,
    STATUSES,
    BalanceReconstructor,
)
from src.utils.report import CleaningReport


@pytest.fixture
def report():
    return CleaningReport()


def frame(amounts, balances, account="A", billing=None, ccy=None, fx=None):
    """
    :param amounts: Signed transaction amounts, in sequence order.
    :param balances: Stated running balances, ``None`` where the source left
        the cell blank.
    :param billing: The other candidate mover, when the test is about which
        one the step picks. Absent by default, which is the shape of a source
        that has no billing column at all.
    :param ccy: The account's own currency, defaulting to USD so that tests
        which are not about currency are not also about conversion.
    :returns: The minimal frame this step reads.
    """
    n = len(amounts)
    df = pd.DataFrame(
        {
            "TXN_SEQ": range(1, n + 1),
            "ACCOUNT_ID": [account] * n,
            "TXN_AMOUNT_CLEANED": amounts,
            "RUNNING_BALANCE": balances,
            "TXN_CCY": ccy if ccy is not None else ["USD"] * n,
            "BILLING_CURRENCY": ["USD"] * n,
            "FX_RATE": fx if fx is not None else [1.0] * n,
        }
    )
    if billing is not None:
        df["BILLING_AMOUNT"] = billing
    return df


def two_regime(native_rows, billing_rows, ccy=None, fx=None):
    """
    A file that keeps its ledger one way and then keeps it another.

    Sized rather than hand-written, because the change has to be worth
    reporting before the step will report it: `regime_switch_penalty` is
    denominated in rows, so a handful of them either side is correctly read as
    noise. That is the property being relied on, not one being worked around,
    which is why the sizes are arguments here.

    The two movers are deliberately unequal on every row, so nothing in the
    result can be a coincidence of the fixture.

    :returns: The frame, and the TXN_SEQ of the first billing-regime row.
    """
    total = native_rows + billing_rows
    amounts = [-(10 + i % 7) for i in range(total)]
    billing = [-(100 + i % 5) for i in range(total)]

    balances, running = [], 1_000_000.0
    for i in range(total):
        running += amounts[i] if i < native_rows else billing[i]
        balances.append(round(running, 2))

    return (
        frame(amounts, balances, billing=billing, ccy=ccy, fx=fx),
        native_rows + 1,
    )


def bases(out):
    return list(out[BASIS].astype(str))


def currencies(out):
    return [None if pd.isna(v) else v for v in out[CURRENCY]]


def normalized(out):
    series = pd.to_numeric(out[NORMALIZED])
    return [None if pd.isna(v) else round(float(v), 2) for v in series]


def run(df, report):
    # Marking and counting are two calls: the step writes what it decided onto
    # every row, and the report is read back off those columns afterwards. In
    # a real run the pipeline does the second call for every step at once.
    step = BalanceReconstructor(report)
    out = step.apply(df)
    step.collect(out)
    return out


def states(out):
    return list(out[STATUS].astype(str))


def discrepancies(out):
    """:returns: The signed forward/backward gap, None where there was none."""
    return [
        None if pd.isna(v) else round(float(v), 2) for v in out[DISCREPANCY]
    ]


def values(out):
    """:returns: The published balances, with every flavour of null as None."""
    series = pd.to_numeric(out[FILLED])
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


def test_an_unreadable_amount_breaks_the_chain_but_not_the_account(report):
    """
    A gap the arithmetic cannot cross stops the two balances either side of
    it from confirming each other. It must not cost the account every row
    after it, though -- the next pair of stated balances starts a fresh
    chain and is judged on its own.

    Note which side row 2 is reached from. Its own amount is the unreadable
    one, so no anchor before it can reach it -- but reaching it backwards
    from row 3 needs only row 3's amount, which is readable. The hole blocks
    one direction and not the other, and the row gets the answer the
    surviving direction gives rather than nothing at all.
    """
    out = run(
        frame([0, None, 0, -10, -20], [100, None, 50, 40, 20]), report
    )
    assert states(out)[:3] == ["UNVERIFIED", "BACKWARD_DERIVED", "OBSERVED"]
    assert states(out)[3:] == ["OBSERVED", "OBSERVED"]
    assert values(out) == [100, 50, 50, 40, 20]


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
    assert FILLED not in out.columns


def test_billing_amount_is_not_used_where_the_local_amount_explains_it(report):
    """
    The unit error this source could contain. Where the ledger is kept in the
    account's own currency, BILLING_AMOUNT is the same purchase in USD, and a
    step built from it is off by the exchange rate -- catastrophically so on a
    currency like LBP. The detector is shown both and picks the one the stated
    balances actually move by, rather than the tidier-looking one.
    """
    opening = -6669698.88
    out = run(
        frame(
            [0, -2369474.06], [opening, opening - 2369474.06],
            # The USD figure for the same purchase. Stepping by this instead
            # would land on -6669750.39, so if it were ever preferred these
            # rows would not reconcile.
            billing=[0, -51.51], ccy=["LBP", "LBP"],
        ),
        report,
    )
    assert bases(out) == ["NATIVE", "NATIVE"]
    assert states(out) == ["OBSERVED", "OBSERVED"]


# --- which column moves the balance ----------------------------------------
#
# The step is given two candidates and no instruction about which is in force.
# Everything below is about it working that out from the stated balances, and
# about it declining to invent a change of convention that is not there.


def test_a_file_with_one_convention_is_never_split(report):
    """
    The null case, and the one a change-point detector gets wrong most easily.
    Every stated balance here moves by the local amount and none of them by
    the billing figure, so there is no seam -- and a step that reported one
    would be finding structure in noise.
    """
    out = run(
        frame(
            [0, -10, -20, -5], [100, 90, 70, 65],
            billing=[0, -1.0, -2.0, -0.5],
        ),
        report,
    )
    assert bases(out) == ["NATIVE"] * 4
    assert states(out) == ["OBSERVED"] * 4


def test_the_seam_is_found_rather_than_configured(report):
    """
    The shape of the real extract in miniature: the ledger moves by the local
    amount, then switches to moving by the billing figure, and nothing in the
    file says where. The step finds the row, names it in the report, and keeps
    reconciling across it -- a balance either side of the change is still
    OBSERVED, because the cumulative total is built from whichever mover was
    actually in force.

    No row number is configured anywhere. Moving the seam in the fixture moves
    the answer, which is the whole claim.
    """
    df, seam = two_regime(120, 120)
    out = run(df, report)

    assert bases(out) == ["NATIVE"] * 120 + ["BILLING"] * 120
    assert set(states(out)) == {"OBSERVED"}

    logged = {m: v for step, m, v in report.entries if step == "balance"}
    assert logged["basis.first_billing_seq"] == seam
    assert logged["basis[NATIVE]"] == 120
    assert logged["basis[BILLING]"] == 120


def test_the_seam_moves_when_the_file_does(report):
    """
    The same file with the change in a different place. Nothing about 120 is
    remembered between runs, which a hardcoded boundary could not manage and
    which is the reason this step detects rather than reads a constant.
    """
    df, seam = two_regime(200, 60)
    out = run(df, report)

    logged = {m: v for step, m, v in report.entries if step == "balance"}
    assert logged["basis.first_billing_seq"] == seam == 201
    assert bases(out) == ["NATIVE"] * 200 + ["BILLING"] * 60


def test_one_unexplained_row_does_not_open_a_regime(report):
    """
    A single row that only the other mover explains is a corrupt amount, not a
    change of accounting convention, and a detector that switched on it would
    carve the file into fragments. Changing convention costs far more than any
    one row can save, so the excursion is never taken -- the row is left in the
    regime it sits in and shows up as a contradiction instead, which is the
    honest description of it.

    This is the property that makes the step safe on a file nobody has looked
    at yet: it is biased towards reporting no change.
    """
    df, _ = two_regime(150, 0)
    # Corrupt one row's local amount so that only the billing figure explains
    # the step the stated balances take across it.
    df.loc[80, "TXN_AMOUNT_CLEANED"] = df.loc[80, "BILLING_AMOUNT"]
    df.loc[80, "RUNNING_BALANCE"] = (
        df.loc[79, "RUNNING_BALANCE"] + df.loc[80, "BILLING_AMOUNT"]
    )
    out = run(df, report)

    assert set(bases(out)) == {"NATIVE"}


def test_a_source_with_no_billing_column_is_all_native(report):
    """
    Nothing to detect and nothing to report. The v4 workbook has no billing
    amount at all, and the step has to run on it without asking for one.
    """
    out = run(frame([0, -10], [100, 90]), report)
    assert bases(out) == ["NATIVE", "NATIVE"]

    logged = {m: v for step, m, v in report.entries if step == "balance"}
    assert "basis.first_billing_seq" not in logged


# --- what currency the answer is in ----------------------------------------


def test_a_native_balance_keeps_its_own_currency(report):
    """
    The reason the currency column exists. A balance rebuilt from the local
    amount is in the local currency, and printing 10,000,000 without saying
    LBP invites it being read as dollars and summed with them.
    """
    out = run(
        frame(
            [0, -1000000], [10000000, 9000000],
            ccy=["LBP"] * 2, fx=[0.000023] * 2,
        ),
        report,
    )
    assert currencies(out) == ["LBP", "LBP"]
    assert normalized(out) == [230.0, 207.0]


def test_a_billing_regime_balance_is_denominated_in_usd(report):
    """
    Where the ledger moves by the billing figure it is keeping its books in
    the billing currency, so that is what the balance is in -- whatever the
    account happens to transact in. The same column therefore carries two
    denominations on one file, which is exactly why it has to say which.
    """
    df, seam = two_regime(120, 120, ccy=["LBP"] * 240, fx=[0.000023] * 240)
    out = run(df, report)

    assert currencies(out) == ["LBP"] * 120 + ["USD"] * 120

    # The LBP rows convert at the row's rate; the USD ones are already dollars
    # and are left alone rather than multiplied by a rate that does not apply.
    filled = list(pd.to_numeric(out[FILLED]))
    assert normalized(out)[seam - 1] == round(filled[seam - 1], 2)
    assert normalized(out)[0] == round(filled[0] * 0.000023, 2)


def test_a_usd_balance_normalizes_at_one_whatever_the_rate_says(report):
    """
    The rule that has to be asserted rather than computed. FX_RATE is not 1 on
    most USD rows of this source, and a conversion that read the column would
    quietly restate dollars as not-quite-dollars -- 100 USD becoming 100.44.
    A balance already in the target currency is worth itself.
    """
    out = run(frame([0, -10], [100, 90], fx=[1.004429, 1.005304]), report)
    assert currencies(out) == ["USD", "USD"]
    assert normalized(out) == [100.0, 90.0]


def test_the_raw_fx_rate_is_never_written_to(report):
    """
    The effective rate used for normalization and the rate the source stated
    are two different things, and the second has to survive the first: it is
    what `consistency` reconciles BILLING_AMOUNT against.
    """
    rates = [1.004429, 1.005304]
    out = run(frame([0, -10], [100, 90], fx=rates), report)
    assert list(out["FX_RATE"]) == rates


def test_a_non_usd_balance_with_no_rate_cannot_be_valued(report):
    """
    The one way a published balance fails to normalize. The balance itself is
    still sound and is still stated -- the missing thing is the rate, and the
    report counts the rows it happened to rather than filling them with a
    number nobody can justify.
    """
    out = run(
        frame([0, -10], [100, 90], ccy=["LBP"] * 2, fx=[None, None]), report
    )
    assert states(out) == ["OBSERVED", "OBSERVED"]
    assert currencies(out) == ["LBP", "LBP"]
    assert normalized(out) == [None, None]

    logged = {m: v for step, m, v in report.entries if step == "balance"}
    assert logged["normalized.unavailable"] == 2


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


# --- the invariant ----------------------------------------------------------
#
# The one property the whole redesign exists to guarantee, asserted directly
# rather than inferred from the cases below: every row carries either a number
# and a status explaining it, or the single status that admits there is none.

def test_every_row_carries_a_number_or_says_why_not(report):
    """
    Stated as an equivalence, not an implication. A null balance beside a
    status claiming provenance and a stated balance stamped UNAVAILABLE are
    both incoherent, and only checking one direction would catch one of them.
    """
    out = run(
        frame([100, 50, 25, 30, 40, 20], [100, 150, None, None, 1000, 1020]),
        report,
    )
    missing = out[FILLED].isna()
    unavailable = out[STATUS].astype(str).eq("UNAVAILABLE")
    assert list(missing) == list(unavailable)


def test_no_row_escapes_the_declared_vocabulary(report):
    """
    A status nobody declared is worse than a wrong one: every consumer
    branches on this column, and an unrecognised value reads as "not one of
    the bad ones" to code written as a blacklist.
    """
    out = run(
        frame([100, 50, 25, 30, 40, 20], [100, 150, None, None, 1000, 1020]),
        report,
    )
    assert set(states(out)) <= set(STATUSES)


def test_a_discrepancy_is_recorded_exactly_where_two_answers_existed(report):
    """
    The discrepancy column is meaningless anywhere the arithmetic had only
    one answer, and mandatory where it had two that disagree. Same
    equivalence, same reason.
    """
    out = run(
        frame([100, 50, 25, 30, 40, 20], [100, 150, None, None, 1000, 1020]),
        report,
    )
    recorded = out[DISCREPANCY].notna()
    broken = out[STATUS].astype(str).eq("CONTRADICTED")
    assert list(recorded) == list(broken)


# --- one-sided reconstruction ----------------------------------------------


def test_rows_before_the_first_stated_balance_count_backwards(report):
    """
    The case that started the redesign: an account whose opening balance the
    source never gave. There is nothing before these rows to count from, and
    a balance after them the arithmetic reaches exactly -- so they are
    counted backwards from it, which is the same equality with the sign
    reversed.
    """
    out = run(frame([-10, -20, 0, -5], [None, None, 100, 95]), report)
    assert states(out) == [
        "BACKWARD_DERIVED", "BACKWARD_DERIVED", "OBSERVED", "OBSERVED",
    ]
    # 100 is the balance after row 2 moved it by 0, so row 1 closed at 100
    # too, and row 0 at 100 + 20 before the 20 left. Counting back, not down.
    assert values(out) == [120, 100, 100, 95]


def test_rows_after_the_last_stated_balance_count_forwards(report):
    """
    The mirror case. Sound arithmetic with nothing after it to confirm the
    answer, which the status says and the value does not hide.
    """
    out = run(frame([0, -10, -20, -30], [100, 90, None, None]), report)
    assert states(out) == [
        "OBSERVED", "OBSERVED", "FORWARD_DERIVED", "FORWARD_DERIVED",
    ]
    assert values(out) == [100, 90, 70, 40]


def test_a_one_sided_figure_is_never_called_proven(report):
    """
    The distinction the status column exists to carry. A one-sided figure is
    exact arithmetic with nothing independent agreeing with it, and a
    consumer filtering on PROVEN must not receive one.
    """
    out = run(frame([0, -10, -20], [100, 90, None]), report)
    assert states(out)[2] == "FORWARD_DERIVED"
    assert "FORWARD_DERIVED" not in PROVEN
    assert "BACKWARD_DERIVED" not in PROVEN


# --- contradictory anchors --------------------------------------------------


def test_a_balance_the_transactions_contradict_is_flagged_not_dropped(report):
    """
    The defect this step exists for, under the new contract. The source says
    the balance fell by 5 while the transaction says 500 left the account.
    Which of the two is wrong is not something the arithmetic can say -- so
    the source's own figure stands and the status says the arithmetic
    disputes it, rather than the number quietly disappearing.
    """
    out = run(frame([0, -500, -500], [1000, 995, 990]), report)
    assert states(out) == ["CONTRADICTED"] * 3
    assert values(out) == [1000, 995, 990]


def test_a_contradicted_gap_keeps_both_answers(report):
    """
    Two pairs of stated balances each check out against their own neighbour,
    but 1000 is unreachable from 150 by the transactions between them --
    money moved that the file does not record. The forward answer is
    published and the discrepancy carries the backward one, so a reader can
    recover both and neither is presented as the truth.

    Adding the discrepancy to the published figure gives the other
    direction's answer exactly, which is why it is signed.
    """
    out = run(
        frame([100, 50, 25, 30, 40, 20], [100, 150, None, None, 1000, 1020]),
        report,
    )
    assert states(out)[2:4] == ["CONTRADICTED", "CONTRADICTED"]
    assert values(out)[2:4] == [175.0, 205.0]

    both = [
        (v, round(v + d, 2))
        for v, d in zip(values(out)[2:4], discrepancies(out)[2:4])
    ]
    assert both == [(175.0, 930.0), (205.0, 960.0)]


def test_a_contradicted_anchor_never_anchors_anything(report):
    """
    A stated balance two neighbours disprove is excluded from the anchor set,
    not merely flagged. Counting from it would spread a known error across
    every row that reaches for it -- a failure the status column could not
    warn anyone about, because the rows it damaged would look derived.
    """
    out = run(frame([0, -500, -500], [1000, 995, 990]), report)
    assert set(states(out)) == {"CONTRADICTED"}


# --- the one case with no number -------------------------------------------


def test_an_account_with_no_reachable_anchor_states_nothing(report):
    """
    UNAVAILABLE, and the only status that carries no figure. An unreadable
    amount severs the tail of this account from the only balance the source
    stated, and counting across the hole would produce a figure short by an
    amount nobody can name.
    """
    out = run(frame([0, -10, None, -25], [100, None, None, None]), report)
    assert states(out)[:2] == ["UNVERIFIED", "FORWARD_DERIVED"]
    assert states(out)[2:] == ["UNAVAILABLE", "UNAVAILABLE"]
    assert values(out)[2:] == [None, None]


def test_a_lone_stated_balance_still_anchors_its_account(report):
    """
    Untested is not disproved. One anchor with nothing to compare it against
    is the weakest evidence there is, and it is still the only evidence there
    is -- refusing to count from it would make the whole account UNAVAILABLE
    on the grounds that its single balance was never contradicted.
    """
    out = run(frame([0, -10], [100, None]), report)
    assert states(out) == ["UNVERIFIED", "FORWARD_DERIVED"]
    assert values(out) == [100, 90]


def test_an_unavailable_row_has_no_currency_and_no_valuation(report):
    """
    Both columns describe a figure. Where there is no figure they say
    nothing, rather than stating a denomination for a balance that does not
    exist and implying one does.
    """
    out = run(frame([0, -10, None, -25], [100, None, None, None]), report)
    assert states(out)[2:] == ["UNAVAILABLE", "UNAVAILABLE"]
    assert currencies(out)[2:] == [None, None]
    assert normalized(out)[2:] == [None, None]


def test_a_contradicted_row_is_still_denominated(report):
    """
    A disputed figure is denominated in something. Leaving the currency off
    would make the dispute unreadable rather than visible -- and the row
    carries a number, so the columns describing that number must carry one
    too.
    """
    out = run(
        frame([0, -500, -500], [1000, 995, 990], ccy=["LBP"] * 3), report
    )
    assert states(out) == ["CONTRADICTED"] * 3
    assert currencies(out) == ["LBP"] * 3
    assert normalized(out) == [1000.0, 995.0, 990.0]
