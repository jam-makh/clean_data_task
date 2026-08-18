"""Amount parsing across accounting, thousands and European conventions."""

import pandas as pd
import pytest

from src.cleaners.amounts import SIGN_FLAG, AmountNormalizer
from src.cleaners.codes import REFUND_LABEL

parse = AmountNormalizer.parse


def signed(rows, report):
    """
    :param rows: (raw amount, processing type label) pairs.
    :returns: The frame after parsing and sign restoration.
    """
    return AmountNormalizer(report).apply(
        pd.DataFrame(
            {
                "TXN_AMOUNT": [r for r, _ in rows],
                "TXN_CCY": ["USD"] * len(rows),
                "PROCESSING_TYPE_CLEANED": [t for _, t in rows],
            }
        )
    )


@pytest.mark.parametrize(
    "raw,currency,expected",
    [
        ("-104.39", "USD", -104.39),
        ("(808.41)", "USD", -808.41),          # accounting negative
        ("(2326526.00)", "LBP", -2326526.0),
        ("1,193.50", "USD", 1193.50),          # comma thousands, dot decimal
        ("172,22", "EUR", 172.22),             # comma decimal
        ("5.727.580,00", "LBP", 5727580.0),    # dot thousands, comma decimal
        ("2.026.953,00", "LBP", 2026953.0),
        ("291,75", "USD", 291.75),
        ("10037714", "LBP", 10037714.0),
        ("-1968163", "LBP", -1968163.0),
    ],
)
def test_conventions(raw, currency, expected):
    assert parse(raw, currency) == pytest.approx(expected)


def test_currency_breaks_the_genuine_tie():
    """
    A lone comma before three digits is ambiguous; a zero-decimal currency
    settles it.
    """
    assert parse("1,500", "LBP") == 1500.0    # no minor unit -> thousands
    assert parse("1,500", "USD") == 1.500     # has minor unit -> decimal


def test_unreadable_returns_none_rather_than_zero():
    assert parse("n/a", "USD") is None
    assert parse("", "USD") is None


def test_source_file_parses_completely(transactions, report):
    """All 15 awkward values in the real file must resolve."""
    df = AmountNormalizer(report).apply(transactions)
    assert df["TXN_AMOUNT_CLEANED"].isna().sum() == 0


def test_a_purchase_that_lost_its_minus_gets_it_back(report):
    """
    The source writes some amounts as text and drops the sign doing it, so a
    purchase arrives as a bare "409.34" with nothing to read a negative from.
    The transaction type is what says which way the money moved.
    """
    df = signed([("409.34", "Purchase")], report)
    assert df["TXN_AMOUNT_CLEANED"].iat[0] == -409.34
    assert SIGN_FLAG in df["VALIDATION_FLAGS"].iat[0]


def test_a_refund_is_positive(report):
    df = signed([("172.22", REFUND_LABEL)], report)
    assert df["TXN_AMOUNT_CLEANED"].iat[0] == 172.22


def test_only_the_sign_moves(report):
    """
    The digits are what the source got right. A European decimal stays parsed
    exactly as parsed; only its sign is taken from the type.
    """
    df = signed([("5.727.580,00", "Purchase")], report)
    assert df["TXN_AMOUNT_CLEANED"].iat[0] == -5727580.0


def test_an_already_correct_row_is_not_counted_as_corrected(report):
    """A count of repairs that includes untouched rows measures nothing."""
    df = signed(
        [("-104.39", "Purchase"), ("(808.41)", "Purchase")], report
    )
    assert list(df["TXN_AMOUNT_CLEANED"]) == [-104.39, -808.41]
    assert df["VALIDATION_FLAGS"].eq("").all()
    restored = [
        value for step, metric, value in report.entries
        if metric == "TXN_AMOUNT_CLEANED.sign_restored"
    ]
    assert restored == [0]


def test_the_real_file_ends_up_consistently_signed(
    transactions, mcc_reference
):
    """Every purchase negative, every refund positive, no exceptions."""
    from src.pipeline import TransactionCleaner

    df = TransactionCleaner(mcc_reference=mcc_reference).run(transactions)
    refund = df["PROCESSING_TYPE_CLEANED"].astype(str) == REFUND_LABEL
    amount = df["TXN_AMOUNT_CLEANED"]
    assert (amount[refund] > 0).all()
    assert (amount[~refund] < 0).all()
