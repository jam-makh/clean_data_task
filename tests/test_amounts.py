"""Amount parsing across accounting, thousands and European conventions."""

import pytest

from cleaning_task.cleaners.amounts import AmountNormalizer

parse = AmountNormalizer.parse


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
    """A lone comma before three digits is ambiguous; a zero-decimal currency settles it."""
    assert parse("1,500", "LBP") == 1500.0    # no minor unit -> thousands
    assert parse("1,500", "USD") == 1.500     # has minor unit -> decimal


def test_unreadable_returns_none_rather_than_zero():
    assert parse("n/a", "USD") is None
    assert parse("", "USD") is None


def test_source_file_parses_completely(transactions, report):
    """All 15 awkward values in the real file must resolve."""
    df = AmountNormalizer(report).apply(transactions)
    assert df["TXN_AMOUNT_CLEAN"].isna().sum() == 0
