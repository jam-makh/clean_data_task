"""Deduplication and ID collision suffixing, on synthetic data."""

import pandas as pd

from src.cleaners.duplicates import DuplicateCleaner


def test_identical_rows_are_dropped(report):
    df = pd.DataFrame({"TXN_ID": [1, 1, 2], "TXN_AMOUNT": [10, 10, 20]})
    out = DuplicateCleaner(report).apply(df)
    assert len(out) == 2


def test_id_collisions_are_suffixed_not_dropped(report):
    """Two different rows sharing an ID may be two real transactions."""
    df = pd.DataFrame(
        {
            "TXN_ID": [1, 1, 2],
            "TXN_AMOUNT": [10, 99, 20],
            "TXN_DATE_TIME_CLEANED": pd.to_datetime(
                ["2022-01-02", "2022-01-01", "2022-01-03"]
            ),
        }
    )
    out = DuplicateCleaner(report).apply(df)
    assert len(out) == 3
    # Ordered by date, so the earlier row takes the _1 suffix. The ID that was
    # already unique is left untouched.
    ids = dict(zip(out["TXN_AMOUNT"], out["TXN_ID_CLEANED"]))
    assert ids[99] == "1_1" and ids[10] == "1_2"
    assert ids[20] == "2"


def test_txn_id_dtype_is_never_mutated(report):
    df = pd.DataFrame({"TXN_ID": [1, 1], "TXN_AMOUNT": [10, 99]})
    out = DuplicateCleaner(report).apply(df)
    assert pd.api.types.is_integer_dtype(out["TXN_ID"])


def test_real_file_has_no_duplicates(transactions, report):
    out = DuplicateCleaner(report).apply(transactions)
    assert len(out) == len(transactions)
    assert out["TXN_ID_CLEANED"].equals(transactions["TXN_ID"].map(str))
    assert out["TXN_ID_CLEANED"].is_unique
