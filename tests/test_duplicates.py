"""Deduplication and ID collision sequencing, on synthetic data."""

import pandas as pd

from cleaning_task.cleaners.duplicates import DuplicateCleaner


def test_identical_rows_are_dropped(report):
    df = pd.DataFrame({"TXN_ID": [1, 1, 2], "TXN_AMOUNT": [10, 10, 20]})
    out = DuplicateCleaner(report).apply(df)
    assert len(out) == 2


def test_id_collisions_are_sequenced_not_dropped(report):
    """Two different rows sharing an ID may be two real transactions."""
    df = pd.DataFrame(
        {
            "TXN_ID": [1, 1, 2],
            "TXN_AMOUNT": [10, 99, 20],
            "TXN_DATE_TIME_CLEAN": pd.to_datetime(
                ["2022-01-02", "2022-01-01", "2022-01-03"]
            ),
        }
    )
    out = DuplicateCleaner(report).apply(df)
    assert len(out) == 3
    # Ordered by date, so the earlier row gets sequence 0.
    seq = dict(zip(out["TXN_AMOUNT"], out["TXN_ID_SEQ"]))
    assert seq[99] == 0 and seq[10] == 1


def test_txn_id_dtype_is_never_mutated(report):
    df = pd.DataFrame({"TXN_ID": [1, 1], "TXN_AMOUNT": [10, 99]})
    out = DuplicateCleaner(report).apply(df)
    assert pd.api.types.is_integer_dtype(out["TXN_ID"])


def test_real_file_has_no_duplicates(transactions, report):
    out = DuplicateCleaner(report).apply(transactions)
    assert len(out) == len(transactions)
    assert (out["TXN_ID_SEQ"] == 0).all()
