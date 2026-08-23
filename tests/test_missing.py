"""Sentinel flagging and the auth-code frequency rule."""

import pandas as pd

from src.cleaners.dates import DateNormalizer
from src.cleaners.missing import MissingValueHandler


def test_terminal_sentinel_is_flagged_not_erased(report):
    df = MissingValueHandler(report).apply(
        pd.DataFrame({"TERMINAL_ID": ["ABC12345", "00000000"]})
    )
    assert list(df["HAS_TERMINAL"]) == [True, False]
    assert df["TERMINAL_ID"].iat[1] == "00000000"  # value survives


def test_auth_code_repeats_are_invalid(report):
    """
    Genuine 6-char codes collide with probability ~0.001, so any repeat is
    planted.
    """
    df = MissingValueHandler(report).apply(
        pd.DataFrame({"AUTH_CODE": ["ZZ0011", "ZZ0011", "UNIQ01", "000000"]})
    )
    assert list(df["AUTH_CODE_VALID"]) == [False, False, True, False]


def test_settle_status_uses_a_column_not_a_placeholder_string(report):
    """
    The sheet writes UNKNOWN into settle_date_cleaned, but only on the way
    out. Inside the pipeline the column stays datetime64 with a true null:
    a literal string here would force it to text and break every sort and
    date calculation done after this step.
    """
    frame = pd.DataFrame(
        {
            "TXN_DATE_TIME": [
                "2022-03-10 00:00:00", "2022-03-10 00:00:00",
                "2022-03-10 00:00:00",
            ],
            "SETTLE_DATE": ["2022-03-12", "0000-00-00", "2022-03-09"],
        }
    )
    dated = DateNormalizer(report).apply(frame)
    df = MissingValueHandler(report).apply(dated)

    assert list(df["SETTLE_DATE_STATUS"]) == [
        "OBSERVED", "MISSING", "ANOMALOUS",
    ]
    assert pd.isna(df["SETTLE_DATE_CLEANED"].iat[1])
    assert pd.api.types.is_datetime64_any_dtype(df["SETTLE_DATE_CLEANED"])


def test_real_file_counts(transactions, report):
    df = MissingValueHandler(report).apply(
        DateNormalizer(report).apply(transactions)
    )
    status = df["SETTLE_DATE_STATUS"].astype(str)
    assert (status == "MISSING").sum() == 14
    assert (status == "ANOMALOUS").sum() == 6
    assert (~df["HAS_TERMINAL"]).sum() == 1051
