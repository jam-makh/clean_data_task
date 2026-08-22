"""Shared fixtures."""

from pathlib import Path

import pandas as pd
import pytest

from src.utils.io import read_workbook

SOURCE = (
    Path(__file__).resolve().parents[1]
    / "data/raw/synthetic_dirty_transactions_v4.xlsx"
)
FORECAST = (
    Path(__file__).resolve().parents[1] / "data/raw/forecast_balance_data.csv"
)


@pytest.fixture(scope="session")
def workbook():
    """
    :returns: (transactions frame, MCC reference) from the real source file.
    """
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")
    return read_workbook(SOURCE)


@pytest.fixture(scope="session")
def transactions(workbook):
    """:returns: The raw transactions frame."""
    return workbook[0]


@pytest.fixture(scope="session")
def mcc_reference(workbook):
    """:returns: MCC code to category."""
    return workbook[1]


@pytest.fixture(scope="session")
def forecast():
    """
    :returns: The forecast-balance source, read as text so the cleaners see
        the same dirt a real load would. Session-scoped: it is 265k rows and
        every step under test wants all of them.
    """
    if not FORECAST.exists():
        pytest.skip(f"source file not present: {FORECAST}")
    return pd.read_csv(
        FORECAST, dtype=str, keep_default_na=False, na_values=[""]
    )


@pytest.fixture
def report():
    """:returns: A fresh report for a single cleaner under test."""
    from src.utils.report import CleaningReport

    return CleaningReport()


@pytest.fixture
def tiny_frame():
    """:returns: A 3-row frame with the columns most cleaners expect."""
    return pd.DataFrame(
        {
            "TXN_ID": [1, 2, 3],
            "ACCOUNT_ID": [10, 10, 11],
            "TXN_DATE_TIME": [
                "2022-03-10 00:51:36", "09/07/2022 17:49",
                "04-22-2022 22:52",
            ],
            "SETTLE_DATE": ["2022-03-13", "0000-00-00", "23-Apr-22"],
            "TXN_AMOUNT": ["-104.39", "(808.41)", "5.727.580,00"],
            "TXN_CCY": ["USD", "USD", "LBP"],
            "MERCHANT_NAME": [
                "SQ *TAKEALOT", "COURSERA.COM *W2PA", "DUBAI MN /ufw",
            ],
            "MCC_CODE": [5411, 8220, 4111],
            "MERCHANT_CITY": ["BEYROUTH", "ECOM", ""],
            "TERMINAL_ID": ["ABC12345", "00000000", "00000000"],
            "AUTH_CODE": ["A1B2C3", "000000", "XYZ999"],
            "PROCESSING_CODE": [0, 20, 1],
            "PROCESSING_TYPE": [
                "Purchase", "Purchase Return/Refund", "ATM Cash Withdrawal",
            ],
            "BILLING_AMOUNT": [-104.39, 808.41, -50.0],
        }
    )
