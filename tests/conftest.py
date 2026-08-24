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


# --- Stage 2: Spark ------------------------------------------------------
# Session-scoped throughout. A Spark session costs several seconds of JVM
# startup and only one can exist per process, so a fixture that built one per
# test would both fail and be slow about it.


@pytest.fixture(scope="session")
def spark():
    """
    :returns: The project's Spark session, configured exactly as a real run
        configures it -- which is the point of there being a factory.

    Skips rather than fails when the JVM side is not there. A machine without
    Java is a machine that cannot run these tests, and that is a setup
    condition with its own diagnostic; failing the suite would report it as a
    code defect nine times out of ten.
    """
    pytest.importorskip("pyspark")

    from src.spark import session as session_module

    try:
        active = session_module.session("parity-tests")
    except Exception as exc:  # noqa: BLE001 - the message is the whole point
        pytest.skip(
            f"could not start Spark ({type(exc).__name__}: {exc}). "
            f"Run `python -m scripts.verify_env` -- it names the cause."
        )
    yield active
    session_module.stop()


@pytest.fixture(scope="session")
def sample_path():
    """
    :returns: Path to the parity sample, cut from the forecast extract on
        first use and cached after. See ``src.spark.sample`` for what "cut"
        means here -- it is chosen, not taken.
    """
    from src.spark import sample as sample_module

    try:
        return sample_module.ensure(source=FORECAST)
    except FileNotFoundError as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope="session")
def sample_frame(sample_path):
    """
    :returns: The sample read by the pandas reader the pipeline itself uses.
        Read through ``read_source`` rather than ``pd.read_csv`` on purpose:
        the parity claim is about the two pipelines, and a test that read the
        pandas side its own way would be comparing Spark against something
        the pipeline never sees.
    """
    from src.utils.io import read_source

    return read_source(sample_path)[0]
