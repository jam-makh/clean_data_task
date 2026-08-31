"""
Shared fixtures.

pandas appears here and in ``tests/harness/`` and nowhere else. The pipeline
is Spark end to end; these are the tests' own tools for building a fixture and
reading a result back, which is a different job from computing one.
"""

from pathlib import Path

import pandas as pd
import pytest

SOURCE = (
    Path(__file__).resolve().parents[1]
    / "data/raw/synthetic_dirty_transactions_v4.xlsx"
)
FORECAST = (
    Path(__file__).resolve().parents[1] / "data/raw/forecast_balance_data.csv"
)


@pytest.fixture(scope="session")
def transactions():
    """
    :returns: The v4 workbook's transactions sheet, as text.

    Read with pandas directly. The pipeline's own reader was part of the
    pandas half and went with it; what these tests need from the file is its
    columns, and openpyxl behind ``read_excel`` gets them.
    """
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")
    return pd.read_excel(SOURCE, sheet_name=0, dtype=str)


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

    from src.spark import spark_setup as session_module

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
        first use and cached after. See ``tests.harness.sample`` for what "cut"
        means here -- it is chosen, not taken.
    """
    from tests.harness import sample as sample_module

    try:
        return sample_module.ensure(source=FORECAST)
    except FileNotFoundError as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope="session")
def sample_frame(sample_path):
    """
    :returns: The parity sample, read the same way ``tests.harness.sample``
        writes it -- text throughout, empty string distinguished from null --
        so a test comparing the two is comparing content and not a reader's
        type inference.
    """
    return pd.read_csv(
        sample_path, dtype=str, keep_default_na=False, na_values=[""]
    )
