"""End-to-end behaviour and the output contract."""

import pandas as pd

from main import build_sheets, clean_transactions
from src.pipeline import TransactionCleaner
from src.utils.columns import (
    INTERNAL,
    RENAMED,
    SUPERSEDED,
    output_names,
    presented,
)
from src.utils.io import write_workbook

ADDED_COLUMNS = [
    "TXN_ID_CLEANED", "TXN_DATE_TIME_CLEANED", "SETTLE_DATE_CLEANED",
    "SETTLE_DATE_STATUS",
    "TXN_AMOUNT_CLEANED", "PROCESSING_CODE_CLEANED", "PROCESSING_TYPE_CLEANED",
    "MCC_CODE_CLEANED", "MCC_CATEGORY", "MCC_CODE_SUGGESTED", "MCC_CONFIDENCE",
    "MERCHANT_NAME_CLEANED", "MERCHANT_PROCESSOR", "MERCHANT_CITY_CLEANED",
    "MERCHANT_COUNTRY_CLEANED",
    "HAS_TERMINAL", "AUTH_CODE_VALID", "IS_ECOMMERCE", "VALIDATION_FLAGS",
]


def test_output_contract(transactions, mcc_reference):
    cleaned, _ = clean_transactions(transactions, mcc_reference=mcc_reference)
    for column in ADDED_COLUMNS:
        assert column in cleaned.columns, column


def test_no_rows_are_lost(transactions, mcc_reference):
    """Rows are only ever mirrored into review sheets, never removed."""
    cleaned, report = clean_transactions(
        transactions, mcc_reference=mcc_reference
    )
    assert len(cleaned) == len(transactions)


def test_originals_are_untouched(transactions, mcc_reference):
    cleaned, _ = clean_transactions(transactions, mcc_reference=mcc_reference)
    for column in transactions.columns:
        assert cleaned[column].equals(transactions[column]), column


def test_steps_are_injectable(transactions):
    """Running one cleaner alone must work, for development and testing."""
    from src.cleaners import DateNormalizer

    cleaned, _ = clean_transactions(transactions, steps=[DateNormalizer])
    assert "TXN_DATE_TIME_CLEANED" in cleaned.columns
    assert "MERCHANT_NAME_CLEANED" not in cleaned.columns


def test_report_records_every_step(transactions, mcc_reference):
    _, report = clean_transactions(transactions, mcc_reference=mcc_reference)
    steps = {step for step, _, _ in report.entries}
    assert {"dates", "amounts", "merchant", "mcc", "consistency"} <= steps


def test_a_stage_marks_rows_and_counts_nothing(transactions, mcc_reference):
    """
    The contract the Spark port rests on.

    A stage that counted while it ran had to hold the total somewhere outside
    the rows -- a closure, or an accumulating `.sum()`. Distributed, that
    total is filled in on each executor and read back empty on the driver,
    with no error and a report full of plausible zeros. So `apply` is allowed
    to write columns and nothing else; every number comes from `metrics`,
    afterwards, out of those columns.
    """
    from src.cleaners import CodeNormalizer
    from src.config.policy import load as load_policy
    from src.pipeline import DEFAULT_STEPS
    from src.utils.report import CleaningReport

    report = CleaningReport()
    policy = load_policy()
    frame = transactions.copy()
    ran = []

    for step_class in DEFAULT_STEPS:
        step = step_class(report, policy=policy)
        ran.append(step)
        if isinstance(step, CodeNormalizer):
            frame = step.apply(frame, mcc_reference=mcc_reference)
        else:
            frame = step.apply(frame)
        assert report.entries == [], f"{step.name} counted during apply"

    for step in ran:
        step.collect(frame)
    assert report.entries, "no step could report from the finished frame"


def test_no_stage_can_write_to_the_report_while_it_runs():
    """
    The old escape hatch is gone, not merely unused.

    `log()` is what let a stage reduce mid-run. Leaving it in place would mean
    the next stage anyone adds can quietly reintroduce the bug this phase
    removed, and it would keep passing every test until it reached a cluster.
    """
    from src.cleaners.base import BaseCleaner

    assert not hasattr(BaseCleaner, "log")


def test_workbook_has_every_sheet(tmp_path, transactions, mcc_reference):
    cleaner = TransactionCleaner(mcc_reference=mcc_reference)
    cleaned = cleaner.run(transactions)
    sheets = build_sheets(transactions, cleaned, cleaner, mcc_reference)
    destination = tmp_path / "out.xlsx"
    write_workbook(destination, sheets)

    written = pd.ExcelFile(destination)
    assert set(written.sheet_names) >= {
        "raw_transactions", "cleaned_transactions", "mcc_codes",
        "pending_settlement", "anomaly_settlement", "mcc_review",
    }
    assert len(written.parse("cleaned_transactions")) == len(
        written.parse("raw_transactions")
    )


def test_cleaned_sheets_carry_no_superseded_raw_columns(
    tmp_path, transactions, mcc_reference
):
    """
    A text TXN_AMOUNT beside a float one invites a total from the wrong
    column.
    """
    cleaner = TransactionCleaner(mcc_reference=mcc_reference)
    cleaned = cleaner.run(transactions)
    sheets = build_sheets(transactions, cleaned, cleaner, mcc_reference)
    destination = tmp_path / "out.xlsx"
    write_workbook(destination, sheets)

    written = pd.ExcelFile(destination)

    def out(names):
        """:returns: Those names as the writer spells them on a sheet."""
        return set(output_names(pd.DataFrame(columns=list(names))).columns)

    for sheet in (
        "cleaned_transactions", "pending_settlement", "anomaly_settlement",
    ):
        columns = set(written.parse(sheet).columns)
        # MATCHES_STATUS is excluded: its cleaned column goes out under the
        # raw name, so there the two are the same name carrying the cleaned
        # value, not a raw column that survived.
        assert not columns & (out(SUPERSEDED) - out(RENAMED.values())), sheet
        # Every superseded raw column has its replacement on the sheet, except
        # where the replacement is itself a working column feeding another.
        #
        # Only the pairs this source actually has: SUPERSEDED covers every
        # profile at once, and the macro trio belongs to a file this fixture
        # is not. A pair whose raw column was never read cannot have lost
        # anything, which is the only thing this assertion protects.
        #
        # A raw column may name more than one replacement, because the two
        # profiles spell the cleaned timestamp differently; the one that ran
        # is the one that has to be present.
        replacements = set()
        for raw, clean in SUPERSEDED.items():
            if raw not in transactions.columns:
                continue
            names = {clean} if isinstance(clean, str) else set(clean)
            present = names & set(cleaned.columns)
            replacements |= present or names
        assert out(replacements - set(INTERNAL)) <= columns, sheet
        assert not columns & out(INTERNAL), sheet

    # The raw values are one sheet away, not gone.
    assert set(written.parse("raw_transactions").columns) == out(
        transactions.columns
    )


def test_presented_keeps_a_raw_column_with_no_replacement(
    transactions,
    mcc_reference,
):
    """A partial pipeline must never drop the only copy of a field."""
    cleaner = TransactionCleaner(steps=[], mcc_reference=mcc_reference)
    view = presented(cleaner.run(transactions))
    assert set(view.columns) == set(transactions.columns)


def test_dates_are_written_in_display_format(
    tmp_path,
    transactions,
    mcc_reference,
):
    """dd-mm-yyyy is applied on write; the frame holds real datetimes."""
    cleaner = TransactionCleaner(mcc_reference=mcc_reference)
    cleaned = cleaner.run(transactions)
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned["TXN_DATE_TIME_CLEANED"]
    )

    destination = tmp_path / "out.xlsx"
    write_workbook(destination, {"cleaned_transactions": cleaned})
    written = pd.ExcelFile(destination).parse(
        "cleaned_transactions", dtype=object
    )
    stamp = str(written["TXN_DATE_TIME_CLEANED"].iat[0])
    assert stamp == "10-03-2022 00:51:36"
