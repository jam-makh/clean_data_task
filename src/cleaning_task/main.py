"""Public entry point: one function that cleans a transactions dataset."""

from pathlib import Path

import pandas as pd

from cleaning_task.pipeline import TransactionCleaner
from cleaning_task.utils.columns import presented
from cleaning_task.utils.io import read_workbook, write_workbook
from cleaning_task.utils.report import CleaningReport

DEFAULT_SOURCE = Path("data/raw/synthetic_dirty_transactions_v4.xlsx")
DEFAULT_OUTPUT = Path("data/output/cleaned_transactions.xlsx")


def clean_transactions(
    source: str | Path | pd.DataFrame = DEFAULT_SOURCE,
    *,
    steps: list[type] | None = None,
    output_path: str | Path | None = None,
    mcc_reference: dict | None = None,
) -> tuple[pd.DataFrame, CleaningReport]:
    """
    Cleans a transactions dataset end to end.

    Accepts a path or an in-memory frame so the same call works against the
    workbook now and a database extract later without a rewrite.

    :param source: Path to an .xlsx, or a DataFrame already in memory.
    :param steps: Cleaner classes to run; defaults to the full pipeline.
    :param output_path: If given, writes the multi-sheet workbook there.
    :param mcc_reference: MCC lookup; read from the workbook when source is a path.
    :returns: (cleaned frame, report).
    """
    if isinstance(source, pd.DataFrame):
        raw, reference = source, (mcc_reference or {})
    else:
        raw, reference = read_workbook(source)
        if mcc_reference:
            reference = mcc_reference

    cleaner = TransactionCleaner(steps=steps, mcc_reference=reference)
    cleaned = cleaner.run(raw)

    if output_path:
        write_workbook(output_path, build_sheets(raw, cleaned, cleaner, reference))

    return cleaned, cleaner.report


def build_sheets(
    raw: pd.DataFrame,
    cleaned: pd.DataFrame,
    cleaner: TransactionCleaner,
    reference: dict,
) -> dict[str, pd.DataFrame]:
    """
    Assembles the workbook as a small database, one sheet per table.

    The settlement sheets **mirror** rows rather than moving them: every source
    row stays in ``cleaned_transactions``, because removing real transactions
    to isolate a date problem would corrupt every total computed from it.

    Every sheet built from ``cleaned`` shows the cleaned columns only. Holding
    ``TXN_AMOUNT`` (text) beside ``TXN_AMOUNT_CLEAN`` (float) invites a total
    computed from the wrong one; the raw values stay one sheet away, keyed by
    ``TXN_ID``.

    :returns: Sheet name to frame.
    """
    view = presented(cleaned)
    sheets = {"raw_transactions": raw, "cleaned_transactions": view}

    if reference:
        sheets["mcc_codes"] = pd.DataFrame(
            sorted(reference.items()), columns=["MCC_CODE", "CATEGORY"]
        )

    if "SETTLE_DATE_STATUS" in view.columns:
        status = view["SETTLE_DATE_STATUS"].astype(str)
        sheets["pending_settlement"] = view[status == "UNKNOWN"]
        sheets["anomaly_settlement"] = view[status == "ANOMALOUS"]

    merchant_step = cleaner.step("merchant")
    if merchant_step is not None:
        sheets["merchant_review"] = merchant_step.review_queue()

    mcc_step = cleaner.step("mcc")
    if mcc_step is not None:
        sheets["mcc_review"] = mcc_step.review_queue()

    sheets["cleaning_report"] = cleaner.report.to_frame()
    return sheets


def main() -> None:
    """Runs the pipeline against the default paths and prints the report."""
    cleaned, report = clean_transactions(output_path=DEFAULT_OUTPUT)
    print(f"Cleaned {len(cleaned)} rows -> {DEFAULT_OUTPUT}\n")
    print(report)


if __name__ == "__main__":
    main()
