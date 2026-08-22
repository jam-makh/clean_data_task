"""Public entry point: one function that cleans a transactions dataset."""

from pathlib import Path

import pandas as pd

from src.config import runtime
from src.config.fingerprint import short_fingerprint
from src.pipeline import TransactionCleaner
from src.rules import loader
from src.utils.columns import output_names, presented
from src.utils.io import read_workbook, write_workbook
from src.utils.report import CleaningReport

# Sentinel rather than a literal default: resolving the configured path at
# import time would make the module unimportable when the config file is
# absent, which is exactly the caller who passes a frame instead of a path.
_FROM_CONFIG = object()


def clean_transactions(
    source: str | Path | pd.DataFrame = _FROM_CONFIG,
    *,
    steps: list[type] | None = None,
    output_path: str | Path | None = None,
    mcc_reference: dict | None = None,
) -> tuple[pd.DataFrame, CleaningReport]:
    """
    Cleans a transactions dataset end to end.

    Accepts a path or an in-memory frame so the same call works against the
    workbook now and a database extract later without a rewrite.

    :param source: Path to an .xlsx, or a DataFrame already in memory;
        defaults to the source in ``config/pipeline.yaml``.
    :param steps: Cleaner classes to run; defaults to the full pipeline.
    :param output_path: If given, writes the multi-sheet workbook there.
    :param mcc_reference: MCC lookup; read from the workbook when source is
        a path.
    :returns: (cleaned frame, report).
    """
    if source is _FROM_CONFIG:
        source = runtime.load().paths.source

    if isinstance(source, pd.DataFrame):
        raw, reference = source, (mcc_reference or {})
    else:
        raw, reference = read_workbook(source)
        if mcc_reference:
            reference = mcc_reference

    cleaner = TransactionCleaner(steps=steps, mcc_reference=reference)
    cleaned = cleaner.run(raw)

    if output_path:
        write_workbook(
            output_path, build_sheets(raw, cleaned, cleaner, reference)
        )

    return cleaned, cleaner.report


def build_sheets(
    raw: pd.DataFrame,
    cleaned: pd.DataFrame,
    cleaner: TransactionCleaner,
    reference: dict,
) -> dict[str, pd.DataFrame]:
    """
    Assembles the workbook as a small database, one sheet per table.

    The settlement sheets mirror rows rather than moving them: every source
    row stays in ``cleaned_transactions``, because removing real transactions
    to isolate a date problem would corrupt every total computed from it.

    Every sheet built from ``cleaned`` shows the cleaned columns only. Holding
    ``TXN_AMOUNT`` (text) beside ``TXN_AMOUNT_CLEANED`` (float) invites a total
    computed from the wrong one; the raw values stay one sheet away, keyed by
    ``TXN_ID``.

    Every sheet is passed through ``output_names`` on the way out, so the
    workbook spells a column one way regardless of which step produced it.

    :returns: Sheet name to frame.
    """
    view = presented(cleaned)
    sheets = {"raw_transactions": raw, "cleaned_transactions": view}

    # Both code columns on the transaction sheet are keys into a lookup that
    # lives once, here, rather than as a label repeated down 2,296 rows.
    sheets["processing_codes"] = pd.DataFrame(
        sorted(loader.processing_codes().items()),
        columns=["PROCESSING_CODE_CLEANED", "PROCESSING_TYPE"],
    )

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
    return {name: output_names(frame) for name, frame in sheets.items()}


def main() -> None:
    """Runs the pipeline against the configured paths and prints the report."""
    paths = runtime.load().paths
    cleaned, report = clean_transactions(
        paths.source, output_path=paths.output
    )
    print(f"Cleaned {len(cleaned)} rows -> {paths.output}")
    print(f"Config fingerprint: {short_fingerprint()}\n")
    print(report)


if __name__ == "__main__":
    main()
