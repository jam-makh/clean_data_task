"""Workbook reading and writing."""

from pathlib import Path

import pandas as pd

DATETIME_DISPLAY = "%d-%m-%Y %H:%M:%S"
DATE_DISPLAY = "%d-%m-%Y"

# Columns rendered as dates only; everything else datetime keeps its time part.
# Matched case-insensitively: a sheet goes out under lowercase names, while a
# frame handed straight to write_workbook still carries the pipeline's own.
DATE_ONLY = {"settle_date_cleaned"}


def read_workbook(path: str | Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Reads the transactions sheet and the MCC reference.

    ``dtype=object`` and ``keep_default_na=False`` are deliberate: pandas would
    otherwise coerce ``"NA"`` and ``""`` to NaN on read, merging the null and
    placeholder categories before the pipeline can tell them apart.

    :param path: Source .xlsx.
    :returns: (transactions frame, MCC code to category).
    """
    book = pd.ExcelFile(path)
    txn_sheet = (
        "Transactions"
        if "Transactions" in book.sheet_names
        else book.sheet_names[0]
    )
    transactions = book.parse(txn_sheet, dtype=object, keep_default_na=False)

    reference: dict[str, str] = {}
    for sheet in book.sheet_names:
        if "MCC" in sheet.upper():
            ref = book.parse(sheet, dtype=object, keep_default_na=False)
            reference = {
                str(r.MCC_CODE).strip().zfill(4): str(r.CATEGORY).strip()
                for r in ref.itertuples()
            }
            break
    return transactions, reference


def write_workbook(path: str | Path, sheets: dict[str, pd.DataFrame]) -> None:
    """
    Writes one sheet per table, formatting datetimes at the last moment.

    Dates live as ``datetime64`` inside the pipeline so they stay sortable;
    the display format is applied only here, on the way out.

    :param path: Destination .xlsx.
    :param sheets: Sheet name to frame.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            formatted = frame.copy()
            for column in formatted.columns:
                if pd.api.types.is_datetime64_any_dtype(formatted[column]):
                    fmt = (
                        DATE_DISPLAY
                        if str(column).lower() in DATE_ONLY
                        else DATETIME_DISPLAY
                    )
                    formatted[column] = formatted[column].dt.strftime(fmt)
            formatted.to_excel(writer, sheet_name=name[:31], index=False)
