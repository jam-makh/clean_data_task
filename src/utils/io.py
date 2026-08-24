"""Workbook reading and writing."""

from pathlib import Path

import pandas as pd

# Fallbacks only. The real formats come from policy.yaml, which is where the
# choice of day-first belongs: it is a judgement about the audience, not a
# fact about the source, and a reader in another region would set it
# differently. These constants keep write_workbook usable when a caller has no
# policy to hand -- a notebook, a test -- without silently disagreeing with a
# configured run, because they are the same values the shipped policy states.
DATETIME_DISPLAY = "%d-%m-%Y %H:%M:%S"
DATE_DISPLAY = "%d-%m-%Y"
MINUTE_DISPLAY = "%d-%m-%Y %H:%M"

# Columns rendered as dates only; everything else datetime keeps its time part.
# Matched case-insensitively: a sheet goes out under lowercase names, while a
# frame handed straight to write_workbook still carries the pipeline's own.
DATE_ONLY = {"settle_date_cleaned"}

# Columns whose absence is written as a word instead of an empty cell. A blank
# reads as "nothing to say here" as easily as "we do not know", and in both of
# these the difference is the whole point: an unsettled transaction is a fact
# about it rather than a gap, and a third of this source's running balances
# were simply never supplied.
#
# Applied here and nowhere earlier, for the reason the module docstring gives
# for the formats: the frame holds a true null until the last moment, so
# nothing that sorts, filters or does arithmetic ever meets the string. Each
# has a machine-readable counterpart carrying the same statement --
# SETTLE_DATE_STATUS and RUNNING_BALANCE_STATUS.
#
# RUNNING_BALANCE_CLEANED is in the set too, and the objection to putting it
# there is answered rather than ignored: its blanks are three different
# answers -- withheld-as-wrong, withheld-as-unverifiable, and
# nothing-to-count-from -- and one word cannot say which. What the word does
# say is the thing a reader of that column actually needs first, that no
# balance is being asserted here, and it says it in the cell rather than by
# the absence of one. Which of the three it was is in
# RUNNING_BALANCE_STATUS, immediately to its right, on every one of those
# rows; the word points at that column instead of standing in for it.
UNKNOWN_TEXT = "UNKNOWN"
UNKNOWN_WHEN_MISSING = {
    "settle_date_cleaned", "running_balance", "running_balance_cleaned",
    "running_balance_adjusted",
}

# A timestamp is rendered to the precision it was actually observed at, so the
# output cannot state a time the source never gave. Writing every row as
# %d-%m-%Y %H:%M:%S would put 00:00:00 on the date-only rows and undo the
# whole point of tracking precision. The companion column names the precision;
# the value rendered beside it must agree with it.
#
# Two entries because the two profiles name their timestamp differently:
# DateNormalizer produces TXN_DATE_TIME_CLEANED for the v4 workbook and
# TimestampNormalizer produces TXN_TS for the forecast extract.
PRECISION_COMPANION = {
    "txn_date_time_cleaned": "txn_date_time_precision",
    "txn_ts": "txn_ts_precision",
}

# The companion columns themselves, which are read to render and then dropped:
# the sheet states one timestamp per row, and the precision is visible in how
# that timestamp is written. `columns.RENDER_ONLY` is the same list under the
# names the pipeline uses; these are the names a sheet carries.
RENDER_ONLY = set(PRECISION_COMPANION.values())

# Extensions this reader understands. A source outside this set is rejected by
# name rather than guessed at: reading a .txt as a CSV usually "works" and
# produces one column of garbage, which is worse than refusing.
WORKBOOK_SUFFIXES = {".xlsx", ".xlsm", ".xls"}
DELIMITED_SUFFIXES = {".csv": ",", ".tsv": "\t", ".txt": ","}


def read_source(path: str | Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Reads a source of any supported kind into the same shape.

    The pipeline is meant to run against whatever file it is given, so the
    format is a property of the path rather than a decision the caller has to
    encode. A workbook may carry an MCC reference sheet; a delimited file
    cannot, and returns an empty one.

    :param path: Source .xlsx/.xlsm/.xls, or .csv/.tsv/.txt.
    :returns: (transactions frame, MCC code to category).
    :raises ValueError: If the extension is not one this reader handles.
    """
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in WORKBOOK_SUFFIXES:
        return read_workbook(path)
    if suffix in DELIMITED_SUFFIXES:
        # Same dtype and NA discipline as the workbook reader, for the same
        # reason: the pipeline decides what "" and "NA" mean, not the reader.
        frame = pd.read_csv(
            path,
            sep=DELIMITED_SUFFIXES[suffix],
            dtype=object,
            keep_default_na=False,
            na_values=[""],
        )
        return frame, {}
    raise ValueError(
        f"Unsupported source type {suffix!r}: {path}. "
        f"Supported: {sorted(WORKBOOK_SUFFIXES | set(DELIMITED_SUFFIXES))}"
    )


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


def render_dates(frame: pd.DataFrame, formats: dict | None = None):
    """
    Renders every datetime column as text, at the precision it was observed.

    Columns named in ``UNKNOWN_WHEN_MISSING`` spell their nulls out, whether
    or not they hold dates; every other column leaves a null blank.

    :param frame: Sheet-ready frame.
    :param formats: Overrides for ``datetime``, ``date`` and ``minute``;
        the module fallbacks are used for anything absent.
    :returns: A copy with datetime columns as strings.
    """
    formats = formats or {}
    full = formats.get("datetime", DATETIME_DISPLAY)
    day = formats.get("date", DATE_DISPLAY)
    minute = formats.get("minute", MINUTE_DISPLAY)
    by_precision = {"SECOND": full, "MINUTE": minute, "DAY": day}

    out = frame.copy()
    lower = {str(c).lower(): c for c in out.columns}
    for column in out.columns:
        if not pd.api.types.is_datetime64_any_dtype(out[column]):
            continue
        key = str(column).lower()
        companion = lower.get(PRECISION_COMPANION.get(key, ""))
        if companion is not None:
            precision = out[companion].astype(str)
            rendered = pd.Series("", index=out.index, dtype=object)
            for level, fmt in by_precision.items():
                hit = precision == level
                if hit.any():
                    rendered[hit] = out.loc[hit, column].dt.strftime(fmt)
            # A precision this writer does not know about still has to render.
            unseen = ~precision.isin(by_precision)
            if unseen.any():
                rendered[unseen] = out.loc[unseen, column].dt.strftime(full)
            out[column] = rendered.where(out[column].notna())
        else:
            out[column] = out[column].dt.strftime(
                day if key in DATE_ONLY else full
            )

    # After the date rendering, so a spelled-out null lands on the string a
    # date column has become as readily as on one that was never a date.
    #
    # Cast to object first: a nullable numeric column -- which is what a
    # withheld balance leaves behind -- refuses a string as its fill value,
    # and the word is a rendering decision, so it is the rendered copy that
    # stops being a number, never the frame the pipeline still holds.
    for column in out.columns:
        if str(column).lower() not in UNKNOWN_WHEN_MISSING:
            continue
        missing = out[column].isna()
        if missing.any():
            out[column] = out[column].astype(object).where(
                ~missing, UNKNOWN_TEXT
            )

    # The precision companion has now done its job. It is an input to this
    # function, not a column of the sheet: it decided whether each timestamp
    # above was written with a time of day, and the rendered value says which
    # it was. Dropped last, so every branch above could still read it.
    spent = [c for c in out.columns if str(c).lower() in RENDER_ONLY]
    return out.drop(columns=spent)


def write_workbook(
    path: str | Path,
    sheets: dict[str, pd.DataFrame],
    formats: dict | None = None,
) -> None:
    """
    Writes one sheet per table, formatting datetimes at the last moment.

    Dates live as ``datetime64`` inside the pipeline so they stay sortable;
    the display format is applied only here, on the way out.

    :param path: Destination .xlsx.
    :param sheets: Sheet name to frame.
    :param formats: Display formats from policy; module fallbacks when absent.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            formatted = render_dates(frame, formats)
            formatted.to_excel(writer, sheet_name=name[:31], index=False)


def write_output(
    path: str | Path,
    sheets: dict[str, pd.DataFrame],
    formats: dict | None = None,
) -> Path:
    """
    Writes the sheets in the format the destination name asks for.

    The multi-sheet workbook is a presentation format and it does not scale:
    openpyxl takes about 500 seconds to write 265195 rows by 57 columns, where
    the cleaning that produced them takes 120. Naming a .csv destination
    writes the same sheets as separate files in about 35 seconds instead. The
    choice is the caller's, made by the extension they ask for, because which
    one is right depends on whether a person or a loader is going to open it.

    :param path: Destination. ``.xlsx`` writes one workbook; ``.csv``/``.tsv``
        writes one file per sheet, named ``<stem>__<sheet><suffix>``.
    :param sheets: Sheet name to frame.
    :param formats: Display formats from policy; module fallbacks when absent.
    :returns: The path written, or the directory holding the parts.
    :raises ValueError: If the extension is not one this writer handles.
    """
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in WORKBOOK_SUFFIXES:
        write_workbook(path, sheets, formats)
        return path
    if suffix in DELIMITED_SUFFIXES:
        path.parent.mkdir(parents=True, exist_ok=True)
        separator = DELIMITED_SUFFIXES[suffix]
        for name, frame in sheets.items():
            part = path.with_name(f"{path.stem}__{name}{path.suffix}")
            render_dates(frame, formats).to_csv(
                part, sep=separator, index=False
            )
        return path.parent
    raise ValueError(
        f"Unsupported output type {suffix!r}: {path}. "
        f"Supported: {sorted(WORKBOOK_SUFFIXES | set(DELIMITED_SUFFIXES))}"
    )
