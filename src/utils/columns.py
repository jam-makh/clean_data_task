"""Which columns the cleaned sheets present, and in what order."""

import pandas as pd

# A raw column is dropped from the presented view only when a cleaned column
# fully supersedes it. Everything else -- keys, currencies, statuses -- has no
# cleaned counterpart and stays as the value it always was.
#
# Nothing is lost: `raw_transactions` carries all 19 source columns byte for
# byte, joinable on TXN_ID, so "what did this look like before" stays
# answerable without leaving the workbook.
SUPERSEDED = {
    "TXN_ID": "TXN_ID_CLEANED",
    "TXN_DATE_TIME": "TXN_DATE_TIME_CLEANED",
    "SETTLE_DATE": "SETTLE_DATE_CLEANED",
    "TXN_AMOUNT": "TXN_AMOUNT_CLEANED",
    "MERCHANT_NAME": "MERCHANT_NAME_CLEANED",
    "MERCHANT_CITY": "MERCHANT_CITY_CLEANED",
    "MERCHANT_COUNTRY": "MERCHANT_COUNTRY_CLEANED",
    "MCC_CODE": "MCC_CODE_CLEANED",
    "MATCHES_STATUS": "MATCHES_STATUS_CLEANED",
    "PROCESSING_CODE": "PROCESSING_CODE_CLEANED",
    "PROCESSING_TYPE": "PROCESSING_TYPE_CLEANED",
}

# Working columns: each one is needed to compute something, and each one would
# be a second version of a fact already on the sheet if it were shown.
#
# PROCESSING_TYPE_CLEANED is the processing_codes sheet, repeated once per row,
# and the code it was looked up by is on the transaction. MCC_CATEGORY is the
# mcc_codes sheet, the same way. MCC_CODE_SUGGESTED has been folded into
# MCC_CODE_CLEANED, which now carries the code that survived validation.
# MERCHANT_COUNTRY_EXPECTED is what MERCHANT_COUNTRY_CLEANED was resolved from.
# MERCHANT_RECOGNISED is the boolean MATCHES_STATUS_CLEANED spells out.
#
# VALIDATION_FLAGS recorded every cross-field contradiction found. Each one is
# now either corrected in place or shown in its own column, so the flag string
# would only restate a value already on the row: a wrong country is corrected
# in MERCHANT_COUNTRY_CLEANED, a wrong ATM code in MCC_CODE_CLEANED, a settle
# date before its transaction in SETTLE_DATE_STATUS and the anomaly_settlement
# sheet. The counts stay in cleaning_report, which is where the question "how
# much was wrong" belongs -- a per-row column answers "is this row wrong",
# which the corrected columns already answer.
INTERNAL = [
    "PROCESSING_TYPE_CLEANED",
    "MCC_CATEGORY",
    "MCC_CODE_SUGGESTED",
    "MERCHANT_COUNTRY_EXPECTED",
    "MERCHANT_RECOGNISED",
    "VALIDATION_FLAGS",
]

# Identity, then money, then merchant, then codes, then flags. Any column not
# named here keeps its position at the end, so a new derived column shows up
# rather than silently vanishing.
PRESENTATION_ORDER = [
    "TXN_ID_CLEANED",
    "ACCOUNT_ID",
    "TXN_DATE_TIME_CLEANED",
    "SETTLE_DATE_CLEANED",
    "SETTLE_DATE_STATUS",
    "TXN_TYPE",
    "TXN_AMOUNT_CLEANED",
    "TXN_CCY",
    "BILLING_AMOUNT",
    "BILLING_CURRENCY",
    "FX_RATE",
    # Confidence sits beside the merchant name rather than with the other MCC
    # columns: it is the first thing a reader needs after knowing who the
    # merchant is, and the code itself is only meaningful once it is trusted.
    "MERCHANT_NAME_CLEANED",
    "MCC_CONFIDENCE",
    "MERCHANT_PROCESSOR",
    "MERCHANT_CITY_CLEANED",
    "MERCHANT_COUNTRY_CLEANED",
    "PROCESSING_CODE_CLEANED",
    "MCC_CODE_CLEANED",
    "TERMINAL_ID",
    "HAS_TERMINAL",
    "AUTH_CODE",
    "AUTH_CODE_VALID",
    "IS_ECOMMERCE",
    "MATCHES_STATUS_CLEANED",
]


def presented(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops raw columns that a cleaned column supersedes and the working columns
    that only fed them, then orders the rest.

    A superseded raw column is only dropped when its replacement is actually
    present, so running a partial pipeline never silently loses the only copy
    of a field.

    :param df: Cleaned frame, raw and derived columns side by side.
    :returns: The view written to the cleaned sheets.
    """
    drop = [
        raw for raw, clean in SUPERSEDED.items()
        if raw in df.columns and clean in df.columns
    ]
    drop += [c for c in INTERNAL if c in df.columns]
    view = df.drop(columns=drop)

    ordered = [c for c in PRESENTATION_ORDER if c in view.columns]
    return view[ordered + [c for c in view.columns if c not in ordered]]


# Sheet-facing names. Every cleaned column keeps its ``_CLEANED`` suffix on the
# sheet: it is what tells a reader that this is the parsed float and not the
# text the source held, and the raw values are one sheet away under the bare
# name, so the distinction stays live.
#
# MATCHES_STATUS is the exception, because it is not a cleaned value. Every
# other cleaned column is a repair of what the source said; this one is
# recomputed from scratch against the current merchant master, and agrees with
# the incoming status only by coincidence. Calling it cleaned would claim a
# provenance it does not have.
RENAMED = {
    "MATCHES_STATUS_CLEANED": "MATCHES_STATUS",
}


def output_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the names a sheet is written under: the rename above, then
    lowercase throughout.

    Every sheet goes through here, so one field is spelled one way across the
    whole workbook. Lowercase because an unquoted identifier folds to lowercase
    in Postgres and DuckDB anyway, and matching that means these names survive
    a load without quoting.

    A rename is skipped when its target name is already taken -- by a raw
    column a partial pipeline left in place. Two columns never collapse into
    one: a name collision would silently drop a column, which is the one thing
    this function must not do.

    :param df: A frame about to be written as a sheet.
    :returns: The same frame with sheet-facing column names.
    """
    renames = {
        old: new for old, new in RENAMED.items()
        if old in df.columns and new not in df.columns
    }
    return df.rename(columns=renames).rename(columns=str.lower)
