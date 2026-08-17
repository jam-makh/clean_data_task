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
    "TXN_DATE_TIME": "TXN_DATE_TIME_CLEAN",
    "SETTLE_DATE": "SETTLE_DATE_CLEAN",
    "TXN_AMOUNT": "TXN_AMOUNT_CLEAN",
    "MERCHANT_NAME": "MERCHANT_NAME_CLEAN",
    "MERCHANT_CITY": "MERCHANT_CITY_CLEAN",
    "MCC_CODE": "MCC_CODE_STR",
    "PROCESSING_CODE": "PROCESSING_CODE_ISO",
    "PROCESSING_TYPE": "PROCESSING_TYPE_CLEAN",
}

# Identity, then money, then merchant, then codes, then flags. Any column not
# named here keeps its position at the end, so a new derived column shows up
# rather than silently vanishing.
PRESENTATION_ORDER = [
    "TXN_ID",
    "TXN_ID_SEQ",
    "ACCOUNT_ID",
    "TXN_DATE_TIME_CLEAN",
    "SETTLE_DATE_CLEAN",
    "SETTLE_DATE_STATUS",
    "TXN_TYPE",
    "TXN_AMOUNT_CLEAN",
    "TXN_CCY",
    "BILLING_AMOUNT",
    "BILLING_CURRENCY",
    "FX_RATE",
    # Confidence sits beside the merchant name rather than with the other MCC
    # columns: it is the first thing a reader needs after knowing who the
    # merchant is, and the code itself is only meaningful once it is trusted.
    "MERCHANT_NAME_CLEAN",
    "MCC_CONFIDENCE",
    "MERCHANT_RECOGNISED",
    "MERCHANT_PROCESSOR",
    "MERCHANT_CITY_CLEAN",
    "MERCHANT_COUNTRY",
    "MERCHANT_COUNTRY_EXPECTED",
    "PROCESSING_CODE_ISO",
    "PROCESSING_TYPE_CLEAN",
    "MCC_CODE_STR",
    "MCC_CATEGORY",
    "MCC_CODE_SUGGESTED",
    "TERMINAL_ID",
    "HAS_TERMINAL",
    "AUTH_CODE",
    "AUTH_CODE_VALID",
    "IS_ECOMMERCE",
    "MATCHES_STATUS",
    "VALIDATION_FLAGS",
]


def presented(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops raw columns that a cleaned column supersedes, then orders the rest.

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
    view = df.drop(columns=drop)

    ordered = [c for c in PRESENTATION_ORDER if c in view.columns]
    return view[ordered + [c for c in view.columns if c not in ordered]]
