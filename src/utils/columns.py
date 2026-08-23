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
    # Two entries' worth of replacement, because the two profiles name the
    # cleaned timestamp differently: DateNormalizer writes
    # TXN_DATE_TIME_CLEANED for the v4 workbook, TimestampNormalizer writes
    # TXN_TS for the forecast extract. The raw column is dropped when either
    # of them is present, and never when neither is.
    "TXN_DATE_TIME": ("TXN_DATE_TIME_CLEANED", "TXN_TS"),
    "SETTLE_DATE": "SETTLE_DATE_CLEANED",
    "TXN_AMOUNT": "TXN_AMOUNT_CLEANED",
    "MERCHANT_NAME": "MERCHANT_NAME_CLEANED",
    "MERCHANT_CITY": "MERCHANT_CITY_CLEANED",
    "MERCHANT_COUNTRY": "MERCHANT_COUNTRY_CLEANED",
    "MCC_CODE": "MCC_CODE_CLEANED",
    "MATCHES_STATUS": "MATCHES_STATUS_CLEANED",
    "PROCESSING_CODE": "PROCESSING_CODE_CLEANED",
    "PROCESSING_TYPE": "PROCESSING_TYPE_CLEANED",
    # The macro trio. Each cleaned column is the stated value where the source
    # gave one and the series lookup where it did not, so the raw column is a
    # strict subset of it and keeping it beside the cleaned one says the same
    # thing twice and worse. What the raw column used to answer -- "was this
    # actually in the file" -- is answered by `raw_transactions`, and how much
    # of the file needed recovering at all is in `cleaning_report`.
    "INTEREST_RATE_INDEX": "INTEREST_RATE_INDEX_CLEANED",
    "INFLATION_INDEX": "INFLATION_INDEX_CLEANED",
    "IS_HOLIDAY_MONTH": "IS_HOLIDAY_MONTH_CLEANED",
}

# Working columns: each one is needed to compute something, and each one would
# be a second version of a fact already on the sheet if it were shown.
#
# PROCESSING_TYPE_CLEANED is the processing_codes sheet, repeated once per row,
# and the code it was looked up by is on the transaction. MCC_CATEGORY is the
# mcc_codes sheet, the same way. MCC_CODE_SUGGESTED has been folded into
# MCC_CODE_CLEANED, which now carries the code that survived validation.
# MERCHANT_COUNTRY_EXPECTED is what MERCHANT_COUNTRY_CLEANED was resolved from.
#
# MERCHANT_KIND, MERCHANT_RECOGNISED and INTERNAL_MOVEMENT are all
# MATCHES_STATUS_CLEANED again. The three merchant kinds and the three
# statuses are the same three states under two vocabularies -- Merchant is
# Confirmed, Internal is Not a merchant, Unidentified is Pending -- and the
# two booleans each answer one of them yes or no. The status column is the one
# that goes out, because it is the column the source itself had. MERCHANT_TYPE
# is shown alongside it and is not a fourth spelling of the same thing: it
# answers the prior question -- whether the row has a counterparty at all --
# where the status answers how far naming that counterparty got.
#
# LOCATION_TYPE was on the sheet and is now a working column, for the reason
# IS_ECOMMERCE is one: it is a reading of MERCHANT_CITY_CLEANED, which is
# beside it and spells the marker out in full. The one thing it said that the
# city did not -- that a row names no place because it is internal traffic --
# is now stated where it belongs, on MERCHANT_TYPE, as a fact about the
# counterparty rather than about the geography.
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
    "MERCHANT_KIND",
    "MERCHANT_RECOGNISED",
    "INTERNAL_MOVEMENT",
    "LOCATION_TYPE",
    "VALIDATION_FLAGS",
    # Provenance columns: each says how a value was arrived at rather than what
    # the value is. They are working columns for the same reason the ones above
    # are -- every one is still computed, still counted in cleaning_report, and
    # still available to a caller holding the frame -- they are simply not a
    # second column on a sheet whose job is to state the cleaned row.
    #
    # The three timestamp qualifiers describe the parse, not the transaction.
    # TXN_TS_PRECISION is the exception that stays: the rendered value itself
    # changes with it, because a DAY row is written without a time of day, so
    # it is the one qualifier a reader needs in order to read the column
    # beside it.
    "TXN_TS_STATUS",
    "TXN_TS_SOURCE",
    "TXN_TS_AMBIGUOUS",
    # Both are a verdict on a column already on the row: AUTH_CODE_VALID on
    # AUTH_CODE, IS_ECOMMERCE on MERCHANT_CITY_CLEANED, which spells the marker
    # out. The counts behind both verdicts stay in cleaning_report.
    "AUTH_CODE_VALID",
    "IS_ECOMMERCE",
    # The macro coverage trio distinguished a value the source stated from one
    # the series recovered. The cleaned value is the same number either way,
    # and how many of each there were is a property of the run rather than of
    # the row, which is what cleaning_report is for.
    "INTEREST_RATE_INDEX_COVERAGE",
    "INFLATION_INDEX_COVERAGE",
    "IS_HOLIDAY_MONTH_COVERAGE",
]

# Identity, then money, then merchant, then codes, then flags. Any column not
# named here keeps its position at the end, so a new derived column shows up
# rather than silently vanishing.
PRESENTATION_ORDER = [
    "TXN_ID_CLEANED",
    "TXN_SEQ",
    "ACCOUNT_ID",
    "USER_ID",
    # Both profiles' timestamps, in one list: a run produces one or the other,
    # never both, and naming both here means neither file has to be special
    # cased. The two qualifiers that survive sit beside the reading for the
    # same reason SETTLE_DATE_STATUS sits beside its date -- how the value is
    # rendered depends on the precision, and the offset is what the rendered
    # clock is a clock in.
    "TXN_DATE_TIME_CLEANED",
    "TXN_TS",
    "TXN_TS_PRECISION",
    "TXN_TS_UTC_OFFSET",
    "SETTLE_DATE_CLEANED",
    "SETTLE_DATE_STATUS",
    "TXN_TYPE",
    "TXN_AMOUNT_CLEANED",
    "TXN_CCY",
    "BILLING_AMOUNT",
    "BILLING_CURRENCY",
    "FX_RATE",
    # The source's own balance, then what could be verified about it. The
    # original stays on the sheet rather than being superseded: a third of its
    # values are withheld as unverifiable, and a reader who wants to know what
    # the file actually said should not have to open another sheet to find out.
    "RUNNING_BALANCE",
    "RUNNING_BALANCE_CLEANED",
    "RUNNING_BALANCE_STATUS",
    # The second balance, and never merged into the first. The cleaned column
    # states a balance only where the arithmetic proves one; this one states
    # what the balance would be if the account's own transactions were the
    # only thing that moved it, which is a different claim and a weaker one.
    # Its own status column says how far it can be trusted per row, and the
    # two sit adjacent so neither can be read without the other.
    "RUNNING_BALANCE_ADJUSTED",
    "RUNNING_BALANCE_ADJUSTED_STATUS",
    # Confidence sits beside the merchant name rather than with the other MCC
    # columns: it is the first thing a reader needs after knowing who the
    # merchant is, and the code itself is only meaningful once it is trusted.
    "MERCHANT_NAME_CLEANED",
    # What the name is: a counterparty, or the bank moving the customer's own
    # money. It sits immediately after the name because it is what tells a
    # reader how to read it -- CARD SETTLEMENT beside CARREFOUR is only
    # confusing until the column beside it says one is not a merchant.
    "MERCHANT_TYPE",
    "MCC_CONFIDENCE",
    "MERCHANT_PROCESSOR",
    "MERCHANT_CITY_CLEANED",
    "MERCHANT_COUNTRY_CLEANED",
    "PROCESSING_CODE_CLEANED",
    "MCC_CODE_CLEANED",
    "TERMINAL_ID",
    "HAS_TERMINAL",
    "AUTH_CODE",
    "MATCHES_STATUS_CLEANED",
    # Last: these are properties of the month the row falls in rather than of
    # the transaction, so they read as context after the row has been read.
    "INTEREST_RATE_INDEX_CLEANED",
    "INFLATION_INDEX_CLEANED",
    "IS_HOLIDAY_MONTH_CLEANED",
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
    def replaced(clean) -> bool:
        """:returns: Whether any of a raw column's replacements is present."""
        names = (clean,) if isinstance(clean, str) else clean
        return any(name in df.columns for name in names)

    drop = [
        raw for raw, clean in SUPERSEDED.items()
        if raw in df.columns and replaced(clean)
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
