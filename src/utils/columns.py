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
    # The cleaned balance states one only where the arithmetic proves it, so
    # it is not a strict superset of the stated column the way the others are:
    # a third of the source's values are withheld as unverifiable, and the
    # sheet says UNKNOWN there rather than repeating a figure that could not
    # be checked. What the source actually said is in `raw_transactions`,
    # which is where the unedited record belongs.
    "RUNNING_BALANCE": "RUNNING_BALANCE_FILLED",
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

# Working columns: everything the pipeline computes that is not the cleaned
# counterpart of a source column.
#
# The rule the cleaned sheet follows is deliberately blunt: **one column per
# source column** -- the cleaned version where a stage produced one, the
# original where none did, and nothing else. A reader of that sheet is asking
# "what does this transaction say", and every column answers it once.
#
# Nothing here is discarded. Each one stays on the frame for the rest of the
# run, each one's totals are in `cleaning_report` -- how many merchants
# resolved, how many rows were internal transfers, how many balances were
# derived rather than stated -- and each one is in AUDIT_COLUMNS below. The
# question a status column answers is "how do you know", and that question is
# answered in the report and in the audit trail rather than in a second column
# on the sheet.
#
# The three groups, and why each is not a cleaned column:
#
# 1. Lookups repeated per row. MCC_CATEGORY is the mcc_codes sheet;
#    MCC_CODE_SUGGESTED has been folded into MCC_CODE_CLEANED, which carries
#    the code that survived validation; MERCHANT_COUNTRY_EXPECTED is what
#    MERCHANT_COUNTRY_CLEANED was resolved from.
#
# 2. Restatements of one another, or of a column already on the sheet.
#    MATCHES_STATUS_CLEANED, MERCHANT_KIND, MERCHANT_RECOGNISED,
#    INTERNAL_MOVEMENT and MERCHANT_TYPE are five spellings of one three-state
#    fact -- Merchant is Confirmed, Internal is Not a merchant, Unidentified is
#    Pending -- and the two booleans each answer one of them yes or no.
#    LOCATION_TYPE and IS_ECOMMERCE are readings of MERCHANT_CITY_CLEANED,
#    which spells the marker out in full. HAS_TERMINAL is a reading of
#    TERMINAL_ID, AUTH_CODE_VALID of AUTH_CODE.
#
#    MATCHES_STATUS_CLEANED is in this group rather than being the cleaned
#    counterpart of MATCHES_STATUS, despite the name. Every other cleaned
#    column repairs what the source said; this one is recomputed from scratch
#    against the current merchant master and agrees with the incoming status
#    only by coincidence. It is a verdict this run reached, which is what the
#    report and the audit trail are for -- and `merchant_review` is the sheet
#    that acts on it.
#
# 3. Verdicts on how a value was arrived at. The timestamp qualifiers describe
#    the parse rather than the transaction; the settlement and balance
#    statuses say how far each value could be verified; MCC_CONFIDENCE and
#    MERCHANT_PROCESSOR say how the code and the name were reached; the macro
#    coverage trio distinguishes a stated value from a recovered one, and the
#    cleaned value is the same number either way.
#
# VALIDATION_FLAGS is group 3 as well: every contradiction it recorded is
# either corrected in the cleaned column or counted in the report.
INTERNAL = [
    "MCC_CATEGORY",
    "MCC_CODE_SUGGESTED",
    "MERCHANT_COUNTRY_EXPECTED",
    "MATCHES_STATUS_CLEANED",
    "MERCHANT_KIND",
    "MERCHANT_RECOGNISED",
    "INTERNAL_MOVEMENT",
    "MERCHANT_TYPE",
    "LOCATION_TYPE",
    "VALIDATION_FLAGS",
    # How the timestamp was read. TXN_TS_PRECISION is absent from this list
    # and still never reaches the sheet: it is in RENDER_ONLY below, because
    # the writer has to read it to know whether this row's timestamp may be
    # written with a time of day at all. Dropping it here instead would put a
    # 00:00:00 on every date-only row, which invents a reading the source
    # never gave.
    "TXN_TS_STATUS",
    "TXN_TS_SOURCE",
    "TXN_TS_AMBIGUOUS",
    "TXN_TS_UTC_OFFSET",
    # How far each value could be verified. SETTLE_DATE_STATUS also selects
    # the pending_settlement and anomaly_settlement sheets, which is done from
    # the full frame in `build_sheets` rather than from this view.
    #
    # RUNNING_BALANCE_STATUS is deliberately NOT here any more. It was
    # internal while the balance column withheld a figure wherever it could
    # not prove one -- a null was its own warning, and the status merely said
    # which kind. Now that the column states a figure on every row it can, the
    # status is the only thing separating a balance two anchors agree on from
    # one reconstructed in a single direction. Dropping it would hand a
    # consumer seven kinds of number spelled identically.
    "SETTLE_DATE_STATUS",
    # Which column was found to move the balance here. A finding about the
    # source rather than a figure, so it sits with the diagnostics: it is how
    # a reader sees that the file changed convention, and where. The practical
    # consequence -- what the balance is denominated in -- is already on
    # RUNNING_BALANCE_CURRENCY, which is why this one stays internal.
    "RUNNING_BALANCE_BASIS",
    # Verdicts on a column already on the row.
    "AUTH_CODE_VALID",
    "HAS_TERMINAL",
    "IS_ECOMMERCE",
    "MCC_CONFIDENCE",
    "MERCHANT_PROCESSOR",
    # The macro coverage trio distinguished a value the source stated from one
    # the series recovered. The cleaned value is the same number either way,
    # and how many of each there were is a property of the run rather than of
    # the row, which is what cleaning_report is for.
    "INTEREST_RATE_INDEX_COVERAGE",
    "INFLATION_INDEX_COVERAGE",
    "IS_HOLIDAY_MONTH_COVERAGE",
    # The diagnostic columns every stage now writes instead of counting as it
    # goes. Each one says what a stage did to this row; together they are what
    # the run's report is derived from, in one pass, after every stage has
    # finished. See `src/utils/audit.py` and `BaseCleaner`.
    #
    # They are off the *sheet* for the reason the three timestamp qualifiers
    # above are: the sheet's job is to state the cleaned row, and a column
    # saying how the value was arrived at is a second reading of the column
    # beside it. That is a decision about this presentation, not about the
    # data -- they stay on the frame throughout, and AUDIT_COLUMNS below is
    # the list that answers "why does this row look like this".
    "TXN_DATE_TIME_FORMAT",
    "SETTLE_DATE_FORMAT",
    "TXN_TS_FORMAT",
    "TXN_TS_SLASH_RESOLUTION",
    "TXN_AMOUNT_COERCION",
    "TXN_AMOUNT_SIGN",
    "PROCESSING_CODE_DIRECTION",
    "EXACT_DUPLICATE_COPIES",
    "TXN_ID_COLLISION",
    "AUTH_CODE_REPEATED",
    "RUNNING_BALANCE_CHAIN_BREAK",
    "MCC_SIGNAL",
]

# The audit trail, and the answer to requirement 7 at the cleaning boundary.
#
# Not currently persisted. `sql/schema.sql` carries the cleaned columns plus
# the provenance a run needs to be reconciled against its own completion event
# -- sync_job_id and cleaned_at -- and stops there, because these 35 were not
# asked for at the database boundary. They stay on the frame and reach the
# workbook, and a table for them can be added later; the write path is
# `src/db/contract.py`, which is one list away from carrying them.
#
# The split this list encodes is between the two questions a stored row has to
# answer. The cleaned columns answer "what is this transaction". These answer
# "how do you know" -- which format read the date, whether the amount was
# reformatted or its sign restored, whether the balance reconciled, which rule
# chose the MCC, how many identical source rows this one stands for. Every one
# of them is a per-row statement, so every one survives the write; a total in
# a report cannot be joined back to the transaction that caused it, which is
# what makes a report an insufficient audit trail on its own.
#
# Internal-only is the short list, and everything on it is internal because it
# is a working value some other published column already states in full, not
# because it is uninteresting: MCC_CATEGORY is the reference sheet repeated
# per row, MCC_CODE_SUGGESTED has been folded into MCC_CODE_CLEANED, and
# MERCHANT_KIND, MERCHANT_RECOGNISED and INTERNAL_MOVEMENT are three spellings
# of MATCHES_STATUS_CLEANED.
AUDIT_COLUMNS = [
    # How each timestamp was read, and how the day/month ambiguity was settled.
    "TXN_DATE_TIME_FORMAT",
    "TXN_TS_FORMAT",
    "TXN_TS_SLASH_RESOLUTION",
    "TXN_TS_STATUS",
    "TXN_TS_SOURCE",
    "TXN_TS_PRECISION",
    "TXN_TS_AMBIGUOUS",
    "TXN_TS_UTC_OFFSET",
    "SETTLE_DATE_FORMAT",
    "SETTLE_DATE_STATUS",
    # What was done to the money, and on whose authority.
    "TXN_AMOUNT_COERCION",
    "TXN_AMOUNT_SIGN",
    "PROCESSING_CODE_DIRECTION",
    # What the arithmetic could and could not prove about the balance. The
    # status and the discrepancy are audit columns AND published ones -- they
    # are listed here because the audit view must carry them, and in
    # PRESENTATION_ORDER because a consumer of the figure cannot read it
    # without them. The chain-break flag stays audit-only: it is a property of
    # the pair of rows rather than of the balance on either.
    "RUNNING_BALANCE_STATUS",
    "RUNNING_BALANCE_DISCREPANCY",
    "RUNNING_BALANCE_CHAIN_BREAK",
    # Identity: what collapsed into this row, and whether its key was shared.
    "EXACT_DUPLICATE_COPIES",
    "TXN_ID_COLLISION",
    # Which rule decided the MCC, and how far the merchant name resolved --
    # including whether the row had a counterparty at all, or was the bank
    # moving the customer's own money.
    "MCC_SIGNAL",
    "MCC_CONFIDENCE",
    "MERCHANT_PROCESSOR",
    "MERCHANT_TYPE",
    "MATCHES_STATUS_CLEANED",
    # What was recoverable, and what was a sentinel rather than a gap.
    "HAS_TERMINAL",
    "AUTH_CODE_VALID",
    "AUTH_CODE_REPEATED",
    "IS_ECOMMERCE",
    "LOCATION_TYPE",
    "MERCHANT_COUNTRY_EXPECTED",
    "INTEREST_RATE_INDEX_COVERAGE",
    "INFLATION_INDEX_COVERAGE",
    "IS_HOLIDAY_MONTH_COVERAGE",
    # Every cross-field contradiction found, per row.
    "VALIDATION_FLAGS",
]

# Identity, then time, then money, then merchant, then codes. One entry per
# source column: the cleaned counterpart where a stage produced one, the
# original where none did. Any column not named here keeps its position at the
# end, so a new derived column shows up rather than silently vanishing --
# which is what makes a missing INTERNAL entry visible instead of silent.
PRESENTATION_ORDER = [
    "TXN_ID_CLEANED",              # <- TXN_ID
    "TXN_SEQ",
    "ACCOUNT_ID",
    "USER_ID",
    # Both profiles' timestamps, in one list: a run produces one or the other,
    # never both, and naming both here means neither file has to be special
    # cased.
    "TXN_DATE_TIME_CLEANED",       # <- TXN_DATE_TIME, v4 profile
    "TXN_TS",                      # <- TXN_DATE_TIME, forecast profile
    # Not a column on the sheet. It survives this list only so the writer can
    # read it, and is dropped once it has been read -- see RENDER_ONLY.
    "TXN_TS_PRECISION",
    "SETTLE_DATE_CLEANED",         # <- SETTLE_DATE
    "TXN_TYPE",
    "TXN_AMOUNT_CLEANED",          # <- TXN_AMOUNT
    "TXN_CCY",
    "BILLING_AMOUNT",
    "BILLING_CURRENCY",
    "FX_RATE",
    # The balance, how it was arrived at, what currency it is in, what it is
    # worth in USD, and -- where the source's own anchors disagree -- by how
    # much. The status sits immediately right of the figure because it is not
    # optional reading: the column now states a balance wherever the
    # arithmetic reaches one, and the status is the only thing that says
    # whether that figure was proven, reconstructed from one side, or taken
    # from inside a span the source cannot reconcile.
    #
    # Three columns rather than one because two questions have to be
    # answerable at a glance: what does this account actually hold, and how
    # does that compare with the account below it. The first needs the
    # original denomination -- 10,000,000 LBP and 5,000 USD are not the same
    # statement and neither is the useful one to hide -- and the second needs
    # a single currency. The currency column sits between them so that
    # neither figure can be read without it.
    "RUNNING_BALANCE_FILLED",      # <- RUNNING_BALANCE
    "RUNNING_BALANCE_STATUS",
    "RUNNING_BALANCE_CURRENCY",
    "RUNNING_BALANCE_NORMALIZED",
    "RUNNING_BALANCE_DISCREPANCY",
    "MERCHANT_NAME_CLEANED",       # <- MERCHANT_NAME
    "MERCHANT_CITY_CLEANED",       # <- MERCHANT_CITY
    "MERCHANT_COUNTRY_CLEANED",    # <- MERCHANT_COUNTRY
    "PROCESSING_CODE_CLEANED",     # <- PROCESSING_CODE
    "PROCESSING_TYPE_CLEANED",     # <- PROCESSING_TYPE
    "MCC_CODE_CLEANED",            # <- MCC_CODE
    "TERMINAL_ID",
    "AUTH_CODE",
    # Last: these are properties of the month the row falls in rather than of
    # the transaction, so they read as context after the row has been read.
    "INTEREST_RATE_INDEX_CLEANED",  # <- INTEREST_RATE_INDEX
    "INFLATION_INDEX_CLEANED",      # <- INFLATION_INDEX
    "IS_HOLIDAY_MONTH_CLEANED",     # <- IS_HOLIDAY_MONTH
]

# Columns the writer reads and does not write.
#
# A timestamp is rendered to the precision it was actually observed at, so a
# row whose source gave no time of day is written as a bare date. The writer
# needs the precision column to know that, and a reader of the sheet does not
# need it afterwards -- the rendered value already shows which it was. So it
# passes through `presented` and `render_dates` consumes it.
#
# The alternative is putting it in INTERNAL, which drops it one step earlier,
# before the writer can see it. Every date-only row would then be written
# 00:00:00: a time of day the source never recorded, indistinguishable on the
# sheet from a transaction that really happened at midnight.
RENDER_ONLY = ["TXN_TS_PRECISION", "TXN_DATE_TIME_PRECISION"]


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
# Empty, and the mechanism is kept rather than the entry. It held exactly one
# rename, MATCHES_STATUS_CLEANED to MATCHES_STATUS, for the one published
# column that was not a repair of what the source said -- and that column is a
# working column now, so there is nothing left to rename. A rename rule for a
# column no sheet carries is the kind of thing that goes quietly wrong later.
RENAMED: dict[str, str] = {}


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
