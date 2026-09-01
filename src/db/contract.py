"""
The frame-to-table contract: which column becomes which, and as what type.

This exists because ``frame.write.jdbc(...)`` is the wrong shape of answer. It
writes whatever columns happen to be on the frame, under whatever names and
types they happen to have, and discovers the disagreement as a driver error
part-way through a write that has already committed some rows. The disagreement
is real and it is not small -- of the 22 columns ``cleaned_transactions``
declares, five do not match what the pipeline produces:

    txn_seq              string     -> BIGINT         never parsed by a stage
    billing_amount       string     -> NUMERIC(18,4)  never parsed by a stage
    fx_rate              string     -> NUMERIC(20,10) never parsed by a stage
    settle_date_cleaned  timestamp  -> DATE           midnight is not a time
    the four doubles     double     -> NUMERIC        binary float into decimal

The first three are raw source columns that no stage cleans, because nothing in
the cleaning has an opinion about them -- they arrive as text from the CSV
reader and would be handed to Postgres as text. The fourth would silently carry
a 00:00:00 into a DATE. The last would let a binary float decide the fourth
decimal place of an amount, which is how a total ends up a cent out.

So the mapping is written down, once, as data. Being data it can be checked
against the frame without a database and against ``sql/schema.sql`` without a
Spark session, which is what ``tests/test_db_contract.py`` does.
"""

from src.config.errors import ConfigError

# The table this describes. Named here rather than passed in: two callers
# disagreeing about which table the contract describes is not a configuration
# option, it is a bug.
TABLE = "cleaned_transactions"

# The upsert key. Produced by the `duplicates` stage, which suffixes it when a
# source TXN_ID is shared, so it is unique where TXN_ID may not be.
KEY = "txn_id_cleaned"

# Frame column -> Spark cast type, in the order sql/schema.sql declares them.
#
# A value of None means the frame's type is already what the column wants and
# no cast is emitted -- stated explicitly rather than left out, so that adding
# a column to the table and forgetting it here is a KeyError with a name in it
# rather than a silently missing column.
#
# The cast strings are Spark's DDL type names, and the decimal precisions match
# the NUMERIC declarations exactly. They have to: casting to decimal(18,2) for
# a NUMERIC(18,4) column would round on the way in and the database would
# accept it without complaint.
COLUMNS: dict[str, str | None] = {
    "TXN_ID_CLEANED": None,
    "TXN_SEQ": "long",
    # None even though both are UUID columns in sql/schema.sql. Spark has no
    # uuid type to cast to, so the string travels as-is and Postgres coerces it
    # -- see the stringtype note in src/db/settings.py. There is nothing to put
    # here; the absence is the decision.
    "ACCOUNT_ID": None,
    "USER_ID": None,
    "TXN_TS": None,
    # timestamp -> DATE. The stage parses settlement to a timestamp because
    # that is what its parser returns; the source states a date and the column
    # is a DATE, so the time part is an artefact of the parse and dropping it
    # here is what keeps the table honest about what was observed.
    "SETTLE_DATE_CLEANED": "date",
    "TXN_AMOUNT_CLEANED": "decimal(18,4)",
    "TXN_CCY": None,
    "BILLING_AMOUNT": "decimal(18,4)",
    "BILLING_CURRENCY": None,
    "FX_RATE": "decimal(20,10)",
    "RUNNING_BALANCE_FILLED": "decimal(18,4)",
    # How that figure was arrived at, and the reason the column above is
    # readable at all. It is persisted rather than left on the frame because
    # the balance is stated on every row the arithmetic reaches, and without
    # this a consumer cannot tell a figure two anchors agree on from one
    # reconstructed in a single direction or one taken from inside a span the
    # source contradicts. Constrained in sql/schema.sql against the same
    # vocabulary the cleaner declares.
    #
    # Cast to string explicitly: the pandas frame holds this as a Categorical
    # and Spark as a plain string, and an uncast Categorical is not something
    # the JDBC writer has a type for.
    "RUNNING_BALANCE_STATUS": "string",
    # Text, not an enum: the set of currencies is a property of the extract,
    # not of the schema, and a CHECK constraint here would reject a file that
    # merely trades in one more currency than this one does.
    "RUNNING_BALANCE_CURRENCY": None,
    "RUNNING_BALANCE_NORMALIZED": "decimal(18,4)",
    # On a CONTRADICTED row, backward reconstruction minus forward. Signed, so
    # the reading the published column does not carry is recoverable exactly.
    # Null on every other status -- there is only one answer to differ from.
    "RUNNING_BALANCE_DISCREPANCY": "decimal(18,4)",
    "MERCHANT_NAME_CLEANED": None,
    "MERCHANT_CITY_CLEANED": None,
    "MERCHANT_COUNTRY_CLEANED": None,
    "PROCESSING_CODE_CLEANED": None,
    "PROCESSING_TYPE_CLEANED": None,
    "MCC_CODE_CLEANED": None,
    "AUTH_CODE": None,
    "INTEREST_RATE_INDEX_CLEANED": "decimal(10,4)",
    "INFLATION_INDEX_CLEANED": "decimal(10,4)",
    "IS_HOLIDAY_MONTH_CLEANED": None,
}

# Written by the writer rather than read off the frame: the run knows which
# load it is and when it ran, and no cleaning stage has any business knowing
# either. `cleaned_at` is left to the column's own DEFAULT now() -- the
# database's clock is the one clock every writer shares, and a timestamp taken
# on the driver would be the JVM's.
SYNC_JOB = "sync_job_id"

# Every column the writer sends, in insert order. cleaned_at is absent by
# design; see above.
TARGET = [name.lower() for name in COLUMNS] + [SYNC_JOB]

# Columns an upsert must never overwrite on a row that already exists.
#
# Only the key, and it is in the list for completeness rather than because
# ON CONFLICT would touch it. `sync_job_id` is deliberately NOT here: when a
# later load carries a row again, the useful answer to "which run put this
# value in the table" is the run that last wrote it, so the column follows the
# write. A first-seen column would be a different question and a second column.
IMMUTABLE = frozenset({KEY})


def project(frame, sync_job_id: str):
    """
    Selects the table's columns from the cleaned frame and casts each to what
    the column holds.

    :param frame: The Spark frame as ``src.spark.pipeline.run`` returned it,
        carrying the cleaned columns and all the working ones besides.
    :param sync_job_id: The load this run is part of, stamped on every row.
    :returns: A frame whose columns are exactly ``TARGET``, in that order, so
        the JDBC write is positional and cannot be silently misaligned.
    :raises ConfigError: If the frame is missing a column the table requires,
        naming all of them at once -- a writer that fails on the first missing
        column makes you run the pipeline again to find the second.
    """
    from pyspark.sql import functions as F

    missing = [name for name in COLUMNS if name not in frame.columns]
    if missing:
        raise ConfigError(
            f"the cleaned frame is missing {len(missing)} column(s) that "
            f"{TABLE} requires: {', '.join(missing)}. Either a stage did not "
            f"run, or the profile does not produce them."
        )

    selected = []
    for name, cast in COLUMNS.items():
        column = F.col(name)
        if cast is not None:
            column = column.cast(cast)
        selected.append(column.alias(name.lower()))

    selected.append(F.lit(sync_job_id).alias(SYNC_JOB))
    return frame.select(*selected)


def merge_statement(staging: str) -> str:
    """
    Builds the one statement that moves a loaded batch into the live table.

    Spark's JDBC writer cannot express ``ON CONFLICT``: its modes are append,
    overwrite, ignore and error, and none of those is an upsert. The way round
    it is the standard one -- Spark bulk-loads a staging table, and a single
    SQL statement merges that into the destination. The merge runs in one
    transaction, so a run either lands completely or not at all.

    ``DISTINCT ON`` is not paranoia. ``ON CONFLICT`` raises if one statement
    presents the same key twice, and a batch that somehow carried a duplicate
    key would fail the whole load with an error naming the constraint rather
    than the cause. Keeping the last occurrence matches what a re-delivery
    means: the newer copy wins.

    :param staging: Name of the staging table, already loaded.
    :returns: The INSERT ... ON CONFLICT DO UPDATE statement.
    """
    columns = ", ".join(TARGET)
    updates = ", ".join(
        [
            f"{name} = EXCLUDED.{name}"
            for name in TARGET
            if name not in IMMUTABLE
        ]
        # Not from EXCLUDED, because the staging row does not carry it: the
        # column means "when the pipeline last wrote this row", and an upsert
        # that rewrote the row did exactly that. Left out, an updated row would
        # keep the timestamp of the load that first inserted it and the column
        # would quietly mean something else on updated rows than on new ones.
        + ["cleaned_at = now()"]
    )
    return (
        f"INSERT INTO {TABLE} ({columns})\n"
        f"SELECT DISTINCT ON ({KEY}) {columns}\n"
        f"  FROM {staging}\n"
        f" ORDER BY {KEY}\n"
        f"ON CONFLICT ({KEY}) DO UPDATE SET {updates}"
    )
