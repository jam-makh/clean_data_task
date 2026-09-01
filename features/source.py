"""
Reading the Stage 2 cleaned transactions the feature build starts from.
The rows are read into a Spark DataFrame and stay one all
the way to the Postgres upsert.

Postgres is the only source. There is no file reader: the cleaned
transactions live in a table. The units decision is
enforced here by never selecting the native-currency ones.
"""

from pyspark.sql import functions as F

from src.config_readers.errors import ConfigError
from src.db.settings import Database

TABLE = "cleaned_transactions"

# The identity of a transaction and where it sits in its account's chain.
#
# txn_seq rather than txn_ts decides which row is last in a month: it is the
# sequence the running balance was reconstructed along, and it is total, where
# txn_ts has ties and rows recorded to the day only.
KEYS = ("user_id", "account_id", "txn_seq", "txn_ts")

# The USD flow column and the code that says which way it moved.
#
# billing_amount, never txn_amount_cleaned. The latter is denominated in
# txn_ccy -- six currencies in this extract -- and summing it across accounts
# adds LBP to USD. billing_currency is carried so that assumption is checked
# rather than trusted.
FLOWS = (
    "billing_amount",
    "billing_currency",
    "processing_code_cleaned",
)

# The USD balance and the status that says how much to trust it.
#
# running_balance_normalized, never running_balance_filled. The latter is in
# running_balance_currency, which differs per account.
BALANCES = ("running_balance_normalized", "running_balance_status")

# What a transaction was for and who it was with.
ACTIVITY = ("mcc_code_cleaned", "merchant_name_cleaned")

COLUMNS = KEYS + FLOWS + BALANCES + ACTIVITY

# Columns that are UUID in Postgres and must be string in the frame.
#
# Spark's Postgres dialect reports uuid as JDBC OTHER, and whether it folds
# that to StringType is a property of the dialect, not of this code. Casting in
# the projection makes the read independent of that: the driver sees text, and
# a dialect change becomes a non-event rather than a Stage 3 that dies at
# schema resolution before it has read a row.
UUID_COLUMNS = ("user_id", "account_id")

# Columns Stage 3 must never read, listed so the ban is testable rather than
# a matter of remembering. Each is native-currency and not comparable across
# accounts; they stay in Stage 2 for diagnostics.
FORBIDDEN = (
    "txn_amount_cleaned",
    "txn_ccy",
    "running_balance_filled",
    "running_balance_currency",
)

# The denomination every monetary feature is expressed in.
USD = "USD"

# The month every later stage groups on. Derived once here so no downstream
# module re-derives it a second way.
MONTH = "month"

_STRINGS = (
    "user_id",
    "account_id",
    "billing_currency",
    "processing_code_cleaned",
    "mcc_code_cleaned",
    "merchant_name_cleaned",
    "running_balance_status",
)

_DOUBLES = ("billing_amount", "running_balance_normalized")


def typed(frame):
    """
    Puts the read columns into the types the build assumes.

    Casts rather than parses. The session runs with ``spark.sql.ansi.enabled``
    off, so a value that will not convert becomes null instead of killing the
    job -- the same contract ``errors="coerce"`` gave, and the reason
    ``rows_without_parseable_month`` is a reported number rather than a crash.

    :param frame: Rows as the reader produced them.
    :returns: The same rows, typed, with ``month`` derived.
    """
    frame = frame.withColumn("txn_ts", F.col("txn_ts").cast("timestamp"))
    frame = frame.withColumn("txn_seq", F.col("txn_seq").cast("long"))

    for column in _DOUBLES:
        frame = frame.withColumn(column, F.col(column).cast("double"))

    for column in _STRINGS:
        frame = frame.withColumn(column, F.col(column).cast("string"))

    # First day of the month the transaction fell in, as a date. Every spine,
    # every groupBy and every window in Stage 3 keys on this column.
    return frame.withColumn(
        MONTH, F.trunc(F.col("txn_ts"), "month").cast("date")
    )


def validate(frame) -> None:
    """
    Checks the assumptions the units decision rests on.

    The currency check costs one small job. It is worth it: summing two
    denominations is the one failure deliverable 5 is about, and it is
    invisible in the output.

    :param frame: The read transactions.
    :raises ConfigError: If a required column is absent, or a row's flow is
        denominated in anything but USD.
    """
    missing = [name for name in COLUMNS if name not in frame.columns]
    if missing:
        raise ConfigError(
            f"{TABLE} is missing {len(missing)} column(s) Stage 3 needs: "
            f"{', '.join(missing)}"
        )

    row = frame.agg(
        F.collect_set("billing_currency").alias("denominations")
    ).first()
    denominations = set(row["denominations"] or [])

    if denominations - {USD}:
        raise ConfigError(
            f"billing_amount is not all USD: found "
            f"{sorted(denominations)}. Stage 3 sums this column across "
            f"accounts, so a second denomination would mix units. Convert "
            f"in Stage 2 or add a conversion step before aggregating."
        )


def from_database(spark, database: Database, table: str = TABLE):
    """
    Reads the cleaned transactions out of Postgres over JDBC.

    Through the JVM's driver, not psycopg2: the rows never enter the Python
    process, which is the whole point of reading them into Spark.

    :param spark: The session.
    :param database: Where to connect.
    :param table: The table to read.
    :returns: One row per transaction, typed, with ``month`` derived.
    """
    # A subquery rather than the bare table name, so the uuid columns arrive as
    # text and the projection is stated to the database instead of to Spark --
    # the same shape src/db/raw.py reads with. The .select() that follows is
    # then an assertion that the two lists agree, which is worth keeping.
    projection = ", ".join(
        f"{column}::text AS {column}" if column in UUID_COLUMNS else column
        for column in COLUMNS
    )
    frame = spark.read.jdbc(
        url=database.jdbc_url,
        table=f"(SELECT {projection} FROM {table}) AS cleaned",
        properties=database.jdbc_properties,
    ).select(*COLUMNS)

    frame = typed(frame)
    validate(frame)
    return frame
