"""
Reading the cleaned transactions the feature build starts from.
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

# Transaction identity and ordering within each account.
# txn_seq determines the last transaction of a month because txn_ts can have ties.
KEYS = ("user_id", "account_id", "txn_seq", "txn_ts")

# USD transaction flows and the processing code used to determine direction.
# billing_amount is used instead of the native-currency txn_amount_cleaned.
FLOWS = (
"billing_amount",
"billing_currency",
"processing_code_cleaned",
)

# USD-normalized balances and their reliability status.
# running_balance_normalized is used instead of the account's native-currency balance.
BALANCES = ("running_balance_normalized", "running_balance_status")

# Transaction category and merchant information.
ACTIVITY = ("mcc_code_cleaned", "merchant_name_cleaned")
COLUMNS = KEYS + FLOWS + BALANCES + ACTIVITY

# Postgres UUID columns are cast to strings during the JDBC read.
UUID_COLUMNS = ("user_id", "account_id")

# Native-currency columns Stage 3 must not use.
# Keeping them explicit makes the restriction testable.
FORBIDDEN = (
"txn_amount_cleaned",
"txn_ccy",
"running_balance_filled",
"running_balance_currency",
)

# Currency used for all monetary features.
USD = "USD"

# Common month key used throughout Stage 3.
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
