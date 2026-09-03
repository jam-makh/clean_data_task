"""
Reading the cleaned transactions the feature build starts from.
The rows are read into a Spark DataFrame and stay one all
the way to the Postgres upsert.

Postgres is the only source. The units decision is
enforced by never selecting the native-currency ones.
"""

from pyspark.sql import functions as F

from src.config_readers.errors import ConfigError
from src.db.settings import Database

# Source table for the features
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

    # Cast transaction time to a Spark timestamp.
    frame = frame.withColumn("txn_ts", F.col("txn_ts").cast("timestamp"))

    # Cast sequence order to a long integer.
    frame = frame.withColumn("txn_seq", F.col("txn_seq").cast("long"))

    # Cast monetary columns to doubles.
    for column in _DOUBLES:
        frame = frame.withColumn(column, F.col(column).cast("double"))

    # Cast identifiers and categorical columns to strings.
    for column in _STRINGS:
        frame = frame.withColumn(column, F.col(column).cast("string"))

    # Derive the first day of each transaction's month.
    return frame.withColumn(
        MONTH, F.trunc(F.col("txn_ts"), "month").cast("date")
    )

def validate(frame) -> None:
    """
    Validate required columns and ensure all transaction flows are in USD.

    :param frame: The read transactions.
    :raises ConfigError: If a required column is absent, or a row's flow is
        denominated in anything but USD.
    """
    # Find Stage 3 columns missing from the source DataFrame.
    missing = [name for name in COLUMNS if name not in frame.columns]

    # Stop if the source schema does not satisfy the Stage 3 contract.
    if missing:
        raise ConfigError(
            f"{TABLE} is missing {len(missing)} column(s) Stage 3 needs: "
            f"{', '.join(missing)}"
        )

    # Collect all distinct billing currencies present in the data.
    row = frame.agg(
        F.collect_set("billing_currency").alias("denominations")
    ).first()

    # Convert the collected currencies to a Python set.
    denominations = set(row["denominations"] or [])

    # Stop if any billing currency other than USD is present.
    if denominations - {USD}:
        raise ConfigError(
            f"billing_amount is not all USD: found "
            f"{sorted(denominations)}. Stage 3 sums this column across "
            f"accounts, so a second denomination would mix units. Convert "
            f"in Stage 2 or add a conversion step before aggregating."
        )


def from_database(spark, database: Database, table: str = TABLE):
    """
    Read cleaned transactions from Postgres, type them, and validate them.

    :param spark: The session.
    :param database: Where to connect.
    :param table: The table to read.
    :returns: One row per transaction, typed, with ``month`` derived.
    """
    # Build the SQL projection and cast Postgres UUIDs to text.
    projection = ", ".join(
        f"{column}::text AS {column}" if column in UUID_COLUMNS else column
        for column in COLUMNS
    )

    # Read only the required Stage 3 columns from Postgres through JDBC.
    frame = spark.read.jdbc(
        url=database.jdbc_url,
        table=f"(SELECT {projection} FROM {table}) AS cleaned",
        properties=database.jdbc_properties,
    ).select(*COLUMNS)

    # Apply the Spark types expected by the feature build.
    frame = typed(frame)

    # Verify the source schema and USD unit assumption.
    validate(frame)

    # Return the prepared transaction DataFrame.
    return frame
