"""
The feature table's columns, declared once with when each becomes knowable.

Generates the Spark schema, the Postgres DDL and the upsert statement from one
list, so the three cannot drift, and enforces the point-in-time rule.

This table is modelling features only. Pipeline diagnostics -- how many
accounts contributed a balance, whether one was carried forward, how many
transactions declared no direction -- are still computed, and are reported by
``features.report``. They are deliberately not columns here: a feature
table that mixes predictors with observability metrics hands Stage 4 columns
it has to know to ignore.
"""

from dataclasses import dataclass

from src.config_readers.errors import ConfigError

TABLE = "feature_store_monthly"

# Where Spark bulk-loads before the merge, named after its destination so the
# pairing is obvious in a table listing. Same arrangement as the Stage 2 sink
# in src/db/migrate.py, and for the same reason: Spark's JDBC writer has no
# upsert mode.
STAGING = f"staging_{TABLE}"

# The grain. One row per user per month, and the upsert key of the table.
KEY = ("user_id", "month")

# When a column's value becomes knowable.

# KEY           identifies the row.
# BEFORE_MONTH  computed only from months strictly before the row's month.
# CALENDAR      a property of the calendar, fixed before the month begins.
# TARGET        the thing being predicted. Reads month M.
KEY_COLUMN, BEFORE_MONTH, CALENDAR, TARGET = (
    "KEY", "BEFORE_MONTH", "CALENDAR", "TARGET",
)

# Types per logical kind, so a money column cannot be declared NUMERIC in one
# place and DOUBLE PRECISION in another.
#
# There is no SHARE kind and no FLAG kind. Both existed for columns this table
# no longer carries -- the spending shares, which are derivable from the
# category amounts and the total, and the carried-forward flag, which is a
# diagnostic. A kind with no column is drift waiting to happen. That rule is
# also why IDENTIFIER means "uuid identifier" rather than there being a separate
# UUID kind alongside it: user_id is IDENTIFIER's only column, so splitting the
# kind would leave one of the two halves empty.
#
# IDENTIFIER is the one kind whose two maps disagree, and the disagreement is
# deliberate. Spark has no uuid type, so the frame carries a string; Postgres
# gets UUID, and the conversion happens at the JDBC boundary -- see the
# stringtype note in src/db/settings.py. Making the Spark side match the
# Postgres side is not possible, and making the Postgres side match Spark is
# what this change undid.
MONEY, COUNT, DATE, IDENTIFIER = "MONEY", "COUNT", "DATE", "IDENTIFIER"

_SPARK = {
    MONEY: "double",
    COUNT: "int",
    DATE: "date",
    IDENTIFIER: "string",
}

_POSTGRES = {
    MONEY: "NUMERIC(18, 4)",
    COUNT: "INTEGER",
    DATE: "DATE",
    IDENTIFIER: "UUID",
}

# frozen means once created, column cannot be modified
@dataclass(frozen=True)
class Column:
    """
    One column of the feature table.

    :param name: Its name, identical in the frame and in Postgres.
    :param kind: Logical kind, which fixes the type in both.
    :param known_at: When the value becomes knowable.
    :param note: Why it is knowable then. Carried into the manifest so the
        point-in-time claim travels with the artifact.
    """

    name: str
    kind: str
    known_at: str
    note: str

    @property
    def spark_type(self) -> str:
        """:returns: The Spark type this column is cast to."""
        return _SPARK[self.kind]

    @property
    def postgres_type(self) -> str:
        """:returns: The Postgres type this column is declared as."""
        return _POSTGRES[self.kind]


def _balance_columns() -> list[Column]:
    """:returns: The lagged and rolling balance-history columns."""
    lagged = [
        Column(
            f"prev_{lag}m_closing_balance_usd",
            MONEY,
            BEFORE_MONTH,
            f"user closing balance {lag} month(s) before this row's month",
        )
        for lag in (1, 2, 3)
    ]
    return lagged + [
        Column(
            "roll3_mean_closing_balance_usd",
            MONEY,
            BEFORE_MONTH,
            "mean closing balance over the 3 months before this row's month",
        ),
        Column(
            "roll3_std_closing_balance_usd",
            MONEY,
            BEFORE_MONTH,
            "sample std of the same 3 months; null under 3 observations",
        ),
        Column(
            "delta_prev_1m_2m_closing_balance_usd",
            MONEY,
            BEFORE_MONTH,
            "prev_1m minus prev_2m; both precede this row's month",
        ),
    ]


def _flow_columns() -> list[Column]:
    """:returns: The money-in and money-out columns."""
    return [
        Column(
            "prev_1m_total_credited_usd",
            MONEY,
            BEFORE_MONTH,
            "positive magnitude credited in the preceding month",
        ),
        Column(
            "prev_1m_total_debited_usd",
            MONEY,
            BEFORE_MONTH,
            "positive magnitude debited in the preceding month",
        ),
        Column(
            "prev_1m_net_flow_usd",
            MONEY,
            BEFORE_MONTH,
            "credited minus debited, preceding month",
        ),
        Column(
            "roll3_mean_total_credited_usd",
            MONEY,
            BEFORE_MONTH,
            "mean monthly credit over the 3 preceding months",
        ),
        Column(
            "roll3_mean_total_debited_usd",
            MONEY,
            BEFORE_MONTH,
            "mean monthly debit over the 3 preceding months",
        ),
        Column(
            "roll3_mean_net_flow_usd",
            MONEY,
            BEFORE_MONTH,
            "mean monthly net flow over the 3 preceding months",
        ),
    ]


def _activity_columns() -> list[Column]:
    """
    :returns: The activity columns, including the point-in-time account
        count.
    """
    return [
        Column(
            "prev_1m_txn_count",
            COUNT,
            BEFORE_MONTH,
            "transactions in the preceding month",
        ),
        Column(
            "prev_1m_distinct_merchants",
            COUNT,
            BEFORE_MONTH,
            "distinct counterparties in the preceding month, internal "
            "descriptors excluded",
        ),
        Column(
            "accounts_held",
            COUNT,
            BEFORE_MONTH,
            "accounts whose first transaction is strictly before this row's "
            "month; never a count over all time",
        ),
    ]


def _calendar_columns() -> list[Column]:
    """:returns: The calendar columns, which read month M legitimately."""
    return [
        Column(
            "month_of_year",
            COUNT,
            CALENDAR,
            "1-12 for this row's month; fixed before the month begins",
        ),
        Column(
            "days_in_month",
            COUNT,
            CALENDAR,
            "28-31 for this row's month; fixed before the month begins",
        ),
    ]


def _spending_columns(categories: tuple[str, ...]) -> list[Column]:
    """
    Total spend, then one amount per category. Amounts only.

    :param categories: The spending vocabulary, in display order.
    :returns: The spending columns.
    """
    return [
        Column(
            "prev_1m_total_spend_usd",
            MONEY,
            BEFORE_MONTH,
            "spend-eligible debits in the preceding month; the denominator "
            "for any share Stage 4 chooses to derive",
        )
    ] + [
        Column(
            f"prev_1m_spend_{category}_usd",
            MONEY,
            BEFORE_MONTH,
            f"{category} spend in the preceding month",
        )
        for category in categories
    ]


def columns(categories: tuple[str, ...]) -> tuple[Column, ...]:
    """
    The whole feature table, in output order.

    :param categories: The spending vocabulary, in display order, as the rule
        tables declare it. Passed in rather than imported so this file has no
        opinion about what the categories are.
    :returns: Every column, keys first and the target last.
    :raises ConfigError: If two columns share a name.
    """
    declared = [
        Column("user_id", IDENTIFIER, KEY_COLUMN, "grain"),
        Column("month", DATE, KEY_COLUMN, "grain; first day of the month"),
        *_balance_columns(),
        *_flow_columns(),
        *_activity_columns(),
        *_calendar_columns(),
        *_spending_columns(categories),
        Column(
            "target_closing_balance_usd",
            MONEY,
            TARGET,
            "user closing balance at the end of this row's month; the label, "
            "never an input",
        ),
    ]

    seen: set[str] = set()
    for column in declared:
        if column.name in seen:
            raise ConfigError(f"duplicate feature column: {column.name}")
        seen.add(column.name)

    return tuple(declared)


def names(categories: tuple[str, ...]) -> list[str]:
    """
    :param categories: The spending vocabulary, in display order.
    :returns: Every column name, in output order.
    """
    return [column.name for column in columns(categories)]


def postgres_types(categories: tuple[str, ...]) -> dict[str, str]:
    """
    The declared type of each column, spelled the way Postgres reports it.

    Base type only, lowercased: ``information_schema.columns.data_type`` says
    ``numeric`` where this file says ``NUMERIC(18, 4)``, and the precision is
    already asserted by the column declaration itself. What this is for is
    catching a *different* type, not a different precision.

    :param categories: The spending vocabulary, in display order.
    :returns: Column name to base type, e.g. ``{"user_id": "uuid"}``.
    """
    return {
        column.name: column.postgres_type.split("(")[0].strip().lower()
        for column in columns(categories)
    }


def feature_names(categories: tuple[str, ...]) -> list[str]:
    """
    
    :param categories: The spending vocabulary, in display order.
    :returns: The columns a model may read: everything but the keys and the
        target.
    """
    return [
        column.name
        for column in columns(categories)
        if column.known_at in (BEFORE_MONTH, CALENDAR)
    ]


def select(frame, categories: tuple[str, ...]):
    """
    Projects a built frame down to the declared columns, in order and typed.

    This is where the diagnostics leave. They ride the internal frames all the
    way to here - lagged like every other fact, so they never become a back
    door to month M - and are dropped by not being selected.

    :param frame: The assembled feature frame.
    :param categories: The spending vocabulary, in display order.
    :returns: The same rows, carrying exactly the declared columns.
    :raises ConfigError: If a declared column is absent.
    """
    from pyspark.sql import functions as F

    present = set(frame.columns)
    missing = [name for name in names(categories) if name not in present]
    if missing:
        raise ConfigError(
            f"{TABLE} is missing {len(missing)} declared column(s): "
            f"{', '.join(missing)}"
        )
    # selects only declared columns, forces the declared type, 
    # and orders them as declared. 
    return frame.select(
        *[
            F.col(column.name).cast(column.spark_type).alias(column.name)
            for column in columns(categories)
        ]
    )


def verify(frame, categories: tuple[str, ...]) -> None:
    """
    Checks a projected frame against this declaration before anything is
    written. In other terms, no column enters the final table unless it 
    has explicitly declared point-in-time semantics.

    :param frame: The projected feature frame.
    :param categories: The spending vocabulary, in display order.
    :raises ConfigError: If a declared column is missing, or the frame carries
        one this file does not declare -- an undeclared column has no stated
        ``known_at``, which is exactly the state the point-in-time rule
        forbids, and is how a diagnostic would leak back into the table.
    """
    declared = names(categories)
    present = list(frame.columns)

    missing = [name for name in declared if name not in present]
    if missing:
        raise ConfigError(
            f"{TABLE} is missing {len(missing)} declared column(s): "
            f"{', '.join(missing)}"
        )

    undeclared = [name for name in present if name not in declared]
    if undeclared:
        raise ConfigError(
            f"{TABLE} carries {len(undeclared)} column(s) with no declared "
            f"known_at: {', '.join(undeclared)}. Declare them in "
            f"features/contract.py or drop them before writing."
        )


def create_table(categories: tuple[str, ...], table: str = TABLE) -> str:
    """
    Generated from the same declaration the frame uses, because a hand-written
    DDL beside a generated schema is a drift waiting to happen.

    :param categories: The spending vocabulary, in display order.
    :param table: Name of the table to create.
    :returns: An idempotent ``CREATE TABLE`` statement.
    """
    lines = [
        "    {name:<38} {type}{null}".format(
            name=column.name,
            type=column.postgres_type,
            null=" NOT NULL" if column.known_at == KEY_COLUMN else "",
        )
        for column in columns(categories)
    ]
    lines.append("    PRIMARY KEY ({keys})".format(keys=", ".join(KEY)))
    body = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n{body}\n)"


def create_staging(table: str = TABLE, staging: str = STAGING) -> str:
    """
    The bulk-load target the merge reads from.

    UNLOGGED because its contents survive exactly one statement: the merge
    reads it and the next run truncates it. Not TEMP -- Spark's JDBC writer
    opens a connection per partition, and a temporary table created on one of
    them is invisible to the rest.

    ``LIKE`` keeps the column types in one place.

    :param table: The live table to mirror.
    :param staging: Name of the staging table.
    :returns: An idempotent ``CREATE UNLOGGED TABLE`` statement.
    """
    return (
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging} "
        f"(LIKE {table} INCLUDING DEFAULTS)"
    )


def merge_statement(
    categories: tuple[str, ...],
    table: str = TABLE,
    staging: str = STAGING,
) -> str:
    """
    Moves a staged build into the live table.

    Spark's JDBC writer cannot express ``ON CONFLICT``: its modes are append,
    overwrite, ignore and error, and none of those is an upsert. Writing
    straight to the live table would mean choosing between "a second run
    fails on the primary key" and "a second run drops what the first wrote",
    and a feature build needs neither -- rebuilding a month must be a no-op
    where nothing changed.

    :param categories: The spending vocabulary, in display order.
    :param table: The live table.
    :param staging: The staging table, already loaded.
    :returns: The ``INSERT ... ON CONFLICT DO UPDATE`` statement.
    """
    every = names(categories)
    columns_sql = ", ".join(every)
    updates = ", ".join(
        f"{name} = EXCLUDED.{name}" for name in every if name not in KEY
    )
    keys = ", ".join(KEY)

    # DISTINCT ON is not paranoia. ON CONFLICT raises if one statement
    # presents the same key twice, and a build that somehow carried a
    # duplicate user-month would fail naming the constraint rather than the
    # cause.
    return (
        f"INSERT INTO {table} ({columns_sql})\n"
        f"SELECT DISTINCT ON ({keys}) {columns_sql}\n"
        f"  FROM {staging}\n"
        f" ORDER BY {keys}\n"
        f"    ON CONFLICT ({keys}) DO UPDATE SET {updates}"
    )
