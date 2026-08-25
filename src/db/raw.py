"""
The landing table, from Python: putting a row in, and getting one back out.

``sql/raw_schema.sql`` is the table; this is everything that touches it.

    insert    a hand-made or seeded row goes in, and its id comes back
    read      Spark reads the named ids back as the frame the cleaners expect
    mark      the consumer records what it did with them

Two directions and two drivers, on purpose. Inserting is a handful of rows
typed by a person or copied out of a CSV, and psycopg2 does it in one round
trip; reading is the input to a Spark job, and going through psycopg2 would
mean collecting the rows onto the driver and rebuilding a frame from them --
which works for one row and is the wrong shape the moment a batch is a
thousand.

The case shift is the subtle part. The table's columns are lower case, because
Postgres folds unquoted identifiers and quoting twenty-two names in every
hand-written query is a tax with no payer. Every cleaning stage looks for the
source's own upper-case spelling, because that is what the CSV header says and
what ``spark_setup.read_csv`` therefore produces. So ``read`` aliases on the
way out, in the same direction and for the same reason ``src/db/contract.py``
lowercases on the way in -- the SQL side is lower, the frame side is upper,
and exactly one place does each translation.
"""

from src.db.settings import Database, connect

# The table this module owns. Named here rather than passed in, for the reason
# ``contract.TABLE`` gives: two callers disagreeing about which table they mean
# is a bug and not a setting.
TABLE = "raw_transactions"

# The event key, and the only thing that travels on Kafka.
ID = "id"

# The frame-side name for it. Suffixed rather than plain ``ID`` because the
# frame it rides on is otherwise the source's own columns, and a bare ``ID``
# among them reads like something the source supplied. It is not: it is this
# table's identity for the row, and the consumer needs it on the frame to be
# able to mark the right rows afterwards.
ID_COLUMN = "RAW_ID"

# The source's 22 columns, in the order the extract's header states them and
# in the spelling the cleaners use. The lower-case SQL names are derived from
# these rather than listed twice -- one list is one place for the next column
# to be forgotten in, and ``tests/test_db_raw.py`` checks this one against both
# the DDL and the CSV header.
SOURCE_COLUMNS = (
    "USER_ID",
    "ACCOUNT_ID",
    "TXN_ID",
    "TXN_SEQ",
    "TXN_DATE_TIME",
    "SETTLE_DATE",
    "TXN_AMOUNT",
    "TXN_CCY",
    "BILLING_AMOUNT",
    "BILLING_CURRENCY",
    "FX_RATE",
    "RUNNING_BALANCE",
    "MERCHANT_NAME",
    "MCC_CODE",
    "MERCHANT_COUNTRY",
    "MERCHANT_CITY",
    "PROCESSING_CODE",
    "PROCESSING_TYPE",
    "AUTH_CODE",
    "INTEREST_RATE_INDEX",
    "INFLATION_INDEX",
    "IS_HOLIDAY_MONTH",
)

# What the status column may hold. Repeated from the DDL's CHECK rather than
# read out of it: a typo'd status would otherwise be caught by the database at
# 3am instead of by the caller immediately.
STATUSES = ("PENDING", "CLEANED", "FAILED")

# Errors are stored to be read in a listing, not to be reconstructed from. A
# Spark traceback is several kilobytes and would make `SELECT * FROM
# raw_transactions` unreadable; the whole thing is in the consumer's log.
ERROR_LIMIT = 500


def _sql_names() -> list[str]:
    """:returns: The source columns as the table spells them."""
    return [name.lower() for name in SOURCE_COLUMNS]


def insert(database: Database, rows, source: str | None = None) -> list[int]:
    """
    Inserts raw rows and returns the ids the database allocated.

    The ids are the point. Nothing else in this system knows them -- they are
    generated here, and the dummy producer publishes exactly what this
    function returns, which is why it returns them rather than a count.

    :param database: Where to write.
    :param rows: An iterable of sequences, each holding the 22 values of
        ``SOURCE_COLUMNS`` in that order. A value may be None; a value may be
        an empty string; the two are different and the table keeps them apart,
        because the pipeline's whole business is telling "absent" from "blank".
    :param source: A note about where these came from, stored as-is.
    :returns: The new ids, in the order the rows were given.
    :raises ValueError: If a row is not 22 values long, naming the length it
        had -- a short row would otherwise be an INSERT error naming a column
        count and not a row.
    """
    prepared = []
    for position, row in enumerate(rows):
        values = list(row)
        if len(values) != len(SOURCE_COLUMNS):
            raise ValueError(
                f"row {position} has {len(values)} values, expected "
                f"{len(SOURCE_COLUMNS)}: {SOURCE_COLUMNS}"
            )
        prepared.append(tuple(values) + (source,))

    if not prepared:
        return []

    columns = ", ".join(_sql_names() + ["source"])
    placeholders = ", ".join(["%s"] * (len(SOURCE_COLUMNS) + 1))
    statement = (
        f"INSERT INTO {TABLE} ({columns}) VALUES ({placeholders}) "
        f"RETURNING {ID}"
    )

    ids: list[int] = []
    # One transaction for the batch: a seed that inserted four rows and died
    # on the fifth would otherwise leave four ids nobody was told about, which
    # is precisely the state this pipeline exists to avoid.
    with connect(database) as connection:
        with connection.cursor() as cursor:
            # executemany does not return rows in psycopg2 -- RETURNING is
            # silently dropped and fetchall raises. So the loop is the API,
            # not an oversight; for the scale this path serves (a person
            # inserting rows to watch them flow) the round trips are free.
            for values in prepared:
                cursor.execute(statement, values)
                ids.append(cursor.fetchone()[0])
    return ids


def read(spark, database: Database, ids):
    """
    Reads the named rows back as the frame the cleaning stages expect.

    The frame is deliberately identical in shape to what
    ``spark_setup.read_csv`` produces -- the same 22 columns, same names, same
    order, every one a string -- plus ``RAW_ID`` at the end. That is what lets
    the consumer hand it straight to ``src.spark.pipeline.run`` with no
    adapter: the stages cannot tell whether the rows came from a file or a
    table, which is the property that makes the streaming path and the batch
    path the same pipeline rather than two that resemble each other.

    :param spark: An active session.
    :param database: Where to read from.
    :param ids: The row ids to fetch.
    :returns: A Spark DataFrame, ordered by id.
    :raises ValueError: If no ids are given, or one is not an integer. Checked
        rather than trusted because these arrive from a Kafka message, and the
        ids are interpolated into SQL below -- see the note there.
    """
    wanted = _as_ids(ids)
    if not wanted:
        raise ValueError("no ids to read")

    # Interpolated rather than parameterised, because this is not a psycopg2
    # query: Spark's JDBC reader takes a *table expression* as a string and
    # offers no placeholder binding for it. That is safe here only because
    # every id has been through int() above -- the conversion is the
    # sanitisation, and removing it would turn a Kafka message into SQL.
    listed = ", ".join(str(i) for i in wanted)
    # NULLIF is what makes this frame the same frame the CSV reader produces,
    # and it is not cosmetic. ``read_csv`` sets ``nullValue=""``, so an empty
    # field in the extract reaches the stages as NULL -- and the `missing`
    # stage counts NULLs. Read without this, a row whose merchant city is
    # blank arrives as the empty string, is not null, is therefore not
    # counted, and the same transaction cleaned through the file and through
    # the table produces two different reports. The difference would show up
    # as a metric quietly reading zero.
    #
    # The cost is that a deliberate empty string cannot be told from an absent
    # value on the frame side. That is not a loss: ``nullValue=""`` imposes
    # exactly the same limit on the batch path, and matching it is the point.
    # The table still keeps the two apart, which is what
    # sql/raw_schema.sql promises and what a person querying it will see.
    selection = ", ".join(
        [f'NULLIF({name.lower()}, \'\') AS "{name}"' for name in SOURCE_COLUMNS]
        + [f'{ID} AS "{ID_COLUMN}"']
    )
    query = (
        f"(SELECT {selection} FROM {TABLE} "
        f" WHERE {ID} IN ({listed}) ORDER BY {ID}) AS raw_batch"
    )

    return spark.read.jdbc(
        url=database.jdbc_url,
        table=query,
        properties=database.jdbc_properties,
    )


def mark(database: Database, ids, status: str, error: str | None = None) -> int:
    """
    Records what the consumer did with these rows.

    :param database: Where the table is.
    :param ids: The rows to update.
    :param status: One of ``STATUSES``.
    :param error: The failure, when there was one. Truncated to
        ``ERROR_LIMIT``; the full traceback belongs in the log.
    :returns: Rows updated, which is fewer than the ids given when one of them
        does not exist -- worth returning rather than asserting, because a
        consumer told about a row someone deleted should log that and carry on
        rather than stop.
    :raises ValueError: On an unknown status, listing the ones there are.
    """
    if status not in STATUSES:
        raise ValueError(
            f"unknown status {status!r}, expected one of {STATUSES}"
        )

    wanted = _as_ids(ids)
    if not wanted:
        return 0

    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE {TABLE} "
                f"   SET status = %s, processed_at = now(), last_error = %s "
                f" WHERE {ID} = ANY(%s)",
                (status, error[:ERROR_LIMIT] if error else None, wanted),
            )
            return cursor.rowcount


def fetch(database: Database, ids) -> list[tuple]:
    """
    The same rows through psycopg2, for a caller with no Spark session --
    a test, or a person at a prompt.

    :param database: Where to read from.
    :param ids: The rows to fetch.
    :returns: One tuple per row: the id, then the 22 source values, then the
        status.
    """
    wanted = _as_ids(ids)
    if not wanted:
        return []

    columns = ", ".join([ID] + _sql_names() + ["status"])
    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {columns} FROM {TABLE} "
                f" WHERE {ID} = ANY(%s) ORDER BY {ID}",
                (wanted,),
            )
            return cursor.fetchall()


def pending_ids(database: Database, limit: int = 100) -> list[int]:
    """
    :param database: Where to look.
    :param limit: Most ids to return.
    :returns: Ids the consumer has not reported on, oldest first. For the
        dummy producer's "emit whatever is outstanding" mode, and for a person
        asking what got missed while the consumer was down. NOT how the
        consumer finds its work -- Kafka is what tells it that; see the note
        on the status column in sql/raw_schema.sql.
    """
    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {ID} FROM {TABLE} WHERE status = 'PENDING' "
                f" ORDER BY {ID} LIMIT %s",
                (int(limit),),
            )
            return [row[0] for row in cursor.fetchall()]


def _as_ids(ids) -> list[int]:
    """
    :param ids: Ids from anywhere -- a command line, a Kafka payload, a list.
    :returns: Them as ints, deduplicated, in ascending order.
    :raises ValueError: If one is not a whole number, naming it. This is the
        boundary where an id stops being text from outside and starts being
        something interpolated into SQL, so the conversion is load-bearing
        rather than tidy.
    """
    if isinstance(ids, (str, bytes, int)):
        ids = [ids]

    out = set()
    for value in ids:
        if isinstance(value, bool):
            raise ValueError(f"{value!r} is not a row id")
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            raise ValueError(f"{value!r} is not a row id") from None
    return sorted(out)


def existing(database: Database, ids) -> list[int]:
    """
    Which of these ids are actually in the table.

    One small query, and it earns its round trip: the alternative is
    discovering that a message named a row nobody has after eleven cleaning
    stages have run over an empty frame. A consumer that is told about a
    deleted row should say so in a second, not in a minute.

    :param database: Where to look.
    :param ids: Ids to check.
    :returns: Those that exist, ascending.
    """
    wanted = _as_ids(ids)
    if not wanted:
        return []

    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {ID} FROM {TABLE} WHERE {ID} = ANY(%s) ORDER BY {ID}",
                (wanted,),
            )
            return [row[0] for row in cursor.fetchall()]
