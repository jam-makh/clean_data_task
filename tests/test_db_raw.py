"""
The landing table: does the DDL, the column list and the source file still
agree, and does a row survive the round trip?

Two halves, and the split is the one ``test_db_contract.py`` draws. Most of
what can go wrong here is a disagreement between three static texts --
``sql/raw_schema.sql``, ``raw.SOURCE_COLUMNS`` and the extract's own header --
and a check that needs a container is a check that gets skipped on the machine
where it would have caught something. Those tests need nothing running.

The rest genuinely needs Postgres and is marked ``db``, which skips when the
container is down.

The failure this file exists to catch is quiet by construction. Every one of
the 22 columns is TEXT, so a list that drifted by one would still INSERT
without complaint -- putting the settlement date in the amount column, and
handing the cleaners a frame that is wrong in a way no type error announces.
"""

import re
from pathlib import Path

import pytest

from src.db import raw
from src.db import settings as db_settings
from scripts import seed_raw

SCHEMA = Path("sql/raw_schema.sql")
SOURCE = Path("data/raw/forecast_balance_data.csv")

# Columns the table has that the source does not: the identity, the
# provenance note, and the consumer's record of what it did. Listed rather
# than pattern-matched, so a new housekeeping column has to be added here on
# purpose and cannot slip in as an unnoticed extra.
HOUSEKEEPING = [
    "id",
    "source",
    "ingested_at",
    "status",
    "processed_at",
    "last_error",
]


def schema_columns() -> list[str]:
    """
    :returns: The table's columns in declaration order, read from the DDL
        rather than from a running database -- the file is the thing under
        review, and a database could be out of date with it.
    """
    text = SCHEMA.read_text(encoding="utf-8")
    body = text.split(f"CREATE TABLE IF NOT EXISTS {raw.TABLE} (", 1)[1]
    body = body.split("\n);", 1)[0]

    columns = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        match = re.match(r"([a-z_]+)\s+(TEXT|BIGINT|TIMESTAMPTZ)", line)
        if match:
            columns.append(match.group(1))
    return columns


def test_the_schema_parses():
    """
    The parser above is load-bearing for the tests below, so it gets its own
    assertion: a regex that quietly matched nothing would make this whole file
    pass while comparing two empty lists.
    """
    columns = schema_columns()

    assert len(columns) == len(raw.SOURCE_COLUMNS) + len(HOUSEKEEPING), (
        f"parsed {len(columns)} columns from {SCHEMA}: {columns}"
    )


def test_the_table_declares_exactly_the_source_columns():
    """
    The list in Python and the list in SQL are the same list, in the same
    order. Order matters and is not merely tidy: ``raw.insert`` builds a
    positional INSERT from ``SOURCE_COLUMNS``, so a table whose columns are
    the same set in a different order would load every value into the wrong
    one, silently, because they are all TEXT.
    """
    columns = schema_columns()
    source_side = [name for name in columns if name not in HOUSEKEEPING]

    assert source_side == [name.lower() for name in raw.SOURCE_COLUMNS]


def test_every_source_column_is_text():
    """
    The landing table's entire contract. A NUMERIC column here would reject
    the unparseable value that requirement 2 asks the pipeline to *count*, and
    the row would never arrive for a stage to mark -- the cleaning report would
    be silent about a row that was rejected before it existed.
    """
    text = SCHEMA.read_text(encoding="utf-8")
    body = text.split(f"CREATE TABLE IF NOT EXISTS {raw.TABLE} (", 1)[1]
    body = body.split("\n);", 1)[0]

    for name in raw.SOURCE_COLUMNS:
        declaration = re.search(rf"^\s*{name.lower()}\s+(\w+)", body, re.M)
        assert declaration, f"{name.lower()} is not declared in {SCHEMA}"
        assert declaration.group(1) == "TEXT", (
            f"{name.lower()} is {declaration.group(1)}, which would coerce on "
            f"the way in; the landing table must not clean anything"
        )


def test_the_column_list_matches_the_extract_header():
    """
    The third text in the agreement. ``SOURCE_COLUMNS`` is what the frame is
    built from and the CSV header is what the batch pipeline reads, so a
    source that gained or reordered a column has to fail here rather than
    produce a streaming frame shaped differently from the batch one.
    """
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")

    from src.spark.spark_setup import header_of

    assert header_of(SOURCE) == list(raw.SOURCE_COLUMNS)


def test_the_id_column_is_numeric_and_generated():
    """
    The event key. Numeric because that is what travels on Kafka, and
    generated because nothing outside the database is in a position to
    allocate one.
    """
    text = SCHEMA.read_text(encoding="utf-8")

    assert re.search(
        r"id\s+BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY", text
    )


def test_the_statuses_in_python_are_the_statuses_in_sql():
    """
    ``raw.mark`` rejects an unknown status before the database gets a chance
    to, which is only an improvement while the two lists agree. If they drift,
    the Python check starts permitting something the CHECK constraint refuses
    and the failure moves from the caller to a stack trace.
    """
    text = SCHEMA.read_text(encoding="utf-8")
    declared = re.search(r"status IN \(([^)]+)\)", text).group(1)
    names = tuple(re.findall(r"'([A-Z]+)'", declared))

    assert names == raw.STATUSES


# ---------------------------------------------------------------------------
# Argument handling -- the boundary where a Kafka payload becomes SQL
# ---------------------------------------------------------------------------


def test_ids_are_normalised_to_ints():
    """
    Ids arrive as text from a command line and as JSON from a message, and
    leave as integers interpolated into a SELECT. That conversion is the
    sanitisation, so it is tested as such rather than assumed.
    """
    assert raw._as_ids(["3", 1, "2", 3]) == [1, 2, 3]
    assert raw._as_ids(7) == [7]
    assert raw._as_ids([]) == []


@pytest.mark.parametrize(
    "value", ["1; DROP TABLE raw_transactions", "", None, "1.5", True, "abc"]
)
def test_a_value_that_is_not_a_row_id_is_refused(value):
    """
    Including ``True``, which ``int()`` would happily turn into 1 -- a boolean
    reaching here means a caller passed the wrong thing, and quietly reading
    it as row 1 is worse than failing.
    """
    with pytest.raises(ValueError, match="not a row id"):
        raw._as_ids([value])


def test_a_short_row_is_refused_by_name():
    """
    A row of the wrong length would otherwise be an INSERT error naming a
    column count, which does not say which row or how long it was.
    """
    with pytest.raises(ValueError, match="expected 22"):
        raw.insert(db_settings.load(), [("only", "three", "values")])


def test_an_unknown_status_is_refused():
    with pytest.raises(ValueError, match="unknown status"):
        raw.mark(db_settings.load(), [1], "DONE")


def test_reading_no_ids_is_an_error_rather_than_an_empty_frame():
    """
    An empty id list means the caller lost track of what it was processing.
    Returning an empty frame would let that pass as "nothing to clean", which
    is the same thing a genuinely empty batch looks like.
    """
    with pytest.raises(ValueError, match="no ids"):
        raw.read(None, db_settings.load(), [])


# ---------------------------------------------------------------------------
# Reading rows out of the extract
# ---------------------------------------------------------------------------


def test_rows_are_cut_from_the_extract_in_column_order():
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")

    rows = seed_raw.rows_from_csv(SOURCE, count=3)

    assert len(rows) == 3
    for row in rows:
        assert len(row) == len(raw.SOURCE_COLUMNS)
    # The first data row of the extract, spot-checked at three positions, so a
    # reader that dropped or shifted a column fails here.
    first = rows[0]
    assert first[raw.SOURCE_COLUMNS.index("TXN_SEQ")] == "77037"
    assert first[raw.SOURCE_COLUMNS.index("TXN_CCY")] == "USD"
    assert first[raw.SOURCE_COLUMNS.index("IS_HOLIDAY_MONTH")] == "False"


def test_the_offset_moves_to_different_rows():
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")

    first = seed_raw.rows_from_csv(SOURCE, count=1)
    later = seed_raw.rows_from_csv(SOURCE, count=1, offset=10)

    assert first != later


def test_blanks_are_preserved_rather_than_nulled():
    """
    The seeder must not clean. It stores what the file says, blanks included,
    and the blank-to-null mapping that ``read_csv`` performs is applied on the
    way *out* instead -- see ``raw.read`` and the test below it. Doing it here
    would mean the table could no longer show a person what actually arrived.
    """
    if not SOURCE.exists():
        pytest.skip(f"source file not present: {SOURCE}")

    rows = seed_raw.rows_from_csv(SOURCE, count=200)

    assert any("" in row for row in rows), (
        "the extract has blanks; if none survived the read, something is "
        "converting them"
    )
    assert all(value is None or isinstance(value, str) for row in rows
               for value in row)


def test_a_source_whose_header_moved_is_refused(tmp_path):
    """
    A positional read of a reordered file would insert every value one column
    to the left, and every column is TEXT, so nothing downstream would notice.
    """
    swapped = list(raw.SOURCE_COLUMNS)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    path = tmp_path / "moved.csv"
    path.write_text(
        ",".join(swapped) + "\n" + ",".join(["x"] * 22) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not have the columns"):
        seed_raw.rows_from_csv(path, count=1)


# ---------------------------------------------------------------------------
# Against a real database
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def database():
    """
    :returns: The connection settings, after checking something answers on the
        port. Skips rather than fails: a stopped container is a setup
        condition with its own diagnostic.
    """
    from src.db import migrate

    settings = db_settings.load()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        psycopg2.connect(settings.dsn).close()
    except Exception as exc:  # noqa: BLE001 - the message is the point
        pytest.skip(
            f"Postgres not reachable at {settings} ({type(exc).__name__}). "
            f"Run `make verify` -- it names the cause."
        )
    migrate.migrate(settings)
    return settings


@pytest.fixture
def seeded(database):
    """
    :returns: (ids, rows) for two rows inserted for this test, removed
        afterwards whether it passed or not -- so the suite can run against
        the same database the pipeline uses without a teardown that could take
        real rows with it.
    """
    import psycopg2

    import uuid

    # USER_ID, ACCOUNT_ID and TXN_ID carry shape checks, so `user_id-1` will
    # not go in. They get a uuid derived from the same string, which keeps the
    # values distinct per row and per column the way the pattern below does,
    # and keeps them reproducible the way a literal would.
    def value(name: str, n: int) -> str:
        plain = f"{name.lower()}-{n}"
        if name in ("USER_ID", "ACCOUNT_ID", "TXN_ID"):
            return str(uuid.uuid5(uuid.NAMESPACE_OID, plain))
        return plain

    rows = [
        tuple(value(name, n) for name in raw.SOURCE_COLUMNS)
        for n in (1, 2)
    ]
    # One blank and one None among the values, because those are the two
    # states the table exists to keep apart and a round trip that conflated
    # them would be invisible in a test using only non-empty strings. The
    # blank one is USER_ID, which the shape check admits deliberately -- see
    # the note on those checks in sql/raw_schema.sql.
    rows[0] = ("",) + rows[0][1:-1] + (None,)

    ids = raw.insert(database, rows, source="test_db_raw")
    yield ids, rows

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {raw.TABLE} WHERE {raw.ID} = ANY(%s)", (ids,)
            )


@pytest.mark.db
def test_inserting_returns_the_ids_the_database_allocated(seeded):
    """
    The ids are the whole product of this call. Nothing else in the system can
    know them, and the dummy producer publishes exactly what came back.
    """
    ids, rows = seeded

    assert len(ids) == len(rows)
    assert all(isinstance(i, int) for i in ids)
    assert ids == sorted(ids), "ids come back in the order the rows were given"


@pytest.mark.db
def test_a_row_survives_the_round_trip_unchanged(database, seeded):
    """
    Including the blank and the null, which is the point: the landing table
    stores what arrived, and the two kinds of absence stay distinguishable.
    """
    ids, rows = seeded

    fetched = raw.fetch(database, ids)

    assert len(fetched) == 2
    first = fetched[0]
    assert first[0] == ids[0]
    assert tuple(first[1:-1]) == rows[0]
    assert first[1] == "", "an empty string must not arrive as null"
    assert first[-2] is None, "a null must not arrive as an empty string"


@pytest.mark.db
def test_a_new_row_starts_pending(database, seeded):
    """
    The consumer has not seen it yet, and the column says so -- which is what
    makes ``pending_ids`` answer "what got missed" rather than "everything".
    """
    ids, _ = seeded

    statuses = {row[0]: row[-1] for row in raw.fetch(database, ids)}

    assert set(statuses.values()) == {"PENDING"}
    assert set(ids) <= set(raw.pending_ids(database, limit=1000))


@pytest.mark.db
def test_marking_records_what_the_consumer_did(database, seeded):
    ids, _ = seeded

    assert raw.mark(database, ids[:1], "CLEANED") == 1

    statuses = {row[0]: row[-1] for row in raw.fetch(database, ids)}
    assert statuses[ids[0]] == "CLEANED"
    assert statuses[ids[1]] == "PENDING", "marking one row must not move both"
    assert ids[0] not in raw.pending_ids(database, limit=1000)


@pytest.mark.db
def test_a_long_error_is_truncated_rather_than_stored_whole(database, seeded):
    """
    A Spark traceback is kilobytes, and a listing of this table has to stay
    readable. The full text is in the consumer's log.
    """
    import psycopg2

    ids, _ = seeded
    raw.mark(database, ids[:1], "FAILED", error="x" * 5000)

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT last_error FROM {raw.TABLE} WHERE {raw.ID} = %s",
                (ids[0],),
            )
            stored = cursor.fetchone()[0]

    assert len(stored) == raw.ERROR_LIMIT


@pytest.mark.db
@pytest.mark.spark
def test_the_frame_is_the_one_the_csv_reader_would_have_produced(
    spark, database, seeded
):
    """
    The property the whole streaming path rests on: a stage cannot tell
    whether its rows came from a file or from this table.

    Same 22 columns, same names, same order, every one a string, plus RAW_ID.
    And blanks arrive as NULL, because ``read_csv`` sets ``nullValue=""`` and
    the `missing` stage counts NULLs -- a blank that arrived as the empty
    string would go uncounted, and the same transaction would report
    differently through the two paths.
    """
    ids, rows = seeded

    frame = raw.read(spark, database, ids)

    assert frame.columns == list(raw.SOURCE_COLUMNS) + [raw.ID_COLUMN]
    types = dict(frame.dtypes)
    assert {types[name] for name in raw.SOURCE_COLUMNS} == {"string"}
    assert types[raw.ID_COLUMN] == "bigint"

    collected = {row[raw.ID_COLUMN]: row for row in frame.collect()}
    assert set(collected) == set(ids)

    blank_row = collected[ids[0]]
    assert blank_row["USER_ID"] is None, (
        "an empty string in the table must reach the frame as null, the way "
        "an empty field in the CSV does"
    )
    assert blank_row["ACCOUNT_ID"] == rows[0][1], "non-blanks are untouched"


@pytest.mark.db
def test_marking_an_id_that_is_not_there_reports_nothing_updated(database):
    """
    A consumer told about a row someone deleted should log it and carry on,
    which it can only do if it is told rather than raised at.
    """
    assert raw.mark(database, [-1], "CLEANED") == 0
