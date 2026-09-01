"""
The write path against a real Postgres: does a batch land, and does re-running
it change anything?

Marked ``db`` and skipped when the container is down. A small hand-built frame
rather than the pipeline's output on purpose -- what is under test here is the
sink, and running eleven cleaning stages first would make a JDBC failure take
ten minutes to reproduce and would fail this file whenever a cleaner broke.
Whether the *pipeline* still produces what the table needs is
``test_db_contract.py``, and it answers in milliseconds.

Every row written carries this run's own sync_job_id and is deleted by it
afterwards, so the tests can run against the same database the pipeline uses
without a teardown that could take real rows with it.
"""

import datetime as dt
import uuid
from decimal import Decimal

import pytest

from src.db import contract, migrate
from src.db import settings as db_settings

pytestmark = [pytest.mark.db, pytest.mark.spark]

# Shaped like the real thing -- the column types and the CHECK constraints
# reject anything that is not -- but unmistakably synthetic in the primary key,
# so a stray row left behind by a killed run is identifiable rather than
# mistaken for data.
#
# The account and user ids can no longer carry that marker: they are UUID
# columns, and `USR-T1` is not a uuid. They are fixed literals rather than
# uuid4() so the rows stay reproducible between runs, which is what lets the
# idempotence test below assert on them. TXN_ID_CLEANED is still TEXT -- see
# the note in sql/schema.sql -- so it keeps the marker for both of them.
ACC_T1 = "aaaaaaaa-0000-4000-8000-000000000001"
ACC_T2 = "aaaaaaaa-0000-4000-8000-000000000002"
USR_T1 = "bbbbbbbb-0000-4000-8000-000000000001"
USR_T2 = "bbbbbbbb-0000-4000-8000-000000000002"

ROWS = [
    ("test-row-0001", "1", ACC_T1, USR_T1, "USD", "LB", "00", "5411"),
    ("test-row-0002", "2", ACC_T1, USR_T1, "USD", "MA", "26", "6012"),
    ("test-row-0003", "3", ACC_T2, USR_T2, "EUR", "LB", "00", "4814"),
]


def frame_for(spark, rows, amount: float = 10.5):
    """
    :returns: A Spark frame carrying exactly the columns ``contract.project``
        reads, with the types the real cleaned frame has -- TXN_SEQ,
        BILLING_AMOUNT and FX_RATE as text, because no stage parses them, and
        the settlement date as a timestamp, because its parser returns one.
    """
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("TXN_ID_CLEANED", T.StringType()),
        T.StructField("TXN_SEQ", T.StringType()),
        T.StructField("ACCOUNT_ID", T.StringType()),
        T.StructField("USER_ID", T.StringType()),
        T.StructField("TXN_TS", T.TimestampType()),
        T.StructField("SETTLE_DATE_CLEANED", T.TimestampType()),
        T.StructField("TXN_AMOUNT_CLEANED", T.DoubleType()),
        T.StructField("TXN_CCY", T.StringType()),
        T.StructField("BILLING_AMOUNT", T.StringType()),
        T.StructField("BILLING_CURRENCY", T.StringType()),
        T.StructField("FX_RATE", T.StringType()),
        T.StructField("RUNNING_BALANCE_FILLED", T.DoubleType()),
        T.StructField("RUNNING_BALANCE_STATUS", T.StringType()),
        T.StructField("RUNNING_BALANCE_CURRENCY", T.StringType()),
        T.StructField("RUNNING_BALANCE_NORMALIZED", T.DoubleType()),
        T.StructField("RUNNING_BALANCE_DISCREPANCY", T.DoubleType()),
        T.StructField("MERCHANT_NAME_CLEANED", T.StringType()),
        T.StructField("MERCHANT_CITY_CLEANED", T.StringType()),
        T.StructField("MERCHANT_COUNTRY_CLEANED", T.StringType()),
        T.StructField("PROCESSING_CODE_CLEANED", T.StringType()),
        T.StructField("PROCESSING_TYPE_CLEANED", T.StringType()),
        T.StructField("MCC_CODE_CLEANED", T.StringType()),
        T.StructField("AUTH_CODE", T.StringType()),
        T.StructField("INTEREST_RATE_INDEX_CLEANED", T.DoubleType()),
        T.StructField("INFLATION_INDEX_CLEANED", T.DoubleType()),
        T.StructField("IS_HOLIDAY_MONTH_CLEANED", T.BooleanType()),
    ])

    built = []
    for key, seq, account, user, ccy, country, code, mcc in rows:
        built.append((
            key, seq, account, user,
            dt.datetime(2024, 3, 14, 15, 9, 26),
            # 15:09 on the settlement timestamp, so the cast to DATE is
            # observably dropping a time rather than trivially preserving one.
            dt.datetime(2024, 3, 16, 15, 9, 26),
            amount, ccy, "99.9999", ccy, "1.2345678901",
            # Row 2 is the UNAVAILABLE case, and the pairing is not optional:
            # the table's CHECK rejects a null balance under any other status
            # and a stated one under this. The row exercises the constraint as
            # much as it exercises the writer.
            None if seq == "2" else -1234.5,
            "UNAVAILABLE" if seq == "2" else "OBSERVED",
            None if seq == "2" else ccy,
            None if seq == "2" else -1234.5,
            None,
            "TEST MERCHANT", "BEIRUT", country, code, "PURCHASE", mcc,
            "A1B2C3", 4.25, 2.5, False,
        ))
    return spark.createDataFrame(built, schema)


@pytest.fixture(scope="module")
def database():
    """
    :returns: The connection settings, after checking something answers on the
        port. Skips rather than fails: a stopped container is a setup
        condition with its own diagnostic.
    """
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
def job(database):
    """
    :returns: A sync_job_id unique to this test, whose rows are removed
        afterwards whether the test passed or not.
    """
    import psycopg2

    identifier = str(uuid.uuid4())
    yield identifier
    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {contract.TABLE} WHERE {contract.SYNC_JOB} = %s",
                (identifier,),
            )


def rows_for(database, job: str) -> list[tuple]:
    """:returns: Every row this job wrote, keyed and ordered for comparison."""
    import psycopg2

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT txn_id_cleaned, txn_seq, txn_amount_cleaned, "
                f"       settle_date_cleaned, fx_rate, running_balance_filled, "
                f"       running_balance_status "
                f"  FROM {contract.TABLE} "
                f" WHERE {contract.SYNC_JOB} = %s ORDER BY txn_id_cleaned",
                (job,),
            )
            return cursor.fetchall()


def test_a_batch_lands_with_its_types_converted(spark, database, job):
    """
    The five columns the contract exists for, checked as the database sees
    them: text into BIGINT and NUMERIC, and a timestamp into DATE.
    """
    from src.db import writer

    written = writer.write(frame_for(spark, ROWS), database, job)
    assert written == len(ROWS)

    rows = rows_for(database, job)
    assert len(rows) == len(ROWS)

    key, seq, amount, settle, fx, balance, status = rows[0]
    assert key == "test-row-0001"
    assert seq == 1, "TXN_SEQ arrived as text and must land as BIGINT"
    assert amount == Decimal("10.5000")
    assert settle == dt.date(2024, 3, 16), "the time part must not reach a DATE"
    assert fx == Decimal("1.2345678901"), "NUMERIC(20,10) must keep ten places"
    assert balance == Decimal("-1234.5000")
    assert status == "OBSERVED", "the status must travel with the figure"


def test_an_unavailable_balance_stays_null(spark, database, job):
    """
    The balance stage states a figure on every row it can reach an anchor
    from. Where it cannot, the null is the claim being made -- and a writer
    that turned it into a zero would be asserting a balance nobody has any
    evidence for. The status beside it says which case this is.
    """
    from src.db import writer

    writer.write(frame_for(spark, ROWS), database, job)

    balances = {
        key: (balance, status)
        for key, _, _, _, _, balance, status in rows_for(database, job)
    }
    assert balances["test-row-0002"] == (None, "UNAVAILABLE")


def test_running_the_same_load_twice_changes_nothing(spark, database, job):
    """
    The property the whole staging-and-merge shape exists for. A consumer will
    re-deliver and a backfill will overlap, so a second run of one load has to
    be a no-op rather than a duplicate-key error or a second copy.
    """
    from src.db import writer

    writer.write(frame_for(spark, ROWS), database, job)
    first = rows_for(database, job)

    writer.write(frame_for(spark, ROWS), database, job)
    second = rows_for(database, job)

    assert first == second
    assert len(second) == len(ROWS)


def test_a_re_run_with_corrected_values_updates_in_place(spark, database, job):
    """
    The other half of idempotent: not merely "does not duplicate", but "the
    newer value wins". An upsert that only inserted would leave a corrected
    load reporting success and changing nothing.
    """
    from src.db import writer

    writer.write(frame_for(spark, ROWS, amount=10.5), database, job)
    writer.write(frame_for(spark, ROWS, amount=99.25), database, job)

    rows = rows_for(database, job)
    assert len(rows) == len(ROWS)
    assert {row[2] for row in rows} == {Decimal("99.2500")}


def test_the_staging_table_does_not_accumulate(spark, database, job):
    """
    Staging is truncated before each load, not after. Cleanup that only runs
    on the success path does not run, and a run that died mid-write would
    otherwise leave its rows to be merged by the next one.
    """
    import psycopg2

    from src.db import writer

    writer.write(frame_for(spark, ROWS), database, job)
    writer.write(frame_for(spark, ROWS), database, job)

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {migrate.STAGING}")
            staged = cursor.fetchone()[0]

    assert staged == len(ROWS)
