"""
The whole streaming path, against the real stack: insert, emit, consume,
clean, upsert.

Every piece of this flow is already tested on its own -- the table in
``test_db_raw.py``, the event in ``test_kafka_ingest_events.py``, the loop in
``test_kafka_consumer.py`` with a fake broker, the narration in
``test_stagelog.py``. Each of those runs in milliseconds and each is worth
more per second than this file is.

This one exists because all four can pass while the flow does not. The seams
are where it breaks: a producer publishing to a topic the consumer is not
subscribed to, a frame the stages cannot read because the JDBC reader spelled
its columns differently, a write that fails because ``RAW_ID`` is on the frame
and not in the table. None of those are visible from either side of the seam,
and all of them are fatal.

So it is marked ``kafka``, ``db`` and ``spark``, skips when any of the three is
absent, and is deliberately economical: **two** pipeline runs for the whole
file, because a run costs a minute or more and a suite nobody waits for is a
suite nobody runs. The first is shared by every assertion about what arrives;
the second exists only to prove the property that a redelivery cannot be
tested without repeating -- idempotence.

Reading its own message back
----------------------------

The consumer is handed a client that has been *assigned* the topic's current
end offset rather than one that subscribes. Subscribing would replay the whole
topic from the beginning -- every message every other test in this repo has
ever published -- and each of those is a Spark job. Assignment starts exactly
where this test starts and reads exactly what this test produced, which is the
same reason ``test_kafka_producer.py`` gives for doing it that way.
"""

import time
import uuid

import pytest

from scripts import dummy_producer
from src.db import raw
from src.db import settings as db_settings
from src.kafka import consumer as consumer_module
from src.kafka import settings as kafka_settings

pytestmark = [pytest.mark.kafka, pytest.mark.db, pytest.mark.spark]

# A merchant name wearing its terminal prefix, a city spelled the way the
# source spells it, and a date in the convention the extract mixes with the
# other one. Chosen so that a successful run is visible in the output rather
# than merely reported: if the cleaning did nothing, these three columns come
# back unchanged and the assertions say which one.
DIRTY = {
    "MERCHANT_NAME": "TRM:31659ZAATARWZEIT",
    "MERCHANT_CITY": "BEYRUT",
    "SETTLE_DATE": "03-Jan-22",
    "TXN_DATE_TIME": "2022-01-01 07:11:25",
}


def row_for(txn_id: str) -> tuple:
    """
    :param txn_id: The transaction id, unique to this run.
    :returns: One raw row in ``raw.SOURCE_COLUMNS`` order, dirty in the three
        ways ``DIRTY`` describes and plausible everywhere else.
    """
    values = {
        "USER_ID": str(uuid.uuid4()),
        "ACCOUNT_ID": str(uuid.uuid4()),
        "TXN_ID": txn_id,
        "TXN_SEQ": "77037",
        "TXN_AMOUNT": "-231.37",
        "TXN_CCY": "USD",
        "BILLING_AMOUNT": "-231.37",
        "BILLING_CURRENCY": "USD",
        "FX_RATE": "1.0",
        # Blank, which is the state the balance stage withholds a figure for
        # and the state the landing table has to keep distinguishable.
        "RUNNING_BALANCE": "",
        "MCC_CODE": "5999",
        "MERCHANT_COUNTRY": "LB",
        "PROCESSING_CODE": "0",
        "PROCESSING_TYPE": "PURCHASE",
        "AUTH_CODE": "XEYR19",
        "INTEREST_RATE_INDEX": "4.519",
        "INFLATION_INDEX": "3.22",
        "IS_HOLIDAY_MONTH": "False",
        **DIRTY,
    }
    return tuple(values[name] for name in raw.SOURCE_COLUMNS)


@pytest.fixture(scope="module")
def database():
    """:returns: Connection settings, skipping when Postgres is not up."""
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


@pytest.fixture(scope="module")
def broker():
    """:returns: Broker settings, skipping when nothing answers."""
    pytest.importorskip("confluent_kafka")
    from confluent_kafka.admin import AdminClient

    from src.kafka import producer

    settings = kafka_settings.load()
    try:
        AdminClient({"bootstrap.servers": settings.servers}).list_topics(
            timeout=5
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"broker not reachable at {settings.servers} "
            f"({type(exc).__name__}). Run `make verify`."
        )
    producer.ensure_topic(settings, settings.raw_topic)
    return settings


@pytest.fixture(scope="module")
def session():
    """
    :returns: A Spark session built the way ``consumer.py`` builds its own.

    Deliberately the consumer's own builder rather than the shared ``spark``
    fixture, because the thread count is the difference between this file
    taking two minutes and taking twenty -- ``local[*]`` starts a Python
    worker per core for a batch of one row, which was measured at six times
    the cost of ``local[1]``.

    ``getOrCreate`` means that if some earlier test in the same process
    already built a session, this returns that one and the master is whatever
    it asked for. That is Spark's rule and not something a fixture can fix:
    one JVM per process. Run this file on its own to get the fast path.
    """
    import consumer_parser as entry_point

    try:
        return entry_point._session("1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"could not start Spark ({type(exc).__name__}: {exc}). "
            f"Run `python -m scripts.verify_env`."
        )


@pytest.fixture(scope="module")
def assigned(broker):
    """
    :returns: A consumer assigned to the ingest topic at its current end.

    Reads only what this module publishes. See the note in the module
    docstring: subscribing would replay every message the repo's other tests
    have ever published, and each one is a Spark job.
    """
    from confluent_kafka import Consumer, TopicPartition

    client = Consumer({
        "bootstrap.servers": broker.servers,
        "group.id": f"streaming-test-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    metadata = client.list_topics(broker.raw_topic, timeout=10)
    client.assign([
        TopicPartition(
            broker.raw_topic,
            partition,
            client.get_watermark_offsets(
                TopicPartition(broker.raw_topic, partition), timeout=10
            )[1],
        )
        for partition in metadata.topics[broker.raw_topic].partitions
    ])
    yield client
    client.close()


@pytest.fixture(scope="module")
def flowed(database, broker, session, assigned):
    """
    Runs the whole flow once, and yields what it produced.

    Module-scoped on purpose. Every assertion below is about the same single
    journey, and re-running the pipeline per test would multiply a two-minute
    file by however many questions are worth asking about one row.

    :returns: (row id, txn id, the RunResult-shaped outcome).
    """
    import psycopg2

    txn_id = f"stream-test-{uuid.uuid4()}"
    [row_id] = raw.insert(database, [row_for(txn_id)], source="test_streaming")

    dummy_producer.emit(row_id, broker)

    outcome = consumer_module.handle(
        [row_id],
        spark=session,
        database=database,
        write=True,
        emit=False,
        # Off: the narration is ``test_stagelog.py``'s subject, and counting
        # every stage would add eleven Spark actions to a test about whether
        # the row arrives.
        verbose=False,
    )

    yield row_id, txn_id, outcome

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM cleaned_transactions WHERE txn_id_cleaned = %s",
                (txn_id,),
            )
            cursor.execute(
                f"DELETE FROM {raw.TABLE} WHERE {raw.ID} = %s", (row_id,)
            )


def cleaned_row(database, txn_id):
    """:returns: The cleaned row for this transaction, or None."""
    import psycopg2

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT merchant_name_cleaned, merchant_city_cleaned, "
                "       settle_date_cleaned, txn_amount_cleaned, "
                "       running_balance_filled, sync_job_id, txn_ts "
                "  FROM cleaned_transactions WHERE txn_id_cleaned = %s",
                (txn_id,),
            )
            return cursor.fetchone()


# ---------------------------------------------------------------------------
# The journey
# ---------------------------------------------------------------------------


def test_the_row_reaches_the_cleaned_table(database, flowed):
    """
    The claim the whole feature makes: a row inserted into raw_transactions
    and announced on Kafka ends up in cleaned_transactions without anybody
    running the pipeline by hand.
    """
    _, txn_id, outcome = flowed

    assert outcome.failed == {}, f"the flow reported failures: {outcome.failed}"
    assert cleaned_row(database, txn_id) is not None, (
        "nothing arrived in cleaned_transactions for this transaction"
    )


def test_the_row_was_actually_cleaned(database, flowed):
    """
    Not merely copied. Three columns went in dirty in three different ways --
    a terminal prefix, a source spelling, a date in one of the two conventions
    the extract mixes -- and a pipeline that moved the row without cleaning it
    would pass the test above and fail this one.
    """
    _, txn_id, _ = flowed

    merchant, city, settle, amount, _, _, _ = cleaned_row(database, txn_id)

    assert merchant == "ZAATAR W ZEIT", (
        f"the terminal prefix survived: {merchant!r}"
    )
    assert city == "BEIRUT", f"the city was not normalised: {city!r}"
    assert (settle.year, settle.month, settle.day) == (2022, 1, 3)
    assert float(amount) == -231.37


def test_a_withheld_balance_arrives_as_null(database, flowed):
    """
    The limitation, asserted rather than left to be discovered. A single row
    has no earlier transaction on its account to count from, so the balance
    stage states no figure -- and the column is null rather than zero, because
    a zero would be a number nobody verified.
    """
    _, txn_id, _ = flowed

    balance = cleaned_row(database, txn_id)[4]

    assert balance is None


def test_the_raw_row_is_marked_cleaned(database, flowed):
    """
    Kafka is the queue; this column is the record. It is what lets a person
    see which rows made it through, and what ``--pending`` reads to re-emit
    the ones that did not.
    """
    row_id, _, _ = flowed

    status = raw.fetch(database, [row_id])[0][-1]

    assert status == "CLEANED"
    assert row_id not in raw.pending_ids(database, limit=1000)


def test_the_job_id_ties_the_row_to_its_load(database, flowed):
    """
    ``sync_job_id`` is what answers "which run put this here", and it has to
    satisfy the column's UUID shape check -- which a generated string derived
    from a row id would not, unless it was derived the way src/jobs.py derives
    everything else.
    """
    row_id, txn_id, _ = flowed
    from src.jobs import job_id_from_digest

    stored = cleaned_row(database, txn_id)[5]

    assert stored == job_id_from_digest(f"{raw.TABLE}:{row_id}")


def test_the_raw_id_does_not_leak_into_the_cleaned_table(database, flowed):
    """
    ``raw.read`` puts RAW_ID on the frame so the consumer can mark the right
    rows afterwards. The contract selects the table's columns explicitly, so
    it never reaches the write -- and if that ever changed, the insert would
    fail on an unknown column rather than quietly adding one.
    """
    import psycopg2

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                " WHERE table_name = 'cleaned_transactions'"
            )
            columns = {row[0] for row in cursor.fetchall()}

    assert "raw_id" not in columns


# ---------------------------------------------------------------------------
# Redelivery
# ---------------------------------------------------------------------------


def test_cleaning_the_same_row_again_changes_nothing(database, flowed):
    """
    The property the whole delivery design rests on, and the only one that
    cannot be tested without running the pipeline twice.

    Offsets are committed after Postgres, so a crash redelivers rather than
    skips -- which is only safe because a second pass over the same row writes
    the same values under the same job id. This is that claim, checked.
    """
    row_id, txn_id, _ = flowed
    before = cleaned_row(database, txn_id)

    from src.runner import run_rows

    again = run_rows([row_id], spark=_active(), connection=database)

    assert again.rows_written == 1, (
        "the upsert reports the row it touched, whether inserted or updated"
    )
    after = cleaned_row(database, txn_id)
    # cleaned_at moves -- it means "when the pipeline last wrote this row",
    # and the pipeline did just write it. Everything a reader would call the
    # data is identical.
    assert after == before


def _active():
    """:returns: The session this process already has."""
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession()


# ---------------------------------------------------------------------------
# The seam the fake broker cannot check
# ---------------------------------------------------------------------------


def test_the_consumer_reads_what_the_producer_published(
    database, broker, session, assigned
):
    """
    The one thing a fake client cannot prove: that the topic the producer
    writes to is the topic the consumer reads from, and that a real Kafka
    message decodes into the id that went in.

    No cleaning here -- the id names a row that does not exist, so the flow
    stops at "read the message, look it up, report it missing", which is
    exactly the seam under test and costs no Spark job at all.
    """
    absent = 900_000 + uuid.uuid4().int % 90_000

    dummy_producer.emit(absent, broker)

    # Polled until it turns up rather than once, for two reasons that are both
    # about this being a real broker. The assigned position is where the topic
    # stood when the fixture was built, so anything this module published
    # earlier is still ahead of us in the log and comes out first. And a
    # message that has been acknowledged is not necessarily one the consumer
    # has fetched yet -- a single poll can legitimately return nothing.
    subscription = kafka_settings.load_subscription()
    ids: list = []
    errors: list = []
    deadline = time.monotonic() + 30
    while absent not in ids and time.monotonic() < deadline:
        found, failed = consumer_module.decode_all(
            consumer_module.poll_batch(assigned, subscription, batch_size=10)
        )
        ids.extend(found)
        errors.extend(failed)

    assert errors == [], f"a real message failed to decode: {errors}"
    assert absent in ids, (
        f"published {absent} to {broker.raw_topic} and read back {ids}"
    )

    outcome = consumer_module.handle(
        [absent], spark=session, database=database, verbose=False
    )
    assert absent in outcome.failed
    assert "no row" in outcome.failed[absent]
