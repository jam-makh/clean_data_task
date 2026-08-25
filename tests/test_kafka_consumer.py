"""
The consumer's loop, without a broker and without Spark.

Everything that decides whether this consumer is *correct* -- when it commits,
what it does with a row that will not clean, whether a failed batch blames the
right row -- is loop logic, and none of it needs Kafka or a JVM to test. So the
broker is a fake with a list of messages in it and the cleaning is a function
that can be told to fail.

That substitution is the point rather than a shortcut. The failure this file
exists to catch is "the offset was committed before the row was written", which
against a real broker is a race you would have to lose on purpose to observe,
and here is an assertion about the order of two calls.
"""

import json

import pytest

from src.kafka import consumer as consumer_module
from src.kafka import ingest_events
from src.kafka.settings import Subscription


class FakeMessage:
    """One Kafka message, or an error notice when ``error`` is given."""

    def __init__(self, value: bytes, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


def event(row_id: int) -> FakeMessage:
    """:returns: A message carrying a well-formed ingest event."""
    return FakeMessage(ingest_events.encode(ingest_events.build(row_id)))


class FakeClient:
    """
    A consumer client that hands out prepared messages and records commits.

    ``commits`` holds the length of ``log`` at each commit, which is what
    makes "did it commit before or after the work" an assertion rather than an
    inspection.
    """

    def __init__(self, batches, log):
        self.batches = list(batches)
        self.log = log
        self.commits = []
        self.closed = False
        self.subscribed = None

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        del timeout
        if not self.batches:
            return None
        batch = self.batches[0]
        if not batch:
            self.batches.pop(0)
            return None
        return batch.pop(0)

    def commit(self, asynchronous=True):
        del asynchronous
        self.commits.append(len(self.log))

    def close(self):
        self.closed = True


@pytest.fixture
def log():
    """:returns: A list every fake records into, in order."""
    return []


@pytest.fixture
def cleaner(log, monkeypatch):
    """
    Replaces the cleaning with a recorder, and returns a way to make it fail.

    :returns: A function taking the ids that should raise.
    """
    failing: set = set()

    def fake_clean(ids, **kwargs):
        del kwargs
        if any(i in failing for i in ids):
            log.append(("clean-failed", tuple(ids)))
            raise RuntimeError(f"row {ids} is broken")
        log.append(("cleaned", tuple(ids)))
        return None

    def fake_mark(database, ids, status, error=None):
        del database, error
        log.append(("mark", tuple(ids), status))
        return len(list(ids))

    def fake_existing(database, ids):
        """Every id exists unless a test says otherwise via ``absent``."""
        del database
        return [i for i in ids if i not in absent]

    absent: set = set()

    monkeypatch.setattr(consumer_module, "clean", fake_clean)
    monkeypatch.setattr(consumer_module.raw, "mark", fake_mark)
    monkeypatch.setattr(consumer_module.raw, "existing", fake_existing)

    def configure(broken=(), missing=()):
        """
        :param broken: Ids whose cleaning should raise.
        :param missing: Ids that are not in the table.
        """
        failing.update(broken)
        absent.update(missing)

    return configure


def run(client, log, **kwargs):
    """Drives one pass of the loop against a fake client."""
    del log
    return consumer_module.consume(
        Subscription(batch_size=kwargs.pop("batch_size", 10)),
        spark=object(),
        database=object(),
        client=client,
        once=True,
        write_line=lambda line: None,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def test_ids_are_taken_in_arrival_order():
    ids, errors = consumer_module.decode_all([event(7), event(3), event(9)])

    assert ids == [7, 3, 9]
    assert errors == []


def test_a_repeated_id_in_one_batch_is_cleaned_once():
    """
    The upsert would make a second copy harmless, but the log would say
    "2 rows" about one transaction -- and a log that miscounts is worse than
    one that says less.
    """
    ids, _ = consumer_module.decode_all([event(7), event(7)])

    assert ids == [7]


def test_a_message_that_is_not_an_event_is_reported_not_raised():
    """
    One malformed message must not stop the consumer. It is counted, said out
    loud, and stepped over.
    """
    ids, errors = consumer_module.decode_all(
        [event(7), FakeMessage(b"not json at all"), event(8)]
    )

    assert ids == [7, 8]
    assert len(errors) == 1
    assert "not JSON" in errors[0]


def test_a_completion_event_on_this_topic_is_skipped():
    payload = {"event": "pipeline.run.completed", "version": 1, "id": 1}

    ids, errors = consumer_module.decode_all(
        [FakeMessage(json.dumps(payload).encode("utf-8"))]
    )

    assert ids == []
    assert "Something else is publishing" in errors[0]


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------


def test_a_batch_stops_at_the_size_limit(log):
    client = FakeClient([[event(i) for i in range(10)]], log)

    messages = consumer_module.poll_batch(client, Subscription(), batch_size=3)

    assert len(messages) == 3


def test_an_error_notice_is_not_a_message(log):
    """
    Partition EOF and rebalance notices arrive as messages with an error set.
    They mean "nothing here", and treating one as data is how a consumer ends
    up trying to clean row None.
    """
    client = FakeClient([[FakeMessage(b"", error="PARTITION_EOF")]], log)

    assert consumer_module.poll_batch(client, Subscription(), 10) == []


# ---------------------------------------------------------------------------
# The guarantee: commit after the work
# ---------------------------------------------------------------------------


def test_the_commit_happens_after_the_cleaning(log, cleaner):
    """
    The one that matters. Committing first would turn every crash from a
    redelivery into a row nobody ever cleans -- and it is one line's
    difference in the loop.
    """
    client = FakeClient([[event(7)]], log)

    run(client, log)

    assert ("cleaned", (7,)) in log
    assert client.commits == [len(log)], (
        f"committed after {client.commits} log entries, of {len(log)}: {log}"
    )


def test_a_cleaned_row_is_marked_before_the_commit(log, cleaner):
    client = FakeClient([[event(7)]], log)

    run(client, log)

    assert log == [("cleaned", (7,)), ("mark", (7,), "CLEANED")]
    assert client.commits == [2]


def test_the_client_it_created_is_closed_on_the_way_out(
    monkeypatch, log, cleaner
):
    """
    Leaving the group deliberately rather than by timing out, so a restart
    rejoins immediately instead of waiting for the broker to notice.
    """
    created = FakeClient([[event(7)]], log)
    monkeypatch.setattr(consumer_module, "_client", lambda s: created)

    consumer_module.consume(
        Subscription(),
        spark=object(),
        database=object(),
        once=True,
        write_line=lambda line: None,
    )

    assert created.closed


def test_a_client_the_caller_owns_is_not_closed(log, cleaner):
    """
    ``consume`` closes what it created and nothing else -- a caller that
    passed its own client is still using it.
    """
    client = FakeClient([[event(7)]], log)

    consumer_module.consume(
        Subscription(),
        spark=object(),
        database=object(),
        client=client,
        once=True,
        write_line=lambda line: None,
    )

    assert client.closed is False


# ---------------------------------------------------------------------------
# When a row will not clean
# ---------------------------------------------------------------------------


def test_a_failing_row_is_marked_failed_and_the_consumer_carries_on(
    log, cleaner
):
    cleaner(broken=[7])
    client = FakeClient([[event(7)]], log)

    cleaned = run(client, log)

    assert cleaned == 0
    assert ("mark", (7,), "FAILED") in log
    assert client.commits, "a row that cannot be cleaned must still commit"


def test_a_failed_batch_is_retried_one_row_at_a_time(log, cleaner):
    """
    The reason the fallback exists. Row 8 is broken; rows 7 and 9 are fine and
    must be cleaned rather than marked FAILED for having travelled with it.
    """
    cleaner(broken=[8])
    client = FakeClient([[event(7), event(8), event(9)]], log)

    cleaned = run(client, log)

    assert cleaned == 2
    marks = {entry[1]: entry[2] for entry in log if entry[0] == "mark"}
    assert marks == {(7,): "CLEANED", (8,): "FAILED", (9,): "CLEANED"}


def test_a_single_row_batch_is_not_retried(log, cleaner):
    """
    The same row through the same code fails the same way. Retrying it is a
    second failure and twice the wait.
    """
    cleaner(broken=[7])
    client = FakeClient([[event(7)]], log)

    run(client, log)

    assert [entry for entry in log if entry[0] == "clean-failed"] == [
        ("clean-failed", (7,))
    ]


def test_the_failure_reason_reaches_the_outcome(log, cleaner):
    cleaner(broken=[8])

    outcome = consumer_module.handle(
        [8], spark=object(), database=object(), verbose=False
    )

    assert outcome.cleaned == []
    assert "is broken" in outcome.failed[8]
    assert outcome.total == 1


def test_a_row_that_is_not_in_the_table_is_reported_not_counted(log, cleaner):
    """
    A producer can name a row that was deleted, or that never existed -- the
    ``--check`` flag is off by default precisely so that this is possible to
    demonstrate. It must be reported, and it must not be counted as cleaned.
    """
    cleaner(missing=[9])

    outcome = consumer_module.handle(
        [9], spark=object(), database=object(), verbose=False
    )

    assert outcome.cleaned == []
    assert "no row 9" in outcome.failed[9]
    assert not any(entry[0] == "cleaned" for entry in log)
    assert not any(entry[0] == "mark" for entry in log), (
        "there is no row to mark; inventing one to hold the error would be "
        "worse than reporting it"
    )


def test_a_batch_holding_a_missing_row_still_cleans_the_others(log, cleaner):
    """
    The accounting bug this split exists to prevent. Cleaning the batch would
    succeed on the rows that exist, and marking the whole batch CLEANED would
    quietly include the one nothing cleaned -- its UPDATE touching no rows,
    its absence never reported.
    """
    cleaner(missing=[9])

    outcome = consumer_module.handle(
        [7, 9], spark=object(), database=object(), verbose=False
    )

    assert outcome.cleaned == [7]
    assert list(outcome.failed) == [9]
    assert ("mark", (7,), "CLEANED") in log
    assert ("mark", (9,), "CLEANED") not in log


def test_a_batch_of_only_bad_messages_still_commits(log, cleaner):
    """
    Nothing to clean, but the messages were dealt with -- and an offset that
    is never committed is a message redelivered forever.
    """
    client = FakeClient([[FakeMessage(b"rubbish")]], log)

    run(client, log)

    assert client.commits == [0]
    assert not any(entry[0] == "cleaned" for entry in log)


# ---------------------------------------------------------------------------
# Stopping
# ---------------------------------------------------------------------------


def test_the_loop_stops_when_asked(log, cleaner):
    """
    Cooperative, and checked between polls: the handler sets a flag rather
    than closing a socket the poll is sitting on.
    """
    client = FakeClient([[event(7)]], log)

    cleaned = consumer_module.consume(
        Subscription(),
        spark=object(),
        database=object(),
        client=client,
        should_stop=lambda: True,
        write_line=lambda line: None,
    )

    assert cleaned == 0
    assert not log, "nothing should have been cleaned after being told to stop"
    assert client.commits == [], "nothing was processed, so nothing to commit"


def test_the_subscription_is_to_the_ingest_topic(log, cleaner):
    """
    Subscribing to the completion topic would hand this consumer the events it
    publishes itself.
    """
    client = FakeClient([], log)
    subscription = Subscription(topic="transactions.raw.ingested.v1")

    consumer_module.consume(
        subscription,
        spark=object(),
        database=object(),
        client=client,
        should_stop=lambda: True,
        write_line=lambda line: None,
    )

    assert client.subscribed is None, (
        "a caller-supplied client is subscribed by the caller"
    )


def test_a_client_it_creates_is_subscribed(monkeypatch, log, cleaner):
    created = FakeClient([], log)
    monkeypatch.setattr(consumer_module, "_client", lambda s: created)

    consumer_module.consume(
        Subscription(topic="transactions.raw.ingested.v1"),
        spark=object(),
        database=object(),
        should_stop=lambda: True,
        write_line=lambda line: None,
    )

    assert created.subscribed == ["transactions.raw.ingested.v1"]


# ---------------------------------------------------------------------------
# The settings that are the guarantee
# ---------------------------------------------------------------------------


def test_auto_commit_is_off():
    """
    With it on, the client commits on a timer whether or not the row was
    written -- so a crash mid-clean leaves a row nobody cleans. This one
    setting is the difference between at-least-once and at-most-once.
    """
    assert Subscription().consumer_config["enable.auto.commit"] is False


def test_a_fresh_group_starts_at_the_beginning():
    """
    ``latest`` would make the first run of a new group skip everything
    published before it started -- indistinguishable, from outside, from a
    consumer that does not work.
    """
    assert Subscription().consumer_config["auto.offset.reset"] == "earliest"


def test_the_poll_interval_allows_for_a_slow_spark_batch():
    """
    Kafka's default is five minutes, and a cold Spark batch can exceed it. The
    broker then revokes the partitions, the commit fails, and the batch is
    redelivered to a consumer that takes just as long -- a livelock, not a
    slow run.
    """
    configured = Subscription().consumer_config["max.poll.interval.ms"]

    assert configured > 300_000
