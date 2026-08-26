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
from pathlib import Path

import pytest

from src.kafka import consumer as consumer_module
from src.kafka import ingest_events
from src.kafka.settings import Subscription


class FakeMessage:
    """
    One Kafka message, or an error notice when ``error`` is given.

    Carries topic/partition/offset because the audit trail records them, and a
    fake that omitted them would let a bug through that only appeared against
    a real broker.
    """

    def __init__(self, value: bytes, error=None, offset=0):
        self._value = value
        self._error = error
        self._offset = offset

    def value(self):
        return self._value

    def error(self):
        return self._error

    def topic(self):
        return "transactions.raw.ingested.v1"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


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


def _nowhere() -> str:
    """:returns: A throwaway path, for a loop whose quarantine is incidental."""
    import tempfile

    return str(Path(tempfile.mkdtemp()) / "undecodable.jsonl")


def run(client, log, **kwargs):
    """
    Drives one pass of the loop against a fake client.

    ``audit_trail`` defaults to a path under the pytest tmp root rather than
    the configured one, so a test that feeds the loop a malformed message
    cannot append to the repo's real quarantine file.
    """
    del log
    return consumer_module.consume(
        Subscription(
            batch_size=kwargs.pop("batch_size", 10),
            audit_trail=str(kwargs.pop("audit_trail", "")) or _nowhere(),
        ),
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
    assert "not JSON" in errors[0].error


def test_a_completion_event_on_this_topic_is_skipped():
    payload = {"event": "pipeline.run.completed", "version": 1, "id": 1}

    ids, errors = consumer_module.decode_all(
        [FakeMessage(json.dumps(payload).encode("utf-8"))]
    )

    assert ids == []
    assert "Something else is publishing" in errors[0].error


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


# ---------------------------------------------------------------------------
# The audit trail
# ---------------------------------------------------------------------------


def test_an_undecodable_message_is_kept_with_its_offset(tmp_path):
    """
    The failure the status column cannot record. A row that will not clean is
    marked FAILED against its id; this message has no id, so the only place it
    can go is the trail -- and what makes the record useful is the offset,
    which is the address of the original in the log.
    """
    trail = tmp_path / "undecodable.jsonl"
    client = FakeClient([[FakeMessage(b"not json at all", offset=17)]], [])

    run(client, [], audit_trail=trail)

    written = [json.loads(line) for line in trail.read_text().splitlines()]
    assert len(written) == 1
    assert written[0]["offset"] == 17
    assert written[0]["topic"] == "transactions.raw.ingested.v1"
    assert "not JSON" in written[0]["error"]

    # The bytes come back exactly, which is the whole point of keeping them.
    import base64

    assert base64.b64decode(written[0]["value_b64"]) == b"not json at all"


def test_a_valid_message_leaves_no_trace_in_the_trail(tmp_path, cleaner):
    """The trail is for refusals, not a second copy of the topic."""
    del cleaner
    trail = tmp_path / "undecodable.jsonl"

    run(FakeClient([[event(7)]], []), [], audit_trail=trail)

    assert not trail.exists()


def test_a_trail_that_cannot_be_written_does_not_stop_the_consumer(
    tmp_path, cleaner, log
):
    """
    A full disk must not turn one malformed message into a dead consumer.
    That would be strictly worse than the skipping this replaced -- so the
    write fails, the loop says so, and the good ids in the same batch are
    still cleaned.
    """
    del cleaner
    lines: list = []
    client = FakeClient(
        [[FakeMessage(b"not json at all"), event(7)]], log
    )

    # A directory where the file should be: the open fails, and nothing about
    # that is this consumer's problem to survive.
    blocked = tmp_path / "undecodable.jsonl"
    blocked.mkdir()

    cleaned = consumer_module.consume(
        Subscription(batch_size=10, audit_trail=str(blocked)),
        spark=object(),
        database=object(),
        client=client,
        once=True,
        write_line=lines.append,
    )

    assert cleaned == 1
    assert ("cleaned", (7,)) in log
    assert any("NOWHERE" in line for line in lines)


def test_the_message_is_kept_before_the_offset_is_committed(tmp_path):
    """
    The same ordering the Postgres write has, for the same reason: the commit
    is the promise that the message was dealt with, so quarantining after it
    would let a crash in between lose the only copy.
    """
    trail = tmp_path / "undecodable.jsonl"
    seen: list = []

    class WatchfulClient(FakeClient):
        def commit(self, asynchronous=False):
            seen.append(("commit", trail.exists()))
            return super().commit(asynchronous=asynchronous)

    run(WatchfulClient([[FakeMessage(b"nope")]], []), [], audit_trail=trail)

    assert seen == [("commit", True)]


def test_a_message_missing_the_usual_methods_is_still_kept(tmp_path):
    """
    ``build`` reads every field through getattr. A record that raised while
    describing a malformed message would replace a small problem with a
    larger one.
    """
    from src.kafka import audit_trail as trail_module

    trail = tmp_path / "undecodable.jsonl"

    class Bare:
        def value(self):
            return b"x"

    assert trail_module.record(Bare(), "no good", trail) is True

    written = json.loads(trail.read_text().splitlines()[0])
    assert written["offset"] is None
    assert written["error"] == "no good"


def test_records_append_rather_than_replace(tmp_path):
    """JSON Lines, so a crash costs one line rather than the whole file."""
    from src.kafka import audit_trail as trail_module

    trail = tmp_path / "undecodable.jsonl"
    trail_module.record(FakeMessage(b"one", offset=1), "first", trail)
    trail_module.record(FakeMessage(b"two", offset=2), "second", trail)

    assert len(trail.read_text().splitlines()) == 2


def test_the_directory_is_made_when_it_is_missing(tmp_path):
    """
    The first undecodable message may well arrive where nothing has written
    yet, and losing it to a missing directory would defeat the point.
    """
    from src.kafka import audit_trail as trail_module

    trail = tmp_path / "never" / "made" / "undecodable.jsonl"

    assert trail_module.record(FakeMessage(b"x"), "why", trail) is True
    assert trail.exists()


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


# ---------------------------------------------------------------------------
# A driver that is gone, as distinct from a row that is bad
# ---------------------------------------------------------------------------


def _heap_death(ids, **kwargs):
    """Cleaning, on a driver whose heap has gone. Every batch dies this way."""
    del ids, kwargs
    raise RuntimeError(
        "Py4JJavaError: An error occurred while calling o1.collectToPython.\n"
        ": java.lang.OutOfMemoryError: Java heap space"
    )


def test_a_dead_driver_stops_the_consumer_instead_of_blaming_the_row(
    log, cleaner, monkeypatch
):
    """
    An OutOfMemoryError says nothing about these ids.

    Left to the ordinary path it would be caught, written into the ``error``
    column of a row that was never read, and the loop would go on to the next
    message and do it again -- which is exactly what happened: six consecutive
    batches "failed" in five seconds each, having done no work, on a driver
    that could no longer execute anything.
    """
    monkeypatch.setattr(consumer_module, "clean", _heap_death)

    with pytest.raises(consumer_module.SessionLost, match="OutOfMemoryError"):
        consumer_module.handle(
            [7], spark=object(), database=object(), verbose=False
        )

    assert not any(
        entry[0] == "mark" and entry[2] == "FAILED" for entry in log
    ), "a heap error must not be recorded as this row's problem"


def test_a_dead_driver_is_not_retried_row_by_row(log, cleaner, monkeypatch):
    """
    The single-row retry exists to attribute blame within a batch. There is no
    blame to attribute here and the retry would run the same doomed job once
    per id, so the batch must abort on the first one.
    """
    attempts = []

    def counting(ids, **kwargs):
        attempts.append(tuple(ids))
        return _heap_death(ids, **kwargs)

    monkeypatch.setattr(consumer_module, "clean", counting)

    with pytest.raises(consumer_module.SessionLost):
        consumer_module.handle(
            [7, 8, 9], spark=object(), database=object(), verbose=False
        )

    assert attempts == [(7, 8, 9)], (
        f"the batch was retried after the driver died: {attempts}"
    )


def test_an_ordinary_failure_is_still_the_row_s_own(log, cleaner):
    """
    The guard has to be narrow. If it caught everything, one bad row would
    stop a consumer that is perfectly capable of carrying on -- so this is the
    same shape of test pointed the other way.
    """
    cleaner(broken=[8])

    outcome = consumer_module.handle(
        [8], spark=object(), database=object(), verbose=False
    )

    assert "is broken" in outcome.failed[8]
    assert ("mark", (8,), "FAILED") in log


# ---------------------------------------------------------------------------
# Recycling the session
# ---------------------------------------------------------------------------


def _drive(client, *, renew_every, renew, batches_expected, **kwargs):
    """
    Runs the loop over every prepared batch and then stops.

    :param client: A ``FakeClient`` with its batches loaded.
    :param renew_every: The subscription's setting.
    :param renew: The factory, or None.
    :param batches_expected: How many batches the client will yield, after
        which ``should_stop`` trips. ``once=True`` cannot be used here --
        recycling happens between batches, so a test that stopped after the
        first one would be testing nothing.
    :returns: What ``consume`` returned.
    """
    seen = {"polls": 0}

    def should_stop():
        seen["polls"] += 1
        return seen["polls"] > batches_expected

    return consumer_module.consume(
        Subscription(
            batch_size=10, audit_trail=_nowhere(), renew_every=renew_every,
        ),
        spark="session-0",
        database=object(),
        client=client,
        renew=renew,
        should_stop=should_stop,
        write_line=lambda line: None,
        **kwargs,
    )


def test_the_session_is_rebuilt_every_nth_batch(log, cleaner):
    """
    The bound the whole change exists for. A driver's footprint is a function
    of how long it has been up, and nothing inside a batch changes that -- so
    the loop has to end the driver periodically, or accept that the footprint
    grows without limit.
    """
    built = []

    def factory():
        built.append(f"session-{len(built) + 1}")
        return built[-1]

    client = FakeClient([[event(i)] for i in range(1, 7)], log)

    _drive(client, renew_every=2, renew=factory, batches_expected=6)

    assert len(built) == 3, (
        f"six batches at every-two should rebuild three times, not {built}"
    )


def test_the_new_session_is_the_one_the_next_batch_uses(log, monkeypatch):
    """
    A rebuild that the loop then ignored would be strictly worse than no
    rebuild at all: the cost of a restart, and a stopped context handed to the
    next batch. So the assertion is on what ``clean`` was given, not on what
    the factory returned.
    """
    handed = []

    def recording_clean(ids, **kwargs):
        handed.append(kwargs["spark"])

    monkeypatch.setattr(consumer_module, "clean", recording_clean)
    monkeypatch.setattr(consumer_module.raw, "mark", lambda *a, **k: 1)
    monkeypatch.setattr(
        consumer_module.raw, "existing", lambda database, ids: list(ids)
    )

    client = FakeClient([[event(i)] for i in range(1, 5)], log)

    _drive(
        client, renew_every=2, renew=lambda: "session-fresh",
        batches_expected=4,
    )

    assert handed[:2] == ["session-0", "session-0"]
    assert handed[2:] == ["session-fresh", "session-fresh"], (
        f"the rebuilt session never reached the pipeline: {handed}"
    )


def test_recycling_happens_after_the_commit(log, cleaner):
    """
    Tearing a driver down between the Postgres write and the Kafka commit
    would open a several-second window in which a crash redelivers a batch
    that is already written. The upsert makes that harmless, but harmless by
    luck is not the same as arranged -- and there is no reason to accept the
    window, because after the commit there is nothing outstanding at all.
    """
    order = []

    client = FakeClient([[event(1)], [event(2)]], log)
    original_commit = client.commit

    def recording_commit(asynchronous=True):
        order.append("commit")
        return original_commit(asynchronous=asynchronous)

    client.commit = recording_commit

    _drive(
        client,
        renew_every=1,
        renew=lambda: order.append("renew") or "session-next",
        batches_expected=2,
    )

    assert order == ["commit", "renew", "commit", "renew"], (
        f"the session was recycled outside the safe window: {order}"
    )


def test_an_idle_consumer_never_recycles(log, cleaner):
    """
    Polls that decode nothing do no Spark work, so there is nothing to
    reclaim. A consumer counting elapsed polls rather than batches would
    restart its driver all night on an empty topic -- burning the cost of the
    fix without ever having had the problem.
    """
    built = []
    client = FakeClient([[], [], []], log)

    _drive(
        client, renew_every=1, renew=lambda: built.append(1),
        batches_expected=3,
    )

    assert built == []


def test_without_a_factory_the_session_is_left_alone(log, cleaner):
    """
    ``renew`` absent means the caller owns the session and expects to still
    have it -- a test, a notebook, ``--once``. Recycling somebody else's
    session would stop a context they are about to use.
    """
    client = FakeClient([[event(i)] for i in range(1, 5)], log)

    cleaned = _drive(client, renew_every=1, renew=None, batches_expected=4)

    assert cleaned == 4


def test_renew_every_zero_turns_recycling_off(log, cleaner):
    """
    The switch, for a run short enough that a driver cannot get old.
    """
    built = []
    client = FakeClient([[event(i)] for i in range(1, 5)], log)

    _drive(
        client, renew_every=0, renew=lambda: built.append(1),
        batches_expected=4,
    )

    assert built == []


def test_a_failed_rebuild_does_not_kill_the_consumer(log, cleaner):
    """
    Recycling is maintenance. A consumer that died because routine maintenance
    hit a transient failure would be a worse consumer than one that never
    recycled -- so the failure is reported and the loop carries on, and the
    next batch reports the real problem with the real error.
    """
    said = []

    def broken():
        raise RuntimeError("no JVM today")

    client = FakeClient([[event(1)], [event(2)]], log)

    consumer_module.consume(
        Subscription(batch_size=10, audit_trail=_nowhere(), renew_every=1),
        spark="session-0",
        database=object(),
        client=client,
        renew=broken,
        should_stop=_stops_after(2),
        write_line=said.append,
    )

    assert any("could not rebuild" in line for line in said), (
        f"a failed recycle has to be said out loud: {said}"
    )
    assert any(entry[0] == "cleaned" for entry in log), (
        "the loop stopped cleaning because maintenance failed"
    )


def _stops_after(polls: int):
    """:returns: A ``should_stop`` that trips once ``polls`` have been made."""
    seen = {"n": 0}

    def should_stop():
        seen["n"] += 1
        return seen["n"] > polls

    return should_stop


def test_recycling_stops_the_session_it_was_given(log, cleaner):
    """
    The session in hand, not whatever session is active in the process.

    A consumer has exactly one, so the two are the same there and the bug is
    invisible. Anywhere else they differ: under pytest the active session
    belongs to a suite-scoped fixture, and the first version of this reached
    for it and tore it down -- which every later Spark test then failed on,
    with an AttributeError naming nothing to do with the consumer.
    """
    stopped = []

    class Session:
        def __init__(self, name):
            self.name = name

        def stop(self):
            stopped.append(self.name)

    mine = Session("mine")
    client = FakeClient([[event(1)], [event(2)]], log)

    consumer_module.consume(
        Subscription(batch_size=10, audit_trail=_nowhere(), renew_every=1),
        spark=mine,
        database=object(),
        client=client,
        renew=lambda: Session("fresh"),
        should_stop=_stops_after(2),
        write_line=lambda line: None,
    )

    assert stopped == ["mine", "fresh"], (
        f"the wrong session was stopped, or none was: {stopped}"
    )
