"""
Publishing against a real broker: does the event actually arrive?

Marked ``kafka`` and skipped when the broker is down. Every test here reads
its own message back rather than trusting the publish call, because the thing
most likely to go wrong is precisely that a publish appears to succeed and
sends nothing -- ``produce()`` is asynchronous and a process that does not
flush exits having queued a message and delivered none.

Offsets are read before publishing and the consumer is *assigned* that exact
position rather than subscribed. Subscription triggers a group rebalance,
which takes seconds and makes the test a race; assignment is deterministic and
reads exactly the messages this test produced.
"""

import uuid

import pytest

from src.kafka import events, producer
from src.kafka import settings as kafka_settings
from tests.test_kafka_events import result_for

pytestmark = pytest.mark.kafka


@pytest.fixture(scope="module")
def broker():
    """:returns: Broker settings, skipping when nothing answers."""
    pytest.importorskip("confluent_kafka")
    settings = kafka_settings.load()
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": settings.servers}).list_topics(
            timeout=5
        )
    except Exception as exc:  # noqa: BLE001 - the message is the point
        pytest.skip(
            f"broker not reachable at {settings.servers} "
            f"({type(exc).__name__}). Run `make verify` -- it names the cause."
        )
    producer.ensure_topic(settings)
    return settings


def end_offsets(broker) -> dict:
    """
    :returns: The next offset to be written, per partition. Recorded before a
        publish so the reader below can start exactly there and see only what
        this test produced.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": broker.servers,
        "group.id": f"probe-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(broker.topic, timeout=10)
        offsets = {}
        for partition in metadata.topics[broker.topic].partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(broker.topic, partition), timeout=10
            )
            offsets[partition] = high
        return offsets
    finally:
        consumer.close()


def read_since(broker, offsets: dict, wanted: str, timeout: float = 20.0):
    """
    :param offsets: Where each partition stood before the publish.
    :param wanted: The message key to look for -- the sync_job_id.
    :returns: (key, payload) for the first message with that key, or None.
    """
    import time

    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": broker.servers,
        "group.id": f"probe-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        consumer.assign([
            TopicPartition(broker.topic, partition, offset)
            for partition, offset in offsets.items()
        ])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            key = message.key().decode("utf-8") if message.key() else None
            if key == wanted:
                return key, events.decode(message.value())
        return None
    finally:
        consumer.close()


def test_the_topic_is_created_once_and_then_left_alone(broker):
    """
    Auto-create is off on purpose, so the topic has to be made deliberately --
    and making it twice must not be an error, because the producer calls this
    before every run.
    """
    assert producer.ensure_topic(broker) is False


def test_an_event_reaches_the_broker(broker):
    """
    The one that matters. ``produce()`` queues; only a flush that returns zero
    proves anything left the process.
    """
    job = str(uuid.uuid4())
    payload = events.build(result_for(sync_job_id=job))
    before = end_offsets(broker)

    producer.publish(payload, broker)

    found = read_since(broker, before, job)
    assert found is not None, "the event was never read back"
    key, received = found
    assert received == payload


def test_the_message_is_keyed_on_the_job_id(broker):
    """
    The key decides the partition, so this is what puts every event for one
    load in order, makes the topic compactable, and gives the consumer
    something to dedupe on.
    """
    job = str(uuid.uuid4())
    before = end_offsets(broker)

    producer.publish(events.build(result_for(sync_job_id=job)), broker)

    key, _ = read_since(broker, before, job)
    assert key == job


def test_an_event_without_a_key_is_refused_before_it_is_sent(broker):
    """
    Rather than published to an arbitrary partition, where it would be
    unorderable and undedupable and nothing would have complained.
    """
    payload = events.build(result_for())
    del payload["sync_job_id"]

    with pytest.raises(ValueError, match="sync_job_id"):
        producer.publish(payload, broker)


def test_an_unreachable_broker_fails_rather_than_looking_fine():
    """
    The failure this whole module exists to rule out: a run that wrote
    265,195 rows, announced nothing, and reported success.

    A short delivery timeout so the test does not sit for thirty seconds --
    the point is that it raises, not how long it waits first.
    """
    from dataclasses import replace

    nowhere = replace(
        kafka_settings.load(), servers="localhost:9", delivery_timeout=5
    )
    payload = events.build(result_for())

    with pytest.raises(producer.PublishError):
        producer.publish(payload, nowhere)


def test_emit_builds_and_publishes_in_one_call(broker):
    """
    What the runner actually calls, and it returns the payload it sent so a
    caller logs what went out rather than a reconstruction of it.
    """
    job = str(uuid.uuid4())
    before = end_offsets(broker)

    payload = producer.emit(result_for(sync_job_id=job), broker)

    assert payload["sync_job_id"] == job
    found = read_since(broker, before, job)
    assert found is not None
    assert found[1] == payload
