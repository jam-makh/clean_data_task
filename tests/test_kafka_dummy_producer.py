"""
The ingest event against a real broker: does the id actually arrive?

Marked ``kafka`` and skipped when the broker is down. The same shape as
``test_kafka_producer.py`` and for the same reasons -- every test reads its own
message back rather than trusting the publish call, because the failure most
worth catching is a publish that appears to succeed and sends nothing; and the
reader is *assigned* a recorded offset rather than subscribed, because
subscription triggers a group rebalance and turns the test into a race.

What is being proved here is narrow and load-bearing: the event lands on the
ingest topic and not on the completion topic, and it is keyed on the id. If
the first were wrong the consumer would never see it; if the second were, two
messages about one row could be reordered across partitions and a redelivery
would stop being a repeat.
"""

import time
import uuid

import pytest

from scripts import dummy_producer
from src.kafka import ingest_events
from src.kafka import producer
from src.kafka import settings as kafka_settings

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
    producer.ensure_topic(settings, settings.raw_topic)
    return settings


def end_offsets(broker, topic: str) -> dict:
    """
    :returns: The next offset to be written, per partition, recorded before a
        publish so a reader can start exactly there and see only what this
        test produced.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": broker.servers,
        "group.id": f"probe-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        offsets = {}
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            offsets[partition] = high
        return offsets
    finally:
        consumer.close()


def read_since(broker, topic: str, offsets: dict, wanted: str, timeout=20.0):
    """
    :param offsets: Where each partition stood before the publish.
    :param wanted: The message key to look for -- the id, as text.
    :returns: (key, payload) for the first message with that key, or None.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": broker.servers,
        "group.id": f"probe-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        consumer.assign([
            TopicPartition(topic, partition, offset)
            for partition, offset in offsets.items()
        ])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            key = message.key().decode("utf-8") if message.key() else None
            if key == wanted:
                return key, ingest_events.decode(message.value())
        return None
    finally:
        consumer.close()


@pytest.fixture
def row_id() -> int:
    """
    :returns: An id unlikely to collide with a real row or with another run of
        this test. Nothing reads it back out of the database -- what is under
        test is the message, and requiring a row to exist first would make a
        broker test depend on Postgres.
    """
    return 900_000 + uuid.uuid4().int % 90_000


def test_the_ingest_topic_is_created_once_and_then_left_alone(broker):
    """
    Auto-create is off on purpose, so the topic has to be made deliberately --
    and making it twice must not be an error, because the producer calls this
    before every emit.
    """
    assert producer.ensure_topic(broker, broker.raw_topic) is False


def test_the_two_topics_are_different_places(broker):
    """
    The setting the config refuses to let collapse. If they were one topic,
    the consumer would be handed the completion events it publishes itself.
    """
    assert broker.raw_topic != broker.topic


def test_an_id_reaches_the_broker(broker, row_id):
    offsets = end_offsets(broker, broker.raw_topic)

    published = dummy_producer.emit(row_id, broker)

    found = read_since(broker, broker.raw_topic, offsets, str(row_id))
    assert found is not None, (
        f"nothing with key {row_id} arrived on {broker.raw_topic}"
    )
    _, payload = found
    assert payload == published, (
        "what arrived must be what emit() reported publishing"
    )
    assert payload["id"] == row_id
    assert payload["table"] == "raw_transactions"


def test_the_message_is_keyed_on_the_id(broker, row_id):
    """
    Not decoration. The key chooses the partition, so keying on the id is what
    keeps two messages about one row in order -- and therefore what makes a
    redelivery a repeat rather than a race.
    """
    offsets = end_offsets(broker, broker.raw_topic)

    dummy_producer.emit(row_id, broker)

    key, _ = read_since(broker, broker.raw_topic, offsets, str(row_id))
    assert key == str(row_id)


def test_the_ingest_event_does_not_land_on_the_completion_topic(
    broker, row_id
):
    """
    The one that would be silent. A misrouted event would leave the consumer
    waiting on an empty topic while the completion topic filled with messages
    nothing reads -- and both processes would look healthy.
    """
    offsets = end_offsets(broker, broker.topic)

    dummy_producer.emit(row_id, broker)

    assert read_since(broker, broker.topic, offsets, str(row_id), timeout=5.0) \
        is None


def test_publishing_to_an_unreachable_broker_fails_rather_than_looking_fine(
    broker, row_id
):
    """
    ``produce()`` queues and returns; a process that does not flush exits
    having delivered nothing and reporting no error. This is the test that the
    flush is real.
    """
    from dataclasses import replace

    nowhere = replace(broker, servers="localhost:9", delivery_timeout=5)

    with pytest.raises(producer.PublishError):
        dummy_producer.emit(row_id, nowhere)
