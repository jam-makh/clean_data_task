"""
Publishing the completion event, and creating the topic it goes to.

Two things here are easy to get wrong and quiet when you do.

**produce() does not publish.** It appends to an in-memory queue and returns
immediately; the client sends in the background. A process that calls
``produce`` and exits sends nothing at all, and reports no error while doing
it. ``flush()`` is what waits for the queue to drain, and its return value --
the number of messages *still* undelivered -- is the only thing that
distinguishes a delivered event from a lost one. So every publish here flushes
and checks, and a failure raises rather than being logged and stepped over: a
pipeline that wrote 265,195 rows and did not tell anyone has half-finished, and
the caller is the only one who can decide what to do about that.

**Auto-create is off**, deliberately -- the note in ``scripts/verify_env.py``
explains why: a producer aimed at a typo'd topic should fail rather than
quietly invent one and publish into a topic nobody is reading. Which means the
topic has to be created on purpose, which is what ``ensure_topic`` is for.

The message key is the ``sync_job_id``. That puts every event for one load on
one partition and therefore in order, and it makes the topic compactable --
the latest event for a job is the one worth keeping. It is also exactly the key
a consumer dedupes on.
"""

from src.kafka import events
from src.kafka.settings import Broker


class PublishError(RuntimeError):
    """
    Raised when an event was not delivered.

    Its own type because the caller's response is specific: the rows are
    already in Postgres and committed, so this is not a reason to roll
    anything back -- it is a reason to re-emit, which is safe because the
    payload is derived from the run and re-publishing it is idempotent on the
    consumer's side.
    """


def _client(broker: Broker):
    """
    :param broker: Where to publish.
    :returns: A configured ``confluent_kafka.Producer``.
    :raises RuntimeError: If the library is missing, naming what needed it --
        the import error alone names only a module.
    """
    try:
        from confluent_kafka import Producer
    except ImportError as exc:  # pragma: no cover - environment failure
        raise RuntimeError(
            "confluent-kafka is required to emit completion events. "
            "pip install -r requirements.txt"
        ) from exc

    return Producer(broker.producer_config)


def ensure_topic(broker: Broker) -> bool:
    """
    Creates the topic if the broker does not already have it.

    :param broker: Where, and with what partition count.
    :returns: True if this call created it, False if it was already there.
    :raises RuntimeError: If the broker cannot be reached, which is worth
        distinguishing from "the topic is missing" -- the fixes are different
        and `make verify` names both.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": broker.servers})
    try:
        metadata = admin.list_topics(timeout=10)
    except Exception as exc:  # noqa: BLE001 - the message is the point
        raise RuntimeError(
            f"could not reach the broker at {broker.servers} "
            f"({type(exc).__name__}: {exc}). Run `make verify`."
        ) from exc

    if broker.topic in metadata.topics:
        return False

    requested = NewTopic(
        broker.topic,
        num_partitions=broker.partitions,
        replication_factor=broker.replication_factor,
    )
    # create_topics returns a future per topic; the result is None on success
    # and raises on failure, including the benign case where another process
    # created the same topic between the check above and this call.
    for topic, future in admin.create_topics([requested]).items():
        try:
            future.result(timeout=15)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                return False
            raise RuntimeError(f"could not create topic {topic}: {exc}") from exc
    return True


def publish(payload: dict, broker: Broker | None = None) -> None:
    """
    Publishes one event and waits for the broker to acknowledge it.

    :param payload: The event, as ``events.build`` produced it.
    :param broker: Where to publish; loaded from config and environment when
        absent.
    :raises PublishError: If the event was not acknowledged within the
        configured delivery timeout, or the broker rejected it.
    :raises ValueError: If the payload has no ``sync_job_id`` to key on, which
        would put the event on an arbitrary partition and make it undedupable.
    """
    broker = broker if broker is not None else _load_broker()

    key = payload.get("sync_job_id")
    if not key:
        raise ValueError("event has no sync_job_id to use as its message key")

    # Captured from the delivery callback, which runs on the client's own
    # thread during flush(). A raise in there would be swallowed, so the error
    # is carried back out and raised on this thread instead.
    failures: list[str] = []

    def delivered(error, message):
        if error is not None:
            failures.append(str(error))
        del message

    client = _client(broker)
    client.produce(
        topic=broker.topic,
        key=key.encode("utf-8"),
        value=events.encode(payload),
        on_delivery=delivered,
    )

    remaining = client.flush(timeout=broker.delivery_timeout)
    if remaining:
        raise PublishError(
            f"{remaining} event(s) still unsent after "
            f"{broker.delivery_timeout}s to {broker.servers}. The rows are "
            f"written; re-emitting is safe."
        )
    if failures:
        raise PublishError(
            f"broker rejected the event: {'; '.join(failures)}. The rows are "
            f"written; re-emitting is safe."
        )


def emit(result, broker: Broker | None = None, engine: str = "spark") -> dict:
    """
    Builds the completion event for a run and publishes it.

    :param result: The ``src.runner.RunResult``.
    :param broker: Where to publish; loaded when absent.
    :param engine: Which engine ran, for the payload.
    :returns: The payload that was published, so a caller can log or assert on
        exactly what went out rather than on a reconstruction of it.
    """
    broker = broker if broker is not None else _load_broker()
    payload = events.build(result, engine=engine)
    ensure_topic(broker)
    publish(payload, broker)
    return payload


def _load_broker() -> Broker:
    """:returns: Broker settings from config and the environment."""
    from src.kafka.settings import load

    return load()
