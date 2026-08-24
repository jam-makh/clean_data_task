"""
What "a row landed in raw_transactions" says on the wire.

The other event this system carries, and the mirror image of ``events.py``.
That one is published *by* a finished run and says what it did; this one is
published *at* the pipeline and asks it to do something -- so it is small, it
names a row rather than a job, and it is the only thing standing between an
INSERT and the cleaning.

The payload
-----------

A JSON object and not a bare integer, which is the obvious first design and a
dead end for three reasons:

* A consumer holding ``42`` cannot tell what it is holding. It cleans row 42
  because it assumes the topic only ever carries these, and the day that
  assumption breaks it breaks silently.
* There is nowhere to put a version, so the first added field is a breaking
  change with no way for a consumer to detect which shape it has.
* ``kafka-console-consumer`` on the topic shows a column of naked integers --
  no time, no origin, nothing to debug with.

So five fields, each earning its place:

``event``        what this message is, so the consumer validates rather than
                 assumes -- see ``decode``.
``version``      the payload's shape. In the payload as well as in the topic
                 name for the reason ``events.py`` gives: the topic's ``.v1``
                 is the wire contract, and the field is for a consumer
                 accepting several minor shapes on one topic.
``occurred_at``  when it was emitted. The gap between this and the row's
                 ``processed_at`` is consumer lag, visible for nothing.
``id``           the work: which row to go and read.
``table``        where to read it from, so the consumer is not one table's
                 consumer by construction.

The **key** is the id as a string, and it is not the same thing as the id in
the payload. Kafka reads the key to choose a partition, so keying on the id
puts every message about one row on one partition and therefore in order --
which is what makes a redelivery a repeat rather than a race.
"""

import json
from datetime import datetime, timezone

from src.kafka import events

# Bump for a breaking change to the payload, and create a new topic when you
# do. Same rule as the completion event, and the same reason: a consumer
# reading v1 must never be handed v2 on the same topic.
VERSION = 1

# The event's name, matching the topic minus its version suffix.
EVENT = "transaction.raw.ingested"


def build(row_id: int, table: str = "raw_transactions") -> dict:
    """
    Renders a landed row as the event payload.

    :param row_id: The id the database allocated, as ``src.db.raw.insert``
        returned it.
    :param table: Which table it landed in.
    :returns: The payload, as a JSON-serialisable dict.
    :raises ValueError: If the id is not a whole number. Checked here as well
        as in ``src.db.raw`` because this is where a bad id becomes a message
        that is already published -- and a message that cannot be acted on
        will sit on the topic being redelivered to a consumer that cannot use
        it.
    """
    if isinstance(row_id, bool) or not isinstance(row_id, int):
        raise ValueError(f"{row_id!r} is not a row id")

    return {
        "event": EVENT,
        "version": VERSION,
        # When the event was produced. UTC with an explicit offset, because a
        # naive timestamp in a message is a timestamp whose meaning depends on
        # which machine reads it.
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "id": row_id,
        "table": table,
    }


def key_for(payload: dict) -> str:
    """
    :param payload: An event payload.
    :returns: The message key -- the id as a string, because Kafka keys are
        bytes and the id is a number, and the conversion has to happen
        somewhere it can be seen rather than inside the publish call.
    """
    return str(payload["id"])


def encode(payload: dict) -> bytes:
    """
    :param payload: An event payload.
    :returns: Its bytes, as they go on the wire.

    Deliberately the completion event's encoder rather than a second one. Both
    events are compact sorted JSON, and two encoders would be two chances for
    the topics to end up spelling the same value differently.
    """
    return events.encode(payload)


def decode(raw: bytes) -> dict:
    """
    Reads a message, and refuses anything that is not one of these.

    The consumer's first line of defence, and the reason it lives here beside
    the writer rather than in the consumer: what a valid message looks like is
    this module's business, and a consumer carrying its own opinion about it
    would be a second definition to keep in step.

    Validation is stricter than ``events.decode`` because the consequence is
    heavier. That one produces a dict somebody reads; this one produces an id
    that is used to fetch and clean a row, so a payload that is *shaped* right
    but carries ``"id": "42; DROP TABLE"`` has to fail here, at the boundary,
    rather than further in.

    :param raw: A message's bytes.
    :returns: The payload, with ``id`` guaranteed to be an int.
    :raises ValueError: On anything that is not this event: not JSON, not an
        object, the wrong event name, a missing field, or an id that is not a
        whole number.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message is not JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(
            f"message is a {type(payload).__name__}, not an event object"
        )

    for required in ("event", "version", "id"):
        if required not in payload:
            raise ValueError(f"event is missing {required!r}")

    # Checked rather than assumed, because a topic is a place other people can
    # publish to. A completion event arriving here -- the mistake
    # config/pipeline.yaml guards against by refusing to let the two topics
    # share a name -- would otherwise be a KeyError on 'id' forty frames later.
    if payload["event"] != EVENT:
        raise ValueError(
            f"event is {payload['event']!r}, not {EVENT!r}. Something else is "
            f"publishing to this topic."
        )

    row_id = payload["id"]
    if isinstance(row_id, bool) or not isinstance(row_id, int):
        raise ValueError(
            f"event id is {row_id!r}, which is not a row id. Ids are numbers "
            f"and this one is a {type(row_id).__name__}."
        )

    # Defaulted rather than required, so a message written before the field
    # existed still reads. A consumer that only serves one table would
    # otherwise reject its own history.
    payload.setdefault("table", "raw_transactions")
    return payload
