"""
The ingest event's payload, checked without a broker.

Split from ``test_kafka_dummy_producer.py`` the way ``test_kafka_events.py``
is split from ``test_kafka_producer.py``, and for the same reason: whether the
message says the right thing and whether it reaches Kafka are different
questions with different failure modes, and only one of them needs a
container.

Most of this file is about ``decode`` refusing things, which is where the
weight sits. The id in a message is used to fetch a row and clean it, so
``decode`` is the boundary between text somebody else wrote and an argument
this pipeline acts on -- and it is the only place that boundary exists.
"""

import json

import pytest

from scripts import dummy_producer
from src.kafka import events, ingest_events


def test_the_payload_says_what_it_is_and_which_row():
    payload = ingest_events.build(42)

    assert payload["event"] == "transaction.raw.ingested"
    assert payload["version"] == 1
    assert payload["id"] == 42
    assert payload["table"] == "raw_transactions"


def test_the_timestamp_is_utc_with_an_explicit_offset():
    """
    A naive timestamp in a message is a timestamp whose meaning depends on
    which machine reads it -- and this one is half of the consumer-lag
    measurement, the other half being the row's processed_at.
    """
    from datetime import datetime

    occurred = datetime.fromisoformat(ingest_events.build(42)["occurred_at"])

    assert occurred.tzinfo is not None
    assert occurred.utcoffset().total_seconds() == 0


def test_the_key_is_the_id_as_text():
    """
    Kafka keys are bytes and the id is a number. Keying on it is what puts
    every message about one row on one partition, and therefore in order.
    """
    payload = ingest_events.build(42)

    assert ingest_events.key_for(payload) == "42"


def test_the_event_survives_a_round_trip():
    payload = ingest_events.build(42)

    assert ingest_events.decode(ingest_events.encode(payload)) == payload


def test_the_wire_format_is_the_completion_event_s():
    """
    One encoder, deliberately. Two would be two chances for the topics to
    spell the same value differently -- and the consumer reads both.
    """
    payload = ingest_events.build(42)

    assert ingest_events.encode(payload) == events.encode(payload)


@pytest.mark.parametrize("row_id", ["42", 4.0, None, True, [42]])
def test_an_id_that_is_not_a_number_is_refused_at_build(row_id):
    """
    Checked when the event is built as well as when it is read, because this
    is the last moment before it becomes a published message -- and a message
    that cannot be acted on does not go away, it sits on the topic being
    redelivered to a consumer that still cannot use it.
    """
    with pytest.raises(ValueError, match="not a row id"):
        ingest_events.build(row_id)


# ---------------------------------------------------------------------------
# decode: the boundary
# ---------------------------------------------------------------------------


def test_a_message_that_is_not_json_is_refused():
    with pytest.raises(ValueError, match="not JSON"):
        ingest_events.decode(b"row 42 arrived")


def test_a_bare_id_is_refused():
    """
    The design this event exists instead of. A consumer holding ``42`` cannot
    tell what it is holding, and one that accepted both shapes would have to
    guess.
    """
    with pytest.raises(ValueError, match="not an event object"):
        ingest_events.decode(b"42")


@pytest.mark.parametrize("missing", ["event", "version", "id"])
def test_a_message_missing_a_required_field_is_refused_by_name(missing):
    payload = ingest_events.build(42)
    del payload[missing]

    with pytest.raises(ValueError, match=missing):
        ingest_events.decode(json.dumps(payload).encode("utf-8"))


def test_a_completion_event_arriving_here_is_refused():
    """
    The mistake config/pipeline.yaml guards against by refusing to let the two
    topics share a name -- checked here too, because a topic is a place other
    people can publish to and the guard only covers this project's own config.
    """
    payload = {
        "event": "pipeline.run.completed",
        "version": 1,
        "id": 42,
        "sync_job_id": "494ffa0d-f52d-5294-bf31-da5277d6ac19",
    }

    with pytest.raises(ValueError, match="Something else is publishing"):
        ingest_events.decode(json.dumps(payload).encode("utf-8"))


@pytest.mark.parametrize(
    "row_id", ["42", "1; DROP TABLE raw_transactions", 4.5, None, True]
)
def test_an_id_that_is_not_a_number_is_refused_at_decode(row_id):
    """
    The one that matters. A payload shaped exactly right whose id is a string
    would otherwise reach ``raw.read``, which interpolates ids into SQL --
    that function checks too, and this is the outer of the two checks rather
    than a substitute for it.
    """
    payload = {"event": ingest_events.EVENT, "version": 1, "id": row_id}

    with pytest.raises(ValueError, match="not a row id"):
        ingest_events.decode(json.dumps(payload).encode("utf-8"))


def test_a_message_without_a_table_reads_as_the_default():
    """
    Defaulted rather than required, so a consumer does not reject messages
    written before the field existed.
    """
    payload = {"event": ingest_events.EVENT, "version": 1, "id": 42}

    decoded = ingest_events.decode(json.dumps(payload).encode("utf-8"))

    assert decoded["table"] == "raw_transactions"


# ---------------------------------------------------------------------------
# The producer's argument handling
# ---------------------------------------------------------------------------


def test_ids_can_be_listed_or_ranged():
    assert dummy_producer.parse_ids("42") == [42]
    assert dummy_producer.parse_ids("42,43") == [42, 43]
    assert dummy_producer.parse_ids("40-44") == [40, 41, 42, 43, 44]
    assert dummy_producer.parse_ids("1, 3-5 ,3") == [1, 3, 4, 5]


@pytest.mark.parametrize("text", ["abc", "1-x", "5-1", "1,,x"])
def test_an_unparseable_id_list_is_refused(text):
    with pytest.raises(ValueError):
        dummy_producer.parse_ids(text)


def test_a_dry_run_publishes_nothing_and_prints_the_payload(capsys):
    """
    The flag exists so the message can be read before a broker is involved --
    which is also how you check the topic and the shape without starting
    anything.
    """
    code = dummy_producer.main(["--id", "42", "--dry-run"])

    assert code == 0
    printed = json.loads(capsys.readouterr().out.strip())
    assert printed["id"] == 42
    assert printed["event"] == ingest_events.EVENT
