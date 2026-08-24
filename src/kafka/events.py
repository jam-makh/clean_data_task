"""
What a completion event says.

One event type so far -- ``pipeline.run.completed`` -- and the payload is a
``RunResult`` rendered as JSON. Building it is separate from publishing it
because the two fail for entirely different reasons and only one of them needs
a broker: whether the payload is right is a question a fast test can answer,
and whether it reaches Kafka is not.

The contract
------------

Three fields make the event useful rather than merely present:

``sync_job_id``   the join key. Derived from the source's contents, so a
                  consumer can match the event to the rows in Postgres, and so
                  a re-delivered event is recognisable as the same load rather
                  than a second one.
``fingerprint``   which rules produced these numbers. Without it "265,195 rows
                  cleaned" is unreproducible -- the same input under a changed
                  policy is a different answer, and the event would not say so.
``metrics``       the per-stage totals. An event that says only "done" is the
                  version of this feature that looks finished and tells the
                  reader nothing.

``version`` is in the payload as well as in the topic name. The topic's ``.v1``
is the wire contract and changing it means a new topic; the field is for a
consumer that wants to accept several minor shapes on one topic without
parsing the topic name to find out which it has.
"""

import json
from datetime import datetime, timezone

# Bump for a breaking change to the payload, and create a new topic when you
# do -- a consumer reading v1 must never be handed v2 on the same topic.
VERSION = 1

# The event's name, matching the topic minus its version suffix. Stated in the
# payload because a consumer subscribed to several topics should not have to
# know which one a message arrived on to know what it is.
EVENT = "pipeline.run.completed"


def build(result, engine: str = "spark") -> dict:
    """
    Renders a finished run as the event payload.

    :param result: The ``src.runner.RunResult`` for the run.
    :param engine: Which engine produced it. In the payload because the two
        engines write to different places, so "what should I go and read" is
        not answerable from the rest of the event.
    :returns: The payload, as a JSON-serialisable dict.
    """
    return {
        "event": EVENT,
        "version": VERSION,
        # When the event was produced, not when the run started. UTC with an
        # explicit offset: a naive timestamp in a message is a timestamp whose
        # meaning depends on which machine reads it.
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "sync_job_id": result.sync_job_id,
        # str() rather than the Path, which is not JSON-serialisable and would
        # otherwise fail at publish time rather than here.
        "source": str(result.source),
        "profile": result.profile,
        "engine": engine,
        "config_fingerprint": result.fingerprint,
        "rows": {
            "read": result.rows_read,
            "written": result.rows_written,
            "dropped": result.rows_dropped,
        },
        "duration_seconds": round(result.seconds, 3),
        "metrics": _serialisable(result.metrics),
    }


def _serialisable(metrics: dict) -> dict:
    """
    :param metrics: The report's totals, which arrive as whatever type the
        aggregate produced -- including numpy and Java-backed integers that
        ``json.dumps`` refuses.
    :returns: The same mapping with every value in a type JSON has.
    """
    out = {}
    for name, value in metrics.items():
        if isinstance(value, bool) or value is None:
            out[name] = value
        elif isinstance(value, (int, float, str)):
            out[name] = value
        else:
            # int() rather than str(): every metric is a count, and a count
            # that arrived as a numpy int64 is still a number to the consumer.
            try:
                out[name] = int(value)
            except (TypeError, ValueError):
                out[name] = str(value)
    return out


def encode(payload: dict) -> bytes:
    """
    :param payload: An event payload.
    :returns: Its bytes, as they go on the wire.
    :raises TypeError: If the payload holds something JSON cannot carry, which
        is worth failing on here rather than inside a delivery callback on a
        background thread where the traceback belongs to nobody.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def decode(raw: bytes) -> dict:
    """
    :param raw: A message's bytes.
    :returns: The payload.
    :raises ValueError: On anything that is not the JSON object this module
        writes -- which is the consumer's first validation and the reason it
        lives beside the writer rather than in the consumer.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message is not JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(
            f"message is a {type(payload).__name__}, not an event object"
        )
    for required in ("event", "version", "sync_job_id"):
        if required not in payload:
            raise ValueError(f"event is missing {required!r}")
    return payload
