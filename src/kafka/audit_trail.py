"""
Where a message that will not decode goes.

    consumer.decode_all  ->  refuses it, and has nowhere to put it
    audit_trail.record   ->  appends it here, bytes and coordinates intact

The counterpart of the ``status`` column on ``raw_transactions``, for the one
failure that column cannot express. A row that will not *clean* is marked
FAILED with its reason and can be retried by id. A message that will not
*decode* has no id -- that is what is wrong with it -- so there is no row to
mark and no id to re-emit. Before this module it was printed and stepped over,
and the offset was committed: the only record that it ever arrived was a line
on stdout that a restart erased.

What is written is enough to go back to the broker. The bytes, so the message
can be inspected without it; the topic, partition and offset, so the original
can be re-read from the log while it is still within retention; the error, so
a reader knows what this consumer objected to. Base64 for the payload, because
a message that failed to decode is by definition not guaranteed to be UTF-8
and a quarantine file that cannot hold the thing it is quarantining is not
one.

JSON Lines rather than a JSON array. An array would have to be read, parsed
and rewritten whole on every bad message, which is slower and turns a crash
mid-write into a corrupted file rather than a torn last line. Appending means
the damage a crash can do is bounded at one record, and ``tail -f`` works.

Nothing here raises. A full disk or an unwritable path must not turn one
malformed message into a stopped consumer -- that would be strictly worse than
the behaviour this replaces, where the message was at least skipped and the
pipeline carried on. The write is attempted, and a failure is reported to the
caller as False and said out loud once.

Local to whoever wrote it. This is a file on the consumer's own filesystem, so
it does not survive a container rebuild unless the configured path is on a
mounted volume, and two consumer replicas would keep two files that nobody
joins. Both are acceptable for a single-consumer pipeline and neither is
acceptable silently, which is why they are written down here and in
``config/pipeline.yaml``.
"""

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_PATH = "data/audit_trail/undecodable.jsonl"


def build(message, error: str) -> dict:
    """
    :param message: The Kafka message that would not decode.
    :param error: What ``ingest_events.decode`` objected to.
    :returns: The record to append.

    Every field is read through ``getattr`` with a fallback. The client's
    message type is not the only thing that reaches here -- a test's fake is
    the other -- and a quarantine record that raised ``AttributeError`` while
    describing a malformed message would replace a small problem with a larger
    one.
    """
    value = _call(message, "value")
    return {
        # When this consumer saw it, not when it was produced. The producer's
        # own timestamp is on the broker; this is the one that lines up with
        # the consumer's log.
        "at": datetime.now(timezone.utc).isoformat(),
        "topic": _call(message, "topic"),
        "partition": _call(message, "partition"),
        # The three together are the address of the original in the log, and
        # the reason this file is a pointer rather than only a copy.
        "offset": _call(message, "offset"),
        "error": error,
        "bytes": len(value) if isinstance(value, (bytes, bytearray)) else None,
        "value_b64": (
            base64.b64encode(bytes(value)).decode("ascii")
            if isinstance(value, (bytes, bytearray))
            else None
        ),
    }


def record(message, error: str, path=None) -> bool:
    """
    Appends one quarantine record.

    :param message: The message that would not decode.
    :param error: What was wrong with it.
    :param path: Where to append; the configured default when absent.
    :returns: True if it is on disk, False if the write failed -- reported
        rather than raised, for the reason in the module docstring. A caller
        that gets False has a message it cannot account for and should say so.

    The parent directory is created rather than assumed. The first undecodable
    message may well arrive on a machine where nothing has written here yet,
    and failing to quarantine it because a directory was missing would lose
    exactly the message this exists to keep.
    """
    path = Path(path if path is not None else DEFAULT_PATH)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(build(message, error), ensure_ascii=False)
        # One open per record, and flushed by the close. A handle held open
        # across the loop would be faster and would also mean a killed
        # consumer loses whatever was still in the buffer -- which is the
        # opposite of the point.
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        return True
    except Exception:
        return False


def _call(message, name):
    """
    :param message: A Kafka message, or anything shaped enough like one.
    :param name: A method to call on it.
    :returns: What it returned, or None if it has no such method or the call
        failed.
    """
    method = getattr(message, name, None)
    if method is None:
        return None
    try:
        return method()
    except Exception:
        return None
