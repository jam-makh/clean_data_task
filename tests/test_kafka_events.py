"""
The completion event's payload, checked without a broker.

Kept separate from ``test_kafka_producer.py`` for the reason the db tests are
split the same way: whether the payload says the right thing and whether it
reaches Kafka are different questions with different failure modes, and only
one of them needs a container. This one answers in milliseconds, so it runs on
every suite.
"""

import json
from pathlib import Path

import pytest

from src.kafka import events
from src.runner import RunResult
from src.utils.report import CleaningReport


def result_for(**overrides) -> RunResult:
    """:returns: A RunResult standing in for a finished run."""
    report = CleaningReport()
    report.record("pipeline", "input_rows", 11417)
    report.record("duplicates", "exact_duplicate_rows", 3)

    defaults = dict(
        sync_job_id="494ffa0d-f52d-5294-bf31-da5277d6ac19",
        source=Path("data/raw/forecast_balance_data.csv"),
        profile="forecast_balance",
        rows_read=11417,
        rows_written=11414,
        report=report,
        fingerprint="abc12345",
        seconds=42.4242,
        metrics={"input_rows": 11417, "output_rows": 11414},
    )
    return RunResult(**{**defaults, **overrides})


def test_the_payload_carries_the_three_fields_that_make_it_useful():
    """
    The join key, the rules that produced the numbers, and the numbers. An
    event missing any one of them is present without being useful: no key and
    it cannot be matched to the rows, no fingerprint and the totals are
    unreproducible, no metrics and it says only "done".
    """
    payload = events.build(result_for())

    assert payload["sync_job_id"] == "494ffa0d-f52d-5294-bf31-da5277d6ac19"
    assert payload["config_fingerprint"] == "abc12345"
    assert payload["metrics"]["output_rows"] == 11414


def test_the_row_counts_are_stated_three_ways():
    payload = events.build(result_for())

    assert payload["rows"] == {"read": 11417, "written": 11414, "dropped": 3}


def test_a_dry_run_says_written_none_rather_than_zero():
    """
    Zero would claim the write happened and produced nothing. None says it did
    not happen, which is a different fact and the one a consumer needs.
    """
    payload = events.build(result_for(rows_written=None))

    assert payload["rows"]["written"] is None


def test_the_source_is_a_string_not_a_path():
    """
    ``Path`` is not JSON-serialisable, so leaving it would fail at publish
    time -- inside a delivery callback on a background thread, where the
    traceback belongs to nobody.
    """
    payload = events.build(result_for())

    assert isinstance(payload["source"], str)
    json.dumps(payload)


def test_the_timestamp_carries_its_offset():
    """
    A naive timestamp in a message is one whose meaning depends on which
    machine reads it.
    """
    payload = events.build(result_for())

    assert payload["occurred_at"].endswith("+00:00")


def test_the_version_is_in_the_payload_as_well_as_the_topic():
    payload = events.build(result_for())

    assert payload["version"] == events.VERSION
    assert payload["event"] == events.EVENT


class Awkward:
    """A metric value of a type json refuses -- a Java-backed integer, say."""

    def __int__(self):
        return 7


def test_a_metric_json_cannot_carry_is_coerced_rather_than_dropped():
    """
    Spark aggregates come back as whatever the JVM produced. Dropping such a
    metric would silently shrink the report; failing on it would lose the
    event for a run that succeeded.
    """
    payload = events.build(result_for(metrics={"odd": Awkward()}))

    assert payload["metrics"]["odd"] == 7
    json.dumps(payload)


def test_a_metric_that_cannot_even_be_an_int_becomes_text():
    payload = events.build(result_for(metrics={"label": object()}))

    assert isinstance(payload["metrics"]["label"], str)
    json.dumps(payload)


def test_none_and_booleans_survive_as_themselves():
    """
    ``int(True)`` is 1, so a naive coercion would turn a flag into a count.
    """
    payload = events.build(
        result_for(metrics={"flag": True, "absent": None, "n": 3})
    )

    assert payload["metrics"] == {"flag": True, "absent": None, "n": 3}


def test_encoding_round_trips():
    payload = events.build(result_for())

    assert events.decode(events.encode(payload)) == payload


def test_encoding_is_stable_for_the_same_payload():
    """
    Keys are sorted, so two encodings of one payload are byte-identical --
    which is what lets a consumer dedupe on the bytes if it wants to, and what
    keeps a diff of two events readable.
    """
    payload = events.build(result_for())

    assert events.encode(payload) == events.encode(payload)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (b"not json at all", "not JSON"),
        (b"[1, 2, 3]", "not an event object"),
        (b'{"event": "x", "version": 1}', "sync_job_id"),
        (b'{"sync_job_id": "x", "version": 1}', "event"),
    ],
)
def test_decode_rejects_what_is_not_an_event(raw, expected):
    """
    The consumer's first validation, and it lives beside the writer so the two
    cannot drift. A malformed message has to be rejected by name rather than
    raising a KeyError three functions later.
    """
    with pytest.raises(ValueError, match=expected):
        events.decode(raw)
