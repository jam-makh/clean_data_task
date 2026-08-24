"""
The stage narration: does it say the right things, and does it stay out of the
way?

Two halves again. The formatting and the listener contract are decidable
without a JVM -- ``pipeline.run`` calls two methods in a known order, and the
lines are strings -- so those tests run on every suite. The one test that
proves the numbers are real needs Spark and is marked accordingly.

The property most worth protecting is the boring one: with no listener,
``pipeline.run`` is exactly the function it was. The parity harness compares
cleaning, and a logging hook that changed the frame, the order, or the failure
behaviour would be a hook that broke the thing it was added to observe.
"""

import pytest

from src.spark import stagelog


class Recorder:
    """A listener that remembers rather than printing."""

    def __init__(self):
        self.calls = []

    def starting(self, name, position, total):
        self.calls.append(("starting", name, position, total))

    def finished(self, name, frame):
        self.calls.append(("finished", name, frame))


@pytest.fixture
def lines():
    """:returns: A list, and a log that writes into it."""
    written = []
    return written, stagelog.StageLog(write=written.append, counts=False)


# ---------------------------------------------------------------------------
# The layer map
# ---------------------------------------------------------------------------


def test_every_ported_stage_has_a_layer():
    """
    A stage with no layer still logs -- it lands in ``DEFAULT_LAYER`` -- so
    this cannot be an assertion inside the log. It is here, where forgetting
    to classify a newly ported stage is caught by the suite rather than by
    someone reading the output and wondering.
    """
    pytest.importorskip("pyspark")
    from src.spark.pipeline import SPARK_STEP_REGISTRY

    unclassified = sorted(
        name for name in SPARK_STEP_REGISTRY if name not in stagelog.LAYERS
    )

    assert not unclassified, (
        f"{unclassified} would log as {stagelog.DEFAULT_LAYER}; give them a "
        f"layer in stagelog.LAYERS"
    )


def test_the_two_ends_are_layers_too():
    """
    The read and the write are not pipeline steps, but they are part of the
    journey a row makes and the consumer logs them through the same call.
    """
    assert stagelog.LAYERS["read"] == "INGESTION"
    assert stagelog.LAYERS["write"] == "PERSISTENCE"


def test_the_layer_column_is_wide_enough_for_every_layer():
    """
    A label longer than the column does not truncate, it pushes the rest of
    the line out of alignment -- and the whole value of a column of layers is
    that it can be read down.
    """
    widest = max(len(layer) for layer in stagelog.LAYERS.values())

    assert widest <= stagelog.LAYER_WIDTH
    assert len(stagelog.DEFAULT_LAYER) <= stagelog.LAYER_WIDTH


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def test_a_stage_line_names_its_layer_and_stage(lines):
    written, log = lines

    log.finished("timestamps", frame=None)

    assert len(written) == 1
    assert "[NORMALIZATION]" in written[0]
    assert "timestamps" in written[0]


def test_an_unclassified_stage_still_logs(lines):
    written, log = lines

    log.finished("something_new", frame=None)

    assert stagelog.DEFAULT_LAYER in written[0]


def test_one_row_is_not_1_rows(lines):
    written, log = lines

    log.event("read", "read raw_transactions", rows=1)
    log.event("read", "read raw_transactions", rows=4)

    assert "1 row " in written[0] + " "
    assert "4 rows" in written[1]


def test_a_large_count_is_grouped(lines):
    """265195 is unreadable at a glance and 265,195 is not."""
    written, log = lines

    log.event("write", "upsert", rows=265195)

    assert "265,195 rows" in written[0]


def test_counts_off_means_no_number_and_no_time(lines):
    """
    A timing with no action in it measures how long Spark took to *plan* the
    step. That is a real number which means nothing, and printing it invites
    it to be read as though it did.
    """
    written, log = lines

    log.finished("codes", frame=None)

    assert "s" not in written[0].split("codes")[1], written[0]
    assert "row" not in written[0]


def test_the_output_is_ascii(lines):
    """
    Not a style preference. This project's terminal writes through cp1252, and
    a line-drawing character reaching it raises UnicodeEncodeError from inside
    the print -- taking down a running consumer over a decoration.
    """
    written, log = lines

    log.opening("raw id 42 | job abc123")
    log.event("read", "read raw_transactions", rows=1)
    log.finished("timestamps", frame=None)
    log.note("something worth saying")
    log.closing("1 row cleaned")

    for line in written:
        line.encode("cp1252")  # raises if anything here is undrawable
        assert line.isascii()


def test_a_long_metric_name_is_truncated_rather_than_wrapped():
    assert stagelog._truncate("short", 10) == "short"
    assert stagelog._truncate("x" * 20, 10) == "xxxxxxx..."
    assert len(stagelog._truncate("x" * 200, stagelog.METRIC_WIDTH)) == (
        stagelog.METRIC_WIDTH
    )


def test_the_footer_reports_elapsed_time(lines):
    written, log = lines

    log.opening("a run")
    log.closing("2 rows cleaned")

    assert "2 rows cleaned" in written[-1]
    assert "s " in written[-1] or written[-1].endswith("s")


def test_closing_without_opening_does_not_raise(lines):
    """
    The consumer's error path calls the footer from a place the header may not
    have been reached -- and a log that raised while reporting a failure would
    replace the failure with its own.
    """
    written, log = lines

    log.closing("failed")

    assert "failed" in written[-1]


# ---------------------------------------------------------------------------
# The listener contract
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_registry(monkeypatch):
    """
    Replaces two stages with functions that return the frame unchanged, so the
    loop can be driven without a JVM. What is under test is the calling, not
    the cleaning.
    """
    pytest.importorskip("pyspark")
    from src.spark import pipeline

    monkeypatch.setitem(pipeline.SPARK_STEP_REGISTRY, "codes", lambda f, p: f)
    monkeypatch.setitem(pipeline.SPARK_STEP_REGISTRY, "geo", lambda f, p: f)
    return pipeline


def test_the_listener_is_told_about_each_step_in_order(stub_registry):
    listener = Recorder()
    frame = object()

    stub_registry.run(frame, ["codes", "geo"], policy=object(), listener=listener)

    assert [call[:2] for call in listener.calls] == [
        ("starting", "codes"),
        ("finished", "codes"),
        ("starting", "geo"),
        ("finished", "geo"),
    ]


def test_the_listener_is_told_where_it_is(stub_registry):
    """
    Position and total, so a log can say "3 of 11" without counting for
    itself -- and so it cannot disagree with the run about how many there are.
    """
    listener = Recorder()

    stub_registry.run(object(), ["codes", "geo"], policy=object(), listener=listener)

    starts = [call for call in listener.calls if call[0] == "starting"]
    assert [(c[2], c[3]) for c in starts] == [(1, 2), (2, 2)]


def test_the_listener_sees_the_frame_the_step_produced(stub_registry):
    listener = Recorder()
    produced = object()
    stub_registry.SPARK_STEP_REGISTRY["codes"] = lambda f, p: produced

    stub_registry.run(object(), ["codes"], policy=object(), listener=listener)

    finished = [c for c in listener.calls if c[0] == "finished"][0]
    assert finished[2] is produced


def test_without_a_listener_the_run_is_what_it_was(stub_registry):
    """
    The property the parity harness depends on. No listener, no calls, no
    change to the frame or the order -- the hook must be invisible when unused.
    """
    frame = object()

    assert stub_registry.run(frame, [], policy=object()) is frame
    assert stub_registry.run(frame, ["codes"], policy=object()) is frame


# ---------------------------------------------------------------------------
# Against a real frame
# ---------------------------------------------------------------------------


@pytest.mark.spark
def test_the_numbers_are_the_stage_s_own(spark):
    """
    The one test that proves the log is measuring rather than decorating.

    A four-row frame with two byte-identical rows: ``duplicates`` drops one,
    and the log has to say so -- both in the row count and in the stage's own
    metric. If the measurement were fake, or read off the wrong frame, this is
    where it shows.
    """
    from src.config.policy import load as load_policy
    from src.spark import pipeline

    rows = [
        ("u1", "a1", "t1", "1", "2022-01-01 07:11:25", "03-Jan-22"),
        ("u1", "a1", "t1", "1", "2022-01-01 07:11:25", "03-Jan-22"),
        ("u2", "a2", "t2", "2", "2022-01-02 08:00:00", "04-Jan-22"),
        ("u3", "a3", "t3", "3", "2022-01-03 09:00:00", "05-Jan-22"),
    ]
    frame = spark.createDataFrame(
        rows,
        "USER_ID string, ACCOUNT_ID string, TXN_ID string, TXN_SEQ string, "
        "TXN_DATE_TIME string, SETTLE_DATE string",
    )

    written = []
    log = stagelog.StageLog(write=written.append, counts=True,
                            policy=load_policy())
    pipeline.run(frame, ["duplicates"], policy=load_policy(), listener=log)

    stage_line = next(line for line in written if "duplicates" in line)
    assert "[DEDUPLICATION]" in stage_line
    assert "3 rows" in stage_line, (
        f"four rows in, one an exact duplicate; the log says: {stage_line}"
    )
    assert any("duplicate" in line.lower() for line in written), (
        f"the stage's own metric is missing from {written}"
    )


@pytest.mark.spark
def test_a_measurement_failure_does_not_fail_the_run(spark, monkeypatch):
    """
    Logging is not allowed to break a pipeline. A stage log that raised would
    turn a working run into a failed one over output nobody's data depends on
    -- but it must say that it could not measure, because a log that quietly
    stops reporting is worse than one that admits it.
    """
    from src.spark import audit, pipeline

    def explode(*args, **kwargs):
        raise RuntimeError("no")

    monkeypatch.setattr(audit, "collect", explode)

    frame = spark.createDataFrame([("t1",)], "TXN_ID string")
    written = []
    log = stagelog.StageLog(write=written.append, counts=True, policy=object())

    log.finished("duplicates", frame)

    assert any("metrics unavailable" in line for line in written)
    assert any("duplicates" in line for line in written)
    del pipeline
