"""
The session factory: what it intends, and what the JVM actually applies.

Split in two on purpose. ``settings()`` is a pure function and its tests cost
nothing, so the settings that change *results* are asserted by name -- if
someone removes the time zone pin or lets ANSI mode back to its default, a
test says so in milliseconds rather than a parity run saying so in minutes
and blaming a stage.

The second half starts a real session and asks it what it ended up with,
because a config key Spark silently ignored is indistinguishable from one it
honoured until you ask.
"""

import pytest

from src.spark import spark_setup as session_module


def test_settings_pin_the_semantics():
    """
    The four settings that change answers rather than speed are stated.

    Asserted against literal values, not against the module constants. A test
    that read the constant would pass no matter what the constant became,
    which makes it a test of nothing at all.
    """
    config = session_module.settings()

    assert config["spark.sql.session.timeZone"] == "UTC"
    assert config["spark.sql.ansi.enabled"] == "false"
    assert config["spark.sql.legacy.timeParserPolicy"] == "CORRECTED"
    assert config["spark.sql.shuffle.partitions"] == "8"


def test_settings_take_overrides():
    """A caller can vary one setting without restating the rest."""
    config = session_module.settings(**{"spark.sql.shuffle.partitions": 1})

    assert config["spark.sql.shuffle.partitions"] == "1"
    # Everything else survives, and the override is stringified -- Spark's
    # config API rejects an int, and a caller passing one should not have to
    # know that.
    assert config["spark.sql.session.timeZone"] == "UTC"


def test_jars_go_on_the_classpath_not_the_distribution_list():
    """
    The JDBC driver is referenced where it lies, never copied.

    ``spark.jars`` copies each jar into the session's temp directory, and on
    Windows the JVM holds that copy open past the shutdown hook that tries to
    delete it -- so every run ends in an IOException that has nothing to do
    with the run. This asserts the mechanism, not just the presence.
    """
    config = session_module.settings()

    if not any(session_module.JARS_DIR.glob("*.jar")):
        pytest.skip("no jars/ to configure")

    assert "spark.jars" not in config
    assert "postgresql" in config["spark.driver.extraClassPath"]
    assert (
        config["spark.driver.extraClassPath"]
        == config["spark.executor.extraClassPath"]
    )


@pytest.mark.spark
def test_live_session_honours_the_pins(spark):
    """
    The JVM agrees with the dict.

    Worth its startup cost because "accepted" and "applied" are different
    things in Spark: a misspelled key is stored and ignored, and driver memory
    is stored, reported back, and has no effect on the heap in local mode.
    Reading the settings back from the live session is the only way to tell
    the first case from a working one.
    """
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"
    assert spark.conf.get("spark.sql.legacy.timeParserPolicy") == "CORRECTED"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "8"


@pytest.mark.spark
def test_second_call_returns_the_same_session(spark):
    """
    One JVM per process, which is the only arrangement that works.

    Also the reason the fixture is session-scoped: a caller asking for a
    session mid-suite must get the one already running rather than a second
    one that cannot exist.
    """
    assert session_module.session("something-else") is spark


def test_settings_bound_what_a_never_ending_session_accumulates():
    """
    The four retention keys are capped, and capped well below Spark's 1000.

    Asserted because nothing in this project ever reads Spark's own history
    back, so nothing else would notice if a default crept in -- until a
    consumer that had been up for an hour died of a heap full of the recorded
    plans of jobs that finished long ago. That failure is expensive to
    diagnose and this test is free.
    """
    config = session_module.settings()

    for key in (
        "spark.ui.retainedJobs",
        "spark.ui.retainedStages",
        "spark.ui.retainedTasks",
        "spark.sql.ui.retainedExecutions",
    ):
        assert int(config[key]) < 1000, f"{key} is back at Spark's default"


class _Boom(Exception):
    """Stands in for a Py4JJavaError, which needs a JVM to construct."""


def test_a_heap_error_is_fatal_however_deeply_it_is_wrapped():
    """
    ``is_fatal`` reads the cause chain, not just the exception in hand.

    The case that matters is the wrapped one: Spark reports an OOM as a job
    failure whose *message* names the OutOfMemoryError, and a check that only
    looked at the outermost type would call that an ordinary bad row and let
    the consumer carry on against a dead driver.
    """
    inner = _Boom("java.lang.OutOfMemoryError: Java heap space")
    outer = _Boom("Job aborted due to stage failure")
    outer.__cause__ = inner

    assert session_module.is_fatal(inner)
    assert session_module.is_fatal(outer)


def test_an_ordinary_failure_is_not_fatal():
    """
    The guard has to be narrow, or it converts every bad row into a stopped
    consumer -- which is the failure mode it exists to prevent, inverted.
    """
    assert not session_module.is_fatal(_Boom("no such column: TXN_SEQ"))
    assert not session_module.is_fatal(ValueError("no rows with id in [7]"))


def test_a_cycle_in_the_cause_chain_does_not_hang():
    """
    A self-referential ``__context__`` is rare and reachable -- exceptions
    raised while handling each other chain both ways -- and a walk that did
    not remember where it had been would spin forever inside a log line.
    """
    first = _Boom("one")
    second = _Boom("two")
    first.__cause__ = second
    second.__cause__ = first

    assert not session_module.is_fatal(first)
