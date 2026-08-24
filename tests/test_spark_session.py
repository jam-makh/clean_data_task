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

from src.spark import session as session_module


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
