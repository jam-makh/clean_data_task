"""
One place that decides what a Spark session in this project means.

Every entry point -- the pipeline, a test, a notebook -- goes through here, so
none of them can disagree about the settings that change results rather than
speed. That distinction is the reason this module exists at all. A session
built ad hoc in a test would still run, and would still be wrong in ways that
never announce themselves: a different session time zone shifts every parsed
timestamp by an offset, a different ANSI setting turns a null into an
exception, a different date parser turns an unparseable string into a
silently wrong date. None of those surface as an error; they surface as a
parity failure three phases later, attributed to the wrong stage.

The settings below are therefore stated rather than inherited, including the
ones whose stated value equals today's default. Inheriting a default is a bet
that it will not change between Spark versions, and two of these have already
changed once.

They live as module constants rather than in ``config/pipeline.yaml`` for the
reason that file's own header gives for the database and Kafka sections: there
is exactly one environment to configure right now, and a config key nothing
varies is a claim the project cannot keep. When a second environment exists --
a cluster master instead of ``local[*]`` -- this grows a ``spark:`` section
and these become its defaults.
"""

import os
import sys
from pathlib import Path

# local[*] rather than a fixed core count: the point of local mode here is to
# use the machine, and the shuffle width below is what actually governs how
# the work gets cut up.
LOCAL_MASTER = "local[*]"

# Spark's default is 200. On 265k rows that is 200 tasks over a few hundred
# kilobytes each, and scheduling overhead dominates the work -- every local
# run becomes slow enough to discourage the iteration this phase depends on.
# 8 is chosen against the data rather than the machine: it keeps a partition
# large enough to be worth a task, while staying at or above the core count so
# no core idles at the tail of a stage.
SHUFFLE_PARTITIONS = "8"

# Fixed, and deliberately not the machine's zone. Every timestamp this
# pipeline parses is interpreted in the session time zone, so leaving it as
# "whatever the host says" makes the cleaned output a property of the laptop
# that produced it: two runs in two zones differ by hours and nothing reports
# it. UTC because the source's offsets are recorded explicitly in
# TXN_TS_UTC_OFFSET, which only means anything against a fixed base.
SESSION_TIMEZONE = "UTC"

# The driver holds a whole frame in memory whenever the parity harness
# collects one, so this is sized for the harness rather than for the cleaning.
DRIVER_MEMORY = "4g"

# ANSI mode decides what an impossible cast does. Spark 4 turns it ON by
# default, which makes ``cast`` raise on the first unparseable value instead
# of returning null -- and this pipeline's whole contract is that an
# unreadable value is COUNTED, not fatal. pandas spells that
# ``errors="coerce"``; the equivalent here is a non-ANSI cast, and it is set
# explicitly because this default flipped between Spark 3 and Spark 4 and
# could flip again.
#
# Turning it off does not make unparseable values invisible: they become null,
# and a null is exactly what every stage's diagnostic column already counts.
# ``try_cast`` stays available where the intent is worth spelling out at the
# call site.
ANSI_ENABLED = "false"

# CORRECTED is the strict date parser: a string that does not match the
# pattern yields null rather than being coaxed into a date. LEGACY would
# accept "2022-13-45" and produce something. Requirement 2 -- unparseable rows
# are counted, not guessed -- is only enforceable under CORRECTED, so it is
# named here rather than left to a default that currently agrees with it.
TIME_PARSER_POLICY = "CORRECTED"

# Where the JDBC driver lives. Attached when present rather than required,
# because Phase 01 has nothing to connect to yet -- but named here rather than
# in the Phase 06 writer, so a session built by a test and a session built by
# the pipeline can reach the same jars.
JARS_DIR = Path("jars")

APP_NAME = "transaction-cleaning"


def _repair_windows_native_path() -> None:
    r"""
    Puts ``%HADOOP_HOME%\bin`` on PATH so the JVM can find ``hadoop.dll``.

    HADOOP_HOME tells Spark where ``winutils.exe`` is. It does not tell the
    JVM where the native library is: that is loaded through
    ``java.library.path``, which on Windows is seeded from PATH. Set one
    without the other and file operations fail with an UnsatisfiedLinkError
    naming a Java method -- which reads as a version mismatch and is really a
    library nobody pointed at.

    Repaired rather than reported, for the reason ``scripts/verify_env``
    repairs it: requiring a correctly ordered PATH on every machine that runs
    this is a setup instruction that will be missed, while deriving it from
    HADOOP_HOME, which is already mandatory, cannot be.
    """
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        return
    native = str(Path(hadoop_home, "bin"))
    if native.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + native


def _pin_python_interpreter() -> None:
    """
    Points both ends of the Python bridge at the interpreter running now.

    Spark's executors launch workers by running ``python3``, which does not
    exist on Windows -- the executable is ``python.exe``. The JVM starts, the
    job plans, and then every task dies with CreateProcess error=2 naming a
    program nobody configured.

    Pinning both settles a second question at the same time: the workers run
    THIS virtualenv's interpreter rather than whichever Python happens to be
    first on PATH and may have none of the project's dependencies.
    ``setdefault``, so a deliberate override survives.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _pin_driver_memory(memory: str) -> None:
    """
    Asks for driver heap in the one way that works in local mode.

    ``spark.driver.memory`` on the builder is read by the cluster launcher
    when it starts a driver JVM elsewhere. In local mode the driver JVM is
    THIS process's JVM, and it has already been launched by the time builder
    configs are applied -- so the setting is accepted, reported back by
    ``spark.conf.get``, and has no effect on the heap. The heap is fixed at
    gateway launch, which reads ``PYSPARK_SUBMIT_ARGS``.

    Both are set anyway: the environment variable to make it true, the config
    key so that anything reading the session's configuration back sees the
    same number rather than a stale default.

    :param memory: A JVM size string, e.g. ``"4g"``.
    """
    # The trailing `pyspark-shell` is not decoration: the launcher treats the
    # last token as the application to submit, and omitting it makes the
    # gateway fail to start complaining about a missing primary resource.
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS", f"--driver-memory {memory} pyspark-shell"
    )


def settings(**overrides) -> dict[str, str]:
    """
    :param overrides: Any ``spark.*`` key to replace, for a caller that needs
        to vary one setting without restating the other eight. Keys are given
        as keyword arguments with dots intact via ``**{...}``.
    :returns: The full configuration a session is built from, as a plain dict
        -- so a test can assert on what the project *intends* without paying
        for a JVM to tell it.
    """
    config = {
        "spark.sql.shuffle.partitions": SHUFFLE_PARTITIONS,
        "spark.sql.session.timeZone": SESSION_TIMEZONE,
        "spark.driver.memory": DRIVER_MEMORY,
        "spark.sql.ansi.enabled": ANSI_ENABLED,
        "spark.sql.legacy.timeParserPolicy": TIME_PARSER_POLICY,
        # Arrow is what makes the parity harness affordable: every comparison
        # crosses the JVM/Python boundary through toPandas(), which without
        # Arrow moves rows one pickled tuple at a time. Fallback stays on so
        # an unsupported type degrades to the slow path instead of failing.
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        # Off because it binds a port per session, and a suite that starts and
        # stops sessions then spends its time on port conflicts and firewall
        # prompts instead of on tests.
        "spark.ui.enabled": "false",
    }

    jars = sorted(JARS_DIR.glob("*.jar")) if JARS_DIR.is_dir() else []
    if jars:
        # extraClassPath rather than `spark.jars`, which is the more usual
        # answer and the wrong one here. `spark.jars` *distributes* a jar: it
        # copies each one into the session's temp directory so executors on
        # other machines can fetch it. In local mode there are no other
        # machines, so the copy buys nothing -- and on Windows the JVM keeps
        # the copy open, so the shutdown hook cannot delete it and every
        # single run ends in a forty-line IOException stack trace that means
        # nothing. extraClassPath points at the jar where it already is.
        #
        # Both ends, because in local mode they are the same JVM and stating
        # only one is a difference that would appear the moment this session
        # is pointed at a real cluster.
        classpath = os.pathsep.join(str(jar.resolve()) for jar in jars)
        config["spark.driver.extraClassPath"] = classpath
        config["spark.executor.extraClassPath"] = classpath

    config.update({key: str(value) for key, value in overrides.items()})
    return config


def session(app_name: str = APP_NAME, **overrides):
    """
    Builds -- or returns -- the session every entry point shares.

    ``getOrCreate`` means the second caller in a process gets the first
    caller's session, overrides and all. That is deliberate: one JVM per
    process is the only arrangement that works, and a suite that built a
    session per test module would spend most of its runtime starting JVMs. It
    is also why the overrides that only take effect at JVM start -- driver
    memory above all -- belong to whoever gets there first.

    :param app_name: Name Spark's own logs and UI report the run under.
    :param overrides: ``spark.*`` settings replacing the module defaults.
    :returns: The configured ``SparkSession``.
    """
    from pyspark.sql import SparkSession

    _repair_windows_native_path()
    _pin_python_interpreter()

    config = settings(**overrides)
    _pin_driver_memory(config["spark.driver.memory"])

    builder = SparkSession.builder.master(LOCAL_MASTER).appName(app_name)
    for key, value in config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    # WARN is Spark's default and it narrates every stage of every job, which
    # buries a test failure under progress bars. Applied after creation
    # because it is a property of the context rather than of the builder.
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop() -> None:
    """
    Stops the active session, if there is one.

    Idempotent, so a teardown can call it without first asking whether a
    session was ever built -- which is the state a skipped test leaves behind.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
