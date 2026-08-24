"""
Everything that happens once per run, before a stage sees a row: how a Spark
session is configured, and how the source file is read into it.

One place, because every entry point -- pipeline, test, notebook -- must agree
on the settings that change *results* rather than speed. A session built ad
hoc in a test would run and be wrong silently: a different session time zone
shifts every parsed timestamp, a different ANSI setting turns a null into an
exception. Those surface as a parity failure three stages later, attributed to
the wrong stage.

Settings are stated rather than inherited, including ones whose stated value
equals today's default -- inheriting a default is a bet it will not change
between Spark versions, and two of these have already changed once.
"""

import csv
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Session configuration
# ---------------------------------------------------------------------------

LOCAL_MASTER = "local[*]"
APP_NAME = "transaction-cleaning"

# Spark's default is 200. On 265k rows that is 200 tasks over a few hundred
# kilobytes each, and scheduling overhead dominates. 8 is chosen against the
# data: large enough to be worth a task, at or above the core count so no core
# idles at the tail of a stage.
SHUFFLE_PARTITIONS = "8"

# Fixed, and deliberately not the machine's zone -- otherwise the cleaned
# output is a property of the laptop that produced it. UTC because the
# source's offsets are recorded explicitly in TXN_TS_UTC_OFFSET, which only
# means anything against a fixed base.
SESSION_TIMEZONE = "UTC"

# Sized for the parity harness, which collects whole frames to the driver.
DRIVER_MEMORY = "4g"

# ANSI mode decides what an impossible cast does. Spark 4 turns it ON by
# default, which makes `cast` raise on the first unparseable value instead of
# returning null -- and this pipeline's contract is that an unreadable value is
# COUNTED, not fatal. pandas spells that `errors="coerce"`; the equivalent here
# is a non-ANSI cast. Set explicitly because this default flipped between
# Spark 3 and 4 and could flip again. Values become null, which is exactly what
# every stage's diagnostic column already counts.
ANSI_ENABLED = "false"

# CORRECTED is the strict date parser: a string that does not match the pattern
# yields null rather than being coaxed into a date. LEGACY would accept
# "2022-13-45" and produce something. Requirement 2 -- unparseable rows are
# counted, not guessed -- is only enforceable under CORRECTED.
TIME_PARSER_POLICY = "CORRECTED"

# How long a freshly spawned Python worker gets to open its socket back to the
# driver before Spark gives up on it. The default is 15s, which is generous on
# Linux and tight here: `local[*]` spawns one worker per core at once, Windows
# process creation is slow, and there is no Unix-domain-socket path on this
# platform -- Spark 4's `spark.python.unix.domain.socket.enabled` is a POSIX
# option, so every worker goes through loopback TCP. A cold start under that
# contention overruns 15s and the worker is killed mid-handshake, which
# surfaces as CANNOT_OPEN_SOCKET followed by "Python worker exited
# unexpectedly" -- an infrastructure failure wearing the costume of a job
# failure. Raised rather than removed: a worker that genuinely cannot connect
# should still fail, just not one that was merely slow to start.
WORKER_SOCKET_TIMEOUT = "120"

# What a crashed Python worker reports on its way out. Without this the JVM can
# only say "exited unexpectedly (crashed)", because the worker died before it
# could send anything back -- the Python-side traceback is lost with the
# process. With it, faulthandler dumps that traceback into the executor log.
# On permanently, not just while debugging: this failure mode is invisible by
# construction, and the cost is a signal handler installed per worker.
WORKER_FAULTHANDLER = "true"

# Attached when present rather than required, so a session built by a test and
# one built by the pipeline reach the same jars.
JARS_DIR = Path("jars")


def _repair_windows_native_path() -> None:
    r"""
    Puts ``%HADOOP_HOME%\bin`` on PATH so the JVM can find ``hadoop.dll``.

    HADOOP_HOME tells Spark where ``winutils.exe`` is. It does not tell the JVM
    where the native library is: that is loaded through ``java.library.path``,
    seeded on Windows from PATH. Set one without the other and file operations
    fail with an UnsatisfiedLinkError naming a Java method -- which reads as a
    version mismatch and is really a library nobody pointed at.
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
    exist on Windows -- every task dies with CreateProcess error=2 naming a
    program nobody configured. Pinning also settles which interpreter: THIS
    virtualenv's, rather than whichever Python is first on PATH and may have
    none of the project's dependencies. ``setdefault``, so an override
    survives.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _pin_driver_memory(memory: str) -> None:
    """
    Asks for driver heap in the one way that works in local mode.

    ``spark.driver.memory`` on the builder is read by the cluster launcher when
    it starts a driver JVM elsewhere. In local mode the driver JVM is THIS
    process's, already launched by the time builder configs apply -- so the
    setting is accepted, reported back by ``spark.conf.get``, and has no effect
    on the heap. The heap is fixed at gateway launch, which reads
    ``PYSPARK_SUBMIT_ARGS``. Both are set: the variable to make it true, the
    config key so anything reading the session back sees the same number.

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
    :param overrides: Any ``spark.*`` key to replace, given as keyword
        arguments with dots intact via ``**{...}``.
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
        # Arrow moves rows one pickled tuple at a time. Fallback stays on so an
        # unsupported type degrades to the slow path instead of failing.
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        # Off because it binds a port per session, and a suite that starts and
        # stops sessions then spends its time on port conflicts and firewall
        # prompts instead of on tests.
        "spark.ui.enabled": "false",
        # Both concern the Python worker processes rather than the JVM, and
        # both exist because a worker that dies is otherwise unattributable.
        "spark.python.authenticate.socketTimeout": WORKER_SOCKET_TIMEOUT,
        "spark.python.worker.faulthandler.enabled": WORKER_FAULTHANDLER,
    }

    jars = sorted(JARS_DIR.glob("*.jar")) if JARS_DIR.is_dir() else []
    if jars:
        # extraClassPath rather than `spark.jars`, which is the more usual
        # answer and the wrong one here. `spark.jars` *distributes* a jar:
        # it copies each one into the session's temp directory so executors on
        # other machines can fetch it. In local mode there are no other
        # machines, so the copy buys nothing -- and on Windows the JVM keeps the
        # copy open, so the shutdown hook cannot delete it and every run ends in
        # a forty-line IOException that means nothing. extraClassPath points at
        # the jar where it already is.
        #
        # Both ends, because in local mode they are the same JVM and stating
        # only one is a difference that would appear the moment this session is
        # pointed at a real cluster.
        classpath = os.pathsep.join(str(jar.resolve()) for jar in jars)
        config["spark.driver.extraClassPath"] = classpath
        config["spark.executor.extraClassPath"] = classpath

    config.update({key: str(value) for key, value in overrides.items()})
    return config


def session(app_name: str = APP_NAME, **overrides):
    """
    Builds -- or returns -- the session every entry point shares.

    ``getOrCreate`` means the second caller in a process gets the first
    caller's session, overrides and all. One JVM per process is the only
    arrangement that works, and it is why the overrides that take effect only
    at JVM start -- driver memory above all -- belong to whoever gets there
    first.

    :param app_name: Name Spark's own logs report the run under.
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
    # buries a test failure under progress bars. Applied after creation because
    # it is a property of the context rather than of the builder.
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop() -> None:
    """
    Stops the active session, if there is one. Idempotent, so a teardown can
    call it without first asking whether a session was ever built -- which is
    the state a skipped test leaves behind.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()


# ---------------------------------------------------------------------------
# Reading the source
# ---------------------------------------------------------------------------
#
# ``inferSchema`` is never used, for the reason ``src/utils/io.py`` gives for
# ``dtype=object`` and ``keep_default_na=False``: the reader must not decide
# what ``""`` or ``"NA"`` or ``"5.727.580,00"`` mean. Inference would coerce on
# the way in, pre-empting the very coercion requirement 2 asks the pipeline to
# count -- the row that failed is gone before any stage could mark it.
#
# So every column arrives as a string exactly as the file spells it, and every
# type in the output is the result of a cast some stage made deliberately.

# The same extension-to-separator table the pandas reader uses. Repeated rather
# than imported because that module reaches for pandas at import time and this
# one must not: an executor has no reason to pay for pandas, and on a cluster
# may not have it.
DELIMITED_SUFFIXES = {".csv": ",", ".tsv": "\t", ".txt": ","}

# utf-8-sig, not utf-8. A CSV exported from Excel begins with a byte order
# mark, and read as plain UTF-8 the first column's name comes back with an
# invisible U+FEFF glued to the front -- so `USER_ID` is not `USER_ID`, every
# lookup on it misses, and the error names a column that is visibly present.
HEADER_ENCODING = "utf-8-sig"


def separator_for(path: str | Path) -> str:
    """
    :param path: Source file.
    :returns: The delimiter its extension implies.
    :raises ValueError: If the extension is not one this reader handles.
        Rejected by name rather than guessed at: reading an unknown format
        usually "works" and produces one column of garbage.
    """
    suffix = Path(path).suffix.lower()
    if suffix not in DELIMITED_SUFFIXES:
        raise ValueError(
            f"Spark reads delimited sources only, got {suffix!r}: {path}. "
            f"Supported: {sorted(DELIMITED_SUFFIXES)}. A workbook has to go "
            f"through src.utils.io.read_source, which is why the profile that "
            f"reads one is not a Spark profile."
        )
    return DELIMITED_SUFFIXES[suffix]


def header_of(path: str | Path, sep: str | None = None) -> list[str]:
    """
    Reads just the first line, with a real CSV parser rather than ``split``: a
    quoted header field containing the delimiter turns one column into two and
    shifts every column after it, silently.

    :param path: Source file.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: Column names in file order.
    :raises ValueError: If the file is empty, or names a column twice --
        duplicate names are accepted by both readers and then resolve to
        whichever copy the engine reaches first, which differs between them.
    """
    path = Path(path)
    sep = sep if sep is not None else separator_for(path)
    with path.open("r", encoding=HEADER_ENCODING, newline="") as handle:
        try:
            names = next(csv.reader(handle, delimiter=sep))
        except StopIteration:
            raise ValueError(f"{path} is empty: no header row to read") from None

    seen = {name for name in names if names.count(name) > 1}
    if seen:
        raise ValueError(
            f"{path} names {sorted(seen)} more than once. Spark and pandas "
            f"disambiguate duplicate columns differently, so a frame read "
            f"from this file cannot be compared against itself."
        )
    return names


def string_schema(path: str | Path, sep: str | None = None):
    """
    Derived from the file's own header rather than written out as a literal
    list of names. A hardcoded schema is a copy of the file that can drift from
    it, and under ``enforceSchema=true`` the drift is silent -- Spark applies
    the names positionally and hands every stage the column to the left of the
    one it asked for. Deriving the names and reading with
    ``enforceSchema=false`` gets the check for free.

    :param path: Source file.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: A ``StructType`` of nullable strings, one field per header
        column, in file order.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField(name, StringType(), nullable=True)
            for name in header_of(path, sep)
        ]
    )


def read_csv(spark, path: str | Path, sep: str | None = None):
    """
    Reads a delimited source as all strings, matching the pandas reader.

    Every option is stated, including the ones whose stated value is today's
    default, because a parity harness comparing two readers is only meaningful
    if both are pinned. The two that are NOT defaults:

    ``enforceSchema=false`` makes Spark check the header against the schema
    instead of applying it positionally. Since the schema was derived from that
    header this can only fail when the file changed underneath, which is
    exactly when a loud failure is worth having.

    ``mode=FAILFAST`` matches what pandas does with a ragged row: raise. Under
    the default PERMISSIVE mode a row with too few fields is null-padded and a
    row with too many is truncated, and the run completes reporting nothing.

    One known and deliberate difference from the pandas reader remains: a
    *quoted* empty field. pandas' ``na_values=[""]`` makes it null; Spark's
    ``emptyValue`` default keeps it an empty string, and the two categories are
    ones this pipeline works hard to keep apart, so neither reader is bent to
    agree with the other. The parity harness reports it if the file ever
    contains one.

    :param spark: An active session, from ``session()``.
    :param path: Source ``.csv``/``.tsv``/``.txt``.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: A DataFrame whose every column is a nullable string.
    """
    path = Path(path)
    sep = sep if sep is not None else separator_for(path)
    return (
        spark.read.format("csv")
        .schema(string_schema(path, sep))
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .option("sep", sep)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        # Not a backslash. pandas doubles a quote to escape it inside a quoted
        # field; Spark's default escape character is `\`, which would read `""`
        # as two fields' worth of confusion. Setting it to `"` is what makes
        # the two readers agree on an embedded quote.
        .option("escape", '"')
        .option("multiLine", "false")
        .option("nullValue", "")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .load(str(path))
    )
