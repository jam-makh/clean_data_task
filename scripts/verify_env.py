"""
Phase 00 environment check: does this machine actually run the Stage 2 stack?

Run before writing pipeline code, not after. Every failure here has a clear
cause and a known fix; the same failure surfacing in the middle of a Spark job
does not, because by then it is wearing a stack trace forty frames deep.

    python -m scripts.verify_env

Checks are ordered so the cheapest and most load-bearing run first, and each
one names what to do about it rather than only what went wrong.

Two of them go past "is the port open" to "does it answer me", because on
this machine a port scan is not evidence: a native PostgreSQL service holds
the conventional port whether or not the container is up, so a passing TCP
check can point at entirely the wrong server. Every connection setting is
read from .env, with the real environment taking precedence -- the same
precedence docker compose applies, so this script cannot disagree with the
containers about which values are in force.
"""

import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"

results: list[tuple[str, str, str]] = []

# Read rather than imported from a config module on purpose. This script has
# to be able to report "your environment is broken" while the environment is
# broken, so it depends on nothing the project itself has to load correctly.
ENV_FILE = Path(".env")

# Duplicated deliberately rather than imported. The jar path now also lives in
# src/spark/spark_setup.py, which puts it on the driver classpath, and the topic
# arrives in config/pipeline.yaml with the Phase 07 event contract -- but this
# script has to be able to report "your environment is broken" while the
# environment is broken, and a diagnostic that imports the project cannot do
# that. The duplication is the price of that independence, and it is one line
# each.
JDBC_JAR = Path("jars/postgresql-42.7.4.jar")
EXPECTED_TOPIC = "pipeline.run.completed.v1"


def load_env_file(path: Path = ENV_FILE) -> dict[str, str]:
    """
    Parses ``.env`` into a dict, ignoring comments and blank lines.

    Hand-rolled rather than python-dotenv because this script is the one that
    runs when nothing else works, and a diagnostic that fails on a missing
    dependency has failed at its only job.

    :param path: The env file to read.
    :returns: Its key/value pairs, empty when the file is absent.
    """
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def setting(env: dict[str, str], key: str, default: str = "") -> str:
    """
    :param env: Values parsed from ``.env``.
    :param key: The variable to resolve.
    :returns: The real environment first, then ``.env``, then the default --
        the same precedence docker compose applies, so this script and the
        containers cannot disagree about which value is in force.
    """
    return os.environ.get(key) or env.get(key) or default


def record(status: str, check: str, detail: str) -> None:
    """:param status: One of PASS, FAIL, WARN."""
    results.append((status, check, detail))


def check_python() -> None:
    version = sys.version_info
    detail = f"{version.major}.{version.minor}.{version.micro} at {sys.prefix}"
    in_venv = sys.prefix != sys.base_prefix
    if not in_venv:
        record(
            WARN,
            "Python",
            f"{detail} -- not running inside a virtualenv. Activate .venv "
            f"first, or packages land somewhere the project cannot see.",
        )
    else:
        record(PASS, "Python", detail)


def check_java() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        record(
            FAIL,
            "JAVA_HOME",
            "unset. Spark reads this directly -- a working `java` on PATH is "
            "not enough. Set it to your JDK root and open a new terminal.",
        )
        return
    if not Path(java_home, "bin", "java.exe").exists():
        record(
            FAIL,
            "JAVA_HOME",
            f"{java_home} has no bin\\java.exe -- this usually means it "
            f"points at a JRE, or one directory too high.",
        )
        return
    record(PASS, "JAVA_HOME", java_home)


def check_hadoop_home() -> None:
    """
    Spark on Windows reaches the local filesystem through Hadoop, which shells
    out to winutils.exe. Apache does not ship it, so it is always a separate
    thing someone put on the machine by hand -- and therefore always the first
    thing to check when local writes fail.
    """
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        record(
            FAIL,
            "HADOOP_HOME",
            "unset. Without it, Spark's first local write fails with a "
            "NullPointerException that names nothing useful.",
        )
        return

    winutils = Path(hadoop_home, "bin", "winutils.exe")
    if not winutils.exists():
        record(FAIL, "HADOOP_HOME", f"no bin\\winutils.exe under {hadoop_home}")
        return

    record(PASS, "HADOOP_HOME", f"{hadoop_home} (winutils.exe present)")

    # hadoop.dll is the native-IO half. Plenty of operations work without it;
    # the ones that do not fail as UnsatisfiedLinkError on NativeIO$Windows,
    # which reads like a Java problem and is really a missing file.
    #
    # Checked for CONTENT, not just existence. A truncated or zero-byte
    # download leaves a file that every `if exists()` in the world reports as
    # present, and that System.loadLibrary then loads to no effect -- so the
    # failure surfaces later, deeper, and describing a Java method signature
    # rather than the empty file that caused it.
    dll = Path(hadoop_home, "bin", "hadoop.dll")
    if not dll.exists():
        record(
            WARN,
            "hadoop.dll",
            "absent from the same bin directory. Not always needed -- the "
            "Spark write check below is what decides.",
        )
    elif dll.stat().st_size == 0:
        record(
            FAIL,
            "hadoop.dll",
            f"{dll} is 0 bytes -- a failed download that left a placeholder. "
            f"Spark will fail with UnsatisfiedLinkError on NativeIO$Windows, "
            f"which names the symbol and not this file. Replace it with a "
            f"build matching the Hadoop version Spark bundles.",
        )
    else:
        record(PASS, "hadoop.dll", f"{dll.stat().st_size / 1024:.0f} KB")

    # HADOOP_HOME tells Spark where winutils.exe is. It does NOT put the
    # native library anywhere the JVM will look: hadoop.dll is loaded through
    # java.library.path, which on Windows is seeded from PATH. Set one without
    # the other and every file operation fails with UnsatisfiedLinkError
    # naming a method -- which reads as a version mismatch and is really a
    # library the JVM never found. The DLL can be present, correct, and
    # exporting exactly the symbol in the error message.
    #
    # So this is repaired rather than reported. Requiring a correctly ordered
    # PATH on every machine that runs the pipeline is a setup instruction that
    # will be missed; deriving it from HADOOP_HOME, which is already
    # mandatory, cannot be. The same two lines belong in the Spark session
    # factory for the same reason.
    native = str(Path(hadoop_home, "bin"))
    if native.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + native
        record(PASS, "native library path", f"{native} added for this process")
    else:
        record(PASS, "native library path", f"{native} already on PATH")


def check_pyspark() -> None:
    try:
        import pyspark
    except ImportError:
        record(FAIL, "pyspark", "not importable -- pip install pyspark")
        return
    record(PASS, "pyspark", pyspark.__version__)

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        record(
            WARN,
            "SPARK_HOME",
            f"set to {spark_home}. PySpark will use those jars instead of its "
            f"own bundled ones. Harmless when the versions match exactly, and "
            f"a source of impossible-looking errors when they do not.",
        )


def check_port(name: str, host: str, port: int) -> bool:
    """
    :returns: True when something is listening.

    A TCP connect is deliberately all this does. It answers "is the container
    up and mapped", which is a different question from "will it serve me", and
    conflating the two makes a slow start look like a broken config.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2)
        if sock.connect_ex((host, port)) == 0:
            record(PASS, name, f"listening on {host}:{port}")
            return True
    record(
        FAIL,
        name,
        f"nothing on {host}:{port} -- is `docker compose up -d` running, and "
        f"does `docker compose ps` show it healthy?",
    )
    return False


def check_env_file(env: dict[str, str]) -> None:
    """
    :param env: Parsed ``.env`` contents.

    Absence is a WARN rather than a FAIL: docker-compose.yml defaults every
    variable it reads, so the stack does come up without one. What it will not
    do is come up with *your* settings, and the failure that produces arrives
    later and looks like something else entirely.
    """
    if not ENV_FILE.exists():
        record(
            WARN,
            ".env",
            "absent -- compose falls back to its built-in defaults. Copy "
            ".env.example and fill it in.",
        )
        return
    record(PASS, ".env", f"{len(env)} setting(s) read")


def check_jdbc_jar() -> None:
    """
    Spark reaches Postgres from inside the JVM, so it needs a Java driver.

    psycopg2 being importable says nothing about this: they are two
    implementations of the same wire protocol for two different runtimes, and
    having the Python one is exactly the state in which the missing Java one
    is most surprising.
    """
    if not JDBC_JAR.exists():
        record(
            FAIL,
            "JDBC jar",
            f"{JDBC_JAR} not found. Spark's JVM cannot use psycopg2 -- "
            f"download the driver into jars/.",
        )
        return
    size_mb = JDBC_JAR.stat().st_size / 1_048_576
    record(PASS, "JDBC jar", f"{JDBC_JAR.name} ({size_mb:.1f} MB)")


def check_postgres_query(env: dict[str, str]) -> None:
    """
    Connects and runs a query, which a port scan cannot substitute for.

    The distinction is not pedantic here. This machine also runs a native
    PostgreSQL service, so *something* answers on the conventional port
    whether or not the container is up -- and a check that only proves
    "something is listening" would pass while pointing at the wrong server
    entirely. Reporting the server version is what makes the two
    distinguishable at a glance: the container is 16.x, the native install is
    not.
    """
    host = setting(env, "POSTGRES_HOST", "localhost")
    port = setting(env, "POSTGRES_PORT", "5433")
    user = setting(env, "POSTGRES_USER", "pipeline")
    database = setting(env, "POSTGRES_DB", "transactions")
    password = setting(env, "POSTGRES_PASSWORD")
    # Named without the password, and deliberately so: this line is printed,
    # and a diagnostic that leaks a credential into a terminal someone pastes
    # into a ticket has created a worse problem than the one it reported.
    target = f"{user}@{host}:{port}/{database}"

    try:
        import psycopg2
    except ImportError:
        record(FAIL, "Postgres query", "psycopg2 not importable; skipped")
        return

    try:
        with psycopg2.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            dbname=database,
            connect_timeout=3,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select version()")
                version = cursor.fetchone()[0]
    except Exception as exc:  # noqa: BLE001 - the message is the whole point
        text = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
        hint = ""
        lowered = text.lower()
        if "does not exist" in lowered:
            hint = (
                " -> the role or database was never created. These are set "
                "only by the FIRST start against an empty pgdata volume; "
                "editing .env afterwards does not reach the server."
            )
        elif "password authentication failed" in lowered:
            hint = (
                " -> right server, wrong credential. If a native Postgres "
                "also runs here, check POSTGRES_PORT points at the container."
            )
        elif "could not connect" in lowered or "refused" in lowered:
            hint = " -> is `docker compose up -d` running and healthy?"
        record(FAIL, "Postgres query", f"{target}: {text}{hint}")
        return

    record(PASS, "Postgres query", f"{target} -> {version.split(' on ')[0]}")


def check_kafka_topic(env: dict[str, str]) -> None:
    """
    Asks the broker for its metadata and looks for the topic by name.

    Auto-creation is disabled in docker-compose.yml, which means a producer
    aimed at a topic that does not exist fails rather than quietly inventing
    one. That is the behaviour we want, and it is also why the topic's
    existence has to be checked rather than assumed.
    """
    servers = setting(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        record(FAIL, "Kafka topic", "confluent-kafka not importable; skipped")
        return

    try:
        metadata = AdminClient(
            {"bootstrap.servers": servers}
        ).list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001 - the message is the whole point
        record(FAIL, "Kafka topic", f"{servers}: {type(exc).__name__}: {exc}")
        return

    if EXPECTED_TOPIC not in metadata.topics:
        known = sorted(t for t in metadata.topics if not t.startswith("__"))
        record(
            FAIL,
            "Kafka topic",
            f"broker reachable at {servers} but '{EXPECTED_TOPIC}' does not "
            f"exist. Topics present: {known or '(none)'}. Create it with "
            f"kafka-topics.sh --create.",
        )
        return

    partitions = len(metadata.topics[EXPECTED_TOPIC].partitions)
    record(
        PASS,
        "Kafka topic",
        f"{EXPECTED_TOPIC} ({partitions} partition(s)) via {servers}",
    )


def check_spark_local_write() -> None:
    """
    The check that actually matters on Windows.

    Starting a session proves Java works. Writing Parquet proves the Hadoop
    filesystem layer works, which is the part winutils exists for -- and the
    part Phase 02 depends on.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        record(FAIL, "Spark write", "pyspark not installed; skipped")
        return

    # Spark's executors launch Python workers by running `python3`, which does
    # not exist on Windows -- the executable is python.exe. The JVM starts,
    # the job plans, and then every task dies with CreateProcess error=2
    # naming a program nobody configured. Pointing both ends at sys.executable
    # fixes that and settles a second question at the same time: the workers
    # run THIS virtualenv's interpreter, rather than whichever Python happens
    # to be first on PATH and may have none of the project's dependencies.
    #
    # setdefault, not assignment, so a deliberate override survives.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    target = Path(tempfile.mkdtemp(prefix="sparkcheck-"))
    spark = None
    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("verify-env")
            # 200 is the default and absurd for a handful of rows; it makes
            # every local run slow enough to discourage iteration.
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        frame = spark.createDataFrame(
            [("a", 1), ("b", 2), ("c", 3)], ["label", "value"]
        )
        out = target / "probe.parquet"
        frame.write.mode("overwrite").parquet(str(out))
        read_back = spark.read.parquet(str(out)).count()

        if read_back == 3:
            record(PASS, "Spark write", f"wrote and re-read 3 rows via {out.name}")
        else:
            record(FAIL, "Spark write", f"expected 3 rows back, got {read_back}")
    except Exception as exc:  # noqa: BLE001 - the message is the whole point
        text = str(exc)
        hint = ""
        if "UnsatisfiedLinkError" in text or "NativeIO" in text:
            hint = " -> hadoop.dll is missing beside winutils.exe"
        elif "HADOOP_HOME" in text or "winutils" in text:
            hint = " -> HADOOP_HOME is unset or points somewhere wrong"
        elif "UnsupportedClassVersion" in text:
            hint = " -> the JDK is older than this Spark build expects"
        record(FAIL, "Spark write", f"{type(exc).__name__}: {text[:220]}{hint}")
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(target, ignore_errors=True)


def main() -> int:
    """:returns: Process exit code -- non-zero when any check failed."""
    env = load_env_file()

    check_python()
    check_java()
    check_hadoop_home()
    check_pyspark()
    check_jdbc_jar()
    check_env_file(env)

    # Both layers, cheapest first, and neither is redundant. The port check
    # separates "the container is not running" from "the container is running
    # and rejecting me", which are the same red line to a user and completely
    # different problems to fix.
    postgres_port = int(setting(env, "POSTGRES_PORT", "5433"))
    if check_port("Postgres", setting(env, "POSTGRES_HOST", "localhost"),
                  postgres_port):
        check_postgres_query(env)

    kafka_servers = setting(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_host, _, kafka_port = kafka_servers.partition(":")
    if check_port("Kafka", kafka_host, int(kafka_port or 9092)):
        check_kafka_topic(env)

    check_spark_local_write()

    width = max(len(check) for _, check, _ in results)
    print()
    for status, check, detail in results:
        print(f"  [{status:<4}] {check:<{width}}  {detail}")

    failures = sum(1 for status, _, _ in results if status == FAIL)
    warnings = sum(1 for status, _, _ in results if status == WARN)
    print()
    print(
        f"  {len(results) - failures - warnings} passed, "
        f"{warnings} warning(s), {failures} failure(s)"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
