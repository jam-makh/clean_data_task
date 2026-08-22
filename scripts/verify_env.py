"""
Phase 00 environment check: does this machine actually run the Stage 2 stack?

Run before writing pipeline code, not after. Every failure here has a clear
cause and a known fix; the same failure surfacing in the middle of a Spark job
does not, because by then it is wearing a stack trace forty frames deep.

    python -m scripts.verify_env

Checks are ordered so the cheapest and most load-bearing run first, and each
one names what to do about it rather than only what went wrong.
"""

import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"

results: list[tuple[str, str, str]] = []


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
    if not Path(hadoop_home, "bin", "hadoop.dll").exists():
        record(
            WARN,
            "hadoop.dll",
            "absent from the same bin directory. Not always needed -- the "
            "Spark write check below is what decides.",
        )


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
    check_python()
    check_java()
    check_hadoop_home()
    check_pyspark()
    check_port("Postgres", "localhost", 5432)
    check_port("Kafka", "localhost", 9092)
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
