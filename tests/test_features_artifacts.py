"""
What the build hands over: the Postgres table, and the run report beside it.

There is no file artifact for the feature table. Stage 4 reads the table
directly, so a Parquet copy would be a second thing to keep in step with it.

The database tests are marked ``db`` and skip when the container is down.
"""

import dataclasses
import json

import pytest

from features import builder, contract, diagnostics, scale
from src.db import settings as db_settings
from features import writer
from features import settings as feature_settings
from src.rules import store
from tests.harness import features


@pytest.fixture(scope="module")
def rules():
    """:returns: The vocabularies, read from the rule files."""
    return store.from_json()


@pytest.fixture
def config(tmp_path):
    """
    :returns: Build settings writing into a temporary directory, so a test run
        never touches the real manifest.
    """
    base = feature_settings.load()
    return dataclasses.replace(
        base,
        output=feature_settings.OutputSettings(
            manifest=tmp_path / "features.manifest.json"
        ),
    )


@pytest.fixture(scope="module")
def source_frame(spark):
    """:returns: The synthetic cleaned transactions."""
    return features.simple(spark)


@pytest.fixture(scope="module")
def database():
    """
    :returns: The connection settings. Skips rather than fails when nothing
        answers on the port.
    """
    settings = db_settings.load()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        psycopg2.connect(settings.dsn).close()
    except Exception as exc:  # noqa: BLE001 - the message is the point
        pytest.skip(
            f"Postgres not reachable ({type(exc).__name__}). "
            f"Run `make verify` -- it names the cause."
        )
    return settings


def test_the_report_records_the_decisions_the_table_rests_on(
    spark, source_frame, rules, config
):
    """
    The units decision, the balance cutoff and the point-in-time claim travel
    with the run rather than living only in a document.
    """
    builder.run(spark, source_frame, rules, config, database=None)
    report = json.loads(config.output.manifest.read_text(encoding="utf-8"))

    assert report["units"]["money"] == "USD"
    assert report["units"]["never_read"]

    eligible = report["balance_quality"]["eligible_statuses"]
    assert eligible == list(config.balance.eligible_statuses)
    assert "CONTRADICTED" not in eligible

    # Every column says when it is knowable, and exactly one reads month M.
    known = report["point_in_time"]["known_at"]
    assert set(known) == set(contract.names(rules.categories))
    targets = [
        name for name, when in known.items() if when == contract.TARGET
    ]
    assert targets == [builder.TARGET]


def test_every_metric_in_the_report_explains_itself(
    spark, source_frame, rules, config
):
    """
    A number with no statement of what it counts is a number somebody will
    misread. The format is what guarantees the explanation exists, rather than
    a convention that holds until someone adds a metric in a hurry.
    """
    builder.run(spark, source_frame, rules, config, database=None)
    report = json.loads(config.output.manifest.read_text(encoding="utf-8"))

    sections = (
        "balance_quality",
        "direction_quality",
        "spending_quality",
        "activity_quality",
    )

    checked = 0
    for section in sections:
        for name, entry in report[section].items():
            if not isinstance(entry, dict) or "value" not in entry:
                # Echoed configuration -- the eligible status list, the
                # category vocabulary -- rather than a measurement.
                continue

            assert entry["what"], f"{section}.{name} says nothing about what"
            assert entry["means"], f"{section}.{name} says nothing about why"
            assert entry["what"].endswith("."), f"{section}.{name}"
            checked += 1

    assert checked > 20


def test_the_diagnostics_left_the_table_and_landed_in_the_report(
    spark, source_frame, rules, config
):
    """
    The move this design is about. None of the five removed columns is in the
    feature table; every one of them is still counted, in the report.
    """
    table, report = builder.run(
        spark, source_frame, rules, config, database=None
    )

    removed = (
        "prev_1m_accounts_contributing",
        "prev_1m_accounts_with_balance",
        "prev_1m_balance_is_carried_forward",
        "prev_1m_undeclared_txn_count",
        "prev_1m_months_since_last_txn",
    )
    for column in removed:
        assert column not in table.columns

    assert "user_months_partial_rollup" in report["balance_quality"]
    assert "account_months_carried_forward" in report["balance_quality"]
    assert "accounts_never_with_balance" in report["balance_quality"]
    assert "txns_undeclared_direction" in report["direction_quality"]
    assert "max_months_since_last_txn" in report["activity_quality"]


def test_the_report_counts_what_the_balance_rule_excluded(
    spark, rules, config
):
    """
    CONTRADICTED and UNAVAILABLE are the two exclusions, and the report says
    how many rows each one cost rather than leaving the cutoff unpriced.
    """
    rows = [
        features.transaction("u1", "u1a", "2022-01", "salary", 100, 100, 1),
        features.transaction(
            "u1", "u1a", "2022-02", "purchase", 50, 999.0, 2,
            status="CONTRADICTED",
        ),
        features.transaction(
            "u1", "u1a", "2022-03", "purchase", 10, 0.0, 3,
            status="UNAVAILABLE",
        ),
    ]
    frame = features.frame(spark, rows)
    counts = diagnostics.transactions(frame, rules, config.balance)

    assert counts["rows_excluded_contradicted"] == 1
    assert counts["rows_excluded_unavailable"] == 1
    assert counts["rows_excluded_by_status_total"] == 2
    assert counts["rows_by_balance_status"]["CONTRADICTED"] == 1


def test_the_report_names_the_slowest_phase(
    spark, source_frame, rules, config
):
    """
    Deliverable 4 asks which step dominated. Measured at materialisation
    barriers, so the answer is a number rather than an impression -- timing a
    lazy plan would measure how long it took to describe the work.
    """
    _, report = builder.run(
        spark, source_frame, rules, config, database=None
    )
    performance = report["performance"]

    assert performance["slowest_phase"] in performance["phase_seconds"]
    assert performance["total_seconds"] > 0
    # The JVM is where the work happens, so that is the figure reported.
    assert performance["jvm_peak_memory_mb"] > 0

    # CPU is bracketed over the same phases as wall clock, so the two maps
    # describe the same run and can be divided by each other.
    assert set(performance["phase_cpu_seconds"]) == set(
        performance["phase_seconds"]
    )
    assert performance["total_cpu_seconds"] > 0
    assert set(performance["phase_parallelism"]) <= set(
        performance["phase_seconds"]
    )

    # The claim the memory figure makes about itself. It is sampled at phase
    # boundaries rather than tracked, and the report has to say so -- a lower
    # bound presented as a peak is the kind of number that gets quoted.
    assert "sampled" in performance["memory_caveat"]


def test_replicating_the_source_multiplies_users_not_months(source_frame):
    """
    Re-keying is what makes the scaling run meaningful. Duplicating rows under
    the same ids would lengthen each user's history instead, which changes the
    shape of the problem rather than its size.
    """
    original = scale.summarise(source_frame)
    replicated = scale.replicate(source_frame, 5)
    summary = scale.summarise(replicated)

    assert summary["rows"] == original["rows"] * 5
    assert summary["users"] == original["users"] * 5
    assert summary["accounts"] == original["accounts"] * 5

    # And months per user are untouched: every replica of a user spans the
    # same calendar the original did. That is the difference between five
    # times the users and five times the history.
    from pyspark.sql import functions as F

    def months_per_user(frame) -> list[int]:
        """:returns: Distinct months held by each user, sorted."""
        counted = frame.groupBy("user_id").agg(
            F.countDistinct("month").alias("months")
        )
        return sorted(row["months"] for row in counted.collect())

    assert months_per_user(replicated) == months_per_user(source_frame) * 5


def test_a_scaled_build_produces_proportionally_many_rows(
    source_frame, rules, config
):
    """
    The grain holds at scale: five times the users over the same months is
    five times the feature rows.
    """
    small = builder.assemble(source_frame, rules, config)
    large = builder.assemble(
        scale.replicate(source_frame, 5), rules, config
    )

    assert large.table.count() == small.table.count() * 5


def test_the_scaled_build_computes_the_same_answer_for_the_real_users(
    source_frame, rules, config
):
    """
    The one that makes every scaling number mean anything.

    Copy zero keeps the original ids, so a 5x build contains the 1x build.
    Restricted back to the original users it therefore has to agree with a 1x
    build column for column -- if it does not, the benchmark is timing a
    different computation and the timings describe nothing.

    Two ways it could fail, and both are real. Derived ids could collide with
    original ones, putting two users' rows under one key. Or a global
    operation could have crept in somewhere -- a window without a partition,
    an aggregate over the whole frame -- in which case adding users would
    change the answer for the users already there.
    """
    from pyspark.sql import functions as F

    small = builder.assemble(source_frame, rules, config).table
    large = builder.assemble(
        scale.replicate(source_frame, 5), rules, config
    ).table

    original = small.select("user_id").distinct()
    restricted = large.join(original, on="user_id", how="inner")

    # Same rows for the same users, and no user gained or lost a month.
    assert restricted.count() == small.count()

    # And the same values. subtract() is a set difference over whole rows, so
    # a single cent moving in a single column on a single month fails this.
    columns = small.columns
    assert restricted.select(columns).subtract(small.select(columns)).count() == 0
    assert small.select(columns).subtract(restricted.select(columns)).count() == 0

    # The collision case, stated separately so it fails with its own message.
    assert large.select("user_id").distinct().count() == (
        original.count() * 5
    )
    assert (
        large.groupBy("user_id", "month").agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
        .count()
    ) == 0


@pytest.mark.db
def test_the_upsert_is_idempotent(
    spark, source_frame, rules, config, database
):
    """
    The table is written by an upsert on the grain, so a re-run replaces rows
    rather than adding them. Written into its own table, dropped afterwards.
    """
    import psycopg2

    name = "test_feature_store_monthly"
    scoped = dataclasses.replace(
        config, database=dataclasses.replace(config.database, table=name)
    )
    build = builder.assemble(source_frame, rules, config)
    expected = build.table.count()

    try:
        first = writer.write(build.table, database, rules, scoped)
        second = writer.write(build.table, database, rules, scoped)
        assert first == second == expected

        with psycopg2.connect(database.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT count(*) FROM {name}")
                assert cursor.fetchone()[0] == expected
    finally:
        with psycopg2.connect(database.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
                cursor.execute(f"DROP TABLE IF EXISTS staging_{name}")


@pytest.mark.db
def test_the_generated_ddl_is_what_postgres_accepts(rules, database):
    """
    The DDL is generated from the same declaration the frame is projected to.
    This is the check that it is also valid SQL, in the right column order.
    """
    import psycopg2

    name = "test_feature_ddl"
    statement = contract.create_table(rules.categories, name)

    try:
        with psycopg2.connect(database.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement)
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (name,),
                )
                columns = [row[0] for row in cursor.fetchall()]

        assert columns == contract.names(rules.categories)
    finally:
        with psycopg2.connect(database.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
