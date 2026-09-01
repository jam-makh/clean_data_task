"""
The Stage 2 chain end to end: read, clean, count, upsert.

This is the file that would have caught every integration bug the unit tests
could not, because until it existed nothing outside the test suite called
``writer.write`` at all -- the sink was proven and unreached.

Run against the parity sample rather than the full extract. The sample is
11,417 rows chosen to be adversarial (see ``tests/harness/sample.py``), which
exercises the same code paths in six minutes instead of forty. What it does
not prove is scale, and nothing here claims to.

The two runs below are deliberate and are most of the runtime. One proves the
chain works; the second proves running it again changes nothing, which is the
property the whole staging-and-merge design exists for and cannot be checked
with a single run.

Marked ``kafka`` as well as ``db`` since the runner emits: the chain does not
end at Postgres, and a test that skipped the announcement would not be testing
the chain.
"""

import pytest

from src.config_readers import runtime
from src.config_readers.errors import ConfigError
from src.db import contract
from src.db import settings as db_settings

pytestmark = [pytest.mark.db, pytest.mark.spark, pytest.mark.kafka]


@pytest.fixture(scope="module")
def database():
    """:returns: Connection settings, skipping when Postgres is not up."""
    settings = db_settings.load()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        psycopg2.connect(settings.dsn).close()
    except Exception as exc:  # noqa: BLE001 - the message is the point
        pytest.skip(
            f"Postgres not reachable at {settings} ({type(exc).__name__}). "
            f"Run `make verify` -- it names the cause."
        )
    return settings


def rows_for(database, job: str) -> int:
    """:returns: How many rows in the live table carry this job id."""
    import psycopg2

    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM {contract.TABLE} "
                f"WHERE {contract.SYNC_JOB} = %s",
                (job,),
            )
            return cursor.fetchone()[0]


@pytest.fixture(scope="module")
def first_run(spark, database, sample_path):
    """
    Runs the chain once. Module-scoped because it is six minutes, and every
    assertion below reads the same result rather than paying for it again.

    Rows are removed afterwards by their job id, so this can run against the
    same database the pipeline uses.
    """
    import psycopg2

    from src.runner import run

    result = run(sample_path, connection=database, spark=spark)
    yield result
    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {contract.TABLE} WHERE {contract.SYNC_JOB} = %s",
                (result.sync_job_id,),
            )


def test_the_chain_reads_cleans_and_writes(first_run, database):
    """
    The whole point of the module: a source file goes in, rows come out in
    Postgres, and the run says what it did.
    """
    assert first_run.rows_read > 0
    assert first_run.rows_written == rows_for(database, first_run.sync_job_id)
    assert first_run.rows_written > 0


def test_the_job_id_is_the_source_s_own(first_run, sample_path):
    """
    Not generated for the run. This is what makes the id joinable to anything
    else derived from the same file, and what makes a replay identifiable.
    """
    from src import jobs

    assert first_run.sync_job_id == jobs.job_id_for(sample_path)


def test_the_run_reports_what_each_stage_did(first_run):
    """
    The report is the completion event's payload. An empty one would make the
    event a bare "done", which is the version of this feature that looks
    finished and tells nobody anything.
    """
    assert first_run.report.entries
    assert first_run.metrics.get("output_rows", 0) > 0
    assert first_run.metrics.get("input_rows", 0) >= first_run.metrics["output_rows"]


def test_the_profile_was_detected_not_assumed(first_run):
    assert first_run.profile == "forecast_balance"


def test_the_run_announced_itself(first_run):
    """
    The chain ends on Kafka, not in Postgres. A run that wrote 11,417 rows and
    told nobody is half-finished, and this is the assertion that would notice.
    """
    assert first_run.event is not None
    assert first_run.event["sync_job_id"] == first_run.sync_job_id
    assert first_run.event["rows"]["written"] == first_run.rows_written


def test_the_event_carries_the_same_fingerprint_as_the_run(first_run):
    """
    "Same input, same rules, same answer" has to be checkable by whoever reads
    the event, not only by whoever ran it.
    """
    assert first_run.event["config_fingerprint"] == first_run.fingerprint
    assert first_run.event["metrics"]["output_rows"] == (
        first_run.metrics["output_rows"]
    )


def test_the_fingerprint_travels_with_the_run(first_run):
    """
    "Same input, same rules, same answer" has to be checkable from the run's
    own record, or the event says a run happened without saying under what.
    """
    from src.config_readers.fingerprint import short_fingerprint

    assert first_run.fingerprint == short_fingerprint()


def test_rows_dropped_accounts_for_the_difference(first_run):
    dropped = first_run.rows_read - first_run.metrics["output_rows"]

    assert first_run.rows_dropped == max(dropped, 0)


def test_running_the_same_source_again_changes_nothing(
    first_run, spark, database, sample_path
):
    """
    Requirement 6 at the runner boundary, and the reason the id is derived
    rather than generated: the second run mints the same job id, upserts the
    same keys with the same values, and leaves a table no reader can tell
    apart from the one the first run left.
    """
    from src.runner import run

    count_before = rows_for(database, first_run.sync_job_id)
    before = checksum_without_write_time(database, first_run.sync_job_id)

    again = run(sample_path, connection=database, spark=spark)

    assert again.sync_job_id == first_run.sync_job_id
    assert again.rows_written == first_run.rows_written
    assert rows_for(database, again.sync_job_id) == count_before
    # cleaned_at moves on an upsert by design, so it is excluded from the
    # comparison rather than allowed to fail it -- the column means "when the
    # pipeline last wrote this row" and the second run did exactly that.
    assert checksum_without_write_time(database, again.sync_job_id) == before


def checksum_without_write_time(database, job: str):
    """
    :returns: A digest over every column except ``cleaned_at``, which is
        expected to move between runs and would otherwise mask the columns
        that must not.
    """
    import psycopg2

    columns = ", ".join(contract.TARGET)
    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT md5(string_agg(row_value, '|' ORDER BY row_value)) "
                f"  FROM (SELECT ROW({columns})::text AS row_value "
                f"          FROM {contract.TABLE} "
                f"         WHERE {contract.SYNC_JOB} = %s) rows",
                (job,),
            )
            return cursor.fetchone()[0]


def test_a_dry_run_reads_and_reports_without_writing(
    spark, database, sample_path, tmp_path
):
    """
    ``--dry-run`` has to be a real run minus the write, not a no-op: the state
    you want when the question is whether the cleaning is right and the
    database is beside the point.

    Run against a cut-down copy rather than the sample itself, and that is the
    point rather than an optimisation. The job id is derived from the source's
    contents, so a dry run over the same file would mint the *same* id as the
    real run above -- and the rows that run wrote would be counted against
    this one, failing a test whose subject had behaved perfectly. A distinct
    source is what makes "wrote nothing" a statement about this run.
    """
    from src.runner import run

    lines = sample_path.read_text(encoding="utf-8").splitlines(keepends=True)
    source = tmp_path / "subset.csv"
    source.write_text("".join(lines[:2001]), encoding="utf-8")

    result = run(
        source, connection=database, spark=spark, write=False, emit=False
    )

    assert result.rows_written is None
    assert result.event is None
    assert result.rows_read > 0
    assert result.report.entries
    assert rows_for(database, result.sync_job_id) == 0


def test_an_unported_profile_says_so_and_names_the_way_out():
    """
    ``transactions_v4`` begins with ``dates``, which has no Spark counterpart
    and is not being ported. Asking Spark to run it is a configuration
    mistake, and the error has to name the escape hatch rather than report an
    empty step list as success.
    """
    from src.runner import run

    with pytest.raises(ConfigError) as raised:
        run(
            runtime.load().paths.source,
            profile="transactions_v4",
            write=False,
            emit=False,
        )

    assert "pandas" in str(raised.value)
