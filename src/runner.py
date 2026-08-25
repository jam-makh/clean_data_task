"""
The Stage 2 chain, wired end to end: read, clean, count, write.

    source .csv
      -> spark_setup.read_csv        every column string, nothing coerced yet
      -> spark.pipeline.run          the profile's steps, in order
      -> spark.pipeline.report       one pass over the marks the stages left
      -> db.writer.write             project, stage, upsert
      -> kafka.producer.emit         announce what the load did
      -> RunResult                   the same thing, for the caller

Everything above this line already existed and was reachable only from tests.
That is the gap this module closes: a pipeline nothing calls is a pipeline
whose integration is unproven, however green its unit tests are.

The **command line** is deliberately not here. It is ``main.py``'s, because
the repo has one entry point and one ``main`` -- this module is a function, so
a test can drive the whole chain without argparse and a consumer can later
drive it without a shell.

Order matters at the end: the rows are committed before the event is
published, never the other way round. An event announcing a load that is not
in the table yet is a race a consumer cannot defend against, whereas rows that
are written and not yet announced are merely un-announced -- and re-emitting
is safe, because the payload is derived from the run.

The cache is not incidental
---------------------------

``report()`` costs two actions over the cleaned frame and the write costs a
third. Uncached, that is the entire eleven-stage pipeline computed three times
-- Spark frames are lazy, so "the frame" is a recipe, not a result. Caching
once after ``run`` is what makes this a single pass over the data instead of
three, and on 265k rows the difference is minutes.
"""

import time
from dataclasses import dataclass, field, replace
from pathlib import Path

from src.config import runtime
from src.config.errors import ConfigError
from src.config.fingerprint import short_fingerprint
from src.db.settings import Database as Connection
from src.utils.report import CleaningReport


@dataclass(frozen=True)
class RunResult:
    """
    What one load did. The completion event's payload, before it is an event.

    :param sync_job_id: The load's identity, derived from the source's
        contents -- see ``src.jobs``.
    :param source: The file read.
    :param profile: The profile that was detected or forced.
    :param rows_read: Rows the reader produced.
    :param rows_written: Rows the upsert inserted or updated. None when the
        run did not write.
    :param report: Per-stage totals, derived from the diagnostic columns.
    :param fingerprint: The config fingerprint the run used, so "same input,
        same rules, same answer" is checkable from the event alone.
    :param seconds: Wall clock, for the event and for noticing a run that
        suddenly takes four times as long.
    :param event: The completion event as published, or None when the run did
        not emit. Held so a caller can log or assert on exactly what went out
        rather than on a reconstruction of it.
    """

    sync_job_id: str
    source: Path
    profile: str
    rows_read: int
    rows_written: int | None
    report: CleaningReport
    fingerprint: str
    seconds: float
    metrics: dict = field(default_factory=dict)
    event: dict | None = None

    @property
    def rows_dropped(self) -> int:
        """
        :returns: Rows the pipeline removed. Only ``duplicates`` drops rows --
            byte-identical copies, counted on the survivor rather than lost --
            so this is that stage's work seen from outside.
        """
        return max(self.rows_read - self.metrics.get("output_rows", 0), 0)

    def summary(self) -> str:
        """:returns: One line, for a log or a terminal."""
        written = "not written" if self.rows_written is None else (
            f"{self.rows_written} written"
        )
        announced = "announced" if self.event else "not announced"
        return (
            f"job {self.sync_job_id} | {self.profile} | "
            f"{self.rows_read} read, {written}, {announced} | "
            f"{self.seconds:.1f}s | config {self.fingerprint}"
        )


def _profile_for(config, frame, requested: str | None):
    """
    :param config: The loaded runtime configuration.
    :param frame: The frame as read, for its column names.
    :param requested: A profile name from the command line, or None to detect.
    :returns: The profile to run.
    :raises ConfigError: When nothing matches, naming what was looked for.
    """
    if requested:
        return config.profile(requested)
    return config.detect(frame.columns)


def run(
    source: str | Path | None = None,
    *,
    profile: str | None = None,
    write: bool | None = None,
    emit: bool | None = None,
    connection: Connection | None = None,
    broker=None,
    spark=None,
) -> RunResult:
    """
    Runs one load, start to finish.

    :param source: The extract to read; ``paths.source`` when absent.
    :param profile: Force a profile instead of detecting one from the columns.
    :param write: Override ``database.enabled`` for this run. False still
        reads, cleans and reports -- which is the state you want when the
        question is whether the cleaning is right rather than whether the
        write is.
    :param emit: Override ``kafka.enabled`` for this run.
    :param connection: Where to write; read from the environment when absent.
    :param broker: Where to announce; read from config and the environment
        when absent.
    :param spark: An existing session, for a caller that already has one. A
        session is created and left running when absent, because the caller
        that did not pass one may still want the frame afterwards.
    :returns: What the run did.
    :raises ConfigError: On an unusable configuration or an undetectable
        source.
    :raises FileNotFoundError: If the source is not there.
    """
    from src.db import writer
    from src.db.settings import load as load_connection
    from src.spark import pipeline as spark_pipeline
    from src.spark.spark_setup import read_csv, session

    started = time.monotonic()
    config = runtime.load()

    source = Path(source) if source is not None else config.paths.source
    if not source.exists():
        raise FileNotFoundError(f"source not found: {source.resolve()}")

    # Derived before anything expensive happens, so a run that cannot be
    # identified fails in a second rather than after eleven stages.
    from src.jobs import job_id_for

    sync_job_id = job_id_for(source)

    spark = spark if spark is not None else session()
    frame = read_csv(spark, source)

    chosen = _profile_for(config, frame, profile)
    names = spark_pipeline.ported(chosen.steps)
    if not names:
        raise ConfigError(
            f"profile {chosen.name!r} has no steps ported to Spark. Its first "
            f"step is {chosen.steps[0]!r}; run it with --engine pandas."
        )

    # Cached before the report and the write, both of which are actions over
    # it. See the module docstring: uncached, this frame is recomputed from
    # the CSV for each one.
    cleaned = spark_pipeline.run(frame, names).cache()

    # `source=frame` is what records `input_rows`. Without it the report is
    # silently missing the one number every other total is read against -- how
    # many rows went in -- and the pandas report has it, so a Spark run would
    # produce a report shaped differently from its own reference. It also
    # costs nothing extra: the count has to happen either way, and asking the
    # report for it means one action rather than a separate `frame.count()`.
    report = spark_pipeline.report(cleaned, names, source=frame)
    metrics = {metric: value for _, metric, value in report.entries}
    rows_read = metrics["input_rows"]

    should_write = config.database.enabled if write is None else write
    rows_written = None
    if should_write:
        connection = connection if connection is not None else load_connection()
        rows_written = writer.write(cleaned, connection, sync_job_id)

    result = RunResult(
        sync_job_id=sync_job_id,
        source=source,
        profile=chosen.name,
        rows_read=rows_read,
        rows_written=rows_written,
        report=report,
        fingerprint=short_fingerprint(),
        seconds=time.monotonic() - started,
        metrics=metrics,
    )

    # After the write, and only after. See the module docstring: an event that
    # announces rows the table does not have yet is a race the consumer cannot
    # defend against.
    should_emit = config.kafka.enabled if emit is None else emit
    if not should_emit:
        return result

    from src.kafka.producer import emit as announce

    return replace(result, event=announce(result, broker, engine="spark"))


def run_rows(
    ids,
    *,
    spark,
    profile: str | None = None,
    write: bool = True,
    emit: bool = False,
    connection: Connection | None = None,
    broker=None,
    listener=None,
    policy=None,
) -> RunResult:
    """
    The same chain, over rows from ``raw_transactions`` instead of a file.

        raw.read            the named ids, as the frame the CSV reader makes
        -> spark.pipeline.run      the profile's steps, in order
        -> spark.pipeline.report   one pass over the marks the stages left
        -> db.writer.write         project, stage, upsert
        -> RunResult

    Deliberately the same shape and the same return type as ``run``. The
    difference between the batch path and the streaming path is *where the
    rows come from* and nothing else -- same steps, same policy, same
    contract, same upsert -- and writing this as a second pipeline would have
    made that a claim rather than a fact.

    Two things differ, and both are consequences of the source rather than
    choices:

    **The session is the caller's.** ``run`` creates one if it must, because a
    batch run is a process. A consumer runs thousands of these and a session
    per message would pay ten seconds of JVM start for a job that takes one --
    so this requires a session and never makes one.

    **The job id covers the batch.** ``run`` derives it from the file's bytes;
    here it is derived from the ids, so redelivering the same batch produces
    the same id and the upsert is a no-op. Two overlapping batches -- ids
    (1, 2) then (2, 3) -- give row 2 the id of whichever wrote it last, which
    is what ``sync_job_id`` already means everywhere else in this schema: the
    load that last wrote this row.

    :param ids: Row ids from ``raw_transactions``.
    :param spark: An active session. Required; see above.
    :param profile: Force a profile instead of detecting one from the columns.
    :param write: Upsert the cleaned rows. False still reads, cleans and
        reports, which is the state you want when the question is whether the
        cleaning is right.
    :param emit: Publish a completion event for this batch. Off by default:
        the batch run announces a file, and a consumer announcing every
        message would put one event per row on a topic meant to carry one per
        load.
    :param connection: Where to write; read from the environment when absent.
    :param broker: Where to announce; read from config when absent.
    :param listener: A ``src.spark.stagelog.StageLog``, or None for silence.
    :param policy: The policy to clean under; loaded when absent.
    :returns: What the batch did.
    :raises ValueError: If no ids are given, or the ids name no rows -- a
        consumer told about a row that is not there must hear so rather than
        report a successful run over nothing.
    :raises ConfigError: If the profile cannot be detected, or has no steps
        ported to Spark.
    """
    from src.config.policy import load as load_policy
    from src.db import raw, writer
    from src.db.settings import load as load_connection
    from src.jobs import job_id_from_digest
    from src.spark import pipeline as spark_pipeline

    started = time.monotonic()
    config = runtime.load()
    policy = policy if policy is not None else load_policy()

    wanted = raw._as_ids(ids)
    if not wanted:
        raise ValueError("no ids to clean")

    connection = connection if connection is not None else load_connection()

    # One small query before any Spark work. Without it, a message naming a
    # row nobody has would be discovered only after eleven cleaning stages had
    # run over an empty frame -- a minute of work to report a fact one SELECT
    # answers in milliseconds.
    present = raw.existing(connection, wanted)
    if not present:
        raise ValueError(f"no rows in {raw.TABLE} with id in {wanted}")
    missing = [i for i in wanted if i not in present]
    if missing:
        # Cleaned anyway, rather than refused. Some of these rows exist and
        # can be cleaned now; the ids that do not are the consumer's to report
        # and to mark, and holding the good rows hostage to the bad ones would
        # be the batch failure mode this pipeline works to avoid.
        wanted = present

    # Derived from the ids rather than generated, for the reason src/jobs.py
    # gives at length: a redelivered batch must be recognisable as the same
    # load and not as a second one. Derived from the ids that are *there*
    # rather than the ids that were asked for, so that a batch redelivered
    # after one of its rows was deleted still identifies as the same load for
    # the rows it can still clean.
    sync_job_id = job_id_from_digest(
        f"{raw.TABLE}:" + ",".join(str(i) for i in wanted)
    )

    frame = raw.read(spark, connection, wanted)

    chosen = _profile_for(config, frame, profile)
    names = spark_pipeline.ported(chosen.steps)
    if not names:
        raise ConfigError(
            f"profile {chosen.name!r} has no steps ported to Spark. Its first "
            f"step is {chosen.steps[0]!r}; the streaming path is Spark only."
        )

    cleaned = spark_pipeline.run(frame, names, policy, listener=listener).cache()
    report = spark_pipeline.report(cleaned, names, policy, source=frame)
    metrics = {metric: value for _, metric, value in report.entries}
    rows_read = metrics["input_rows"]

    if not rows_read:
        # Distinguished from "cleaned nothing", which is what an empty frame
        # would otherwise be reported as. The ids came from a message, and a
        # message naming rows that are not in the table is a real condition
        # with a real cause -- a deleted row, a producer pointed at another
        # database -- and the consumer can only say so if it is told.
        cleaned.unpersist()
        raise ValueError(
            f"no rows in {raw.TABLE} with id in {wanted}"
        )

    rows_written = None
    if write:
        rows_written = writer.write(cleaned, connection, sync_job_id)

    result = RunResult(
        sync_job_id=sync_job_id,
        # The table, standing where the file's path stands in a batch run.
        # str() of it is what reaches the completion event, and
        # "raw_transactions" is the honest answer to "where did these come
        # from" -- the ids are in the job id, not here, because a source is a
        # place and not a selection.
        source=Path(raw.TABLE),
        profile=chosen.name,
        rows_read=rows_read,
        rows_written=rows_written,
        report=report,
        fingerprint=short_fingerprint(),
        seconds=time.monotonic() - started,
        metrics=metrics,
    )

    cleaned.unpersist()
    if not emit:
        return result

    from src.kafka.producer import emit as announce

    return replace(result, event=announce(result, broker, engine="spark"))
