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
