"""
Narrating a run while it happens, layer by layer.

``pipeline.report`` says what a run did once it is over, which is the right
shape for a batch job and the wrong one for a consumer: a process that sits
there for an hour and prints one table at the end is indistinguishable, while
it is working, from one that is stuck. This is the other view -- each stage
announced as it runs, grouped by the layer it belongs to, with what it changed.

    +- raw id 42 | job 4f2c9b1e ---------------------------
    | [INGESTION    ] read raw_transactions        1 row   0.31s
    | [NORMALIZATION] timestamps                   1 row   0.44s
    |     TXN_TS_STATUS[OBSERVED] = 1
    | [ENRICHMENT   ] macro                        1 row   0.12s
    | ...
    | [PERSISTENCE  ] upsert cleaned_transactions  1 row   0.52s
    +- 1 row cleaned in 4.8s -----------------------------

ASCII, and not by preference
----------------------------

The box above is drawn with ``+`` and ``|`` rather than the line-drawing
characters that would look better, because this project's terminal is a
Windows one and Python writes to it through cp1252 by default. A ``U+2502``
reaching that console does not render as a fallback glyph -- it raises
``UnicodeEncodeError`` from inside the print, which would take down a running
consumer over a decoration. The same goes for the ellipsis in a truncated
metric name, which is three dots here for the same reason.

The counting is not free, and that is the whole design question
------------------------------------------------------------------

A Spark frame is a recipe, not a result. Between stages there is nothing to
count without running the recipe -- so a stage log that prints row counts
forces one action per stage, and an eleven-stage run over 265k rows becomes
eleven passes over the data instead of one. That is not a log, it is a
performance bug with a nice format.

So ``counts`` is a switch, and it is off unless someone asked for it:

* the **batch run** leaves it off, because 265k rows through eleven counted
  stages costs minutes to say things ``report()`` says at the end for free;
* the **consumer** leaves it off too, which it did not always. A batch of one
  row makes each action cheap and the total is still eleven actions and eleven
  cached frames per message, on a driver that never restarts -- see
  ``spark_setup.RETAINED_JOBS`` for where that ends up. ``consumer.py
  --trace`` turns it on for a batch someone is watching.

With ``counts`` off the stages are still announced, just without numbers and
without timings -- because a timing with no action in it measures how long
Spark took to *plan* the step, which is a real number that means nothing and
would be read as though it meant something.

Turning it off costs no auditability, which is the only reason it can be off.
``audit()`` writes the run's ``CleaningReport`` into the same log at the end
of every batch, unconditionally: what each step coerced, dropped or flagged,
over the finished frame, in two actions rather than eleven. The stage lines
are the narration and were never the record.

What the numbers are, and are not
---------------------------------

Each stage's line reports that stage's own metrics, evaluated **at that point
in the run**. The final ``report()`` evaluates the same metrics over the
finished frame. They can disagree, legitimately: ``duplicates`` drops rows, so
a count taken before it and the same count taken at the end are over different
row sets. The report is the authority and this is the narration -- which is
why the footer prints the report's own totals rather than a sum of the lines
above it.
"""

import time

from src.spark.spark_setup import is_fatal
from src.utils.report import CleaningReport

# Which layer each stage belongs to. The grouping is editorial -- Spark has no
# opinion about it -- and it is here rather than in each stage module because
# it is a statement about the shape of the pipeline as a whole, which no single
# stage is in a position to make.
#
# Note that the layers interleave rather than running in blocks: `macro` is
# enrichment and runs second, before three normalization stages. That is the
# profile's order and it is correct -- the macro join needs the month that
# `timestamps` produces, and `codes` does not need either. A log that sorted
# these into tidy blocks would be describing a pipeline that does not exist.
LAYERS = {
    # Text into values: parse, pad, sign, and settle what is absent.
    "timestamps": "NORMALIZATION",
    "codes": "NORMALIZATION",
    "amounts": "NORMALIZATION",
    "missing": "NORMALIZATION",
    # Rows into distinct rows.
    "duplicates": "DEDUPLICATION",
    # Values this pipeline computes rather than reads.
    "balance": "DERIVATION",
    # Reference data joined in from outside the row.
    "macro": "ENRICHMENT",
    "merchant": "ENRICHMENT",
    "geo": "ENRICHMENT",
    "mcc": "ENRICHMENT",
    # Disagreements found and flagged, nothing repaired.
    "consistency": "VALIDATION",
    # The two ends, which are not pipeline steps at all -- they belong to
    # whoever reads and whoever writes, and are here so that one vocabulary
    # covers the whole journey a row makes.
    "read": "INGESTION",
    "write": "PERSISTENCE",
}

# For a step nobody has classified. Deliberately not "UNKNOWN": a newly ported
# stage is not a mystery, it is a stage whose layer nobody has written down,
# and the log should keep working while that is true.
DEFAULT_LAYER = "CLEANING"

# Width of the bracketed layer, so the stage names line up in a column. The
# longest label above is NORMALIZATION at 13.
LAYER_WIDTH = 13

# Width of the stage-name column.
NAME_WIDTH = 24

# Past this, a metric line is truncated. Long enough for the longest metric
# name the stages produce plus its value, short enough to stay on one line in
# a normal terminal.
METRIC_WIDTH = 96


class StageLog:
    """
    Prints what each stage did, as it does it.

    Built to be passed to ``src.spark.pipeline.run`` as its ``listener``, and
    used directly by the consumer for the two ends -- the read and the write
    -- which are not pipeline steps but are part of the same journey and
    belong in the same narration.

    Every method is defensive about its own failure. A stage log that raises
    would turn a working pipeline into a broken one over a formatting mistake,
    which is an unacceptable trade for output nobody's data depends on -- so a
    failure while measuring is printed and stepped over. It is printed, and not
    swallowed: a log that quietly stops reporting is worse than one that says
    it could not.
    """

    def __init__(self, write=print, counts: bool = True, policy=None):
        """
        :param write: Where a line goes. ``print`` normally; a list's
            ``append`` in a test, which is what makes this testable without
            capturing stdout.
        :param counts: Evaluate each stage's metrics as it finishes. Costs one
            Spark action per stage -- see the module docstring. Off means the
            stages are still announced, without numbers and without timings.
        :param policy: The policy the run is using. The metrics functions read
            it for thresholds they do not themselves apply, so passing the
            run's own is the only correct thing to do; loaded when absent,
            which is safe only because the two would be the same file.
        """
        self.write = write
        self.counts = counts
        self._policy = policy
        self._started: float | None = None
        self._step_started: float | None = None
        self._rows: int | None = None
        # Frames cached for measurement, oldest first. A list rather than a
        # single slot because a stage whose metrics raised leaves one behind:
        # with one slot that frame would be cached and no longer referenced,
        # so nothing could ever release it. Here it is simply still in the
        # list, and the next release takes it.
        self._held: list = []

    # -- the two ends ------------------------------------------------------

    def opening(self, title: str) -> None:
        """
        :param title: What this run is -- the row id and the job id, for the
            consumer. Starts the clock the footer reports against.
        """
        self._started = time.monotonic()
        self.write(f"\n+- {title} " + "-" * max(0, 56 - len(title)))

    def event(self, step: str, message: str, rows=None, seconds=None) -> None:
        """
        One line that is not a pipeline step: the read, the write, a note.

        :param step: A key in ``LAYERS`` -- ``"read"`` or ``"write"`` -- or
            any name, which lands in ``DEFAULT_LAYER``.
        :param message: What happened.
        :param rows: Row count, when the caller has one to hand. Not measured
            here: the caller that just read or wrote them already knows,
            and asking Spark again would be a second action for a number
            somebody is holding.
        :param seconds: How long it took.
        """
        self.write(self._line(step, message, rows, seconds))

    def note(self, message: str) -> None:
        """
        :param message: An aside -- a warning, a skipped row, a retry. Indented
            under the line above it rather than given a layer of its own,
            because it is a remark about that line and not another step.
        """
        self.write(f"|     {message}")

    def closing(self, summary: str) -> None:
        """
        :param summary: The run's own account of itself. The caller's, and
            normally read off ``report()`` rather than accumulated from the
            lines above -- see the module docstring on why those two can
            legitimately differ.
        """
        elapsed = (
            f" in {time.monotonic() - self._started:.1f}s"
            if self._started is not None else ""
        )
        self.write(f"+- {summary}{elapsed} " + "-" * max(0, 40 - len(summary)))
        self._started = None
        # The last stage's frame, which nothing releases but this. Called here
        # because the footer is written after the caller has finished with the
        # frame -- it has reported on it and written it by now -- and a
        # consumer that runs for days would otherwise accumulate one cached
        # batch per message until the session ran out of room.
        self._release()

    def audit(self, report: CleaningReport) -> None:
        """
        Writes the run's own audit trail into the log.

        :param report: The ``CleaningReport`` ``pipeline.report`` produced --
            what was coerced, dropped or flagged, and by which step.

        Separate from the per-stage lines above, and not an alternative to
        them. Those are narration: this stage's metrics, evaluated at this
        point in the run, printed so a watching operator can see the work
        happening. This is the record: every step's metrics over the finished
        frame, which is the same report the batch run writes as a sheet and
        the same one ``RunResult.report`` carries to the completion event.

        It is written unconditionally, because ``counts`` is a switch on the
        *narration* and must not be a switch on the *audit*. The two costs are
        not comparable -- the narration is one Spark action per stage, this is
        two for the whole run over a frame the runner has already cached -- so
        there is no configuration in which turning the audit off is the saving
        anyone wanted. A consumer that cleaned a row and said nothing about
        what it changed would be the silent cleaning this pipeline exists to
        not do.

        Zero-valued metrics are kept here, unlike in the stage lines. A stage
        line drops them because eleven ``= 0``s would bury the one number that
        is not; a record drops nothing, because "this check ran and found
        nothing" and "this check did not run" are different findings and only
        one of them is good news.
        """
        if not report.entries:
            self.note("(audit: nothing recorded)")
            return
        self.note("audit:")
        width = max(len(step) for step, _, _ in report.entries)
        for step, metric, value in report.entries:
            self.note(_truncate(
                f"  {step:<{width}}  {metric} = {value}", METRIC_WIDTH
            ))

    def _release(self, keep_last: bool = False) -> None:
        """
        Drops held frames from Spark's cache.

        :param keep_last: Keep the most recent, which the run is still
            building on. False at the end, when it is not.

        Failure here is swallowed rather than reported, unlike everywhere else
        in this class. Unpersisting is a hint: the frame may already be gone,
        the session may be stopping, and none of that is worth a line in the
        middle of a run that has otherwise succeeded.
        """
        while len(self._held) > (1 if keep_last else 0):
            frame = self._held.pop(0)
            try:
                frame.unpersist()
            except Exception:  # noqa: BLE001 - a failed cache hint is not news
                pass

    # -- the listener protocol --------------------------------------------

    def starting(self, name: str, position: int, total: int) -> None:
        """
        :param name: The step about to run.
        :param position: Its place in the run, from one.
        :param total: How many steps there are.

        Prints nothing. The line is written when the step *finishes*, because
        that is when there is something to say about it -- announcing a step
        before it runs would double the output to report that the pipeline
        intends to do what it is about to do.
        """
        del name, position, total
        self._step_started = time.monotonic()

    def finished(self, name: str, frame) -> None:
        """
        :param name: The step that just ran.
        :param frame: The frame as it left it.
        """
        if not self.counts:
            self.write(self._line(name, name))
            return

        seconds = (
            time.monotonic() - self._step_started
            if self._step_started is not None else None
        )
        try:
            rows, metrics = self._measure(name, frame)
        except Exception as exc:  # noqa: BLE001 - logging must not fail a run
            self.write(self._line(name, name, None, seconds))
            self.note(f"(metrics unavailable: {type(exc).__name__}: {exc})")
            # Swallowed, but not indiscriminately. "Logging must not fail a
            # run" is the rule for a metric that could not be computed; it is
            # the wrong rule for a driver that has run out of heap, where the
            # run is already over and every stage after this one will print
            # the same line for the same reason. Reporting that as a missing
            # metric is how a dead session goes on consuming messages and
            # marking good rows FAILED -- see spark_setup.is_fatal.
            if is_fatal(exc):
                raise
            return

        # Measured after the aggregate, which is the action: the time before
        # it is plan construction and would report every stage as instant.
        seconds = (
            time.monotonic() - self._step_started
            if self._step_started is not None else None
        )
        self._rows = rows
        self.write(self._line(name, name, rows, seconds))
        for metric, value in metrics:
            self.note(_truncate(f"{metric} = {value}", METRIC_WIDTH))

    # -- internals ---------------------------------------------------------

    def _measure(self, name: str, frame):
        """
        Evaluates one stage's metrics over the frame as it stands.

        One action, not two: the row count rides along in the same aggregate
        as the stage's own metrics rather than costing a ``count()`` of its
        own -- the same trick ``pipeline.report`` uses for ``output_rows``.

        The cache is the difference between a log and a hang
        ----------------------------------------------------

        A Spark frame is a recipe. Measuring after stage *n* runs stages 1..n;
        measuring after stage *n+1* runs 1..n+1 **from the source again**,
        because nothing kept the intermediate. Over eleven stages that is
        sixty-six stage-executions rather than eleven, and it is worse than
        the arithmetic suggests: ``merchant`` carries an Arrow-batched Python
        UDF, so every one of those re-executions pays to start Python workers
        again.

        Measured, not assumed: two rows through the eleven-stage profile did
        not finish in ten minutes without this, and had reached Spark job 54.

        So each measured frame is cached -- ``cache()`` returns the same
        object, so the loop continues from the materialised frame -- and the
        previous stage's cache is released once the next one has been
        computed. Peak memory is therefore two stages' worth of one batch, and
        this only ever happens under ``counts``, which is already the caller
        saying "I am paying for actions to watch this happen".

        :param name: The step whose metrics to read.
        :param frame: The frame it produced.
        :returns: (rows, [(metric, value), ...]) with zero-valued metrics
            dropped -- a stage that found nothing has nothing to report, and
            eleven lines of ``= 0`` would bury the one line that is not.
        """
        from pyspark.sql import functions as F

        from src.spark import audit
        from src.spark.pipeline import SPARK_METRICS_REGISTRY

        frame.cache()
        self._held.append(frame)
        requests = audit.Requests()
        if name in SPARK_METRICS_REGISTRY:
            requests.add(name, SPARK_METRICS_REGISTRY[name](frame, self.policy))
        requests.add("stagelog", [("rows", audit.Scalar(F.count(F.lit(1))))])

        report = CleaningReport()
        audit.collect(frame, requests, report)

        # Released only now, after the aggregate above has materialised this
        # stage's frame -- the previous one is what it was computed from, and
        # dropping it any earlier would mean recomputing exactly what the
        # cache was there to avoid. This one stays until the next stage has
        # been measured, for the same reason.
        self._release(keep_last=True)

        rows = None
        metrics = []
        for step, metric, value in report.entries:
            if step == "stagelog" and metric == "rows":
                rows = value
            elif value:
                metrics.append((metric, value))
        return rows, metrics

    @property
    def policy(self):
        """
        :returns: The policy the metrics functions read, loaded on first use.
            Lazy so that constructing a log costs nothing -- a consumer builds
            one per message.
        """
        if self._policy is None:
            from src.config.policy import load

            self._policy = load()
        return self._policy

    def _line(self, step: str, message: str, rows=None, seconds=None) -> str:
        """:returns: One formatted row of the narration."""
        layer = LAYERS.get(step, DEFAULT_LAYER)
        counted = "" if rows is None else _plural(rows)
        timed = "" if seconds is None else f"{seconds:6.2f}s"
        return (
            f"| [{layer:<{LAYER_WIDTH}}] {message:<{NAME_WIDTH}} "
            f"{counted:>12} {timed}"
        ).rstrip()


def _plural(rows: int) -> str:
    """:returns: ``1 row`` or ``4 rows``, because a log that says "1 rows" is
    a log nobody proofread."""
    return f"{rows:,} row" + ("" if rows == 1 else "s")


def _truncate(text: str, width: int) -> str:
    """:returns: ``text``, shortened with an ellipsis if it is over ``width``. Three dots
    and not U+2026 -- see the note on cp1252 in the module docstring."""
    return text if len(text) <= width else text[: width - 3] + "..."
