"""
Reading the run's totals back out of the diagnostic columns, on Spark.

The counterpart of ``src/utils/audit.py``, and the second half of the contract
that file describes: every stage marks each row with what it did to it, nothing
counts while the rows are being touched, and one pass at the end turns those
marks into the report. On the pandas side that split is a convenience. Here it
is the only design that works at all -- a stage that counted as it went would
force a job per stage, and a running total kept in a closure would be filled in
on each executor and read back, empty, on the driver.

What a stage returns
--------------------

A stage's ``metrics(frame, policy)`` yields ``(name, request)`` pairs, in the
order the report should read them. A request is not a number; it is a
description of how to get one, so that every stage's requests can be executed
together rather than one at a time. Two kinds:

``Scalar``  one number, from one aggregate expression -- a count, a distinct
            count, a minimum. Every scalar in the whole run is evaluated in a
            single ``agg``.
``Tally``   a label column whose distinct values are not known in advance, and
            which therefore has to be grouped rather than aggregated. Every
            tally in the run is evaluated in a single grouped pass.

So a full report is two actions over the finished frame, plus one count of the
source, regardless of how many stages ran or how many numbers each asks for.
Adding a metric costs no additional pass.

One divergence from pandas, stated because it is real and cannot be closed
here. ``utils.audit.ranked`` breaks a tie between two equally common labels by
first appearance in the frame; a Spark frame has no first appearance to appeal
to after a shuffle, so ``Tally`` breaks ties by label instead. The
``(step, metric, value)`` triples are the same either way -- only the order two
tied rows appear in can differ -- which is why a report comparison should be
made as a mapping and not as a sequence.
"""

import re
from dataclasses import dataclass, field

from pyspark.sql import Column, Window
from pyspark.sql import functions as F

# Scratch columns the counting pass materialises for itself, e.g. the window
# behind a business-key repeat. Prefixed so a name collision with a real
# column is impossible, and dropped with the frame the moment the pass ends.
SCRATCH = "_audit_"


@dataclass(frozen=True)
class Scalar:
    """
    One number, read in the aggregate pass.

    :param aggregate: An aggregate expression over the finished frame.
    :param prepare: ``(name, expression)`` columns to add to the frame before
        aggregating. For window expressions, which cannot be nested inside an
        aggregate and so have to be materialised first. Names are already
        prefixed with ``SCRATCH`` by the helpers below.
    :param nonzero: Report this only when the count is not zero, which is the
        pandas ``if count:`` guard -- a stage reporting a zero for a question
        it never asked claims it looked and found nothing.
    """

    aggregate: Column
    prepare: tuple[tuple[str, Column], ...] = ()
    nonzero: bool = False


@dataclass(frozen=True)
class Tally:
    """
    A count per distinct label, read in the grouped pass.

    :param label: A string expression per row. Null means "this row is not
        part of this tally" and is dropped -- which is how a stage expresses
        ``df.loc[mask, column]`` without the collector having to know what the
        mask meant.
    :param template: How a label becomes a metric name, e.g. ``"signal[{}]"``.
    """

    label: Column
    template: str = "{}"


# ---------------------------------------------------------------------------
# What a stage asks for
# ---------------------------------------------------------------------------


def rows(mask: Column, nonzero: bool = False) -> Scalar:
    """
    :param mask: Boolean expression, possibly null.
    :param nonzero: Suppress the metric when nothing matched.
    :returns: A request for how many rows it selects. A null is not a hit --
        a mask states that something is true of a row, and "unknown" is not it
        -- which ``when`` already does, since a null condition falls through
        to ``otherwise``. That is ``mask.fillna(False)`` on the pandas side,
        arrived at without a coalesce.
    """
    return Scalar(
        F.coalesce(
            F.sum(F.when(mask, F.lit(1)).otherwise(F.lit(0))), F.lit(0)
        ),
        nonzero=nonzero,
    )


def distinct(values: Column, nonzero: bool = False) -> Scalar:
    """
    :param values: Any expression. Null rows are excluded, which is what
        ``Series.nunique()`` does -- so a stage narrowing to a subset writes
        ``F.when(mask, column)`` and gets ``df.loc[mask, column].nunique()``.
    :param nonzero: Suppress the metric when the subset was empty. Reaches a
        pandas ``if mask.any():`` guard from the other side, since a distinct
        count over no rows is zero and only over no rows.
    :returns: A request for how many different values it holds.
    """
    return Scalar(F.count_distinct(values), nonzero=nonzero)


def minimum(value: Column) -> Scalar:
    """
    :param value: An expression, null on the rows that do not count.
    :returns: A request for the smallest non-null value, or nothing at all
        when every row is null. The second case is the pandas
        ``if rejected.any():`` guard: a minimum over no rows is not zero, it
        is a question with no answer, and the collector drops it rather than
        answering it wrongly.
    """
    return Scalar(F.min(value))


def shared(keys, nonzero: bool = False) -> Scalar:
    """
    ``DataFrame.duplicated(subset=keys, keep=False)`` as a request.

    Every row of every group of more than one, not the group count and not the
    excess -- and nulls group together, which is what ``duplicated`` does with
    NaN keys.

    A window rather than a mark on the row, because the stage that would have
    marked it deliberately does not: the business keys name raw source columns
    that nothing overwrites, so the answer at the end of the run is the answer
    at the time, and marking it would put a column on the Spark frame that
    pandas never produces.

    :param keys: Column names forming the key.
    :param nonzero: Suppress the metric when nothing repeats.
    :returns: A request for how many rows share their key with another.
    """
    keys = list(keys)
    name = f"{SCRATCH}shared_{'_'.join(keys)}"
    repeats = F.count(F.lit(1)).over(Window.partitionBy(*keys)) > 1
    request = rows(F.col(name), nonzero=nonzero)
    return Scalar(request.aggregate, ((name, repeats),), nonzero)


def ranked(label: Column, template: str = "{}") -> Tally:
    """
    :param label: The label column, null on rows outside the tally.
    :param template: How a label becomes a metric name.
    :returns: A request for one metric per distinct label, commonest first.
    """
    return Tally(label, template)


def carries(flags: Column, code: str) -> Column:
    """
    :param flags: The ``VALIDATION_FLAGS`` column.
    :param code: One flag code.
    :returns: Whether the row carries exactly that code, matched between
        delimiters so ``FX_RATE_OFF`` never matches ``FX_RATE_OFF_REFERENCE``.

    The pandas side splits the column once and tallies every code together,
    which is cheaper there because the split is the expensive part. Here the
    expensive part is the pass, and one pass already covers every code in the
    run, so each code gets its own predicate and the two produce the same
    counts.
    """
    pattern = rf"(?:^|;){re.escape(code)}(?:;|$)"
    return F.coalesce(flags, F.lit("")).rlike(pattern)


# ---------------------------------------------------------------------------
# Executing them
# ---------------------------------------------------------------------------


@dataclass
class Requests:
    """One run's worth of requests, in the order the report should read."""

    entries: list[tuple[str, str, object]] = field(default_factory=list)

    def add(self, step: str, pairs) -> None:
        """
        :param step: Name of the stage reporting.
        :param pairs: Its ``(metric, request)`` pairs, in report order.
        """
        for metric, request in pairs:
            self.entries.append((step, metric, request))


def collect(frame, requests: Requests, report) -> None:
    """
    Executes every request and writes the answers into the report.

    Two actions over ``frame``, in this order and no more: one ``agg`` holding
    every scalar in the run, and -- only if some stage asked for one -- one
    grouped pass holding every tally. Both read the finished frame, so the
    frame is computed twice unless the caller has cached it; that is the
    caller's decision to make, since it is the caller who knows whether the
    frame is a 6,000-row sample or a quarter of a million rows off a disk.

    :param frame: The frame as the last stage left it.
    :param requests: What every stage asked for, in report order.
    :param report: The ``CleaningReport`` to write into.
    """
    scalars = [
        (index, entry)
        for index, entry in enumerate(requests.entries)
        if isinstance(entry[2], Scalar)
    ]
    tallies = [
        (index, entry)
        for index, entry in enumerate(requests.entries)
        if isinstance(entry[2], Tally)
    ]

    prepared = frame
    for _, (_, _, request) in scalars:
        for name, expression in request.prepare:
            if name not in prepared.columns:
                prepared = prepared.withColumn(name, expression)

    # Aliased positionally. A metric name is free text -- ``status[OBSERVED]``,
    # ``REQUIRED_NULL[TXN_ID]`` -- and none of it is a legal column name, so
    # the position is what carries the identity through the aggregate.
    values: dict[int, object] = {}
    if scalars:
        row = prepared.agg(
            *[
                request.aggregate.alias(f"m{index}")
                for index, (_, _, request) in scalars
            ]
        ).collect()[0]
        for index, _ in scalars:
            values[index] = row[f"m{index}"]

    tallied = _tally(prepared, tallies) if tallies else {}

    for index, (step, metric, request) in enumerate(requests.entries):
        if isinstance(request, Tally):
            for label, count in tallied.get(index, []):
                report.record(step, request.template.format(label), count)
            continue
        value = values[index]
        # A null is a question with no answer -- a minimum over no rows --
        # and is dropped rather than reported as a zero, which would be a
        # different claim. ``nonzero`` is the stage's own guard for a count
        # it only wants to see when it happened.
        if value is None or (request.nonzero and not value):
            continue
        report.record(step, metric, int(value))


def _tally(frame, tallies) -> dict[int, list[tuple[str, int]]]:
    """
    Counts every label column in the run in one grouped pass.

    The columns are stacked into one ``(slot, label)`` pair per row per tally
    and exploded, so N label columns become one ``groupBy`` rather than N.

    :param frame: The finished frame.
    :param tallies: ``(index, (step, metric, Tally))`` for every tally asked
        for.
    :returns: Request index to ``(label, count)``, commonest first. Ties break
        by label -- see the module docstring for why they cannot break by
        first appearance.
    """
    stacked = F.array(
        *[
            F.struct(
                F.lit(index).alias("slot"),
                request.label.cast("string").alias("label"),
            )
            for index, (_, _, request) in tallies
        ]
    )
    counted = (
        frame.select(F.explode(stacked).alias("pair"))
        .select("pair.slot", "pair.label")
        .where(F.col("label").isNotNull())
        .groupBy("slot", "label")
        .count()
        .collect()
    )

    out: dict[int, list[tuple[str, int]]] = {}
    for row in counted:
        out.setdefault(row["slot"], []).append((row["label"], int(row["count"])))
    for labels in out.values():
        labels.sort(key=lambda pair: (-pair[1], pair[0]))
    return out
