"""
The Spark orchestrator, and the ledger of what has actually been ported.

It mirrors ``src/pipeline.py`` deliberately -- same step names, same registry
shape, same "a profile is a list of strings in YAML rather than a list of
imports in Python" indirection -- because the parity harness runs the two
side by side and any difference in how they are *driven* would show up as a
difference in what they produce.

The registry is the ledger, and it is a fact worth reading off one line rather
than inferring from which files exist: ``ported()`` is what the harness asks to
decide how far down a profile it can compare, so a stage becomes tested by
being registered here and by nothing else. Registering a stage that is not
finished turns its parity test red immediately, which is the intended way
round.

``forecast_balance`` is now ported end to end -- every step in that profile is
below, so the cumulative parity test compares the whole pipeline rather than a
prefix of it. ``transactions_v4`` is ported not at all, and deliberately: its
first step is ``dates``, which is the v4 workbook's date handling and has no
counterpart here.

The step contract is ``BaseCleaner.apply``'s minus the report: a function of a
DataFrame and the policy, returning a new DataFrame. What replaces the report
is diagnostic columns the stage marks and a collection pass that counts them
at the end. Both halves exist now: every stage below marks its rows, every
stage below has a ``metrics`` twin naming what to count, and ``report()``
executes the lot in two actions over the finished frame -- see
``src/spark/audit.py`` for why two and not one per stage.
"""

from pyspark.sql import functions as F

from src.config.policy import Policy
from src.config.policy import load as load_policy
from src.spark import audit
from src.spark.cleaners import (
    amounts,
    balance,
    codes,
    consistency,
    duplicates,
    geo,
    macro,
    mcc,
    merchant,
    missing,
    timestamps,
)
from src.utils.report import CleaningReport

# Step name to the thing that runs it, for the steps that have been ported.
# The names are the ones ``config/pipeline.yaml`` profiles already use, so a
# profile does not know or care which engine runs it.
#
# `dates` is deliberately absent and stays that way: it belongs to the v4
# workbook profile, which is not being ported -- the source in hand is the
# forecast_balance CSV, and sql/schema.sql scopes itself to it for the same
# reason. With `consistency` the forecast_balance profile is ported end to end.
SPARK_STEP_REGISTRY: dict[str, object] = {
    "timestamps": timestamps.apply,
    "macro": macro.apply,
    "duplicates": duplicates.apply,
    "codes": codes.apply,
    "amounts": amounts.apply,
    "balance": balance.apply,
    "missing": missing.apply,
    "merchant": merchant.apply,
    "geo": geo.apply,
    "mcc": mcc.apply,
    "consistency": consistency.apply,
}


# The other half of each registered stage: what to count once every stage has
# run. Kept as a second mapping rather than as a tuple in the first, because
# the two are asked for at different times and by different code -- ``run``
# never needs a metrics function and ``report`` never needs an apply.
#
# A stage MUST appear in both, which
# ``test_every_ported_stage_can_be_counted`` asserts: a stage registered
# here and not there runs correctly and is never mentioned in the report,
# which reads exactly like a stage that found nothing to do.
SPARK_METRICS_REGISTRY: dict[str, object] = {
    "timestamps": timestamps.metrics,
    "macro": macro.metrics,
    "duplicates": duplicates.metrics,
    "codes": codes.metrics,
    "amounts": amounts.metrics,
    "balance": balance.metrics,
    "missing": missing.metrics,
    "merchant": merchant.metrics,
    "geo": geo.metrics,
    "mcc": mcc.metrics,
    "consistency": consistency.metrics,
}


def ported(names) -> list[str]:
    """
    The longest run of steps from the start of a profile that Spark can do.

    A *prefix* rather than a filter, because these stages are not independent:
    ``amounts`` signs by the type ``codes`` resolved, ``balance`` moves by the
    figure ``amounts`` parsed. Running the ported subset out of order would
    compare a Spark frame that skipped a stage against a pandas frame that
    did not, and report the skipped stage's columns as the difference.

    :param names: Step names in profile order.
    :returns: The leading names that are registered, stopping at the first
        that is not.
    """
    prefix = []
    for name in names:
        if name not in SPARK_STEP_REGISTRY:
            break
        prefix.append(name)
    return prefix


def steps_for(names) -> list:
    """
    :param names: Step names in run order.
    :returns: The registered implementations they name.
    :raises KeyError: If a name is not registered, listing what is -- and
        naming the pandas registry, because "unknown step" and "known step,
        not yet ported" are different problems with different fixes and the
        same symptom.
    """
    unknown = [n for n in names if n not in SPARK_STEP_REGISTRY]
    if unknown:
        raise KeyError(
            f"step(s) {unknown} have no Spark implementation; ported so far: "
            f"{sorted(SPARK_STEP_REGISTRY) or 'none'}. If the name is right, "
            f"the stage is simply not ported yet -- src/pipeline.py has the "
            f"pandas one."
        )
    return [SPARK_STEP_REGISTRY[n] for n in names]


def run(frame, names, policy: Policy | None = None):
    """
    Runs the named steps over a Spark frame, in order.

    :param frame: A Spark DataFrame, all columns string, from
        ``src.spark.spark_setup.read_csv``.
    :param names: Step names in run order.
    :param policy: Resolved once here and handed to every step, for the reason
        ``TransactionCleaner`` gives: an executor resolving its own policy
        would be reading a file that may not be on the machine it is running
        on. Loaded eagerly when absent so a malformed policy fails before the
        first row moves.
    :returns: The transformed frame. With no names, the frame unchanged --
        which is what makes the harness's "compare the raw read" case a real
        comparison rather than a special case.
    """
    policy = policy if policy is not None else load_policy()
    for step in steps_for(names):
        frame = step(frame, policy)
    return frame


def report(cleaned, names, policy: Policy | None = None, source=None):
    """
    Reads the run's totals back off the finished frame.

    The counterpart of ``TransactionCleaner.run``'s second loop, and the same
    contract: nothing was counted while the rows were moving, so everything is
    derived here, in step order, from the diagnostic columns the stages left
    behind. Collecting in step order reproduces the order the pandas report
    reads in, metric for metric.

    Cost is fixed rather than per-stage. Every stage's scalar metrics go into
    one ``agg`` and every stage's label tallies into one grouped pass, so a
    report over eleven stages is two actions over ``cleaned`` -- plus one
    count of ``source``, when the caller wants ``input_rows``.

    Because it is two actions, ``cleaned`` is computed twice unless it is
    cached; caching it is the caller's decision, since the caller is the one
    who knows whether it is a 6,000-row sample or a quarter of a million rows
    that took a minute to produce.

    :param cleaned: The frame as ``run`` returned it.
    :param names: The step names that ran, in run order.
    :param policy: The same policy the run used. Resolved here when absent,
        which is safe only because every ``metrics`` reads the policy for
        thresholds it does not itself apply -- if the two generations of the
        file disagreed, the report would name checks the run never made.
        Passing the run's own policy is therefore the correct thing to do.
    :param source: The frame that went in, counted for ``input_rows``. Omitted
        rather than guessed when absent: the input row count cannot be
        recovered from the output once a stage has dropped rows, and a report
        that silently reports the output count twice is worse than one that
        reports the number once.
    :returns: A ``CleaningReport``, ready to print or to write as a sheet.
    """
    policy = policy if policy is not None else load_policy()
    unknown = [n for n in names if n not in SPARK_METRICS_REGISTRY]
    if unknown:
        raise KeyError(
            f"step(s) {unknown} have no Spark metrics; ported so far: "
            f"{sorted(SPARK_METRICS_REGISTRY) or 'none'}"
        )

    out = CleaningReport()
    if source is not None:
        out.record("pipeline", "input_rows", source.count())

    requests = audit.Requests()
    for name in names:
        requests.add(name, SPARK_METRICS_REGISTRY[name](cleaned, policy))
    # ``output_rows`` rides along in the same aggregate as everything else
    # rather than costing a ``count()`` of its own, and is recorded last for
    # the same reason pandas records it last: it is the run's closing
    # statement, not one of the steps'.
    requests.add("pipeline", [("output_rows", audit.Scalar(F.count(F.lit(1))))])

    audit.collect(cleaned, requests, out)
    return out
