"""
The harness in its working position: pandas against Spark, on the real file.

This is the test that grows. Today the Spark registry is empty, so what it
compares is the two readers -- which is already a real assertion and not a
trivially true one: it says that 11,417 rows and 22 columns of a genuinely
dirty extract arrive identically through two completely different readers,
including which cells are null and which are the empty string.

Each later phase registers stages in ``src.spark.pipeline`` and this same test
starts comparing further down the profile, without being edited. That is the
whole reason ``ported()`` returns a prefix of the profile rather than a list
of files that happen to exist: the boundary of what is claimed moves by one
line in the registry, and the claim is checked immediately.
"""

import pytest

from src.config import runtime
from src.pipeline import TransactionCleaner, steps_for
from src.spark import pipeline as spark_pipeline
from tests.harness.parity import assert_parity
from src.spark.spark_setup import read_csv


@pytest.fixture(scope="session")
def profile_steps(sample_frame):
    """
    :returns: The step names the sample's own columns select.

    Detected rather than named, so the harness runs whatever the pipeline
    would run on this file. Hardcoding ``forecast_balance`` here would let the
    two diverge -- the harness could go on comparing a step list the pipeline
    had stopped using.
    """
    return runtime.load().detect(sample_frame.columns).steps


@pytest.mark.spark
def test_readers_agree(spark, sample_path, sample_frame):
    """
    Both readers see the same file, cell for cell.

    The foundation everything else stands on. If the Spark reader coerced a
    blank to an empty string, or dropped a byte order mark differently, every
    later stage would fail parity for a reason that had nothing to do with
    the stage.
    """
    result = assert_parity(sample_frame, read_csv(spark, sample_path))

    assert result.left_rows == len(sample_frame)
    assert len(result.compared) == len(sample_frame.columns) - 1


@pytest.fixture(scope="session")
def full_run(spark, sample_path, sample_frame, profile_steps):
    """
    :returns: ``(names, pandas_cleaner, spark_frame)`` for the whole ported
        prefix, run once.

    Both the frame comparison and the report comparison below need the same
    two runs, and the Spark half of it is the most expensive thing in the
    suite. Running it once and asserting twice is not only faster: it means
    the report being compared is the report of the frame being compared, which
    is the claim actually worth making.
    """
    names = spark_pipeline.ported(profile_steps)
    cleaner = TransactionCleaner(steps=steps_for(names))
    cleaner.run(sample_frame)
    return names, cleaner, spark_pipeline.run(read_csv(spark, sample_path), names)


@pytest.mark.spark
def test_ported_stages_match(full_run):
    """
    Everything ported so far produces the same frame in both engines.

    Run as one cumulative comparison rather than stage by stage because that
    is how the stages actually run: ``amounts`` signs by what ``codes``
    resolved, ``balance`` moves by what ``amounts`` parsed, and a stage
    compared in isolation against a different upstream is not being compared
    at all.

    With nothing ported this degenerates to the reader comparison above, which
    is why it passes today without being vacuous.
    """
    _, cleaner, spark_out = full_run

    assert_parity(cleaner.result, spark_out)


@pytest.mark.spark
def test_reports_match(full_run, sample_path, spark):
    """
    The two engines account for the run identically, metric for metric.

    The frame comparison above says the rows agree; this says the two
    pipelines agree about what they *did* to them, which is a separate claim
    and the one a reader of the report is relying on. A stage that produced
    the right column while marking the wrong rows passes the first and fails
    this.

    Compared as a mapping rather than as a sequence, for the reason
    ``src/spark/audit.py`` states: a tie between two equally common labels
    breaks by first appearance in pandas and by label in Spark, which can
    reorder two rows that hold the same numbers. Every ``(step, metric)`` key
    and every value still has to match exactly, in both directions -- a metric
    present on one side and absent on the other is the failure this is most
    likely to catch.
    """
    names, cleaner, spark_out = full_run
    actual = spark_pipeline.report(
        spark_out, names, source=read_csv(spark, sample_path)
    )

    expected = {(s, m): v for s, m, v in cleaner.report.entries}
    reported = {(s, m): v for s, m, v in actual.entries}
    differing = {
        key: (expected.get(key, "<absent>"), reported.get(key, "<absent>"))
        for key in set(expected) | set(reported)
        if expected.get(key, "<absent>") != reported.get(key, "<absent>")
    }

    assert not differing, (
        "pandas vs spark, by (step, metric): "
        + "; ".join(
            f"{step}.{metric} {left} != {right}"
            for (step, metric), (left, right) in sorted(differing.items())
        )
    )


def test_every_ported_stage_can_be_counted():
    """
    A stage in one registry and not the other.

    The failure it prevents is silent in the worst way: the run produces the
    right frame, the report simply never mentions the stage, and a step that
    quietly nulled four hundred rows reads exactly like one that changed
    nothing -- which is the thing ``CleaningReport`` exists to make impossible.
    """
    missing = set(spark_pipeline.SPARK_STEP_REGISTRY) - set(
        spark_pipeline.SPARK_METRICS_REGISTRY
    )
    extra = set(spark_pipeline.SPARK_METRICS_REGISTRY) - set(
        spark_pipeline.SPARK_STEP_REGISTRY
    )

    assert not missing, f"ported but never counted: {sorted(missing)}"
    assert not extra, f"counted but never run: {sorted(extra)}"


def test_the_registry_is_a_prefix_of_the_profile(profile_steps):
    """
    Nothing is registered under a name the profile does not use.

    A Spark step registered as ``timestamp`` while the profile says
    ``timestamps`` would never run and never be compared, and the harness
    would go on passing while claiming coverage it does not have. No JVM
    needed to check it, so it runs on every suite.
    """
    unknown = set(spark_pipeline.SPARK_STEP_REGISTRY) - set(profile_steps)

    assert not unknown, (
        f"registered under name(s) no profile runs: {sorted(unknown)}"
    )


def test_ported_stops_at_the_first_gap():
    """
    The prefix rule, asserted directly rather than through a Spark run.

    Stages are not independent, so running the registered subset out of order
    would compare a Spark frame that skipped a stage against a pandas frame
    that did not -- and report the skipped stage's columns as the difference,
    which points at the wrong stage entirely.
    """
    registry = dict(spark_pipeline.SPARK_STEP_REGISTRY)
    try:
        spark_pipeline.SPARK_STEP_REGISTRY.clear()
        spark_pipeline.SPARK_STEP_REGISTRY.update({"a": object(), "c": object()})

        assert spark_pipeline.ported(["a", "b", "c"]) == ["a"]
    finally:
        spark_pipeline.SPARK_STEP_REGISTRY.clear()
        spark_pipeline.SPARK_STEP_REGISTRY.update(registry)


def test_an_unported_step_names_the_pandas_registry():
    """
    "Unknown step" and "known step, not ported yet" look identical and are
    not: one is a typo and one is a Monday.

    ``dates`` rather than any other name, because it is the one step that is
    unported by decision rather than by not having got to it yet -- it is the
    v4 workbook's date handling, and the forecast profile uses ``timestamps``
    instead. Every other name here would make this test a countdown that goes
    red the day the stage lands, which is what happened to the one it
    replaced.
    """
    with pytest.raises(KeyError, match="not ported yet"):
        spark_pipeline.steps_for(["dates"])
