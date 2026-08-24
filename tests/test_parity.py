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
from src.spark.parity import assert_parity
from src.spark.source import read_csv


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


@pytest.mark.spark
def test_ported_stages_match(spark, sample_path, sample_frame, profile_steps):
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
    names = spark_pipeline.ported(profile_steps)

    pandas_out = TransactionCleaner(steps=steps_for(names)).run(sample_frame)
    spark_out = spark_pipeline.run(read_csv(spark, sample_path), names)

    assert_parity(pandas_out, spark_out)


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
    """
    with pytest.raises(KeyError, match="not ported yet"):
        spark_pipeline.steps_for(["balance"])
