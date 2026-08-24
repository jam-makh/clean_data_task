"""
The counting pass's own behaviour, on frames written here.

``tests/test_parity.py`` asserts the thing that matters -- the two engines
report the same numbers about the same file -- and it is the test that has to
pass before anything ships. It cannot, however, say much about the branches
the sample never takes: the forecast extract has no exact duplicates, no
business-key repeats and no metric that comes out zero and is meant to
disappear, so a helper that got those wrong would pass parity in silence.

Each frame below is four rows long and exists to take one of those branches.
"""

import pytest

from src.utils.report import CleaningReport

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from src.spark import audit  # noqa: E402


@pytest.fixture
def frame(spark):
    """
    :returns: Four rows, with a repeated key, a null, and a label column whose
        two commonest labels tie.
    """
    return spark.createDataFrame(
        [
            ("a", "x", 1, "red"),
            ("a", "x", 2, "red"),
            ("b", "y", 3, "blue"),
            ("c", None, 4, "blue"),
        ],
        "key string, other string, seq int, label string",
    )


def read(frame, pairs) -> dict:
    """
    :param frame: The frame to count over.
    :param pairs: ``(metric, request)`` as a stage would return them.
    :returns: The report as ``metric -> value``, absent metrics absent.
    """
    requests = audit.Requests()
    requests.add("step", pairs)
    report = CleaningReport()
    audit.collect(frame, requests, report)
    return {metric: value for _, metric, value in report.entries}


@pytest.mark.spark
def test_a_null_condition_is_not_a_hit(frame):
    """
    ``fillna(False)`` without the fillna.

    The row whose ``other`` is null must not count toward a test of what
    ``other`` is. A mask states that something is true of a row, and
    "unknown" is not it -- and in Spark the comparison yields null rather
    than False, so a helper reaching for ``sum(cast(mask))`` would count it
    as nothing but a helper reaching for ``count(mask)`` would count it as
    something.
    """
    values = read(frame, [
        ("is_x", audit.rows(F.col("other") == "x")),
        ("is_not_x", audit.rows(F.col("other") != "x")),
    ])

    assert values == {"is_x": 2, "is_not_x": 1}


@pytest.mark.spark
def test_nothing_matched_still_reports_zero(frame):
    """
    A question that was asked and answered "none" is not the same as a
    question nobody asked, and only the second one is allowed to vanish.
    """
    values = read(frame, [
        ("asked", audit.rows(F.col("other") == "zzz")),
        ("guarded", audit.rows(F.col("other") == "zzz", nonzero=True)),
    ])

    assert values == {"asked": 0}


@pytest.mark.spark
def test_a_minimum_over_no_rows_is_not_reported(frame):
    """
    The ``if rejected.any():`` guard, reached without asking twice.

    A minimum over nothing is not zero -- zero is a sequence number this file
    could genuinely hold -- so the metric has to disappear rather than be
    answered wrongly.
    """
    values = read(frame, [
        ("first", audit.minimum(F.when(F.col("key") == "b", F.col("seq")))),
        ("none", audit.minimum(F.when(F.col("key") == "zzz", F.col("seq")))),
    ])

    assert values == {"first": 3}


@pytest.mark.spark
def test_distinct_ignores_the_rows_it_was_not_asked_about(frame):
    """
    ``df.loc[mask, column].nunique()`` written as a null-guarded expression:
    the rows outside the mask become null, and null is not a value.
    """
    values = read(frame, [
        ("all", audit.distinct(F.col("other"))),
        ("subset", audit.distinct(F.when(F.col("key") == "a", F.col("other")))),
        ("empty", audit.distinct(
            F.when(F.col("key") == "zzz", F.col("other")), nonzero=True
        )),
    ])

    assert values == {"all": 2, "subset": 1}


@pytest.mark.spark
def test_shared_counts_every_row_of_every_repeat(frame):
    """
    ``duplicated(keep=False)``, not ``keep="first"`` and not a group count.

    Two rows share ``key``, so the answer is two rather than one: the metric
    asks how many rows are implicated, and a business-key repeat implicates
    both halves of it -- neither is the copy.
    """
    values = read(frame, [
        ("one_key", audit.shared(["key"])),
        ("two_keys", audit.shared(["key", "seq"])),
    ])

    assert values == {"one_key": 2, "two_keys": 0}


@pytest.mark.spark
def test_a_tally_names_one_metric_per_label(frame):
    """
    Commonest first, and the rows the stage excluded excluded.

    The two labels tie at two rows each, which is the case the module
    docstring calls out: pandas would break the tie by first appearance and
    this breaks it by label, so ``blue`` leads. Both engines report the same
    two numbers under the same two names either way.
    """
    values = read(frame, [
        ("label", audit.ranked(F.col("label"), "label[{}]")),
        ("narrow", audit.ranked(
            F.when(F.col("key") == "a", F.col("label")), "narrow[{}]"
        )),
    ])

    assert values == {
        "label[blue]": 2, "label[red]": 2, "narrow[red]": 2,
    }


@pytest.mark.spark
def test_a_flag_code_is_matched_between_delimiters(spark):
    """
    ``FX_RATE_OFF`` must never be found inside ``FX_RATE_OFF_REFERENCE``.

    The pandas side splits the column and compares whole codes, so a
    substring cannot match there; here the same guarantee has to be spelled
    into the pattern, and getting it wrong would inflate one validation total
    by every row carrying a longer code that starts the same way.
    """
    flags = spark.createDataFrame(
        [("FX_RATE_OFF_REFERENCE",), ("A;FX_RATE_OFF",), ("FX_RATE_OFF;B",),
         ("",)],
        "VALIDATION_FLAGS string",
    )

    values = read(flags, [
        ("short", audit.rows(audit.carries(F.col("VALIDATION_FLAGS"),
                                           "FX_RATE_OFF"))),
        ("long", audit.rows(audit.carries(F.col("VALIDATION_FLAGS"),
                                          "FX_RATE_OFF_REFERENCE"))),
    ])

    assert values == {"short": 2, "long": 1}


@pytest.mark.spark
def test_a_bracketed_code_is_not_read_as_a_character_class(spark):
    """
    ``REQUIRED_NULL[TXN_ID]`` is a literal, and every real flag code from the
    required-column checks has brackets in it -- so an unescaped pattern would
    turn the commonest family of codes into a character class matching single
    letters, and report a total for a check nobody made.
    """
    flags = spark.createDataFrame(
        [("REQUIRED_NULL[TXN_ID]",), ("REQUIRED_NULLT",), ("",)],
        "VALIDATION_FLAGS string",
    )

    values = read(flags, [
        ("literal", audit.rows(audit.carries(F.col("VALIDATION_FLAGS"),
                                             "REQUIRED_NULL[TXN_ID]"))),
    ])

    assert values == {"literal": 1}
