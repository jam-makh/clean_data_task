"""
Tests of the safety net itself.

A harness that always passes is worse than no harness: it converts "we have
not checked" into "we have checked and it is fine". So roughly half of these
assert that a difference IS caught, and the rest assert that the specific
differences the two engines are entitled to -- row order, column order, dtype,
Categorical storage -- are not reported as findings.

The line between those two lists is the design, and it is drawn in one place
so that no per-stage test has to redraw it. Everything here is pandas against
pandas: the comparison logic is engine-agnostic by construction, so testing it
needs no JVM and runs in milliseconds.
"""

import numpy as np
import pandas as pd
import pytest

from tests.harness.parity import assert_parity, compare


@pytest.fixture
def base():
    """:returns: A small frame with one column of each kind that matters."""
    return pd.DataFrame(
        {
            "TXN_SEQ": ["1", "2", "3", "4"],
            "AMOUNT": [10.0, -20.5, 0.0, np.nan],
            "STATUS": ["OBSERVED", "DERIVED", None, "UNKNOWN"],
            "NOTE": ["a", "", None, "d"],
        }
    )


# --- what must be caught -------------------------------------------------


def test_a_changed_value_is_caught(base):
    """One cell, named by column, with its key attached."""
    other = base.copy()
    other.loc[1, "STATUS"] = "CONTRADICTED"

    result = compare(base, other)

    assert not result.ok
    assert [d.column for d in result.differences] == ["STATUS"]
    difference = result.differences[0]
    assert difference.mismatches == 1
    assert difference.examples[0][0] == "2"


def test_null_and_empty_string_are_different_answers(base):
    """
    The distinction this pipeline exists to preserve.

    A blank cell means "the source said nothing"; an empty string means "the
    source said this". Every stage is written around that difference, so a
    harness that let them compare equal would bless exactly the bug that
    matters most.
    """
    other = base.copy()
    other.loc[2, "NOTE"] = ""

    result = compare(base, other)

    assert not result.ok
    assert result.differences[0].column == "NOTE"


def test_a_missing_column_is_caught(base):
    """A stage that has not run yet, reported as what it is."""
    result = compare(base, base.drop(columns=["STATUS"]))

    assert not result.ok
    assert result.only_left == ("STATUS",)


def test_a_missing_row_is_caught_by_key_not_by_count(base):
    """
    Dropping one row and adding another keeps the count and changes the keys.

    Aligning on position would call this equal for every column, which is why
    the harness joins on the key rather than sorting both sides.
    """
    other = pd.concat(
        [base.iloc[1:], base.iloc[:1].assign(TXN_SEQ=["99"])], ignore_index=True
    )

    result = compare(base, other)

    assert not result.ok
    assert result.missing_keys[0] == 1
    assert result.extra_keys[0] == 1


def test_a_float_beyond_tolerance_is_caught(base):
    """A cent is a real difference; the tolerance is for the last bits."""
    other = base.copy()
    other.loc[0, "AMOUNT"] = 10.01

    assert not compare(base, other).ok


def test_a_value_against_a_null_is_caught(base):
    """Two nulls agree. A null and a number do not, in either direction."""
    other = base.copy()
    other.loc[3, "AMOUNT"] = 0.0

    result = compare(base, other)

    assert not result.ok
    assert result.differences[0].column == "AMOUNT"


def test_assert_parity_reports_what_differed(base):
    """
    The message is the point.

    An assertion that only says "frames differ" sends the person who wrote the
    stage off to discover what the harness already knows.
    """
    other = base.copy()
    other.loc[0, "STATUS"] = "wrong"

    with pytest.raises(AssertionError, match="STATUS"):
        assert_parity(base, other)


# --- what must not be reported -------------------------------------------


def test_row_order_is_not_a_finding(base):
    """Spark's output order is a function of partitioning, and means nothing."""
    assert compare(base, base.iloc[::-1].reset_index(drop=True)).ok


def test_column_order_is_not_a_finding_by_default(base):
    """
    ``select`` and ``withColumn`` reorder freely.

    Reported, so a caller who cares can ask -- the presented sheet does -- but
    not a failure, because that order is imposed at the end by
    ``src.utils.columns.presented`` regardless of what either engine did.
    """
    shuffled = base[["NOTE", "TXN_SEQ", "STATUS", "AMOUNT"]]

    assert compare(base, shuffled).ok
    assert compare(base, shuffled).order_differs
    assert not compare(base, shuffled, strict_order=True).ok


def test_dtype_is_not_a_finding(base):
    """
    The same number is the same answer as ``float64`` or as text.

    This is the case that occurs constantly during a port: pandas has cast a
    column and Spark has not yet, or the Arrow round trip has widened one.
    """
    other = base.copy()
    other["AMOUNT"] = other["AMOUNT"].astype(object).map(
        lambda v: v if pd.isna(v) else str(v)
    )

    assert compare(base, other).ok


def test_categorical_storage_is_not_a_finding(base):
    """
    ``BALANCE_STATUS`` is a Categorical in pandas and has no Spark equivalent.

    The categories are storage; the labels are the answer.
    """
    other = base.copy()
    other["STATUS"] = pd.Categorical(other["STATUS"])

    assert compare(base, other).ok


def test_float_noise_within_tolerance_is_not_a_finding(base):
    """
    Spark sums in partition order and pandas in row order.

    The low bits of a running total differ by construction, which is a fact
    about float addition rather than about the pipeline.
    """
    other = base.copy()
    other.loc[0, "AMOUNT"] = 10.0 + 1e-12

    assert compare(base, other).ok


def test_tolerance_is_per_column(base):
    """
    Loosening one comparison must not loosen the others.

    Stated per column so that the reason -- "this column accumulates" -- stays
    attached to the column it is a reason about.
    """
    other = base.copy()
    other.loc[0, "AMOUNT"] = 10.005

    assert compare(base, other, tolerances={"AMOUNT": 0.01}).ok
    assert not compare(base, other, tolerances={"NOTE": 0.01}).ok


def test_timestamps_compare_as_instants():
    """
    A parsed timestamp and its text are the same answer.

    Comparing them as text would report every row as different and say nothing
    about which ones actually parsed differently -- which is the only question
    a timestamp parity check is asking.
    """
    left = pd.DataFrame(
        {
            "TXN_SEQ": ["1", "2"],
            "TS": pd.to_datetime(["2022-01-01 07:11:25", "2022-02-01 00:00:00"]),
        }
    )
    right = left.copy()
    right["TS"] = ["2022-01-01 07:11:25", "2022-02-01 00:00:00"]

    assert compare(left, right).ok

    right.loc[1, "TS"] = "2022-02-02 00:00:00"
    assert not compare(left, right).ok


def test_booleans_compare_across_spellings():
    """
    ``IS_HOLIDAY_MONTH`` arrives as the text ``"False"`` and leaves as a bool.

    Two nulls agree; a null and a False do not, because "not stated" and
    "stated as no" are different answers in a column whose whole job is to
    say which months were holidays.
    """
    left = pd.DataFrame({"TXN_SEQ": ["1", "2", "3"], "FLAG": [True, False, None]})
    right = pd.DataFrame(
        {"TXN_SEQ": ["1", "2", "3"], "FLAG": ["True", "False", None]}
    )

    assert compare(left, right).ok

    right.loc[2, "FLAG"] = "False"
    assert not compare(left, right).ok


# --- alignment ------------------------------------------------------------


def test_a_repeated_key_is_refused(base):
    """
    Not a cross product, and not a confusing pandas traceback.

    A non-unique key does not fail where it is chosen, it fails inside the
    join -- so it is checked where the cause can still be named.
    """
    doubled = pd.concat([base, base.iloc[:1]], ignore_index=True)

    # Named explicitly, which is the path the check exists for. Left to choose
    # for itself the harness skips the repeated column and runs out of
    # candidates, which is the other message below.
    with pytest.raises(ValueError, match="repeats"):
        compare(doubled, doubled, key="TXN_SEQ")

    with pytest.raises(ValueError, match="present but not unique"):
        compare(doubled, doubled)


def test_no_usable_key_says_what_it_tried():
    """
    "Absent" and "present but not unique" have different fixes.

    A message that only said "no usable key" would leave the caller to work
    out which -- and the answer is in the frame the harness is already
    holding.
    """
    frame = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    with pytest.raises(ValueError, match="TXN_SEQ: not in both frames"):
        compare(frame, frame)


def test_the_comparison_can_be_narrowed(base):
    """
    A per-stage check asserts on the columns that stage produces.

    Without it, the first stage's test would fail on every column the last
    stage has not written yet, and the harness would be unusable until the
    port was finished -- which is the opposite of what it is for.
    """
    other = base.drop(columns=["NOTE"])

    assert compare(base, other, columns=["TXN_SEQ", "STATUS", "AMOUNT"]).ok
    assert not compare(base, other).ok
