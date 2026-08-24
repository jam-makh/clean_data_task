"""
The safety net: does the Spark pipeline give the same answer as the pandas one?

Porting 4,300 lines of cleaning logic is only survivable if "it still works"
is a thing a machine checks after every stage rather than a thing a person
believes. That is all this module is -- a comparison strict enough to catch a
real divergence and forgiving enough not to fire on the ways the two engines
legitimately differ in *representation*.

The distinction between those two is the entire design problem, and it is
settled here once so that no individual stage's test has to settle it again:

* **Row order is representation.** Spark's output order is a function of
  partitioning; pandas' is insertion order. Both frames are aligned on a key
  before anything is compared, and a mismatch in ordering is not a finding.
* **Column order is representation, until the last step.** Spark's ``select``
  and ``withColumn`` reorder freely. Order is reported but does not fail a
  comparison unless the caller asks, because the one place it matters --
  the presented sheet -- is imposed by ``src.utils.columns.presented`` at the
  end regardless of what either engine did.
* **dtype is representation.** A column of parsed money is the same answer
  whether it arrives as ``float64`` or ``double``, and a status column is the
  same answer as a ``Categorical`` or as ``object``. Values are compared
  after both sides are coerced to a common kind, chosen from the pair.
* **Null-vs-empty-string is NOT representation.** It is the distinction this
  pipeline exists to preserve, so nulls compare equal only to nulls, and an
  empty string is a value like any other.
* **Float equality is not representation either, but exact equality is a
  lie.** Spark sums in partition order and pandas sums in row order, so the
  low bits of a running total differ by construction. Numeric comparison is
  therefore within a tolerance the caller states -- small by default, and
  meant to be raised deliberately for a column where the difference is
  understood, never quietly.

The result is a report rather than a boolean. "Not equal" is not actionable;
"BALANCE_STATUS differs on 41 rows, here are five of them with their keys" is.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# Tried in order; the first one present in both frames and unique in both is
# the key rows are aligned on. TXN_SEQ leads because it is the source's own
# global ordering and is present from the raw read onward, so the same key
# works before any stage has run and after all of them have. TXN_ID_CLEANED
# is next because it is what the Phase 06 upsert keys on -- comparing on it
# is the same alignment the database will do.
KEY_PREFERENCE = ("TXN_SEQ", "TXN_ID_CLEANED", "TXN_ID")

# Small enough that a real arithmetic disagreement cannot hide under it, large
# enough to absorb the last bits of a float sum accumulated in a different
# order. Money is rounded to the cent by the cleaners themselves, so anything
# this tolerance forgives is representation, not a different answer.
TOLERANCE = 1e-9

# What a null becomes when a column is compared as text. A private sentinel
# rather than "" or "None", because both of those are values this source
# actually contains, and a null that compares equal to the string "None" would
# make the harness agree with a bug.
NULL = "\x00NULL\x00"

# How many disagreeing rows a difference carries as evidence. Enough to see a
# pattern -- all in one account, all on the same timestamp format -- without
# printing a column.
EXAMPLES = 5


@dataclass(frozen=True)
class ColumnDifference:
    """
    One column that does not match, and the evidence.

    :param column: The column name.
    :param mismatches: How many aligned rows disagree.
    :param kind: How the two sides were compared -- ``numeric``,
        ``datetime``, ``boolean`` or ``text``. Named because a text
        comparison of a column that should have been numeric is itself a
        finding: it means one side never cast.
    :param examples: Up to ``EXAMPLES`` of ``(key, left value, right value)``.
    """

    column: str
    mismatches: int
    kind: str
    examples: tuple[tuple[object, object, object], ...] = ()

    def __str__(self) -> str:
        head = f"{self.column}: {self.mismatches} row(s) differ ({self.kind})"
        rows = "\n".join(
            f"      key={key!r}  pandas={left!r}  spark={right!r}"
            for key, left, right in self.examples
        )
        return f"{head}\n{rows}" if rows else head


@dataclass(frozen=True)
class ParityResult:
    """
    The verdict on one comparison, in enough detail to act on.

    :param key: The column(s) rows were aligned on.
    :param left_rows: Row count of the reference (pandas) frame.
    :param right_rows: Row count of the frame under test (Spark).
    :param only_left: Columns the reference has and the other does not --
        i.e. what the port has not produced yet.
    :param only_right: Columns the port invented.
    :param order_differs: Whether the shared columns are in a different order.
        Informational by default; see the module docstring.
    :param missing_keys: Keys present in the reference and absent from the
        port, up to ``EXAMPLES``, with the total.
    :param extra_keys: The same the other way round.
    :param differences: One entry per disagreeing column.
    """

    key: tuple[str, ...]
    left_rows: int
    right_rows: int
    only_left: tuple[str, ...] = ()
    only_right: tuple[str, ...] = ()
    order_differs: bool = False
    missing_keys: tuple[int, tuple] = (0, ())
    extra_keys: tuple[int, tuple] = (0, ())
    differences: tuple[ColumnDifference, ...] = ()
    strict_order: bool = False
    compared: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        """
        :returns: Whether the two frames say the same thing.

        Column *presence* counts and column *order* does not, unless the
        caller asked for order. A missing column is a stage that has not run;
        a reordered one is a ``select`` that chose differently.
        """
        return not (
            self.only_left
            or self.only_right
            or self.left_rows != self.right_rows
            or self.missing_keys[0]
            or self.extra_keys[0]
            or self.differences
            or (self.strict_order and self.order_differs)
        )

    def __str__(self) -> str:
        if self.ok:
            return (
                f"parity OK: {self.left_rows} rows x "
                f"{len(self.compared)} column(s), aligned on "
                f"{'+'.join(self.key)}"
            )

        lines = [
            f"parity FAILED, aligned on {'+'.join(self.key)}",
            f"  rows: pandas={self.left_rows} spark={self.right_rows}",
        ]
        if self.only_left:
            lines.append(f"  missing from spark: {list(self.only_left)}")
        if self.only_right:
            lines.append(f"  only in spark: {list(self.only_right)}")
        if self.missing_keys[0]:
            lines.append(
                f"  {self.missing_keys[0]} key(s) missing from spark, e.g. "
                f"{list(self.missing_keys[1])}"
            )
        if self.extra_keys[0]:
            lines.append(
                f"  {self.extra_keys[0]} key(s) only in spark, e.g. "
                f"{list(self.extra_keys[1])}"
            )
        if self.strict_order and self.order_differs:
            lines.append("  shared columns are in a different order")
        for difference in self.differences:
            lines.append(f"    {difference}")
        return "\n".join(lines)


def to_pandas(frame) -> pd.DataFrame:
    """
    Brings either kind of frame to the driver as pandas.

    :param frame: A ``pyspark.sql.DataFrame`` or a ``pandas.DataFrame``.
    :returns: A pandas frame. Already-pandas input is returned as-is, so a
        caller comparing two pandas frames -- which is what every parity test
        does before its stage is ported -- pays nothing.
    """
    if isinstance(frame, pd.DataFrame):
        return frame
    if not hasattr(frame, "toPandas"):
        raise TypeError(
            f"expected a pandas or Spark DataFrame, got {type(frame).__name__}"
        )
    try:
        return frame.toPandas()
    except Exception:
        # The Arrow path is version-sensitive at the pandas/pyspark boundary
        # -- this project runs a pandas newer than the pyspark build formally
        # supports, and that pairing warns on import. Falling back to the row
        # path keeps the harness usable when it breaks: slower, and no
        # comparison it performs is any weaker for it.
        columns = list(frame.columns)
        rows = [tuple(row) for row in frame.collect()]
        return pd.DataFrame(rows, columns=columns)


def _is_numeric(series: pd.Series) -> bool:
    """:returns: Whether the column already holds numbers (bools excluded)."""
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(
        series
    )


def _kind(left: pd.Series, right: pd.Series) -> str:
    """
    Decides how one column is compared, from the pair of dtypes.

    Chosen from the pair rather than from either side alone, because the
    interesting case during a port is precisely when the two disagree: pandas
    has parsed a column to datetime and Spark still has the string, or the
    other way round. Comparing those as text would report every row as
    different and say nothing about why; comparing them as datetimes reports
    the rows that actually parsed differently, which is the real question.

    :returns: ``datetime``, ``boolean``, ``numeric`` or ``text``.
    """
    for test, name in (
        (pd.api.types.is_datetime64_any_dtype, "datetime"),
        (pd.api.types.is_bool_dtype, "boolean"),
        (_is_numeric, "numeric"),
    ):
        if test(left) or test(right):
            return name
    return "text"


def _as_text(series: pd.Series) -> pd.Series:
    """
    :returns: The column as strings, with nulls mapped to a private sentinel
        so they compare equal to each other and to nothing else. A Categorical
        loses its categories here, which is intended: the categories are a
        pandas storage detail and Spark has no equivalent, while the labels
        are the answer.
    """
    missing = series.isna()
    return series.astype(object).where(~missing, NULL).map(
        lambda value: value if value is NULL else str(value)
    )


def _as_boolean(series: pd.Series) -> pd.Series:
    """
    :returns: The column as nullable booleans.

    The strings are handled because this is exactly the boundary where one
    side has cast and the other has not: a source column reading ``"False"``
    is the same answer as a parsed ``False``, and a harness that called those
    different would fire on every row of IS_HOLIDAY_MONTH.
    """
    truth = {
        "true": True, "false": False, "t": True, "f": False,
        "1": True, "0": False, "yes": True, "no": False,
    }

    def convert(value):
        if value is None or value is pd.NaT or (
            isinstance(value, float) and np.isnan(value)
        ):
            return pd.NA
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        return truth.get(str(value).strip().lower(), pd.NA)

    return series.map(convert).astype("boolean")


def _mismatch(left: pd.Series, right: pd.Series, kind: str, tolerance: float):
    """
    :param kind: From ``_kind``.
    :param tolerance: Absolute tolerance for the numeric comparison.
    :returns: A boolean mask, True where the two disagree. Two nulls agree in
        every kind; a null and a value never do.
    """
    if kind == "numeric":
        a = pd.to_numeric(left, errors="coerce")
        b = pd.to_numeric(right, errors="coerce")
        both_null = a.isna() & b.isna()
        close = pd.Series(
            np.isclose(
                a.to_numpy(dtype="float64", na_value=np.nan),
                b.to_numpy(dtype="float64", na_value=np.nan),
                atol=tolerance,
                rtol=0.0,
                equal_nan=False,
            ),
            index=a.index,
        )
        return ~(both_null | close)

    if kind == "datetime":
        # utc=True then tz-drop: one side may carry an offset the other does
        # not, and the instant is the answer either way. errors="coerce"
        # rather than raise, so a string that never parsed shows up as a
        # difference on its own row instead of killing the comparison.
        a = pd.to_datetime(left, errors="coerce", utc=True).dt.tz_localize(None)
        b = pd.to_datetime(right, errors="coerce", utc=True).dt.tz_localize(None)
        return ~((a.isna() & b.isna()) | (a == b))

    if kind == "boolean":
        a, b = _as_boolean(left), _as_boolean(right)
        # `a == b` on a nullable boolean yields NA wherever either side is
        # null, so it has to be filled before it can be used as a mask -- and
        # filled with False, because "unknown" is not agreement. The two-nulls
        # case is granted separately, above the fill.
        return ~((a.isna() & b.isna()) | (a == b).fillna(False))

    a, b = _as_text(left), _as_text(right)
    return a != b


def _choose_key(left: pd.DataFrame, right: pd.DataFrame) -> tuple[str, ...]:
    """
    :returns: The first preferred column present and unique in both frames.
    :raises ValueError: If none qualifies, naming what was tried and why each
        was rejected -- "no usable key" on its own leaves the caller guessing
        between "absent" and "not unique", which have different fixes.
    """
    reasons = []
    for name in KEY_PREFERENCE:
        if name not in left.columns or name not in right.columns:
            reasons.append(f"{name}: not in both frames")
            continue
        if left[name].duplicated().any() or right[name].duplicated().any():
            reasons.append(f"{name}: present but not unique")
            continue
        return (name,)
    raise ValueError(
        "no column to align rows on. Tried "
        + "; ".join(reasons)
        + ". Pass key= explicitly, or -- if the frame legitimately has no "
        "unique column -- a tuple of columns that is unique together."
    )


def compare(
    left,
    right,
    *,
    key: str | tuple[str, ...] | None = None,
    columns=None,
    tolerance: float = TOLERANCE,
    tolerances: dict[str, float] | None = None,
    examples: int = EXAMPLES,
    strict_order: bool = False,
) -> ParityResult:
    """
    Compares a pandas result against a Spark one, column for column.

    :param left: The reference -- the pandas pipeline's output.
    :param right: The frame under test -- the Spark pipeline's output. May
        itself be a pandas frame, which is what makes the harness usable
        before anything is ported.
    :param key: Column(s) to align rows on; chosen from ``KEY_PREFERENCE``
        when absent.
    :param columns: Restrict the comparison to these, for a per-stage check
        that asserts on the columns that stage produces and stays silent
        about the ones a later stage will.
    :param tolerance: Absolute tolerance for every numeric column.
    :param tolerances: Per-column overrides, for a column whose difference is
        understood and bounded. Stated per column rather than by raising the
        global tolerance, so loosening one comparison cannot quietly loosen
        the others.
    :param examples: Disagreeing rows carried as evidence per column.
    :param strict_order: Whether a different column order fails the check.
    :returns: The verdict.
    """
    left = to_pandas(left)
    right = to_pandas(right)
    key = (key,) if isinstance(key, str) else key
    key = tuple(key) if key else _choose_key(left, right)

    missing_key = [k for k in key if k not in left.columns or k not in right.columns]
    if missing_key:
        raise ValueError(f"key column(s) {missing_key} absent from one side")

    # Checked even when the caller named the key, because a non-unique key
    # does not fail here -- it fails inside the alignment below, as a cross
    # product, and the traceback that produces describes pandas indexing
    # rather than the frame that caused it.
    for name, frame in (("pandas", left), ("spark", right)):
        if frame.duplicated(subset=list(key)).any():
            repeats = int(frame.duplicated(subset=list(key)).sum())
            raise ValueError(
                f"key {'+'.join(key)} repeats on {repeats} row(s) of the "
                f"{name} frame, so rows cannot be paired by it"
            )

    shared = [c for c in left.columns if c in set(right.columns)]
    only_left = tuple(c for c in left.columns if c not in set(right.columns))
    only_right = tuple(c for c in right.columns if c not in set(left.columns))
    order_differs = shared != [c for c in right.columns if c in set(left.columns)]

    if columns is not None:
        wanted = set(columns)
        unknown = sorted(wanted - set(shared) - set(only_left) - set(only_right))
        if unknown:
            raise ValueError(
                f"asked to compare column(s) {unknown}, which neither frame has"
            )
        shared = [c for c in shared if c in wanted]
        only_left = tuple(c for c in only_left if c in wanted)
        only_right = tuple(c for c in only_right if c in wanted)

    # Aligned on the key, not on position. Sorting both by it would be enough
    # for equal-length frames and would silently mis-pair every row after the
    # first missing one otherwise; an index join reports the absent keys as
    # absent, which is the finding.
    left_indexed = left.set_index(list(key)).sort_index()
    right_indexed = right.set_index(list(key)).sort_index()

    left_keys = left_indexed.index
    right_keys = right_indexed.index
    missing = left_keys.difference(right_keys)
    extra = right_keys.difference(left_keys)
    common = left_keys.intersection(right_keys)

    differences: list[ColumnDifference] = []
    compared = [c for c in shared if c not in key]
    for column in compared:
        a = left_indexed.loc[common, column]
        b = right_indexed.loc[common, column]
        kind = _kind(a, b)
        atol = (tolerances or {}).get(column, tolerance)
        mask = _mismatch(a, b, kind, atol)
        count = int(mask.sum())
        if not count:
            continue
        sample = mask[mask].index[:examples]
        differences.append(
            ColumnDifference(
                column=column,
                mismatches=count,
                kind=kind,
                examples=tuple(
                    (index, a.loc[index], b.loc[index]) for index in sample
                ),
            )
        )

    return ParityResult(
        key=key,
        left_rows=len(left),
        right_rows=len(right),
        only_left=only_left,
        only_right=only_right,
        order_differs=order_differs,
        missing_keys=(len(missing), tuple(missing[:examples])),
        extra_keys=(len(extra), tuple(extra[:examples])),
        differences=tuple(differences),
        strict_order=strict_order,
        compared=tuple(compared),
    )


def assert_parity(left, right, **kwargs) -> ParityResult:
    """
    :param kwargs: As ``compare``.
    :returns: The result, so a passing test can still report what it checked.
    :raises AssertionError: With the full report as its message. The report is
        the point -- an assertion that only says "frames differ" makes the
        person who wrote the stage go and find out what this function already
        knows.
    """
    result = compare(left, right, **kwargs)
    if not result.ok:
        raise AssertionError(str(result))
    return result
