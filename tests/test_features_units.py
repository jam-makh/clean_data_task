"""
The unit decision, and the structural guards that keep it true.

Every monetary column is USD; the native-currency columns are never read; the
window layer is the only place a lag is taken; nothing reaches forwards in
time; and the whole build is PySpark, which the engine guard below is what
actually enforces.
"""

import ast
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.features import source
from tests.harness import features

# Where the build lives. Parsed rather than imported, because what is under
# test is that a module does not contain an operation at all.
PACKAGE = Path("src/features")

# The one module allowed to lag. Every other point-in-time column in the table
# is produced by calling into it.
WINDOWS = PACKAGE / "windows.py"

# The Spark functions that read another row's value along the ordering. These
# are what ``shift`` was in the pandas build.
LAG_FUNCTIONS = frozenset({"lag", "lead"})

# The window bound that reaches into the future. The build holds values
# forward in three places -- a balance persists through a quiet month, a
# staleness counter points back at the last active one, and the account count
# accumulates -- and forward is safe because it can only reach the past. This
# bound is the same operation pointed the other way, and no column here has
# any business using it.
FORWARD_BOUNDS = frozenset({"unboundedFollowing"})

# The engines this package must not use. Not a style preference: a single
# ``toPandas`` in the middle of the build would pull the whole dataset into
# the driver and quietly undo the reason Stage 3 runs on Spark at all.
BANNED_MODULES = frozenset({"pandas", "numpy"})

BANNED_CALLS = frozenset({"toPandas", "toLocalIterator"})


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """
    :param tree: A parsed module.
    :returns: The ids of every string node that is a docstring. Prose
        explaining why a column is *not* read is not a read of it, so
        these are excluded from the scans below.
    """
    found = set()
    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            continue
        body = getattr(node, "body", [])
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            found.add(id(body[0].value))
    return found


def _code_strings(path: Path) -> set[str]:
    """
    :param path: A module to read.
    :returns: Every string literal it uses as code. Comments never reach the
        tree at all, and docstrings are dropped.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = _docstring_nodes(tree)
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    }


def _attribute_calls(path: Path) -> set[str]:
    """
    :param path: A module to read.
    :returns: Every attribute name it calls, e.g. ``lag`` in ``F.lag(c, 1)``.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }


def _attribute_names(path: Path) -> set[str]:
    """
    :param path: A module to read.
    :returns: Every attribute it reads, called or not -- window bounds like
        ``Window.unboundedFollowing`` are read, never called.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }


def _imported_roots(path: Path) -> set[str]:
    """
    :param path: A module to read.
    :returns: The top-level package of everything it imports.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_the_source_reader_selects_only_usd_columns():
    """
    ``billing_amount`` is USD on every row; ``txn_amount_cleaned`` is in
    ``txn_ccy`` and summing it across accounts would add LBP to USD.
    """
    assert "billing_amount" in source.COLUMNS
    assert "running_balance_normalized" in source.COLUMNS

    for forbidden in source.FORBIDDEN:
        assert forbidden not in source.COLUMNS


def test_a_second_denomination_stops_the_build(spark):
    """
    Not a warning. A feature table that mixes units is not wrong in an
    obvious way, which is exactly why this refuses rather than notes it.
    """
    frame = features.simple(spark)
    mixed = frame.withColumn(
        "billing_currency",
        F.when(F.col("txn_seq") == 1, F.lit("EUR")).otherwise(
            F.col("billing_currency")
        ),
    )

    with pytest.raises(Exception, match="not all USD"):
        source.validate(mixed)


def test_a_missing_column_names_every_one_that_is_missing(spark):
    """
    A validator that fails on the first missing column makes you run the
    pipeline again to find the second.
    """
    frame = features.simple(spark).drop(
        "running_balance_normalized", "mcc_code_cleaned"
    )
    with pytest.raises(Exception) as failure:
        source.validate(frame)

    message = str(failure.value)
    assert "running_balance_normalized" in message
    assert "mcc_code_cleaned" in message


def test_the_whole_build_is_pyspark():
    """
    The engine guard.

    Stage 3 computes on Spark DataFrames from the read to the upsert. A
    pandas import anywhere in this package is either a conversion of the
    dataset -- which defeats the point -- or the first step towards one, and
    either way it should fail here rather than be noticed in a review six
    months from now.
    """
    offenders = {}
    for path in sorted(PACKAGE.glob("*.py")):
        found = sorted(_imported_roots(path) & BANNED_MODULES)
        if found:
            offenders[path.name] = found

    assert offenders == {}, (
        f"non-Spark engines imported by the feature build: {offenders}. "
        f"Stage 3 computes on PySpark DataFrames; keep pandas out of it."
    )

    # And the package really is Spark, rather than being empty of both.
    engines = set()
    for path in sorted(PACKAGE.glob("*.py")):
        engines |= _imported_roots(path)
    assert "pyspark" in engines


def test_nothing_in_the_build_collects_the_dataset():
    """
    The other half of the same rule. ``toPandas`` on a built frame moves every
    row into the driver, which turns a distributed build into a single-process
    one with extra steps.

    ``collect`` is not banned outright: the build uses it for a handful of
    validation rows and for the report's own numbers, which are aggregates and
    not the dataset.
    """
    offenders = {}
    for path in sorted(PACKAGE.glob("*.py")):
        found = sorted(_attribute_calls(path) & BANNED_CALLS)
        if found:
            offenders[path.name] = found

    assert offenders == {}, f"dataset collected into the driver: {offenders}"


def test_no_module_but_windows_takes_a_lag():
    """
    The point-in-time rule is architectural, not a habit. If another module
    could lag, the rule would hold only as long as everyone remembered it.
    """
    offenders = {}
    for path in sorted(PACKAGE.glob("*.py")):
        if path == WINDOWS:
            continue
        found = sorted(_attribute_calls(path) & LAG_FUNCTIONS)
        if found:
            offenders[path.name] = found

    assert offenders == {}, (
        f"lag functions outside windows.py: {offenders}. Route them through "
        f"src/features/windows.py so the shift is taken in one place."
    )

    # And the guard is load-bearing: windows.py really does take the lags.
    assert _attribute_calls(WINDOWS) & LAG_FUNCTIONS


def test_no_window_in_the_build_reaches_forwards():
    """
    A frame that ends at ``unboundedFollowing`` reads months after the row's
    own. Every window here ends at the current row or earlier, and that is
    what makes holding a value forward safe -- forward fill reaches the past;
    the same operation pointed the other way reaches the future.
    """
    offenders = {}
    for path in sorted(PACKAGE.glob("*.py")):
        found = sorted(_attribute_names(path) & FORWARD_BOUNDS)
        if found:
            offenders[path.name] = found

    assert offenders == {}, f"forward-reaching windows in the build: {offenders}"


def test_the_forbidden_columns_appear_nowhere_in_the_build():
    """
    The units decision holds only if the native-currency columns are never
    read. Stated as a scan so it is checkable rather than remembered.
    """
    offenders = {}
    for path in sorted(PACKAGE.glob("*.py")):
        # source.py names them in order to ban them, and report.py copies that
        # list into the manifest. Those are the two places they may appear.
        if path.name in ("source.py", "report.py"):
            continue
        found = sorted(_code_strings(path) & set(source.FORBIDDEN))
        if found:
            offenders[path.name] = found

    assert offenders == {}, (
        f"native-currency columns read outside source.py: {offenders}"
    )


def test_the_month_is_the_first_day_of_the_month(spark):
    """
    Every join in the build is on this column, so a date that kept its day
    would silently produce no matches.
    """
    frame = features.simple(spark)
    days = {
        row["day"]
        for row in frame.select(
            F.dayofmonth("month").alias("day")
        ).distinct().collect()
    }
    assert days == {1}


def test_a_transaction_with_no_timestamp_carries_no_month(spark):
    """
    An unparseable ``txn_ts`` must not be assigned a month. Dropping it from
    the timeline is the honest outcome; guessing one would put a transaction
    in a month the source never claimed.
    """
    rows = [
        features.transaction("u1", "u1a", "2022-01", "salary", 100, 100, 1),
    ]
    row = dict(rows[0])
    row["txn_ts"] = None

    frame = features.frame(spark, [row])
    months = [record["month"] for record in frame.select("month").collect()]

    assert months == [None]
