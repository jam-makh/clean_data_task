"""
Reading the run's totals back out of the diagnostic columns.

Every stage marks each row with what it did to it; nothing counts while the
rows are being touched. These helpers are the other half of that contract --
the single pass at the end of the run that turns those marks into the report.

Each one is deliberately a whole-column operation with no Python-level
accumulator, because the same derivation has to survive being executed across
a cluster. A running total kept in a closure is the shape that does not: it
would be filled in on each executor and read back, empty, on the driver.
"""

import re

import pandas as pd


def rows(mask) -> int:
    """
    :param mask: Boolean column, possibly nullable.
    :returns: How many rows it selects. A null is not a hit -- a mask is a
        statement that something is true of a row, and "unknown" is not it.
    """
    return int(pd.Series(mask).fillna(False).astype(bool).sum())


def distinct(values) -> int:
    """
    :param values: Any column.
    :returns: How many different values it holds, nulls excluded.
    """
    return int(pd.Series(values).nunique())


def ranked(values) -> list[tuple[str, int]]:
    """
    Tallies a label column, commonest first.

    Ties break by first appearance rather than arbitrarily. That is stricter
    than ``value_counts()``, which sorts unstably and can therefore order two
    equally common labels differently on two runs over the same rows -- an
    ordering that would show up as a spurious diff between the pandas and
    Spark reports and cost an afternoon to chase.

    :param values: Label column; only labels actually present are returned.
    :returns: ``(label, count)`` pairs, commonest first.
    """
    series = pd.Series(values)
    tally = series.value_counts()
    # dict.fromkeys keeps first-appearance order; sorted is stable, so equal
    # counts stay in it.
    seen = list(dict.fromkeys(series))
    return [
        (label, int(tally[label]))
        for label in sorted(seen, key=lambda label: -tally[label])
    ]


def flag_tally(flags) -> dict[str, int]:
    """
    Counts every code in a semicolon-joined flag column in one pass.

    One pass rather than one per code: the column is split and exploded once,
    so adding a fourteenth validation rule costs nothing.

    :param flags: The ``VALIDATION_FLAGS`` column.
    :returns: Flag code to the number of rows carrying it.
    """
    series = pd.Series(flags).fillna("").astype(str)
    present = series[series.ne("")]
    if present.empty:
        return {}
    counts = present.str.split(";").explode().value_counts()
    return {str(code): int(n) for code, n in counts.items()}


def carries(flags, code: str):
    """
    :param flags: The ``VALIDATION_FLAGS`` column.
    :param code: One flag code.
    :returns: Boolean mask of the rows carrying exactly that code, matched
        between delimiters so ``FX_RATE_OFF`` never matches
        ``FX_RATE_OFF_REFERENCE``.
    """
    pattern = rf"(?:^|;){re.escape(code)}(?:;|$)"
    return pd.Series(flags).fillna("").astype(str).str.contains(
        pattern, regex=True
    )
