"""
The point-in-time layer: the only place a lag or a rolling window is taken.

Every ``prev_*``, ``roll*_*`` and ``delta_*`` column in the feature table is
produced here, from monthly facts that describe month M, shifted so that a row
for M sees only months before it.

On Spark that is a window frame rather than a positional shift, which makes
the rule easier to read off the code: ``rowsBetween(-3, -1)`` says in the
expression itself that month M is outside the window.
"""

from dataclasses import dataclass

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config.errors import ConfigError
from features.settings import WindowSettings

# The grain a window is taken within. A frame without a partition would walk
# off the end of one user and into the start of the next.
GROUP = "user_id"

# The column a window is ordered along.
ORDER = "month"

MEAN, STD = "mean", "std"


@dataclass(frozen=True)
class Lagged:
    """
    One monthly fact and how far back the feature table reads it.

    :param fact: The unprefixed column on the monthly facts frame.
    :param lags: How many months back to emit, one column each.
    :param rolling: Whether to emit a rolling mean over the window.
    :param rolling_std: Whether to emit a rolling standard deviation too.
    :param delta: Whether to emit the change between lag 1 and lag 2.
    """

    fact: str
    lags: tuple[int, ...] = (1,)
    rolling: bool = False
    rolling_std: bool = False
    delta: bool = False


def lag_name(fact: str, months: int) -> str:
    """
    :param fact: The monthly fact.
    :param months: How many months back.
    :returns: The feature column name.
    """
    return f"prev_{months}m_{fact}"


def rolling_name(fact: str, window: int, stat: str) -> str:
    """
    :param fact: The monthly fact.
    :param window: Months in the window.
    :param stat: ``mean`` or ``std``.
    :returns: The feature column name.
    """
    return f"roll{window}_{stat}_{fact}"


def delta_name(fact: str) -> str:
    """
    :param fact: The monthly fact.
    :returns: The name of its lag-1-minus-lag-2 column.
    """
    return f"delta_prev_1m_2m_{fact}"


def _ordering():
    """:returns: The window every expression here is taken over."""
    return Window.partitionBy(GROUP).orderBy(ORDER)


def verify_grain(facts) -> None:
    """
    Checks that no user-month appears twice before any window is taken.

    Costs one small job, and buys the assumption every frame below rests on.
    A repeated month would make ``lag(1)`` read a duplicate of the current
    month rather than the previous one -- a leak that produces plausible
    numbers and no error.

    :param facts: Monthly facts on the dense user spine.
    :raises ConfigError: If a user-month appears more than once.
    """
    repeated = (
        facts.groupBy(GROUP, ORDER)
        .agg(F.count("*").alias("rows"))
        .filter(F.col("rows") > 1)
        .limit(5)
        .collect()
    )
    if repeated:
        offenders = ", ".join(
            f"{row[GROUP]}@{row[ORDER]}" for row in repeated
        )
        raise ConfigError(
            f"duplicate (user_id, month) row(s) on the monthly facts, e.g. "
            f"{offenders}. Lags are taken over an ordered window, so a "
            f"repeated month would shift by the wrong distance."
        )


def shift(fact: str, months: int):
    """
    The value of a monthly fact ``months`` months earlier, within the user.

    This is the whole point-in-time rule in one operation. A row for month M
    reads the row ``months`` positions earlier on a dense spine, so the value
    it gets belongs to a month strictly before M.

    :param fact: The column to read.
    :param months: How many months back. Must be at least one.
    :returns: A column carrying the shifted values.
    :raises ConfigError: If asked for a lag of zero or less, which would hand
        month M its own value.
    """
    if months < 1:
        raise ConfigError(
            f"a lag must be at least 1 month, got {months}. A lag of 0 is "
            f"the row's own month and is the leak this layer exists to "
            f"prevent."
        )
    return F.lag(F.col(fact), months).over(_ordering())


def rolling(fact: str, window: WindowSettings, stat: str):
    """
    A rolling statistic over the months before the row's own.

    The frame ends at ``-1``, not at the current row. Including month M in its
    own window is a leak of exactly one month, which is invisible in the
    output and fatal in Stage 4.

    ``min_periods`` is a count over the same frame rather than a separate
    setting Spark understands: a mean over three months is published only
    where three of them carried a value, so it is never a mean over however
    many happened to be there.

    :param fact: The column to read.
    :param window: How many months, and how many are required.
    :param stat: ``mean`` or ``std``. The standard deviation is the sample
        one, so three identical months give zero and two months give null.
    :returns: A column carrying the statistic.
    :raises ConfigError: If asked for a statistic this does not compute.
    """
    if stat not in (MEAN, STD):
        raise ConfigError(f"unsupported rolling statistic: {stat!r}")

    frame = _ordering().rowsBetween(-window.rolling_months, -1)
    column = F.col(fact)
    statistic = (
        F.avg(column) if stat == MEAN else F.stddev_samp(column)
    ).over(frame)

    return F.when(
        F.count(column).over(frame) >= F.lit(window.min_periods), statistic
    )


def build(facts, plan: tuple[Lagged, ...], window: WindowSettings):
    """
    Applies the whole plan, producing every point-in-time column at once.

    Every expression shares one window specification, so Spark sorts each
    user's months once and evaluates the lot in a single window stage rather
    than one per column.

    :param facts: Monthly facts on the dense user spine.
    :param plan: What to lag and how far.
    :param window: Rolling window shape.
    :returns: The keys plus one column per requested lag, rolling statistic
        and delta. Nothing describing month M survives into this frame.
    :raises ConfigError: If the plan names a fact the frame does not carry.
    """
    present = set(facts.columns)
    missing = [item.fact for item in plan if item.fact not in present]
    if missing:
        raise ConfigError(
            f"the lag plan names {len(missing)} fact(s) the monthly facts do "
            f"not carry: {', '.join(missing)}"
        )

    built = [F.col(GROUP), F.col(ORDER)]

    for item in plan:
        for months in item.lags:
            built.append(
                shift(item.fact, months).alias(lag_name(item.fact, months))
            )

        if item.rolling:
            built.append(
                rolling(item.fact, window, MEAN).alias(
                    rolling_name(item.fact, window.rolling_months, MEAN)
                )
            )

        if item.rolling_std:
            built.append(
                rolling(item.fact, window, STD).alias(
                    rolling_name(item.fact, window.rolling_months, STD)
                )
            )

        if item.delta:
            # Both terms are already lagged, so the difference is too. Stated
            # as a subtraction of two shifted columns rather than a diff on
            # the raw fact, which would reach into month M.
            built.append(
                (shift(item.fact, 1) - shift(item.fact, 2)).alias(
                    delta_name(item.fact)
                )
            )

    return facts.select(*built)
