"""
Replicating the source to a larger size, for the scaling run deliverable 4
asks for.

Users and accounts are re-keyed per copy, so five times the rows is five times
the users rather than five times the history per user.
"""

from pyspark.sql import functions as F

from src.config.errors import ConfigError

# How a replicated key is spelled. The suffix is the copy number, so the
# original rows stay recognisable and a key cannot collide across copies.
SUFFIX = "#"

COPY = "_scale_copy"


def replicate(frame, factor: int):
    """
    Explodes the source to ``factor`` times its size.

    Re-keying rather than plain duplication is what makes the timing
    meaningful. Duplicating rows under the same user ids would lengthen each
    user's history, which changes the shape of the problem: the group-by would
    see the same number of groups and the lags would run over more months.
    Re-keying holds months per user constant and multiplies the group count,
    which is the axis a feature build actually has to scale along.

    One ``explode`` rather than a union of ``factor`` frames. A union builds a
    plan with ``factor`` branches that the optimiser then has to reason about
    separately; the explode is a single scan whose output is ``factor`` rows
    per input row.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param factor: How many copies. One returns the frame unchanged.
    :returns: The exploded frame.
    :raises ConfigError: If the factor is less than one.
    """
    if factor < 1:
        raise ConfigError(f"scale factor must be at least 1, got {factor}")
    if factor == 1:
        return frame

    copies = frame.withColumn(
        COPY, F.explode(F.sequence(F.lit(0), F.lit(factor - 1)))
    )

    for column in ("user_id", "account_id"):
        copies = copies.withColumn(
            column,
            # Copy zero keeps the original keys, so the replicated run is a
            # strict superset of the real one and the two are comparable.
            F.when(F.col(COPY) == 0, F.col(column)).otherwise(
                F.concat(
                    F.col(column), F.lit(SUFFIX), F.col(COPY).cast("string")
                )
            ),
        )

    return copies.drop(COPY)


def summarise(frame) -> dict:
    """
    The three numbers that describe how big a run was.

    One ``agg``, so the summary costs one job rather than three.

    :param frame: The frame about to be built from.
    :returns: Rows, users and accounts.
    """
    row = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("account_id").alias("accounts"),
    ).first()

    return {
        "rows": int(row["rows"]),
        "users": int(row["users"]),
        "accounts": int(row["accounts"]),
    }
