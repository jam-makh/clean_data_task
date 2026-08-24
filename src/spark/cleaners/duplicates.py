"""
Exact-row deduplication and transaction-ID collision suffixing.

The pandas original computes group sizes by factorizing every column and
running one pass over the code matrix, then calls ``drop_duplicates``. Its own
docstring predicts what that becomes here, and it is right: grouping by every
column both removes the duplicates and counts them, in one shuffle. The two
operations are the same equivalence, so doing them separately would shuffle
twice to learn the same thing.

The two cases this stage handles are different and only one of them drops a
row. A byte-identical row is a double-load and the copy is discarded, with the
survivor carrying how many it stands for -- the only place a dropped row can
still be counted, since the rows themselves are gone by the end of the run.
Two *different* rows sharing a TXN_ID is an upstream key fault and may well be
two real transactions, so neither is dropped; they are disambiguated in the ID
itself.

One known divergence, unexercised by either source in hand. pandas orders a
collision group with a stable sort, so rows that tie on the ordering column
keep their original file order -- and for the ``forecast_balance`` profile
there IS no ordering column present (``TXN_DATE_TIME_CLEANED`` is the v4
name; this profile's timestamp stage produces ``TXN_TS``), which makes the
suffix order pure file order. Spark has no file order to appeal to after a
shuffle, so ties are broken here on the remaining columns instead, which is
deterministic but need not agree with pandas' ordering. It cannot fire on this
data: the forecast extract has zero exact duplicates and zero TXN_ID
collisions across all 265,195 rows. Closing it properly means giving the
pandas side the same explicit tiebreak, which is a change to the reference
implementation and is not made here.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.spark import audit
from src.spark.spark_utils import text

# How many byte-identical source rows this one row stands for. Normally 1.
COPIES = "EXACT_DUPLICATE_COPIES"

# Whether this row's TXN_ID was shared with another row and therefore had to
# be suffixed to make TXN_ID_CLEANED unique.
COLLISION = "TXN_ID_COLLISION"


def apply(
    frame,
    policy,
    id_col: str = "TXN_ID",
    order_col: str = "TXN_DATE_TIME_CLEANED",
):
    """
    Drops byte-identical rows, then suffixes any remaining ``TXN_ID``
    collisions so the cleaned ID is unique on its own.

    :param frame: The frame as the profile's earlier stages left it. Every
        column counts toward "identical", including the ones those stages
        derived -- which is what the pandas original does, since it groups on
        ``df.columns`` at the moment it runs.
    :param policy: Unused. The business-key definitions in
        ``policy.duplicates`` are read by the counting pass, not here: the
        pandas ``metrics`` recomputes them from raw source columns at report
        time rather than marking them, and marking them here would put a
        column on the Spark frame that pandas never produces.
    :param id_col: Column expected to be unique.
    :param order_col: Parsed date used to order collisions, when present.
    :returns: The frame, deduplicated, with the copy count, cleaned ID and
        collision flag added.
    """
    source_columns = list(frame.columns)

    # One shuffle for both jobs. Nulls group together, which is what
    # ``factorize`` giving every NaN the code -1 does on the pandas side, so
    # a row that is identical including in its blanks is still a duplicate.
    frame = frame.groupBy(*source_columns).agg(
        F.count(F.lit(1)).alias(COPIES)
    )

    if id_col not in source_columns:
        return frame

    cleaned = f"{id_col}_CLEANED"
    frame = frame.withColumn(cleaned, text(id_col))

    # Set on every row, not only the colliding ones -- the pandas original
    # assigns the whole boolean series before it checks whether any of it is
    # true, so a source with no collisions still carries an all-False column.
    frame = frame.withColumn(
        COLLISION, F.count(F.lit(1)).over(Window.partitionBy(id_col)) > 1
    )

    # The ordering column first when the profile has one, then everything
    # else. The tail is what makes this deterministic rather than merely
    # arbitrary: exact duplicates are gone by now, so the full row is unique
    # and the order is total. See the module docstring for why it is not
    # necessarily pandas' order.
    ordering = [F.col(order_col)] if order_col in source_columns else []
    ordering += [F.col(column) for column in source_columns]
    sequence = F.row_number().over(
        Window.partitionBy(id_col).orderBy(*ordering)
    )

    # Only the members of a collision group are suffixed -- an ID that was
    # already unique is left exactly as it was, so the common row's cleaned ID
    # is still the ID a person would recognise.
    return frame.withColumn(
        cleaned,
        F.when(
            F.col(COLLISION),
            F.concat(F.col(cleaned), F.lit("_"), sequence.cast("string")),
        ).otherwise(F.col(cleaned)),
    )


def metrics(frame, policy):
    """
    What deduplication removed, and what it deliberately did not.

    :param frame: The frame as the last stage left it.
    :param policy: Read for the business-key definitions, which are a
        judgement about which columns ought to identify a transaction and
        live in ``config/policy.yaml`` with the reasoning that picked them.
    :returns: ``(metric, request)`` pairs in report order.
    """
    out = []
    if COPIES in frame.columns:
        # Every source row is accounted for: each survivor says how many it
        # stands for, so the total minus the surviving rows is what was
        # dropped. Stated as one aggregate rather than as a count taken
        # before and after, because a count taken mid-run is the extra pass
        # this whole contract exists to remove.
        out.append((
            "exact_duplicate_rows_dropped",
            audit.Scalar(
                F.coalesce(F.sum(F.col(COPIES)), F.lit(0))
                - F.count(F.lit(1))
            ),
        ))

    for keys in policy.duplicates.business_keys:
        if set(keys).issubset(frame.columns):
            out.append((
                f"business_key_repeats[{'+'.join(keys)}]",
                audit.shared(keys),
            ))

    if COLLISION in frame.columns:
        out.append(("txn_id_collisions", audit.rows(F.col(COLLISION))))

    return out
