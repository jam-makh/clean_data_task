"""
Running balance: fill what the arithmetic proves, withhold everything else.

Every operation in the pandas original is a groupwise scan over transactions
in sequence order, so every one of them is a window function here -- one
``Window.partitionBy(ACCOUNT_ID).orderBy(TXN_SEQ)`` and a handful of frames cut
from it. The translation is mechanical with two exceptions worth stating.

``ffill``/``bfill`` within an account become ``last(..., ignorenulls=True)``
over the rows up to here and ``first(..., ignorenulls=True)`` over the rows
from here on. That is also how "the previous *anchor*" is expressed: the offset
column is null on every unstated row, so ignoring nulls over the preceding rows
skips the blanks and lands on the last stated balance -- which is exactly what
``anchors.groupby(keys).shift(1)`` does on the pandas side, where the shift is
taken on the anchors alone so the blanks between them cannot shift what counts
as adjacent.

Rounding is ``bround``, not ``round``. Spark's ``round`` is HALF_UP and
numpy's -- which is what ``Series.round`` calls -- is half-to-even. On a column
of money accumulated by repeated addition the tie case is rare and it is not
never, and a cent that disagrees between the two engines would surface as a
parity failure on a value that is not actually wrong. ``bround`` is the
half-to-even one.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.spark.spark_utils import chain as when_chain

# What is known about each row's balance. See src/cleaners/balance.py for what
# each state means -- the vocabulary is the pandas original's and must stay
# identical, since the two are compared state for state.
STATUSES = ["OBSERVED", "DERIVED", "CONTRADICTED", "UNVERIFIED", "UNKNOWN"]
ADJUSTED_STATUSES = [
    "VERIFIED", "CONFIRMED", "CONTRADICTED", "UNTESTED", "NO_ANCHOR",
]

SOURCE = "RUNNING_BALANCE"
CLEANED = "RUNNING_BALANCE_CLEANED"
STATUS = "RUNNING_BALANCE_STATUS"
ADJUSTED = "RUNNING_BALANCE_ADJUSTED"
ADJUSTED_STATUS = "RUNNING_BALANCE_ADJUSTED_STATUS"
CHAIN_BREAK = "RUNNING_BALANCE_CHAIN_BREAK"

# Scratch columns. Prefixed and dropped at the end, so an intermediate cannot
# be mistaken for output by a later stage or by the writer.
_SCRATCH = "_bal_"


def apply(
    frame,
    policy,
    source_col: str = SOURCE,
    amount_col: str = "TXN_AMOUNT_CLEANED",
    sequence_col: str = "TXN_SEQ",
    group_col: str = "ACCOUNT_ID",
):
    """
    Recovers the running balance where the arithmetic can be verified, and
    refuses to state one anywhere else.

    Two stated balances in the same account, with the transactions between
    them, are a closed system: the later must equal the earlier plus those
    transactions. Expressed per row as ``offset = balance - cumulative
    amount``, that closure becomes an equality -- two stated balances agree
    exactly when they carry the same offset. Every decision here is that one
    comparison.

    :param frame: Frame with the amount column the ``amounts`` stage produced.
    :param policy: Read for ``balance.reconcile_tolerance`` only.
    :param source_col: The stated balance, as the source spells it.
    :param amount_col: What moves the balance. ``TXN_AMOUNT_CLEANED``, in the
        account's own currency -- never ``BILLING_AMOUNT``, which is stated in
        USD on every row of this source and would be a unit error, and which
        has to stay untouched because it is what ``consistency`` reconciles
        against.
    :param sequence_col: The source's own global ordering.
    :param group_col: The account a balance belongs to.
    :returns: The frame with the two balance columns, their two status columns
        and the chain-break flag added. Unchanged if any input column is
        absent, which is how the pandas original behaves on a profile that
        carries no balance at all.
    """
    needed = {source_col, amount_col, sequence_col, group_col}
    if not needed.issubset(frame.columns):
        return frame

    tolerance = policy.balance.reconcile_tolerance

    # TXN_SEQ is unique across the file, so ordering by it is total within an
    # account and every window below is deterministic. Cast because the column
    # arrives as text and a string sort would put "10" before "9".
    order = F.col(sequence_col).cast("long")
    account = Window.partitionBy(group_col).orderBy(order)

    running = account.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    upto = account.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    onward = account.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    before_here = account.rowsBetween(Window.unboundedPreceding, -1)
    after_here = account.rowsBetween(1, Window.unboundedFollowing)

    def ffill(column):
        return F.last(column, ignorenulls=True).over(upto)

    def bfill(column):
        return F.first(column, ignorenulls=True).over(onward)

    stated = F.col(source_col).cast("double")
    moves = F.col(amount_col).cast("double")

    frame = frame.withColumn(f"{_SCRATCH}stated", stated).withColumn(
        f"{_SCRATCH}moves", moves
    )
    stated = F.col(f"{_SCRATCH}stated")
    moves = F.col(f"{_SCRATCH}moves")

    # An unreadable amount breaks the chain rather than counting as zero.
    # Tracked as a running count instead of a null so a later stated balance
    # can start a fresh chain: one bad row must not cost the account every row
    # after it.
    chain = F.sum(
        F.when(moves.isNull(), F.lit(1)).otherwise(F.lit(0))
    ).over(running)
    # Rounded because money is exact to the cent and a float sum of it is not:
    # adding a few thousand 2-decimal figures leaves residue in the low bits,
    # and rounding to the precision the values actually have makes the
    # tolerance mean what it says rather than spending itself on
    # representation error.
    cumulative = F.bround(
        F.sum(F.coalesce(moves, F.lit(0.0))).over(running), 2
    )

    frame = frame.withColumn(f"{_SCRATCH}chain", chain).withColumn(
        f"{_SCRATCH}cumulative", cumulative
    )
    chain = F.col(f"{_SCRATCH}chain")
    cumulative = F.col(f"{_SCRATCH}cumulative")

    frame = frame.withColumn(
        f"{_SCRATCH}offset",
        F.when(stated.isNotNull(), stated - cumulative),
    )
    offset = F.col(f"{_SCRATCH}offset")
    anchored = offset.isNotNull()

    # The offset and chain at the nearest stated balance on either side. Null
    # on the unstated rows is what makes ignorenulls land on an anchor.
    anchor_chain = F.when(anchored, chain)
    frame = frame.withColumn(
        f"{_SCRATCH}prev_offset",
        F.last(offset, ignorenulls=True).over(before_here),
    ).withColumn(
        f"{_SCRATCH}next_offset",
        F.first(offset, ignorenulls=True).over(after_here),
    ).withColumn(
        f"{_SCRATCH}prev_chain",
        F.last(anchor_chain, ignorenulls=True).over(before_here),
    ).withColumn(
        f"{_SCRATCH}next_chain",
        F.first(anchor_chain, ignorenulls=True).over(after_here),
    )
    prev_offset = F.col(f"{_SCRATCH}prev_offset")
    next_offset = F.col(f"{_SCRATCH}next_offset")
    prev_chain = F.col(f"{_SCRATCH}prev_chain")
    next_chain = F.col(f"{_SCRATCH}next_chain")

    def agrees(other_offset, other_chain):
        return F.coalesce(
            (F.abs(offset - other_offset) <= F.lit(tolerance))
            & (other_chain == chain),
            F.lit(False),
        )

    def reachable(other_offset, other_chain):
        return F.coalesce(
            other_offset.isNotNull() & (other_chain == chain), F.lit(False)
        )

    confirmed = agrees(prev_offset, prev_chain) | agrees(
        next_offset, next_chain
    )
    # A stated balance is only contradicted by a neighbour the arithmetic could
    # actually reach. One with no neighbour at all, or whose neighbours sit the
    # far side of an unreadable amount, was never tested -- and untested is not
    # failed.
    tested = reachable(prev_offset, prev_chain) | reachable(
        next_offset, next_chain
    )

    # Blank rows take the offset of the stated balances bracketing them, and
    # are filled only when those two agree -- the same test the anchors
    # themselves passed, applied to the gap between them.
    frame = frame.withColumn(f"{_SCRATCH}before", ffill(offset)).withColumn(
        f"{_SCRATCH}after", bfill(offset)
    ).withColumn(
        f"{_SCRATCH}chain_before", ffill(anchor_chain)
    ).withColumn(
        f"{_SCRATCH}chain_after", bfill(anchor_chain)
    )
    before = F.col(f"{_SCRATCH}before")
    after = F.col(f"{_SCRATCH}after")
    chain_before = F.col(f"{_SCRATCH}chain_before")
    chain_after = F.col(f"{_SCRATCH}chain_after")

    closed = F.coalesce(
        (F.abs(before - after) <= F.lit(tolerance))
        & (chain_before == chain_after)
        & (chain_before == chain),
        F.lit(False),
    )

    # pandas assigns these in sequence and the LAST assignment wins; a `when`
    # chain reads first-wins, so the order below is the reverse of the
    # original's. The three unstated cases are mutually exclusive -- `closed`
    # cannot be true where `before` is null -- so only the stated ones actually
    # depend on precedence.
    frame = frame.withColumn(
        STATUS,
        when_chain(
            [
                (anchored & confirmed, F.lit("OBSERVED")),
                (anchored & tested & ~confirmed, F.lit("CONTRADICTED")),
                (anchored, F.lit("UNVERIFIED")),
                (closed, F.lit("DERIVED")),
                (before.isNull(), F.lit("UNKNOWN")),
            ],
            otherwise=F.lit("UNVERIFIED"),
        ),
    )
    status = F.col(STATUS)

    frame = frame.withColumn(
        CLEANED,
        when_chain(
            [
                (status == "OBSERVED", stated),
                # Rounded for the same reason the cumulative sum is: the offset
                # came out of a float subtraction, and carrying that residue
                # into the published figure prints money to eleven decimal
                # places. The rounding is well inside the tolerance the value
                # has already passed.
                (status == "DERIVED", F.bround(cumulative + before, 2)),
            ]
        ),
    )
    value = F.col(CLEANED)

    # --- the adjusted column -------------------------------------------
    #
    # Only rows this pipeline was willing to publish may anchor a projection. A
    # merely *stated* balance is not enough -- the contradicted ones are stated
    # too, and they are the corrupted series.
    frame = frame.withColumn(
        f"{_SCRATCH}trusted", F.when(value.isNotNull(), offset)
    )
    trusted = F.col(f"{_SCRATCH}trusted")
    reach = F.when(trusted.isNotNull(), chain)

    frame = frame.withColumn(
        f"{_SCRATCH}t_before", ffill(trusted)
    ).withColumn(
        f"{_SCRATCH}t_after", bfill(trusted)
    ).withColumn(
        f"{_SCRATCH}reach_before", ffill(reach)
    ).withColumn(
        f"{_SCRATCH}reach_after", bfill(reach)
    )

    # An unreadable amount is a hole in the running total, so a balance the far
    # side of one cannot be counted from: the projection would be short by an
    # amount nobody can name.
    frame = frame.withColumn(
        f"{_SCRATCH}usable_before",
        F.when(
            F.col(f"{_SCRATCH}reach_before") == chain,
            F.col(f"{_SCRATCH}t_before"),
        ),
    ).withColumn(
        f"{_SCRATCH}usable_after",
        F.when(
            F.col(f"{_SCRATCH}reach_after") == chain,
            F.col(f"{_SCRATCH}t_after"),
        ),
    )
    usable_before = F.col(f"{_SCRATCH}usable_before")
    usable_after = F.col(f"{_SCRATCH}usable_after")

    # Each row counts from the NEAREST trusted balance rather than one anchor
    # per account, because drift accumulates with distance. Rows before an
    # account's first trusted balance count backwards from it, which is the
    # same arithmetic with the sign reversed.
    base = F.coalesce(usable_before, usable_after)
    bracketed = usable_before.isNotNull() & usable_after.isNotNull()
    closes = F.abs(usable_before - usable_after) <= F.lit(tolerance)

    frame = frame.withColumn(ADJUSTED, F.bround(cumulative + base, 2))
    frame = frame.withColumn(
        ADJUSTED_STATUS,
        when_chain(
            [
                # First, because it wins: a row that already carries a
                # published balance is not a projection at all, whatever the
                # rows around it are doing.
                (value.isNotNull(), F.lit("VERIFIED")),
                (bracketed & ~closes, F.lit("CONTRADICTED")),
                (bracketed & closes, F.lit("CONFIRMED")),
                (base.isNotNull(), F.lit("UNTESTED")),
            ],
            otherwise=F.lit("NO_ANCHOR"),
        ),
    )

    # --- chain breaks ---------------------------------------------------
    #
    # Two published balances can each be verified against their own neighbours
    # and still not account for the step between them. Every value is exactly
    # as sound as its status claims, but a reader differencing the column would
    # meet an unexplained jump, so the row it starts at says so. A plain lead,
    # not an ignorenulls one: the question is about the next row, not the next
    # published balance.
    following = F.lead(value, 1).over(account)
    step = F.lead(moves, 1).over(account)
    frame = frame.withColumn(
        CHAIN_BREAK,
        F.coalesce(
            value.isNotNull()
            & following.isNotNull()
            & (F.abs(value + step - following) > F.lit(tolerance)),
            F.lit(False),
        ),
    )

    return frame.drop(
        *[c for c in frame.columns if c.startswith(_SCRATCH)]
    )
