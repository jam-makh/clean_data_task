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

import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.schema.balance import (
    BASES,
    BASIS,
    BILLING,
    CHAIN_BREAK,
    CURRENCY,
    DISCREPANCY,
    FILLED,
    NATIVE,
    NORMALIZED,
    PROVEN,
    SOURCE,
    STATUS,
    STATUSES,
    USD,
    boundaries,
    segment,
)
from src.spark import audit
from src.spark.spark_utils import chain as when_chain

# The vocabulary, the column names and the segmentation are the pandas
# original's, imported rather than restated. They must stay identical -- the
# parity harness compares the two state for state and value for value -- and a
# second spelling of any of them is a way for that to stop being true silently.
__all__ = [
    "STATUSES", "BASES", "SOURCE", "FILLED", "CURRENCY", "NORMALIZED",
    "STATUS", "BASIS", "DISCREPANCY", "CHAIN_BREAK", "apply", "metrics",
]

# Scratch columns. Prefixed and dropped at the end, so an intermediate cannot
# be mistaken for output by a later stage or by the writer.
_SCRATCH = "_bal_"


def _detect_regimes(
    frame, policy, source_col, amount_col, billing_col, sequence_col, account
):
    """
    Decides, per row, which column moved the balance.

    Same question and same answer as the pandas original: every pair of
    consecutive rows in an account where the source states a balance on both
    is evidence about one row's mover, and the labelling of that evidence is
    the cheapest one allowing for a per-change penalty.

    The segmentation itself is irreducibly sequential -- the cheapest label
    for a row depends on the row before it, all the way back -- so there is no
    window function that computes it. What there is instead is a very small
    summary: the evidence is one boolean pair per stated-balance pair, which
    on this source is 126015 rows out of 265195, and the *answer* is a handful
    of change points. So the evidence is collected to the driver, segmented by
    the same ``segment`` the pandas path calls, and the result comes back as a
    ``when`` chain on ``TXN_SEQ`` -- a broadcast comparison against three or
    four constants, evaluated per row with no Python.

    That is one extra job over a narrow projection, and it buys the property
    the whole step rests on: no row number is configured anywhere.

    :param account: The account window, ordered by sequence.
    :returns: A column expression yielding NATIVE or BILLING.
    """
    if billing_col not in frame.columns:
        return F.lit(NATIVE)

    tolerance = policy.balance.reconcile_tolerance
    penalty = policy.balance.regime_switch_penalty

    stated = F.col(source_col).cast("double")
    # The previous ROW's balance, not the previous stated one: the mover on
    # this row explains one step, and a gap of blanks between two anchors
    # spans several.
    delta = stated - F.lag(stated, 1).over(account)

    def explains(column):
        return F.coalesce(
            F.abs(delta - F.col(column).cast("double")) <= F.lit(tolerance),
            F.lit(False),
        )

    # Two selects: a window expression cannot appear in a WHERE, so the delta
    # is projected first and the rows carrying no evidence dropped after.
    evidence = frame.select(
        F.col(sequence_col).cast("long").alias("seq"),
        delta.alias("delta"),
        explains(amount_col).alias("native"),
        explains(billing_col).alias("billing"),
    ).where(F.col("delta").isNotNull()).select(
        "seq", "native", "billing"
    ).orderBy("seq").collect()

    if not evidence:
        return F.lit(NATIVE)

    keys = np.array([row["seq"] for row in evidence], dtype="int64")
    cost = np.vstack([
        [not row["native"] for row in evidence],
        [not row["billing"] for row in evidence],
    ])
    marks = boundaries(keys, segment(cost, penalty))

    order = F.col(sequence_col).cast("long")
    # Highest change point first: `chain` is first-wins, so testing the
    # boundaries in descending order is what makes each row take the label of
    # the last change at or before it. The first run's label is the
    # `otherwise`, which is also what rows before any evidence get.
    return when_chain(
        [
            (order >= F.lit(int(key)), F.lit(label))
            for key, label in reversed(marks[1:])
        ],
        otherwise=F.lit(marks[0][1]),
    )


def apply(
    frame,
    policy,
    source_col: str = SOURCE,
    amount_col: str = "TXN_AMOUNT_CLEANED",
    billing_col: str = "BILLING_AMOUNT",
    sequence_col: str = "TXN_SEQ",
    group_col: str = "ACCOUNT_ID",
    currency_col: str = "TXN_CCY",
    billing_currency_col: str = "BILLING_CURRENCY",
    fx_col: str = "FX_RATE",
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
    :param amount_col: One candidate mover: ``TXN_AMOUNT_CLEANED``, in the
        account's own currency.
    :param billing_col: The other: ``BILLING_AMOUNT``. Which of the two is in
        force is detected per row rather than assumed -- see
        ``_detect_regimes``. Neither column is written to; ``BILLING_AMOUNT``
        in particular has to survive untouched because ``consistency``
        reconciles against it.
    :param sequence_col: The source's own global ordering.
    :param group_col: The account a balance belongs to.
    :param currency_col: The account's own currency, which denominates a
        balance built from ``amount_col``.
    :param billing_currency_col: The billing denomination, which denominates a
        balance built from ``billing_col``.
    :param fx_col: The row's rate, used to value a non-USD balance in USD.
        Read, never written.
    :returns: The frame with the three balance columns, the basis and status
        columns, the projection and the chain-break flag added. Unchanged if
        any required input column is absent, which is how the pandas original
        behaves on a profile that carries no balance at all.
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

    # Which column moves the balance, per row. Decided before any of the
    # arithmetic below, so that every cumulative total, offset and closure
    # test is asked about the convention actually in force on that row.
    basis = _detect_regimes(
        frame, policy, source_col, amount_col, billing_col, sequence_col,
        account,
    )
    frame = frame.withColumn(BASIS, basis)
    basis = F.col(BASIS)

    moves = F.when(
        basis == F.lit(NATIVE), F.col(amount_col).cast("double")
    ).otherwise(
        F.col(billing_col).cast("double")
        if billing_col in frame.columns else F.lit(None).cast("double")
    )

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

    # Which anchors may be counted from. A stated balance two reachable
    # neighbours disprove is excluded: projecting from it would spread a known
    # error over every row that counts from it. One with no reachable
    # neighbour is kept -- untested is not disproved.
    #
    # `anchored &` is not redundant. `tested` asks whether a reachable stated
    # balance exists on either side, and on an UNSTATED row that is usually
    # true -- while `confirmed` compares this row's null offset and is always
    # false. Without the guard every blank row reads as disproved. The pandas
    # original cannot make this mistake because it computes both on the
    # anchors-only subframe and reindexes with fill_value=False; here the
    # expressions span the whole frame and the guard has to be written.
    disproved = anchored & tested & ~confirmed
    frame = frame.withColumn(
        f"{_SCRATCH}trusted", F.when(anchored & ~disproved, offset)
    )
    trusted = F.col(f"{_SCRATCH}trusted")
    reach = F.when(trusted.isNotNull(), chain)

    frame = frame.withColumn(
        f"{_SCRATCH}before", ffill(trusted)
    ).withColumn(
        f"{_SCRATCH}after", bfill(trusted)
    ).withColumn(
        f"{_SCRATCH}reach_before", ffill(reach)
    ).withColumn(
        f"{_SCRATCH}reach_after", bfill(reach)
    )

    # An unreadable amount is a hole in the running total, so an anchor the far
    # side of one cannot be counted from: the reconstruction would be short by
    # an amount nobody can name.
    frame = frame.withColumn(
        f"{_SCRATCH}usable_before",
        F.when(F.col(f"{_SCRATCH}reach_before") == chain,
               F.col(f"{_SCRATCH}before")),
    ).withColumn(
        f"{_SCRATCH}usable_after",
        F.when(F.col(f"{_SCRATCH}reach_after") == chain,
               F.col(f"{_SCRATCH}after")),
    )
    usable_before = F.col(f"{_SCRATCH}usable_before")
    usable_after = F.col(f"{_SCRATCH}usable_after")

    # The two reconstructions. Rounded for the same reason the cumulative sum
    # is: the offset came out of a subtraction, and carrying that residue into
    # the published figure prints money to eleven decimal places.
    frame = frame.withColumn(
        f"{_SCRATCH}forward", F.bround(cumulative + usable_before, 2)
    ).withColumn(
        f"{_SCRATCH}backward", F.bround(cumulative + usable_after, 2)
    )
    forward = F.col(f"{_SCRATCH}forward")
    backward = F.col(f"{_SCRATCH}backward")

    bracketed = usable_before.isNotNull() & usable_after.isNotNull()
    closes = F.abs(usable_before - usable_after) <= F.lit(tolerance)

    # pandas assigns these in sequence and the LAST assignment wins; a `when`
    # chain reads first-wins, so the order below is the reverse of the
    # original's.
    frame = frame.withColumn(
        STATUS,
        when_chain(
            [
                (anchored & confirmed, F.lit("OBSERVED")),
                (anchored & ~tested, F.lit("UNVERIFIED")),
                (disproved, F.lit("CONTRADICTED")),
                (bracketed & ~closes, F.lit("CONTRADICTED")),
                (bracketed & closes, F.lit("DERIVED")),
                (usable_after.isNotNull() & usable_before.isNull(),
                 F.lit("BACKWARD_DERIVED")),
                (usable_before.isNotNull() & usable_after.isNull(),
                 F.lit("FORWARD_DERIVED")),
            ],
            otherwise=F.lit("UNAVAILABLE"),
        ),
    )
    status = F.col(STATUS)

    frame = frame.withColumn(
        FILLED,
        when_chain(
            [
                (status.isin("OBSERVED", "UNVERIFIED"), stated),
                (status.isin("DERIVED", "FORWARD_DERIVED"), forward),
                (status == "BACKWARD_DERIVED", backward),
                # A contradicted row keeps the source's own figure where the
                # source gave one; only where it said nothing does the forward
                # reconstruction stand in.
                (status == "CONTRADICTED", F.coalesce(stated, forward)),
            ]
        ),
    )
    value = F.col(FILLED)

    frame = frame.withColumn(
        DISCREPANCY,
        F.when(status == "CONTRADICTED", F.bround(backward - forward, 2)),
    )

    # --- denomination and the USD valuation -----------------------------
    #
    # The denomination follows the mover, because that is what the figure was
    # accumulated from. Both columns are null wherever no balance was
    # published: there is no denomination for a figure that was withheld.
    def cleaned_text(column, fallback):
        if column not in frame.columns:
            return F.lit(fallback)
        return F.upper(F.trim(F.col(column).cast("string")))

    currency = F.when(
        value.isNotNull(),
        F.when(
            basis == F.lit(BILLING),
            cleaned_text(billing_currency_col, USD),
        ).otherwise(cleaned_text(currency_col, None)),
    )
    frame = frame.withColumn(CURRENCY, currency)
    currency = F.col(CURRENCY)

    # 1.0 by definition where the balance is already in USD -- asserted from
    # the currency, not read from FX_RATE, which is not 1 on most USD rows of
    # this source. The raw column is untouched; this is a separate effective
    # rate that exists only inside this expression.
    rate = (
        F.col(fx_col).cast("double")
        if fx_col in frame.columns else F.lit(None).cast("double")
    )
    effective = F.when(currency == F.lit(USD), F.lit(1.0)).otherwise(rate)
    frame = frame.withColumn(NORMALIZED, F.bround(value * effective, 2))

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


def metrics(
    frame,
    policy,
    amount_col: str = "TXN_AMOUNT_CLEANED",
    sequence_col: str = "TXN_SEQ",
    group_col: str = "ACCOUNT_ID",
):
    """
    The state counts, the breaks left in the published series, which mover was
    found to be in force where, and, when the arithmetic rejects part of the
    file, where that part begins and how much of the book it touches.

    The regime boundary is reported rather than configured. It is the finding -- "the
    source changed behaviour here" -- and a finding belongs in the report,
    where someone can act on it upstream, not in a constant that quietly
    encodes it as normal.

    :param frame: The frame as the last stage left it.
    :param policy: Unused; the tolerance decided the states, and the states
        are what is counted.
    :param amount_col: What moved the balance, read back to find the holes.
    :param sequence_col: The source's own global ordering.
    :param group_col: The account a balance belongs to.
    :returns: ``(metric, request)`` pairs in report order.
    """
    if STATUS not in frame.columns:
        return []

    # An unreadable amount is a hole in the running total. Read back off the
    # cleaned column, which no stage after this one writes to, so the answer
    # is the one the arithmetic actually worked from.
    out = [(
        "amount.unreadable",
        audit.rows(F.col(amount_col).cast("double").isNull()),
    )]

    status = F.col(STATUS)
    for state in STATUSES:
        out.append((
            f"status[{state}]", audit.rows(status == F.lit(state))
        ))

    # What the detector found. One label over the whole file means one
    # convention; the seq is where the second one starts. Suppressed when the
    # file never changes convention, which is the pandas guard reached the
    # same way the contradicted one below is.
    basis = F.col(BASIS)
    for state in BASES:
        out.append((
            f"basis[{state}]", audit.rows(basis == F.lit(state))
        ))
    out.append((
        "basis.first_billing_seq",
        audit.minimum(
            F.when(
                basis == F.lit(BILLING), F.col(sequence_col).cast("long")
            )
        ),
    ))

    # The denomination the published balances came out in, and how many of
    # them the USD valuation could not be computed for -- a missing rate on a
    # non-USD row is the one way a published balance fails to normalize.
    out.append(("currency.stated", audit.rows(F.col(CURRENCY).isNotNull())))
    out.append((
        "normalized.unavailable",
        audit.rows(F.col(FILLED).isNotNull() & F.col(NORMALIZED).isNull()),
    ))

    # The invariant, counted rather than asserted, so that a run which breaks
    # it says so in its own report instead of only in a test.
    out.append(("balance.stated", audit.rows(F.col(FILLED).isNotNull())))
    out.append(("balance.unavailable", audit.rows(F.col(FILLED).isNull())))
    out.append(("balance.proven", audit.rows(status.isin(*sorted(PROVEN)))))
    out.append(("chain.breaks", audit.rows(F.col(CHAIN_BREAK))))

    # Null -- and therefore not reported at all -- when nothing was rejected,
    # which is the pandas ``if rejected.any():`` guard arrived at without a
    # second pass to ask the question.
    rejected = status == F.lit("CONTRADICTED")
    out.append((
        "contradicted.first_seq",
        audit.minimum(F.when(rejected, F.col(sequence_col).cast("long"))),
    ))
    # How wide the rejection is, next to where it starts -- one account
    # rejected wholesale and every account rejected in places are different
    # findings and the boundary alone cannot tell them apart. Suppressed at
    # zero, which is the pandas ``if rejected.any():`` guard reached from the
    # other side: no rejected rows means no distinct accounts among them.
    out.append((
        "contradicted.accounts",
        audit.distinct(
            F.when(rejected, F.col(group_col)), nonzero=True
        ),
    ))
    return out
