"""
Running balance: fill what the arithmetic proves, withhold everything else.

The source states a balance on 65% of rows and leaves the rest blank. The
blanks are recoverable, because a balance is not an independent reading -- it
is the previous balance plus the transaction. What makes that safe to act on
here is that the stated values are dense enough to *check* the recovery: most
gaps are closed at both ends by a stated balance, so a filled value can be
compared against what the source says it should have reached.

That check is the whole design. It is not a one-time validation that was run
and then trusted; it is the condition a value must pass to be published at
all. A missing transaction, a mis-parsed amount, or a balance that was never
in this currency all show up the same way -- the arithmetic misses the far
anchor -- and all produce a withheld value rather than a wrong one.

Which amount moves the balance is the one thing this step does not infer.
It is ``TXN_AMOUNT_CLEANED``, the transaction in the account's own currency,
signed by the direction its processing code declares. ``BILLING_AMOUNT`` is
never used: it is stated in USD on every row of this source regardless of what
the account transacts in, so adding it to a local-currency balance is a unit
error. It also has to stay untouched, because it is what
``ConsistencyValidator`` reconciles against -- a column used to build the
balance could not also be the independent check on it.
"""

import pandas as pd

from src.cleaners.base import BaseCleaner

# What is known about each row's balance.
#
# OBSERVED      the source stated it, and it agrees with the arithmetic
#               against a neighbouring stated balance.
# DERIVED       the source left it blank; it was computed from a stated
#               balance and confirmed against the next one.
# CONTRADICTED  the source stated it and the arithmetic proves it wrong. The
#               value is withheld: a number known to be wrong is worse on a
#               sheet than no number, because it will be summed.
# UNVERIFIED    nothing here could be checked -- no stated balance follows it,
#               or an amount along the way is unreadable. Withheld.
# UNKNOWN       no stated balance precedes it, so there is nothing to count
#               from. Withheld.
STATUSES = ["OBSERVED", "DERIVED", "CONTRADICTED", "UNVERIFIED", "UNKNOWN"]

SOURCE = "RUNNING_BALANCE"
CLEANED = "RUNNING_BALANCE_CLEANED"
STATUS = "RUNNING_BALANCE_STATUS"


class BalanceReconstructor(BaseCleaner):
    """
    Recovers the running balance where the arithmetic can be verified, and
    refuses to state one anywhere else.

    Two stated balances in the same account, with the transactions between
    them, are a closed system: the later balance must equal the earlier plus
    those transactions. Expressed per row as ``offset = balance - cumulative
    amount``, that closure becomes an equality -- two stated balances agree
    exactly when they carry the same offset. Every decision here is that one
    comparison.

    An anchor whose offset matches a neighbour is confirmed, and the blanks
    between the two are filled from the same offset. An anchor that matches no
    neighbour is contradicted and its value is dropped. Nothing else is
    written.

    On this source that separates the file cleanly without being told where to
    look: the first 188469 rows close on every one of their 122007 gaps, and
    the rows after them close on none, because the balance there was built
    from ``BILLING_AMOUNT`` and is denominated in a different currency than the
    figure it was applied to. No seam position is configured, and none should
    be -- a hardcoded row number is a fact about one extract, and the next one
    would carry it silently into a file it does not describe.
    """

    name = "balance"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.config.get("source_col", SOURCE)
        amount = self.config.get("amount_col", "TXN_AMOUNT_CLEANED")
        order = self.config.get("sequence_col", "TXN_SEQ")
        group = self.config.get("group_col", "ACCOUNT_ID")

        needed = {source, amount, order, group}
        if not needed.issubset(df.columns):
            return df

        df = df.copy()
        # Sorted for the arithmetic only; every result is written back by
        # index, so the frame leaves this step in the order it arrived.
        ordered = df.index[df[order].astype("int64").argsort(kind="stable")]
        account = df.loc[ordered, group]
        stated = pd.to_numeric(df.loc[ordered, source], errors="coerce")
        moves = pd.to_numeric(df.loc[ordered, amount], errors="coerce")

        # An unreadable amount breaks the chain rather than counting as zero.
        # Tracked as a running count instead of a null so that a later stated
        # balance can start a fresh chain: one bad row must not cost the
        # account every row after it.
        broken = moves.isna()
        chain = broken.groupby(account).cumsum()
        # Rounded because money is exact to the cent and a float sum of it is
        # not: adding a few thousand 2-decimal figures leaves residue in the
        # low bits, and on this source 126 pairs landed within a hundredth of
        # a cent of the tolerance purely from that. Rounding to the precision
        # the values actually have makes the tolerance mean what it says,
        # rather than quietly spending itself on representation error.
        cumulative = moves.fillna(0).groupby(account).cumsum().round(2)
        self.log("amount.unreadable", int(broken.sum()))

        offset = (stated - cumulative).where(stated.notna())
        status, value = self._resolve(
            account, offset, cumulative, chain, stated
        )

        df[CLEANED] = value.reindex(df.index)
        df[STATUS] = pd.Categorical(
            status.reindex(df.index), categories=STATUSES
        )
        self._report(df, ordered, order, account, moves)
        return df

    def _resolve(self, account, offset, cumulative, chain, stated):
        """
        Classifies every row and computes the values that survive.

        :param offset: ``balance - cumulative amount`` at stated rows, null
            elsewhere. Equal offsets on two stated rows mean the transactions
            between them account for the whole change in balance.
        :param chain: Running count of unreadable amounts, so two rows can be
            told to sit either side of a gap in the arithmetic.
        :returns: (status per row, value per row).
        """
        tolerance = self.policy.balance.reconcile_tolerance
        anchored = offset.notna()

        # Each stated balance against the stated balance on either side of it,
        # within the account. Compared on the anchors alone so that the blank
        # rows between them do not shift what counts as adjacent.
        anchors = offset[anchored]
        keys = account[anchored]
        links = chain[anchored]

        def agrees(shift: int) -> pd.Series:
            other = anchors.groupby(keys).shift(shift)
            same_chain = links.groupby(keys).shift(shift) == links
            return ((anchors - other).abs() <= tolerance) & same_chain

        confirmed = (agrees(1) | agrees(-1)).fillna(False)

        # A stated balance is only contradicted by a neighbour the arithmetic
        # could actually reach. One with no neighbour at all, or whose
        # neighbours sit the far side of an unreadable amount, was never
        # tested -- and untested is not failed. Calling it CONTRADICTED would
        # accuse the source of an error the data never demonstrated.
        def reachable(shift: int) -> pd.Series:
            other = anchors.groupby(keys).shift(shift)
            same_chain = links.groupby(keys).shift(shift) == links
            return other.notna() & same_chain

        tested = (reachable(1) | reachable(-1)).fillna(False)

        status = pd.Series("UNVERIFIED", index=offset.index, dtype=object)
        status[anchored] = "UNVERIFIED"
        status[confirmed[confirmed].index] = "OBSERVED"
        contradicted = tested & ~confirmed
        status[contradicted[contradicted].index] = "CONTRADICTED"

        # Blank rows take the offset of the stated balances bracketing them,
        # and are filled only when those two agree -- which is the same test
        # the anchors themselves passed, applied to the gap between them.
        before = offset.groupby(account).ffill()
        after = offset.groupby(account).bfill()
        chain_before = chain.where(anchored).groupby(account).ffill()
        chain_after = chain.where(anchored).groupby(account).bfill()
        closed = (
            (before - after).abs().le(tolerance)
            & chain_before.eq(chain_after)
            & chain_before.eq(chain)
        )

        blank = ~anchored
        status[blank & closed] = "DERIVED"
        status[blank & ~closed & before.notna()] = "UNVERIFIED"
        status[blank & before.isna()] = "UNKNOWN"

        value = pd.Series(pd.NA, index=offset.index, dtype="Float64")
        value[status == "OBSERVED"] = stated[status == "OBSERVED"]
        derived = status == "DERIVED"
        # Rounded for the same reason the cumulative sum above is: the offset
        # it is added to came out of a float subtraction, and carrying that
        # residue into the published figure prints balances to eleven decimal
        # places on a column of money that is exact to the cent. The rounding
        # is well inside the tolerance the value already passed.
        value[derived] = (cumulative + before)[derived].round(2)
        return status, value

    def _report(self, df, ordered, order, account, moves):
        """
        Records the state counts, the breaks left in the published series,
        and, when the arithmetic rejects part of the file, where that part
        begins.

        The boundary is reported rather than configured. It is the finding --
        "the source changed behaviour here" -- and a finding belongs in the
        report, where someone can act on it upstream, not in a constant that
        quietly encodes it as normal.
        """
        status = df[STATUS].astype(str)
        for state in STATUSES:
            self.log(f"status[{state}]", int((status == state).sum()))

        # Two published balances can each be verified against their own
        # neighbours and still not account for the step between them. Every
        # value is exactly as sound as its status claims, but a reader
        # differencing the column would meet an unexplained jump, so the count
        # is stated rather than left to be discovered. On this source it is
        # the seam: all of them are an account's last row before the change in
        # convention beside its first row after it.
        values = pd.to_numeric(df.loc[ordered, CLEANED], errors="coerce")
        following = values.groupby(account).shift(-1)
        step = moves.groupby(account).shift(-1)
        joined = values.notna() & following.notna()
        drift = (values + step - following).abs()
        self.log(
            "chain.breaks",
            int((joined & drift.gt(self.policy.balance.reconcile_tolerance))
                .sum()),
        )

        rejected = df.index[status == "CONTRADICTED"]
        if len(rejected):
            first = int(df.loc[rejected, order].astype("int64").min())
            self.log("contradicted.first_seq", first)
            self.log(
                "contradicted.accounts",
                int(df.loc[rejected, "ACCOUNT_ID"].nunique())
                if "ACCOUNT_ID" in df.columns else 0,
            )
