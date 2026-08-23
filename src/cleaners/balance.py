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

# What is known about each row's ADJUSTED balance -- the second column, which
# states a balance on every row it can rather than only where one is proven.
#
# VERIFIED      the row already has a published balance; the adjusted column
#               repeats it and adds nothing.
# CONFIRMED     projected, and a later trusted balance in the account is
#               reached exactly. The projection is not merely arithmetic here,
#               it is checked.
# CONTRADICTED  projected, and a later trusted balance is NOT reached. The
#               value is still stated, because this column's job is to state
#               one -- but the file is missing money that moved, and this
#               figure is wrong by however much.
# UNTESTED      projected, with no trusted balance after it to check against.
#               Arithmetically sound, evidentially unsupported.
# NO_ANCHOR     the account never had a trusted balance, so there is nothing
#               to project from. No value.
ADJUSTED_STATUSES = [
    "VERIFIED", "CONFIRMED", "CONTRADICTED", "UNTESTED", "NO_ANCHOR",
]

SOURCE = "RUNNING_BALANCE"
CLEANED = "RUNNING_BALANCE_CLEANED"
STATUS = "RUNNING_BALANCE_STATUS"
ADJUSTED = "RUNNING_BALANCE_ADJUSTED"
ADJUSTED_STATUS = "RUNNING_BALANCE_ADJUSTED_STATUS"


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

        adjusted, adjusted_status = self._adjust(
            account, offset, cumulative, chain, value
        )
        df[ADJUSTED] = adjusted.reindex(df.index)
        df[ADJUSTED_STATUS] = pd.Categorical(
            adjusted_status.reindex(df.index), categories=ADJUSTED_STATUSES
        )

        self._report(df, ordered, order, account, moves)
        return df

    def _adjust(self, account, offset, cumulative, chain, value):
        """
        States a balance on every row it can, moved by the transaction alone.

        The published column withholds a value wherever the arithmetic cannot
        prove one, which leaves 80808 rows blank. This one answers the other
        question -- "what would the balance be if the only thing that moved it
        were this account's own transactions?" -- and answers it everywhere
        there is a trusted balance to count from.

        The two columns are deliberately not merged. Everything here is
        arithmetic on ``TXN_AMOUNT_CLEANED``, in the account's own currency,
        and none of it is evidence: where the source records a transfer that
        never appears as a transaction row, the projection misses it and every
        row after it is wrong by that amount, silently and permanently. The
        status column says which rows that is known to have happened to.

        Each row counts from the *nearest* trusted balance rather than one
        anchor per account, because drift accumulates with distance: anchoring
        the whole file at each account's first verified row reproduces the
        surviving later balances 43.6% of the time, and re-anchoring at the
        nearest one raises that to 67.9%. Rows before an account's first
        trusted balance count backwards from it, which is the same arithmetic
        with the sign reversed.

        :param offset: ``balance - cumulative amount`` at stated rows.
        :param value: The published balance, null where it was withheld.
        :returns: (adjusted balance per row, status per row).
        """
        tolerance = self.policy.balance.reconcile_tolerance

        # Only rows this pipeline was willing to publish may anchor a
        # projection. A merely *stated* balance is not enough -- the
        # contradicted ones are stated too, and they are the corrupted series.
        trusted = offset.where(value.notna())
        before = trusted.groupby(account).ffill()
        after = trusted.groupby(account).bfill()

        # An unreadable amount is a hole in the running total, so a balance
        # the far side of one cannot be counted from: the projection would be
        # short by an amount nobody can name.
        reach = chain.where(trusted.notna())
        reach_before = reach.groupby(account).ffill()
        reach_after = reach.groupby(account).bfill()
        usable_before = before.where(reach_before.eq(chain))
        usable_after = after.where(reach_after.eq(chain))

        base = usable_before.fillna(usable_after)
        adjusted = (cumulative + base).round(2).astype("Float64")

        # Whether a later trusted balance agrees with what this row projects.
        # It is the same closure test the published column runs; the
        # difference is only that here a failure annotates the value instead
        # of withholding it.
        bracketed = usable_before.notna() & usable_after.notna()
        closes = (usable_before - usable_after).abs().le(tolerance)

        state = pd.Series("NO_ANCHOR", index=offset.index, dtype=object)
        state[base.notna()] = "UNTESTED"
        state[bracketed & closes] = "CONFIRMED"
        state[bracketed & ~closes] = "CONTRADICTED"
        # Last, so it wins: a row that already carries a published balance is
        # not a projection at all, whatever the rows around it are doing.
        state[value.notna()] = "VERIFIED"
        return adjusted, state

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

        adjusted = df[ADJUSTED_STATUS].astype(str)
        for state in ADJUSTED_STATUSES:
            self.log(f"adjusted[{state}]", int((adjusted == state).sum()))
        # What the second column buys: rows the published one leaves blank
        # and this one can state. Reported next to the contradicted count so
        # the gain is never read without the cost beside it.
        self.log(
            "adjusted.fills_a_withheld_row",
            int((df[CLEANED].isna() & df[ADJUSTED].notna()).sum()),
        )

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
