"""
Running balance: state a figure wherever the arithmetic reaches, and say how.

The source states a balance on 65% of rows and leaves the rest blank. The
blanks are recoverable, because a balance is not an independent reading -- it
is the previous balance plus the transaction. Reconstruction runs in both
directions: forward from the last trusted balance before a row, and backward
from the first trusted one after it. Between two anchors both directions
apply and agreeing is the check; outside every bracket only one does, and the
arithmetic is no less exact for having nothing to confirm it.

Every row therefore carries a number and a status, and the status is not
decoration. It is the whole of what distinguishes a figure two independent
claims agree on from one reconstructed in a single direction, and from one
standing inside a span the source's own anchors refuse to reconcile.
``RUNNING_BALANCE_STATUS`` is published beside the figure for exactly that
reason -- a consumer that cannot tell them apart will average them together.

The earlier design of this step withheld a value wherever it could not prove
one, on the principle that a wrong number is worse than none. The principle
survives; what changed is where it is enforced. A withheld value is not
neutral downstream -- it is a null that a feature build must either drop or
impute, and both of those are decisions made further from the evidence than
this module is. Stating the figure and naming its provenance moves the
decision to where the evidence still exists. The only rows left without a
number are the ones where there genuinely is none: no trusted anchor in the
account is reachable from them, in either direction.

Which amount moves the balance is *detected*, not assumed
-------------------------------------------------------

This source does not use one mover throughout. For its first 188469 rows the
stated balance moves by ``TXN_AMOUNT_CLEANED``, the transaction in the
account's own currency; from row 188470 on it moves by ``BILLING_AMOUNT``
instead. Nothing in the file announces that, and an earlier version of this
step assumed the first convention everywhere -- which is why the whole second
half of the extract came out CONTRADICTED.

The seam is found rather than configured. Every pair of consecutive stated
balances in an account is asked which of the two candidate movers accounts for
the step between them, and the resulting evidence is segmented in sequence
order by ``_detect_regimes``. A hardcoded row number is a fact about one
extract; the next one would carry it silently into a file it does not
describe. The detector handles a file with no seam, one seam, or several,
because it is told to find changes rather than to find *the* change.

Two stated balances either side of a seam still agree, and that is not luck:
the running total is accumulated from whichever mover governs each row, so the
difference between two cumulative totals is exactly the sum of the movers that
were actually in force between them. The seam needs no special case.

What currency the answer is in
------------------------------

A balance rebuilt from ``TXN_AMOUNT_CLEANED`` is in the account's own currency,
``TXN_CCY``. A balance rebuilt from ``BILLING_AMOUNT`` is in the billing
currency, which ``BILLING_CURRENCY`` states as USD on every row of this source.
Those are different denominations and the column would be uninterpretable
without saying which is which, so ``RUNNING_BALANCE_CURRENCY`` says it on every
row that carries a balance at all.

``RUNNING_BALANCE_NORMALIZED`` then values that balance in USD, so that
anything downstream aggregating across accounts is adding comparable figures.
A balance already in USD normalizes to itself at an effective rate of exactly
1.0 -- asserted from the currency, not computed from ``FX_RATE``, which is not
1 on every USD row of this source. Anything else is valued at the row's own
``FX_RATE``. That is a point-in-time valuation: the rate belongs to the
transaction on this row, while the balance accumulated over many rows at many
rates, so it answers "what is this balance worth now" and not "what was each
historical movement worth when it happened". The raw ``FX_RATE`` column is
read and never written.
"""

import numpy as np
import pandas as pd

from src.cleaners.base import BaseCleaner
from src.utils import audit

# How each row's balance was arrived at. Every row gets one of these, and
# every row but UNAVAILABLE gets a number beside it -- the column states a
# figure wherever the arithmetic can produce one, and this column is what
# makes that safe to read.
#
# The order is strongest evidence first, and it is the order a consumer should
# apply a threshold in: everything above the line it draws is usable, and the
# statuses are declared here in that order so the line is a slice.
#
# OBSERVED          the source stated it, and a neighbouring stated balance
#                   confirms the arithmetic between them. Two independent
#                   claims agreeing.
# DERIVED           the source left it blank, and the trusted balances either
#                   side of it agree about what it must be. Computed, but
#                   checked from both directions.
# FORWARD_DERIVED   counted forward from the last trusted balance in the
#                   account. Nothing after it to check against, because there
#                   is no later trusted balance. Sound arithmetic, one-sided
#                   evidence.
# BACKWARD_DERIVED  counted backwards from the first trusted balance in the
#                   account -- the same arithmetic with the sign reversed, for
#                   the rows that precede any anchor. Also one-sided.
# UNVERIFIED        the source stated it and nothing could test it: no
#                   reachable neighbour to compare against. The value is the
#                   source's own, unchecked.
# CONTRADICTED      the trusted balances bracketing this row do NOT agree, so
#                   the file is missing money that moved somewhere in the
#                   span. A figure is still stated -- the source's own where
#                   it gave one, the forward reconstruction otherwise -- and
#                   RUNNING_BALANCE_DISCREPANCY says by how much the two
#                   directions disagree. Never treat one of these as a fact.
# UNAVAILABLE       no trusted balance anywhere in the account is reachable
#                   from this row, so there is nothing to count from in either
#                   direction. The only status that carries no number, and the
#                   only one on which RUNNING_BALANCE_FILLED is null.
STATUSES = [
    "OBSERVED",
    "DERIVED",
    "FORWARD_DERIVED",
    "BACKWARD_DERIVED",
    "UNVERIFIED",
    "CONTRADICTED",
    "UNAVAILABLE",
]

# The statuses whose figure the arithmetic actually proved, as opposed to
# merely computed. Named rather than spelled out at each use because three
# layers ask the same question -- the invariant test, the report, and any
# consumer choosing what to trust -- and they must ask it identically.
PROVEN = frozenset({"OBSERVED", "DERIVED"})

# The statuses that carry a number. Everything except the one that does not,
# written this way so that adding a status forces a decision here.
NUMERIC = frozenset(STATUSES) - {"UNAVAILABLE"}

# Which column was found to move the balance on this row. Not a setting -- the
# detector's answer, written onto the rows so that the report can count it and
# a reader can see where the source changed convention.
NATIVE = "NATIVE"
BILLING = "BILLING"
BASES = [NATIVE, BILLING]

SOURCE = "RUNNING_BALANCE"
FILLED = "RUNNING_BALANCE_FILLED"
CURRENCY = "RUNNING_BALANCE_CURRENCY"
NORMALIZED = "RUNNING_BALANCE_NORMALIZED"
STATUS = "RUNNING_BALANCE_STATUS"
BASIS = "RUNNING_BALANCE_BASIS"

# On a CONTRADICTED row, what the two directions disagree by, signed as
# ``backward - forward``. Signed rather than absolute so that the reading it
# does not carry is recoverable: the published figure plus this column is the
# other direction's answer, exactly. Null on every other status, because
# nowhere else are there two answers to differ.
DISCREPANCY = "RUNNING_BALANCE_DISCREPANCY"

# The currency a balance is normalized *to*, and therefore the one whose
# effective rate is 1.0 by definition rather than by lookup.
USD = "USD"

# Whether the published balance on this row fails to lead to the one on the
# next row of the same account. A property of the pair, recorded on the
# earlier of the two, because that is the row a reader differencing the column
# is standing on when the jump appears.
CHAIN_BREAK = "RUNNING_BALANCE_CHAIN_BREAK"


def segment(cost: np.ndarray, penalty: float) -> np.ndarray:
    """
    The cheapest labelling of a sequence, given a per-row cost for each label
    and a fixed cost to change label.

    Exact rather than heuristic, and linear in the number of rows: with two
    states the usual dynamic program collapses to carrying one running total
    per state and a single back-pointer per row.

    Module level, and imported by the Spark port rather than reimplemented
    there. It is the one part of this step that is irreducibly sequential --
    each row's cheapest label depends on the row before it -- so the Spark
    path collects the same evidence to the driver and calls this same
    function. Two implementations of a dynamic program would be two chances to
    disagree on a file where the seam is marginal, and the parity harness
    would be comparing them rather than the pipeline.

    :param cost: ``(2, n)`` -- what each label costs on each row.
    :param penalty: What changing label costs, in the same units.
    :returns: ``(n,)`` of 0/1, the label chosen for each row.
    """
    n = cost.shape[1]
    back = np.zeros((n, 2), dtype=np.int8)
    totals = cost[:, 0].astype(float)

    for i in range(1, n):
        switched = totals[::-1] + penalty
        stay = totals <= switched
        back[i] = np.where(stay, [0, 1], [1, 0])
        totals = np.where(stay, totals, switched) + cost[:, i]

    path = np.empty(n, dtype=np.int8)
    state = int(np.argmin(totals))
    for i in range(n - 1, -1, -1):
        path[i] = state
        state = back[i, state]
    return path


def boundaries(keys, path) -> list[tuple]:
    """
    Compresses a per-row labelling into the points where it changes.

    :param keys: The ordering value -- ``TXN_SEQ`` -- for each labelled row.
    :param path: The labelling ``segment`` returned.
    :returns: ``(key, label)`` at the first row of each run, in order. The
        first entry's key is where the evidence starts, not where the file
        does, so rows before it belong to that first run too.
    """
    changed = np.empty(len(path), dtype=bool)
    changed[0] = True
    changed[1:] = path[1:] != path[:-1]
    return [
        (k, BASES[int(p)])
        for k, p in zip(np.asarray(keys)[changed], path[changed])
    ]


class BalanceReconstructor(BaseCleaner):
    """
    Recovers the running balance where the arithmetic can be verified, and
    refuses to state one anywhere else.

    Two stated balances in the same account, with the transactions between
    them, are a closed system: the later balance must equal the earlier plus
    those transactions. Expressed per row as ``offset = balance - cumulative
    mover``, that closure becomes an equality -- two stated balances agree
    exactly when they carry the same offset. Every decision here is that one
    comparison.

    An anchor whose offset matches a neighbour is confirmed, and the blanks
    between the two are filled from the same offset. An anchor that matches no
    neighbour is contradicted and its value is dropped. Nothing else is
    written.

    Which mover the cumulative total is built from is decided per row by
    ``_detect_regimes`` before any of that runs, so the closure test is asked
    about the convention actually in force rather than about an assumed one.
    """

    name = "balance"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.config.get("source_col", SOURCE)
        amount = self.config.get("amount_col", "TXN_AMOUNT_CLEANED")
        billing = self.config.get("billing_col", "BILLING_AMOUNT")
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
        native = pd.to_numeric(df.loc[ordered, amount], errors="coerce")
        billed = (
            pd.to_numeric(df.loc[ordered, billing], errors="coerce")
            if billing in df.columns else None
        )

        basis = self._detect_regimes(account, stated, native, billed)
        # The mover actually in force on each row. Everything downstream reads
        # this and nothing else, which is what keeps the seam from needing a
        # special case anywhere below.
        moves = native.where(basis.eq(NATIVE), billed)

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

        offset = (stated - cumulative).where(stated.notna())
        status, value, discrepancy = self._resolve(
            account, offset, cumulative, chain, stated
        )

        df[FILLED] = value.reindex(df.index)
        df[STATUS] = pd.Categorical(
            status.reindex(df.index), categories=STATUSES
        )
        df[DISCREPANCY] = discrepancy.reindex(df.index)
        df[BASIS] = pd.Categorical(
            basis.reindex(df.index), categories=BASES
        )

        currency, normalized = self._denominate(df, ordered, basis, value)
        df[CURRENCY] = currency.reindex(df.index)
        df[NORMALIZED] = normalized.reindex(df.index)

        df[CHAIN_BREAK] = self._chain_breaks(df, ordered, account, moves)
        return df

    def _detect_regimes(self, account, stated, native, billed) -> pd.Series:
        """
        Decides, for each row, which column moved the balance.

        The evidence is every pair of consecutive rows in an account where the
        source states a balance on both. The step between them is a fact, and
        each candidate mover either accounts for it or does not::

            delta        = balance[i] - balance[i-1]
            explains_native  = |delta - TXN_AMOUNT_CLEANED[i]| <= tolerance
            explains_billing = |delta - BILLING_AMOUNT[i]|     <= tolerance

        A regime is a run of sequence numbers over which one mover keeps
        winning, so the answer is a segmentation of that evidence in sequence
        order rather than a majority vote over the file. It is computed as a
        two-state Viterbi pass: each row costs 1 if the mover it is labelled
        with fails to explain that row's step, and changing label costs
        ``regime_switch_penalty``. The cheapest labelling is the answer.

        That formulation is what makes the step work on files this one says
        nothing about. A file with a single convention pays a switch penalty
        it can never recoup and comes back all one label; a file with three
        seams comes back with three, because nothing in the cost function
        knows how many it is looking for. Isolated rows that only one mover
        happens to explain -- coincidence, on small amounts -- cannot open a
        regime of their own, because a two-switch excursion has to save more
        than twice the penalty to be worth taking.

        Rows carrying no evidence, and rows before the first evidence in the
        file, take the label of the nearest evidence row in sequence order.

        :param stated: The stated balance, null where the source was blank.
        :param native: ``TXN_AMOUNT_CLEANED``, the account-currency mover.
        :param billed: ``BILLING_AMOUNT``, or None when the source has no such
            column -- in which case there is nothing to detect and every row
            is NATIVE.
        :returns: NATIVE/BILLING per row, on the sequence-ordered index.
        """
        index = stated.index
        if billed is None:
            return pd.Series(NATIVE, index=index, dtype=object)

        tolerance = self.policy.balance.reconcile_tolerance
        penalty = self.policy.balance.regime_switch_penalty

        # The previous ROW's balance, not the previous stated one: the mover
        # on this row explains one step, and a gap of blanks between two
        # anchors spans several. Only adjacent pairs are evidence about a
        # single row's mover.
        previous = stated.groupby(account).shift(1)
        delta = stated - previous
        evidence = delta.notna()
        if not evidence.any():
            return pd.Series(NATIVE, index=index, dtype=object)

        explains = np.vstack([
            (delta - native).abs().le(tolerance)[evidence].to_numpy(),
            (delta - billed).abs().le(tolerance)[evidence].to_numpy(),
        ])
        path = segment(~explains, penalty)

        labels = pd.Series(
            np.where(path == 0, NATIVE, BILLING),
            index=index[evidence],
            dtype=object,
        )
        # ffill then bfill along the sequence-ordered index: a row with no
        # evidence of its own belongs to the regime it sits inside.
        return labels.reindex(index).ffill().bfill()

    def _denominate(self, df, ordered, basis, value):
        """
        States what currency each published balance is in, and what it is
        worth in USD.

        The denomination follows the mover, because that is what the figure
        was accumulated from: a balance built from ``TXN_AMOUNT_CLEANED`` is
        in ``TXN_CCY``, and one built from ``BILLING_AMOUNT`` is in
        ``BILLING_CURRENCY``.

        The normalization rate is 1.0 wherever that currency is USD. Asserted,
        not looked up -- ``FX_RATE`` is not 1 on most USD rows of this source,
        and multiplying a USD balance by it would restate dollars as
        not-quite-dollars. Everywhere else the row's own ``FX_RATE`` converts
        the balance to USD, which is a valuation at this row's rate and not a
        replay of the history that built the balance.

        Both columns are null wherever no balance was published, which after
        the redesign is the UNAVAILABLE rows alone: there is no denomination
        for a figure that does not exist. They are stated on every other row,
        contradicted ones included -- a disputed figure is still denominated
        in something, and leaving the currency off would make the dispute
        unreadable rather than visible.

        :param basis: NATIVE/BILLING per row, sequence-ordered.
        :param value: The published balance, null where it was withheld.
        :returns: (currency per row, USD valuation per row).
        """
        native_ccy = self.config.get("currency_col", "TXN_CCY")
        billing_ccy = self.config.get("billing_currency_col",
                                      "BILLING_CURRENCY")
        fx_col = self.config.get("fx_col", "FX_RATE")

        def column(name, default=None):
            if name not in df.columns:
                return pd.Series(default, index=basis.index, dtype=object)
            return (
                df.loc[ordered, name].astype("string")
                .str.strip().str.upper()
            )

        currency = column(billing_ccy, USD).where(
            basis.eq(BILLING), column(native_ccy)
        )
        # A balance nobody published has no denomination to state.
        currency = currency.where(value.notna()).astype(object)

        if fx_col in df.columns:
            rate = pd.to_numeric(df.loc[ordered, fx_col], errors="coerce")
        else:
            rate = pd.Series(np.nan, index=basis.index, dtype="float64")
        # 1.0 by definition where the balance is already in USD. This is the
        # one place the raw column is deliberately overridden rather than
        # read, and it is overridden only where arithmetic makes it moot.
        effective = rate.where(~currency.eq(USD).fillna(False), 1.0)

        normalized = (value * effective).round(2).astype("Float64")
        return currency, normalized

    def _chain_breaks(self, df, ordered, account, moves) -> pd.Series:
        """
        Marks each published balance that does not lead to the next one.

        Two published balances can each be verified against their own
        neighbours and still not account for the step between them. Every
        value is exactly as sound as its status claims, but a reader
        differencing the column would meet an unexplained jump, so the row it
        starts at says so.

        The step is the detected mover, not one fixed column, so a pair
        straddling a change of convention is tested against the amount that
        actually moved it rather than being reported as a break it is not.

        :param ordered: The frame's index in transaction-sequence order.
        :returns: Boolean per row, on the frame's own index.
        """
        values = pd.to_numeric(df.loc[ordered, FILLED], errors="coerce")
        following = values.groupby(account).shift(-1)
        step = moves.groupby(account).shift(-1)
        joined = values.notna() & following.notna()
        drift = (values + step - following).abs()
        broken = joined & drift.gt(self.policy.balance.reconcile_tolerance)
        return (
            broken.reindex(df.index).fillna(False).astype(bool)
        )

    def _resolve(self, account, offset, cumulative, chain, stated):
        """
        Classifies every row and computes the figure that goes with it.

        The rule is one comparison applied twice. ``offset = balance -
        cumulative mover`` turns "the later balance must equal the earlier
        plus the transactions between" into an equality: two balances agree
        exactly when they carry the same offset. Reconstructing a row is then
        adding an anchor's offset back to this row's cumulative total, and it
        can be done from an anchor before this row or from one after it. The
        two reconstructions are what everything below is decided by.

            forward  = cumulative + (nearest trusted offset at or before)
            backward = cumulative + (nearest trusted offset at or after)

        A row bracketed by two trusted anchors gets both, and whether they
        match is the evidence. A row outside every bracket -- before an
        account's first anchor, or after its last -- gets exactly one, which
        is sound arithmetic that nothing independent confirms. Only a row that
        can reach no trusted anchor at all gets neither, and that is the sole
        case this step has no number for.

        Which anchors may be counted from is the other half. A *stated*
        balance is not automatically one: where two reachable neighbours
        disprove it, projecting from it would spread a known error over every
        row that counts from it, so it is excluded from the anchor set and
        becomes a row to be reconstructed like any other. A stated balance
        with no reachable neighbour is kept -- untested is not disproved, and
        an account whose only anchor is unconfirmed still has better evidence
        for that figure than for any alternative.

        :param offset: ``balance - cumulative mover`` at stated rows, null
            elsewhere.
        :param chain: Running count of unreadable amounts, so two rows can be
            told to sit either side of a gap in the arithmetic.
        :returns: (status per row, value per row, discrepancy per row).
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

        # A stated balance is only disproved by a neighbour the arithmetic
        # could actually reach. One with no neighbour at all, or whose
        # neighbours sit the far side of an unreadable amount, was never
        # tested -- and untested is not failed. Calling it contradicted would
        # accuse the source of an error the data never demonstrated.
        def reachable(shift: int) -> pd.Series:
            other = anchors.groupby(keys).shift(shift)
            same_chain = links.groupby(keys).shift(shift) == links
            return other.notna() & same_chain

        tested = (reachable(1) | reachable(-1)).fillna(False)
        disproved = (tested & ~confirmed).reindex(offset.index, fill_value=False)

        # The anchor set: every stated balance the arithmetic has not
        # disproved. Nothing else may be counted from.
        trusted = offset.where(anchored & ~disproved)

        before = trusted.groupby(account).ffill()
        after = trusted.groupby(account).bfill()
        # An unreadable amount is a hole in the running total, so an anchor
        # the far side of one cannot be counted from: the reconstruction would
        # be short by an amount nobody can name.
        reach = chain.where(trusted.notna())
        usable_before = before.where(reach.groupby(account).ffill().eq(chain))
        usable_after = after.where(reach.groupby(account).bfill().eq(chain))

        # Rounded for the same reason the cumulative sum is: the offset came
        # out of a float subtraction, and carrying that residue into the
        # published figure prints money to eleven decimal places on a column
        # that is exact to the cent. The rounding is well inside the tolerance
        # the value is then tested against.
        forward = (cumulative + usable_before).round(2)
        backward = (cumulative + usable_after).round(2)

        bracketed = usable_before.notna() & usable_after.notna()
        closes = (usable_before - usable_after).abs().le(tolerance)

        # Assigned weakest first, because the last write wins and each line
        # below describes a stronger claim than the one above it. The three
        # stated-row cases come last and are mutually exclusive among
        # themselves, so their order relative to each other does not matter.
        status = pd.Series("UNAVAILABLE", index=offset.index, dtype=object)
        status[usable_before.notna() & usable_after.isna()] = "FORWARD_DERIVED"
        status[usable_after.notna() & usable_before.isna()] = "BACKWARD_DERIVED"
        status[bracketed & closes] = "DERIVED"
        status[bracketed & ~closes] = "CONTRADICTED"
        status[anchored & confirmed.reindex(offset.index, fill_value=False)] = (
            "OBSERVED"
        )
        status[anchored & ~tested.reindex(offset.index, fill_value=False)] = (
            "UNVERIFIED"
        )
        status[disproved] = "CONTRADICTED"

        value = pd.Series(pd.NA, index=offset.index, dtype="Float64")
        told = status.isin(["OBSERVED", "UNVERIFIED"])
        value[told] = stated[told]
        counted_forward = status.isin(["DERIVED", "FORWARD_DERIVED"])
        value[counted_forward] = forward[counted_forward]
        counted_back = status.eq("BACKWARD_DERIVED")
        value[counted_back] = backward[counted_back]
        # A contradicted row keeps the source's own figure where the source
        # gave one -- a stated balance is an observation even when the rows
        # around it refuse to agree with it, and replacing it with a
        # reconstruction would discard the only direct evidence there is. Only
        # where the source said nothing does the forward reconstruction stand
        # in, and the discrepancy beside it says what that choice cost.
        broken = status.eq("CONTRADICTED")
        value[broken] = stated[broken].fillna(forward[broken])

        discrepancy = pd.Series(pd.NA, index=offset.index, dtype="Float64")
        discrepancy[broken] = (backward - forward)[broken].round(2)
        return status, value, discrepancy

    def metrics(self, df: pd.DataFrame):
        """
        Reads the state counts, the breaks left in the published series, which
        mover was found to be in force where, and, when the arithmetic rejects
        part of the file, where that part begins.

        The regime boundary is reported rather than configured. It is the
        finding -- "the source changed behaviour here" -- and a finding
        belongs in the report, where someone can act on it upstream, not in a
        constant that quietly encodes it as normal.
        """
        if STATUS not in df.columns:
            return

        amount = self.config.get("amount_col", "TXN_AMOUNT_CLEANED")
        order = self.config.get("sequence_col", "TXN_SEQ")
        # An unreadable amount is a hole in the running total. Read back off
        # the cleaned column, which no step after this one writes to, so the
        # answer is the same one the arithmetic worked from.
        yield (
            "amount.unreadable",
            audit.rows(pd.to_numeric(df[amount], errors="coerce").isna()),
        )

        status = df[STATUS].astype(str)
        for state in STATUSES:
            yield f"status[{state}]", audit.rows(status.eq(state))

        # What the detector found. One label over the whole file means one
        # convention; the seq is where the second one starts.
        basis = df[BASIS].astype(str)
        for state in BASES:
            yield f"basis[{state}]", audit.rows(basis.eq(state))
        billing = basis.eq(BILLING)
        if billing.any() and not billing.all():
            yield (
                "basis.first_billing_seq",
                int(df.loc[billing, order].astype("int64").min()),
            )

        # The denomination the published balances came out in, and how many of
        # them the USD valuation could actually be computed for -- a missing
        # rate on a non-USD row is the one way a published balance can fail to
        # normalize.
        yield "currency.stated", audit.rows(df[CURRENCY].notna())
        yield (
            "normalized.unavailable",
            audit.rows(df[FILLED].notna() & df[NORMALIZED].isna()),
        )

        # The invariant, counted rather than asserted, so that a run which
        # breaks it says so in its own report instead of only in a test.
        yield "balance.stated", audit.rows(df[FILLED].notna())
        yield "balance.unavailable", audit.rows(df[FILLED].isna())
        yield "balance.proven", audit.rows(status.isin(PROVEN))

        # How wrong the contradicted rows could be is not summarised here.
        # It is per row, in RUNNING_BALANCE_DISCREPANCY, because one file can
        # hold both a cent of rounding and a nine-figure hole and a single
        # aggregate over the two answers nothing. What belongs in a report is
        # how many rows are in that state and where they start.
        broken = status.eq("CONTRADICTED")

        yield "chain.breaks", audit.rows(df[CHAIN_BREAK])

        if broken.any():
            yield (
                "contradicted.first_seq",
                int(df.loc[broken, order].astype("int64").min()),
            )
            yield (
                "contradicted.accounts",
                audit.distinct(df.loc[broken, "ACCOUNT_ID"])
                if "ACCOUNT_ID" in df.columns else 0,
            )
