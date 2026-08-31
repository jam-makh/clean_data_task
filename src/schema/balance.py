"""
Running balance: state a figure wherever the arithmetic reaches, and say how.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

import numpy as np


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
