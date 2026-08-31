"""
MCC resolution: curated overrides, rules, and a review queue.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

import math
from collections import Counter


from src.rules import loader

# Three states, because three is what a reader can act on: the code is settled,
# it is inferred, or a human still has to decide. Which rule produced a settled
# code is recorded in the review sheet and in merchants.json, so collapsing the
# tiers here costs no provenance.
HIGH, MEDIUM, PENDING = "HIGH", "MEDIUM", "PENDING"
CONFIDENCE_ORDER = [HIGH, MEDIUM, PENDING]

# Which rule decided this row's MCC: a curated assertion, the ATM rule, the
# catch-all override, the suspect-code tiebreak, or the majority vote.
SIGNAL = "MCC_SIGNAL"


class MccResolver:
    """
    Assigns an MCC suggestion and a confidence tier without ever overwriting
    ``MCC_CODE``.

    Signals are applied in priority order: a curated override, then the
    deterministic ATM rule, then the catch-all override, then a tiebreak
    against suspect codes, then majority vote scored by a binomial tail.
    """

    name = "mcc"


    @staticmethod
    def _resolve(observed, counts, catch_all, suspect, thresholds) -> dict:
        """
        Applies the automated signals to one conflicting merchant.

        :returns: The decision dict for that merchant.
        """
        (top, top_n), *rest = counts.most_common()
        second_n = rest[0][1] if rest else 0
        n = sum(counts.values())
        p_value = MccResolver._binomial_tail(n, len(counts), top_n)

        specific = {c: k for c, k in counts.items() if c != catch_all}

        # A catch-all carries no positive information, so it can never win
        # against a specific code -- this deliberately overrides the majority.
        # "Win" includes drawing: a tie the catch-all is part of is a tie only
        # because a code meaning "something else" was counted as evidence, and
        # sending that to a human to arbitrate asks them to weigh nothing
        # against something.
        if specific and counts.get(catch_all, 0) >= max(specific.values()):
            # Highest count, and on a tie the lowest code. The tiebreak is
            # stated rather than left to `max`, which returns the first
            # maximal element in iteration order -- and a Counter's iteration
            # order is the order the codes first appear in the FILE. That made
            # the winner a property of how the extract happened to be sorted:
            # re-sort the source and the same merchant resolves to a different
            # category, silently. It also cannot be ported, because a Spark
            # frame has no file order to appeal to after a shuffle.
            #
            # Lowest code is arbitrary but it is *stated*, which is the whole
            # point. On this source it changes one merchant (JUMIA, 5812 and
            # 5411 tied at 3 apiece) and 88 rows. Preferring a non-suspect code
            # here -- the rule the top-count tie a few lines below already
            # uses -- would be the more principled tiebreak and reaches the
            # same answer on this data; it is a change to what the pipeline
            # decides rather than to how reproducibly it decides it, so it is
            # not made here.
            best = min(specific, key=lambda code: (-specific[code], code))
            return {
                "mcc": best,
                "confidence": MEDIUM,
                "signal": "catch_all_override",
                "observed": observed,
                "p_value": p_value,
            }

        if top_n == second_n:
            tied = {c for c, k in counts.items() if k == top_n}
            non_suspect = tied - suspect
            if len(non_suspect) == 1:
                return {
                    "mcc": next(iter(non_suspect)),
                    "confidence": MEDIUM,
                    "signal": "suspect_tiebreak",
                    "observed": observed,
                    "p_value": p_value,
                }
            return {
                "mcc": None,
                "confidence": PENDING,
                "signal": "unresolved_tie",
                "observed": observed,
                "p_value": p_value,
            }

        if p_value <= thresholds["binomial_high"]:
            tier = HIGH
        elif p_value <= thresholds["binomial_medium"]:
            tier = MEDIUM
        else:
            tier = PENDING
        return {
            "mcc": top if tier != PENDING else None,
            "confidence": tier,
            "signal": "weak_majority" if tier == PENDING else "majority",
            "observed": observed,
            "p_value": p_value,
        }

    # Terms below this fraction of the sum so far cannot move it at double
    # precision, and the tail is monotone by the time the loop checks, so the
    # rest of it cannot either. This is what keeps the cost proportional to
    # the width of the distribution rather than to the row count: the forecast
    # file gives merchants with n in the hundreds of thousands, and summing
    # every term there is both unnecessary and, at math.comb(265000, i), not
    # representable.
    _TAIL_EPS = 1e-18

    @staticmethod
    def _binomial_tail(n: int, k: int, hits: int) -> float:
        """
        Probability of seeing at least ``hits`` of one code in ``n`` rows if
        all ``k`` observed codes were equally likely — a small value means the
        majority is unlikely to be an accident.

        Summed in log space against the largest term rather than term by term.
        The direct form overflows once n reaches a few thousand -- math.comb
        returns an exact int of thousands of digits and multiplying it by a
        float raises OverflowError -- which put every merchant in the forecast
        file out of reach. Working from the ratio between neighbouring terms
        keeps every intermediate near 1 no matter how large n is.

        :returns: The upper-tail probability.
        """
        if hits > n:
            return 0.0
        if hits <= 0:
            return 1.0
        if k <= 1:
            # One observed code, so every row carries it: seeing at least
            # `hits` of it is certain, and 1 - p would be a division by zero.
            return 1.0

        p = 1 / k
        if hits > n * p:
            # Above the mean the terms fall away from i = hits, so the sum can
            # start there and stop as soon as they stop mattering.
            return MccResolver._tail_sum(n, p, hits, n, +1)
        # At or below the mean they fall the other way, and starting at hits
        # would climb to the peak before descending -- the accumulator would
        # overflow before the answer appeared. The lower tail descends from
        # hits - 1 instead, and the answer is what it leaves behind. Precision
        # is lost in the subtraction, but only in the range where the result
        # is close to 1 and no threshold is anywhere near it.
        return 1.0 - MccResolver._tail_sum(n, p, hits - 1, 0, -1)

    @staticmethod
    def _tail_sum(n: int, p: float, start: int, stop: int, step: int) -> float:
        """
        Sums the binomial PMF from ``start`` to ``stop`` inclusive, walking in
        the direction ``step``, which must be the downhill one.

        Every term is carried as a multiple of the one at ``start``, so the
        accumulator stays close to 1 and only the single starting term needs a
        logarithm.

        :returns: The summed probability.
        """
        log_first = (
            math.lgamma(n + 1)
            - math.lgamma(start + 1)
            - math.lgamma(n - start + 1)
            + start * math.log(p)
            + (n - start) * math.log1p(-p)
        )

        odds = p / (1 - p)
        relative, term = 1.0, 1.0
        i = start
        while i != stop:
            if step > 0:
                term *= (n - i) / (i + 1) * odds
            else:
                term *= i / (n - i + 1) / odds
            relative += term
            if term < relative * MccResolver._TAIL_EPS:
                break
            i += step

        total = log_first + math.log(relative)
        # Underflow rather than an error: a tail this small is zero at every
        # threshold that reads it, and math.exp would raise instead.
        return math.exp(total) if total > -745.0 else 0.0
