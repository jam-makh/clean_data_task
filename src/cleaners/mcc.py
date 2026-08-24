"""MCC resolution: curated overrides, rules, and a review queue."""

import math
from collections import Counter

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.utils import audit

# Three states, because three is what a reader can act on: the code is settled,
# it is inferred, or a human still has to decide. Which rule produced a settled
# code is recorded in the review sheet and in merchants.json, so collapsing the
# tiers here costs no provenance.
HIGH, MEDIUM, PENDING = "HIGH", "MEDIUM", "PENDING"
CONFIDENCE_ORDER = [HIGH, MEDIUM, PENDING]

# Which rule decided this row's MCC: a curated assertion, the ATM rule, the
# catch-all override, the suspect-code tiebreak, or the majority vote.
SIGNAL = "MCC_SIGNAL"


class MccResolver(BaseCleaner):
    """
    Assigns an MCC suggestion and a confidence tier without ever overwriting
    ``MCC_CODE``.

    Signals are applied in priority order: a curated override, then the
    deterministic ATM rule, then the catch-all override, then a tiebreak
    against suspect codes, then majority vote scored by a binomial tail.
    """

    name = "mcc"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        needed = {"MCC_CODE_CLEANED", "MERCHANT_NAME_CLEANED"}
        if not needed.issubset(df.columns):
            return df

        df = df.copy()
        rules = loader.mcc_rules()
        # Only entries that actually assert an MCC are overrides; a master
        # entry carrying just aliases says nothing about categorisation.
        overrides = {
            name: entry
            for name, entry in loader.merchants().items()
            if "mcc" in entry
        }
        catch_all = rules["catch_all"]
        suspect = set(rules["suspect_codes"])
        thresholds = rules["confidence"]

        decisions = self._decide_per_merchant(
            df, overrides, catch_all, suspect, thresholds
        )
        self.decisions = decisions

        suggested, confidence, signal = [], [], []
        pairs = zip(df["MERCHANT_NAME_CLEANED"], df["MCC_CODE_CLEANED"])
        for merchant, current in pairs:
            decision = decisions.get(merchant)
            if decision is None or decision["mcc"] in (None, current):
                suggested.append("")
                confidence.append(
                    decision["confidence"] if decision else "NONE"
                )
                signal.append(decision["signal"] if decision else "")
            else:
                suggested.append(decision["mcc"])
                confidence.append(decision["confidence"])
                signal.append(decision["signal"])

        df["MCC_CODE_SUGGESTED"] = suggested
        df["MCC_CONFIDENCE"] = pd.Categorical(
            confidence, categories=CONFIDENCE_ORDER
        )
        # Which rule decided this row's code. It repeats across every row of a
        # merchant, which is why it is not on the presented sheet -- but it
        # stays on the frame, because the alternative is a total in the report
        # that no row can be traced back to. The review sheet still shows one
        # row per decision; this shows which decision reached this
        # transaction.
        df[SIGNAL] = signal

        df = self._apply_deterministic(df, rules)

        # One MCC column leaves the pipeline, holding the code that survived
        # validation. A suggestion only exists at HIGH or MEDIUM confidence --
        # PENDING resolves to no code at all -- so adopting it here never
        # promotes a guess. The code the file arrived with is still in
        # raw_transactions, and MCC_CONFIDENCE says how the final one was
        # reached.
        adopted = df["MCC_CODE_SUGGESTED"] != ""
        df.loc[adopted, "MCC_CODE_CLEANED"] = df.loc[
            adopted, "MCC_CODE_SUGGESTED"
        ]
        return df

    def metrics(self, df: pd.DataFrame):
        if "MCC_CONFIDENCE" not in df.columns:
            return

        # Split once for every deterministic rule rather than once per rule.
        tally = audit.flag_tally(df.get("VALIDATION_FLAGS", ""))
        for rule in loader.mcc_rules().get("deterministic", []):
            # A rule whose trigger column is absent never ran, and reporting
            # a zero for it would claim it did.
            if rule["when_column"] in df.columns:
                yield rule["flag"], tally.get(rule["flag"], 0)

        confidence = df["MCC_CONFIDENCE"].astype(str)
        for tier in CONFIDENCE_ORDER:
            count = audit.rows(confidence.eq(tier))
            if count:
                yield f"confidence[{tier}]", count

        # Reported so the provenance the three tiers no longer distinguish
        # stays measurable: how many rows rest on a human assertion rather
        # than on a heuristic.
        if SIGNAL in df.columns:
            named = df.loc[df[SIGNAL].ne(""), SIGNAL]
            for name, count in audit.ranked(named):
                yield f"signal[{name}]", count

        adopted = audit.rows(df["MCC_CODE_SUGGESTED"].ne(""))
        yield "rows_with_suggestion", adopted
        yield "mcc_code.reassigned", adopted

    def _decide_per_merchant(
        self, df, overrides, catch_all, suspect, thresholds
    ) -> dict[str, dict]:
        """
        :returns: Merchant key to {mcc, confidence, signal, observed, p_value}.
        """
        out: dict[str, dict] = {}
        for merchant, group in df.groupby("MERCHANT_NAME_CLEANED", sort=False):
            if not merchant:
                continue
            counts = Counter(group["MCC_CODE_CLEANED"])
            observed = dict(counts.most_common())

            # A human assertion is settled by definition.
            if merchant in overrides:
                out[merchant] = {
                    "mcc": overrides[merchant]["mcc"],
                    "confidence": HIGH,
                    "signal": "curated",
                    "observed": observed,
                    "p_value": None,
                }
                continue

            # One code across every row of this merchant: nothing disagrees,
            # so there is nothing to resolve and nothing to review.
            if len(counts) == 1:
                out[merchant] = {
                    "mcc": None,
                    "confidence": HIGH,
                    "signal": "consistent",
                    "observed": observed,
                    "p_value": None,
                }
                continue

            out[merchant] = self._resolve(
                observed, counts, catch_all, suspect, thresholds
            )
        return out

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
            best = max(specific, key=specific.get)
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

    def _apply_deterministic(
        self, df: pd.DataFrame, rules: dict
    ) -> pd.DataFrame:
        """
        Applies rules where another column independently fixes the MCC.

        :returns: The frame with deterministic overrides applied.
        """
        for rule in rules.get("deterministic", []):
            column = rule["when_column"]
            if column not in df.columns:
                continue
            target = df[column].map(self.text) == rule["when_value"]
            wrong = target & (df["MCC_CODE_CLEANED"] != rule["expect_mcc"])
            df.loc[wrong, "MCC_CODE_SUGGESTED"] = rule["expect_mcc"]
            df.loc[wrong, "MCC_CONFIDENCE"] = "HIGH"
            df.loc[wrong, SIGNAL] = "deterministic"

            # Flag per row, not only in the report -- a deterministic violation
            # has to be traceable to the transaction that caused it.
            if "VALIDATION_FLAGS" not in df.columns:
                df["VALIDATION_FLAGS"] = ""
            df.loc[wrong, "VALIDATION_FLAGS"] = (
                df.loc[wrong, "VALIDATION_FLAGS"]
                .replace("", pd.NA)
                .fillna(rule["flag"])
                .where(
                    lambda s: s == rule["flag"],
                    lambda s: s + ";" + rule["flag"],
                )
            )
        return df

    def review_queue(self) -> pd.DataFrame:
        """
        Builds the human work queue: one row per merchant whose MCC is still
        undecided. ``HIGH`` and ``MEDIUM`` are settled and do not appear —
        putting a resolved merchant in a work queue trains reviewers to skim
        it.

        :returns: Frame ready to write as the ``mcc_review`` sheet.
        """
        rows = []
        for merchant, d in getattr(self, "decisions", {}).items():
            if d["confidence"] != PENDING:
                continue
            rows.append(
                {
                    "MERCHANT_NAME_CLEANED": merchant,
                    "MCC_OBSERVED": str(d["observed"]),
                    "MCC_CODE_SUGGESTED": d["mcc"] or "",
                    "MCC_CONFIDENCE": d["confidence"],
                    "BINOMIAL_P": (
                        round(d["p_value"], 4) if d["p_value"] else None
                    ),
                    "SIGNAL": d["signal"],
                    "ROW_COUNT": sum(d["observed"].values()),
                }
            )
        return pd.DataFrame(rows).sort_values(
            ["MCC_CONFIDENCE", "ROW_COUNT"], ascending=[True, False]
        ) if rows else pd.DataFrame(
            columns=[
                "MERCHANT_NAME_CLEANED", "MCC_OBSERVED", "MCC_CODE_SUGGESTED",
                "MCC_CONFIDENCE", "BINOMIAL_P", "SIGNAL", "ROW_COUNT",
            ]
        )
