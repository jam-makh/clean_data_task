"""Code padding, MCC confidence tiers, and override hygiene."""

import pandas as pd

from src.cleaners.codes import CodeNormalizer
from src.cleaners.merchant import MerchantCleaner
from src.rules import loader
from src.cleaners.mcc import MccResolver


def test_leading_zeros_are_restored(report, tiny_frame):
    """An int column destroyed them; the ISO form is a fixed-width string."""
    df = CodeNormalizer(report).apply(tiny_frame)
    assert list(df["PROCESSING_CODE_CLEANED"]) == ["00", "20", "01"]
    assert list(df["MCC_CODE_CLEANED"]) == ["5411", "8220", "4111"]


def test_label_is_regenerated_from_the_code(report, tiny_frame):
    """
    A future file spelling the label differently still lands on one value.
    """
    tiny_frame = tiny_frame.assign(
        PROCESSING_TYPE=["purchase", "refund", "atm"]
    )
    df = CodeNormalizer(report).apply(tiny_frame)
    assert list(df["PROCESSING_TYPE_CLEANED"].astype(str)) == [
        "Purchase", "Purchase Return/Refund", "ATM Cash Withdrawal",
    ]


def test_unknown_mcc_is_reported(report, tiny_frame):
    step = CodeNormalizer(report)
    out = step.apply(tiny_frame, mcc_reference={"5411": "Grocery"})
    step.collect(out)
    assert ("codes", "mcc.not_in_reference", 2) in report.entries


def _score(counts, merchant="TEST MERCHANT"):
    """
    :param counts: MCC code to row count.
    :returns: The decision dict MccResolver reaches for that merchant.
    """
    rows = [{"MERCHANT_NAME_CLEANED": merchant, "MCC_CODE_CLEANED": c}
            for c, n in counts.items() for _ in range(n)]
    from src.utils.report import CleaningReport
    validator = MccResolver(CleaningReport())
    validator.apply(pd.DataFrame(rows))
    return validator.decisions[merchant]


def test_catch_all_never_beats_a_specific_code():
    """
    USJ BEIRUT is 5999:7 / 8220:3 -- majority vote alone returns the wrong
    answer.
    """
    decision = _score({"5999": 7, "8220": 3})
    assert decision["mcc"] == "8220"
    assert decision["signal"] == "catch_all_override"


def test_tie_between_suspect_and_specific_resolves_to_specific():
    """
    5812 is a real category, so it only loses head-to-head against another
    specific code.
    """
    decision = _score({"5812": 2, "5651": 2})
    assert decision["mcc"] == "5651"
    assert decision["signal"] == "suspect_tiebreak"


def test_tie_involving_the_catch_all_is_taken_by_the_earlier_rule():
    """
    5999 carries no meaning at all, so the catch-all rule settles it before
    the tiebreak.
    """
    decision = _score({"5999": 2, "5651": 2})
    assert decision["mcc"] == "5651"
    assert decision["signal"] == "catch_all_override"


def test_tie_between_two_specific_codes_is_left_unresolved():
    decision = _score({"5411": 2, "5732": 2})
    assert decision["mcc"] is None
    assert decision["confidence"] == "PENDING"


def test_strong_majority_scores_high():
    decision = _score({"4511": 7, "5651": 1})
    assert decision["confidence"] == "HIGH"
    assert decision["p_value"] <= 0.05


def test_weak_majority_is_pending_and_suggests_nothing():
    decision = _score({"4511": 2, "5651": 1})
    assert decision["confidence"] == "PENDING"
    assert decision["mcc"] is None


def test_single_code_merchant_is_not_a_conflict():
    """Nothing disagrees, so the code is settled and never enters the queue."""
    decision = _score({"5411": 5})
    assert decision["confidence"] == "HIGH"
    assert decision["signal"] == "consistent"


def test_curated_override_beats_every_signal():
    """PRET A MANGER is genuinely 5812 despite 5812 being noise elsewhere."""
    decision = _score({"5812": 8, "5999": 3}, merchant="PRET A MANGER")
    assert decision["confidence"] == "HIGH"
    assert decision["signal"] == "curated"
    assert decision["mcc"] == "5812"


def test_atm_rule_is_deterministic(report):
    """
    The reference labels 6011 'ATM Cash Withdrawal' -- the same string
    PROCESSING_TYPE uses.
    """
    df = pd.DataFrame(
        {
            "MERCHANT_NAME_CLEANED": ["SOME ATM", "SOME ATM"],
            "MCC_CODE_CLEANED": ["6011", "5999"],
            "PROCESSING_TYPE": ["ATM Cash Withdrawal", "ATM Cash Withdrawal"],
        }
    )
    out = MccResolver(report).apply(df)
    assert out["MCC_CODE_SUGGESTED"].iat[1] == "6011"
    assert "MCC_ATM_MISMATCH" in out["VALIDATION_FLAGS"].iat[1]


# --- merchant master hygiene -----------------------------------------------

def test_every_entry_has_provenance():
    """Without it, nobody can later tell a verified fact from a guess."""
    for name, entry in loader.merchants().items():
        assert entry.get("added_by"), name
        assert entry.get("added"), name


def test_every_asserted_mcc_exists_in_the_reference(mcc_reference):
    for name, entry in loader.merchants().items():
        if "mcc" in entry:
            assert entry["mcc"] in mcc_reference, name


def test_no_alias_is_claimed_by_two_merchants():
    """Otherwise which merchant an alias resolves to depends on dict order."""
    loader.merchant_aliases()  # raises ValueError on a collision


def test_no_master_entry_is_stale(transactions, forecast):
    """
    Keys match the cleaned merchant name, which shifts as MerchantCleaner
    improves. A dead entry must fail here rather than silently do nothing.

    Both sources count. The master serves the workbook and the forecast
    extract, and an entry the workbook happens not to use is not thereby dead.
    Liveness is asked of the resolver rather than of exact membership, because
    the resolver is how a name actually reaches an entry: the forecast source
    truncates, and HOTEL DIEU DE FRANCE is only ever reached by a prefix.
    """
    processors = loader.processors()
    resolve = MerchantCleaner._resolver(loader.merchant_aliases())
    live = {
        resolve(MerchantCleaner.clean_one(v, processors)[0])
        for source in (transactions, forecast)
        for v in source["MERCHANT_NAME"]
    }
    dead = sorted(set(loader.merchants()) - live)
    assert not dead, f"master entries matching no merchant: {dead}"


# --- the three-state confidence contract -----------------------------------

def test_confidence_has_exactly_three_states(transactions, mcc_reference):
    """
    Three is what a reader can act on: settled, inferred, or still a
    decision.
    """
    from main import clean_transactions

    cleaned, _ = clean_transactions(transactions, mcc_reference=mcc_reference)
    assert set(cleaned["MCC_CONFIDENCE"].astype(str)) <= {
        "HIGH", "MEDIUM", "PENDING",
    }


def test_only_pending_merchants_reach_the_review_queue(
    transactions,
    mcc_reference,
):
    """
    A settled merchant in a work queue trains reviewers to skim it, which is
    how the one row that needs a decision gets missed.
    """
    from src.pipeline import TransactionCleaner

    cleaner = TransactionCleaner(mcc_reference=mcc_reference)
    cleaner.run(transactions)
    queue = cleaner.step("mcc").review_queue()
    assert set(queue["MCC_CONFIDENCE"]) <= {"PENDING"}
    assert (
        queue["MCC_CODE_SUGGESTED"] == ""
    ).all(), "a PENDING merchant has no candidate"


def test_signal_is_carried_per_row_but_kept_off_the_sheet(
    transactions, mcc_reference
):
    """
    Which rule chose a row's MCC has to be answerable about that row.

    It used to be computed, counted, and dropped, which left the report
    claiming a total no transaction could be traced back to. It is a
    diagnostic column now: on the frame, so the count is derived from it and
    the database can persist it, and off the presented sheet, where it would
    only repeat one merchant's decision down every one of its rows.
    """
    from main import clean_transactions
    from src.utils.columns import presented

    cleaned, _ = clean_transactions(transactions, mcc_reference=mcc_reference)
    assert "MCC_SIGNAL" in cleaned.columns
    assert "MCC_SIGNAL" not in presented(cleaned).columns
