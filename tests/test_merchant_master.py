"""Resolution beyond exact spelling, and what is not a merchant at all."""

import pytest

from src.cleaners.merchant import INTERNAL, MERCHANT, UNIDENTIFIED, MerchantCleaner
from src.rules import loader
from src.utils.report import CleaningReport

PROCESSORS = loader.processors()


def kinds(names):
    """:returns: The frame a MerchantCleaner produces for these raw names."""
    import pandas as pd

    frame = pd.DataFrame({"MERCHANT_NAME": names})
    return MerchantCleaner(CleaningReport()).apply(frame)


# --- the same merchant written two ways ------------------------------------

@pytest.mark.parametrize(
    "fused,spaced",
    [
        ("BLOMBANK", "BLOM BANK"),
        ("BANKAUDI", "BANK AUDI"),
        ("HOTELDIEU", "HOTEL DIEU"),
        ("METROCASHCARRY", "METRO CASH CARRY"),
        ("LULUHYPERMARKET", "LULU HYPERMARKET"),
    ],
)
def test_a_fused_spelling_is_the_same_merchant(fused, spaced):
    """
    The source writes every merchant both with and without spaces. BLOMBANK
    is not a second bank, and treating it as one split 393 merchants into
    twice that many.
    """
    out = kinds([fused, spaced])
    assert out["MERCHANT_NAME_CLEANED"].nunique() == 1
    assert out["MERCHANT_RECOGNISED"].all()


# --- truncation -------------------------------------------------------------

@pytest.mark.parametrize(
    "truncated",
    [
        "AMERICAN UNIVERSITY BEIRU",
        "AMERICAN UNIVERSITY BEIR",
        "AMERICANUNIVERSITYBE",
    ],
)
def test_a_unique_prefix_resolves_to_the_one_merchant_it_can_be(truncated):
    """
    The source truncates names to a field width, leaving one merchant as nine
    lengths of itself. A prefix that only one merchant extends identifies it.
    """
    out = kinds([truncated])
    assert out["MERCHANT_RECOGNISED"].iat[0]


def test_an_ambiguous_prefix_resolves_to_nothing():
    """
    Two candidates means the evidence does not identify one. Picking whichever
    sorted first is exactly what the review queue exists to prevent.
    """
    resolve = MerchantCleaner._resolver({"CARREFOUR MAROC": "CARREFOUR MAROC",
                                         "CARREFOUR LEBANON": "CARREFOUR LEBANON"})
    assert resolve("CARREFOUR") == ""


def test_a_short_prefix_is_never_enough():
    """'A' prefixes half the master; a prefix has to actually identify."""
    resolve = MerchantCleaner._resolver(loader.merchant_aliases())
    assert resolve("AM") == ""
    assert resolve("CAR") == ""


def test_a_never_merge_member_is_out_of_reach_of_the_prefix_pass():
    """
    WTRS would otherwise reach WAITROSE by prefix alone, which is the merge
    trap_pairs.json exists to forbid.
    """
    resolve = MerchantCleaner._resolver(loader.merchant_aliases())
    for group in loader.trap_pairs():
        for member in group["members"]:
            canonical = member["canonical"]
            stub = canonical.replace(" ", "")[:8]
            resolved = resolve(stub)
            assert resolved in ("", canonical), (stub, resolved)


# --- what is not a merchant -------------------------------------------------

@pytest.mark.parametrize(
    "descriptor",
    ["CARD SETTLEMENT", "CARDSETTLEMENT", "STANDING ORDER SAVINGS",
     "TRANSFER TO CURRENT", "SWEEP FROM CURRENT", "CARD PAYMENT"],
)
def test_an_internal_movement_is_not_a_merchant(descriptor):
    """
    41293 rows describe money moving inside the bank. Counted as a merchant,
    CARD SETTLEMENT is the largest one in the file by a factor of seven.
    """
    out = kinds([descriptor])
    assert out["MERCHANT_KIND"].iat[0] == INTERNAL
    assert out["INTERNAL_MOVEMENT"].iat[0]


@pytest.mark.parametrize(
    "descriptor,label",
    [
        ("CARD SETTLEMENT", "CARD SETTLEMENT"),
        ("CARDSETTLEMENT", "CARD SETTLEMENT"),
        ("CARD PAYMENT", "CARD SETTLEMENT"),
        ("STANDING ORDER SAVINGS", "STANDING ORDER"),
        ("TRANSFER TO CURRENT", "INTERNAL TRANSFER"),
        ("SWEEP FROM CURRENT", "INTERNAL TRANSFER"),
    ],
)
def test_an_internal_movement_is_named_in_words(descriptor, label):
    """
    The name written is the movement, spelled the way a reader reads it. The
    kind token behind it is a key -- STANDING_ORDER, with the underscore the
    key needed -- and a key has no business in a column that otherwise holds
    CARREFOUR and SPINNEYS.
    """
    out = kinds([descriptor])
    assert out["MERCHANT_NAME_CLEANED"].iat[0] == label


def test_every_movement_label_is_words_not_a_key():
    """
    Guards the rule rather than the three current entries: a label added to
    internal_descriptors.json as a bare kind token would go straight onto the
    sheet.
    """
    for kind, label in loader.internal_movement_labels().items():
        assert "_" not in label, (kind, label)
        assert label == label.upper(), (kind, label)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("CARD SETTLEMENT", INTERNAL),
        ("SWEEP TO SAVINGS", INTERNAL),
        ("CARREFOUR", MERCHANT),
        ("ZZQQ WIDGETS BEIRUT", MERCHANT),
    ],
)
def test_merchant_type_asks_only_whether_there_is_a_counterparty(
    name, expected
):
    """
    Two states, not three. A name nobody has resolved yet is still a
    counterparty -- how far resolving it got is MATCHES_STATUS's question,
    and answering it twice in two vocabularies is what the working columns
    exist to avoid.
    """
    out = kinds([name])
    assert out["MERCHANT_TYPE"].iat[0] == expected


def test_an_employer_is_a_merchant_not_a_movement():
    """
    Separated by PROCESSING_TYPE, not by looks: these are SALARY_CREDIT and
    are real counterparties, however much they resemble the descriptors.
    """
    out = kinds(["INDEVCO", "MUREX", "AUB PAYROLL"])
    assert set(out["MERCHANT_KIND"]) == {MERCHANT}


def test_an_unknown_name_is_neither_and_is_queued():
    cleaner = MerchantCleaner(CleaningReport())
    import pandas as pd

    out = cleaner.apply(pd.DataFrame({"MERCHANT_NAME": ["ZZQQ WIDGETS BEIRUT"]}))
    assert out["MERCHANT_KIND"].iat[0] == UNIDENTIFIED
    assert len(cleaner.review_queue()) == 1


def test_the_review_queue_holds_no_internal_movements():
    """
    They are unrecognised as merchants because they are not merchants, which
    is a decision already taken, not a question for a reviewer.
    """
    cleaner = MerchantCleaner(CleaningReport())
    import pandas as pd

    cleaner.apply(pd.DataFrame({
        "MERCHANT_NAME": ["CARD SETTLEMENT", "ZZQQ WIDGETS BEIRUT"]
    }))
    queue = cleaner.review_queue()
    assert list(queue["MERCHANT_NAME_CLEANED"]) == ["ZZQQ WIDGETS BEIRUT"]


# --- the master itself ------------------------------------------------------

def test_no_descriptor_leaked_into_the_merchant_master():
    """The two files must not both claim the same name."""
    descriptors = set(loader.internal_descriptors())
    despaced = {k.replace(" ", "") for k in loader.merchant_aliases()}
    assert not descriptors & despaced


def test_truncated_card_pmt_suffixes_are_stripped():
    """
    The -CARD PMT- suffix is itself truncated, so each length survived as its
    own spelling.
    """
    for raw in ["H AND M CARD", "H AND M CARD P", "H AND M CARD PM",
                "H AND M -CARD PMT-", "H AND M CARDPMT"]:
        cleaned = MerchantCleaner.clean_one(raw, PROCESSORS)[0]
        assert loader.merchant_aliases().get(cleaned) == "H AND M", raw


def test_a_short_dashed_suffix_is_still_stripped():
    """
    Cut to 'C', 'CA' or 'CAR' the suffix keeps its dash, which is the only
    thing left identifying it.
    """
    for raw in ["H AND M -C", "H AND M -CA", "H AND M -CAR"]:
        cleaned = MerchantCleaner.clean_one(raw, PROCESSORS)[0]
        assert loader.merchant_aliases().get(cleaned) == "H AND M", raw


def test_a_truncated_name_is_not_read_as_a_truncated_suffix():
    """
    The field width cuts merchant names at the same lengths it cuts the
    suffix, and a dashless tail is always the name. Reading it as the suffix
    deleted the real last word: METRO CASH CARRY became METRO CASH, and seven
    short names were left too small for the prefix pass to recover.
    """
    cases = {
        "MADE C": "MADE COM",
        "ABC C": "ABC CONSULTING",
        "Costa C": "COSTA COFFEE",
        "Grand Ca": "GRAND CINEMAS ABC",
        "SAMS C": "SAMS CLUB",
        "SNCF C": "SNCF CONNECT",
        "MAYO C": "MAYO CLINIC",
        "TRM:69628 METRO CASH CAR": "METRO CASH CARRY",
        "TRM:59728 PHARMACIE C": "PHARMACIE CENTRALE",
        "TRM:36335 AUB MEDICAL C": "AUB MEDICAL CENTER",
    }
    resolve = MerchantCleaner._resolver(loader.merchant_aliases())
    for raw, canonical in cases.items():
        cleaned = MerchantCleaner.clean_one(raw, PROCESSORS)[0]
        assert resolve(cleaned) == canonical, f"{raw} -> {cleaned}"


def test_the_source_leaves_no_unrecognised_merchant(transactions):
    """
    The master is meant to cover this file completely, and a name the
    cleaners mangled on the way in looks exactly like a name nobody has
    added yet. Both are failures here.
    """
    out = MerchantCleaner(CleaningReport()).apply(transactions)
    missing = sorted(
        set(out.loc[~out["MERCHANT_RECOGNISED"], "MERCHANT_NAME_CLEANED"])
    )
    assert not missing, missing
