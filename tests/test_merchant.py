"""Merchant cleaning, especially the gated '*' split."""

import pytest

from src.cleaners.merchant import MerchantCleaner
from src.rules import loader
from src.utils.report import CleaningReport

PROCESSORS = loader.processors()


def clean(name):
    """:returns: (cleaned name, processor prefix)."""
    return MerchantCleaner.clean_one(name, PROCESSORS)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("SQ *TAKEALOT", "TAKEALOT"),
        ("PP*GRAMMARLY", "GRAMMARLY"),
        ("WPY*SAUDIA  AIRLINES", "SAUDIA AIRLINES"),
        ("IZ *BP FUEL", "BP FUEL"),
    ],
)
def test_processor_prefix_is_stripped(raw, expected):
    """
    Merchant is on the right only when the left token is a known processor.
    """
    assert clean(raw)[0] == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("COURSERA.COM *W2PA", "COURSERA"),
        ("XOOMTRANSFER.COM *1467", "XOOMTRANSFER"),
        ("MRCD LIBRE *BF8T", "MRCD LIBRE"),
    ],
)
def test_merchant_on_the_left_is_kept(raw, expected):
    """A blind split at '*' would destroy these 114 merchants."""
    assert clean(raw)[0] == expected


def test_prefix_is_reported_separately():
    assert clean("SQ *TAKEALOT")[1] == "SQ"
    assert clean("COURSERA.COM *W2PA")[1] == ""


@pytest.mark.parametrize(
    "raw,expected",
    [
        # A second '*' carries the acquirer reference. Codes with a digit were
        # already dropped as reference codes; the all-letter ones were not, and
        # survived as fake variants of a merchant we already knew.
        ("WPY*DEUTSCHE  BAHN *TNAS", "DEUTSCHE BAHN"),
        ("SQ *BINANCE *LRCW", "BINANCE"),
        ("SP *NOTION LABS *GTLD", "NOTION LABS"),
        ("SQ *DISNEY PLUS *BJHY", "DISNEY PLUS"),
    ],
)
def test_second_star_reference_code_is_stripped(raw, expected):
    assert clean(raw)[0] == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("AMERICAN UNIVERSITY BR", "AMERICAN UNIVERSITY"),
        ("MAKEUP STORE BEIRUT BR", "MAKEUP STORE BEIRUT"),
        # Numbered branches were already handled; bare BR was not.
        ("CREPAWAY BR 306", "CREPAWAY"),
    ],
)
def test_trailing_branch_marker_is_stripped(raw, expected):
    assert clean(raw)[0] == expected


def test_a_leading_number_is_part_of_the_name():
    """
    Store numbers and reference codes go; a leading numeral is neither, and
    dropping it silently renamed the merchant.
    """
    assert clean("7 ELEVEN")[0] == "7 ELEVEN"
    assert clean("REPSOL #70 *4GK9")[0] == "REPSOL"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("DUBAI MN /ufw", "DUBAI MN"),
        ("STRBCKS  /bww", "STRBCKS"),
        ("AWS CLOUD SVCS  22883", "AWS CLOUD SVCS"),
        ("sp *sncf connect", "SNCF CONNECT"),
    ],
)
def test_noise_is_removed(raw, expected):
    assert clean(raw)[0] == expected


def test_blank_input_survives():
    assert clean(None) == ("", "")
    assert clean("") == ("", "")


def test_nothing_becomes_empty_on_the_real_file(transactions, report):
    """Cleaning must never erase a merchant entirely."""
    df = MerchantCleaner(report).apply(transactions)
    assert (df["MERCHANT_NAME_CLEANED"] == "").sum() == 0


# --- the merchant master and its review queue ------------------------------

def test_aliases_collapse_to_one_canonical_merchant(transactions):
    """
    AWS arrives in four spellings. String cleaning gets them close but never
    equal, so without the master they group as four merchants.
    """
    cleaner = MerchantCleaner(CleaningReport())
    out = cleaner.apply(transactions)
    aws = out[out["MERCHANT_NAME"].astype(str).str.upper().str.contains("AWS")]
    assert set(aws["MERCHANT_NAME_CLEANED"]) == {"AWS CLOUD SERVICES"}
    assert aws["MERCHANT_RECOGNISED"].all()


def test_aws_stays_separate_from_amazon(transactions):
    """
    Same parent, different business -- merging them would erase the MCC
    signal.
    """
    cleaner = MerchantCleaner(CleaningReport())
    out = cleaner.apply(transactions)
    names = set(out["MERCHANT_NAME_CLEANED"])
    assert {"AWS CLOUD SERVICES", "AMAZON MARKETPLACE"} <= names


def test_unrecognised_merchant_is_flagged_and_queued(transactions):
    """
    A name absent from the master is never guessed at -- it goes to review.
    """
    probe = transactions.head(5).copy()
    probe.loc[probe.index[:2], "MERCHANT_NAME"] = "SQ *ZOOMBA FITNESS BEIRUT"

    cleaner = MerchantCleaner(CleaningReport())
    out = cleaner.apply(probe)

    unknown = out[~out["MERCHANT_RECOGNISED"]]
    assert set(unknown["MERCHANT_NAME_CLEANED"]) == {"ZOOMBA FITNESS BEIRUT"}

    queue = cleaner.review_queue()
    assert list(queue["MERCHANT_NAME_CLEANED"]) == ["ZOOMBA FITNESS BEIRUT"]
    assert queue["ROW_COUNT"].iat[0] == 2
    # The raw spelling is what a reviewer needs to recognise the merchant.
    assert "SQ *ZOOMBA FITNESS BEIRUT" in queue["RAW_SPELLINGS"].iat[0]


def test_every_observed_name_is_recognised(transactions):
    """
    The master is meant to cover this file completely. A gap here means a
    merchant was added to the data without being added to the master.
    """
    cleaner = MerchantCleaner(CleaningReport())
    out = cleaner.apply(transactions)
    missing = sorted(
        set(out.loc[~out["MERCHANT_RECOGNISED"], "MERCHANT_NAME_CLEANED"])
    )
    assert not missing, missing


# --- affixes the forecast source wraps around the name ---------------------

@pytest.mark.parametrize(
    "raw",
    [
        "ECOM/H AND M", "POS H AND M", "POSHANDM", "TRM:10372 H AND M",
        "TRM:87669HANDM", "H AND M/REF162491", "  H AND M -CARD PMT-",
        "HANDM-CARDPMT-", "ecom/handm", "h and m",
    ],
)
def test_channel_and_terminal_affixes_are_stripped(raw):
    """
    ECOM/ sits on 11.8% of PURCHASE rows and 11.8% of ATM_WITHDRAWAL rows, so
    it marks nothing about how the transaction was acquired -- it is noise
    wrapped around the name, and 279 spellings of this one merchant differ by
    little else.
    """
    cleaned = clean(raw)[0]
    assert loader.merchant_aliases().get(cleaned) == "H AND M"


def test_a_slash_prefix_does_not_eat_the_first_word():
    """
    REF_SUFFIX matched the slash inside a channel prefix and deleted the word
    after it, so ECOM/LULU HYPERMARKET became "ECOM HYPERMARKET". The space
    before a real reference code is what tells the two apart.
    """
    assert clean("ECOM/LULU HYPERMARKET")[0] == "LULU HYPERMARKET"
    assert clean("ECOM/NOON")[0] == "NOON"


def test_a_fused_terminal_id_does_not_swallow_the_merchant():
    """
    TRM:<digits> runs straight into the name, and the fused token read as one
    alphanumeric reference code -- which reduced 4150 rows to "TRM".
    """
    assert clean("TRM:31659ZAATARWZEIT")[0] == "ZAATARWZEIT"
    assert clean("TRM:11903 H AND M")[0] == "H AND M"


def test_a_reference_code_is_still_stripped():
    """The fix must not cost the behaviour it was narrowing."""
    assert clean("DUBAI MN /ufw")[0] == "DUBAI MN"
    assert clean("STRBCKS  /bww")[0] == "STRBCKS"


def test_no_name_is_reduced_to_an_affix_on_the_forecast_source(forecast):
    """6733 rows used to survive as the bare string "TRM" or "ECOM"."""
    names = forecast["MERCHANT_NAME"].dropna().unique()
    cleaned = {MerchantCleaner.clean_one(n, PROCESSORS)[0] for n in names}
    assert not cleaned & {"TRM", "POS", "ECOM", "REF", ""}
