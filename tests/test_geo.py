"""City normalization and the city/country agreement check."""

import pandas as pd
import pytest

from src.cleaners.geo import CityNormalizer
from src.rules import loader
from src.utils.report import CleaningReport
from src.validators.consistency import ConsistencyValidator

ALIASES, ECOMMERCE = loader.city_aliases()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("BEYROUTH", "BEIRUT"),
        ("BEYRUT", "BEIRUT"),
        ("BEIRUT LB", "BEIRUT"),
        # Both transliterations of Ashrafieh; the second was missing from the
        # alias map and split the district into two cities.
        ("ASHRAFIYA", "ACHRAFIEH"),
        ("ASHRAFIEH", "ACHRAFIEH"),
        # Byblos and Jbeil are the same city under Greek and Arabic names --
        # confirmed by the merchants that appear under both.
        ("JBEIL", "BYBLOS"),
        ("JBAIL", "BYBLOS"),
        ("AL HAMRA", "HAMRA"),
    ],
)
def test_city_variants_collapse(raw, expected):
    assert ALIASES.get(raw, raw) == expected


def frame(rows):
    """:returns: A minimal frame carrying just the geo columns."""
    return pd.DataFrame(rows, columns=["MERCHANT_CITY", "MERCHANT_COUNTRY"])


def normalise(rows):
    """:returns: The frame after city normalization."""
    return CityNormalizer(CleaningReport()).apply(frame(rows))


def test_city_implies_its_country():
    out = normalise([("PARIS", "FR"), ("BEYROUTH", "LB")])
    assert list(out["MERCHANT_COUNTRY_EXPECTED"]) == ["FR", "LB"]


def test_ecommerce_marker_implies_no_country():
    """
    INTERNET is a card-not-present marker, not a place. The country still
    describes the merchant, so the pair is not a contradiction and must not
    be flagged -- this is the case that looks like an error and is not.
    """
    out = normalise([("INTERNET", "LB"), ("ECOM", "US"), ("", "GB")])
    assert list(out["MERCHANT_COUNTRY_EXPECTED"]) == ["", "", ""]

    flagged = ConsistencyValidator(CleaningReport()).apply(out)
    assert not flagged["VALIDATION_FLAGS"].str.contains("GEO").any()


def test_mismatched_country_is_flagged():
    out = normalise([("PARIS", "GB"), ("PARIS", "FR")])
    flagged = ConsistencyValidator(CleaningReport()).apply(out)
    assert list(
        flagged["VALIDATION_FLAGS"].str.contains("GEO_CITY_COUNTRY_MISMATCH")
    ) == [
        True,
        False,
    ]


def test_unknown_city_is_never_flagged():
    """An absent city is not evidence of a mismatch, only of a gap."""
    out = normalise([("REYKJAVIK", "IS")])
    assert out["MERCHANT_COUNTRY_EXPECTED"].iat[0] == ""

    flagged = ConsistencyValidator(CleaningReport()).apply(out)
    assert flagged["VALIDATION_FLAGS"].iat[0] == ""


def test_country_is_never_overwritten():
    """Same discipline as MCC and the settle date: expose, never assert."""
    out = normalise([("PARIS", "GB")])
    assert out["MERCHANT_COUNTRY"].iat[0] == "GB"


def test_every_reference_city_is_a_canonical_name():
    """
    The reference is keyed on the cleaned city. An entry keyed on a spelling
    that CityNormalizer collapses away would silently never match.
    """
    stale = [
        city for city in loader.city_countries()
        if ALIASES.get(city, city) != city
    ]
    assert not stale, f"reference keyed on non-canonical cities: {stale}"


def test_mismatch_is_confined_to_the_country_column(
    transactions,
    mcc_reference,
):
    """
    The corruption is one-sided: on every flagged row the city is right and
    the country is wrong. If a future file breaks that, the fix is a different
    one and this test should fail rather than quietly mislead.
    """
    from main import clean_transactions

    cleaned, _ = clean_transactions(transactions, mcc_reference=mcc_reference)
    bad = cleaned[
        cleaned["VALIDATION_FLAGS"].str.contains("GEO_CITY_COUNTRY_MISMATCH")
    ]
    assert len(bad) == 148
    # Four injected values account for all of it, so this is noise in one
    # column rather than genuine geography we failed to model.
    assert set(bad["MERCHANT_COUNTRY"]) <= {"US", "TR", "GB", "IE", "SE"}
