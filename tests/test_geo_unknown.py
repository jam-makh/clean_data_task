"""Absent location data says UNKNOWN; a marker says what it is instead."""

import pandas as pd
import pytest

from src.cleaners.geo import CityNormalizer
from src.rules import loader
from src.utils.report import CleaningReport


def run(cities, countries=None):
    """:returns: The frame with the geo columns added."""
    frame = pd.DataFrame(
        {
            "MERCHANT_CITY": cities,
            "MERCHANT_COUNTRY": countries or [""] * len(cities),
        }
    )
    return CityNormalizer(CleaningReport()).apply(frame)


def test_a_missing_city_says_unknown_rather_than_going_blank():
    """
    46550 rows of the forecast source have no city. A blank cell reads as an
    oversight; UNKNOWN reads as checked and genuinely absent.
    """
    out = run(["", None, "BEIRUT"])
    assert list(out["MERCHANT_CITY_CLEANED"]) == ["UNKNOWN", "UNKNOWN", "BEIRUT"]
    assert list(out["LOCATION_TYPE"]) == ["UNKNOWN", "UNKNOWN", "PHYSICAL"]


def test_a_missing_city_is_never_guessed_from_the_country():
    """
    One country holds many cities, so the inference does not run that way.
    The country is kept, the city stays UNKNOWN.
    """
    out = run([""], ["LB"])
    assert out["MERCHANT_CITY_CLEANED"].iat[0] == "UNKNOWN"
    assert out["MERCHANT_COUNTRY_CLEANED"].iat[0] == "LB"


def test_an_internal_marker_is_not_a_missing_city():
    """
    INTERNAL covers 24614 rows of settlement, transfer, salary and interest
    with no purchases at all. It states that there was no merchant location,
    which is not the same as failing to record one.
    """
    out = run(["INTERNAL"])
    assert out["LOCATION_TYPE"].iat[0] == "INTERNAL"
    assert out["MERCHANT_CITY_CLEANED"].iat[0] == "INTERNAL"
    # No geography, so no expectation -- and no expectation must never read
    # as a mismatch.
    assert out["MERCHANT_COUNTRY_EXPECTED"].iat[0] == ""


def test_neither_marker_nor_unknown_claims_a_country():
    out = run(["INTERNAL", "ECOM", ""])
    assert list(out["MERCHANT_COUNTRY_EXPECTED"]) == ["", "", ""]


# --- country inferred from city --------------------------------------------

def test_the_country_is_inferred_from_the_city():
    """A city sits in exactly one country, so the city settles it."""
    out = run(["BEIRUT", "DUBAI", "PARIS", "TALLINN"])
    assert list(out["MERCHANT_COUNTRY_EXPECTED"]) == ["LB", "AE", "FR", "EE"]


def test_the_city_outranks_a_wrong_stated_country():
    """
    The stated country is 85.8% right in the forecast source and BEIRUT
    appears against all twelve codes. Where the two disagree the city wins,
    and 30982 rows are corrected this way.
    """
    out = run(["BEIRUT"], ["TR"])
    assert out["MERCHANT_COUNTRY_CLEANED"].iat[0] == "LB"


def test_the_stated_country_stands_when_the_city_cannot_settle_it():
    """Correcting is not the same as discarding."""
    out = run(["", "INTERNAL"], ["LB", "AE"])
    assert list(out["MERCHANT_COUNTRY_CLEANED"]) == ["LB", "AE"]


def test_both_absent_is_unknown_not_blank():
    out = run([""], [""])
    assert out["MERCHANT_COUNTRY_CLEANED"].iat[0] == "UNKNOWN"


def test_transliterations_reach_the_same_country():
    """BEYROUTH and BEIRUT are one city and must not resolve differently."""
    out = run(["BEYROUTH", "BEYRUT", "BEIRUT LB", "BEIRUT"])
    assert set(out["MERCHANT_COUNTRY_EXPECTED"]) == {"LB"}


# --- the reference itself ---------------------------------------------------

def test_every_city_in_the_source_resolves_to_a_country(forecast):
    """
    39950 rows used to carry a city the reference did not know. A gap here
    means a city was added to the data without being added to the reference.
    """
    out = CityNormalizer(CleaningReport()).apply(forecast)
    stranded = out[
        out["LOCATION_TYPE"].eq("PHYSICAL")
        & out["MERCHANT_COUNTRY_EXPECTED"].eq("")
    ]
    assert stranded.empty, sorted(set(stranded["MERCHANT_CITY_CLEANED"]))


def test_no_marker_is_also_listed_as_a_place():
    """
    A value cannot be both a city with a country and a statement that there
    was no location.
    """
    markers = set(loader.non_geographic_cities())
    assert not markers & set(loader.city_countries())
