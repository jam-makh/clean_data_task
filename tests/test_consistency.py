"""Cross-field checks: the amount stated twice has to reconcile."""

import pandas as pd
import pytest

from src.rules import loader
from src.validators.consistency import (
    FX_REFERENCE_TOLERANCE,
    FX_TOLERANCE,
    ConsistencyValidator,
)


def flags(rows, report):
    """
    :param rows: (local amount, rate, billing amount) triples.
    :returns: The VALIDATION_FLAGS column after the validator has run.
    """
    df = pd.DataFrame(
        {
            "TXN_AMOUNT_CLEANED": [a for a, _, _ in rows],
            "FX_RATE": [r for _, r, _ in rows],
            "BILLING_AMOUNT": [b for _, _, b in rows],
        }
    )
    return ConsistencyValidator(report).apply(df)["VALIDATION_FLAGS"]


def test_a_reconciling_row_is_not_flagged(report):
    """11.20 EUR at 1.070281 is 11.99 USD, and the file says so."""
    assert flags([(-11.20, 1.070281, -11.99)], report).iat[0] == ""


def test_the_rate_multiplies_the_amount(report):
    """
    Dividing instead would make this row reconcile, which is how the wrong
    direction hides: on the USD rows, where the rate is 1, both agree.
    """
    assert flags([(-1193.50, 0.268572, -320.54)], report).iat[0] == ""
    off_by_the_inverse = flags([(-1193.50, 0.268572, -4443.86)], report)
    assert "FX_RECONCILE_MISMATCH" in off_by_the_inverse.iat[0]


def test_a_billing_amount_off_by_a_factor_of_a_hundred_is_flagged(report):
    result = flags([(-154.77, 1.0, -1.55)], report)
    assert "FX_RECONCILE_MISMATCH" in result.iat[0]


def test_signs_are_compared_as_magnitudes(report):
    """
    The local amount is signed by transaction type and the billing amount by
    its own convention. Comparing them signed would flag every refund.
    """
    assert flags([(172.22, 1.0, 172.22)], report).iat[0] == ""


@pytest.mark.parametrize(
    "drift,flagged",
    [(FX_TOLERANCE / 2, False), (FX_TOLERANCE * 2, True)],
)
def test_rate_rounding_stays_inside_the_tolerance(drift, flagged, report):
    """
    FX_RATE is stored to six decimals, so a large amount reconciles to within
    a rounding error rather than exactly. That is not a contradiction.
    """
    result = flags([(-1000.0, 1.0, -1000.0 * (1 + drift))], report)
    assert ("FX_RECONCILE_MISMATCH" in result.iat[0]) is flagged


def test_a_missing_rate_is_a_gap_not_a_contradiction(report):
    """Nothing to reconcile against cannot be evidence of disagreement."""
    assert flags([(-50.0, None, -50.0)], report).iat[0] == ""


def rate_flags(rows, report):
    """
    :param rows: (currency, stated rate) pairs.
    :returns: The VALIDATION_FLAGS column after the validator has run.
    """
    df = pd.DataFrame(
        {
            "TXN_CCY": [c for c, _ in rows],
            "FX_RATE": [r for _, r in rows],
        }
    )
    return ConsistencyValidator(report).apply(df)["VALIDATION_FLAGS"]


def test_a_plausible_rate_passes(report):
    reference = loader.fx_rates()
    assert rate_flags([("EUR", reference["EUR"])], report).iat[0] == ""


def test_ordinary_movement_is_not_a_defect(report):
    """
    A rate from a different day is not a wrong rate. Every floating currency
    in the file stays within 4.3% of its own median across seven months.
    """
    reference = loader.fx_rates()
    drifted = reference["GBP"] * (1 + FX_REFERENCE_TOLERANCE / 2)
    assert rate_flags([("GBP", drifted)], report).iat[0] == ""


def test_a_dead_peg_is_caught(report):
    """
    The check exists for this row: 1507.5 LBP to the dollar was the official
    peg long after it stopped describing anything. Reconciliation cannot see
    it -- a stale rate and a billing amount computed from that stale rate
    agree with each other perfectly.
    """
    result = rate_flags([("LBP", 1 / 1507.5)], report)
    assert "FX_RATE_OFF_REFERENCE" in result.iat[0]


def test_an_unknown_currency_is_not_judged(report):
    """No reference means no opinion, not a violation."""
    assert rate_flags([("XYZ", 1.5)], report).iat[0] == ""


def test_every_currency_in_the_file_has_a_reference(transactions):
    """
    A missing entry would silently exempt a whole currency from the check.
    """
    reference = loader.fx_rates()
    present = {str(c).strip().upper() for c in transactions["TXN_CCY"]}
    assert present <= set(reference), present - set(reference)
