"""Macro recovery: lookup, never imputation, and the difference between
a value that is missing and one that was never in the panel."""

import pandas as pd
import pytest

from src.cleaners.macro import MacroCleaner
from src.cleaners.timestamps import TimestampNormalizer
from src.rules import loader
from src.utils.report import CleaningReport


def run(frame):
    """:returns: The frame with timestamps normalised and macro recovered."""
    report = CleaningReport()
    return MacroCleaner(report).apply(
        TimestampNormalizer(report).apply(frame)
    )


def frame_of(dates, **columns):
    base = {
        "TXN_DATE_TIME": dates,
        "ACCOUNT_ID": ["A"] * len(dates),
        "TXN_SEQ": list(range(1, len(dates) + 1)),
        "MERCHANT_COUNTRY": ["LB"] * len(dates),
        "INTEREST_RATE_INDEX": [None] * len(dates),
        "INFLATION_INDEX": [None] * len(dates),
        "IS_HOLIDAY_MONTH": [None] * len(dates),
    }
    base.update(columns)
    return pd.DataFrame(base)


# --- the series are facts about a month, not about a transaction -----------

def test_the_rate_is_recovered_from_the_month_alone():
    """
    One value per year-month across the whole file, so the month names it
    exactly. A per-user mode would be wrong for every user spanning two
    months, and a median would invent a number no month ever had.
    """
    out = run(frame_of(["2022-01-15 10:00:00"]))
    assert out["INTEREST_RATE_INDEX_CLEANED"].iat[0] == 4.519
    assert out["INTEREST_RATE_INDEX_COVERAGE"].iat[0] == "RECOVERED"


def test_inflation_is_recovered_per_country_not_globally():
    """
    Keyed on the merchant country: LB and DE run wildly different series in
    the same month, so a single global figure would be wrong for both.
    """
    out = run(
        frame_of(
            ["2022-06-15 10:00:00"] * 2, MERCHANT_COUNTRY=["LB", "DE"]
        )
    )
    values = list(out["INFLATION_INDEX_CLEANED"])
    assert values[0] != values[1]
    assert values[0] == pytest.approx(19.49)
    assert values[1] == pytest.approx(1.17)


def test_the_holiday_flag_follows_the_country_calendar():
    """
    A fixed (country, calendar-month) rule. December is a holiday month in
    LB and not in EE, in every year.
    """
    out = run(
        frame_of(
            ["2022-12-15 10:00:00"] * 2, MERCHANT_COUNTRY=["LB", "EE"]
        )
    )
    assert list(out["IS_HOLIDAY_MONTH_CLEANED"]) == [True, False]


def test_an_observed_value_is_left_alone():
    """Recovery fills gaps; it does not overwrite what the source stated."""
    out = run(
        frame_of(["2022-01-15 10:00:00"], INTEREST_RATE_INDEX=["9.999"])
    )
    assert out["INTEREST_RATE_INDEX_CLEANED"].iat[0] == 9.999
    assert out["INTEREST_RATE_INDEX_COVERAGE"].iat[0] == "OBSERVED"


# --- absent, not applicable, and unrecoverable are three different things ---

def test_a_country_outside_the_panel_is_not_a_gap_to_fill():
    """
    Six of the twelve countries carry no inflation figure in any month. Those
    17873 nulls are the absence of a series, not the loss of a value --
    imputing them would fabricate an economy.
    """
    out = run(
        frame_of(["2022-06-15 10:00:00"], MERCHANT_COUNTRY=["GB"])
    )
    assert out["INFLATION_INDEX_COVERAGE"].iat[0] == "OUT_OF_PANEL"
    assert pd.isna(out["INFLATION_INDEX_CLEANED"].iat[0])


def test_a_month_outside_the_window_is_unrecoverable_not_invented():
    """
    One real row sits 14 minutes past the end of the covered window. There is
    no series for it, and saying so beats extrapolating one.
    """
    out = run(frame_of(["2030-01-15 10:00:00"]))
    assert out["INTEREST_RATE_INDEX_COVERAGE"].iat[0] == "UNRECOVERABLE"
    assert pd.isna(out["INTEREST_RATE_INDEX_CLEANED"].iat[0])


def test_out_of_panel_outranks_unrecoverable():
    """
    A country with no series and a month with no entry both yield a null and
    they mean opposite things. The one that is a property of the panel wins,
    because it is the one that is never going to be fixed by better data.
    """
    out = run(
        frame_of(["2030-01-15 10:00:00"], MERCHANT_COUNTRY=["GB"])
    )
    assert out["INFLATION_INDEX_COVERAGE"].iat[0] == "OUT_OF_PANEL"


# --- the rule file itself ---------------------------------------------------

def test_the_holiday_rule_is_stable_across_years():
    """
    144 (country, calendar-month) groups, 0 varying. If a future file breaks
    this the flag is no longer a calendar rule and the lookup is the wrong
    tool for it.
    """
    values = loader.macro_series()["is_holiday_month"]["values"]
    by_country_month: dict[tuple[str, int], set] = {}
    for key, flag in values.items():
        month, country = key.split("|")
        by_country_month.setdefault(
            (country, int(month[5:7])), set()
        ).add(flag)
    varying = {k: v for k, v in by_country_month.items() if len(v) > 1}
    assert not varying, varying


def test_the_interest_series_covers_every_month_it_claims():
    """One global series, one value per month, no holes inside the window."""
    values = loader.macro_series()["interest_rate_index"]["values"]
    months = sorted(values)
    expected = pd.period_range(months[0], months[-1], freq="M")
    assert [str(p) for p in expected] == months


def test_the_two_country_lists_do_not_overlap():
    """
    A country is either in the inflation panel or not. Being in both lists
    would make OUT_OF_PANEL depend on evaluation order.
    """
    series = loader.macro_series()["inflation_index"]
    assert not set(series["covered_countries"]) & set(
        series["uncovered_countries"]
    )


# --- the real file ----------------------------------------------------------

def test_every_recoverable_gap_on_the_source_is_recovered(forecast):
    """
    Nothing should be left UNRECOVERABLE except the single row past the end
    of the window. A larger number means the rule file has a hole.
    """
    out = run(forecast)
    for column in ("INTEREST_RATE_INDEX", "INFLATION_INDEX",
                   "IS_HOLIDAY_MONTH"):
        stranded = int(
            (out[f"{column}_COVERAGE"] == "UNRECOVERABLE").sum()
        )
        assert stranded <= 1, (column, stranded)


def test_recovery_never_overwrites_an_observed_value(forecast):
    """The audit that matters: recovery is additive."""
    out = run(forecast)
    for column in ("INTEREST_RATE_INDEX", "INFLATION_INDEX"):
        observed = out[column].notna()
        original = pd.to_numeric(out.loc[observed, column], errors="coerce")
        assert (
            out.loc[observed, f"{column}_CLEANED"] == original
        ).all()
