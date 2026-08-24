"""Date parsing: the separator rule, epoch millis, and date-shaped nulls."""

import pandas as pd
import pytest

from src.cleaners.dates import DateNormalizer


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2022-03-10 00:51:36", "2022-03-10 00:51:36"),
        ("2022-03-13", "2022-03-13 00:00:00"),
        # Slash means day-first: 630 unambiguous rows agree, none dissent.
        ("09/07/2022 17:49", "2022-07-09 17:49:00"),
        ("15/06/2022 09:40", "2022-06-15 09:40:00"),
        # Dash means month-first: 371 unambiguous rows agree, none dissent.
        ("04-22-2022 22:52", "2022-04-22 22:52:00"),
        ("03-30-2022 14:15", "2022-03-30 14:15:00"),
        ("23-Apr-22", "2022-04-23 00:00:00"),
        ("1660775652000", "2022-08-17 22:34:12"),
    ],
)
def test_each_format_parses(raw, expected, report):
    df = DateNormalizer(report).apply(pd.DataFrame({"TXN_DATE_TIME": [raw]}))
    assert df["TXN_DATE_TIME_CLEANED"].iat[0] == pd.Timestamp(expected)


def test_slash_and_dash_disagree_on_the_same_digits(report):
    """
    09/07 is 9 July; 09-07 is 7 September. The separator is the only signal.
    """
    df = DateNormalizer(report).apply(
        pd.DataFrame(
            {"TXN_DATE_TIME": ["09/07/2022 10:00", "09-07-2022 10:00"]}
        )
    )
    parsed = df["TXN_DATE_TIME_CLEANED"]
    assert parsed.iat[0] == pd.Timestamp("2022-07-09 10:00")
    assert parsed.iat[1] == pd.Timestamp("2022-09-07 10:00")


@pytest.mark.parametrize("token", ["", "0000-00-00", "1970-01-01"])
def test_date_shaped_nulls_become_nat(token, report):
    """0000-00-00 and epoch zero are nulls wearing a date costume."""
    df = DateNormalizer(report).apply(pd.DataFrame({"SETTLE_DATE": [token]}))
    assert pd.isna(df["SETTLE_DATE_CLEANED"].iat[0])


def test_unrecognised_format_is_nat_not_a_guess(report):
    """An unparsed date is a bug signal and must never be silently invented."""
    step = DateNormalizer(report)
    df = step.apply(pd.DataFrame({"TXN_DATE_TIME": ["March 3rd 2022"]}))
    assert pd.isna(df["TXN_DATE_TIME_CLEANED"].iat[0])
    # The row says so itself, which is the half that survives the move to
    # Spark: the count below is derived from this column, not accumulated
    # while the rows were being read.
    assert df["TXN_DATE_TIME_FORMAT"].iat[0] == "UNPARSEABLE"

    step.collect(df)
    assert ("dates", "TXN_DATE_TIME.unparseable", 1) in report.entries


def test_source_file_parses_completely(transactions, report):
    """
    Every date in the real file must parse; a regression here is a new
    format.
    """
    df = DateNormalizer(report).apply(transactions)
    assert df["TXN_DATE_TIME_CLEANED"].isna().sum() == 0
    assert df["SETTLE_DATE_CLEANED"].isna().sum() == 14
