"""Wall-clock normalization: precision, offsets, and the day/month split."""

import pandas as pd
import pytest

from src.cleaners.timestamps import TimestampNormalizer
from src.utils.report import CleaningReport


def run(frame, **config):
    """:returns: The frame with the timestamp columns added."""
    return TimestampNormalizer(CleaningReport(), **config).apply(frame)


def frame_of(values, **columns):
    """:returns: A frame carrying ``values`` as TXN_DATE_TIME."""
    base = {
        "TXN_DATE_TIME": values,
        "ACCOUNT_ID": ["A"] * len(values),
        "TXN_SEQ": list(range(1, len(values) + 1)),
    }
    base.update(columns)
    return pd.DataFrame(base)


# --- readings the source already wrote as a clock are never moved -----------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2022-02-01 10:15:34", "2022-02-01 10:15:34"),
        ("2022-06-01 01:49:16", "2022-06-01 01:49:16"),
        ("2022/01/08 20:06", "2022-01-08 20:06:00"),
        ("01-Jan-22 19:37", "2022-01-01 19:37:00"),
        ("Jan 12, 2022", "2022-01-12 00:00:00"),
    ],
)
def test_a_written_clock_survives_digit_for_digit(raw, expected):
    """
    The whole point of the offset column: a reading the source stated is
    copied, not converted. A shift here would be silent and unrecoverable.
    """
    out = run(frame_of([raw]))
    assert out["TXN_TS"].iat[0] == pd.Timestamp(expected)
    assert out["TXN_TS_SOURCE"].iat[0] == "AS_WRITTEN"


def test_epoch_is_rendered_in_the_source_clock():
    """
    1640988000 is 2021-12-31 22:00 UTC and 2022-01-01 00:00 in the source
    zone. Reading it as UTC moves the row into a month the file does not
    cover, and takes its macro lookups with it.
    """
    out = run(frame_of(["1640988000"]))
    assert out["TXN_TS"].iat[0] == pd.Timestamp("2022-01-01 00:00:00")
    assert out["TXN_TS_SOURCE"].iat[0] == "OFFSET_APPLIED"


def test_the_offset_label_tracks_daylight_saving():
    """Derived from each reading's own date, not from a fixed constant."""
    out = run(frame_of(["2022-02-01 10:15:34", "2022-06-01 01:49:16"]))
    assert list(out["TXN_TS_UTC_OFFSET"]) == ["UTC+2", "UTC+3"]


def test_a_dst_gap_reading_is_still_expressible():
    """
    2022-03-27 00:00 does not exist in the source zone -- tz_localize raises
    on it, and 74 rows of the file are like this. Storing the offset as a
    label rather than a tz-aware dtype is what keeps them representable.
    """
    out = run(frame_of(["2022-03-27 00:00:00"]))
    assert out["TXN_TS"].iat[0] == pd.Timestamp("2022-03-27 00:00:00")
    assert out["TXN_TS_UTC_OFFSET"].iat[0]


# --- precision: midnight is a floor, not an observation --------------------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2022-02-01 10:15:34", "SECOND"),
        ("2022/01/08 20:06", "MINUTE"),
        ("01-Jan-22 19:37", "MINUTE"),
        ("Jan 12, 2022", "DAY"),
        # A date-only value that went through an epoch column arrives as a
        # local midnight and carries no time of day either.
        ("1641938400", "DAY"),
        ("1711836000", "SECOND"),
    ],
)
def test_precision_records_what_was_actually_observed(raw, expected):
    assert run(frame_of([raw]))["TXN_TS_PRECISION"].iat[0] == expected


def test_a_date_only_row_is_not_a_midnight_transaction():
    """
    Both spellings render as 00:00:00 because the column has one dtype. The
    flag is the only thing that stops that being read as an observed time.
    """
    out = run(frame_of(["Jan 12, 2022", "1641938400"]))
    assert (out["TXN_TS"] == pd.Timestamp("2022-01-12")).all()
    assert list(out["TXN_TS_PRECISION"]) == ["DAY", "DAY"]


# --- the day-first / month-first split -------------------------------------

def test_an_impossible_month_forces_day_first():
    """10814 rows are settled by this alone."""
    out = run(frame_of(["14/01/2022 04:41"]))
    assert out["TXN_TS"].iat[0] == pd.Timestamp("2022-01-14 04:41")


def test_an_impossible_day_forces_month_first():
    """And 44902 by its mirror. Both conventions are in the one column."""
    out = run(frame_of(["01/13/2022 00:02"]))
    assert out["TXN_TS"].iat[0] == pd.Timestamp("2022-01-13 00:02")


def test_the_macro_rate_settles_what_the_fields_cannot():
    """
    The rate is constant within a year-month, so it names the month without
    consulting the string. Here 4.519 is only ever seen in January, which
    makes 01/09 the ninth of January and not the first of September.
    """
    frame = frame_of(
        ["01/31/2022 08:00", "01/09/2022 00:58"],
        INTEREST_RATE_INDEX=["4.519", "4.519"],
    )
    out = run(frame)
    assert out["TXN_TS"].iat[1] == pd.Timestamp("2022-01-09 00:58")
    assert not out["TXN_TS_AMBIGUOUS"].iat[1]


def test_the_macro_rate_can_also_choose_day_first():
    """The oracle is not a dressed-up preference for month-first."""
    frame = frame_of(
        ["09/30/2022 08:00", "01/09/2022 00:58"],
        INTEREST_RATE_INDEX=["4.879", "4.879"],
    )
    out = run(frame)
    assert out["TXN_TS"].iat[1] == pd.Timestamp("2022-09-01 00:58")


def test_an_unresolvable_reading_is_flagged_not_hidden():
    """
    With no oracle and no neighbours, 02/03 could be either date. It takes
    the majority reading and says so.
    """
    out = run(frame_of(["02/03/2022 01:49"]))
    assert out["TXN_TS_AMBIGUOUS"].iat[0]


def test_a_same_number_date_is_not_ambiguous():
    """
    01/01 reads identically both ways, so there is nothing at stake. 3081
    rows look unresolved and are not; flagging them would bury the 4 that
    genuinely are.
    """
    out = run(frame_of(["01/01/2022 01:49"]))
    assert not out["TXN_TS_AMBIGUOUS"].iat[0]


# --- settlement -------------------------------------------------------------

def test_a_far_future_settlement_sentinel_becomes_null():
    """
    9999-12-31 is outside the bounds of a nanosecond timestamp, so parsing it
    raises rather than returning something wrong. 353 rows carry it.
    """
    frame = frame_of(
        ["2022-01-01 00:00:00"] * 3,
        SETTLE_DATE=["9999-12-31", "0000-00-00", "03-Jan-22"],
    )
    out = run(frame)
    assert out["SETTLE_DATE_CLEANED"].isna().tolist() == [True, True, False]


def test_settlement_keeps_no_time_of_day():
    """Date-only by design of the field, not by loss."""
    frame = frame_of(["2022-01-01 00:00:00"], SETTLE_DATE=["03-Jan-22"])
    assert run(frame)["SETTLE_DATE_CLEANED"].iat[0] == pd.Timestamp(
        "2022-01-03"
    )


# --- the real file ----------------------------------------------------------

def test_every_row_of_the_source_parses(forecast):
    """
    An unparsed date means a format nobody has handled, which is a bug signal
    rather than a missing value.
    """
    out = run(forecast)
    assert out["TXN_TS"].isna().sum() == 0


def test_no_row_lands_outside_the_covered_window(forecast):
    """
    Reading the epoch column as UTC drops 10 rows into 2021-12, a month with
    no macro data at all. That is the failure this whole step exists to
    prevent, so it is asserted rather than assumed.
    """
    out = run(forecast)
    assert out["TXN_TS"].min() >= pd.Timestamp("2022-01-01")


# --- what is unknown is stated as unknown ----------------------------------

def test_a_date_only_row_states_that_the_time_is_unknown():
    """
    The rendered 00:00:00 is a floor the parser supplied. TXN_TS_STATUS is
    the word that stops it being read as an observed midnight.
    """
    out = run(frame_of(["Jan 12, 2022", "1641938400"]))
    assert list(out["TXN_TS_STATUS"]) == ["TIME_UNKNOWN", "TIME_UNKNOWN"]


def test_a_full_timestamp_is_observed():
    out = run(frame_of(["2022-02-01 10:15:34"]))
    assert out["TXN_TS_STATUS"].iat[0] == "OBSERVED"


def test_an_unsettled_reading_states_that_the_date_is_ambiguous():
    out = run(frame_of(["02/03/2022 01:49"]))
    assert out["TXN_TS_STATUS"].iat[0] == "DATE_AMBIGUOUS"


def test_ambiguity_outranks_a_missing_time():
    """
    A row that is both is reported as ambiguous, because that is the one a
    reader must not act on.
    """
    out = run(frame_of(["02/03/2022 00:00"]))
    assert out["TXN_TS_STATUS"].iat[0] == "DATE_AMBIGUOUS"


def test_nothing_on_the_source_is_left_status_free(forecast):
    """Every row carries one of the four words, and none is UNKNOWN."""
    out = run(forecast)
    assert out["TXN_TS_STATUS"].notna().all()
    assert (out["TXN_TS_STATUS"] == "UNKNOWN").sum() == 0
