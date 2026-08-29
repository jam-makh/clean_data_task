"""Date rendering at observed precision, and output dispatch by extension."""

import pandas as pd
import pytest

from src.utils.io import (
    DELIMITED_SUFFIXES,
    UNKNOWN_TEXT,
    WORKBOOK_SUFFIXES,
    read_source,
    render_dates,
    write_output,
)


def precision_frame() -> pd.DataFrame:
    """
    :returns: One datetime column carrying all three precisions, beside the
        companion column that names each row's precision.
    """
    return pd.DataFrame(
        {
            "txn_id": ["a", "b", "c"],
            "txn_date_time_cleaned": pd.to_datetime(
                [
                    "2022-03-10 00:51:36",
                    "2022-07-09 17:49:00",
                    "2022-04-22 00:00:00",
                ]
            ),
            "txn_date_time_precision": ["SECOND", "MINUTE", "DAY"],
        }
    )


def sheet_set() -> dict[str, pd.DataFrame]:
    """
    :returns: Two sheets carrying the values a delimited writer can corrupt --
        an embedded separator, embedded quotes, and a null.
    """
    return {
        "cleaned_transactions": pd.DataFrame(
            {
                "txn_id": ["a", "b"],
                "merchant_name_cleaned": ["ACME, INC", 'SAY "HI"'],
                "txn_amount_cleaned": [-104.39, 808.41],
                "merchant_city_cleaned": ["BEIRUT", None],
            }
        ),
        "cleaning_report": pd.DataFrame(
            {"step": ["amounts"], "metric": ["sign_restored"], "value": [13]}
        ),
    }


# --- render_dates: a timestamp is spelled at the precision it was observed ---


def test_each_precision_renders_at_its_own_width():
    """
    Three levels, one column. The companion decides the format, so a frame
    holding all three comes out spelled three ways.
    """
    out = render_dates(precision_frame())
    assert list(out["txn_date_time_cleaned"]) == [
        "10-03-2022 00:51:36",
        "09-07-2022 17:49",
        "22-04-2022",
    ]


def test_a_day_precision_row_never_states_a_time():
    """
    The 18592 date-only rows carry midnight as a placeholder, not as an
    observation. Rendering them at full width would print 00:00:00 and turn a
    time nobody recorded into a time the sheet asserts.
    """
    out = render_dates(precision_frame())
    assert ":" not in out["txn_date_time_cleaned"].iloc[2]


def test_midnight_is_rendered_by_precision_not_by_value():
    """
    The boundary the companion column exists for: two rows holding the
    identical instant, one observed to the second and one only to the day. A
    renderer that inferred precision from the value -- "midnight means no time
    was given" -- would collapse them, and would be wrong about the
    transaction that genuinely happened at 00:00:00.
    """
    frame = pd.DataFrame(
        {
            "txn_date_time_cleaned": pd.to_datetime(
                ["2022-03-10 00:00:00", "2022-03-10 00:00:00"]
            ),
            "txn_date_time_precision": ["SECOND", "DAY"],
        }
    )
    assert list(render_dates(frame)["txn_date_time_cleaned"]) == [
        "10-03-2022 00:00:00",
        "10-03-2022",
    ]


def test_zero_seconds_is_rendered_by_precision_not_by_value():
    """
    The same boundary one level up: :00 seconds is a real observation on a
    SECOND row and an absent one on a MINUTE row.
    """
    frame = pd.DataFrame(
        {
            "txn_date_time_cleaned": pd.to_datetime(
                ["2022-03-10 17:49:00", "2022-03-10 17:49:00"]
            ),
            "txn_date_time_precision": ["SECOND", "MINUTE"],
        }
    )
    assert list(render_dates(frame)["txn_date_time_cleaned"]) == [
        "10-03-2022 17:49:00",
        "10-03-2022 17:49",
    ]


def test_an_unrecognised_precision_still_renders():
    """
    A level added upstream and not yet known here must not produce an empty
    cell: the row has a timestamp the pipeline parsed correctly, and dropping
    it would lose data to a vocabulary mismatch.
    """
    frame = pd.DataFrame(
        {
            "txn_date_time_cleaned": pd.to_datetime(["2022-03-10 00:51:36"]),
            "txn_date_time_precision": ["MILLISECOND"],
        }
    )
    rendered = render_dates(frame)["txn_date_time_cleaned"]
    assert rendered.iloc[0] == "10-03-2022 00:51:36"


def test_a_missing_timestamp_stays_missing():
    """Not the string "NaT", which reads as a value on the sheet."""
    frame = pd.DataFrame(
        {
            "txn_date_time_cleaned": pd.to_datetime(
                ["2022-03-10 00:51:36", None]
            ),
            "txn_date_time_precision": ["SECOND", "DAY"],
        }
    )
    out = render_dates(frame)
    assert out["txn_date_time_cleaned"].iloc[0] == "10-03-2022 00:51:36"
    assert pd.isna(out["txn_date_time_cleaned"].iloc[1])


def test_the_configured_formats_win_over_the_module_fallbacks():
    """
    Day-first is the house convention, not a fact about the data, so a caller
    reading in another region must be able to set all three.
    """
    out = render_dates(
        precision_frame(),
        {
            "datetime": "%Y-%m-%d %H:%M:%S",
            "minute": "%Y-%m-%d %H:%M",
            "date": "%Y-%m-%d",
        },
    )
    assert list(out["txn_date_time_cleaned"]) == [
        "2022-03-10 00:51:36",
        "2022-07-09 17:49",
        "2022-04-22",
    ]


def test_an_override_of_one_format_leaves_the_others_at_their_fallback():
    """Each level is looked up alone; a partial policy is not all-or-nothing."""
    out = render_dates(precision_frame(), {"date": "%Y-%m-%d"})
    assert list(out["txn_date_time_cleaned"]) == [
        "10-03-2022 00:51:36",
        "09-07-2022 17:49",
        "2022-04-22",
    ]


def test_a_column_without_a_companion_renders_at_full_width():
    """
    Only txn_date_time_cleaned has a precision column. Every other
    datetime is stated to the
    second by the source, so it is rendered that way.
    """
    frame = pd.DataFrame(
        {"txn_date_time_cleaned": pd.to_datetime(["2022-03-10 00:51:36"])}
    )
    out = render_dates(frame)
    assert out["txn_date_time_cleaned"].iloc[0] == "10-03-2022 00:51:36"


@pytest.mark.parametrize(
    "name", ["settle_date_cleaned", "SETTLE_DATE_CLEANED"]
)
def test_the_settle_date_is_rendered_without_a_time(name):
    """
    A settlement date has no time of day to state. Matched case-insensitively
    because a sheet goes out under lowercase names while a frame handed
    straight to the writer still carries the pipeline's own -- and the same
    column must not render two ways depending on which caller got there first.
    """
    frame = pd.DataFrame({name: pd.to_datetime(["2022-03-13 00:00:00"])})
    assert render_dates(frame)[name].iloc[0] == "13-03-2022"


@pytest.mark.parametrize(
    "name", ["settle_date_cleaned", "SETTLE_DATE_CLEANED"]
)
def test_a_settle_date_that_is_not_known_is_written_as_a_word(name):
    """
    An empty cell reads as "nothing to say" as easily as "not settled yet",
    so the one column where that difference matters spells it out. Case
    handled the same way as the date-only rendering above, and for the same
    reason.
    """
    frame = pd.DataFrame({name: pd.to_datetime(["2022-03-13", None])})
    assert list(render_dates(frame)[name]) == ["13-03-2022", UNKNOWN_TEXT]


@pytest.mark.parametrize(
    "name",
    [
        "running_balance_filled",
        "RUNNING_BALANCE_FILLED",
        "running_balance_normalized",
    ],
)
def test_a_withheld_balance_is_written_as_a_word(name):
    """
    A withheld balance is a decision, not an oversight: the arithmetic either
    could not reach the row or proved the stated figure wrong. The cell says
    so rather than leaving the reader to guess from a blank, and the status
    column beside it says which of the two it was.

    Written on the rendered copy only -- the column is still nullable floats
    in the frame, which is what a string fill on a numeric column would
    otherwise break.
    """
    frame = pd.DataFrame({name: pd.array([1234.56, None], dtype="Float64")})
    assert list(render_dates(frame)[name]) == [1234.56, UNKNOWN_TEXT]


def test_an_unparseable_timestamp_is_still_left_blank():
    """
    Only the columns that opted in carry a placeholder. Everywhere else a
    null stays a null, so the word cannot start appearing in columns nobody
    decided it belonged in.
    """
    frame = pd.DataFrame(
        {
            "txn_date_time_cleaned": pd.to_datetime(["2022-03-10", None]),
            "txn_date_time_precision": ["DAY", "DAY"],
        }
    )
    assert pd.isna(render_dates(frame)["txn_date_time_cleaned"].iat[1])


def test_non_datetime_columns_are_untouched():
    out = render_dates(precision_frame())
    assert list(out["txn_id"]) == ["a", "b", "c"]


def test_the_precision_companion_is_read_and_then_dropped():
    """
    It is an input to rendering, not a column of the sheet.

    The rendered value already says which precision it was -- a DAY row is
    written as a bare date -- so a reader needs the companion only to produce
    that, never to read it. Dropping it any earlier would be the bug: the
    writer could no longer tell a date-only row from a real midnight, and
    would put 00:00:00 on every one of them.
    """
    rendered = render_dates(precision_frame())
    assert "txn_date_time_precision" not in rendered.columns
    assert list(rendered["txn_date_time_cleaned"]) == [
        "10-03-2022 00:51:36",   # SECOND, and the time is stated
        "09-07-2022 17:49",      # MINUTE, no seconds were ever recorded
        "22-04-2022",            # DAY, no time of day was ever recorded
    ]


def test_rendering_leaves_the_callers_frame_alone():
    """
    Rendering happens on the way out. A caller that writes a workbook and then
    keeps working with the frame must still have sortable datetimes.
    """
    frame = precision_frame()
    render_dates(frame)
    assert pd.api.types.is_datetime64_any_dtype(frame["txn_date_time_cleaned"])


# --- write_output: the destination name picks the writer --------------------


def test_a_workbook_name_writes_one_file_with_every_sheet(tmp_path):
    written = write_output(tmp_path / "out.xlsx", sheet_set())
    assert written == tmp_path / "out.xlsx"
    assert set(pd.ExcelFile(written).sheet_names) == {
        "cleaned_transactions",
        "cleaning_report",
    }


def test_a_csv_name_writes_one_file_per_sheet(tmp_path):
    """
    openpyxl takes ~500s on 265195 rows where the cleaning takes 120, so the
    same sheets go out as parts instead. The stem is shared and the sheet name
    is the suffix, which is what keeps a run's outputs identifiable as one set.
    """
    written = write_output(tmp_path / "out.csv", sheet_set())
    assert written == tmp_path
    assert (tmp_path / "out__cleaned_transactions.csv").exists()
    assert (tmp_path / "out__cleaning_report.csv").exists()


@pytest.mark.parametrize(
    "suffix,separator", [(".csv", ","), (".tsv", "\t"), (".txt", ",")]
)
def test_each_delimited_suffix_writes_its_own_separator(
    tmp_path, suffix, separator
):
    write_output(tmp_path / f"out{suffix}", sheet_set())
    text = (tmp_path / f"out__cleaning_report{suffix}").read_text()
    assert text.splitlines()[0] == separator.join(["step", "metric", "value"])


def test_an_unsupported_extension_is_refused_by_name(tmp_path):
    """
    Writing a .parquet as CSV would succeed and produce a file nothing can
    read as parquet, so the extension is checked rather than assumed.
    """
    with pytest.raises(ValueError) as exc:
        write_output(tmp_path / "out.parquet", sheet_set())
    assert ".parquet" in str(exc.value)


def test_the_two_writers_are_offered_the_same_extensions():
    """
    A file this pipeline can read and then not write back is a trap. The
    reader and the writer share both suffix tables, so they cannot drift.
    """
    assert set(WORKBOOK_SUFFIXES) | set(DELIMITED_SUFFIXES) == {
        ".xlsx",
        ".xlsm",
        ".xls",
        ".csv",
        ".tsv",
        ".txt",
    }


def test_a_csv_round_trips_with_its_header_order_intact(tmp_path):
    """
    Column order is part of the output contract: a loader reading positionally
    would silently swap merchant name and city if the writer reordered them.
    """
    sheets = sheet_set()
    write_output(tmp_path / "out.csv", sheets)
    back = pd.read_csv(
        tmp_path / "out__cleaned_transactions.csv",
        keep_default_na=False,
        na_values=[""],
    )
    assert list(back.columns) == list(sheets["cleaned_transactions"].columns)
    assert list(back["txn_amount_cleaned"]) == [-104.39, 808.41]


def test_a_value_containing_the_separator_survives_the_round_trip(tmp_path):
    """
    "ACME, INC" written unquoted into a CSV becomes two columns, and every
    field after it shifts by one for that row alone.
    """
    write_output(tmp_path / "out.csv", sheet_set())
    back = pd.read_csv(
        tmp_path / "out__cleaned_transactions.csv",
        keep_default_na=False,
        na_values=[""],
    )
    assert list(back["merchant_name_cleaned"]) == ["ACME, INC", 'SAY "HI"']
    assert len(back.columns) == 4


def test_a_null_is_written_as_an_empty_field_not_as_text(tmp_path):
    """
    A literal "nan" in the file is indistinguishable from a merchant city
    genuinely called that, and read_source treats only "" as null.
    """
    write_output(tmp_path / "out.csv", sheet_set())
    part = tmp_path / "out__cleaned_transactions.csv"
    assert "nan" not in part.read_text().lower()
    back = pd.read_csv(part, keep_default_na=False, na_values=[""])
    assert pd.isna(back["merchant_city_cleaned"].iloc[1])


def test_a_long_sheet_name_is_trimmed_only_for_the_workbook(tmp_path):
    """
    Excel refuses a sheet name over 31 characters. A filename has no such
    limit, so the CSV part keeps the name whole rather than inheriting a
    restriction from a format it is not being written in.
    """
    name = "cleaned_transactions_with_a_very_long_name"
    frame = {name: pd.DataFrame({"a": [1]})}

    book = write_output(tmp_path / "out.xlsx", frame)
    assert pd.ExcelFile(book).sheet_names == [name[:31]]

    write_output(tmp_path / "out.csv", frame)
    assert (tmp_path / f"out__{name}.csv").exists()


@pytest.mark.parametrize("suffix", [".xlsx", ".csv"])
def test_a_missing_destination_directory_is_created(tmp_path, suffix):
    """
    Both writers, because a caller pointing at a dated output folder should
    not have to guess which format needs the folder to exist first.
    """
    write_output(tmp_path / "nested" / "deeper" / f"out{suffix}", sheet_set())
    assert (tmp_path / "nested" / "deeper").is_dir()


def test_both_writers_spell_a_timestamp_the_same_way(tmp_path):
    """
    The destination decides the container, never the content. Someone who
    switched to CSV parts because the workbook got too slow must not find the
    dates spelled differently on the other side.
    """
    book = write_output(tmp_path / "a.xlsx", {"t": precision_frame()})
    write_output(tmp_path / "b.csv", {"t": precision_frame()})

    from_book = pd.read_excel(book, dtype=object)[
        "txn_date_time_cleaned"
    ].astype(str)
    from_csv = pd.read_csv(tmp_path / "b__t.csv", dtype=object)[
        "txn_date_time_cleaned"
    ]
    assert list(from_book) == list(from_csv)


def test_what_the_writer_produces_the_reader_takes_back(tmp_path):
    """
    The CSV parts are the Stage 2 hand-off, so the loader has to be able to
    read them back with the discipline it reads any source with.
    """
    write_output(tmp_path / "out.csv", sheet_set())
    frame, reference = read_source(tmp_path / "out__cleaned_transactions.csv")
    assert list(frame["txn_id"]) == ["a", "b"]
    assert reference == {}, "a delimited file carries no MCC sheet"
