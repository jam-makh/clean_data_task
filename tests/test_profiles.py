"""
Profiles and the CLI: the same pipeline against a file it has not seen.

Detection reads column names and nothing else, so these tests never start a
JVM -- ``tests/test_runner.py`` covers the run itself and pays for it.
"""

import pytest

import main as entry
from src.config import runtime
from src.config.errors import ConfigError
from src.spark.pipeline import SPARK_STEP_REGISTRY, ported, steps_for


def config():
    """:returns: The shipped runtime config."""
    return runtime.load()


# --- the registry -----------------------------------------------------------

def test_every_ported_step_exists():
    """
    A typo in a profile must fail at load. Silently skipping a step would
    produce a plausible-looking output with a whole cleaning concern missing.

    Checked over the ported prefix rather than the whole profile: the v4
    workbook profile opens with ``dates``, which has no Spark implementation
    and is not getting one -- the source in hand is the forecast extract.
    ``ported`` is the ledger of what exists, so asking it what runs and then
    resolving exactly that is the honest version of this check.
    """
    for profile in config().profiles:
        steps_for(ported(profile.steps))


def test_an_unknown_step_names_what_is_known():
    with pytest.raises(KeyError) as caught:
        steps_for(["timestamps", "nonsense"])
    assert "nonsense" in str(caught.value)
    assert "timestamps" in str(caught.value)


def test_no_profile_repeats_a_step():
    """Running a cleaner twice would double-count every metric it logs."""
    for profile in config().profiles:
        assert len(set(profile.steps)) == len(profile.steps), profile.name


# --- detection --------------------------------------------------------------

def test_the_forecast_source_selects_its_own_profile(forecast):
    chosen = config().detect(forecast.columns)
    assert chosen.name == "forecast_balance"
    assert "timestamps" in chosen.steps and "macro" in chosen.steps


def test_the_workbook_selects_its_own_profile(transactions):
    chosen = config().detect(transactions.columns)
    assert chosen.name == "transactions_v4"
    assert "dates" in chosen.steps


def test_the_two_profiles_cannot_both_match(forecast, transactions):
    """
    Detection is first-match-wins, so overlapping profiles would make the
    result depend on file order rather than on the data.
    """
    settings = config()
    forecast_profile = settings.profile("forecast_balance")
    workbook_profile = settings.profile("transactions_v4")
    assert not workbook_profile.matches(forecast.columns)
    assert not forecast_profile.matches(transactions.columns)


def test_an_undescribed_source_is_an_error_not_a_guess():
    """
    The two profiles parse dates in ways that are silently wrong for each
    other's files, so defaulting to either would corrupt months without
    raising. Refusing names what was wanted instead.
    """
    with pytest.raises(ConfigError) as caught:
        config().detect(["SOME_COLUMN", "ANOTHER"])
    assert "TXN_SEQ" in str(caught.value)
    assert "--profile" in str(caught.value)


def test_an_unknown_profile_name_lists_the_real_ones():
    with pytest.raises(ConfigError) as caught:
        config().profile("does_not_exist")
    assert "forecast_balance" in str(caught.value)


# --- the command line -------------------------------------------------------

def test_source_defaults_to_the_configured_path():
    args = entry.build_parser().parse_args([])
    assert args.source is None and args.profile is None


def test_the_source_is_positional():
    args = entry.build_parser().parse_args(["data/raw/some_file.csv"])
    assert args.source == "data/raw/some_file.csv"


def test_the_profile_can_be_forced():
    args = entry.build_parser().parse_args(["f.csv", "--profile", "forecast_balance"])
    assert args.profile == "forecast_balance"


def test_an_unconfigured_profile_is_rejected_by_the_parser():
    """argparse refuses the value rather than the pipeline discovering it."""
    with pytest.raises(SystemExit):
        entry.build_parser().parse_args(["f.csv", "--profile", "made_up"])


def test_a_missing_source_exits_two_not_one(capsys):
    """
    Distinguished on purpose: retrying a file that has not landed yet is
    reasonable, retrying a malformed profile is not.
    """
    assert entry.main(["data/raw/definitely_not_here.csv"]) == 2
    assert "not found" in capsys.readouterr().err.lower()


# --- the CLI ----------------------------------------------------------------
#
# The dispatch itself, checked without a JVM. What these pin is that main
# resolves its arguments and hands off -- ``tests/test_runner.py`` covers what
# the run then does, end to end, and pays six minutes for the privilege.


def test_the_source_reaches_the_run(monkeypatch):
    """
    An unflagged run resolves its source and hands it over. A regression here
    would send the wrong file, or none.
    """
    called = {}

    def fake(source, args):
        called["source"] = source
        return 0

    monkeypatch.setattr(entry, "_run_spark", fake)

    assert entry.main(["data/raw/forecast_balance_data.csv"]) == 0
    assert called["source"].name == "forecast_balance_data.csv"


def test_dry_run_is_carried_through(monkeypatch):
    """
    ``--dry-run`` has to reach the run, which is what suppresses both the
    database write and the Kafka announcement.
    """
    seen = {}

    def fake(source, args):
        seen["dry_run"] = args.dry_run
        return 0

    monkeypatch.setattr(entry, "_run_spark", fake)

    assert entry.main(["data/raw/forecast_balance_data.csv", "--dry-run"]) == 0
    assert seen["dry_run"] is True


def test_a_missing_source_is_caught_before_the_run(monkeypatch):
    """
    Exit 2 without starting a JVM. Checking the file first is what keeps a
    typo'd path cheap instead of costing Spark startup to discover.
    """
    def refuse(*_args, **_kwargs):
        raise AssertionError("a missing source reached the run")

    monkeypatch.setattr(entry, "_run_spark", refuse)

    assert entry.main(["data/raw/definitely_not_here.csv"]) == 2
