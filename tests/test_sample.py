"""
The sample is a claim, and these are the ways it could stop being true.

A sample is only worth running against if it is *representative in the ways
the stages care about* and *identical on every run*. Neither property is
visible by looking at the file, and both fail silently: a sample that lost its
trap-pair rows still passes every test that does not need them, and a sample
that drifts between runs turns one failing stage into a bug report nobody can
reproduce.
"""

import hashlib
import json

import pytest

from src.spark import sample as sample_module


@pytest.fixture(scope="session")
def manifest(sample_path):
    """:returns: The sidecar describing how the cached sample was cut."""
    return json.loads(
        sample_module._manifest(sample_path).read_text(encoding="utf-8")
    )


def test_the_sample_is_deterministic(sample_path):
    """
    The same source produces the same bytes, every run and every machine.

    The same determinism requirement the Phase 06 upsert key carries, for the
    same reason. It rules out ``hash()``, which is salted per process, and
    ``DataFrame.sample``, whose seeding is a numpy implementation detail --
    both of which look deterministic in a single session and are not.
    """
    before = hashlib.sha256(sample_path.read_bytes()).hexdigest()
    sample_module.build(destination=sample_path)
    after = hashlib.sha256(sample_path.read_bytes()).hexdigest()

    assert before == after


def test_accounts_arrive_whole(sample_frame, forecast, manifest):
    """
    A selected account brings all of its rows.

    The property the entire sampling strategy exists for.
    ``BalanceReconstructor`` works over
    ``partitionBy(ACCOUNT_ID).orderBy(TXN_SEQ)``: drop rows out of the middle
    of an account and every chain breaks, every gap becomes unclosable, and
    the stage reports UNVERIFIED where the real run reports DERIVED. Both
    engines would agree on that wrong answer and the parity test would pass
    having proved nothing.
    """
    accounts = set(manifest["accounts"])
    expected = forecast[forecast["ACCOUNT_ID"].isin(accounts)]

    assert len(sample_frame) == len(expected)
    assert (
        sample_frame.groupby("ACCOUNT_ID").size().to_dict()
        == expected.groupby("ACCOUNT_ID").size().to_dict()
    )


def test_both_sides_of_the_balance_seam_are_present(sample_frame):
    """
    The source's stated balances close on the early rows and stop closing
    partway through, because the later ones were built from BILLING_AMOUNT
    and are denominated in a different currency than the figure they were
    applied to.

    A sample confined to one side would exercise one branch of the balance
    stage. Asserted as coverage of the sequence range rather than against a
    seam position, because ``BalanceReconstructor`` refuses to hardcode one --
    a row number is a fact about one extract, and a sampler that smuggled it
    back in would carry it silently into the next.
    """
    sequence = sample_frame["TXN_SEQ"].astype(int)
    span = sequence.max() - sequence.min()

    assert (sequence < sequence.min() + span / 4).any()
    assert (sequence > sequence.max() - span / 4).any()


def test_every_timestamp_format_in_the_source_is_present(
    sample_frame, forecast
):
    """
    The sample carries an epoch integer, an ISO string and a day-first string.

    Requirement 2 is about unparseable and ambiguous rows being counted rather
    than guessed, and a sample holding one format would let a format table
    lose an entry without a single test noticing.
    """
    shapes = sample_frame["TXN_DATE_TIME"].fillna("").map(sample_module._shape)
    everything = forecast["TXN_DATE_TIME"].fillna("").map(sample_module._shape)

    assert set(everything.unique()) == set(shapes.unique())


def test_trap_pair_merchants_are_present(sample_frame):
    """
    Requirement 8's first named test case needs rows to run on.

    Skipped rather than failed when this particular source names none of the
    protected merchants: the trap-pair file covers both sources, and a sample
    cut from an extract that happens to contain none of them is a fact about
    the extract, not a broken sampler.
    """
    names = sample_module._trap_names()
    upper = sample_frame["MERCHANT_NAME"].fillna("").str.upper()
    hits = sum(int(upper.str.contains(name, regex=False).any()) for name in names)

    if not hits:
        pytest.skip("this source names none of the protected merchants")
    assert hits


def test_withheld_balances_are_present(sample_frame):
    """
    Roughly a third of the source states no balance, and that third is where
    every fill-or-withhold decision in the balance stage lives.
    """
    assert sample_frame["RUNNING_BALANCE"].isna().any()
    assert sample_frame["RUNNING_BALANCE"].notna().any()


def test_a_stale_sample_is_rebuilt(tmp_path, sample_path):
    """
    A cache nobody invalidates is the one failure mode nobody notices,
    because everything still passes -- against last week's file.

    The manifest records the strategy version and the source's byte count, and
    a mismatch in either rebuilds. Byte count rather than mtime: mtime changes
    on every clone and checkout, so it would rebuild constantly, and it does
    not change when a file is edited in place to the same length -- both too
    sensitive and not sensitive enough.
    """
    # Cut from the cached sample rather than the 68 MB extract: it is a
    # source file in its own right -- same header, same profile, same
    # readers -- which is one of the things making the sample a sample and
    # not a fixture.
    destination = tmp_path / "sample.csv"
    destination.write_text("TXN_SEQ\n1\n", encoding="utf-8")
    sample_module._manifest(destination).write_text(
        json.dumps(
            {
                # Only this is stale. Everything else matches, so a rebuild
                # can only be attributed to the version check.
                "spec_version": sample_module.SPEC_VERSION - 1,
                "source": str(sample_path),
                "budget": sample_module.ACCOUNTS,
                "source_bytes": sample_path.stat().st_size,
            }
        ),
        encoding="utf-8",
    )

    sample_module.ensure(source=sample_path, destination=destination)

    assert destination.read_text(encoding="utf-8") != "TXN_SEQ\n1\n"


def test_a_current_sample_is_not_rebuilt(sample_path):
    """
    The other half: reading a 68 MB source on every test session because the
    cache check is too strict is a real cost, just a quieter one.
    """
    before = sample_path.stat().st_mtime_ns

    sample_module.ensure(destination=sample_path)

    assert sample_path.stat().st_mtime_ns == before
