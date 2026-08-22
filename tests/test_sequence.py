"""TXN_SEQ: what the gaps mean, and the invariant the parser leans on."""

import numpy as np
import pandas as pd


def test_the_sequence_is_globally_dense(forecast):
    """
    1..N with nothing missing. The gaps that show up when you group by
    account are not lost rows -- see the test below for what they are -- and
    a real gap here would mean the extract dropped something.
    """
    sequence = forecast["TXN_SEQ"].astype("int64")
    assert sequence.is_unique
    assert sequence.min() == 1
    assert sequence.max() == len(forecast)


def test_an_intra_account_gap_is_the_users_other_account(forecast):
    """
    The observation that starts this: sequence skips 2 or 3 within one
    account. Every small gap is filled by the same user's other cards, which
    is why nothing needs repairing. Users hold up to four accounts.
    """
    frame = forecast.assign(
        seq=forecast["TXN_SEQ"].astype("int64")
    ).sort_values("seq")
    owner = pd.factorize(frame["USER_ID"])[0]
    previous = frame.groupby("ACCOUNT_ID")["seq"].shift()
    size = frame["seq"] - previous

    # Large gaps are the file's own block layout and say nothing about a
    # user; the small ones are the claim being tested.
    mixed = 0
    for position in np.flatnonzero(((size > 1) & (size <= 50)).values):
        start = int(previous.values[position])
        end = int(frame["seq"].values[position])
        between = owner[start:end - 1]
        if not (between == owner[position]).all():
            mixed += 1
    assert mixed == 0


def test_sequence_orders_an_account_chronologically(forecast):
    """
    The licence for the bracket pass in TimestampNormalizer. It is a weak
    signal and the last one tried, but it is only usable at all because
    within an account the sequence really is time order.
    """
    from src.cleaners.timestamps import TimestampNormalizer
    from src.utils.report import CleaningReport

    out = TimestampNormalizer(CleaningReport()).apply(forecast)
    frame = out.assign(seq=out["TXN_SEQ"].astype("int64"))
    correlation = frame.groupby("ACCOUNT_ID").apply(
        lambda g: g["seq"].rank().corr(g["TXN_TS"].rank()),
        include_groups=False,
    )
    assert correlation.median() > 0.99
