"""
The sample the parity harness runs on, chosen rather than taken.

265k rows through both pipelines on every assertion is not a test, it is a
build step -- and the first 500 rows are not a sample, they are January.
This module cuts a subset small enough to iterate on and specific enough that
passing on it means something.

Two properties decide the whole design.

**It samples accounts, not rows.** ``BalanceReconstructor`` works over
``partitionBy(ACCOUNT_ID).orderBy(TXN_SEQ)``: a balance is the previous
balance plus the transactions between them. Take every third row and every
chain in the file breaks, every gap becomes unclosable, and the balance stage
reports UNVERIFIED on a sample where the real run reports DERIVED. The
comparison would still pass -- both engines would agree on the same wrong
answer -- and would prove nothing about the stage it was written for. So a
selected account brings all of its rows.

**The accounts are chosen for what they contain.** Five strata, each aimed at
a specific thing a stage can get wrong, then a deterministic fill. The
selection is a pure function of the file: the same input file produces the
same sample on every machine and every run, which is the same determinism
requirement the upsert key carries in Phase 06 and for the same reason -- a
harness whose sample drifts turns one failing stage into an unreproducible
report.

Determinism here rules out ``hash()``, which is salted per process, and
``DataFrame.sample``, whose seeding is a numpy implementation detail. The fill
stratum orders accounts by a SHA-256 of the account id: stable across
processes, across versions, and across machines, and uncorrelated with the
account id's own ordering, which is what makes it a fill rather than a slice.
"""

import hashlib
import json
import re
from pathlib import Path

import pandas as pd

from src.rules import loader

# Bump when the strategy below changes. It is written into the manifest and
# compared on load, so a sample cut by an older rule set is rebuilt rather
# than silently reused -- a stale sample is the one failure mode of a cache
# that nobody notices, because everything still passes.
SPEC_VERSION = 1

SOURCE = Path("data/raw/forecast_balance_data.csv")
SAMPLE = Path("data/interim/parity_sample.csv")

# Roughly 16 accounts at a median of 369 rows lands near 6,000 rows: the
# pandas pipeline runs it in a couple of seconds, which is the difference
# between a harness that runs after every edit and one that runs at the end
# of the day.
ACCOUNTS = 16

GROUP = "ACCOUNT_ID"
ORDER = "TXN_SEQ"
TIMESTAMP = "TXN_DATE_TIME"
MERCHANT = "MERCHANT_NAME"
BALANCE = "RUNNING_BALANCE"


def _shape(value: str) -> str:
    """
    :param value: A raw cell.
    :returns: Its shape with the content removed -- digits to ``9``, letters
        to ``A``, everything else kept. ``"2022-01-01 07:11:25"`` and
        ``"1640988000"`` are two shapes; ``"03-Jan-22"`` is a third. Grouping
        by shape is what lets the sampler ask for one of each *format* without
        being told what the formats are, which matters because the formats
        live in ``src/rules/json`` and this module has no business duplicating
        them.
    """
    return re.sub(r"\d", "9", re.sub(r"[A-Za-z]", "A", str(value)))


def _trap_names() -> set[str]:
    """
    :returns: Every canonical name and alias the never-merge file protects,
        uppercased. Read from the rule file rather than listed here, so a new
        trap pair starts being sampled for the moment it is declared.
    """
    names: set[str] = set()
    for group in loader.trap_pairs():
        for member in group.get("members", []):
            names.add(str(member["canonical"]).upper())
            names.update(str(a).upper() for a in member.get("aliases", []))
    return names


def _stable_order(accounts) -> list[str]:
    """
    :param accounts: Account ids.
    :returns: Them, ordered by a SHA-256 of the id. Deterministic everywhere
        and unrelated to the ids' own sort order, so taking a prefix of this
        is a sample rather than a slice of the alphabet.
    """
    return sorted(
        accounts,
        key=lambda a: hashlib.sha256(str(a).encode("utf-8")).hexdigest(),
    )


def _greedy_cover(by_account: dict[str, set], budget: int) -> list[str]:
    """
    Picks accounts that between them carry the most distinct values.

    :param by_account: Account id to the set of values it contains.
    :param budget: How many accounts to return at most.
    :returns: Accounts in the order chosen. Greedy set cover: repeatedly take
        the account adding the most values not yet covered, breaking ties by
        the stable order above so the result does not depend on dict ordering.
    """
    covered: set = set()
    chosen: list[str] = []
    remaining = dict(by_account)
    while remaining and len(chosen) < budget:
        best = max(
            _stable_order(remaining),
            key=lambda account: len(remaining[account] - covered),
        )
        gain = remaining.pop(best) - covered
        if not gain:
            break
        covered |= gain
        chosen.append(best)
    return chosen


def choose_accounts(frame: pd.DataFrame, budget: int = ACCOUNTS) -> list[str]:
    """
    The five strata, in priority order, then the fill.

    :param frame: The full source, read as text.
    :param budget: How many accounts the sample may contain.
    :returns: Account ids, each stratum's contribution in the order chosen and
        duplicates removed. Fewer than ``budget`` only when the file has
        fewer accounts than that.

    The strata:

    1. **The balance seam.** The source's stated balances close on the early
       rows and stop closing partway through, because the later ones were
       built from ``BILLING_AMOUNT`` and are in a different currency than the
       figure they were applied to. An account confined to one side would
       exercise one branch of the stage; the widest ``TXN_SEQ`` spans are the
       accounts that cross it. Where the seam falls is never asserted here --
       ``BalanceReconstructor`` refuses to hardcode a row number for good
       reasons, and a sampler that hardcoded one would smuggle the same fact
       back in through the back door.
    2. **The edges of the seam.** The accounts with the narrowest spans, which
       are the ones that live entirely on one side -- the case where an
       account never gets a second anchor and every row must come back
       UNKNOWN or UNVERIFIED.
    3. **Trap pairs.** Accounts naming a merchant the never-merge file
       protects, so requirement 8's first named test case has rows to run on.
    4. **Timestamp formats.** Greedy cover over the shapes of
       ``TXN_DATE_TIME``, so the sample carries an epoch integer, an ISO
       string and a day-first string rather than whichever the first account
       happened to use.
    5. **Missingness.** The accounts withholding the most running balances,
       which is where the fill and the withhold decisions both live.
    """
    accounts = frame[GROUP]
    picked: list[str] = []

    def take(candidates, limit: int) -> None:
        """
        Adds up to ``limit`` accounts this stratum has not already got.

        Per-stratum limits rather than one shared budget, so an early stratum
        with many candidates cannot starve a later one. The last call passes
        the whole budget as its limit, which is what makes it the fill.
        """
        added = 0
        for account in candidates:
            if added >= limit or len(picked) >= budget:
                return
            if account not in picked:
                picked.append(account)
                added += 1

    if ORDER in frame.columns:
        sequence = pd.to_numeric(frame[ORDER], errors="coerce")
        span = sequence.groupby(accounts).agg(["min", "max"])
        width = (span["max"] - span["min"]).sort_values(ascending=False)
        take(list(width.index[:4]), 4)
        take(list(width.index[-2:]), 2)

    if MERCHANT in frame.columns:
        traps = _trap_names()
        if traps:
            pattern = "|".join(sorted(re.escape(name) for name in traps))
            hit = frame[MERCHANT].fillna("").str.upper().str.contains(
                pattern, regex=True
            )
            if hit.any():
                # Most trap-name rows first: an account with two of them is
                # worth more than two accounts with one each.
                ranked = hit.groupby(accounts).sum().sort_values(ascending=False)
                take([a for a in ranked.index if ranked[a] > 0][:3], 3)

    if TIMESTAMP in frame.columns:
        shapes = frame[TIMESTAMP].fillna("").map(_shape)
        by_account = {
            account: set(values)
            for account, values in shapes.groupby(accounts)
        }
        take(_greedy_cover(by_account, 4), 4)

    if BALANCE in frame.columns:
        blank = frame[BALANCE].isna().groupby(accounts).mean()
        take(list(blank.sort_values(ascending=False).index[:2]), 2)

    take(_stable_order(accounts.unique()), budget)
    return picked[:budget]


def build(
    source: str | Path = SOURCE,
    destination: str | Path = SAMPLE,
    budget: int = ACCOUNTS,
) -> Path:
    """
    Cuts the sample and writes it beside a manifest describing it.

    Rows keep their original file order, and the file keeps its header, so the
    sample is a source file in its own right: the pipeline, the profile
    detector and both readers treat it exactly as they treat the full extract.

    :param source: The full extract.
    :param destination: Where to write the sample.
    :param budget: How many accounts it may contain.
    :returns: The path written.
    """
    source, destination = Path(source), Path(destination)
    frame = pd.read_csv(source, dtype=str, keep_default_na=False, na_values=[""])
    accounts = choose_accounts(frame, budget)
    subset = frame[frame[GROUP].isin(accounts)]

    destination.parent.mkdir(parents=True, exist_ok=True)
    # index=False and na_rep="" so the written file spells a null the way the
    # source does. A sample whose blanks read "nan" would hand every stage a
    # value the real file never contains.
    subset.to_csv(destination, index=False, na_rep="")
    _manifest(destination).write_text(
        json.dumps(
            {
                "spec_version": SPEC_VERSION,
                "source": str(source),
                # Size rather than mtime: mtime changes on every clone and
                # checkout, which would rebuild the sample constantly, and it
                # does not change when a file is edited in place to the same
                # length -- so it is both too sensitive and not sensitive
                # enough. A byte count is neither.
                "source_bytes": source.stat().st_size,
                "budget": budget,
                "accounts": sorted(accounts),
                "rows": int(len(subset)),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return destination


def _manifest(destination: Path) -> Path:
    """:returns: The sidecar path describing a sample."""
    return destination.with_suffix(".manifest.json")


def ensure(
    source: str | Path = SOURCE,
    destination: str | Path = SAMPLE,
    budget: int = ACCOUNTS,
) -> Path:
    """
    Returns the cached sample, rebuilding it when it no longer describes the
    source it claims to come from.

    :param source: The full extract.
    :param destination: Where the sample lives.
    :param budget: How many accounts it may contain.
    :returns: The sample path.
    :raises FileNotFoundError: If the source is absent and no sample is
        cached -- the one case where there is nothing to do and nothing to
        say about it beyond which file is missing.
    """
    source, destination = Path(source), Path(destination)
    manifest = _manifest(destination)

    if destination.exists() and manifest.exists():
        try:
            recorded = json.loads(manifest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            recorded = {}
        current = {
            "spec_version": SPEC_VERSION,
            "source": str(source),
            "budget": budget,
        }
        matches = all(recorded.get(k) == v for k, v in current.items())
        if matches and (
            not source.exists()
            or recorded.get("source_bytes") == source.stat().st_size
        ):
            return destination

    if not source.exists():
        raise FileNotFoundError(
            f"no cached sample at {destination} and the source it would be "
            f"cut from is absent: {source}"
        )
    return build(source, destination, budget)
