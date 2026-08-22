"""
The never-merge assertions in ``trap_pairs.json``, enforced.

These pairs were settled once, by hand, against MCC and country. Until now the
record of that work lived in prose -- a ``note`` field inside the merchant
master and a table in ARCHITECTURE.md -- and prose cannot fail. Nothing stopped
a later edit from folding ``WTRSTNS`` into ``WAITROSE`` as an alias, and
nothing would have noticed.

Four things are checked, and they are different in kind. The first two are
integrity: the rule file has to talk about merchants that actually exist. The
third is the assertion itself. The fourth checks the *justification* -- that
the evidence named in ``settled_by`` really does separate the members -- so a
group cannot claim MCC settled it while carrying identical MCCs.
"""

import pandas as pd
import pytest

from src.cleaners.merchant import MerchantCleaner
from src.rules import loader

TRAP_GROUPS = loader.trap_pairs()
GROUP_IDS = [group["id"] for group in TRAP_GROUPS]


@pytest.fixture(scope="module")
def aliases():
    """:returns: Every known spelling mapped to its canonical merchant."""
    return loader.merchant_aliases()


@pytest.mark.parametrize("group", TRAP_GROUPS, ids=GROUP_IDS)
def test_every_named_merchant_exists_in_the_master(group, aliases):
    """
    A never-merge rule naming a merchant the master has never heard of is not
    protecting anything. It usually means the master entry was renamed and the
    rule was left behind.
    """
    for member in group["members"]:
        canonical = member["canonical"]
        assert canonical in aliases, (
            f"{group['id']}: {canonical!r} is named in trap_pairs.json but "
            f"has no entry in merchants.json"
        )


@pytest.mark.parametrize("group", TRAP_GROUPS, ids=GROUP_IDS)
def test_every_listed_alias_resolves_to_its_stated_owner(group, aliases):
    """
    The aliases in this file are the confusable spellings -- ``WTRS``,
    ``WTRSTNS``, ``CRM``. Each must resolve to the merchant this file says
    owns it. If one has quietly moved, the trap has already been sprung.
    """
    for member in group["members"]:
        canonical = member["canonical"]
        for alias in member["aliases"]:
            assert aliases.get(alias) == canonical, (
                f"{group['id']}: alias {alias!r} should resolve to "
                f"{canonical!r}, got {aliases.get(alias)!r}"
            )


@pytest.mark.parametrize("group", TRAP_GROUPS, ids=GROUP_IDS)
def test_members_never_collapse_into_one_merchant(group, aliases):
    """
    The assertion this file exists for.

    Every spelling belonging to the group is resolved, and the members must
    land on as many distinct canonical names as there are members. Two of them
    agreeing means the master has merged merchants that are not the same
    business.
    """
    resolved = {}
    for member in group["members"]:
        canonical = member["canonical"]
        for spelling in [canonical, *member["aliases"]]:
            resolved[spelling] = aliases.get(spelling, spelling)

    landing = {member["canonical"]: resolved[member["canonical"]]
               for member in group["members"]}
    assert len(set(landing.values())) == len(group["members"]), (
        f"{group['id']}: members collapsed into one merchant -- {landing}. "
        f"These are different businesses: {group['note']}"
    )


@pytest.mark.parametrize("group", TRAP_GROUPS, ids=GROUP_IDS)
def test_the_stated_evidence_actually_separates_them(group):
    """
    Checks the reasoning, not the outcome.

    ``settled_by: [mcc]`` is a claim that MCC is what tells these merchants
    apart. If two members carry the same MCC, the claim is false and the group
    is resting on an argument that does not hold -- worth catching, because the
    next person to edit the master will read that field and trust it.

    Groups settled by name carry no category evidence and are exempt: the
    TOTAL family is separated by the naming rule itself.
    """
    for field in group["settled_by"]:
        if field == "name":
            continue
        values = [
            member[field] for member in group["members"] if field in member
        ]
        assert len(values) >= 2, (
            f"{group['id']}: claims to be settled by {field!r}, but fewer "
            f"than two members state one"
        )
        assert len(set(values)) == len(values), (
            f"{group['id']}: claims to be settled by {field!r}, but members "
            f"share values {values} -- the stated justification does not hold"
        )


def test_confusable_spellings_survive_the_cleaner(report):
    """
    End to end through the real step rather than the lookup alone.

    The alias map is only half the path: a raw string is normalised first, and
    a normaliser that stripped trailing characters or collapsed repeated
    consonants would merge these before the map ever saw them.
    """
    raw = ["WTRS", "WTRSTNS", "MEZYAN", "MEZYANE", "CRM"]
    frame = pd.DataFrame({"MERCHANT_NAME": raw})

    cleaned = MerchantCleaner(report).apply(frame)["MERCHANT_NAME_CLEANED"]
    resolved = dict(zip(raw, cleaned))

    assert resolved["WTRS"] == "WAITROSE"
    assert resolved["WTRSTNS"] == "WATERSTONES"
    assert resolved["MEZYAN"] == "MEZYAN"
    assert resolved["MEZYANE"] == "MEZYANE"
    assert resolved["CRM"] == "CAREEM", (
        "CRM is Careem (4121, AE), not Crepaway (5812, LB)"
    )


def test_unsettled_cases_are_not_asserted():
    """
    ``TSC S`` against ``SULTAN CENTER`` is recorded as unsettled: both 5411,
    but in different countries, so the evidence disagrees and nothing was
    asserted. It is listed in the file so the *absence* of a decision is
    documented, and it must stay out of the enforced section -- promoting a
    guess to a rule is exactly what the review queue exists to prevent.
    """
    unsettled = loader.load("trap_pairs")["unsettled"]
    enforced = {
        member["canonical"]
        for group in TRAP_GROUPS
        for member in group["members"]
    }
    for case in unsettled:
        assert not enforced.intersection(case["names"]), (
            f"{case['id']} is listed as unsettled but also enforced"
        )
