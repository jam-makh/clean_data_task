"""
The Stage 3 rule vocabularies: the JSON seed, the Postgres tables, and that the
two say the same thing.

The database tests are marked ``db`` and skip when the container is down.
"""

import pytest

from src.db import settings as db_settings
from src.rules import loader, store
from src.rules.loader import CREDIT, DEBIT


@pytest.fixture(scope="module")
def rules():
    """:returns: The vocabularies, read from the rule files."""
    return store.from_json()


@pytest.fixture(scope="module")
def database():
    """
    :returns: The connection settings, with the rule tables applied. Skips
        rather than fails when nothing answers on the port.
    """
    settings = db_settings.load()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        psycopg2.connect(settings.dsn).close()
    except Exception as exc:  # noqa: BLE001 - the message is the point
        pytest.skip(
            f"Postgres not reachable at {settings} ({type(exc).__name__}). "
            f"Run `make verify` -- it names the cause."
        )
    store.migrate(settings)
    return settings


def test_every_spend_eligible_code_is_a_debit(rules):
    """
    Spending is debit-only. A credit that claims to be spending is incoherent
    whichever half is the mistake, and the table has a CHECK saying so.
    """
    for code in rules.spend_eligible:
        assert rules.directions.get(code) == DEBIT, code


def test_transfer_out_is_a_debit_that_is_not_spending(rules):
    """
    The decision the build was designed around, asserted against the rule
    files rather than against the code that reads them.
    """
    assert rules.directions["23"] == DEBIT
    assert "23" not in rules.spend_eligible


def test_a_fee_is_a_debit_that_is_spending(rules):
    """The other half of the same decision."""
    assert rules.directions["24"] == DEBIT
    assert "24" in rules.spend_eligible


def test_no_credit_is_spend_eligible(rules):
    """Every declared credit is excluded from the spending split."""
    credits = [
        code
        for code, direction in rules.directions.items()
        if direction == CREDIT
    ]
    assert credits
    assert not set(credits) & rules.spend_eligible


def test_every_declared_code_has_an_eligibility_verdict(rules):
    """
    A code the eligibility map has never seen would default to not-spending
    silently. Making it explicit is what keeps that a decision.
    """
    eligibility = loader.spend_eligible_codes()
    assert set(eligibility) == set(rules.labels)


def test_exactly_one_category_is_the_residual(rules):
    """
    An unmapped MCC needs exactly one place to go. Zero leaves it nowhere; two
    makes the destination depend on iteration order.
    """
    assert rules.residual in rules.categories
    assert rules.categories.count(rules.residual) == 1


def test_every_mapped_mcc_names_a_declared_category(rules):
    """
    A typo here would produce an eighth spending column that nothing declared
    and the contract would then reject the whole table.
    """
    assert set(rules.mcc_categories.values()) <= set(rules.categories)


def test_every_mcc_the_merchant_master_asserts_is_mapped(rules):
    """
    The merchant master is where a human pins an MCC to a merchant. One
    asserted there and absent here would fall to the residual without anyone
    deciding it should.
    """
    asserted = {
        entry["mcc"]
        for entry in loader.merchants().values()
        if "mcc" in entry
    }
    assert asserted <= set(rules.mcc_categories)


def test_the_catch_all_mcc_maps_to_the_residual(rules):
    """
    ``mcc_rules.json`` declares 5999 a catch-all with no positive meaning.
    Routing it to a real category would assert something the code does not
    carry.
    """
    catch_all = loader.mcc_rules()["catch_all"]
    assert rules.mcc_categories[catch_all] == rules.residual


def test_an_unmapped_mcc_falls_to_the_residual(rules):
    """Including no MCC at all, which the source sometimes carries."""
    assert rules.category_of("0000") == rules.residual
    assert rules.category_of(None) == rules.residual


@pytest.mark.db
def test_the_tables_say_what_the_rule_files_say(database, rules):
    """
    The seed is a projection of the rule files, so the reviewed source stays
    in git and the served copy cannot drift from it unnoticed.
    """
    written = store.seed(database, rules)
    assert written["rule_processing_codes"] == len(rules.labels)
    assert written["rule_spending_categories"] == len(rules.categories)
    assert written["rule_mcc_categories"] == len(rules.mcc_categories)

    served = store.from_database(database)
    assert served == rules


@pytest.mark.db
def test_seeding_twice_leaves_the_same_tables(database, rules):
    """
    Delete-then-insert rather than upsert, so a rule the files no longer
    declare cannot survive in the table where a build would still read it.
    """
    store.seed(database, rules)
    first = store.from_database(database)
    store.seed(database, rules)
    assert store.from_database(database) == first
