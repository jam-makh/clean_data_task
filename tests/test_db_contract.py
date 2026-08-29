"""
The frame-to-table contract, checked without a database and without a JVM.

Both of the things this file compares -- ``src/db/contract.py`` and
``sql/schema.sql`` -- are static text, so the question "do they still agree"
does not need Postgres running to answer. That matters more than it sounds:
the way this contract breaks is that someone adds a column to the table, or a
stage stops producing one, and the failure otherwise surfaces as a driver
error part-way through a write. A check that needs a container is a check that
gets skipped on the machine where it would have caught something.

What needs a real database is in ``test_db_write.py`` and is marked ``db``.
"""

import re
from pathlib import Path

import pytest

from src.config.errors import ConfigError
from src.db import contract, migrate

SCHEMA = Path("sql/schema.sql")

# Columns the table fills itself. Absent from the contract on purpose, so the
# comparison below has to know about them rather than silently tolerating any
# mismatch it happens to find.
SELF_FILLED = {"cleaned_at"}


def schema_columns() -> dict[str, str]:
    """
    :returns: Column name to declared SQL type, in declaration order, read
        from the DDL rather than from a running database -- the file is the
        thing under review, and a database could be out of date with it.
    """
    text = SCHEMA.read_text(encoding="utf-8")
    body = text.split(f"CREATE TABLE IF NOT EXISTS {contract.TABLE} (", 1)[1]
    body = body.split("\n);", 1)[0]

    columns: dict[str, str] = {}
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        match = re.match(r"([a-z_]+)\s+([A-Z]+(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?)", line)
        if match:
            columns[match.group(1)] = match.group(2).replace(" ", "")
    return columns


def test_the_schema_parses():
    """
    The parser above is load-bearing for every test below, so it gets its own
    assertion. A regex that quietly matched nothing would make the whole file
    pass while comparing two empty sets.
    """
    columns = schema_columns()

    assert len(columns) > 20, f"parsed only {len(columns)} columns: {columns}"
    assert columns["txn_id_cleaned"] == "TEXT"
    assert columns["txn_seq"] == "BIGINT"
    assert columns["fx_rate"] == "NUMERIC(20,10)"


def test_every_table_column_is_written_or_self_filled():
    """
    Nothing in the table is left unaccounted for.

    The failure this catches is adding a column to sql/schema.sql and not to
    the contract: the write then succeeds and the column is null or defaulted
    on every row, which looks like a data problem rather than a wiring one.
    """
    unaccounted = set(schema_columns()) - set(contract.TARGET) - SELF_FILLED

    assert not unaccounted, (
        f"{contract.TABLE} declares column(s) the writer never sends: "
        f"{sorted(unaccounted)}. Add them to contract.COLUMNS, or to "
        f"SELF_FILLED here if the database fills them."
    )


def test_the_writer_sends_nothing_the_table_lacks():
    """
    The mirror of the above, and the one that fails loudly rather than
    quietly: a column the table does not have is a driver error at write time.
    """
    unknown = set(contract.TARGET) - set(schema_columns())

    assert not unknown, (
        f"the writer sends column(s) {contract.TABLE} does not declare: "
        f"{sorted(unknown)}"
    )


def test_insert_order_matches_declaration_order():
    """
    Not required for correctness -- the INSERT names its columns -- and worth
    holding anyway. The two lists are read side by side by anyone changing
    either, and a shared order is what makes that reading cheap.
    """
    declared = [c for c in schema_columns() if c not in SELF_FILLED]

    assert contract.TARGET == declared


@pytest.mark.parametrize(
    "column,precision",
    [
        ("txn_amount_cleaned", "decimal(18,4)"),
        ("billing_amount", "decimal(18,4)"),
        ("fx_rate", "decimal(20,10)"),
        ("running_balance_filled", "decimal(18,4)"),
        ("running_balance_normalized", "decimal(18,4)"),
        ("interest_rate_index_cleaned", "decimal(10,4)"),
        ("inflation_index_cleaned", "decimal(10,4)"),
    ],
)
def test_decimal_casts_match_the_column_they_land_in(column, precision):
    """
    A cast narrower than the column rounds on the way in, and Postgres accepts
    it without complaint -- the value is valid for the column, it is just not
    the value the pipeline computed. Money is the whole subject here, so the
    precisions are asserted pair by pair rather than trusted to review.
    """
    frame_name = column.upper()

    assert contract.COLUMNS[frame_name] == precision
    assert schema_columns()[column] == precision.upper().replace("DECIMAL", "NUMERIC")


def test_the_key_is_never_overwritten():
    """The upsert must not update the column it matched on."""
    assert contract.KEY in contract.IMMUTABLE
    assert f"{contract.KEY} = EXCLUDED" not in contract.merge_statement("s")


def test_the_merge_updates_every_other_column():
    """
    An upsert that inserts new rows but does not refresh existing ones is the
    subtle half-working version of this feature: re-running a corrected load
    would leave the old values in place and report success.
    """
    statement = contract.merge_statement(migrate.STAGING)

    for name in contract.TARGET:
        if name in contract.IMMUTABLE:
            continue
        assert f"{name} = EXCLUDED.{name}" in statement, f"{name} not refreshed"


def test_the_merge_refreshes_the_write_timestamp():
    """
    cleaned_at means "when the pipeline last wrote this row", so an upsert
    that rewrites a row has to move it. Left out, the column would mean
    first-written on updated rows and last-written on new ones.
    """
    assert "cleaned_at = now()" in contract.merge_statement(migrate.STAGING)


def test_the_merge_tolerates_a_duplicate_key_in_one_batch():
    """
    ON CONFLICT raises if a single statement presents the same key twice, and
    the error names the constraint rather than the batch that caused it.
    """
    assert f"DISTINCT ON ({contract.KEY})" in contract.merge_statement("s")


def test_a_missing_column_is_reported_with_its_name():
    """
    All of them at once, not the first one. A writer that fails on the first
    missing column makes you re-run an eleven-stage pipeline to discover the
    second.
    """
    class FrameWithoutColumns:
        columns: list[str] = []

    with pytest.raises(ConfigError) as raised:
        contract.project(FrameWithoutColumns(), "00000000-0000-0000-0000-000000000000")

    message = str(raised.value)
    assert "TXN_ID_CLEANED" in message
    assert "FX_RATE" in message
