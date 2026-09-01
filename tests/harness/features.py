"""
A deterministic synthetic cleaned-transactions frame for the Stage 3 tests.

Small enough to reason about by hand, and shaped to exercise the cases the
build has to get right: quiet months, staggered account openings, and debits
that are not spending.

Built as a Spark DataFrame, because that is what Stage 3 consumes. The rows
are declared as plain dicts and handed to ``createDataFrame`` with an explicit
schema -- inference over eleven rows would decide the types from whatever the
first row happened to hold.
"""

import datetime
import uuid

from features import source

# Namespace for the fixture's identifiers. Any constant uuid does; this one is
# the fixture's own so a synthetic id cannot collide with a real one.
_NAMESPACE = uuid.UUID("9f2c1a4e-7b30-5d68-a1c2-0e5f8d3b647a")


def handle(name: str) -> str:
    """
    The uuid a short fixture handle stands for.

    ``user_id`` and ``account_id`` are UUID columns, so the fixture cannot key
    on ``"u1"`` any more. The tests still say ``"u1"`` -- a six-row frame you
    reason about by hand is unreadable spelled in uuids -- and this is the one
    place the two representations meet.

    uuid5, so a handle is the same id on every run and in every test. Nothing
    depends on the specific value; what matters is that it is stable and that
    distinct handles stay distinct.

    :param name: A short handle such as ``"u1"`` or ``"u1a"``.
    :returns: Its uuid, as a string.
    """
    return str(uuid.uuid5(_NAMESPACE, name))

# The month every account in the fixture is silent, so the dense spine and the
# carry-forward have something to prove.
QUIET_MONTH = datetime.date(2022, 4, 1)

# One row of the vocabulary per shape of transaction the tests care about.
#
# 23 is Transfer Out: a real debit that is not spending, which is the case the
# spend-eligibility rule exists for. 26 is Settlement Credit. 99 is not in the
# rule file at all, so it declares no direction and must enter neither total.
SHAPES = {
    "purchase": ("00", "5411", "CARREFOUR"),
    "dining": ("00", "5812", "ZAATAR W ZEIT"),
    "atm": ("01", "6011", "ATM"),
    "transfer_out": ("23", "6012", "INTERNAL TRANSFER"),
    "fee": ("24", "6011", "ATM"),
    "salary": ("21", "6012", "CARD SETTLEMENT"),
    "unmapped": ("00", "1111", "SOMEWHERE NEW"),
    "undeclared": ("99", "5411", "CARREFOUR"),
}

# Stated rather than inferred, for the reason in the module docstring. The
# names and order match what ``source.COLUMNS`` selects.
SCHEMA = (
    "user_id string, "
    "account_id string, "
    "txn_seq long, "
    "txn_ts timestamp, "
    "billing_amount double, "
    "billing_currency string, "
    "processing_code_cleaned string, "
    "running_balance_normalized double, "
    "running_balance_status string, "
    "mcc_code_cleaned string, "
    "merchant_name_cleaned string"
)


def transaction(
    user: str,
    account: str,
    month: str,
    shape: str,
    amount: float,
    balance: float,
    seq: int,
    day: int = 5,
    status: str = "OBSERVED",
) -> dict:
    """
    One cleaned transaction, spelled the way ``source`` hands them over.

    :param user: Owning user, as a short handle -- see ``handle``.
    :param account: Owning account, likewise.
    :param month: Month as ``YYYY-MM``.
    :param shape: A key of ``SHAPES``.
    :param amount: Unsigned magnitude in USD.
    :param balance: The running balance after this row, in USD.
    :param seq: Position in the account's chain.
    :param day: Day of month, so several rows can share a month in order.
    :param status: The running-balance status this row carries.
    :returns: The row.
    """
    code, mcc, merchant = SHAPES[shape]
    year, month_number = (int(part) for part in month.split("-"))
    # Credits arrive positive and debits negative, matching what the source
    # writes. The build derives direction from the code regardless.
    signed = amount if code in ("21", "26") else -amount

    return {
        "user_id": handle(user),
        "account_id": handle(account),
        "txn_seq": int(seq),
        "txn_ts": datetime.datetime(year, month_number, day, 10, 0, 0),
        "billing_amount": float(signed),
        "billing_currency": "USD",
        "processing_code_cleaned": code,
        "running_balance_normalized": float(balance),
        "running_balance_status": status,
        "mcc_code_cleaned": mcc,
        "merchant_name_cleaned": merchant,
    }


def frame(spark, rows: list[dict]):
    """
    :param spark: The session.
    :param rows: Rows from ``transaction``.
    :returns: The frame typed as ``source`` would have returned it, with
        ``month`` derived.
    """
    ordered = [
        {key: row[key] for key in _FIELD_ORDER} for row in rows
    ]
    return source.typed(spark.createDataFrame(ordered, schema=SCHEMA))


_FIELD_ORDER = (
    "user_id",
    "account_id",
    "txn_seq",
    "txn_ts",
    "billing_amount",
    "billing_currency",
    "processing_code_cleaned",
    "running_balance_normalized",
    "running_balance_status",
    "mcc_code_cleaned",
    "merchant_name_cleaned",
)


def simple_rows() -> list[dict]:
    """
    Two users over six months, with a quiet month and a late second account.

    ``u1`` holds one account throughout. ``u2`` opens a second account in
    March, which is what makes ``accounts_held`` observable. Nobody transacts
    in April.

    :returns: The rows, before they become a frame.
    """
    return [
        transaction("u1", "u1a", "2022-01", "salary", 1000, 1000, 1),
        transaction("u1", "u1a", "2022-02", "purchase", 200, 800, 2),
        transaction("u1", "u1a", "2022-02", "transfer_out", 100, 700, 3, 20),
        transaction("u1", "u1a", "2022-03", "dining", 50, 650, 4),
        transaction("u1", "u1a", "2022-05", "atm", 150, 500, 5),
        transaction("u1", "u1a", "2022-06", "fee", 25, 475, 6),

        transaction("u2", "u2a", "2022-01", "salary", 2000, 2000, 7),
        transaction("u2", "u2a", "2022-02", "purchase", 300, 1700, 8),
        transaction("u2", "u2b", "2022-03", "salary", 500, 500, 9),
        transaction("u2", "u2a", "2022-03", "unmapped", 100, 1600, 10),
        transaction("u2", "u2a", "2022-05", "undeclared", 40, 1600, 11),
        transaction("u2", "u2b", "2022-06", "dining", 60, 440, 12),
    ]


def simple(spark):
    """
    :param spark: The session.
    :returns: The cleaned transactions of ``simple_rows``.
    """
    return frame(spark, simple_rows())


def rows_by_key(table, *keys) -> dict:
    """
    Collects a small built frame into a dict, for assertions.

    The one place a test is allowed to leave Spark. Reading four rows back to
    assert on them is the assertion, not the pipeline -- nothing under
    ``features`` does this, and ``test_features_engine`` is what keeps
    that true.

    :param table: The frame to collect.
    :param keys: Columns forming the key of the returned dict.
    :returns: Key tuple (or bare key, for one column) to the row as a dict.
    """
    collected = {}
    for row in table.collect():
        data = row.asDict()
        key = tuple(data[name] for name in keys)
        collected[key[0] if len(key) == 1 else key] = data
    return collected
