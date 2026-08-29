"""
One line describing the whole cleaned table, for proving a replay changed
nothing.

    python -m scripts.fingerprint

Run it, replay an event, run it again. The counts and the digest are identical
if the write was idempotent; the timestamps are not, because an upsert that
rewrote a row did exactly that.

Exists so the idempotency check does not require a psql session, a GUI or a
remembered query. The claim being demonstrated is a property of the *table*,
so the check has to read the table -- but reading it should be one command.

The digest covers the key and the amount rather than every column. A hash over
all of them would change when `cleaned_at` moves, which is precisely the field
an idempotent upsert is expected to move, and the check would fail on the
behaviour it is meant to confirm.
"""

import sys

from src.db import contract
from src.db import settings as db_settings

# `rows` and `keys` are separately interesting: equal counts mean the primary
# key is doing its job, and a gap between them would be impossible -- it is
# there so that an impossible number is visible rather than assumed away.
QUERY = f"""
SELECT count(*)                        AS rows,
       count(DISTINCT {contract.KEY})  AS keys,
       coalesce(sum(txn_amount_cleaned), 0) AS total,
       count(DISTINCT sync_job_id)     AS loads,
       max(cleaned_at)                 AS last_write,
       md5(string_agg({contract.KEY} || ':' || txn_amount_cleaned,
                      ',' ORDER BY {contract.KEY})) AS digest
  FROM {contract.TABLE}
"""


def read(database=None) -> dict:
    """
    :param database: Where to look; loaded from config and environment when
        absent.
    :returns: The row as a dict, digest included. Returns rather than prints so
        a test can assert on two calls being equal.
    """
    import psycopg2

    database = database if database is not None else db_settings.load()
    with psycopg2.connect(database.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(QUERY)
            names = [column.name for column in cursor.description]
            return dict(zip(names, cursor.fetchone()))


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Unused; present so this matches the other scripts' shape.
    :returns: 0, or 1 if the table cannot be read.
    """
    try:
        state = read()
    except Exception as exc:  # noqa: BLE001 -- the message is the whole point
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    if not state["rows"]:
        print("cleaned_transactions is empty -- nothing to fingerprint yet.")
        return 0

    print(f"rows       {state['rows']}")
    print(f"keys       {state['keys']}")
    print(f"total      {state['total']}")
    print(f"loads      {state['loads']}")
    print(f"last write {state['last_write']}")
    print(f"digest     {state['digest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
