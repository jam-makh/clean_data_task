"""
Puts rows into ``raw_transactions``, so there is something to emit an event
about.

    python -m scripts.seed_raw --count 3
    python -m scripts.seed_raw --count 5 --offset 1000
    python -m scripts.seed_raw --dirty

The employee's step 2 is "manually insert a transaction into this table", and
this is the repeatable version of that. It is a *convenience*, not part of the
flow: a plain ``INSERT`` typed at psql does the same job, and the consumer
cannot tell the difference. What it buys is that the rows are real -- cut from
the same extract the batch pipeline reads, dirt intact -- so what the consumer
prints is the cleaning actually doing something, rather than eleven stages
reporting nothing to do over a row somebody typed carefully.

Which is what ``--dirty`` is for. Rows in the extract are mostly fine; a run
that cleans a clean row is indistinguishable from a run that cleans nothing.
``--dirty`` picks rows that at least one stage will visibly change.

Values are inserted exactly as the file spells them, including the empty
strings. That is the whole contract of the landing table -- see the header of
sql/raw_schema.sql -- and a seeder that "helpfully" nulled blanks or stripped
whitespace would be doing cleaning, in the one place that must not.

Prints the ids it created, one per line, because the next command needs them:

    python -m scripts.dummy_producer --id <the id this printed>
"""

import argparse
import csv
import sys
from pathlib import Path

from src.db import migrate, raw
from src.db import settings as db_settings

# utf-8-sig for the reason spark_setup.HEADER_ENCODING gives: an Excel-exported
# CSV opens with a byte order mark, and read as plain utf-8 the first column is
# named "﻿USER_ID" -- which is not USER_ID, and every lookup on it misses.
ENCODING = "utf-8-sig"


def rows_from_csv(
    path: Path, count: int, offset: int = 0, dirty: bool = False
) -> list[tuple]:
    """
    Reads rows out of the extract in ``raw.SOURCE_COLUMNS`` order.

    Streams rather than loading the file: the extract is 265k rows and this
    function is asked for three of them.

    :param path: The source CSV.
    :param count: How many rows to take.
    :param offset: Data rows to skip first, so two runs can seed different
        rows rather than the same three every time.
    :param dirty: Take only rows something will visibly clean.
    :returns: One tuple per row, values as text exactly as the file spells
        them.
    :raises ValueError: If the file's header is not the one
        ``raw.SOURCE_COLUMNS`` describes, naming the difference. A positional
        read of a file whose columns moved would insert every value one column
        to the left, and nothing downstream would notice.
    """
    with path.open("r", encoding=ENCODING, newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"{path} is empty: no header row") from None

        if [name.strip() for name in header] != list(raw.SOURCE_COLUMNS):
            raise ValueError(
                f"{path} does not have the columns {raw.TABLE} expects.\n"
                f"  file:  {header}\n"
                f"  table: {list(raw.SOURCE_COLUMNS)}"
            )

        taken: list[tuple] = []
        for position, row in enumerate(reader):
            if position < offset:
                continue
            if len(row) != len(raw.SOURCE_COLUMNS):
                continue
            if dirty and not _is_dirty(row):
                continue
            taken.append(tuple(row))
            if len(taken) >= count:
                break

    if not taken:
        raise ValueError(
            f"{path} yielded no rows at offset {offset}"
            + (" matching --dirty" if dirty else "")
        )
    return taken


# Column positions in SOURCE_COLUMNS order, named so the predicate below reads
# as a sentence rather than as index arithmetic.
_SETTLE_DATE = raw.SOURCE_COLUMNS.index("SETTLE_DATE")
_TXN_AMOUNT = raw.SOURCE_COLUMNS.index("TXN_AMOUNT")
_RUNNING_BALANCE = raw.SOURCE_COLUMNS.index("RUNNING_BALANCE")
_MERCHANT_NAME = raw.SOURCE_COLUMNS.index("MERCHANT_NAME")
_MERCHANT_CITY = raw.SOURCE_COLUMNS.index("MERCHANT_CITY")
_MCC_CODE = raw.SOURCE_COLUMNS.index("MCC_CODE")


def _is_dirty(row) -> bool:
    """
    :param row: A source row, as text.
    :returns: True if at least one stage will visibly change it.

    Deliberately shallow. It is a *selector for a demo*, not a validator, and
    it must not become one -- whether a row is actually dirty is the
    pipeline's answer to give, and a second opinion here that disagreed would
    be the more confusing kind of wrong. The four tests below are the dirt that
    is obvious from the text alone: a missing settlement date, a missing
    balance, an amount written in a European convention, and a merchant name
    still wearing its terminal prefix.
    """
    if not row[_SETTLE_DATE].strip() or not row[_RUNNING_BALANCE].strip():
        return True
    if not row[_MCC_CODE].strip() or not row[_MERCHANT_CITY].strip():
        return True
    if "," in row[_TXN_AMOUNT]:
        return True
    return ":" in row[_MERCHANT_NAME]


def build_parser() -> argparse.ArgumentParser:
    """:returns: The command line."""
    from src.config import runtime

    default_source = runtime.load().paths.source
    parser = argparse.ArgumentParser(
        prog="python -m scripts.seed_raw",
        description=(
            "Insert rows from the extract into raw_transactions and print "
            "their ids. The ids are what the dummy producer publishes."
        ),
    )
    parser.add_argument(
        "-n", "--count", type=int, default=1,
        help="How many rows to insert (default: 1).",
    )
    parser.add_argument(
        "--offset", type=int, default=0,
        help=(
            "Data rows to skip in the source first, so repeated runs seed "
            "different transactions rather than the same ones."
        ),
    )
    parser.add_argument(
        "--dirty", action="store_true",
        help=(
            "Take only rows a stage will visibly change -- a missing "
            "settlement date, a missing balance, a European-formatted amount, "
            "a merchant name with a terminal prefix. Use this when the point "
            "is to watch the cleaning happen."
        ),
    )
    parser.add_argument(
        "-s", "--source", default=str(default_source),
        help=f"The extract to cut rows from (default: {default_source}).",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Print the ids only, for a shell that wants to capture them.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Arguments to parse; ``sys.argv[1:]`` when absent.
    :returns: Exit code -- 0 inserted, 1 bad arguments or unusable source,
        2 the source file is missing.
    """
    args = build_parser().parse_args(argv)
    source = Path(args.source)

    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 2
    if args.count < 1:
        print("--count must be at least 1", file=sys.stderr)
        return 1

    try:
        rows = rows_from_csv(source, args.count, args.offset, args.dirty)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    database = db_settings.load()
    # Called here rather than assumed, on the same principle as the writer:
    # a first run against a fresh container should not need a separate
    # remembered step. It is idempotent, so it costs one round trip.
    migrate.migrate(database)

    ids = raw.insert(database, rows, source=str(source))

    if args.quiet:
        print("\n".join(str(i) for i in ids))
        return 0

    print(f"Inserted {len(ids)} row(s) into {raw.TABLE} from {source}")
    for identifier, row in zip(ids, rows):
        txn = row[raw.SOURCE_COLUMNS.index("TXN_ID")]
        merchant = row[_MERCHANT_NAME] or "(no merchant)"
        print(f"  id={identifier}  txn_id={txn}  {merchant}")
    print("\nEmit them with:")
    print(f"  python -m scripts.dummy_producer --ids {','.join(map(str, ids))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
