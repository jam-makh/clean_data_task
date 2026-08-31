"""
The manual emitter: tells the consumer that a row arrived.

    python -m scripts.dummy_producer --id 42
    python -m scripts.dummy_producer --ids 42,43,44
    python -m scripts.dummy_producer --pending
    python -m scripts.dummy_producer --id 42 --dry-run

Dummy in the sense that nothing here decides when to emit (a person does,
by running it). All three produce exactly the message this script produces,
which is the point -- the consumer cannot tell the difference, so replacing
this with a real emitter later changes nothing downstream.

What it is NOT is a second way to run the pipeline. It publishes an id and
exits. Whether anything happens next depends entirely on whether a consumer is
listening, and if none is, the message waits on the topic until one is.

NOTES:
1. --pending emits every row still at ``status = 'PENDING'`` -- rows that
were never attempted, because the consumer was down when they landed. It is
safe to run repeatedly: the id is derived, the write is an upsert, and a row
cleaned twice is a row cleaned once -- see ``src/jobs.py``.
2. It does not pick up FAILED rows and a bulk re-emit of failures whose cause is
still unfixed would simply fail them all again. To retry one once you have
fixed the cause, pass its id to ``--ids``; ``last_error`` in
``raw_transactions`` says which ids those are and why.
"""

import argparse
import sys

from src.db import raw
from src.db import settings as db_settings
from src.kafka import ingest_events, producer
from src.kafka import settings as kafka_settings


def emit(row_id: int, broker=None, table: str = raw.TABLE) -> dict:
    """
    Builds and publishes one ingest event.

    :param row_id: The row that landed.
    :param broker: Where to publish; loaded from config and environment when
        absent.
    :param table: Which table it landed in.
    :returns: The payload that was published, so a caller can print or assert
        on exactly what went out rather than on a reconstruction of it.
    :raises PublishError: If the broker did not acknowledge it.
    """
    broker = broker if broker is not None else kafka_settings.load()
    payload = ingest_events.build(row_id, table=table)
    producer.publish(
        payload,
        broker,
        topic=broker.raw_topic,
        key=ingest_events.key_for(payload),
    )
    return payload


def parse_ids(text: str) -> list[int]:
    """
    :param text: Ids as typed: ``42``, ``42,43``, ``40-44``, or a mix.
    :returns: Them as a sorted list of ints.
    :raises ValueError: On anything that is not an id or a range, naming the
        part that failed rather than the whole string.
    """
    found: list[int] = []
    for part in text.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part.lstrip("-"):
            low, _, high = part.partition("-")
            try:
                low, high = int(low), int(high)
            except ValueError:
                raise ValueError(f"{part!r} is not a range of ids") from None
            if high < low:
                raise ValueError(f"{part!r} counts backwards")
            found.extend(range(low, high + 1))
        else:
            try:
                found.append(int(part))
            except ValueError:
                raise ValueError(f"{part!r} is not a row id") from None
    return sorted(set(found))


def build_parser() -> argparse.ArgumentParser:
    """:returns: The command line."""
    parser = argparse.ArgumentParser(
        prog="python -m scripts.dummy_producer",
        description=(
            "Publish 'this row arrived' events for rows already in "
            "raw_transactions. Stands in for whatever would really emit them."
        ),
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--id", type=int, help="One row id to announce.",
    )
    selection.add_argument(
        "--ids",
        help="Several: '42,43' or a range '40-44', or both.",
    )
    selection.add_argument(
        "--pending",
        action="store_true",
        help=(
            "Every row still PENDING, oldest first -- the ones that were "
            "never attempted. Not FAILED rows; retry those by id."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=100,
        help="With --pending, the most ids to emit (default: 100).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Refuse to announce an id that is not in the table. Off by "
            "default: the producer's job is to say a row arrived, and a "
            "consumer that is told about a row it cannot find should say so "
            "itself -- which is a thing worth being able to demonstrate."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the payloads that would be published, and publish none.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Arguments to parse; ``sys.argv[1:]`` when absent.
    :returns: Exit code -- 0 published, 1 bad arguments or nothing to publish,
        3 the broker did not acknowledge. 3 rather than 1 to match main.py,
        where it already means "the work is safe, only the announcement
        failed".
    """
    args = build_parser().parse_args(argv)

    if args.pending:
        ids = raw.pending_ids(db_settings.load(), limit=args.limit)
        if not ids:
            print(
                "Nothing pending: every row has been attempted. Any that "
                "failed are FAILED, not PENDING -- retry those by id."
            )
            return 1
    elif args.ids:
        try:
            ids = parse_ids(args.ids)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
    else:
        ids = [args.id]

    if not ids:
        print("No ids to announce.", file=sys.stderr)
        return 1

    if args.check:
        database = db_settings.load()
        known = {row[0] for row in raw.fetch(database, ids)}
        missing = [i for i in ids if i not in known]
        if missing:
            print(
                f"not in {raw.TABLE}: {missing}. Seed rows with "
                f"`make seed-raw`, or drop --check to announce them anyway.",
                file=sys.stderr,
            )
            return 1

    if args.dry_run:
        for row_id in ids:
            payload = ingest_events.build(row_id, table=raw.TABLE)
            print(ingest_events.encode(payload).decode("utf-8"))
        return 0

    broker = kafka_settings.load()
    # Created here rather than assumed, for the reason the Makefile's
    # kafka-topic target gives: auto-create is off, so a producer aimed at a
    # topic nobody made would fail with "unknown topic" -- which reads like a
    # broker problem and is really a setup step nobody ran.
    producer.ensure_topic(broker, broker.raw_topic)

    published = 0
    try:
        for row_id in ids:
            payload = emit(row_id, broker)
            print(
                f"-> {broker.raw_topic}  key={ingest_events.key_for(payload)}"
                f"  {ingest_events.encode(payload).decode('utf-8')}"
            )
            published += 1
    except producer.PublishError as exc:
        # Reported with a count, because a partial run matters here: the ids
        # already published will be cleaned, and re-running for the rest is
        # safe but re-running for all of them is safer and equally correct.
        print(
            f"\n{published} of {len(ids)} published, then: {exc}",
            file=sys.stderr,
        )
        return 3

    print(f"\nPublished {published} event(s) to {broker.raw_topic}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
