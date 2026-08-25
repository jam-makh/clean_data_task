"""
The cleaning consumer, as a process.

    python consumer.py                 listen until stopped
    python consumer.py --once          clean one batch and exit
    python consumer.py --batch-size 1  one Spark job per transaction
    python consumer.py --dry-run       clean and report, write nothing

At the repo root beside ``main.py``, and named ``consumer.py``, because it is
the other half of the same pair: ``main.py`` runs the pipeline over a file,
this runs it over whatever Kafka says has arrived. The logic is in
``src/kafka/consumer.py``; this file is the command line, the signal handler
and the session -- the three things a *process* owns and a function should
not.

Stopping it
-----------

Ctrl-C once, and it finishes the batch it is on, commits, leaves the consumer
group and exits. That is not politeness: leaving the group deliberately means
a restart rejoins immediately rather than waiting for the broker to notice the
old member stopped answering, and committing first means the batch it just
cleaned is not redelivered.

Ctrl-C twice kills it where it stands. The row it was cleaning is still
PENDING and its offset is uncommitted, so restarting cleans it again -- which
is safe, because the upsert is idempotent.
"""

import argparse
import signal
import sys


def build_parser() -> argparse.ArgumentParser:
    """:returns: The command line, with the configured defaults in the help."""
    from src.config import runtime

    config = runtime.load()
    consumer = config.kafka.consumer
    parser = argparse.ArgumentParser(
        prog="python consumer.py",
        description=(
            "Listen for 'a row landed in raw_transactions' events, clean each "
            "row through the Spark pipeline, and upsert it into "
            "cleaned_transactions."
        ),
    )
    parser.add_argument(
        "--once", action="store_true",
        help=(
            "Clean the first batch that arrives, then exit. How the flow is "
            "demonstrated without leaving a process running."
        ),
    )
    parser.add_argument(
        "--batch-size", type=int,
        help=(
            "Most ids to gather into one Spark job. 1 gives one job per "
            f"transaction, which is the clearest thing to watch. Default: "
            f"{consumer.batch_size}."
        ),
    )
    parser.add_argument(
        "--group",
        help=(
            "Consumer group to join. Kafka tracks committed offsets per "
            "group, so a new name replays the whole topic from the "
            f"beginning. Default: {consumer.group_id}."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Read, clean and report, but write nothing to "
            "cleaned_transactions. The state you want when the question is "
            "whether the cleaning is right rather than whether the write is. "
            "Offsets are still committed and rows are still marked, because "
            "the consumer did in fact deal with them."
        ),
    )
    parser.add_argument(
        "--emit", action="store_true",
        help=(
            "Publish a completion event per batch on "
            f"{config.kafka.topic}. Off by default: that topic carries one "
            "event per load, and a consumer announcing every message would "
            "flood it."
        ),
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help=(
            "Do not count or print each stage. Faster -- counting costs a "
            "Spark action per stage -- and much less to read."
        ),
    )
    parser.add_argument(
        "--workers", default="1",
        help=(
            "Spark local worker threads, as the number in local[N]. One by "
            "default, which is the opposite of what a batch job wants and the "
            "right answer here: a batch of a few rows has nothing to "
            "parallelise, and every extra thread is another Python worker "
            "process to start -- which on Windows costs more than the "
            "cleaning does. Use '*' for a core per CPU."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Arguments to parse; ``sys.argv[1:]`` when absent.
    :returns: Exit code -- 0 stopped cleanly, 1 a setting was unusable,
        2 the broker or the database could not be reached.
    """
    args = build_parser().parse_args(argv)

    from dataclasses import replace

    from src.config.errors import ConfigError
    from src.db.settings import load as load_connection
    from src.kafka import consumer as consumer_module
    from src.kafka.settings import load as load_broker
    from src.kafka.settings import load_subscription

    try:
        subscription = load_subscription()
        if args.group:
            subscription = replace(subscription, group_id=args.group)
        database = load_connection()
        broker = load_broker() if args.emit else None
    except ConfigError as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    # Stop is cooperative: the handler sets a flag and the loop notices it
    # between polls. A handler that closed the client from inside a signal
    # would be tearing down a socket that the poll below is sitting on.
    stopping = {"now": False}

    def stop(signum, frame):
        del signum, frame
        if stopping["now"]:
            # Second Ctrl-C. The first was ignored by something long-running
            # -- a Spark job, usually -- and someone who presses it twice
            # means it, so the default handler gets its way.
            raise KeyboardInterrupt
        stopping["now"] = True
        print("\nStopping after this batch. Ctrl-C again to stop now.")

    signal.signal(signal.SIGINT, stop)

    spark = _session(args.workers)

    try:
        cleaned = consumer_module.consume(
            subscription,
            spark=spark,
            database=database,
            broker=broker,
            batch_size=args.batch_size,
            once=args.once,
            write=not args.dry_run,
            emit=args.emit,
            verbose=not args.quiet,
            should_stop=lambda: stopping["now"],
        )
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 0
    except RuntimeError as exc:
        # Both "the library is missing" and "the broker is unreachable" arrive
        # as this, and both are setup rather than code -- 2, matching main.py's
        # "the thing I was told to read is not there".
        print(str(exc), file=sys.stderr)
        return 2

    print(f"\n{cleaned} row(s) cleaned.")
    return 0


def _session(workers: str):
    """
    Builds the session the consumer keeps for its whole life.

    One session, not one per message: the JVM takes about ten seconds to
    start, which would dwarf the cleaning of a single row and would be paid
    again for every message.

    Three settings differ from a batch run, and all three are consequences of
    the batches being tiny:

    ``local[N]`` with N of 1. ``local[*]`` is right when there is data to
    divide and wrong here -- Spark starts a Python worker per thread, Windows
    process creation is slow (``spark_setup`` documents raising the worker
    socket timeout to 120s for exactly this reason), and a batch of two rows
    has nothing for the other fifteen threads to do but be started.

    ``shuffle.partitions`` of 1. The default 8 splits a two-row frame into
    eight tasks, seven of which are empty and all of which are scheduled.

    ``showConsoleProgress`` off. The progress bar rewrites its line
    continuously, which turns the stage log into confetti.

    :param workers: The N in local[N], as text, so ``*`` is expressible.
    :returns: The session.
    """
    from src.spark import spark_setup

    return spark_setup.session(
        "cleaning-consumer",
        master=f"local[{workers}]",
        **{
            "spark.sql.shuffle.partitions": "1",
            "spark.ui.showConsoleProgress": "false",
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())
