"""
Public entry point: read the extract, clean it on Spark, upsert to Postgres,
and report what the load did.

One engine. The pipeline was built twice -- once in pandas as the Stage 1
reference and once on Spark as the Stage 2 deliverable -- and the two were
held to the same answers by a parity harness that ran both over the same
sample and compared column for column. That harness had done its job by the
time the port was complete: it passed on all eleven stages, and a second
implementation kept only to be compared against is a second implementation to
maintain. The pandas half is gone and Spark is the pipeline.

What survived the deletion is the vocabulary both halves shared -- the column
names, the status values and a handful of pure functions -- which now lives in
``src/schema/`` and is imported by the Spark stages. Every line of it is
unchanged from the reviewed pandas modules it was lifted out of.
"""

import argparse
import sys
from pathlib import Path

from src.config import runtime
from src.config.errors import ConfigError


def build_parser() -> argparse.ArgumentParser:
    """
    :returns: The command-line parser, with the configured defaults shown in
        the help text so ``--help`` answers "what will this do" without
        opening the YAML.
    """
    config = runtime.load()
    names = [p.name for p in config.profiles]
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "Clean a transactions dataset on Spark and upsert it to "
            "Postgres. The source is a delimited extract -- .csv, .tsv or "
            ".txt; the Spark reader does not read workbooks. Which cleaning "
            "steps run is decided by the profile, detected from the file's "
            "own columns unless --profile says otherwise."
        ),
    )
    parser.add_argument(
        "source",
        nargs="?",
        help=(
            "Source file. Defaults to paths.source in config/pipeline.yaml "
            f"({config.paths.source})."
        ),
    )
    parser.add_argument(
        "-p", "--profile",
        choices=names or None,
        help=(
            "Force a profile instead of detecting one from the columns. "
            f"Configured: {', '.join(names) if names else 'none'}."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read, clean and report, but write no database rows.",
    )
    parser.add_argument(
        "--no-emit",
        action="store_true",
        help=(
            "Write to the database but announce nothing on Kafka. For when "
            "the broker is down and the cleaning is what you are checking. "
            "The default is kafka.enabled in config/pipeline.yaml "
            f"({config.kafka.enabled})."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Arguments to parse; ``sys.argv[1:]`` when absent.
    :returns: Process exit code -- 0 clean, 1 bad configuration or source,
        2 source missing, 3 written but not announced. Distinguished because
        a scheduler retrying a missing file is reasonable, retrying a
        malformed profile is not, and 3 means the data is safe and only the
        event needs re-sending.
    """
    args = build_parser().parse_args(argv)
    config = runtime.load()
    source = Path(args.source) if args.source else config.paths.source

    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 2

    return _run_spark(source, args)


def _run_spark(source: Path, args) -> int:
    """
    Read, clean, count, upsert, report.

    Its own function, and not merely for tidiness: ``main`` checks the source
    exists before calling it, and keeping the two separable is what lets a
    test prove a typo'd path costs exit code 2 rather than a JVM startup.

    pyspark is imported here rather than at module scope so that ``--help``
    does not pay for it, and so a machine with no JVM gets a usable error
    instead of an import traceback.

    :param source: The extract to read.
    :param args: Parsed arguments.
    :returns: Process exit code, on the same scheme as ``main``.
    """
    from src.kafka.producer import PublishError
    from src.runner import run

    # --dry-run suppresses both outputs, not just the database one: a run
    # that writes nothing but still announces "265,195 rows cleaned" would put
    # a lie on the topic, and a consumer has no way to tell it from the truth.
    emit = None if not args.dry_run else False
    if args.no_emit:
        emit = False

    try:
        result = run(
            source,
            profile=args.profile,
            write=not args.dry_run,
            emit=emit,
        )
    except PublishError as exc:
        # Distinguished from a failed run, because it is not one. The rows are
        # committed; only the announcement failed, and re-running is safe.
        print(f"Rows written, but the event was not published: {exc}",
              file=sys.stderr)
        return 3
    except (ConfigError, KeyError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    written = (
        "(dry run, nothing written)" if result.rows_written is None
        else f"{result.rows_written} rows upserted"
    )
    announced = (
        f"announced on {result.event['event']}" if result.event
        else "not announced"
    )
    print(f"Source: {result.source}")
    print(f"Profile: {result.profile}")
    print(f"Sync job: {result.sync_job_id}")
    print(f"Read {result.rows_read} rows -> {written}, {announced}")
    print(f"Elapsed: {result.seconds:.1f}s")
    print(f"Config fingerprint: {result.fingerprint}\n")
    print(result.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
