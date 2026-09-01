"""
Stage 3 entry point: build the monthly feature table from cleaned transactions.

Reads the cleaned transactions and the rule vocabularies from Postgres, runs
the whole build on Spark, and upserts the result back into Postgres. Tables in,
table out: the only file a run touches is the manifest and data-quality report
it writes beside the build.
"""

import argparse
import json
import sys

from features import builder
from src.config_readers.errors import ConfigError
from src.db import settings as db_settings
from features import settings as feature_settings
from src.rules import store
from src.spark import spark_setup

APP_NAME = "stage3-feature-build"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    :param argv: Command line, defaulting to the real one.
    :returns: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Build the Stage 3 monthly feature table."
    )
    parser.add_argument(
        "--seed-rules",
        action="store_true",
        help="Load the rule tables from src/rules/json/ and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Command line, defaulting to the real one.
    :returns: Process exit code.
    """
    args = parse_args(argv)
    config = feature_settings.load()

    try:
        database = db_settings.load()
    except ConfigError as exc:
        print(f"database settings: {exc}", file=sys.stderr)
        return 2

    if args.seed_rules:
        store.migrate(database)
        written = store.seed(database)
        print(json.dumps(written, indent=2))
        return 0

    spark = spark_setup.session(APP_NAME)

    try:
        rules = store.from_database(database)
        frame = builder.load_source(spark, database)
    except (ConfigError, RuntimeError) as exc:
        print(f"cannot start the build: {exc}", file=sys.stderr)
        return 2

    try:
        _, manifest = builder.run(spark, frame, rules, config, database)
    except ConfigError as exc:
        print(f"build failed: {exc}", file=sys.stderr)
        return 1

    performance = manifest["performance"]
    print(
        json.dumps(
            {
                "feature_rows": manifest["coverage"]["feature_rows"]["value"],
                "users": manifest["coverage"]["users"]["value"],
                "months": manifest["coverage"]["months"],
                "table": manifest["destination"]["table"],
                "rows_written": manifest["destination"]["rows_written"],
                "slowest_phase": performance["slowest_phase"],
                "total_seconds": performance["total_seconds"],
                "jvm_peak_memory_mb": performance["jvm_peak_memory_mb"],
                "manifest": str(config.output.manifest),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
