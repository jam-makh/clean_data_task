"""
Stage 3 entry point: build the monthly feature table from cleaned transactions.

Reads the cleaned transactions and the rule vocabularies from Postgres, runs
the whole build on Spark, and upserts the result back into Postgres. Tables in,
table out: the only file a run touches is the manifest and data-quality report
it writes beside the build.
"""

import argparse
import dataclasses
import json
import sys

from features import builder
from src.config.errors import ConfigError
from src.db import settings as db_settings
from features import scale
from features import settings as feature_settings
from src.rules import store
from src.spark import spark_setup

# Where the vocabularies come from. `db` is the deliverable; `json` is the
# escape hatch for a machine with no Postgres, and what the tests use.
RULES_DB, RULES_JSON = "db", "json"

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
        "--rules",
        choices=(RULES_DB, RULES_JSON),
        default=RULES_DB,
        help="Where the spending and direction vocabularies come from.",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="Replicate the source this many times before building.",
    )
    parser.add_argument(
        "--no-database",
        action="store_true",
        help="Build and report without writing the feature table.",
    )
    parser.add_argument(
        "--seed-rules",
        action="store_true",
        help="Load the rule tables from src/rules/json/ and exit.",
    )
    return parser.parse_args(argv)


def scaled_output(
    config: feature_settings.FeatureSettings, factor: int
) -> feature_settings.FeatureSettings:
    """
    Points a scaling run at its own artifacts.

    The replicated table is a timing exercise, not the deliverable. Letting it
    overwrite the real one would leave Stage 4 reading five copies of every
    user, so both the manifest and the destination table get the factor in
    their name.

    :param config: The build settings.
    :param factor: The replication factor.
    :returns: The settings, with the outputs suffixed.
    """
    if factor <= 1:
        return config

    manifest = config.output.manifest
    return dataclasses.replace(
        config,
        output=feature_settings.OutputSettings(
            manifest=manifest.with_name(
                f"{manifest.stem}.x{factor}{manifest.suffix}"
            )
        ),
        database=dataclasses.replace(
            config.database, table=f"{config.database.table}_x{factor}"
        ),
    )


def resolve_rules(choice: str, database) -> store.Rules:
    """
    :param choice: ``db`` or ``json``.
    :param database: Postgres settings, used only for ``db``.
    :returns: The vocabularies this build runs against.
    """
    if choice == RULES_JSON:
        return store.from_json()
    return store.from_database(database)


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
        rules = resolve_rules(args.rules, database)
        frame = builder.load_source(spark, database)
    except (ConfigError, RuntimeError) as exc:
        print(f"cannot start the build: {exc}", file=sys.stderr)
        return 2

    if args.scale > 1:
        frame = scale.replicate(frame, args.scale)
        config = scaled_output(config, args.scale)

    print(json.dumps(scale.summarise(frame), indent=2))

    try:
        _, manifest = builder.run(
            spark,
            frame,
            rules,
            config,
            None if args.no_database else database,
        )
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
