"""Public entry point: one function that cleans a transactions dataset."""

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.config import runtime
from src.config.errors import ConfigError
from src.config.fingerprint import short_fingerprint
from src.pipeline import TransactionCleaner, steps_for
from src.rules import loader
from src.utils.columns import output_names, presented
from src.utils.io import read_source, write_output
from src.utils.report import CleaningReport

# Sentinel rather than a literal default: resolving the configured path at
# import time would make the module unimportable when the config file is
# absent, which is exactly the caller who passes a frame instead of a path.
_FROM_CONFIG = object()


def clean_transactions(
    source: str | Path | pd.DataFrame = _FROM_CONFIG,
    *,
    steps: list[type] | None = None,
    output_path: str | Path | None = None,
    mcc_reference: dict | None = None,
    profile: str | None = None,
) -> tuple[pd.DataFrame, CleaningReport]:
    """
    Cleans a transactions dataset end to end.

    Accepts a path or an in-memory frame so the same call works against the
    workbook now and a database extract later without a rewrite.

    :param source: Path to an .xlsx, or a DataFrame already in memory;
        defaults to the source in ``config/pipeline.yaml``.
    :param steps: Cleaner classes to run; defaults to the full pipeline.
    :param output_path: If given, writes the multi-sheet workbook there.
    :param mcc_reference: MCC lookup; read from the workbook when source is
        a path.
    :param profile: Named profile from ``config/pipeline.yaml``; detected from
        the source's columns when absent. Ignored if ``steps`` is given.
    :returns: (cleaned frame, report).
    """
    if source is _FROM_CONFIG:
        source = runtime.load().paths.source

    if isinstance(source, pd.DataFrame):
        raw, reference = source, (mcc_reference or {})
    else:
        raw, reference = read_source(source)
        if mcc_reference:
            reference = mcc_reference

    # An explicit step list wins; otherwise the profile decides, by name when
    # one was given and by the source's own columns when not. The steps are
    # never inferred from the file's extension -- two files in the same format
    # can still need different parsers, which is the whole reason profiles
    # exist.
    if steps is None:
        config = runtime.load()
        if config.profiles:
            chosen = (
                config.profile(profile) if profile
                else config.detect(raw.columns)
            )
            steps = steps_for(chosen.steps)

    cleaner = TransactionCleaner(steps=steps, mcc_reference=reference)
    cleaned = cleaner.run(raw)

    if output_path:
        write_output(
            output_path,
            build_sheets(raw, cleaned, cleaner, reference),
            formats=cleaner.policy.output.as_dict(),
        )

    return cleaned, cleaner.report


def build_sheets(
    raw: pd.DataFrame,
    cleaned: pd.DataFrame,
    cleaner: TransactionCleaner,
    reference: dict,
) -> dict[str, pd.DataFrame]:
    """
    Assembles the workbook as a small database, one sheet per table.

    The settlement sheets mirror rows rather than moving them: every source
    row stays in ``cleaned_transactions``, because removing real transactions
    to isolate a date problem would corrupt every total computed from it.

    Every sheet built from ``cleaned`` shows the cleaned columns only. Holding
    ``TXN_AMOUNT`` (text) beside ``TXN_AMOUNT_CLEANED`` (float) invites a total
    computed from the wrong one; the raw values stay one sheet away, keyed by
    ``TXN_ID``.

    Every sheet is passed through ``output_names`` on the way out, so the
    workbook spells a column one way regardless of which step produced it.

    :returns: Sheet name to frame.
    """
    view = presented(cleaned)
    sheets = {"raw_transactions": raw, "cleaned_transactions": view}

    # Both code columns on the transaction sheet are keys into a lookup that
    # lives once, here, rather than as a label repeated down 2,296 rows.
    sheets["processing_codes"] = pd.DataFrame(
        sorted(loader.processing_codes().items()),
        columns=["PROCESSING_CODE_CLEANED", "PROCESSING_TYPE"],
    )

    if reference:
        sheets["mcc_codes"] = pd.DataFrame(
            sorted(reference.items()), columns=["MCC_CODE", "CATEGORY"]
        )

    # Selected from the full frame rather than from the view. The status is a
    # working column and does not appear on any sheet, but it is still what
    # decides which rows these two sheets hold -- and a sheet whose selection
    # criterion had to be visible in its own output would be a strange
    # constraint. The rows written are the view's, so all three sheets carry
    # the same columns.
    if "SETTLE_DATE_STATUS" in cleaned.columns:
        status = cleaned["SETTLE_DATE_STATUS"].astype(str)
        sheets["pending_settlement"] = view[(status == "MISSING").to_numpy()]
        sheets["anomaly_settlement"] = view[(status == "ANOMALOUS").to_numpy()]

    merchant_step = cleaner.step("merchant")
    if merchant_step is not None:
        sheets["merchant_review"] = merchant_step.review_queue()

    mcc_step = cleaner.step("mcc")
    if mcc_step is not None:
        sheets["mcc_review"] = mcc_step.review_queue()

    sheets["cleaning_report"] = cleaner.report.to_frame()
    return {name: output_names(frame) for name, frame in sheets.items()}


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
            "Clean a transactions dataset. The source may be an .xlsx "
            "workbook or a .csv/.tsv extract; which cleaning steps run is "
            "decided by the profile, detected from the file's own columns "
            "unless --profile says otherwise."
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
        "-o", "--output",
        help=(
            "Destination. A .xlsx name writes one multi-sheet workbook; a "
            ".csv name writes one file per sheet, which is the practical "
            "choice above about fifty thousand rows. Defaults to "
            f"paths.output in config/pipeline.yaml ({config.paths.output})."
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
        help="Run and report, but write no output file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Arguments to parse; ``sys.argv[1:]`` when absent.
    :returns: Process exit code -- 0 clean, 1 bad configuration or source,
        2 source missing. Distinguished because a scheduler retrying a
        missing file is reasonable and retrying a malformed profile is not.
    """
    args = build_parser().parse_args(argv)
    paths = runtime.load().paths
    source = Path(args.source) if args.source else paths.source
    output = None if args.dry_run else Path(args.output or paths.output)

    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 2

    try:
        cleaned, report = clean_transactions(
            source, output_path=output, profile=args.profile
        )
    except (ConfigError, KeyError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(f"Source: {source}")
    print(f"Cleaned {len(cleaned)} rows -> "
          f"{output or '(dry run, nothing written)'}")
    print(f"Config fingerprint: {short_fingerprint()}\n")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
