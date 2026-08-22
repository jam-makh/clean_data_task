"""Derives the macro rule file from the source, rather than transcribing it.

The three macro columns are not transaction attributes. Each is constant
across a whole group, which is what makes them recoverable by lookup instead
of imputable by model:

    INTEREST_RATE_INDEX  one value per (year-month)             42/42 groups
    INFLATION_INDEX      one value per (year-month, country)   252/252 groups
    IS_HOLIDAY_MONTH     one value per (year-month, country)   504/504 groups

This script asserts that constancy before it writes anything. If a future
file breaks it, the assertion is the finding -- the column would no longer be
a broadcast macro series, and the lookup would be the wrong tool for it.

Run with:  python -m scripts.extract_macro_series
"""

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/raw/forecast_balance_data.csv"
TARGET = ROOT / "src/rules/json/macro_series.json"

INTEREST = "INTEREST_RATE_INDEX"
INFLATION = "INFLATION_INDEX"
HOLIDAY = "IS_HOLIDAY_MONTH"
COUNTRY = "MERCHANT_COUNTRY"


def month_index(frame: pd.DataFrame) -> pd.Series:
    """
    :param frame: Raw source rows.
    :returns: Year-month as ``YYYY-MM``, from a timestamp normalised the same
        way the pipeline normalises it -- the epoch column rendered in the
        source clock, so a local midnight keeps its own calendar month.
    """
    from src.cleaners.timestamps import TimestampNormalizer
    from src.utils.report import CleaningReport

    cleaned = TimestampNormalizer(CleaningReport()).apply(frame)
    return cleaned["TXN_TS"].dt.to_period("M").astype(str)


def constant_or_die(frame: pd.DataFrame, column: str, keys: list[str]) -> dict:
    """
    :param keys: Columns the value is claimed to be constant within.
    :returns: Joined key to the single observed value.
    :raises SystemExit: If any group holds more than one distinct value, which
        would mean the column is not a broadcast series after all.
    """
    present = frame.dropna(subset=[column] + keys)
    counts = present.groupby(keys)[column].nunique()
    if (counts > 1).any():
        offenders = counts[counts > 1]
        sys.exit(
            f"{column} is not constant within {keys}: "
            f"{len(offenders)} groups vary, e.g. {offenders.head(3).to_dict()}"
        )
    values = present.groupby(keys)[column].first()
    return {"|".join(map(str, k)) if isinstance(k, tuple) else str(k): v
            for k, v in values.items()}


def main() -> None:
    if not SOURCE.exists():
        sys.exit(f"source not found: {SOURCE}")
    frame = pd.read_csv(SOURCE, dtype=str, keep_default_na=False,
                        na_values=[""])
    frame["_ym"] = month_index(frame)
    for column in (INTEREST, INFLATION):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame[HOLIDAY] = frame[HOLIDAY].map(
        {"True": True, "False": False, "TRUE": True, "FALSE": False}
    )

    interest = constant_or_die(frame, INTEREST, ["_ym"])
    inflation = constant_or_die(frame, INFLATION, ["_ym", COUNTRY])
    holiday = constant_or_die(frame, HOLIDAY, ["_ym", COUNTRY])

    # Which countries the inflation panel covers at all. Six of the twelve
    # never carry a value in any month, so their nulls are the absence of a
    # series rather than the loss of one -- 17873 rows that must not be
    # imputed, and cannot be recovered either.
    covered = sorted({k.split("|")[1] for k in inflation})
    observed = sorted(frame[COUNTRY].dropna().unique())

    payload = {
        "_comment": (
            "Macro series broadcast onto every transaction row, extracted "
            "from the source by scripts/extract_macro_series.py and verified "
            "constant within their key before writing. These are facts about "
            "the world on a date, not attributes of a transaction: nothing "
            "about a purchase moves them, only its month and country do. "
            "That is why the missing ones are recovered by lookup and never "
            "imputed -- a per-user mode would be wrong for every user whose "
            "rows span more than one month."
        ),
        "interest_rate_index": {
            "_comment": (
                "One global series keyed on year-month; a single value covers "
                "all 12 countries. Rises 4.519 to 6.544 across the window. It "
                "appears on purchase rows because these are revolving credit "
                "accounts -- PROCESSING_TYPE carries INTEREST, CARD_PAYMENT "
                "and SETTLEMENT_CREDIT, and the median RUNNING_BALANCE is "
                "negative -- so the rate is the month's reference rate on the "
                "carried balance, not a rate charged on that purchase."
            ),
            "key": "YYYY-MM",
            "values": interest,
        },
        "inflation_index": {
            "_comment": (
                "Per-country cumulative CPI change from a Jan-2022 base, not "
                "a month-over-month rate: LB runs 3.22 to 19.49 over the "
                "first six months while DE runs 0.16 to 1.17. Keyed on the "
                "MERCHANT country, so it is the inflation where the "
                "transaction happened."
            ),
            "key": "YYYY-MM|CC",
            "covered_countries": covered,
            "uncovered_countries": [c for c in observed if c not in covered],
            "values": inflation,
        },
        "is_holiday_month": {
            "_comment": (
                "A fixed (country, calendar-month) rule, identical in every "
                "year: 144 country-month groups, 0 with more than one value. "
                "Stored per year-month anyway so the lookup shape matches "
                "inflation and a future file that does vary by year can be "
                "represented without a schema change."
            ),
            "key": "YYYY-MM|CC",
            "values": holiday,
        },
    }
    TARGET.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        f"wrote {TARGET.relative_to(ROOT)}: "
        f"{len(interest)} months, {len(inflation)} inflation cells, "
        f"{len(holiday)} holiday cells, "
        f"{len(covered)}/{len(observed)} countries in the inflation panel"
    )


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    main()
