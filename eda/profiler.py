"""Read-only profiling of the raw transactions file: duplicates, nulls, missing values."""

import re
from pathlib import Path

import pandas as pd

# Strings that mean "no value" but are not NaN, so pandas counts them as real data.
# Compared case-insensitively after stripping. "NA" is included even though it is a
# valid country code (Namibia) — the per-column report shows where it landed, so a
# false positive is visible rather than silent.
PLACEHOLDERS = {
    "",
    "-",
    "--",
    "?",
    "#N/A",
    "N/A",
    "NA",
    "NAN",
    "NONE",
    "NULL",
    "MISSING",
    "UNKNOWN",
    # Date-shaped nulls. "0000-00-00" is the MySQL zero-date; "1970-01-01" is
    # epoch zero, which is what a null timestamp becomes when it is converted
    # as a number. Both are safe to treat as missing here because the dataset
    # is 2022 card activity — neither can be a real transaction date.
    "0000-00-00",
    "0000-00-00 00:00:00",
    "1970-01-01",
    "1970-01-01 00:00:00",
}

# Filler values used instead of a blank: an all-zero terminal ID, an all-X
# reference. Same meaning as a null, but they pass every null check.
SENTINEL_PATTERN = re.compile(r"^(?:0+|X+|9+|-+)$", re.IGNORECASE)

# Business keys checked for duplicates beyond the exact-row and TXN_ID checks:
# the same transaction re-exported under a fresh ID would show up here.
DUPLICATE_KEYS = [
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT", "MERCHANT_NAME"],
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT"],
    ["ACCOUNT_ID", "TXN_AMOUNT", "MERCHANT_NAME", "AUTH_CODE"],
]

DISPLAY_COLS = [
    "TXN_ID",
    "TXN_DATE_TIME",
    "SETTLE_DATE",
    "TXN_AMOUNT",
    "TXN_CCY",
    "MERCHANT_NAME",
    "MCC_CODE",
    "MERCHANT_COUNTRY",
]


class DataProfiler:
    """
    Profiles a transactions file without modifying it.

    Reports three categories — duplicates, nulls, and missing (placeholder)
    values — each with a count, a percentage of total rows, and the first 10
    offending rows so the shape of the problem is visible before any cleaning
    decision is made.

    :param path: Path to the source Excel file.
    :param id_col: Column expected to be a unique transaction key.
    :param sheet: Sheet name or index to read.
    """

    SAMPLE_SIZE = 10

    def __init__(self, path: str | Path, id_col: str = "TXN_ID", sheet: str | int = 0):
        self.path = Path(path)
        self.id_col = id_col
        self.sheet = sheet
        self.df: pd.DataFrame | None = None
        self.results: dict = {}

    def load(self) -> "DataProfiler":
        """
        Loads the file with no type coercion.

        ``dtype=object`` and ``keep_default_na=False`` are deliberate: pandas
        would otherwise turn strings like "NA" or "" into NaN on read, which
        would merge the null and missing categories before we can tell them
        apart.

        :returns: self, for chaining.
        """
        self.df = pd.read_excel(self.path, sheet_name=self.sheet)
        self.raw = pd.read_excel(
            self.path, sheet_name=self.sheet, dtype=object, keep_default_na=False
        )
        return self

    @property
    def n_rows(self) -> int:
        """:returns: Total row count of the loaded file."""
        return len(self.df)

    def _pct(self, count: int) -> float:
        """
        :param count: Number of affected rows.
        :returns: That count as a percentage of total rows.
        """
        return round(100 * count / self.n_rows, 2) if self.n_rows else 0.0

    # Category 1 — duplicates
    def profile_duplicates(self) -> dict:
        """
        Finds exact full-row duplicates and, separately, duplicated transaction IDs.

        The two are kept apart because they mean different things: an identical
        row is almost certainly a double-load, whereas two different rows sharing
        a TXN_ID is an upstream key problem and may still be two real transactions.

        :returns: Dict with counts, percentages and sample frames for both kinds.
        """
        df = self.df

        full_mask = df.duplicated(keep=False)
        full_extra = int(df.duplicated(keep="first").sum())

        id_mask = df.duplicated(subset=[self.id_col], keep=False)
        # IDs repeated on rows that are NOT byte-identical — the subtler case.
        id_only_mask = id_mask & ~full_mask

        # A third pass on business keys: the same purchase re-exported with a new
        # TXN_ID is invisible to both checks above.
        by_key = {}
        for keys in DUPLICATE_KEYS:
            if not set(keys).issubset(df.columns):
                continue
            key_mask = df.duplicated(subset=keys, keep=False) & ~full_mask
            by_key[" + ".join(keys)] = {
                "rows": int(key_mask.sum()),
                "pct": self._pct(int(key_mask.sum())),
                "sample": self._sample(df[key_mask]),
            }

        out = {
            "full_row_rows": int(full_mask.sum()),
            "full_row_removable": full_extra,
            "full_row_pct": self._pct(int(full_mask.sum())),
            "id_collision_rows": int(id_only_mask.sum()),
            "id_collision_pct": self._pct(int(id_only_mask.sum())),
            "n_unique_ids": int(df[self.id_col].nunique()),
            "by_business_key": by_key,
            "full_row_sample": self._sample(df[full_mask].sort_values(self.id_col)),
            "id_collision_sample": self._sample(
                df[id_only_mask].sort_values(self.id_col)
            ),
        }
        self.results["duplicates"] = out
        return out

    # Category 2 — nulls
    def profile_nulls(self) -> dict:
        """
        Counts true nulls (NaN / None / empty cells) per column.

        :returns: Dict with a per-column table, the row-level count/percentage
            of rows holding at least one null, and a sample of those rows.
        """
        df = self.df
        null_counts = df.isna().sum()
        table = self._column_table(null_counts)

        row_mask = df.isna().any(axis=1)
        out = {
            "by_column": table,
            "rows_affected": int(row_mask.sum()),
            "rows_pct": self._pct(int(row_mask.sum())),
            "sample": self._sample(df[row_mask]),
        }
        self.results["nulls"] = out
        return out

    # Category 3 — missing / placeholder values
    def profile_missing(self) -> dict:
        """
        Counts placeholder values — cells that hold text meaning "nothing" and
        so survive an ``isna()`` check. Two kinds: word placeholders ("N/A",
        "NULL", blanks) and sentinels ("00000000"), counted together but
        reported per column so each is traceable.

        Read from the untyped copy so the original cell text is intact.

        :returns: Dict with a per-column table, the row-level count/percentage,
            a sample of affected rows, and which tokens were seen in each column.
        """
        raw = self.raw
        mask = raw.map(self._is_placeholder) | raw.map(self._is_sentinel)
        table = self._column_table(mask.sum())

        # Which token was actually matched, per column — makes false positives
        # such as "NA" (Namibia) in a country column obvious.
        tokens = {}
        for col in raw.columns:
            hits = raw[col][mask[col]]
            if len(hits):
                tokens[col] = sorted({repr(v) for v in hits.unique()})[:5]

        row_mask = mask.any(axis=1)
        out = {
            "by_column": table,
            "tokens_by_column": tokens,
            "rows_affected": int(row_mask.sum()),
            "rows_pct": self._pct(int(row_mask.sum())),
            "sample": self._sample(self.df[row_mask]),
        }
        self.results["missing"] = out
        return out

    @staticmethod
    def _is_placeholder(value) -> bool:
        """
        :param value: Any cell value.
        :returns: True if the cell is a string that stands in for a real value.
        """
        if not isinstance(value, str):
            return False
        return value.strip().upper() in PLACEHOLDERS

    @staticmethod
    def _is_sentinel(value) -> bool:
        """
        :param value: Any cell value.
        :returns: True if the cell is a filler run of one character ("00000000").
        """
        if not isinstance(value, str):
            return False
        stripped = value.strip()
        return bool(stripped) and bool(SENTINEL_PATTERN.match(stripped))

    # Helpers
    def _column_table(self, counts: pd.Series) -> pd.DataFrame:
        """
        :param counts: Per-column affected-cell counts.
        :returns: Non-zero counts with percentages, worst column first.
        """
        table = pd.DataFrame({"count": counts})
        table["pct_of_rows"] = table["count"].map(self._pct)
        return table[table["count"] > 0].sort_values("count", ascending=False)

    def _sample(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        :param frame: Rows flagged by one of the profilers.
        :returns: First 10 rows, narrowed to the readable columns.
        """
        cols = [c for c in DISPLAY_COLS if c in frame.columns]
        return frame[cols].head(self.SAMPLE_SIZE)

    # Report
    def report(self) -> "DataProfiler":
        """
        Runs all three profilers and prints the findings.

        :returns: self, for chaining.
        """
        dup = self.profile_duplicates()
        nul = self.profile_nulls()
        mis = self.profile_missing()

        self._header(f"FILE: {self.path.name}")
        print(f"Rows: {self.n_rows}   Columns: {len(self.df.columns)}")
        print("\nDtypes:")
        print(self.df.dtypes.to_string())

        self._header("1. DUPLICATES")
        print(
            f"Exact full-row duplicates : {dup['full_row_rows']} rows "
            f"({dup['full_row_pct']}%) -> {dup['full_row_removable']} droppable"
        )
        print(
            f"{self.id_col} collisions (non-identical rows): "
            f"{dup['id_collision_rows']} rows ({dup['id_collision_pct']}%)"
        )
        print(f"Unique {self.id_col}: {dup['n_unique_ids']} of {self.n_rows}")
        print("\nBusiness-key duplicates (same txn re-exported under a new ID):")
        for key, res in dup["by_business_key"].items():
            print(f"  {res['rows']:>5} rows ({res['pct']}%)  on  {key}")
        self._show("First 10 full-row duplicates", dup["full_row_sample"])
        self._show(
            f"First 10 {self.id_col} collisions", dup["id_collision_sample"]
        )

        self._header("2. NULLS (NaN / empty cells)")
        print(
            f"Rows with >=1 null: {nul['rows_affected']} ({nul['rows_pct']}% of rows)"
        )
        self._show("By column", nul["by_column"])
        self._show("First 10 rows with nulls", nul["sample"])

        self._header("3. MISSING (placeholders & sentinels)")
        print(
            f"Rows with >=1 placeholder: {mis['rows_affected']} "
            f"({mis['rows_pct']}% of rows)"
        )
        self._show("By column", mis["by_column"])
        if mis["tokens_by_column"]:
            print("\nTokens found per column:")
            for col, vals in mis["tokens_by_column"].items():
                print(f"  {col}: {', '.join(vals)}")
        self._show("First 10 rows with placeholders", mis["sample"])

        return self

    @staticmethod
    def _header(title: str) -> None:
        """:param title: Section title to print between separators."""
        print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")

    @staticmethod
    def _show(title: str, frame: pd.DataFrame) -> None:
        """
        :param title: Label for the block.
        :param frame: Frame to print, or a note if it is empty.
        """
        print(f"\n--- {title} ---")
        print(frame.to_string() if len(frame) else "(none)")


if __name__ == "__main__":
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 50)

    DataProfiler("data/raw/synthetic_dirty_transactions_v4.xlsx").load().report()
