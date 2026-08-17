"""Amount parsing for text-stored numbers with mixed conventions."""

import re

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner

AMOUNT_COLUMNS = {"TXN_AMOUNT": "TXN_AMOUNT_CLEAN"}

# Currencies with no minor unit in practice, where a trailing ",000" group can
# only be a thousands separator.
ZERO_DECIMAL = {"LBP", "JPY", "KRW", "VND", "IQD"}

_CLEAN = re.compile(r"[^\d.,()-]")


class AmountNormalizer(BaseCleaner):
    """
    Converts amount text to float, resolving three separate conventions:
    accounting negatives ``(808.41)``, thousands separators ``1,193.50``, and
    European decimals ``5.727.580,00``.

    Where a single comma is genuinely ambiguous, the currency decides.
    """

    name = "amounts"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ccy = df["TXN_CCY"] if "TXN_CCY" in df.columns else pd.Series([""] * len(df))

        for source, target in AMOUNT_COLUMNS.items():
            if source not in df.columns:
                continue
            values = [
                self.parse(raw, self.text(c)) for raw, c in zip(df[source], ccy)
            ]
            df[target] = pd.to_numeric(pd.Series(values, index=df.index))

            present = df[source].map(lambda v: self.text(v) != "")
            failed = int((df[target].isna() & present).sum())
            recovered = int(
                sum(
                    1
                    for raw in df[source]
                    if pd.to_numeric(pd.Series([raw]), errors="coerce").isna().iat[0]
                    and self.text(raw) != ""
                )
            )
            self.log(f"{source}.unparseable", failed)
            self.log(f"{source}.reformatted", recovered - failed)

        return df

    @staticmethod
    def parse(raw, currency: str = "") -> float | None:
        """
        :param raw: Raw amount cell, text or numeric.
        :param currency: ISO currency code, used only to break a genuine tie.
        :returns: The value as a float, or None if it cannot be read.
        """
        if raw is None or raw != raw:
            return None
        if isinstance(raw, (int, float)):
            return float(raw)

        text = _CLEAN.sub("", str(raw).strip())
        if not text:
            return None

        negative = text.startswith("(") and text.endswith(")")
        text = text.strip("()")
        if text.startswith("-"):
            negative = True
            text = text[1:]
        if not text:
            return None

        has_dot, has_comma = "." in text, "," in text
        if has_dot and has_comma:
            # Whichever appears last is the decimal separator.
            decimal = "." if text.rfind(".") > text.rfind(",") else ","
            text = text.replace("," if decimal == "." else ".", "").replace(
                decimal, "."
            )
        elif has_comma:
            text = AmountNormalizer._single_separator(text, ",", currency)
        elif has_dot:
            text = AmountNormalizer._single_separator(text, ".", currency)

        try:
            value = float(text)
        except ValueError:
            return None
        return -value if negative else value

    @staticmethod
    def _single_separator(text: str, sep: str, currency: str) -> str:
        """
        Decides whether a lone separator is decimal or thousands.

        More than one occurrence is always thousands. A single one followed by
        exactly three digits is ambiguous, and is read as thousands only when
        the currency has no minor unit.

        :param text: Digits plus one kind of separator.
        :param sep: The separator present.
        :param currency: ISO currency code.
        :returns: Text with ``.`` as the decimal point.
        """
        parts = text.split(sep)
        if len(parts) > 2:
            return "".join(parts)
        if len(parts[1]) == 3 and currency.upper() in ZERO_DECIMAL:
            return "".join(parts)
        return ".".join(parts)
