"""Amount parsing for text-stored numbers with mixed conventions."""

import re

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.rules.loader import CREDIT

AMOUNT_COLUMNS = {"TXN_AMOUNT": "TXN_AMOUNT_CLEANED"}

# The flag raised on a row whose sign had to be restored.
SIGN_FLAG = "AMOUNT_SIGN_RESTORED"

_CLEAN = re.compile(r"[^\d.,()-]")


class AmountNormalizer(BaseCleaner):
    """
    Converts amount text to float, resolving three separate conventions:
    accounting negatives ``(808.41)``, thousands separators ``1,193.50``, and
    European decimals ``5.727.580,00``.

    Where a single comma is genuinely ambiguous, the currency decides.

    Then it restores the sign, which the source loses on amounts it wrote out
    as text. It is lost two ways, and both files show one: 13 rows of the v4
    workbook arrive as a bare ``409.34`` on a purchase, with no minus to read
    one from, and 45 rows of the forecast extract carry accounting parentheses
    on credit codes whose own ``BILLING_AMOUNT`` is positive.

    The transaction type says which way the money moved, so the sign is taken
    from there -- from the direction ``processing_codes.json`` declares for
    the code, never from a rule about which labels count as money coming back.
    """

    name = "amounts"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ccy = (
            df["TXN_CCY"]
            if "TXN_CCY" in df.columns
            else pd.Series([""] * len(df))
        )

        for source, target in AMOUNT_COLUMNS.items():
            if source not in df.columns:
                continue
            values = [
                self.parse(raw, self.text(c))
                for raw, c in zip(df[source], ccy)
            ]
            df[target] = pd.to_numeric(pd.Series(values, index=df.index))

            present = df[source].map(lambda v: self.text(v) != "")
            failed = int((df[target].isna() & present).sum())
            recovered = int(
                sum(
                    1
                    for raw in df[source]
                    if pd.to_numeric(
                        pd.Series([raw]), errors="coerce"
                    ).isna().iat[0]
                    and self.text(raw) != ""
                )
            )
            self.log(f"{source}.unparseable", failed)
            self.log(f"{source}.reformatted", recovered - failed)

        return self.restore_signs(df)

    def restore_signs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Signs every amount from the direction its processing code declares:
        CREDIT is money arriving and is positive, DEBIT is money leaving and
        is negative. Which code is which is the rule file's statement, never
        this module's.

        The rule this replaced -- "a refund is the only credit, everything
        else is a debit" -- held for the three codes the v4 workbook uses and
        mis-signed every credit in the forecast extract, where salary,
        transfer in, interest, settlement, card payment and bonus are all
        money arriving.

        The magnitude is never touched -- only the sign is taken from the
        direction, because the digits are what the source got right and the
        sign is what it dropped. Rows already carrying the correct sign are
        left alone and not counted.

        Each corrected row is flagged as well as counted: the value on it now
        differs from the one in ``raw_transactions``, and that has to be
        traceable to the transaction rather than only to a total in the report.

        :param df: Frame with amounts parsed and the processing code resolved.
        :returns: The frame with every amount signed by its declared direction.
        """
        if "PROCESSING_CODE_CLEANED" not in df.columns:
            return df

        # Only the codes the rule file actually declares. A direction is what
        # authorises rewriting the number, so a code nobody has classified
        # leaves its amount exactly as the source wrote it: an unknown code is
        # a gap in the table, and inventing a sign for it would corrupt the
        # one field this step is trusted not to invent.
        directions = loader.processing_code_directions()
        declared = df["PROCESSING_CODE_CLEANED"].map(self.text).map(directions)
        credit = declared.eq(CREDIT)
        known = declared.notna()
        self.log("sign.code_without_direction", int((~known).sum()))

        for target in AMOUNT_COLUMNS.values():
            if target not in df.columns:
                continue
            magnitude = df[target].abs()
            signed = magnitude.where(credit, -magnitude).where(
                known, df[target]
            )
            wrong = df[target].notna() & known & (signed != df[target])
            df[target] = signed

            if "VALIDATION_FLAGS" not in df.columns:
                df["VALIDATION_FLAGS"] = ""
            df.loc[wrong, "VALIDATION_FLAGS"] = (
                df.loc[wrong, "VALIDATION_FLAGS"]
                .replace("", pd.NA)
                .fillna(SIGN_FLAG)
                .where(
                    lambda s: s == SIGN_FLAG,
                    lambda s: s + ";" + SIGN_FLAG,
                )
            )
            self.log(f"{target}.sign_restored", int(wrong.sum()))

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
        zero_decimal = loader.zero_decimal_currencies()
        if len(parts[1]) == 3 and currency.upper() in zero_decimal:
            return "".join(parts)
        return ".".join(parts)
