"""
Amount parsing for text-stored numbers with mixed conventions.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

import re

import numpy as np

from src.rules import loader
from src.rules.loader import CREDIT

AMOUNT_COLUMNS = {"TXN_AMOUNT": "TXN_AMOUNT_CLEANED"}

# The flag raised on a row whose sign had to be restored.
SIGN_FLAG = "AMOUNT_SIGN_RESTORED"

# How the number in the cleaned column was arrived at.
#
# PARSED       the source wrote something Python reads as a number directly.
# REFORMATTED  it did not, and one of the three conventions this step knows --
#              accounting parentheses, thousands separators, European decimals
#              -- turned it into one.
# UNPARSEABLE  the source wrote something, and none of them could read it. The
#              cleaned value is null and the row is still here.
# ABSENT       the source wrote nothing. Not a failure.
PARSED, REFORMATTED, UNPARSEABLE, ABSENT = (
    "PARSED", "REFORMATTED", "UNPARSEABLE", "ABSENT",
)

# Where the sign on the cleaned amount came from.
#
# AS_STATED   the source's own sign, and the declared direction agrees with it.
# RESTORED    the source lost it and the direction supplied it. The value on
#             this row now differs from the one in raw_transactions.
# NOT_SIGNED  no direction was declared for the code, or there is no amount to
#             sign. Whatever the source wrote is what stands.
AS_STATED, RESTORED, NOT_SIGNED = "AS_STATED", "RESTORED", "NOT_SIGNED"

# What processing_codes.json says the code does to the balance, per row. A
# code the rule file has not classified is named rather than left blank: it is
# the reason this row's amount was not touched.
UNDECLARED = "UNDECLARED"
DIRECTION = "PROCESSING_CODE_DIRECTION"

_CLEAN = re.compile(r"[^\d.,()-]")


def coercion_column(source: str) -> str:
    """:returns: The column recording how ``source`` was read as a number."""
    return f"{source}_COERCION"


def sign_column(source: str) -> str:
    """:returns: The column recording where ``source``'s sign came from."""
    return f"{source}_SIGN"


class AmountNormalizer:
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
