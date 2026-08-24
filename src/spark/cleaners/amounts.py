"""
Amount parsing and sign restoration, without a Python call per row.

``AmountNormalizer.parse`` is a twelve-line function over one string, and the
temptation is to wrap it in a UDF and be done. What that would cost is the
whole point of the exercise: a Python UDF serialises every cell across the
JVM boundary one row at a time, so the cheapest stage in the pipeline becomes
the most expensive, and it does it invisibly -- the query still plans, still
runs, and is thirty times slower for a reason nothing in the plan names.

So it is rebuilt as expressions. The five decisions the parser makes survive
one for one:

1. everything outside ``0-9 . , ( ) -`` is deleted;
2. wrapping parentheses mean negative, and so does a leading minus;
3. when both separators are present, whichever appears LAST is the decimal
   point and the other is noise;
4. when only one is present it is a thousands separator if it appears more
   than once, and if it appears once with exactly three digits behind it the
   currency decides -- three digits after the only separator can only be a
   thousands group in a currency that has no minor unit;
5. what still will not read as a number is null, and the row stays.

The sign is then taken from the direction ``processing_codes.json`` declares
for the row's code, never from a rule about which labels count as money
coming back. A code the file has not classified leaves its amount exactly as
the source wrote it.
"""

from pyspark.sql import functions as F

from src.cleaners.amounts import (
    ABSENT,
    AMOUNT_COLUMNS,
    AS_STATED,
    DIRECTION,
    NOT_SIGNED,
    PARSED,
    REFORMATTED,
    RESTORED,
    SIGN_FLAG,
    UNDECLARED,
    UNPARSEABLE,
    coercion_column,
    sign_column,
)
from src.rules import loader
from src.rules.loader import CREDIT
from src.spark.spark_utils import chain, lookup, one_of, text

FLAGS = "VALIDATION_FLAGS"

# The characters ``_CLEAN`` keeps. Written as an explicit range rather than
# ``\d``: Java's ``\d`` is ASCII-only by default and Python's ``\d`` in a
# ``str`` pattern matches every Unicode decimal digit, so an Arabic-Indic
# numeral would survive one filter and not the other. Neither engine can
# then *parse* it, but they would disagree about whether the cell was empty,
# which is the difference between ABSENT and UNPARSEABLE.
_KEEP = "[^0-9.,()-]"


def _count(value, character: str) -> "F.Column":
    """
    :param value: A string column.
    :param character: One character, given as a Java regex literal.
    :returns: How many times it occurs.
    """
    return F.length(value) - F.length(F.regexp_replace(value, character, ""))


def _single_separator(value, separator: str, literal: str, currency):
    """
    Decides whether a lone separator is a decimal point or a thousands mark.

    :param value: The digits-and-one-separator string.
    :param separator: The separator as a Java regex literal.
    :param literal: The same separator as plain text, for ``substring_index``.
    :param currency: The row's currency column.
    :returns: The value with ``.`` as the decimal point, or with the
        separator removed where it was a thousands mark.
    """
    zero_decimal = loader.zero_decimal_currencies()
    removed = F.regexp_replace(value, separator, "")
    decimal = F.regexp_replace(value, separator, ".")

    # More than one occurrence is always thousands. A single one with exactly
    # three digits behind it is ambiguous, and reads as thousands only where
    # the currency has no minor unit for those digits to be.
    thousands = (_count(value, separator) > 1) | (
        (F.length(F.substring_index(value, literal, -1)) == 3)
        & one_of(F.upper(currency), zero_decimal)
    )
    return F.when(thousands, removed).otherwise(decimal)


def parse(raw, currency):
    """
    ``AmountNormalizer.parse`` as a column expression.

    :param raw: The raw amount column, untouched -- not ``text(...)``, because
        the character filter below removes whitespace anyway and stripping
        first would only hide which of the two did it.
    :param currency: The row's currency, used solely to break a genuine tie.
    :returns: The amount as a double, null where nothing could be read.
    """
    kept = F.regexp_replace(raw, _KEEP, "")
    # Both readings of "negative", in the order the original takes them:
    # wrapping parentheses first, then a leading minus on what is left.
    wrapped = kept.startswith("(") & kept.endswith(")")
    unwrapped = F.regexp_replace(kept, "^[()]+|[()]+$", "")
    minus = unwrapped.startswith("-")
    body = F.when(
        minus, unwrapped.substr(F.lit(2), F.length(unwrapped))
    ).otherwise(unwrapped)

    has_dot, has_comma = body.contains("."), body.contains(",")
    # Whichever appears last is the decimal separator. Measured from the end
    # -- ``instr`` on the reversed string -- so the smaller distance is the
    # later character, and both are non-zero here because both are present.
    dot_is_last = F.instr(F.reverse(body), ".") < F.instr(F.reverse(body), ",")

    normalised = chain(
        [
            (
                has_dot & has_comma,
                F.when(dot_is_last, F.regexp_replace(body, ",", "")).otherwise(
                    F.regexp_replace(
                        F.regexp_replace(body, "\\.", ""), ",", "."
                    )
                ),
            ),
            (has_comma, _single_separator(body, ",", ",", currency)),
            (has_dot, _single_separator(body, "\\.", ".", currency)),
        ],
        otherwise=body,
    )

    value = normalised.cast("double")
    # Every ``return None`` in the original, in one place: a null cell, a cell
    # with no usable characters at all, a cell that was nothing but brackets
    # or a bare minus, and a cell that survived all of that and still would
    # not read as a number.
    unreadable = (
        raw.isNull()
        | (F.length(kept) == 0)
        | (F.length(body) == 0)
        | value.isNull()
        | F.isnan(value)
    )
    return F.when(unreadable, F.lit(None).cast("double")).otherwise(
        F.when(wrapped | minus, -value).otherwise(value)
    )


def _coercion(raw, parsed):
    """
    :returns: How the number in the cleaned column was arrived at -- which is
        the gap between what the source would read as a number unaided and
        what this stage got out of it, stated per row.
    """
    direct = raw.cast("double")
    # ``isnan`` as well as ``isNull``: Spark casts the string "nan" to a real
    # NaN where ``pd.to_numeric`` produces a missing value, and only one of
    # those is null. Neither source contains one, which is exactly why the
    # difference would have been found in production rather than here.
    unreadable_directly = direct.isNull() | F.isnan(direct)
    return chain(
        [
            (text(raw) == "", F.lit(ABSENT)),
            (parsed.isNull(), F.lit(UNPARSEABLE)),
            (unreadable_directly, F.lit(REFORMATTED)),
        ],
        otherwise=F.lit(PARSED),
    )


def _restore_signs(frame, policy):
    """
    Signs every amount by the direction its processing code declares.

    CREDIT is money arriving and is positive, DEBIT is money leaving and is
    negative. Which code is which is ``processing_codes.json``'s statement.
    A code the rule file has not classified authorises nothing, so its amount
    is left exactly as the source wrote it and the row says so.

    :returns: The frame with the amounts signed, the sign provenance recorded,
        and every corrected row flagged.
    """
    if "PROCESSING_CODE_CLEANED" not in frame.columns:
        return frame

    directions = loader.processing_code_directions()
    declared = lookup(text("PROCESSING_CODE_CLEANED"), directions)
    frame = frame.withColumn("_amt_declared", declared)
    declared = F.col("_amt_declared")

    known = declared.isNotNull()
    credit = declared == F.lit(CREDIT)
    frame = frame.withColumn(
        DIRECTION, F.coalesce(declared, F.lit(UNDECLARED))
    )

    for source, target in AMOUNT_COLUMNS.items():
        if target not in frame.columns:
            continue
        current = F.col(target)
        magnitude = F.abs(current)
        signed = F.when(
            ~known, current
        ).otherwise(F.when(credit, magnitude).otherwise(-magnitude))

        # Computed against the amount as it stands, so it has to be
        # materialised before the amount is rewritten -- ``withColumn`` reads
        # the frame it is called on, and the next call would compare the new
        # value against itself and find nothing wrong ever.
        wrong = f"_amt_wrong_{source}"
        frame = frame.withColumn(
            wrong, current.isNotNull() & known & (signed != current)
        )
        frame = frame.withColumn(target, signed).withColumn(
            sign_column(source),
            chain(
                [
                    (F.col(wrong), F.lit(RESTORED)),
                    (known & F.col(target).isNotNull(), F.lit(AS_STATED)),
                ],
                otherwise=F.lit(NOT_SIGNED),
            ),
        )

        # The flag as well as the mark: the value on this row now differs from
        # the one in ``raw_transactions``, and that has to be traceable to the
        # transaction rather than only to a total in the report.
        existing = (
            F.coalesce(F.col(FLAGS), F.lit(""))
            if FLAGS in frame.columns
            else F.lit("")
        )
        frame = frame.withColumn(
            FLAGS,
            F.when(
                F.col(wrong),
                F.when(
                    existing.isin("", SIGN_FLAG), F.lit(SIGN_FLAG)
                ).otherwise(F.concat(existing, F.lit(";"), F.lit(SIGN_FLAG))),
            ).otherwise(existing),
        ).drop(wrong)

    return frame.drop("_amt_declared")


def apply(frame, policy):
    """
    Parses every amount, then signs it by its declared direction.

    :param frame: Frame with the processing code already resolved -- this
        stage follows ``codes`` for that reason and no other.
    :param policy: Unused directly; the currency judgement it might have held
        is a fact about the currency and lives in ``currencies.json``.
    :returns: The frame with the cleaned amount, how it was read, and where
        its sign came from.
    """
    currency = (
        text("TXN_CCY") if "TXN_CCY" in frame.columns else F.lit("")
    )

    for source, target in AMOUNT_COLUMNS.items():
        if source not in frame.columns:
            continue
        raw = F.col(source)
        frame = frame.withColumn(target, parse(raw, currency)).withColumn(
            coercion_column(source), _coercion(raw, F.col(target))
        )

    return _restore_signs(frame, policy)
