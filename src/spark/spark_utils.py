"""
The shared column expressions every Spark stage is built out of.

Membership rule, because a file called "utils" attracts anything nobody wants
to name: something belongs here only if it is stateless, called by two or more
stages, and holds no domain knowledge. Nothing in here knows what a merchant,
a currency or a processing code is -- that lives in ``cleaners/``.

Two halves. The expression helpers are the Spark counterpart of
``BaseCleaner.text`` and friends: column expressions rather than functions of a
value, because the whole point of the port is that no Python runs per row. The
broadcast helpers below them are for rule tables too large to inline.
"""

import unicodedata
from functools import lru_cache

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Column expressions
# ---------------------------------------------------------------------------

# Every codepoint ``str.isspace()`` is true for, which is what ``str.strip()``
# removes -- and the reason `trim` is not used below: Spark's `trim` removes
# the ASCII space and nothing else, so a cell whose country reads as a
# non-breaking space followed by 'LB' would survive
# it with the space attached, miss every lookup, and be reported as an unknown
# country. Written as ranges and rendered into a Java character class so the
# two spellings of "whitespace" cannot drift: this list IS the pandas
# definition.
_WHITESPACE = (
    (0x09, 0x0D), (0x1C, 0x1F), (0x20, 0x20), (0x85, 0x85), (0xA0, 0xA0),
    (0x1680, 0x1680), (0x2000, 0x200A), (0x2028, 0x2029), (0x202F, 0x202F),
    (0x205F, 0x205F), (0x3000, 0x3000),
)

# ``\x{..}`` escapes rather than the characters themselves: a literal control
# character inside a regex string is invisible in a diff and impossible to
# review.
_WS_CLASS = "".join(
    rf"\x{{{low:x}}}" if low == high else rf"\x{{{low:x}}}-\x{{{high:x}}}"
    for low, high in _WHITESPACE
)

# Anchored both ends in one pattern so the strip is a single pass.
STRIP = f"^[{_WS_CLASS}]+|[{_WS_CLASS}]+$"


def text(column: str | Column) -> Column:
    """
    ``BaseCleaner.text`` as a column expression.

    :param column: A column or its name.
    :returns: The value stripped of whitespace, with null as the empty string.
    """
    column = F.col(column) if isinstance(column, str) else column
    return F.coalesce(F.regexp_replace(column, STRIP, ""), F.lit(""))


def strip(column: str | Column) -> Column:
    """
    ``str.strip`` as a column expression, keeping a null a null.

    ``text`` is this plus "a null is an empty string". They are separate
    because half the call sites want the null back: a stage that has already
    decided what an absent value means must not have that decision made for it
    a second time.

    :param column: A column or its name.
    :returns: The value with leading and trailing whitespace removed.
    """
    column = F.col(column) if isinstance(column, str) else column
    return F.regexp_replace(column, STRIP, "")


def lookup(
    key: Column, mapping: dict, default: Column | None = None
) -> Column:
    """
    A dict lookup as a ``create_map`` literal, not a join.

    For a table of twenty processing codes this is the cheaper answer by a wide
    margin: a join against a twenty-row frame still plans a broadcast exchange,
    while the map is evaluated in the same expression tree as the rest of the
    stage. Past a few hundred entries it stops being cheaper -- see ``joined``.

    :param key: The expression to look up.
    :param mapping: Key to value. Values are taken as literals of whatever
        Python type they are, so a mixed-type mapping is rejected by Spark here
        rather than producing a surprising cast later.
    :param default: What an absent key yields; null when not given, which is
        what ``dict.get`` returns and what every caller's ``fillna`` expects.
    :returns: The looked-up value.
    """
    if not mapping:
        return default if default is not None else F.lit(None)
    pairs = []
    for map_key, value in mapping.items():
        pairs.extend([F.lit(map_key), F.lit(value)])
    found = F.element_at(F.create_map(*pairs), key)
    return found if default is None else F.coalesce(found, default)


@lru_cache(maxsize=1)
def _fold_pairs() -> tuple[str, str]:
    """
    :returns: ``(accented characters, their base letters)`` as two aligned
        strings, derived from Python's own Unicode tables.

    Built by asking ``unicodedata`` what each character decomposes to and
    keeping the ones whose decomposition is a single base letter plus marks.
    Derived rather than listed -- a transcribed table of accented characters is
    a copy of the Unicode database that can disagree with it -- and bounded at
    U+3000 because everything above it is either already unaccented or
    decomposes to more than one character, which ``translate`` cannot express.
    """
    source, target = [], []
    for codepoint in range(0x80, 0x3000):
        character = chr(codepoint)
        base = "".join(
            c
            for c in unicodedata.normalize("NFD", character)
            if unicodedata.category(c) != "Mn"
        )
        if len(base) == 1 and base != character:
            source.append(character)
            target.append(base)
    return "".join(source), "".join(target)


def deaccent(column: Column) -> Column:
    """
    ``normalize("NFD", value)`` with the combining marks dropped.

    Spark exposes no Unicode normaliser, so the two things NFD-then-drop-Mn
    actually does are done separately: a precomposed character is translated to
    its base letter, and a mark already standing on its own is deleted.

    :param column: A string column.
    :returns: The same text with diacritics removed.
    """
    source, target = _fold_pairs()
    return F.regexp_replace(F.translate(column, source, target), r"\p{Mn}", "")


def zfill(column: Column, width: int) -> Column:
    """
    ``str.zfill`` as a column expression.

    ``lpad`` is the obvious translation and it is wrong in both directions. It
    TRUNCATES a value longer than the width -- ``lpad("123", 2, "0")`` is
    ``"12"`` -- which would silently rewrite a six-digit processing code from a
    different network into a two-digit one that means something else entirely,
    on a column whose whole point is that it is positional. And it pads in
    front of a sign, where ``zfill`` pads after it.

    The sign case cannot arise in either code column this project pads today.
    It is handled anyway because the cost is one branch and the alternative is
    a function that is only correct for the inputs it happens to have seen.

    :param column: The string to pad.
    :param width: Minimum width.
    :returns: The value, left-padded with zeros to at least ``width``.
    """
    sign = column.substr(F.lit(1), F.lit(1))
    signed = sign.isin("+", "-")
    body = column.substr(F.lit(2), F.length(column))
    padded = F.when(
        signed, F.concat(sign, F.lpad(body, max(width - 1, 0), "0"))
    ).otherwise(F.lpad(column, width, "0"))
    return F.when(F.length(column) < width, padded).otherwise(column)


def one_of(key: Column, values) -> Column:
    """
    :param key: The expression to test.
    :param values: A collection of literals.
    :returns: Whether the key is one of them, and False -- never null -- when
        the collection is empty. ``isin()`` on an empty list yields a literal
        False in Spark, but only as an accident of how it folds; stating it
        keeps a stage whose rule file went empty from turning every row null.
    """
    values = sorted(values)
    return F.lit(False) if not values else key.isin(values)


def chain(branches, otherwise: Column | None = None) -> Column:
    """
    Folds ``(condition, value)`` pairs into one ``when`` chain.

    Written out because nearly every stage here is one: pandas assigns into a
    series in priority order and the LAST assignment wins, while Spark reads a
    ``when`` chain in the order written and the FIRST branch wins. Porting a
    pandas block therefore means reversing it, and doing that by hand in nine
    stages is how a stage ends up with its precedence quietly inverted.

    :param branches: ``(condition, value)`` in Spark priority order -- highest
        priority first, which is the reverse of the pandas assignment order.
    :param otherwise: Value when no branch matches; null when not given.
    :returns: The chained expression.
    """
    branches = list(branches)
    if not branches:
        return otherwise if otherwise is not None else F.lit(None)
    condition, value = branches[0]
    expression = F.when(condition, value)
    for condition, value in branches[1:]:
        expression = expression.when(condition, value)
    return expression if otherwise is None else expression.otherwise(otherwise)


# ---------------------------------------------------------------------------
# Rule tables too large to inline
# ---------------------------------------------------------------------------
#
# ``lookup`` above stops being the cheapest answer well before the merchant
# master: 2,100 entries become 4,200 literal expressions inside the query plan,
# serialised to every task on every stage that touches it, and blow the codegen
# method limit long before that becomes the problem.
#
# Past that size the table belongs in a DataFrame and the lookup belongs in a
# join -- and the join has to be broadcast. A plain join between 265,195
# transactions and a 2,100-row table plans a shuffle: both sides are
# hash-partitioned on the join key and moved, so a small lookup costs a full
# redistribution of the data it is looking things up in. ``broadcast()`` sends
# the small side to every executor instead and leaves the large side where it
# is.
#
# Spark will often infer this on its own -- autoBroadcastJoinThreshold defaults
# to 10MB. It is stated anyway: inference is based on statistics, statistics on
# a frame built from a Python list are estimates, and a plan that silently
# reverts to a shuffle join is a performance change nothing reports.
#
# Every table is derived from the rule files at call time. Nothing here
# declares a merchant, a country or a rate.

# What a Python value in a rule file becomes as a Spark column type. Stated
# rather than inferred, because inference from a list of rows reads the first
# row: a holiday series whose first month is ``false`` would infer boolean,
# while an interest series whose first value happens to be a whole number would
# infer long and silently truncate every rate after it.
TYPES = {
    "string": StringType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
}


def table(spark, mapping: dict, key: str, value: str, value_type: str):
    """
    Builds one lookup table, ready to join.

    :param spark: The active session.
    :param mapping: Key to value, from a rule file.
    :param key: Name the key column takes -- which must be the name of the
        column it will be joined against, since the join is by name.
    :param value: Name the value column takes.
    :param value_type: One of ``TYPES``.
    :returns: A two-column DataFrame, marked for broadcast.
    :raises ValueError: On an unknown type name, listing what is known.

    Keys are unique by construction -- they came from a dict -- which is what
    makes the join safe to leave as a left join. A lookup table with a repeated
    key silently multiplies rows, and the resulting frame passes every check
    except a row count.
    """
    if value_type not in TYPES:
        raise ValueError(
            f"no Spark type for {value_type!r}; known: {sorted(TYPES)}"
        )
    schema = StructType(
        [
            StructField(key, StringType(), nullable=False),
            StructField(value, TYPES[value_type], nullable=True),
        ]
    )
    rows = [(str(k), v) for k, v in mapping.items()]
    return F.broadcast(spark.createDataFrame(rows, schema))


def joined(frame, key: Column, mapping: dict, value: str, value_type: str):
    """
    Adds one column to a frame by broadcast-joining a rule table onto it.

    A LEFT join, and a null key is what makes that the right kind: a row whose
    key could not be built -- because the timestamp it was keyed on never
    parsed -- matches nothing, keeps its row, and takes a null value. That is
    ``dict.get`` returning ``None``, which is what the pandas original does
    with the same row.

    :param frame: The frame to add to.
    :param key: Expression producing the lookup key.
    :param mapping: Key to value, from a rule file.
    :param value: Name of the column added. Must not already exist.
    :param value_type: One of ``TYPES``.
    :returns: The frame with the column added and the key column removed.
    """
    if value in frame.columns:
        raise ValueError(
            f"{value!r} is already a column; a join would produce two of "
            f"that name and every later reference to it would be ambiguous"
        )
    # Named for the column it feeds, so two lookups in one stage cannot collide
    # on a shared scratch name -- which they would do silently, the second join
    # dropping the first one's key.
    key_column = f"_rule_key_{value}"
    return (
        frame.withColumn(key_column, key)
        .join(
            table(frame.sparkSession, mapping, key_column, value, value_type),
            on=key_column,
            how="left",
        )
        .drop(key_column)
    )
