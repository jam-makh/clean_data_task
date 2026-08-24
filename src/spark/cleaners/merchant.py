"""
Merchant name cleaning: the string work as expressions, the fuzzy match as a
join plus one Arrow-batched fallback.

``MerchantCleaner`` does two separable things, and the port treats them
differently on purpose.

**Reducing a raw string to a stable key is regular.** It is a fixed sequence
of substitutions -- split on the processor star, strip the channel and
terminal affixes, delete reference codes and legal suffixes, drop the tokens
that are store numbers -- and every one of them is a ``regexp_replace``, an
``upper`` or a higher-order function over the token array. None of it needs
Python, so none of it uses Python. The regexes themselves are imported from
the pandas module rather than retyped: they are parsing algorithm, they live
in code by the project's own rule, and two copies of a regex is two regexes.

**Deciding what a cleaned name identifies is not regular.** The resolver
tries an exact key, then a prefix that exactly one master entry extends, then
the same again after trimming a truncated ``-CARD PMT-`` tail off the end --
recursively. The first of those three is a lookup and is done as a broadcast
join, which is the honest shape for it: a 2,100-row master against 265,195
transactions, sent to the executors rather than shuffling the transactions.

The other two are genuinely fuzzy and are done in a ``pandas_udf`` over the
rows the join missed. Three things make that the right call rather than a
retreat:

* it is the *same* Python function the pandas pipeline runs, so the two
  cannot disagree about a prefix -- parity here is by construction, not by
  testing;
* a ``pandas_udf`` moves a whole Arrow batch across the JVM boundary and
  hands it to Python as one Series, where a plain ``udf`` serialises one row
  at a time and pays the round trip 265,195 times. On this stage that is the
  difference between seconds and minutes, for identical output;
* it runs on a fraction of the rows. Names the exact join resolved are
  passed in as null and returned immediately, so the fuzzy path costs what
  the fuzzy cases cost.

What this stage does not build is the ``merchant_review`` queue. That is a
second frame rather than a column, its shape belongs to the sheet writer, and
the parity harness compares frames -- so it is Phase 06's, with the rest of
the output side.
"""

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.cleaners.merchant import (
    AFFIXES,
    ALNUM_REF,
    BRANCH,
    CONFIRMED,
    INTERNAL,
    LEGAL,
    MERCHANT,
    NOT_A_MERCHANT,
    PENDING,
    PUNCT,
    REF_SUFFIX,
    STAR_REF,
    TRAILING_BRANCH,
    UNIDENTIFIED,
    UNKNOWN_PROCESSOR,
    URL_SUFFIX,
    MerchantCleaner,
)
from src.rules import loader
from src.spark import spark_utils as rule_tables
from src.spark.spark_utils import chain, deaccent, strip, text

INPUT = "MERCHANT_NAME"

# A pure number is only a store number when something precedes it, which is
# what keeps "7 ELEVEN" intact. Written as an explicit ASCII range rather than
# ``\d``: Java's ``\d`` is ASCII-only and Python's ``str.isdigit`` is not, and
# the two would disagree about an Arabic-Indic numeral -- one keeping the
# token, the other dropping it.
_DIGITS = "^[0-9]+$"


def _index(known: dict[str, str]) -> dict[str, str]:
    """
    :param known: Any spelling to what it identifies.
    :returns: The same, keyed on ``MerchantCleaner._key`` -- letters and
        digits only, which is what makes BLOM BANK and BLOMBANK one lookup.
        First spelling wins on a collision, which is ``setdefault``'s
        behaviour in the original and is why this is built here rather than
        with a dict comprehension.
    """
    index: dict[str, str] = {}
    for spelling, identity in known.items():
        index.setdefault(MerchantCleaner._key(spelling), identity)
    return index


def clean(value):
    """
    ``MerchantCleaner.clean_one`` as expressions.

    :param value: The raw merchant column.
    :returns: ``(cleaned name, processor prefix)``, both never null -- the
        original returns ``("", "")`` for an absent value and the empty
        string is a value the rest of the stage reads.
    """
    processors = sorted(loader.processors())
    raw = F.coalesce(value, F.lit(""))
    folded = deaccent(F.upper(raw))

    # The ``*`` split, gated on the processor whitelist because the merchant
    # sits on either side of it: ``SQ *TAKEALOT`` puts it on the right,
    # ``COURSERA.COM *W2PA`` on the left. A blind split corrupts 114
    # merchants.
    starred = folded.contains("*")
    left = F.substring_index(folded, "*", 1)
    right = F.substring(folded, F.instr(folded, "*") + 1, F.length(folded))
    left_key = strip(F.regexp_replace(strip(left), r"\.[A-Z]{2,10}$", ""))
    by_processor = starred & left_key.isin(processors) & (strip(right) != "")

    prefix = F.when(by_processor, left_key).otherwise(F.lit(""))
    body = chain(
        [(~starred, folded), (by_processor, right)], otherwise=left
    )

    # Before PUNCT, which would turn '/' and ':' into spaces and leave each
    # affix looking like an ordinary leading word.
    for affix in AFFIXES:
        body = F.regexp_replace(strip(body), affix.pattern, "")

    for pattern in (STAR_REF, REF_SUFFIX, URL_SUFFIX, PUNCT, BRANCH, LEGAL):
        body = F.regexp_replace(body, pattern.pattern, " ")

    # Drop reference codes and store numbers. ``split`` on a run of whitespace
    # after collapsing it, so an empty token cannot appear where pandas'
    # bare ``split()`` would have produced none.
    tokens = F.split(strip(F.regexp_replace(body, r"\s+", " ")), " ")
    kept = F.filter(
        tokens,
        lambda token, position: ~(
            token.rlike(ALNUM_REF.pattern)
            | (token.rlike(_DIGITS) & (position > 0))
        ),
    )
    # Only if something other than a bare number survives: a merchant whose
    # whole name is digits keeps it.
    body = F.when(
        F.exists(kept, lambda token: ~token.rlike(_DIGITS)),
        F.array_join(kept, " "),
    ).otherwise(body)

    body = strip(F.regexp_replace(body, r"\s+", " "))
    cleaned = strip(F.regexp_replace(body, TRAILING_BRANCH.pattern, ""))

    # An empty source is empty output, decided before any of the above and
    # applied after it, because a string of nothing but punctuation is not the
    # same case -- that one goes through the whole pipeline and comes out
    # empty on its own.
    blank = strip(raw) == ""
    return (
        F.when(blank, F.lit("")).otherwise(cleaned),
        F.when(blank, F.lit("")).otherwise(prefix),
    )


def _fuzzy(known: dict[str, str]):
    """
    Builds the Arrow-batched fallback for one index.

    :param known: Any spelling to what it identifies.
    :returns: A ``pandas_udf`` taking cleaned names and returning what each
        identifies, or ``""``. A null name -- which is how the caller says
        "already resolved, do not bother" -- returns ``""`` without a lookup.

    The resolver is the pandas pipeline's own, closed over here and shipped
    with the task. Rebuilding it per batch would sort a 2,100-entry index
    once per Arrow batch rather than once per task, so it is built when the
    UDF is defined -- on the driver, which is also what makes the rule file a
    driver-side read rather than something every executor has to find.
    """
    resolve = MerchantCleaner._resolver(known)

    @F.pandas_udf(StringType())
    def resolver(names: pd.Series) -> pd.Series:
        # ``name is None`` alone is not the null test. Arrow hands a null back
        # as a float NaN rather than as None, so a row the exact join already
        # answered -- which is passed in as null precisely to skip it --
        # reached ``resolve`` as a float and died on ``.upper()``. The
        # ``value != value`` half is the same test ``clean_one`` uses on the
        # pandas side, for the same reason.
        return names.map(
            lambda name: ""
            if name is None or name != name
            else resolve(name)
        )

    return resolver


def _resolved(frame, name, known: dict[str, str], column: str):
    """
    Resolves a cleaned name against one index: exact by join, the rest by UDF.

    :param frame: The frame.
    :param name: The cleaned-name column.
    :param known: Any spelling to what it identifies.
    :param column: Name of the column the answer lands in.
    :returns: The frame with ``column`` added, holding what the name
        identifies or ``""``.
    """
    exact = f"_merchant_exact_{column}"
    key = F.regexp_replace(F.upper(name), r"[^A-Z0-9]", "")
    frame = rule_tables.joined(frame, key, _index(known), exact, "string")

    # Only the misses reach Python. The join has already answered for every
    # name spelled a way the master knows, which on this source is most rows
    # and all of the common merchants.
    return frame.withColumn(
        column,
        F.coalesce(
            F.col(exact),
            _fuzzy(known)(
                F.when(F.col(exact).isNull(), name).otherwise(
                    F.lit(None).cast("string")
                )
            ),
        ),
    ).drop(exact)


def apply(frame, policy):
    """
    Reduces every merchant string to a stable name and says what it is.

    :param frame: Frame carrying the raw merchant column.
    :param policy: Unused -- the master, the descriptors and the processor
        whitelist are all vocabulary.
    :returns: The frame with the cleaned name and the five readings of it that
        later stages and the review queue ask for.
    """
    if INPUT not in frame.columns:
        return frame

    cleaned, prefix = clean(F.col(INPUT))
    name = "_merchant_name"
    frame = frame.withColumn(name, cleaned).withColumn(
        "MERCHANT_PROCESSOR",
        F.when(prefix == "", F.lit(UNKNOWN_PROCESSOR)).otherwise(prefix),
    )

    # Descriptors are asked first because the question they answer comes
    # first: a row describing money moving inside the bank has no counterparty
    # to look up, and looking one up anyway is what put CARD SETTLEMENT at the
    # top of the merchant table.
    frame = _resolved(
        frame, F.col(name), loader.internal_descriptors(), "_merchant_kind"
    )
    frame = _resolved(
        frame, F.col(name), loader.merchant_aliases(), "_merchant_canonical"
    )

    movement = F.col("_merchant_kind")
    canonical = F.col("_merchant_canonical")
    labels = loader.internal_movement_labels()

    kind = chain(
        [
            (movement != "", F.lit(INTERNAL)),
            (canonical != "", F.lit(MERCHANT)),
        ],
        otherwise=F.lit(UNIDENTIFIED),
    )
    frame = frame.withColumn("MERCHANT_KIND", kind)
    kind = F.col("MERCHANT_KIND")

    frame = frame.withColumn(
        "MERCHANT_NAME_CLEANED",
        chain(
            [
                # Named by the movement rather than by the descriptor: the
                # descriptor is truncated to eleven different lengths and
                # eight of them identify the kind without identifying which
                # transfer it was.
                (
                    kind == F.lit(INTERNAL),
                    F.coalesce(
                        F.element_at(
                            F.create_map(
                                *[
                                    item
                                    for pair in labels.items()
                                    for item in (F.lit(pair[0]), F.lit(pair[1]))
                                ]
                            ),
                            movement,
                        ),
                        movement,
                    ),
                ),
                (kind == F.lit(MERCHANT), canonical),
            ],
            otherwise=F.col(name),
        ),
    )

    return (
        frame.withColumn(
            "MERCHANT_TYPE",
            F.when(kind == F.lit(INTERNAL), F.lit(INTERNAL)).otherwise(
                F.lit(MERCHANT)
            ),
        )
        .withColumn("MERCHANT_RECOGNISED", kind == F.lit(MERCHANT))
        .withColumn("INTERNAL_MOVEMENT", kind == F.lit(INTERNAL))
        # Recomputed from this run rather than carried over from the file: the
        # incoming status was decided against whatever master existed then,
        # and a row that resolves against the master now is matched now.
        .withColumn(
            "MATCHES_STATUS_CLEANED",
            chain(
                [
                    (kind == F.lit(MERCHANT), F.lit(CONFIRMED)),
                    (kind == F.lit(INTERNAL), F.lit(NOT_A_MERCHANT)),
                ],
                otherwise=F.lit(PENDING),
            ),
        )
        .drop(name, "_merchant_kind", "_merchant_canonical")
    )
