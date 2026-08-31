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

The other two are genuinely fuzzy, and they are still joins.

The fuzzy rule is a prefix rule: a truncated name identifies a merchant when
exactly one entry in the master extends it. Which prefixes satisfy that is a
property of the master alone -- 2,100 entries, known before a single
transaction is read -- so it is resolved once on the driver into a flat
``prefix -> identity`` map, and the executors do an equality join against it
like every other rule lookup in this pipeline.

That is the whole trick, and it is worth stating why it beats the obvious
alternatives. A UDF would ship a Python function to every task and pay a
serialisation boundary per batch. A non-equi join on ``LIKE key || '%'``
would be a broadcast nested loop -- 2,100 comparisons per row. Expanding the
*master* into its prefixes instead turns the same question into an equality
join on a table of roughly 25,000 rows, which broadcasts, and the row side
never leaves the JVM.

The truncation fallback -- a name cut mid-way through a trailing
``-CARD PMT-`` suffix -- is the same lookup applied to successively trimmed
spellings, and ``coalesce`` over those attempts in order is the recursion the
original expressed with a recursive call.

What this stage does not build is the ``merchant_review`` queue. That is a
second frame rather than a column, its shape belongs to the sheet writer, and
the parity harness compares frames -- so it is Phase 06's, with the rest of
the output side.
"""

from pyspark.sql import functions as F

from src.schema.merchant import (
    AFFIXES,
    ALNUM_REF,
    BRANCH,
    CONFIRMED,
    INTERNAL,
    LEGAL,
    MERCHANT,
    MIN_PREFIX,
    NOT_A_MERCHANT,
    PENDING,
    PUNCT,
    REF_SUFFIX,
    STAR_REF,
    TRAILING_BRANCH,
    TRUNCATED_TAIL,
    UNIDENTIFIED,
    UNKNOWN_PROCESSOR,
    URL_SUFFIX,
    MerchantCleaner,
)
from src.rules import loader
from src.spark import audit
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


# How many times the truncated-suffix trim is retried. See _resolved.
TRIM_PASSES = 3


def _prefixes(index: dict[str, str]) -> dict[str, str]:
    """
    Every prefix that identifies exactly one thing, resolved on the driver.

    ``MerchantCleaner._resolver`` answers "does this key prefix exactly one
    identity" per lookup, by scanning a sorted index. The answer depends only
    on the master, so it is computed once here for every prefix the master
    admits, and the per-row question becomes a dictionary hit.

    Ambiguity is counted over identities, not spellings: AMERICAN UNIVERSITY
    and AMERICAN UNIVERSITY BEIRUT are two spellings of one merchant, and a
    prefix reaching both has identified it. A prefix reaching two different
    merchants identifies neither and is left out -- which is also what makes
    trap_pairs.json hold: a never-merge group has two identities by
    definition, so no prefix into it survives.

    :param index: Key to identity, as ``_index`` built it.
    :returns: Prefix to the single identity it names. Prefixes shorter than
        ``MIN_PREFIX`` are absent: four characters is the floor at which a
        fragment is evidence at all.
    """
    found: dict[str, set] = {}
    for key, identity in index.items():
        for length in range(MIN_PREFIX, len(key) + 1):
            found.setdefault(key[:length], set()).add(identity)

    return {
        prefix: identities.pop()
        for prefix, identities in found.items()
        if len(identities) == 1
    }


def _lookup(known: dict[str, str]) -> dict[str, str]:
    """
    The one table both passes of the resolver read.

    Exact spellings are merged over the prefixes rather than joined
    separately, because an exact hit outranks a prefix hit and a later key
    wins the merge. One dictionary means one join per spelling attempted
    instead of two.

    :param known: Any spelling to what it identifies.
    :returns: Key to identity, covering exact spellings and unambiguous
        prefixes.
    """
    index = _index(known)
    lookup = {**_prefixes(index), **index}
    # A name with no letters or digits identifies nothing. The original
    # returns "" for it before consulting the index at all, so an entry in
    # the master that happened to reduce to an empty key must not answer for
    # every punctuation-only name in the source.
    lookup.pop("", None)
    return lookup


def _trimmed(name):
    """
    :param name: The cleaned-name column.
    :returns: The same name with one trailing fragment of a truncated
        ``CARD PMT`` suffix removed, and surrounding space stripped.
    """
    return F.trim(F.regexp_replace(name, TRUNCATED_TAIL.pattern, ""))


def _resolved(frame, name, known: dict[str, str], column: str):
    """
    Resolves a cleaned name against one index, entirely in Spark.

    One broadcast join per spelling attempted, and ``coalesce`` picks the
    first that answered. The order is the original's: the full name, then the
    same name with one truncated suffix fragment trimmed, and so on. Trimming
    is bounded rather than recursive because the suffix pattern removes one
    trailing token per pass and the longest chain the vocabulary admits is
    two -- ``FOO CARD C`` losing ``C`` then ``CARD``. A third pass is carried
    for margin and costs a join over a table that is already broadcast.

    :param frame: The frame.
    :param name: The cleaned-name column.
    :param known: Any spelling to what it identifies.
    :param column: Name of the column the answer lands in.
    :returns: The frame with ``column`` added, holding what the name
        identifies or ``""``.
    """
    lookup = _lookup(known)
    scratches = []
    spelling = name

    for attempt in range(TRIM_PASSES + 1):
        scratch = f"_merchant_try{attempt}_{column}"
        key = F.regexp_replace(F.upper(spelling), r"[^A-Z0-9]", "")
        frame = rule_tables.joined(frame, key, lookup, scratch, "string")
        scratches.append(scratch)
        spelling = _trimmed(spelling)

    # An empty string, never null: the caller reads this as "nothing
    # identified", which is what the pandas resolver returned too.
    frame = frame.withColumn(
        column, F.coalesce(*[F.col(s) for s in scratches], F.lit(""))
    )
    return frame.drop(*scratches)


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


def metrics(frame, policy):
    """
    How many names the master recognised, and what the rest were.

    Six columns, all of them already on the row before this stage finished,
    and none of them touched by anything downstream -- MCC resolution reads
    the cleaned merchant name but never rewrites it.

    :param frame: The frame as the last stage left it.
    :param policy: Unused; the master, the descriptors and the processor
        whitelist are all vocabulary.
    :returns: ``(metric, request)`` pairs in report order.
    """
    if "MERCHANT_NAME_CLEANED" not in frame.columns:
        return []

    name = F.col("MERCHANT_NAME_CLEANED")
    unknown = F.col("MERCHANT_KIND") == F.lit(UNIDENTIFIED)
    return [
        (
            "matches_status.pending",
            audit.rows(F.col("MATCHES_STATUS_CLEANED") == F.lit(PENDING)),
        ),
        (
            "merchants_distinct",
            audit.distinct(F.when(F.col("MERCHANT_RECOGNISED"), name)),
        ),
        (
            "merchant.internal_movement_rows",
            audit.rows(F.col("INTERNAL_MOVEMENT")),
        ),
        (
            "merchant.internal",
            audit.ranked(
                F.when(F.col("INTERNAL_MOVEMENT"), name),
                "merchant.internal[{}]",
            ),
        ),
        ("merchant.unrecognised_rows", audit.rows(unknown)),
        (
            "merchant.unrecognised_names",
            audit.distinct(F.when(unknown, name)),
        ),
        (
            "processor_prefix_stripped",
            audit.rows(
                F.col("MERCHANT_PROCESSOR") != F.lit(UNKNOWN_PROCESSOR)
            ),
        ),
        ("empty_after_clean", audit.rows(name == "")),
    ]
