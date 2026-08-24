"""
MCC resolution: curated overrides, rules, and a review queue.

The only stage whose decision is not a function of a row. An MCC is settled
per *merchant*, by looking at every code that merchant's transactions carry and
asking whether the majority is unlikely to be an accident -- so the unit of
work is the merchant, and there are a few thousand of those against a quarter
of a million rows.

That shape decides the port. The count is the part that has to be distributed,
and it is one ``groupBy(merchant, code).count()``; the decision is a few
thousand rows of Python and it runs on the driver, from where the answer is
broadcast back and joined on. Nothing per-row crosses into Python at any point.

The driver half calls ``MccResolver._resolve`` -- the pandas implementation --
rather than restating it. That is deliberate and it is the main reason this
stage is short. The binomial tail it rests on is summed in log space against
the largest term, with an early exit and an underflow guard, and a second
implementation of it in Spark expressions would be both slower and a standing
invitation for the two engines to disagree by a threshold's width on a merchant
nobody looks at. Reusing it makes that class of divergence impossible rather
than merely unlikely.

Order had to be settled before this would agree, and it was not a theoretical
worry. ``Counter.most_common`` breaks a tie by insertion order, which on the
pandas side is the order the codes first appear in the FILE -- so which of two
tied codes won was a property of how the extract happened to be sorted, and
Spark has no file order to appeal to after a shuffle. One merchant on the
parity sample hits it (JUMIA, 5812 and 5411 tied at three rows each, 88 rows
affected). The fix is in ``MccResolver._resolve``, which now states the
tiebreak instead of inheriting it; the counts are fed here in the matching
order, count descending then code ascending.

The review queue -- one row per still-undecided merchant -- is not built here.
It is an analyst artifact rather than pipeline output, and the decisions it
needs are already a small table on the driver, so it belongs with the writer.
"""

from collections import Counter

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.cleaners.mcc import HIGH, SIGNAL, MccResolver
from src.rules import loader
from src.spark.spark_utils import text

FLAGS = "VALIDATION_FLAGS"
SUGGESTED = "MCC_CODE_SUGGESTED"
CONFIDENCE = "MCC_CONFIDENCE"
CLEANED = "MCC_CODE_CLEANED"
MERCHANT = "MERCHANT_NAME_CLEANED"

# Scratch columns carrying the merchant's decision onto each of its rows.
_SCRATCH = "_mcc_"


def _decide(counts: dict[str, Counter], overrides, catch_all, suspect,
            thresholds) -> dict[str, dict]:
    """
    The per-merchant decision, on the driver.

    Mirrors ``MccResolver._decide_per_merchant``: a curated assertion is
    settled by definition, a merchant whose rows all carry one code has nothing
    to resolve, and everything else goes to the scored signals.

    :param counts: Merchant to a Counter of its codes, already ordered.
    :returns: Merchant to the decision dict ``_resolve`` produces.
    """
    decisions: dict[str, dict] = {}
    for merchant, tally in counts.items():
        observed = dict(tally.most_common())

        if merchant in overrides:
            decisions[merchant] = {
                "mcc": overrides[merchant]["mcc"],
                "confidence": HIGH,
                "signal": "curated",
                "observed": observed,
                "p_value": None,
            }
            continue

        # One code across every row of this merchant: nothing disagrees, so
        # there is nothing to resolve and nothing to review.
        if len(tally) == 1:
            decisions[merchant] = {
                "mcc": None,
                "confidence": HIGH,
                "signal": "consistent",
                "observed": observed,
                "p_value": None,
            }
            continue

        decisions[merchant] = MccResolver._resolve(
            observed, tally, catch_all, suspect, thresholds
        )
    return decisions


def _tallies(frame) -> dict[str, Counter]:
    """
    The distributed half: how many rows each merchant carries of each code.

    :returns: Merchant to a Counter built in count-descending, code-ascending
        order -- stated rather than inherited, since the order a Counter was
        filled in decides which of two tied codes ``most_common`` puts first.
    """
    rows = (
        frame.groupBy(MERCHANT, CLEANED)
        .count()
        # Sorted here rather than on the driver so the order is a property of
        # the query and not of whatever order collect happened to return.
        .orderBy(F.col(MERCHANT), F.col("count").desc(), F.col(CLEANED))
        .collect()
    )

    tallies: dict[str, Counter] = {}
    for row in rows:
        merchant = row[MERCHANT]
        # An unnamed merchant is not a merchant. pandas drops the null key at
        # groupby and skips the empty one in the loop; both land here.
        if not merchant:
            continue
        tallies.setdefault(merchant, Counter())[row[CLEANED]] = row["count"]
    return tallies


def _decision_frame(spark, decisions: dict[str, dict]):
    """
    :returns: The decisions as a broadcast frame, one row per merchant, ready
        to join onto the transactions.
    """
    schema = StructType(
        [
            StructField(MERCHANT, StringType(), nullable=False),
            StructField(f"{_SCRATCH}mcc", StringType(), nullable=True),
            StructField(f"{_SCRATCH}confidence", StringType(), nullable=True),
            StructField(f"{_SCRATCH}signal", StringType(), nullable=True),
        ]
    )
    rows = [
        (merchant, d["mcc"], d["confidence"], d["signal"])
        for merchant, d in decisions.items()
    ]
    return F.broadcast(spark.createDataFrame(rows, schema))


def apply(frame, policy):
    """
    Assigns an MCC suggestion and a confidence tier without ever overwriting
    the code the file arrived with.

    Signals are applied in priority order: a curated override, then the
    deterministic ATM rule, then the catch-all override, then a tiebreak
    against suspect codes, then majority vote scored by a binomial tail.

    :param frame: Frame with ``MCC_CODE_CLEANED`` from ``codes`` and
        ``MERCHANT_NAME_CLEANED`` from ``merchant``; unchanged without both,
        which is how the pandas original behaves.
    :param policy: Unused. Every threshold this stage reads is in
        ``mcc_rules.json``, beside the codes it describes.
    :returns: The frame with the suggestion, the confidence, the signal and the
        adopted code.
    """
    if not {CLEANED, MERCHANT}.issubset(frame.columns):
        return frame

    rules = loader.mcc_rules()
    # Only entries that actually assert an MCC are overrides; a master entry
    # carrying just aliases says nothing about categorisation.
    overrides = {
        name: entry
        for name, entry in loader.merchants().items()
        if "mcc" in entry
    }

    decisions = _decide(
        _tallies(frame),
        overrides,
        rules["catch_all"],
        set(rules["suspect_codes"]),
        rules["confidence"],
    )

    if decisions:
        frame = frame.join(
            _decision_frame(frame.sparkSession, decisions),
            on=MERCHANT,
            how="left",
        )
    else:
        for suffix in ("mcc", "confidence", "signal"):
            frame = frame.withColumn(
                f"{_SCRATCH}{suffix}", F.lit(None).cast("string")
            )

    decided = F.col(f"{_SCRATCH}mcc")
    frame = frame.withColumn(
        # Blank where the merchant has no decision, where the decision names no
        # code, and where it names the code the row already carries: a
        # suggestion is only a suggestion if it would change something.
        SUGGESTED,
        F.when(
            decided.isNull() | (decided == F.col(CLEANED)), F.lit("")
        ).otherwise(decided),
    ).withColumn(
        # The joined value as it stands, null where the merchant had no
        # decision at all. pandas spells that "NONE" and then makes the column
        # a Categorical over HIGH/MEDIUM/PENDING -- and since "NONE" is not one
        # of the categories, what actually reaches the frame is a null. A
        # coalesce to the literal string here would be a one-word bug that only
        # the parity harness would catch.
        CONFIDENCE,
        F.col(f"{_SCRATCH}confidence"),
    ).withColumn(
        # Which rule decided this row's code. It repeats across every row of a
        # merchant, which is why it is not on the presented sheet -- but it
        # stays on the frame, because the alternative is a total in the report
        # that no row can be traced back to.
        SIGNAL,
        F.coalesce(F.col(f"{_SCRATCH}signal"), F.lit("")),
    )

    frame = _apply_deterministic(frame, rules)

    # One MCC column leaves the pipeline, holding the code that survived
    # validation. A suggestion only exists at HIGH or MEDIUM confidence --
    # PENDING resolves to no code at all -- so adopting it here never promotes
    # a guess.
    frame = frame.withColumn(
        CLEANED,
        F.when(F.col(SUGGESTED) != "", F.col(SUGGESTED)).otherwise(
            F.col(CLEANED)
        ),
    )

    return frame.drop(
        *[c for c in frame.columns if c.startswith(_SCRATCH)]
    )


def _apply_deterministic(frame, rules: dict):
    """
    Applies rules where another column independently fixes the MCC.

    :returns: The frame with the deterministic overrides applied, and the
        violation flagged per row -- not only in the report, because a
        deterministic violation has to be traceable to the transaction that
        caused it.
    """
    for rule in rules.get("deterministic", []):
        column = rule["when_column"]
        # A rule whose trigger column is absent never ran.
        if column not in frame.columns:
            continue

        wrong = (text(column) == F.lit(rule["when_value"])) & (
            F.col(CLEANED) != F.lit(rule["expect_mcc"])
        )
        marker = f"{_SCRATCH}wrong"
        frame = frame.withColumn(marker, F.coalesce(wrong, F.lit(False)))

        existing = (
            F.coalesce(F.col(FLAGS), F.lit(""))
            if FLAGS in frame.columns
            else F.lit("")
        )
        frame = (
            frame.withColumn(
                SUGGESTED,
                F.when(
                    F.col(marker), F.lit(rule["expect_mcc"])
                ).otherwise(F.col(SUGGESTED)),
            )
            .withColumn(
                CONFIDENCE,
                F.when(F.col(marker), F.lit(HIGH)).otherwise(
                    F.col(CONFIDENCE)
                ),
            )
            .withColumn(
                SIGNAL,
                F.when(F.col(marker), F.lit("deterministic")).otherwise(
                    F.col(SIGNAL)
                ),
            )
            .withColumn(
                FLAGS,
                F.when(
                    F.col(marker),
                    F.when(
                        existing.isin("", rule["flag"]), F.lit(rule["flag"])
                    ).otherwise(
                        F.concat(existing, F.lit(";"), F.lit(rule["flag"]))
                    ),
                ).otherwise(existing),
            )
            .drop(marker)
        )
    return frame
