"""
Code columns: the padding and the lookups, as expressions.

The simplest stage in the pipeline and the one that shows the shape of the
port most clearly. Three of its four columns are ``text(...)`` followed by a
pad or a map lookup, and the only judgement in it -- the two field widths --
comes from the policy rather than from anything here.

The one thing worth reading twice is what an unknown code produces.
``PROCESSING_TYPE_CLEANED`` is a Categorical on the pandas side, built from
labels where an unknown code contributed ``""``; since ``""`` is not one of
the categories, pandas stores a null. So the cleaned type of a code nobody
has classified is NULL, not blank -- and this port has to produce a null too,
because null and empty string are the distinction the whole pipeline is built
to preserve. ``lookup`` with no default does exactly that; a ``fillna("")``
here would be a one-word bug that no test outside the parity harness would
notice.
"""

from pyspark.sql import functions as F

from src.rules import loader
from src.spark import audit
from src.spark.spark_utils import lookup, text, zfill


def apply(frame, policy, mcc_reference: dict | None = None):
    """
    Restores the leading zeros an integer column destroyed and regenerates the
    labels from the reference rather than trusting the incoming text.

    :param frame: All-string frame.
    :param policy: Read for the two field widths, which are a property of the
        source network and live in ``config/policy.yaml`` with the reasoning
        that fixes them.
    :param mcc_reference: MCC code to category, from the workbook. Empty by
        default, matching ``TransactionCleaner``: a delimited source carries
        no reference sheet, so every row's category is blank and
        ``mcc.not_in_reference`` is not reported at all. Taken as an argument
        rather than read from a file so that the driver decides once and the
        executors are handed the answer -- the same reason the policy is
        injected.
    :returns: The frame with the code columns added.
    """
    codes = loader.processing_codes()
    widths = policy.codes

    if "PROCESSING_CODE" in frame.columns:
        frame = frame.withColumn(
            "PROCESSING_CODE_CLEANED",
            zfill(text("PROCESSING_CODE"), widths.processing_code_width),
        ).withColumn(
            # No default: an unclassified code is null here, not blank. See
            # the module docstring -- this is the Categorical's doing and it
            # is load-bearing.
            "PROCESSING_TYPE_CLEANED",
            lookup(F.col("PROCESSING_CODE_CLEANED"), codes),
        )

    if "MCC_CODE" in frame.columns:
        frame = frame.withColumn(
            "MCC_CODE_CLEANED", zfill(text("MCC_CODE"), widths.mcc_width)
        ).withColumn(
            # Blank rather than null, and the asymmetry with the line above is
            # the pandas original's: this one never becomes a Categorical, so
            # ``reference.get(c, "")`` is what reaches the column.
            "MCC_CATEGORY",
            lookup(
                F.col("MCC_CODE_CLEANED"), mcc_reference or {}, F.lit("")
            ),
        )

    return frame


# What ``Categorical.astype(str)`` spells a null as, and the reason the
# disagreement count below is not simply a string comparison: an unclassified
# code's cleaned type is null, pandas reads that back as the text ``"nan"``,
# and ``"nan"`` disagrees with whatever the source wrote. Reproducing the
# spelling reproduces the count.
_NULL_AS_TEXT = "nan"


def metrics(frame, policy, mcc_reference: dict | None = None):
    """
    What the reference could not classify, and where the source contradicts it.

    Every count here reads either a column this stage wrote or a raw column,
    and a raw column is never overwritten by any stage, so reading one at the
    end of the run gives exactly what reading it during the stage would have.
    That is what lets this stage add no provenance column of its own: the
    provenance is the source value, still sitting there.

    :param frame: The frame as the last stage left it.
    :param policy: Read for the MCC field width, for the same reason
        ``apply`` reads it.
    :param mcc_reference: The reference the run was given, if any. Empty by
        default and empty in practice on this path -- a delimited source
        carries no reference sheet -- in which case ``mcc.not_in_reference``
        is not reported at all rather than reported as every row.
    :returns: ``(metric, request)`` pairs in report order.
    """
    columns = set(frame.columns)
    out = []

    if "PROCESSING_CODE_CLEANED" in columns:
        known = loader.processing_codes()
        out.append((
            "processing_code.unknown",
            audit.rows(
                ~F.coalesce(
                    F.col("PROCESSING_CODE_CLEANED").isin(*sorted(known)),
                    F.lit(False),
                )
            ),
        ))
        if "PROCESSING_TYPE" in columns:
            resolved = F.coalesce(
                F.col("PROCESSING_TYPE_CLEANED"), F.lit(_NULL_AS_TEXT)
            )
            out.append((
                "processing_type.disagrees_with_code",
                audit.rows(
                    (text("PROCESSING_TYPE") != resolved) & (resolved != "")
                ),
            ))

    if "MCC_CODE" in columns:
        if mcc_reference and "MCC_CATEGORY" in columns:
            out.append((
                "mcc.not_in_reference",
                audit.rows(F.col("MCC_CATEGORY") == ""),
            ))
        # From the raw column, not MCC_CODE_CLEANED: the ``mcc`` stage adopts
        # its suggestions into that column later in the run, so counting it
        # at the end would answer "how many codes did we publish" where this
        # metric asks "how many did the source distinguish".
        out.append((
            "mcc.distinct",
            audit.distinct(
                zfill(text("MCC_CODE"), policy.codes.mcc_width)
            ),
        ))

    return out
