"""
Recovery of the macro columns, as three broadcast joins on the month.

The stage the join model fits best: each of the three columns is a series
keyed by the month, or by the month and the country, and the value a missing
row should carry is already stated by its neighbours -- exactly and without a
model. That is a lookup, and a lookup against a 42-, 252- or 504-row table is
what ``broadcast`` is for. See ``src/spark/spark_utils.py`` for why it is spelled
rather than left to the optimiser.

Two things are easy to get wrong here and both are about nulls.

A row whose ``TXN_TS`` never parsed has no month, so it has no key, so the
LEFT join gives it a null value -- which is the same null ``keys.map(lookup)``
produces for a NaN key in pandas, arrived at the same way. It is left to
happen rather than special-cased.

A row that STATES a value keeps it, even when that value is unreadable. The
translation is ``when(stated, cast(stated)).otherwise(recovered)`` and NOT
``coalesce(cast(stated), recovered)``: the second one quietly repairs a
corrupt stated value from the series, which is imputation wearing a lookup's
clothes and would contradict the 236,045 values the file already gives.
"""

from pyspark.sql import functions as F

from src.cleaners.macro import COVERAGE, SERIES, TRUTHY
from src.rules import loader
from src.spark import audit
from src.spark import spark_utils as rule_tables
from src.spark.spark_utils import chain, lookup, one_of, text

TIMESTAMP = "TXN_TS"

# What each series column holds once it is a value rather than text. The
# holiday flag is the odd one and the reason ``_coerce`` exists at all: the
# source spells it ``True``/``False`` and the rule file states a JSON boolean,
# so the two halves of one column arrive as two different types.
_TYPES = {"IS_HOLIDAY_MONTH": "boolean"}


def _coerce(source: str, stated, recovered):
    """
    :param source: The source column name, which decides the type.
    :param stated: The raw text the source carried, null where it carried
        nothing.
    :param recovered: The value the series states for this row's key.
    :returns: The one value, as the type the column actually holds -- so a
        recovered value and an observed one are indistinguishable downstream,
        which is the point of recovering it.
    """
    if _TYPES.get(source) == "boolean":
        # A spelling the truth table has never seen becomes null here and
        # raises on the pandas side, where ``astype("boolean")`` refuses a
        # leftover string. Neither is a silent wrong answer, and this source
        # spells the column two ways only.
        return F.when(
            stated.isNotNull(), lookup(F.upper(text(stated)), TRUTHY)
        ).otherwise(recovered)
    return F.when(stated.isNotNull(), stated.cast("double")).otherwise(
        recovered
    )


def apply(frame, policy):
    """
    Fills the macro columns by lookup, never by imputation.

    :param frame: Frame with ``TXN_TS`` already parsed -- this stage follows
        ``timestamps`` because the key it joins on is the very thing that was
        damaged on the rows it is recovering.
    :param policy: Unused; every number here is a published series.
    :returns: The frame with each series' cleaned column and its coverage.
    """
    if TIMESTAMP not in frame.columns:
        return frame

    series = loader.macro_series()
    month = F.date_format(F.col(TIMESTAMP), "yyyy-MM")

    for source, key, context in SERIES:
        if source not in frame.columns:
            continue
        if any(name not in frame.columns for name in context):
            continue

        table = series[key]
        uncovered = list(table.get("uncovered_countries", ()))
        value_type = _TYPES.get(source, "double")

        # ``"YYYY-MM"`` or ``"YYYY-MM|CC"``, built the way the rule file keys
        # itself. Null when the month is null, and that is deliberate: the
        # join then matches nothing and the row is UNRECOVERABLE, which is
        # what a row with no readable date actually is.
        lookup_key = month
        for name in context:
            lookup_key = F.concat(lookup_key, F.lit("|"), text(name))

        recovered = f"_macro_{source}"
        frame = rule_tables.joined(
            frame, lookup_key, table["values"], recovered, value_type
        )

        stated = F.col(source)
        gap = stated.isNull()
        # Named before the join rather than inferred from its failure: a
        # country outside the panel and a month missing from the rule file
        # both produce a null, and they mean opposite things.
        outside = (
            gap & one_of(text(context[0]), uncovered)
            if uncovered and context
            else F.lit(False)
        )

        frame = frame.withColumn(
            f"{source}_CLEANED", _coerce(source, stated, F.col(recovered))
        ).withColumn(
            f"{source}_COVERAGE",
            chain(
                [
                    (outside, F.lit("OUT_OF_PANEL")),
                    (gap & F.col(recovered).isNotNull(), F.lit("RECOVERED")),
                    (gap, F.lit("UNRECOVERABLE")),
                ],
                otherwise=F.lit("OBSERVED"),
            ),
        ).drop(recovered)

    return frame


def metrics(frame, policy):
    """
    How each series' rows were covered, read off the coverage columns.

    The coverage column was always the per-row statement; only the counting
    moved. A series this stage could not join at all leaves no coverage
    column, and so reports nothing rather than a row of zeros.

    :param frame: The frame as the last stage left it.
    :param policy: Unused; a coverage state is a fact about the join.
    :returns: ``(metric, request)`` pairs in report order.
    """
    out = []
    for source, _, _ in SERIES:
        column = f"{source}_COVERAGE"
        if column not in frame.columns:
            continue
        status = F.col(column)
        for value in COVERAGE:
            out.append((
                f"{source}.{value.lower()}",
                audit.rows(status == F.lit(value), nonzero=True),
            ))
    return out
