"""
Wall-clock normalization, as column expressions and windows.

The pandas original at ``src/cleaners/timestamps.py`` is the specification;
this is the same decision tree with the row loops taken out. Three things
change shape in the move and nothing else does:

**strptime becomes a Java pattern.** ``timestamp_formats.json`` states each
format the way Python spells it, and it stays the only place a format is
declared -- the translation below is an algorithm, not a second vocabulary,
for the same reason the merchant regexes are code and the merchant names are
data. A second ``java`` key in the rule file would be the same fact written
twice, and two spellings of one fact drift.

**Strict parsing replaces ``errors="coerce"``.** Under CORRECTED (set in
``src/spark/spark_setup.py``) a value that does not match its pattern parses to
null instead of being coaxed into a date, and with ANSI off the cast does not
raise. That pairing is what requirement 2 asks for: an unparseable row is
null, and null is what ``TXN_TS_FORMAT`` counts as UNPARSEABLE.

**The two weak passes over the ``/`` ambiguity become a join and a window.**
The macro oracle is a group of already-placed rows joined back onto the open
ones; the sequence bracket is ``last``/``first`` with ``ignoreNulls`` over
``partitionBy(ACCOUNT_ID).orderBy(TXN_SEQ)``. Neither needs a row at a time,
and the sequential dependency that is real -- the second oracle reads the
rows the first one placed -- is expressed as a second join rather than
pretended away.

The one thing that looks like it should need a UDF and does not is the UTC
offset label. pandas asks ``ZoneInfo`` for the offset at each naive reading,
which resolves a DST gap with ``fold=0``, i.e. the offset in force *before*
the transition. ``wall - to_utc_timestamp(wall, zone)`` gives exactly that
number without a Python call: Java resolves a gap by shifting the reading
forward by the gap width and applying the post-transition offset, and since
the gap width IS the difference between the two offsets, the shift cancels in
the subtraction and what is left is the pre-transition offset. An ambiguous
reading agrees for the simpler reason that both sides pick the earlier of the
two candidate offsets.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.cleaners.timestamps import (
    BRACKET_DAY_FIRST,
    BRACKET_MONTH_FIRST,
    DAY_FIRST,
    DAY_FIRST_DATE,
    FORCED_DAY_FIRST,
    FORCED_MONTH_FIRST,
    MACRO_ORACLES,
    MAJORITY,
    MONTH_FIRST,
    MONTH_FIRST_DATE,
    NULL_TOKEN,
    SETTLE_FORMAT,
    TS_FORMAT,
    TS_SLASH,
    UNPARSEABLE,
    UNRECOGNISED,
)
from src.rules import loader
from src.spark.spark_utils import chain, one_of, text

INPUT = "TXN_DATE_TIME"
SEQUENCE = "TXN_SEQ"
GROUP = "ACCOUNT_ID"

# Working column names. Prefixed so that a stage which crashes half way
# through leaves something obviously not part of the output, and dropped
# before the frame is returned -- the parity harness reports an extra column
# as a finding, which is the correct reaction to a stage that leaked scaffold.
_SCRATCH = "_ts_"

# The strptime directives this source's rule file actually uses, and the Java
# pattern letters that mean the same thing. Kept to what is in use rather than
# filled out speculatively: an unused translation is an untested one, and the
# failure it produces -- a date parsed to the wrong field -- is silent.
#
# ``%Y`` becomes ``yyyy`` rather than ``uuuu`` because Spark rewrites one to
# the other itself; both are the proleptic year under STRICT resolution.
#
# ``%d`` becomes ``d`` and not ``dd``, which is the one width that has to be
# variable: ``%b %d, %Y`` covers ``Jan 8, 2022`` as well as ``Jan 14, 2022``
# and strptime reads either, while Java's ``dd`` demands exactly two digits
# and returns null on the first. Widening it costs nothing because every
# branch is gated by its rule's own anchored regex, so the parser only ever
# sees a value of the shape that rule describes -- the regex is what bounds
# the width, in both engines.
_DIRECTIVES = {
    "%Y": "yyyy",
    "%y": "yy",
    "%m": "MM",
    "%d": "d",
    "%b": "MMM",
    "%H": "HH",
    "%M": "mm",
    "%S": "ss",
}

# Characters Java's pattern syntax gives a meaning to and strptime does not,
# so a literal one in a format has to be quoted on the way across. The comma
# in ``%b %d, %Y`` is the only such character in this rule file today, and the
# check is general because the next format added will not announce itself.
_JAVA_RESERVED = set("GyuMLdQqYwWEecFaBhKkHmsSAnNVvzOXxZp'#{}[]()<>")


def java_pattern(strptime: str) -> str:
    """
    Translates a strptime format into the Java datetime pattern for it.

    :param strptime: A format as ``timestamp_formats.json`` states it.
    :returns: The equivalent Java pattern, with literal text quoted.
    :raises ValueError: On a directive with no translation, naming it. A
        format that silently lost a field would parse the wrong number into
        the wrong place and produce a plausible date, which is the one
        failure mode no downstream check would catch.

    Two-digit years are the one inexact corner and it is inexact in a bounded
    way: ``%y`` pivots at 69 in Python (69-99 are 1900s) and Spark's ``yy``
    reads every two-digit year into the 2000s. Every such value in this
    source is 22 to 25, where the two agree, and the formats carrying it are
    ``\\d{2}``-anchored so a 1900s date cannot appear without the rule file
    changing first.
    """
    out, index = [], 0
    while index < len(strptime):
        character = strptime[index]
        if character == "%":
            directive = strptime[index:index + 2]
            if directive not in _DIRECTIVES:
                raise ValueError(
                    f"no Java equivalent for {directive!r} in {strptime!r}; "
                    f"translatable directives: {sorted(_DIRECTIVES)}"
                )
            out.append(_DIRECTIVES[directive])
            index += 2
            continue
        out.append(f"'{character}'" if character in _JAVA_RESERVED else character)
        index += 1
    return "".join(out)


def _parse(raw, strptime: str):
    """
    :param raw: The stripped source column.
    :param strptime: A concrete strptime format -- never one of the rule
        file's sentinel values, which the caller handles.
    :returns: The parsed timestamp, null where the value does not match.
    """
    return F.to_timestamp(raw, java_pattern(strptime))


def offset_label(wall):
    """
    :param wall: A naive wall-clock column, null where nothing parsed.
    :returns: ``UTC+N`` for the zone's offset on that reading, ``""`` for a
        null reading -- which is what ``_offsets`` returns for a NaT and what
        every reader of the column expects to mean "no reading".

    The zone is not a parameter: the offset is a property of the source's own
    clock, which ``timestamp_formats.json`` states once, and passing it in
    would let a caller label a reading with a zone the file was never written
    in.
    """
    zone = loader.timestamp_formats()["zone"]["name"]
    seconds = wall.cast("long") - F.to_utc_timestamp(wall, zone).cast("long")
    # Floor, not truncation, matching ``total_seconds() // 3600``. The two
    # differ only for a zone west of Greenwich on a non-whole-hour offset,
    # which is exactly the case a truncating port would get wrong and never
    # be told about.
    hours = F.floor(seconds / 3600)
    return F.when(
        wall.isNull(), F.lit("")
    ).otherwise(
        F.concat(
            F.lit("UTC"),
            F.when(hours < 0, F.lit("-")).otherwise(F.lit("+")),
            F.abs(hours).cast("string"),
        )
    )


def _direct_formats(rules):
    """
    :param rules: The rule file's format list.
    :returns: The rules that name a concrete strptime pattern, i.e. everything
        the two sentinel branches below do not handle.
    """
    return [
        rule for rule in rules
        if rule["strptime"] not in ("AMBIGUOUS_SLASH", "EPOCH_SECONDS")
    ]


def _format_label(raw, rules, null_tokens):
    """
    :returns: Which rule read this row, before the UNPARSEABLE correction --
        the rule file's own name, or one of the two answers that are not a
        rule. First match wins, which is the ``& ~matched`` in the pandas
        loop said the other way round.
    """
    return chain(
        [(one_of(raw, null_tokens), F.lit(NULL_TOKEN))]
        + [(raw.rlike(rule["regex"]), F.lit(rule["name"])) for rule in rules],
        otherwise=F.lit(UNRECOGNISED),
    )


def _epoch(raw, zone: str):
    """
    :returns: The instant in the epoch column rendered as a reading on the
        source's clock. The only branch in this stage that moves a value, for
        the reason the pandas original gives: the source stored an instant
        rather than a reading, and rendering it as a clock takes an offset.
    """
    return F.from_utc_timestamp(F.timestamp_seconds(raw.cast("long")), zone)


def _slash_candidates(raw):
    """
    :returns: ``(month-first reading, day-first reading, first field, second
        field)`` for the ambiguous format. Both readings are computed for
        every row in it; which one survives is what the three passes decide.
    """
    return (
        _parse(raw, MONTH_FIRST),
        _parse(raw, DAY_FIRST),
        F.substring(raw, 1, 2).cast("int"),
        F.substring(raw, 4, 2).cast("int"),
    )


def _macro_pass(frame, column: str, context, wall: str, route: str, candidates):
    """
    Settles open ``NN/NN`` rows against the month their macro value belongs to.

    Legitimate for the reason the pandas original states: the rate is constant
    within a year-month across the whole file, so it is evidence about the
    month that does not come from the date string being judged. The table is
    built only from rows already placed, which is what keeps it independent of
    what it is about to decide.

    :param frame: The frame, carrying the working columns.
    :param column: The oracle column, e.g. ``INTEREST_RATE_INDEX``.
    :param context: Extra columns its key is built from.
    :param wall: Name of the working column holding the readings so far.
    :param route: Name of the working column holding which pass settled each.
    :param candidates: ``(month-first, day-first)`` column names.
    :returns: The frame, with more rows placed. Unchanged when the oracle's
        columns are absent, which is the pandas ``continue``.
    """
    if column not in frame.columns:
        return frame
    if any(name not in frame.columns for name in context):
        return frame

    key = text(column)
    for name in context:
        key = F.concat(key, F.lit("|"), text(name))
    keyed = f"{_SCRATCH}key"
    frame = frame.withColumn(keyed, key)

    month_first, day_first = candidates
    # Months the already-placed rows show for each key. Built from the same
    # slash subset the pandas version builds it from -- ``out.dropna()`` there
    # is this filter here, and a row of another format never enters the table
    # in either engine.
    placed = (
        frame.filter(F.col(wall).isNotNull())
        .groupBy(keyed)
        .agg(F.collect_set(F.date_format(wall, "yyyy-MM")).alias(f"{_SCRATCH}months"))
    )
    frame = frame.join(F.broadcast(placed), on=keyed, how="left")

    months = F.col(f"{_SCRATCH}months")
    # ``pd.notna(value) and value.strftime(...) in months``: a candidate that
    # never parsed fits nothing, and a key with no table entry settles
    # nothing, so both collapse to False rather than to null.
    def fits(candidate):
        return F.coalesce(
            months.isNotNull()
            & F.array_contains(months, F.date_format(candidate, "yyyy-MM")),
            F.lit(False),
        )

    open_row = F.col(wall).isNull() & F.col(route).eqNullSafe(F.lit(""))
    fits_month, fits_day = fits(F.col(month_first)), fits(F.col(day_first))

    # Materialised before either output column is touched, and this is not
    # style. ``withColumn`` builds a projection over the frame it is called
    # on, so the second call in a chain sees the column the first one just
    # rewrote -- and every condition here is "is this row still open", which
    # placing a reading makes false. Written the obvious way, the reading
    # lands and the route that explains it never does.
    settled, picked = f"{_SCRATCH}settled", f"{_SCRATCH}picked"
    frame = frame.withColumn(
        settled, open_row & (fits_month != fits_day)
    ).withColumn(picked, fits_month)

    return (
        frame.withColumn(
            wall,
            F.when(
                F.col(settled) & F.col(picked), F.col(month_first)
            )
            .when(F.col(settled), F.col(day_first))
            .otherwise(F.col(wall)),
        )
        .withColumn(
            route,
            F.when(F.col(settled), F.lit(f"MACRO[{column}]")).otherwise(
                F.col(route)
            ),
        )
        .drop(keyed, f"{_SCRATCH}months", settled, picked)
    )


def _bracket_pass(frame, wall: str, route: str, candidates):
    """
    Brackets each remaining row between its nearest placed neighbours in
    transaction order and keeps the reading that fits.

    ``ffill``/``bfill`` within an account become ``last``/``first`` with
    ``ignoreNulls`` over the two half-open frames. The ordering column is
    unique across the file, so the window has no ties to break and the pass is
    the same on every run -- which the ``groupby(...).ffill()`` it replaces
    was only because the pandas sort is stable.

    :returns: The frame, with more rows placed.
    """
    if SEQUENCE not in frame.columns or GROUP not in frame.columns:
        return frame

    ordered = Window.partitionBy(GROUP).orderBy(F.col(SEQUENCE).cast("long"))
    backwards = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    forwards = ordered.rowsBetween(Window.currentRow, Window.unboundedFollowing)

    low, high = f"{_SCRATCH}low", f"{_SCRATCH}high"
    frame = frame.withColumn(
        low, F.last(F.col(wall), ignorenulls=True).over(backwards)
    ).withColumn(
        high, F.first(F.col(wall), ignorenulls=True).over(forwards)
    )

    month_first, day_first = candidates

    def between(candidate):
        """``Series.between`` is inclusive, and False rather than null when
        either end is missing -- a comparison against NaT is False in pandas
        and null in Spark, and null would leak into the ``only`` tests."""
        return F.coalesce(
            (F.col(candidate) >= F.col(low)) & (F.col(candidate) <= F.col(high)),
            F.lit(False),
        )

    open_row = F.col(wall).isNull() & F.col(route).eqNullSafe(F.lit(""))
    fits_month, fits_day = between(month_first), between(day_first)

    # Materialised for the reason ``_macro_pass`` gives at length: both output
    # columns are decided by a condition that writing either one falsifies.
    chose_month, chose_day = f"{_SCRATCH}bmonth", f"{_SCRATCH}bday"
    frame = frame.withColumn(
        chose_month, open_row & fits_month & ~fits_day
    ).withColumn(chose_day, open_row & fits_day & ~fits_month)

    return (
        frame.withColumn(
            wall,
            F.when(F.col(chose_month), F.col(month_first))
            .when(F.col(chose_day), F.col(day_first))
            .otherwise(F.col(wall)),
        )
        .withColumn(
            route,
            F.when(F.col(chose_month), F.lit(BRACKET_MONTH_FIRST))
            .when(F.col(chose_day), F.lit(BRACKET_DAY_FIRST))
            .otherwise(F.col(route)),
        )
        .drop(low, high, chose_month, chose_day)
    )


def _settle_dates(frame, rules, null_tokens):
    """
    :returns: The frame with ``SETTLE_DATE_CLEANED`` and its format label.

    Date-only in every spelling, so no offset and no precision: the field is
    a date by design rather than a timestamp that lost its time. The ``/``
    ambiguity is settled by field range alone here, exactly as in pandas --
    there is no macro oracle for it and none is invented.
    """
    raw = text("SETTLE_DATE")
    blank = one_of(raw, null_tokens)
    label = _format_label(raw, rules, null_tokens)

    branches = []
    for rule in rules:
        matched = raw.rlike(rule["regex"])
        if rule["strptime"] == "AMBIGUOUS_SLASH":
            month_first = _parse(raw, MONTH_FIRST_DATE)
            day_first = _parse(raw, DAY_FIRST_DATE)
            first = F.substring(raw, 1, 2).cast("int")
            forced_day = (first > 12) & day_first.isNotNull()
            branches.append(
                (matched, F.when(forced_day, day_first).otherwise(month_first))
            )
            continue
        branches.append((matched, _parse(raw, rule["strptime"])))

    value = chain([(blank, F.lit(None).cast("timestamp"))] + branches)
    unreadable = value.isNull() & ~blank & ~label.eqNullSafe(F.lit(UNRECOGNISED))
    return frame.withColumn("SETTLE_DATE_CLEANED", value).withColumn(
        SETTLE_FORMAT, F.when(unreadable, F.lit(UNPARSEABLE)).otherwise(label)
    )


def apply(frame, policy):
    """
    Resolves ``TXN_DATE_TIME`` to one wall clock plus the provenance of it.

    :param frame: All-string frame from ``src.spark.spark_setup.read_csv``.
    :param policy: Unused -- every judgement this stage makes is in
        ``timestamp_formats.json``, which is vocabulary rather than policy.
        Taken anyway because the registry hands every step the same pair, and
        a step that quietly took a different signature would fail at the call
        site rather than here.
    :returns: The frame with the timestamp columns added.
    """
    if INPUT not in frame.columns:
        return frame

    rules = loader.timestamp_formats()
    zone = rules["zone"]["name"]
    formats, null_tokens = rules["formats"], rules["null_tokens"]
    raw = text(INPUT)

    label = _format_label(raw, formats, null_tokens)
    slash_rule = next(
        (r for r in formats if r["strptime"] == "AMBIGUOUS_SLASH"), None
    )
    epoch_rule = next(
        (r for r in formats if r["strptime"] == "EPOCH_SECONDS"), None
    )

    wall, route = f"{_SCRATCH}wall", f"{_SCRATCH}route"
    month_first, day_first = f"{_SCRATCH}month", f"{_SCRATCH}day"

    # Everything a single expression can settle, in one pass: the concrete
    # formats, the epoch column, and the ``/`` rows whose fields rule out one
    # of the two readings.
    branches = []
    for rule in _direct_formats(formats):
        branches.append((raw.rlike(rule["regex"]), _parse(raw, rule["strptime"])))
    if epoch_rule is not None:
        branches.append((raw.rlike(epoch_rule["regex"]), _epoch(raw, zone)))

    frame = frame.withColumn(f"{_SCRATCH}raw", raw)
    raw = F.col(f"{_SCRATCH}raw")

    if slash_rule is None:
        frame = frame.withColumn(wall, chain(branches)).withColumn(
            route, F.lit("")
        )
        ambiguous = F.lit(False)
    else:
        candidate_month, candidate_day, first, second = _slash_candidates(raw)
        frame = frame.withColumn(month_first, candidate_month).withColumn(
            day_first, candidate_day
        )
        is_slash = raw.rlike(slash_rule["regex"])
        # Mutually exclusive by construction: forcing month-first needs the
        # second field above 12 and so the first at or below it, and forcing
        # day-first needs the opposite of both.
        forced_day = is_slash & (first > 12) & F.col(day_first).isNotNull()
        forced_month = is_slash & (second > 12) & F.col(month_first).isNotNull()

        frame = frame.withColumn(
            wall,
            chain(
                branches
                + [
                    (forced_month, F.col(month_first)),
                    (forced_day, F.col(day_first)),
                ]
            ),
        ).withColumn(
            route,
            chain(
                [
                    (forced_month, F.lit(FORCED_MONTH_FIRST)),
                    (forced_day, F.lit(FORCED_DAY_FIRST)),
                ],
                otherwise=F.lit(""),
            ),
        )

        # The three weaker passes, in decreasing order of confidence. Each one
        # reads the rows its predecessor placed, which is why they are three
        # statements and not one expression.
        candidates = (month_first, day_first)
        for column, context in MACRO_ORACLES:
            frame = _macro_pass(frame, column, context, wall, route, candidates)
        frame = _bracket_pass(frame, wall, route, candidates)

        # Month-first is the majority reading and every row the rate could
        # judge agreed with it. Recorded as a guess regardless, which is the
        # part that matters -- and materialised first, for the reason the two
        # passes above give.
        still_open = f"{_SCRATCH}open"
        frame = frame.withColumn(
            still_open,
            is_slash
            & F.col(wall).isNull()
            & F.col(route).eqNullSafe(F.lit("")),
        )
        frame = frame.withColumn(
            wall,
            F.when(F.col(still_open), F.col(month_first)).otherwise(
                F.col(wall)
            ),
        ).withColumn(
            route,
            F.when(F.col(still_open), F.lit(MAJORITY)).otherwise(F.col(route)),
        )
        # Unresolved is not ambiguous: most of these are dates whose two
        # readings are the same day. Null-tolerant because ``NaT != NaT`` is
        # True in pandas and null in Spark, and both mean "not the same
        # reading" here.
        ambiguous = F.coalesce(
            F.col(route).eqNullSafe(F.lit(MAJORITY))
            & F.coalesce(
                F.col(month_first) != F.col(day_first), F.lit(True)
            ),
            F.lit(False),
        )

    precision = chain(
        [
            (
                raw.rlike(epoch_rule["regex"])
                if epoch_rule is not None
                else F.lit(False),
                F.when(
                    (F.hour(wall) == 0)
                    & (F.minute(wall) == 0)
                    & (F.second(wall) == 0),
                    F.lit("DAY"),
                ).otherwise(F.lit(epoch_rule["precision"] if epoch_rule else "")),
            )
        ]
        + [
            (raw.rlike(rule["regex"]), F.lit(rule["precision"]))
            for rule in formats
            if rule["strptime"] != "EPOCH_SECONDS"
        ]
    )

    matched = one_of(raw, null_tokens) | F.coalesce(
        chain(
            [(raw.rlike(rule["regex"]), F.lit(True)) for rule in formats],
            otherwise=F.lit(False),
        ),
        F.lit(False),
    )
    unreadable = (
        F.col(wall).isNull() & ~one_of(raw, null_tokens) & matched
    )

    frame = (
        frame.withColumn("TXN_TS", F.col(wall))
        .withColumn("TXN_TS_UTC_OFFSET", offset_label(F.col(wall)))
        # ``pd.Categorical(..., categories=PRECISIONS)`` turns the empty
        # string the unmatched rows carry into a null, because "" is not one
        # of the categories. Spelled out here: an empty string and a null are
        # the distinction this pipeline exists to preserve, so the port must
        # not quietly publish one where pandas publishes the other.
        .withColumn(
            "TXN_TS_PRECISION",
            F.when(precision.eqNullSafe(F.lit("")), F.lit(None).cast("string"))
            .otherwise(precision),
        )
        .withColumn(
            "TXN_TS_SOURCE",
            F.when(
                raw.rlike(epoch_rule["regex"]) if epoch_rule is not None
                else F.lit(False),
                F.lit("OFFSET_APPLIED"),
            ).otherwise(F.lit("AS_WRITTEN")),
        )
        .withColumn("TXN_TS_AMBIGUOUS", ambiguous)
        .withColumn(
            TS_FORMAT,
            F.when(unreadable, F.lit(UNPARSEABLE)).otherwise(label),
        )
        .withColumn(
            TS_SLASH,
            F.col(route) if slash_rule is not None else F.lit(""),
        )
        # Weakest claim wins, which in a ``when`` chain means it goes first:
        # a row that is both date-only and ambiguous is reported as
        # ambiguous, and one that never parsed as unknown.
        .withColumn(
            "TXN_TS_STATUS",
            chain(
                [
                    (F.col("TXN_TS").isNull(), F.lit("UNKNOWN")),
                    (F.col("TXN_TS_AMBIGUOUS"), F.lit("DATE_AMBIGUOUS")),
                    (
                        F.col("TXN_TS_PRECISION").eqNullSafe(F.lit("DAY")),
                        F.lit("TIME_UNKNOWN"),
                    ),
                ],
                otherwise=F.lit("OBSERVED"),
            ),
        )
    )

    if "SETTLE_DATE" in frame.columns:
        frame = _settle_dates(frame, rules["settle_formats"], null_tokens)

    return frame.drop(
        *[c for c in frame.columns if c.startswith(_SCRATCH)]
    )
