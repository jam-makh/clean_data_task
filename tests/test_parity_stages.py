"""
One parity test per ported stage, on the columns that stage produces.

``tests/test_parity.py`` asks the cumulative question -- does the whole
registered prefix of the profile agree -- and it is the one that has to pass
before anything ships. This file asks the narrower one that makes a failure
cheap to act on: *which* stage disagrees, and on which of its columns.

Both are needed and neither replaces the other. The cumulative test would
catch a stage that agrees in isolation and then feeds the next one something
subtly different, but when it fails it reports every column downstream of the
break. These report one stage's columns, so the first red test names the
stage, and the columns it names are the ones that stage wrote.

Each test runs the *same* step list through both engines -- the minimum chain
that stage depends on, not the whole profile -- so what is compared is one
stage's work over an identical input. The chains are written out rather than
sliced from the profile because a stage's real dependencies are narrower than
its position: ``codes`` reads nothing any earlier stage produces, and running
four stages before it to test it would attribute their failures to it.
"""

import pandas as pd
import pytest

from src.pipeline import TransactionCleaner, steps_for
from src.spark import pipeline as spark_pipeline
from tests.harness.parity import assert_parity
from src.spark.spark_setup import read_csv


@pytest.fixture(scope="session")
def stage_parity(spark, sample_path, sample_frame):
    """
    :returns: A callable ``(steps, columns=None) -> ParityResult`` that runs
        the chain in both engines and asserts on the columns the chain added.

    The pandas side is cached on the step list. Several of these chains share
    a prefix and the pandas pipeline over 11k rows is a couple of seconds
    each; without the cache the file spends most of its runtime recomputing
    ``timestamps`` for stages that only needed it as an input.
    """
    cache: dict[tuple[str, ...], object] = {}

    def run(steps, columns=None):
        key = tuple(steps)
        if key not in cache:
            cache[key] = TransactionCleaner(steps=steps_for(list(steps))).run(
                sample_frame
            )
        expected = cache[key]
        actual = spark_pipeline.run(read_csv(spark, sample_path), list(steps))

        # Everything the chain added, when the caller does not narrow it
        # further. Taken from the pandas frame so that a column the port
        # simply did not write is reported as missing rather than skipped --
        # which is exactly what an unfinished stage looks like.
        produced = [
            column
            for column in expected.columns
            if column not in sample_frame.columns
        ]
        return assert_parity(expected, actual, columns=columns or produced)

    return run


@pytest.fixture(scope="session")
def written_parity(spark):
    """
    :returns: A callable ``(rows, steps, columns=None) -> ParityResult`` that
        runs a frame written out here through both engines.

    The sample is real data and that is its whole value; it is also only the
    cases the source happens to contain. Where a stage has a branch the file
    never takes -- a currency with no minor unit, a transaction ID shared by
    two rows -- passing on the sample says nothing about it, and a test that
    says nothing while looking like it says something is the failure mode the
    harness exists to prevent.

    Rows go in as strings and only as strings, which is what the readers
    produce on both sides, so a case written here reaches each stage in the
    same shape a real row would. ``TXN_SEQ`` is required because it is what
    the harness aligns on.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    def run(rows: dict, steps, columns=None):
        assert "TXN_SEQ" in rows, "TXN_SEQ is what rows are aligned on"
        names = list(rows)
        height = len(rows[names[0]])
        expected = TransactionCleaner(steps=steps_for(list(steps))).run(
            pd.DataFrame(rows, dtype=object)
        )
        schema = StructType(
            [StructField(name, StringType(), True) for name in names]
        )
        written = spark.createDataFrame(
            [tuple(rows[name][index] for name in names) for index in range(height)],
            schema,
        )
        actual = spark_pipeline.run(written, list(steps))
        produced = [c for c in expected.columns if c not in names]
        return assert_parity(expected, actual, columns=columns or produced)

    return run


@pytest.mark.spark
def test_timestamps(stage_parity):
    """
    Every reading, and the provenance of every reading.

    The ten columns here are the whole of ``TimestampNormalizer``'s output,
    and three of them are the ones worth the trouble. ``TXN_TS_FORMAT``
    distinguishes a value no rule described from one a rule matched and could
    not read, which is the difference between a gap in the rule file and dirt
    in the source. ``TXN_TS_SLASH_RESOLUTION`` says which of the three passes
    settled a ``NN/NN`` date, so a port that reached the right timestamp by
    the wrong route -- the majority guess landing where the macro oracle
    should have decided -- fails here rather than passing quietly. And
    ``TXN_TS_UTC_OFFSET`` is computed from the zone rather than from a Python
    call, which is the claim most likely to be wrong at a DST boundary.
    """
    result = stage_parity(["timestamps"])

    assert len(result.compared) == 10


@pytest.mark.spark
def test_codes(stage_parity):
    """
    The padding and the two lookups.

    Run on its own rather than after ``timestamps``, because it reads nothing
    any earlier stage writes -- a chain here would only widen the blast radius
    of a failure.

    The column doing the real work in this comparison is
    ``PROCESSING_TYPE_CLEANED``, where an unclassified code has to arrive as
    null and not as the empty string. The two spellings look identical in
    every report and differ in the one place it matters, and the harness is
    built to refuse to conflate them.
    """
    result = stage_parity(["codes"])

    assert "PROCESSING_TYPE_CLEANED" in result.compared


@pytest.mark.spark
def test_amounts(stage_parity):
    """
    Every amount, how it was read, and where its sign came from.

    After ``codes`` because the sign comes from the direction that stage
    resolved -- running ``amounts`` alone would leave every row NOT_SIGNED in
    both engines and agree about nothing.

    The sample does reach the interesting branches: 44 of its amounts need
    reformatting, across accounting parentheses, thousands separators and
    European decimals, and 2 have their sign restored. The branch it does NOT
    reach is the zero-decimal tiebreak, which is why ``test_amount_conventions``
    below exists.
    """
    result = stage_parity(["codes", "amounts"], columns=[
        "TXN_AMOUNT_CLEANED", "TXN_AMOUNT_COERCION",
        "TXN_AMOUNT_SIGN", "PROCESSING_CODE_DIRECTION", "VALIDATION_FLAGS",
    ])

    assert result.ok


@pytest.mark.spark
def test_amount_conventions(written_parity):
    """
    The parser's branches, including the ones the sample never takes.

    Each row is a decision the two engines could make differently:

    * ``5.727.580,00`` in LBP -- both separators, so the LAST one is the
      decimal point regardless of what the currency is;
    * ``1,193.50`` and ``1.193,50`` -- the same number written two ways;
    * ``5,727`` in LBP against the same string in USD -- the tiebreak. One
      separator with exactly three digits behind it is a thousands group in a
      currency with no minor unit and a decimal point in one that has;
    * ``(808.41)`` -- accounting negative, on a CREDIT code, which is the
      shape that makes ``AmountNormalizer`` restore a sign;
    * ``-`` and ``()`` and ``£$`` -- cells that survive the character filter
      as nothing at all, which must be null and not zero;
    * ``12.34.56`` -- three groups and one separator, so thousands, and then
      a number;
    * a blank and a null, which are ABSENT rather than UNPARSEABLE, and are
      not the same as each other anywhere else in this pipeline.
    """
    amounts = [
        "5.727.580,00", "1,193.50", "1.193,50", "5,727", "5,727",
        "(808.41)", "-", "()", "£$", "12.34.56", "", None, "-104.39",
    ]
    result = written_parity(
        {
            "TXN_SEQ": [str(i) for i in range(1, len(amounts) + 1)],
            "TXN_AMOUNT": amounts,
            "TXN_CCY": [
                "LBP", "USD", "EUR", "LBP", "USD",
                "USD", "USD", "USD", "USD", "USD", "USD", "USD", "USD",
            ],
            # 26 is Settlement Credit and 00 is Purchase: one CREDIT and one
            # DEBIT, so both arms of the sign restoration are exercised.
            "PROCESSING_CODE": ["26", "00"] * 6 + ["00"],
        },
        ["codes", "amounts"],
        columns=["TXN_AMOUNT_CLEANED", "TXN_AMOUNT_COERCION", "TXN_AMOUNT_SIGN"],
    )

    assert result.ok


@pytest.mark.spark
def test_missing(stage_parity):
    """
    The sentinels, and the one column here that is not a property of its row.

    After ``timestamps`` because the settlement status is a statement about
    ``SETTLE_DATE_CLEANED``, which does not exist until that stage has parsed
    it.

    ``AUTH_CODE_REPEATED`` is the column under real test. pandas tallies the
    codes in a ``Counter`` on one machine; the port counts them in a window
    partitioned by the code itself. Those are the same number only if the
    blank codes are excluded from both, and a port that forgot to would mark
    every blank row as carrying a planted value -- which on this sample is a
    difference of hundreds of rows, not an edge case.
    """
    result = stage_parity(
        ["timestamps", "missing"],
        columns=["AUTH_CODE_REPEATED", "AUTH_CODE_VALID", "SETTLE_DATE_STATUS"],
    )

    assert result.ok


@pytest.mark.spark
def test_geo(stage_parity):
    """
    The city, the country, and the two readings of the city.

    ``missing`` is in the chain because ``IS_ECOMMERCE`` reads
    ``HAS_TERMINAL`` where the profile produced one. This source has no
    terminal column, so that arm is inert here -- included anyway, because a
    chain that quietly drops a stage's input is how a port passes a test the
    real run would fail.

    ``MERCHANT_COUNTRY_EXPECTED`` is the column to watch: it must be the
    empty string where the city implies no country, and UNKNOWN nowhere. The
    cleaned country beside it is the reverse -- never blank. A port that
    treated the two the same would pass every check except this one.
    """
    result = stage_parity(
        ["timestamps", "missing", "geo"],
        columns=[
            "MERCHANT_CITY_CLEANED", "LOCATION_TYPE", "IS_ECOMMERCE",
            "MERCHANT_COUNTRY_EXPECTED", "MERCHANT_COUNTRY_CLEANED",
        ],
    )

    assert result.ok


@pytest.mark.spark
def test_duplicates(stage_parity):
    """
    The no-op path, over real rows.

    Worth having precisely because it *is* a no-op on this source: the
    forecast extract has no byte-identical rows and no repeated TXN_ID in any
    of its 265,195, so every row must come out with one copy, an unsuffixed
    cleaned ID and a false collision flag. A port that dropped or renamed
    anything here would be inventing dirt, which is the failure mode that
    survives longest unnoticed -- the report simply claims work that never
    happened.
    """
    result = stage_parity(["duplicates"])

    assert result.ok
    assert "TXN_ID_COLLISION" in result.compared


@pytest.mark.spark
def test_exact_duplicate_rows(written_parity):
    """
    The branch the source never takes: byte-identical rows.

    Three copies of one row and one of another, so the survivor of the group
    has to carry three and the singleton one. This is the only place a dropped
    row can still be counted -- the rows themselves are gone by the end of the
    run -- so a copy count that came back as 1 would silently under-report
    every double-load the pipeline was built to find.

    The identical rows share a TXN_SEQ, which is deliberate: a true duplicate
    duplicates the key too. The harness still aligns, because it is the
    post-deduplication frames that are compared and TXN_SEQ is unique in both.
    """
    result = written_parity(
        {
            "TXN_SEQ": ["1", "1", "1", "2"],
            "TXN_ID": ["1000123", "1000123", "1000123", "1000124"],
            "TXN_AMOUNT": ["10.00", "10.00", "10.00", "20.00"],
        },
        ["duplicates"],
    )

    assert result.ok
    assert result.left_rows == 2


@pytest.mark.spark
def test_id_collisions(written_parity):
    """
    Two different rows sharing a TXN_ID: an upstream key fault, not a
    double-load.

    Neither row is dropped -- they may well be two real transactions -- and
    both are suffixed so the cleaned ID is unique on its own and every
    downstream join can key on one column. The third row's ID was already
    unique and must come back untouched: suffixing every row instead of only
    the collided ones would be invisible in a row count and wrong in every
    join.
    """
    result = written_parity(
        {
            "TXN_SEQ": ["1", "2", "3"],
            "TXN_ID": ["1000123", "1000123", "1000124"],
            "TXN_AMOUNT": ["10.00", "20.00", "30.00"],
        },
        ["duplicates"],
    )

    assert result.ok
    assert result.left_rows == 3


@pytest.mark.spark
def test_balance(stage_parity):
    """
    The reconstruction, and every state it refuses to publish.

    The chain is the minimum this stage depends on: ``codes`` resolves the
    direction, ``amounts`` signs and parses the figure that moves the balance,
    and nothing else here is read. ``timestamps`` is deliberately absent --
    the arithmetic orders by TXN_SEQ, not by date.

    The columns worth watching are the two status columns. A port that got the
    arithmetic right and the *evidence* wrong would agree on every published
    number and still be wrong about which of them are proven -- UNVERIFIED
    published as OBSERVED is a claim the data never made, and CONTRADICTED
    collapsing to UNVERIFIED hides a source that contradicts itself.
    """
    result = stage_parity(["codes", "amounts", "balance"])

    assert result.ok
    assert "RUNNING_BALANCE_STATUS" in result.compared
    assert "RUNNING_BALANCE_ADJUSTED_STATUS" in result.compared


@pytest.mark.spark
def test_merchant(stage_parity):
    """
    The cleaned name, and the five readings of it.

    This stage had no test of its own until now, and it was broken the whole
    time: the exact-match join passes a null into the fuzzy fallback to mean
    "already answered, skip this row", Arrow delivers that null to Python as a
    float NaN rather than as None, and the UDF's ``name is None`` guard let it
    through to a ``.upper()`` that could not survive it. Nothing caught it
    because the cumulative harness stopped at the first unported stage, which
    sat two places earlier in the profile.

    ``MERCHANT_NAME_CLEANED`` is what everything downstream keys on -- the MCC
    decision is per merchant name -- so a divergence here does not stay here.
    """
    result = stage_parity(
        ["missing", "merchant"],
        columns=[
            "MERCHANT_NAME_CLEANED", "MERCHANT_KIND", "MERCHANT_TYPE",
            "MERCHANT_RECOGNISED", "INTERNAL_MOVEMENT",
            "MATCHES_STATUS_CLEANED",
        ],
    )

    assert result.ok


@pytest.mark.spark
def test_mcc(stage_parity):
    """
    The per-merchant decision, and the row it reaches.

    ``merchant`` is in the chain because the decision is keyed on the cleaned
    name, and ``codes`` because it is the padded code that gets counted. The
    binomial maths is not reimplemented on the Spark side -- the driver calls
    the pandas ``_resolve`` -- so what this test actually proves is the part
    that IS reimplemented: that the counts feeding it are the same counts, that
    the decision reaches every row of its merchant, and that adoption fires on
    the same rows.

    ``MCC_CONFIDENCE`` is the column to watch. A merchant with no decision at
    all reads "NONE" in the pandas source and then becomes NULL, because the
    column is a Categorical over three tiers and "NONE" is not one of them. A
    port that published the literal string would agree everywhere except here.
    """
    result = stage_parity(
        ["codes", "missing", "merchant", "mcc"],
        columns=[
            "MCC_CODE_SUGGESTED", "MCC_CONFIDENCE", "MCC_SIGNAL",
            "MCC_CODE_CLEANED", "VALIDATION_FLAGS",
        ],
    )

    assert result.ok


@pytest.mark.spark
def test_consistency(stage_parity):
    """
    Every cross-field check, and the string they accumulate into.

    Compared on ``VALIDATION_FLAGS`` alone because that is the whole output --
    and because it is a joined string rather than a set, so this also pins the
    ORDER the checks ran in. Two engines that raised the same flags in a
    different order would produce different cells and this would catch it.

    The chain is everything the checks read. ``CODE_TYPE_MISMATCH`` is the one
    worth naming: an unclassified processing code leaves
    ``PROCESSING_TYPE_CLEANED`` null, pandas stringifies that Categorical null
    to "nan", and the comparison against the stated type therefore FAILS and
    raises the flag. A port that compared against a null instead would drop
    the flag on exactly those rows and agree everywhere else.
    """
    result = stage_parity(
        [
            "timestamps", "codes", "amounts", "missing", "merchant", "geo",
            "consistency",
        ],
        columns=["VALIDATION_FLAGS"],
    )

    assert result.ok
