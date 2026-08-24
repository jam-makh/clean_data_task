"""
The reader decides nothing, and these are the ways that could quietly stop
being true.

Every assertion here is about a failure that does not announce itself. A
column inferred as a double has already discarded the rows that would not
parse; a header read with a BOM still attached names a column that looks
identical to the one every stage asks for; a schema applied positionally
hands each stage the column to the left of the one it wanted. None of those
raise. All of them produce a full run and a wrong answer.
"""

import pytest

from src.spark import spark_setup as source


def _write(tmp_path, name, text):
    """:returns: A path holding ``text``, written as UTF-8 without a BOM."""
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_separator_follows_the_extension(tmp_path):
    """A .tsv is tab-separated and a .csv is not, without being told."""
    assert source.separator_for("a.csv") == ","
    assert source.separator_for("b.tsv") == "\t"


def test_workbook_is_refused_by_name():
    """
    Spark cannot read an .xlsx, and the message says which reader can.

    Refusing beats guessing for the reason ``src/utils/io.py`` gives: an
    unknown format read as CSV usually "works" and produces one column of
    garbage.
    """
    with pytest.raises(ValueError, match="delimited sources only"):
        source.separator_for("book.xlsx")


def test_header_survives_a_byte_order_mark(tmp_path):
    """
    The first column's name is the name, not the name with U+FEFF glued on.

    This is the highest-value assertion in the file. A CSV exported from Excel
    starts with a BOM; read as plain UTF-8 the first column is
    ``"\\ufeffUSER_ID"``, which prints as ``USER_ID``, compares unequal to
    ``USER_ID``, and makes every lookup on it miss while the column is
    visibly present in every diagnostic.
    """
    path = tmp_path / "bom.csv"
    path.write_text("USER_ID,TXN_ID\n1,2\n", encoding="utf-8-sig")

    assert source.header_of(path) == ["USER_ID", "TXN_ID"]


def test_header_is_parsed_not_split(tmp_path):
    """A quoted delimiter in a header name does not become a column break."""
    path = _write(tmp_path, "quoted.csv", '"LAST, FIRST",AMOUNT\nx,1\n')

    assert source.header_of(path) == ["LAST, FIRST", "AMOUNT"]


def test_duplicate_column_names_are_refused(tmp_path):
    """
    A file naming a column twice cannot be compared against itself.

    pandas renames the second to ``TXN_ID.1``; Spark keeps both and resolves
    references to whichever it reaches first. Two readers with two different
    answers is precisely the situation the parity harness cannot report on,
    so it is refused at the door instead.
    """
    path = _write(tmp_path, "dupes.csv", "TXN_ID,AMOUNT,TXN_ID\n1,2,3\n")

    with pytest.raises(ValueError, match="more than once"):
        source.header_of(path)


def test_empty_file_says_so(tmp_path):
    """Not an IndexError from inside a csv reader."""
    path = _write(tmp_path, "empty.csv", "")

    with pytest.raises(ValueError, match="no header row"):
        source.header_of(path)


@pytest.mark.spark
def test_every_column_arrives_as_a_string(spark, sample_path):
    """
    No inference, on any column, however numeric it looks.

    TXN_SEQ is a bare integer on every row and FX_RATE is a bare float; both
    would be inferred confidently and wrongly, in the sense that the inference
    would do the coercion this pipeline is required to do explicitly and
    count.
    """
    frame = source.read_csv(spark, sample_path)

    kinds = {field.dataType.simpleString() for field in frame.schema.fields}
    assert kinds == {"string"}


@pytest.mark.spark
def test_blank_reads_as_null_not_as_empty_string(spark, sample_path):
    """
    The distinction the whole pipeline is built to preserve.

    RUNNING_BALANCE is blank on roughly a third of this source. If those
    arrived as ``""`` rather than null, the balance stage would be reasoning
    about a value the file does not contain, and ``UNKNOWN`` would stop
    meaning "no balance stated".
    """
    from pyspark.sql import functions as F

    frame = source.read_csv(spark, sample_path)
    counts = frame.select(
        F.count(F.when(F.col("RUNNING_BALANCE").isNull(), 1)).alias("null"),
        F.count(F.when(F.col("RUNNING_BALANCE") == "", 1)).alias("blank"),
    ).collect()[0]

    assert counts["null"] > 0
    assert counts["blank"] == 0


@pytest.mark.spark
def test_a_renamed_column_is_refused_rather_than_shifted(spark, tmp_path):
    """
    ``enforceSchema=false`` earns its keep here.

    Under the default, Spark discards the header and applies the schema by
    position -- so a file whose columns were reordered or renamed loads
    cleanly and every stage reads the wrong column. The schema is derived from
    the header, so this can only fire when the file and the schema disagree,
    which is exactly when it should.
    """
    path = _write(tmp_path, "shifted.csv", "TXN_SEQ,AMOUNT\n1,2\n")
    schema = source.string_schema(path)

    other = _write(tmp_path, "renamed.csv", "TXN_SEQ,TOTAL\n1,2\n")
    reader = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("enforceSchema", "false")
        .load(str(other))
    )

    with pytest.raises(Exception):
        reader.collect()


@pytest.mark.spark
def test_a_ragged_row_fails_rather_than_pads(spark, tmp_path):
    """
    FAILFAST matches what pandas does with the same file: raise.

    Under PERMISSIVE a short row is null-padded and a long one truncated, and
    the run completes reporting nothing -- which would put the two pipelines
    in silent disagreement about a file neither of them should accept.
    """
    path = _write(tmp_path, "ragged.csv", "A,B,C\n1,2,3\n4,5\n")

    with pytest.raises(Exception):
        source.read_csv(spark, path).collect()
