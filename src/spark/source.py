"""
Reading the source into Spark, with the reader deciding nothing.

``inferSchema`` is never used here, and the reason is the same one
``src/utils/io.py`` gives for ``dtype=object`` and ``keep_default_na=False``
on the pandas side: the reader must not decide what ``""`` or ``"NA"`` or
``"5.727.580,00"`` mean. Inference would sample the file, guess a type per
column, and coerce on the way in -- which pre-empts the very coercion
requirement 2 asks the pipeline to count. A column read as double has already
turned its unparseable values into nulls somewhere nobody logged, and the row
that failed is gone before any stage could mark it.

So every column arrives as a string, exactly as the file spells it, and every
type in the output is the result of a cast some stage made deliberately.

The schema is derived from the file's own header rather than written out as a
literal list of 22 names. A hardcoded schema is a copy of the file that can
drift from it, and the failure when it drifts is silent under
``enforceSchema=true``: Spark would apply the names positionally and hand
every stage the column to the left of the one it asked for. Deriving the names
and then reading with ``enforceSchema=false`` gets the check for free -- Spark
compares the header it finds against the schema it was given and refuses the
file if they disagree.

Which columns a given file is *required* to have is a separate question, and
one the project already answers: ``config/pipeline.yaml`` profiles declare
their detection columns, and ``runtime.Runtime.detect`` takes any iterable of
names -- a Spark DataFrame's ``.columns`` included.
"""

import csv
from pathlib import Path

# The same extension-to-separator table the pandas reader uses. Repeated here
# rather than imported because that module reaches for pandas at import time
# and this one must not: an executor importing this file has no reason to pay
# for pandas, and on a cluster it may not have it.
DELIMITED_SUFFIXES = {".csv": ",", ".tsv": "\t", ".txt": ","}

# utf-8-sig, not utf-8. A CSV exported from Excel begins with a byte order
# mark, and read as plain UTF-8 the first column's name comes back with an
# invisible U+FEFF glued to the front -- so `USER_ID` is not `USER_ID`, every
# lookup on it misses, and the error names a column that is visibly present.
HEADER_ENCODING = "utf-8-sig"


def separator_for(path: str | Path) -> str:
    """
    :param path: Source file.
    :returns: The delimiter its extension implies.
    :raises ValueError: If the extension is not one this reader handles.
        Rejected by name rather than guessed at, for the reason
        ``src/utils/io.py`` gives: reading an unknown format usually "works"
        and produces one column of garbage, which is worse than refusing.
    """
    suffix = Path(path).suffix.lower()
    if suffix not in DELIMITED_SUFFIXES:
        raise ValueError(
            f"Spark reads delimited sources only, got {suffix!r}: {path}. "
            f"Supported: {sorted(DELIMITED_SUFFIXES)}. A workbook has to go "
            f"through src.utils.io.read_source, which is why the profile that "
            f"reads one is not a Spark profile."
        )
    return DELIMITED_SUFFIXES[suffix]


def header_of(path: str | Path, sep: str | None = None) -> list[str]:
    """
    Reads just the first line, with a real CSV parser rather than ``split``.

    A quoted header field containing the delimiter is rare and completely
    silent when it goes wrong: splitting on commas turns one column into two,
    the schema then has one name too many, and every column after it shifts.

    :param path: Source file.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: Column names in file order.
    :raises ValueError: If the file is empty, or names a column twice --
        duplicate names are accepted by both readers and then resolve to
        whichever copy the engine reaches first, which differs between them.
    """
    path = Path(path)
    sep = sep if sep is not None else separator_for(path)
    with path.open("r", encoding=HEADER_ENCODING, newline="") as handle:
        try:
            names = next(csv.reader(handle, delimiter=sep))
        except StopIteration:
            raise ValueError(f"{path} is empty: no header row to read") from None

    seen = {name for name in names if names.count(name) > 1}
    if seen:
        raise ValueError(
            f"{path} names {sorted(seen)} more than once. Spark and pandas "
            f"disambiguate duplicate columns differently, so a frame read "
            f"from this file cannot be compared against itself."
        )
    return names


def string_schema(path: str | Path, sep: str | None = None):
    """
    :param path: Source file.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: A ``StructType`` of nullable strings, one field per header
        column, in file order.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField(name, StringType(), nullable=True)
            for name in header_of(path, sep)
        ]
    )


def read_csv(spark, path: str | Path, sep: str | None = None):
    """
    Reads a delimited source as all strings, matching the pandas reader.

    Every option below is stated, including the ones whose stated value is
    today's default, because a parity harness comparing two readers is only
    meaningful if both readers are pinned. The two that are NOT defaults:

    ``enforceSchema=false`` makes Spark check the header against the schema
    instead of applying the schema positionally and discarding the header.
    Since the schema was derived from that header this can only fail when the
    file changed underneath, which is exactly when a loud failure is worth
    having.

    ``mode=FAILFAST`` matches what pandas does with a ragged row: raise. Under
    the default PERMISSIVE mode a row with too few fields is null-padded and a
    row with too many is truncated, and the run completes reporting nothing.

    One known and deliberate difference from the pandas reader remains: a
    *quoted* empty field. pandas' ``na_values=[""]`` makes it null; Spark's
    ``emptyValue`` default keeps it an empty string, and the two categories
    are ones this pipeline works hard to keep apart, so neither reader is
    bent to agree with the other. The parity harness reports it if the file
    ever contains one.

    :param spark: An active session, from ``src.spark.session.session``.
    :param path: Source ``.csv``/``.tsv``/``.txt``.
    :param sep: Delimiter; derived from the extension when absent.
    :returns: A DataFrame whose every column is a nullable string.
    """
    path = Path(path)
    sep = sep if sep is not None else separator_for(path)
    return (
        spark.read.format("csv")
        .schema(string_schema(path, sep))
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .option("sep", sep)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        # Not a backslash. pandas doubles a quote to escape it inside a quoted
        # field; Spark's default escape character is `\`, which would read
        # `""` as two fields' worth of confusion. Setting it to `"` is what
        # makes the two readers agree on an embedded quote.
        .option("escape", '"')
        .option("multiLine", "false")
        .option("nullValue", "")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .load(str(path))
    )
