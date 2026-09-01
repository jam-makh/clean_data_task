"""
Writing the feature table to Postgres, idempotently.

The shape is: stage, merge.

    stage   Spark bulk-loads the projected frame into an unlogged mirror
            table over JDBC
    merge   one INSERT ... ON CONFLICT moves that mirror into the live table

The middle step exists because Spark's JDBC writer has four modes -- append,
overwrite, ignore, error -- and none of them is an upsert. Writing straight to
the live table means choosing between "a second run fails on the primary key"
and "a second run drops everything the first one wrote", and a feature build
needs neither: rebuilding a month that has not changed must be a no-op.

The same arrangement as the Stage 2 sink in ``src/db/writer.py``, and for the
same reason. It is repeated rather than shared because the two tables have
different keys and different columns, and the one thing worth sharing -- the
reasoning -- is written down in both places.
"""

from src.config_readers.errors import ConfigError
from src.db.settings import Database, connect

from features import contract
from features.settings import FeatureSettings

# Every statement here goes through the project's one connection helper, which
# owns the "psycopg2 is missing" message. Note what its context manager does:
# it commits, and it does NOT close.
_connect = connect


def migrate(
    database: Database, rules, config: FeatureSettings
) -> None:
    """
    Creates the live table and its staging mirror if they are absent.

    Both statements are idempotent, and both are generated from
    ``contract.py`` rather than written by hand, so the DDL cannot drift from
    the frame that fills it.

    :param database: Where the tables live.
    :param rules: The vocabularies, which fix the spending columns.
    :param config: The build settings, which name the table.
    """
    live = config.database.table
    staging = f"staging_{live}"

    with _connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(contract.create_table(rules.categories, live))

    verify_shape(database, rules, config)

    with _connect(database) as connection:
        with connection.cursor() as cursor:
            # Dropped and rebuilt rather than created if absent. The staging
            # table is declared LIKE the live one, so a staging table left
            # over from an older column list would silently be the wrong
            # shape -- and unlike the live table it holds nothing worth
            # keeping, since every run truncates it anyway.
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")
            cursor.execute(contract.create_staging(live, staging))


def live_columns(database: Database, table: str) -> list[tuple[str, str]]:
    """
    :param database: Where to look.
    :param table: The table to describe.
    :returns: ``(name, data_type)`` pairs in ordinal order, empty if the table
        does not exist. The type comes back because a column can drift in type
        as well as in existence -- see ``verify_shape``.
    """
    with _connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                " WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]


def verify_shape(
    database: Database, rules, config: FeatureSettings
) -> None:
    """
    Checks the live table's columns against the contract before writing.

    ``CREATE TABLE IF NOT EXISTS`` does nothing to a table that already
    exists, so a column removed from ``contract.py`` does not disappear from a
    database built by an earlier version -- it stays, holding whatever the
    last build put there, and the new build's INSERT simply does not mention
    it. Nothing errors. Stage 4 reads a table that still has the column and
    has no way to know its values are stale.

    This is the destination-side twin of ``contract.verify``. That one refuses
    a frame carrying an undeclared column; this one refuses a table carrying
    one, for the same reason -- a column nobody declared has no stated
    meaning, and a stale one is worse than a missing one because it looks
    like data.

    Types are compared as well as names, for the same reason. A table built
    before ``user_id`` became UUID still has every declared column and would
    pass a name-only check, and the failure would then surface as a type error
    inside the merge -- a message about a statement, far from the cause. Here
    it is a message about the table, with the fix in it.

    :param database: Where the table lives.
    :param rules: The vocabularies, which fix the spending columns.
    :param config: The build settings, which name the table.
    :raises ConfigError: If the table's columns are not exactly the declared
        ones, naming the drift and how to clear it.
    """
    table = config.database.table
    live = live_columns(database, table)
    if not live:
        return

    present = [name for name, _ in live]
    types = dict(live)

    declared = contract.names(rules.categories)
    declared_types = contract.postgres_types(rules.categories)

    stale = [name for name in present if name not in declared]
    missing = [name for name in declared if name not in present]
    wrong = [
        f"{name} is {types[name]}, declared {declared_types[name]}"
        for name in declared
        if name in types and types[name] != declared_types[name]
    ]

    if not stale and not missing and not wrong:
        return

    problems = []
    if wrong:
        problems.append(
            f"{len(wrong)} column(s) of the wrong type: {'; '.join(wrong)}"
        )
    if stale:
        problems.append(
            f"{len(stale)} column(s) the contract no longer declares: "
            f"{', '.join(stale)}"
        )
    if missing:
        problems.append(
            f"{len(missing)} declared column(s) absent: "
            f"{', '.join(missing)}"
        )

    raise ConfigError(
        f"{table} does not match features/contract.py -- "
        + "; ".join(problems)
        + ". CREATE TABLE IF NOT EXISTS cannot alter a table it finds, so a "
        "schema change needs the table rebuilt: run `make features-reset`, "
        "which drops it and builds again."
    )


def stage(table, database: Database, config: FeatureSettings) -> None:
    """
    Bulk-loads the projected frame into the staging table.

    Truncated first, and through psycopg2 rather than Spark's overwrite mode.
    ``mode("overwrite")`` on a JDBC target DROPS the table and recreates it
    from the frame's own schema, which would replace a table whose types are
    the whole point with one Spark inferred.

    The truncate happens before the load rather than after, on the principle
    that cleanup which only runs on the success path does not run: a build
    that dies mid-write leaves rows here, and the next build's truncate is
    what removes them.

    :param table: The projected feature table.
    :param database: Where to write.
    :param config: The build settings.
    """
    staging = f"staging_{config.database.table}"

    with _connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE {staging}")

    table.write.mode("append").option(
        "batchsize", str(config.database.batch_size)
    ).jdbc(
        url=database.jdbc_url,
        table=staging,
        properties=database.jdbc_properties,
    )


def merge(database: Database, rules, config: FeatureSettings) -> int:
    """
    Runs the upsert that moves the staged build into the live table.

    One statement, one transaction: the build lands completely or not at all.
    There is no partial state for a reader to reason about, which is what
    makes a failed run safe to simply re-run.

    :param database: Where both tables live.
    :param rules: The vocabularies, which fix the spending columns.
    :param config: The build settings.
    :returns: Rows inserted or updated.
    """
    live = config.database.table
    statement = contract.merge_statement(
        rules.categories, live, f"staging_{live}"
    )

    with _connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return cursor.rowcount


def write(
    table, database: Database, rules, config: FeatureSettings
) -> int:
    """
    The whole sink: ensure the tables exist, stage the build, merge it.

    :param table: The projected feature table.
    :param database: Where to write.
    :param rules: The vocabularies.
    :param config: The build settings.
    :returns: Rows inserted or updated in the live table.
    """
    migrate(database, rules, config)
    stage(table, database, config)
    return merge(database, rules, config)
