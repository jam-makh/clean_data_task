"""
Writing a cleaned Spark frame to Postgres, idempotently.

The shape is: project, stage, merge.

    project   the frame's 24 columns, cast to what the table holds, plus the
              load's sync_job_id -- see src/db/contract.py
    stage     Spark bulk-loads that into an unlogged mirror table over JDBC
    merge     one INSERT ... ON CONFLICT moves the mirror into the live table

The middle step exists because Spark's JDBC writer has four modes -- append,
overwrite, ignore, error -- and none of them is an upsert. Writing straight to
the live table means choosing between "a second run fails on the primary key"
and "a second run drops everything the first one wrote", and the pipeline needs
neither: re-running a load must be a no-op, because a Kafka consumer will
re-deliver and a backfill will overlap.

The cost of the staging table is one extra write of the batch. What it buys is
that the merge is a single statement in a single transaction, so a run either
lands whole or not at all -- there is no half-written state anyone has to
reason about, and a failed run is safe to simply run again.
"""

from src.db import contract, migrate
from src.db.settings import Database

def _batch_size(given: int | None) -> str:
    """
    :param given: An explicit override, or None to read the configured value.
    :returns: Rows per JDBC round trip, as the string the option takes.
    """
    if given is not None:
        return str(given)
    from src.config import runtime

    return str(runtime.load().database.batch_size)


def stage(
    frame, database: Database, sync_job_id: str, batch_size: int | None = None
) -> None:
    """
    Loads the projected frame into the staging table, replacing its contents.

    :param frame: The cleaned frame, as ``src.spark.pipeline.run`` returned it.
    :param database: Where to write.
    :param sync_job_id: The load these rows belong to.
    :param batch_size: Rows per round trip; ``database.batch_size`` from
        config when absent.
    """
    projected = contract.project(frame, sync_job_id)

    # Truncate first, and through psycopg2 rather than Spark's overwrite mode.
    # `mode("overwrite")` on a JDBC target DROPS the table and recreates it
    # from the frame's own schema -- which would replace a table whose types
    # and constraints are the whole point with one Spark inferred, and would
    # silently undo every CHECK in sql/schema.sql on the first run.
    migrate.truncate_staging(database)

    projected.write.mode("append").option(
        "batchsize", _batch_size(batch_size)
    ).jdbc(
        url=database.jdbc_url,
        table=migrate.STAGING,
        properties=database.jdbc_properties,
    )


def write(
    frame, database: Database, sync_job_id: str, batch_size: int | None = None
) -> int:
    """
    The whole sink: ensure the tables exist, stage the batch, merge it.

    Idempotent on ``sync_job_id`` and the row key together. Running the same
    load twice writes the same rows with the same values and the same job id,
    so the second run changes nothing a reader can observe -- which is the
    property a re-delivering consumer needs, and the reason the id is derived
    from the source rather than generated per run.

    :param frame: The cleaned frame.
    :param database: Where to write.
    :param sync_job_id: The load these rows belong to.
    :param batch_size: Rows per round trip; configured value when absent.
    :returns: Rows inserted or updated in the live table.
    """
    migrate.migrate(database)
    stage(frame, database, sync_job_id, batch_size)
    return migrate.merge(database)
