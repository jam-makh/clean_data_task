"""
The cleaning consumer: ids in, cleaned rows out.

    poll transactions.raw.ingested.v1
      -> ingest_events.decode        refuse anything that is not one of ours
      -> gather up to batch_size     one Spark job for a burst, not one each
      -> runner.run_rows             read, clean, report, upsert
      -> raw.mark CLEANED / FAILED   record what happened to each row
      -> commit                      and only now

The order of the last two lines is the whole delivery guarantee. Offsets are
committed **after** Postgres has committed, never before, so the failure mode
is a redelivered row rather than a skipped one -- and a redelivered row is a
no-op, because ``db/writer.py`` upserts and ``run_rows`` derives the job id
from the ids. At-least-once by construction, idempotent by design.

Auto-commit would invert that. The client commits on a timer whether or not
the message has been dealt with, so a consumer that dies mid-clean has already
told Kafka it was finished and the row is cleaned by nobody. That is why
``Subscription.consumer_config`` turns it off, and why this module commits by
hand.

What happens when a row will not clean
--------------------------------------

It is marked FAILED with the reason, the offset is committed, and the consumer
carries on. Not a retry loop: the same row through the same code produces the
same failure, and a consumer that retries it forever stops being a consumer.
Not a halt, either -- one unparseable row must not stop the other thousand.

The row is not lost. It is in ``raw_transactions`` with ``status = 'FAILED'``
and ``last_error`` saying why, and re-emitting its id is how you retry it once
the cause is fixed -- which ``scripts/dummy_producer.py --pending`` does in
bulk.

The batch and the fallback
--------------------------

A Spark job over one row and a job over fifty cost nearly the same, so a burst
of ids is gathered and cleaned together. The cost of that is attribution: if
the batch fails, which row broke it? So a failed batch of more than one is
retried one row at a time, which is slow and only happens on the path that was
already going wrong. Then the failure lands on the row that caused it, and the
other forty-nine are cleaned instead of being marked FAILED alongside it.
"""

import time
from dataclasses import dataclass, field

from src.db import raw
from src.kafka import ingest_events
from src.kafka.settings import Subscription


@dataclass
class Outcome:
    """
    What one batch did, from the consumer's point of view rather than the
    pipeline's.

    :param cleaned: Ids written to ``cleaned_transactions``.
    :param failed: Id to the error that stopped it.
    :param skipped: Messages that were not events this consumer understands,
        as their decode errors. Counted rather than listed by id, because a
        message that would not decode has no id to list.
    :param seconds: Wall clock for the batch, including the Spark job.
    """

    cleaned: list = field(default_factory=list)
    failed: dict = field(default_factory=dict)
    skipped: list = field(default_factory=list)
    seconds: float = 0.0

    @property
    def total(self) -> int:
        """:returns: Messages accounted for."""
        return len(self.cleaned) + len(self.failed) + len(self.skipped)


def decode_all(messages) -> tuple[list, list]:
    """
    Turns raw messages into ids, refusing what is not one of ours.

    :param messages: Kafka messages, as polled.
    :returns: (ids in arrival order without repeats, decode errors).

    A repeated id inside one batch is dropped rather than cleaned twice. The
    upsert would make the second copy harmless, but it would also make the
    log say "2 rows" about one transaction, and a log that miscounts is worse
    than one that says less.
    """
    ids: list = []
    errors: list = []
    for message in messages:
        try:
            payload = ingest_events.decode(message.value())
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if payload["id"] not in ids:
            ids.append(payload["id"])
    return ids, errors


def clean(ids, *, spark, database, broker=None, write=True, emit=False,
          verbose=True, policy=None, log=None):
    """
    Cleans one batch of ids, narrating it.

    :param ids: Row ids to clean.
    :param spark: The consumer's long-lived session.
    :param database: Where the rows are, and where they go.
    :param broker: Where to announce, when announcing.
    :param write: Upsert the results.
    :param emit: Publish a completion event for the batch.
    :param verbose: Count and print each stage as it runs.
    :param policy: The policy to clean under; loaded when absent.
    :param log: A ``StageLog`` to narrate into; one is made when absent.
    :returns: The ``RunResult``.
    :raises Exception: Whatever the pipeline raised. Deliberately not caught
        here -- this function cleans a batch, and deciding what a failure
        means is ``handle``'s job.
    """
    from src.runner import run_rows
    from src.spark.stagelog import StageLog

    log = log if log is not None else StageLog(counts=verbose, policy=policy)
    listed = ",".join(str(i) for i in ids)
    log.opening(f"raw id {listed}" if len(ids) == 1 else f"raw ids {listed}")
    log.event("read", f"read {raw.TABLE}", rows=len(ids))

    try:
        result = run_rows(
            ids,
            spark=spark,
            write=write,
            emit=emit,
            connection=database,
            broker=broker,
            listener=log if verbose else None,
            policy=policy,
        )
    except Exception:
        # The footer runs on the failure path too. It is what releases the
        # cached frames the stage log is holding -- and a consumer that leaked
        # one per failed message would run out of room in an afternoon.
        log.closing("failed")
        raise

    written = (
        "not written" if result.rows_written is None
        else f"upsert {result.rows_written}"
    )
    log.event("write", f"{written} -> cleaned_transactions",
              rows=result.rows_written)
    log.closing(f"{result.rows_read} row(s) cleaned")
    return result


def handle(ids, **kwargs) -> Outcome:
    """
    Cleans a batch, and decides what a failure means.

    Batch first; one at a time if that fails and there was more than one. See
    the module docstring: the second attempt exists so the failure lands on
    the row that caused it rather than on everything that travelled with it.

    :param ids: Row ids from one poll.
    :param kwargs: Passed to ``clean``.
    :returns: What happened to each id.
    """
    started = time.monotonic()
    outcome = Outcome()
    database = kwargs["database"]

    # Split before cleaning, so the accounting is exact. A batch holding both
    # kinds would otherwise be reported wholesale: the run succeeds on the
    # rows that exist, and every id in the batch is marked CLEANED --
    # including the ones nothing cleaned, whose UPDATE quietly touches no
    # rows and whose absence is never reported to anybody.
    present = raw.existing(database, ids)
    for row_id in ids:
        if row_id not in present:
            # Not marked. There is no row to mark, which is the whole finding,
            # and inventing one to hold the error would be worse than saying
            # so here.
            outcome.failed[row_id] = f"no row {row_id} in {raw.TABLE}"
    if not present:
        outcome.seconds = time.monotonic() - started
        return outcome
    ids = present

    try:
        clean(ids, **kwargs)
        outcome.cleaned = list(ids)
        raw.mark(database, ids, "CLEANED")
        outcome.seconds = time.monotonic() - started
        return outcome
    except Exception as exc:  # noqa: BLE001 - every failure is one row's
        if len(ids) == 1:
            raw.mark(database, ids, "FAILED", error=f"{type(exc).__name__}: {exc}")
            outcome.failed[ids[0]] = f"{type(exc).__name__}: {exc}"
            outcome.seconds = time.monotonic() - started
            return outcome

    # More than one, and the batch failed. Retry singly so the blame is
    # attributable -- slow, and only on a path that was already failing.
    for row_id in ids:
        try:
            clean([row_id], **kwargs)
            outcome.cleaned.append(row_id)
            raw.mark(database, [row_id], "CLEANED")
        except Exception as exc:  # noqa: BLE001
            reason = f"{type(exc).__name__}: {exc}"
            outcome.failed[row_id] = reason
            raw.mark(database, [row_id], "FAILED", error=reason)

    outcome.seconds = time.monotonic() - started
    return outcome


def poll_batch(client, subscription: Subscription, batch_size: int) -> list:
    """
    Gathers up to ``batch_size`` messages, waiting only for the first.

    :param client: A confluent_kafka ``Consumer``, or anything with the same
        ``poll``.
    :param subscription: For the poll timeout.
    :param batch_size: Most messages to take.
    :returns: The messages, possibly empty.

    The second and later polls use a zero timeout, so the batch is "everything
    already waiting" rather than "wait around in case more arrives". A
    consumer that lingered hoping for company would add its own latency to
    every single-message run -- which is the case this exists to serve well.
    """
    messages = []
    timeout = subscription.poll_timeout
    while len(messages) < batch_size:
        message = client.poll(timeout)
        timeout = 0
        if message is None:
            break
        if message.error():
            # An error message is not a message. Partition EOF and rebalance
            # notices arrive this way and mean "nothing here", not "something
            # broke" -- and treating them as data is how a consumer ends up
            # trying to clean row None.
            break
        messages.append(message)
    return messages


def consume(
    subscription: Subscription | None = None,
    *,
    spark,
    database=None,
    broker=None,
    client=None,
    batch_size: int | None = None,
    once: bool = False,
    write: bool = True,
    emit: bool = False,
    verbose: bool = True,
    should_stop=None,
    write_line=print,
) -> int:
    """
    The loop. Subscribes, and cleans what arrives until asked to stop.

    :param subscription: Where and how to read; loaded when absent.
    :param spark: The long-lived session. Required -- see ``run_rows`` on why
        a session per message is not an option.
    :param database: Where the rows are; loaded when absent.
    :param broker: Where to announce, when ``emit``.
    :param client: A Kafka consumer, for a caller that has one -- a test with
        a fake, normally. Created and closed here when absent.
    :param batch_size: Override the configured batch size.
    :param once: Return after the first batch that contained anything. For
        ``--once``, which is how the flow is demonstrated without leaving a
        process running.
    :param write: Upsert the cleaned rows.
    :param emit: Publish a completion event per batch.
    :param verbose: Count and print each stage.
    :param should_stop: Called between polls; a true return ends the loop.
        The signal handler lives in ``consumer.py`` at the repo root rather
        than here, because installing one is a decision a *process* makes and
        this is a function that a test also calls.
    :param write_line: Where the consumer's own lines go.
    :returns: Rows cleaned.
    """
    from src.config.policy import load as load_policy
    from src.db.settings import load as load_connection

    subscription = (
        subscription if subscription is not None else _load_subscription()
    )
    database = database if database is not None else load_connection()
    batch_size = batch_size or subscription.batch_size
    # Loaded once, here. Every message would otherwise re-read and re-validate
    # the same YAML, and a policy that changed under a running consumer would
    # mean two rows in one topic cleaned under different rules -- with the
    # config fingerprint on each still claiming to explain them.
    policy = load_policy()

    owned = client is None
    if owned:
        client = _client(subscription)
        client.subscribe([subscription.topic])

    write_line(
        f"Listening on {subscription.topic} at {subscription.servers} "
        f"as {subscription.group_id!r} (batch up to {batch_size}). "
        f"Ctrl-C to stop."
    )

    cleaned_total = 0
    try:
        while not (should_stop is not None and should_stop()):
            messages = poll_batch(client, subscription, batch_size)
            if not messages:
                continue

            ids, errors = decode_all(messages)
            for error in errors:
                write_line(f"  skipped a message: {error}")

            if ids:
                outcome = handle(
                    ids,
                    spark=spark,
                    database=database,
                    broker=broker,
                    write=write,
                    emit=emit,
                    verbose=verbose,
                    policy=policy,
                )
                outcome.skipped = errors
                cleaned_total += len(outcome.cleaned)
                for row_id, reason in outcome.failed.items():
                    write_line(f"  id {row_id} FAILED: {reason}")

            # After the work, and after Postgres. This one line is the
            # delivery guarantee; moving it above the handle() call would
            # convert every crash from a redelivery into a lost row.
            client.commit(asynchronous=False)

            if once:
                break
    finally:
        if owned:
            # Leaves the group deliberately rather than by timing out, so a
            # restart rejoins immediately instead of waiting for the broker to
            # notice the old member is gone.
            client.close()

    return cleaned_total


def _client(subscription: Subscription):
    """
    :param subscription: What to configure it with.
    :returns: A confluent_kafka ``Consumer``.
    :raises RuntimeError: If the library is missing, naming what needed it.
    """
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:  # pragma: no cover - environment failure
        raise RuntimeError(
            "confluent-kafka is required to run the cleaning consumer. "
            "pip install -r requirements.txt"
        ) from exc

    return Consumer(subscription.consumer_config)


def _load_subscription() -> Subscription:
    """:returns: Consumer settings from config and the environment."""
    from src.kafka.settings import load_subscription

    return load_subscription()
