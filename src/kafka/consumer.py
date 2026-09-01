"""
The cleaning consumer: ids in, cleaned rows out.

    poll transactions.raw.ingested.v1
      -> ingest_events.decode        refuse anything that is not one of ours
      -> gather up to batch_size     one Spark job for a burst, not one each
      -> runner.run_rows             read, clean, report, upsert
      -> raw.mark CLEANED / FAILED   record what happened to each row
      -> commit                      and only now
      -> every renew_every batches   stop the driver and build another

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

What happens when a message will not decode
-------------------------------------------

It is appended to the audit trail -- ``src/kafka/audit_trail.py``, configured
at ``kafka.consumer.audit_trail`` -- and then the offset is committed.

It cannot be marked the way a failed row is. A FAILED row is recorded in
``raw_transactions`` against its id; a message that will not decode has no id,
which is exactly what is wrong with it, so there is no row to mark and nothing
to re-emit. The status column structurally cannot hold this failure, and
before the audit trail existed the only trace was a line on stdout that a
restart erased.

The write cannot stop the consumer. ``record`` returns False instead of
raising, and the loop says so on the line it was already printing -- because a
consumer killed by a full disk while reporting a malformed message is worse
than the skipping it replaced.

The session does not last forever, on purpose
---------------------------------------------

A batch run is a process. It starts a driver, cleans a file, exits, and every
byte the driver accumulated goes with it -- so nothing in the Spark half of
this project was ever written against the question "what does this cost after
the four hundredth time?". A consumer asks exactly that question, and the
answer turned out to be: enough to kill it.

What accumulates is not a leak in the sense of a bug. Cached blocks and
broadcast relations stay reachable until the plans that reference them are
collected. The status listeners keep a bounded history, and bounded is not
small when each entry holds a query plan. The Python worker pool holds
processes. Each is a few megabytes; none of them is zero; and the sum over
hundreds of batches is a 4 GB driver whose heap is mostly the residue of work
that finished an hour ago. Observed, not theorised: this consumer reached
Spark stage 1777 and 673 live broadcasts in one session and then spent twelve
minutes failing to clean batches of ONE ROW, first with "not enough memory to
build and broadcast", then with OutOfMemoryError on everything.

Two of the three fixes for that are settings -- ``spark_setup`` caps what the
listeners retain, and ``consumer.py``'s session turns off broadcast joins for
batches too small to want them. Both are worth having and neither is
sufficient, because both slow the growth down and neither stops it. The driver
footprint remains a function of uptime.

So the loop ends the driver periodically and builds another: ``renew_every``
batches, configured in ``config/pipeline.yaml``, defaulting to 50. That is the
only one of the three that bounds anything -- it resets the function rather
than reducing its slope. It costs a few seconds of session start per fifty
batches, and it is done after the commit, where nothing is outstanding.

The other half of the same problem is the *first* failure rather than the
hundredth. An OutOfMemoryError is a fact about the driver and not about the
row in hand, so ``handle`` raises ``SessionLost`` rather than marking rows
FAILED and moving on -- see that class, and ``spark_setup.is_fatal``.

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
from src.kafka import audit_trail, ingest_events
from src.kafka.settings import Subscription
from src.spark import spark_setup


@dataclass
class Outcome:
    """
    What one batch did, from the consumer's point of view rather than the
    pipeline's.

    :param cleaned: Ids written to ``cleaned_transactions``.
    :param failed: Id to the error that stopped it.
    :param skipped: Messages that were not events this consumer understands,
        as ``Undecodable`` records. Counted rather than listed by id, because
        a message that would not decode has no id to list -- which is also why
        each one is appended to the audit trail on the way past, since the
        status column on ``raw_transactions`` has no row to mark for it.
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


@dataclass(frozen=True)
class Undecodable:
    """
    A message this consumer had to refuse, and the message itself.

    The message is carried rather than only its error because it is the only
    thing that can be quarantined. Its bytes and its offset are what let
    somebody go back and see what actually arrived, and a decode error on its
    own -- which is all this used to keep -- describes the failure without
    preserving any part of the thing that failed.

    :param message: The Kafka message, as polled.
    :param error: What ``ingest_events.decode`` objected to.
    """

    message: object
    error: str

    def __str__(self) -> str:
        """:returns: The error, so a caller printing one of these reads the
        same line it read when this was a bare string."""
        return self.error


def decode_all(messages) -> tuple[list, list]:
    """
    Turns raw messages into ids, refusing what is not one of ours.

    :param messages: Kafka messages, as polled.
    :returns: (ids in arrival order without repeats, ``Undecodable`` records
        for the rest).

    A repeated id inside one batch is dropped rather than cleaned twice. The
    upsert would make the second copy harmless, but it would also make the
    log say "2 rows" about one transaction, and a log that miscounts is worse
    than one that says less.

    The refused messages come back whole. Deciding what to do with one is not
    this function's business -- the loop quarantines it -- but *keeping* it is,
    because this is the last place the message exists.
    """
    ids: list = []
    errors: list = []
    for message in messages:
        try:
            payload = ingest_events.decode(message.value())
        except ValueError as exc:
            errors.append(Undecodable(message, str(exc)))
            continue
        if payload["id"] not in ids:
            ids.append(payload["id"])
    return ids, errors


def clean(ids, *, spark, database, broker=None, write=True, emit=False,
          verbose=True, counts=False, policy=None, log=None):
    """
    Cleans one batch of ids, narrating it.

    :param ids: Row ids to clean.
    :param spark: The consumer's long-lived session.
    :param database: Where the rows are, and where they go.
    :param broker: Where to announce, when announcing.
    :param write: Upsert the results.
    :param emit: Publish a completion event for the batch.
    :param verbose: Narrate the batch at all. False is silence, and silences
        the audit trail with everything else -- which is a thing a test asks
        for and not a thing a running consumer should.
    :param counts: Evaluate and print each stage's metrics *as it runs*. Costs
        one Spark action per stage on top of the run, so it is off by default
        -- see the note below on why that does not cost any auditability.
    :param policy: The policy to clean under; loaded when absent.
    :param log: A ``StageLog`` to narrate into; one is made when absent.
    :returns: The ``RunResult``.
    :raises Exception: Whatever the pipeline raised. Deliberately not caught
        here -- this function cleans a batch, and deciding what a failure
        means is ``handle``'s job.
    """
    from src.runner import run_rows
    from src.spark.stagelog import StageLog

    log = log if log is not None else StageLog(counts=counts, policy=policy)
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

    # The audit trail, and the reason `counts` can default to off.
    #
    # Requirement: no silent cleaning anywhere in the chain -- what was
    # coerced, dropped or flagged, and why, at the cleaning stage, at the
    # database write, and here. The per-stage lines above are narration and
    # were never the record: they are this stage's metrics at this point in
    # the run, they cost a Spark action each, and with `counts` off they carry
    # no numbers. `result.report` is the record. It is every step's metrics
    # over the finished frame, computed by `pipeline.report` in two actions
    # whether or not anybody is watching, and it is the same object the batch
    # run writes as a sheet and `RunResult` carries to the completion event.
    #
    # Printed before the write line rather than after, so the order on screen
    # is the order in the chain: cleaned, accounted for, then written.
    if verbose:
        log.audit(result.report)

    written = (
        "not written" if result.rows_written is None
        else f"upsert {result.rows_written}"
    )
    log.event("write", f"{written} -> cleaned_transactions",
              rows=result.rows_written)
    log.closing(f"{result.rows_read} row(s) cleaned")
    return result


class SessionLost(RuntimeError):
    """
    The Spark driver is gone, and no further message can be cleaned.

    A RuntimeError because that is what ``consumer.py``'s ``main`` already
    treats as "the environment, not the code" and exits 2 on -- the same door
    a missing Kafka library and an unreachable broker go through, and this
    belongs with them: the process must be restarted, and nothing about the
    messages it was reading is wrong.

    Raised rather than absorbed so that the offset is *not* committed. The
    batch that was in flight when the heap went is not a failed batch, it is
    an unattempted one, and leaving the offset where it is means a restarted
    consumer picks it up again instead of a reader finding rows marked FAILED
    with a Java stack trace that says nothing about them.
    """


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
        # Unless it is not one row's. An OutOfMemoryError is a fact about the
        # driver and not about these ids: retrying them singly below would run
        # the same doomed job once per id, and marking them FAILED would put a
        # heap error in the `error` column of rows that were never read. Both
        # happened on the run this guard was written for.
        if spark_setup.is_fatal(exc):
            raise SessionLost(
                f"the Spark driver failed unrecoverably while cleaning "
                f"{ids}: {type(exc).__name__}: {exc}. The batch was not "
                f"committed and will be redelivered; restart the consumer."
            ) from exc
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
            if spark_setup.is_fatal(exc):
                raise SessionLost(
                    f"the Spark driver failed unrecoverably while cleaning "
                    f"id {row_id}: {type(exc).__name__}: {exc}. The batch was "
                    f"not committed and will be redelivered; restart the "
                    f"consumer."
                ) from exc
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
    counts: bool = False,
    renew=None,
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
    :param verbose: Narrate each batch, audit trail included.
    :param counts: Additionally evaluate and print each stage as it runs, at
        one Spark action per stage. See ``clean``.
    :param renew: How to build a replacement session, called with nothing and
        returning one. Absent means the session is never recycled, which is
        the right answer for a caller that owns the session and expects to
        still have it afterwards -- a test, a notebook -- and the wrong one
        for a process that runs for days. ``consumer.py`` passes its own
        ``_session``. See ``renew_every`` in ``config/pipeline.yaml`` for what
        recycling is for.
    :param should_stop: Called between polls; a true return ends the loop.
        The signal handler lives in ``consumer.py`` at the repo root rather
        than here, because installing one is a decision a *process* makes and
        this is a function that a test also calls.
    :param write_line: Where the consumer's own lines go.
    :returns: Rows cleaned.
    """
    from src.config_readers.policy import load as load_policy
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
    # Batches since the session was last built. Batches and not messages: what
    # accumulates on the driver is per Spark job, and a batch is one job's
    # worth however many ids travelled in it.
    since_renewal = 0
    try:
        while not (should_stop is not None and should_stop()):
            messages = poll_batch(client, subscription, batch_size)
            if not messages:
                continue

            ids, errors = decode_all(messages)
            for refused in errors:
                # Written before the commit below, for the same reason the
                # Postgres write is: the offset is the consumer's promise that
                # it has dealt with the message, and quarantining after the
                # promise would mean a crash in between loses the only copy.
                # Erring the other way appends the record twice, which the
                # offset in it makes obvious to a reader -- the file has no
                # upsert to make a redelivery a no-op the way Postgres does,
                # and a duplicate line is the cheaper of the two failures.
                kept = audit_trail.record(
                    refused.message, refused.error, subscription.audit_trail,
                )
                where = (
                    subscription.audit_trail if kept
                    else "NOWHERE -- the audit trail could not be written"
                )
                write_line(f"  skipped a message: {refused.error}  -> {where}")

            if ids:
                outcome = handle(
                    ids,
                    spark=spark,
                    database=database,
                    broker=broker,
                    write=write,
                    emit=emit,
                    verbose=verbose,
                    counts=counts,
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

            # And after the commit, which is the only safe place for it.
            #
            # Recycling tears down a driver. Doing it before the offset is
            # committed would put a several-second gap between "the rows are
            # in Postgres" and "Kafka knows", and a crash inside that gap
            # redelivers a batch that was already written -- harmless, because
            # the write is an upsert, but harmless by luck rather than by
            # arrangement. Here there is nothing outstanding: the batch is
            # written, committed, and finished, and the session about to be
            # stopped is holding nothing anybody is waiting for.
            #
            # Not inside `if ids`, because a poll that decoded nothing still
            # did no Spark work -- so an idle consumer never renews, which is
            # correct: there is nothing to reclaim.
            if ids:
                since_renewal += 1
            if renew is not None and subscription.renew_every and (
                since_renewal >= subscription.renew_every
            ):
                spark = _renew(spark, renew, write_line)
                since_renewal = 0
    finally:
        if owned:
            # Leaves the group deliberately rather than by timing out, so a
            # restart rejoins immediately instead of waiting for the broker to
            # notice the old member is gone.
            client.close()

    return cleaned_total


def _renew(spark, build, write_line):
    """
    Stops the session and builds another, announcing both.

    :param spark: The session that has done its batches.
    :param build: Called with nothing; returns the replacement.
    :param write_line: Where the two lines go.
    :returns: The new session, or the old one if the rebuild failed.

    The session it was handed is the session it stops. Not
    ``spark_setup.stop()``, which stops whatever session is *active* in this
    process -- a distinction with no visible difference in a consumer, which
    has exactly one, and a real one anywhere else: under pytest the active
    session belongs to a suite-scoped fixture, and a loop that reached for the
    active one would tear down the session every later test is still using.
    That is not a testing artefact to work around, it is this function
    reaching for a global when it had the object in its hand.

    Stopping does not restart the JVM -- the py4j gateway outlives it and the
    heap is the same heap. What it does is make the whole ``SparkContext``
    object graph unreachable: cached blocks, broadcast relations, listener
    stores, the shuffle manager's bookkeeping, and the pool of Python worker
    processes, none of which the collector could touch while a live context
    held them. The next collection then has somewhere to go. That is a weaker
    claim than "the memory is freed", and it is the true one.

    Announced rather than silent. A consumer that goes quiet for six seconds
    every fiftieth batch and says nothing about why is a consumer somebody
    will eventually debug.

    A failed rebuild keeps the old session rather than raising. It is still
    there -- ``stop`` is what already ran, and a stopped context is not a
    usable one, so this is a thin promise -- but the alternative is a consumer
    that dies of a transient failure while performing routine maintenance, and
    the next batch will report the real problem with the real error. Said out
    loud, because a recycle that did not happen is exactly the fact somebody
    reading this log later needs.
    """
    write_line("  recycling the Spark session ...")
    try:
        # getattr because the thing being recycled is only ever a session in
        # production. A test drives this loop with a placeholder, and a
        # recycle that raised AttributeError on it would be testing the
        # placeholder rather than the schedule.
        stop = getattr(spark, "stop", None)
        if stop is not None:
            stop()
        replacement = build()
    except Exception as exc:  # noqa: BLE001 - maintenance must not kill a run
        write_line(
            f"  could not rebuild the Spark session: "
            f"{type(exc).__name__}: {exc}"
        )
        return spark
    write_line("  Spark session rebuilt.")
    return replacement


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
