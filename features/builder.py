"""
The Stage 3 orchestration: cleaned transactions in, feature table out.

Assembles the spine, the monthly facts and the point-in-time layer, projects
the result down to the declared columns, then upserts it into Postgres and
writes the run report beside it.

Spark end to end. The frame that leaves ``source`` is the frame that reaches
the JDBC writer, and no step between the two brings rows into the Python
process.

Timing a lazy engine
--------------------

A Spark frame is a plan, not a result, so timing the call that builds one
measures how long it took to describe the work. Every phase below therefore
ends at a barrier -- the frame is cached and counted, which forces it -- and
the duration recorded is the time to actually produce it. That costs the
count, and it buys an answer to "which step dominated" that is a measurement
rather than a guess.

Peak memory is sampled from the JVM heap at those same barriers, because that
is where the work happens. The Python driver's own heap is reported too, and
is expected to be small: if it ever is not, something has started collecting
rows.

CPU is measured over the same brackets, across the whole process tree. Wall
clock alone cannot tell a phase that is slow because there is a lot of work
from one that is slow because it is doing that work on a single core, and it
is the second kind that stops scaling first.
"""

import json
import time
import tracemalloc
from dataclasses import dataclass, field

try:  # pragma: no cover - absence is reported as zero, not as a failure
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

from pyspark import StorageLevel
from pyspark.sql import functions as F

from features import activity, balances, contract, monthly_facts, report, source, spine, windows
from src.config_readers.errors import ConfigError
from src.db.settings import Database
from features import diagnostics
from features import writer
from features.settings import FeatureSettings
from src.rules.store import Rules

TARGET = "target_closing_balance_usd"

MEGABYTE = 1024 * 1024


@dataclass
class Timings:
    """
    Wall clock per phase, and the peak heap over the whole build.

    Deliverable 4 asks which step dominated; measuring it beats guessing, and
    a dict of durations makes the answer a number rather than an impression.

    :param phases: Phase name to wall-clock seconds, in the order they ran.
    :param cpu: Phase name to CPU seconds over the whole process tree.
    :param jvm_peak_mb: Largest JVM heap in use seen at any barrier.
    :param driver_peak_mb: Peak Python heap in the driver process.
    """

    phases: dict[str, float] = field(default_factory=dict)
    cpu: dict[str, float] = field(default_factory=dict)
    jvm_peak_mb: float = 0.0
    driver_peak_mb: float = 0.0

    def record(self, phase: str, seconds: float) -> None:
        """
        :param phase: Name of the phase that just finished.
        :param seconds: How long it took.
        """
        self.phases[phase] = round(seconds, 4)

    def record_cpu(self, phase: str, seconds: float) -> None:
        """
        :param phase: Name of the phase that just finished.
        :param seconds: CPU seconds it burned across the process tree.
        """
        self.cpu[phase] = round(seconds, 4)

    @property
    def slowest(self) -> str:
        """:returns: The phase that took longest, or empty if none ran."""
        if not self.phases:
            return ""
        return max(self.phases, key=self.phases.get)

    @property
    def total(self) -> float:
        """:returns: Wall clock for the whole build."""
        return round(sum(self.phases.values()), 4)

    @property
    def total_cpu(self) -> float:
        """:returns: CPU seconds for the whole build."""
        return round(sum(self.cpu.values()), 4)

    @property
    def parallelism(self) -> dict[str, float]:
        """
        CPU seconds over wall seconds, per phase: cores actually kept busy.

        The number wall clock alone hides. A phase sitting near 1.0 ran
        serially, and a serial phase is the one that caps the whole build no
        matter how much parallelism the rest of it has.

        :returns: Phase name to effective cores, for phases that took
            measurable time.
        """
        return {
            phase: round(self.cpu[phase] / seconds, 2)
            for phase, seconds in self.phases.items()
            if seconds > 0 and phase in self.cpu
        }


def jvm_heap_mb(spark) -> float:
    """
    The JVM heap currently in use, in megabytes.

    Sampled rather than tracked. A true peak needs a listener on task-end
    metrics; this is read at phase boundaries, which is where the largest
    frames are materialised and therefore where the interesting numbers are.
    Stated plainly in the report so nobody reads it as more than it is.

    :param spark: The session, for its JVM gateway.
    :returns: Bytes in use, as megabytes.
    """
    try:
        runtime = spark._jvm.java.lang.Runtime.getRuntime()
        used = runtime.totalMemory() - runtime.freeMemory()
        return round(used / MEGABYTE, 2)
    except Exception:  # pragma: no cover - gateway shape is version-specific
        return 0.0


def cpu_seconds() -> float:
    """
    CPU time burned by this process and everything under it.

    The process tree, not this process. Spark runs local here, which means the
    JVM doing the work is a child this process started through py4j --
    ``time.process_time()`` would report the driver's own bookkeeping and miss
    every task, which is most of the build.

    Reported rather than asserted on: divided by wall clock it says how many
    cores a phase actually kept busy, and a phase that cannot get above one is
    the one to look at first when the build stops scaling.

    :returns: User plus system seconds across the tree, or 0.0 if psutil is
        absent or a child exited while it was being read.
    """
    if psutil is None:
        return 0.0
    try:
        process = psutil.Process()
        times = process.cpu_times()
        total = times.user + times.system

        for child in process.children(recursive=True):
            try:
                child_times = child.cpu_times()
                total += child_times.user + child_times.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # A child that exited mid-walk took its own total with it.
                # Undercounting one is better than failing the build for it.
                continue

        return total
    except Exception:  # pragma: no cover - platform-specific
        return 0.0


class _Phase:
    """
    A context manager that times one phase and forces it to actually happen.

    The frame handed to ``barrier`` is cached and counted on the way out, so
    the recorded duration is the time to compute it rather than the time to
    plan it.
    """

    def __init__(self, timings: Timings, name: str, spark=None):
        """
        :param timings: Where to record the duration.
        :param name: What to record it under.
        :param spark: The session, for the heap sample. Optional.
        """
        self.timings = timings
        self.name = name
        self.spark = spark
        self.started = 0.0
        self.cpu_started = 0.0
        self.frames: list = []

    def barrier(self, frame):
        """
        Marks a frame as one this phase must materialise before it ends.

        :param frame: The frame to force.
        :returns: The same frame, cached.
        """
        frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
        self.frames.append(frame)
        return frame

    def __enter__(self) -> "_Phase":
        """:returns: This phase, timing started."""
        self.cpu_started = cpu_seconds()
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc_info) -> bool:
        """
        :param exc_info: The exception, if the body raised.
        :returns: False, so an exception inside the phase still propagates.
        """
        if exc_info[0] is None:
            for frame in self.frames:
                frame.count()

        self.timings.record(self.name, time.perf_counter() - self.started)
        self.timings.record_cpu(self.name, cpu_seconds() - self.cpu_started)

        if self.spark is not None:
            self.timings.jvm_peak_mb = max(
                self.timings.jvm_peak_mb, jvm_heap_mb(self.spark)
            )
        return False


@dataclass
class Build:
    """
    One assembled build, and the internal frames the report reads.

    :param table: The projected feature table, in contract order.
    :param facts: Monthly facts on the dense user spine, diagnostics included.
    :param filled: The account spine with balances carried forward.
    """

    table: object
    facts: object
    filled: object


def assemble(
    frame,
    rules: Rules,
    config: FeatureSettings,
    timings: Timings | None = None,
    through=None,
    spark=None,
) -> Build:
    """
    Builds the feature table without writing anything.

    Separated from the writes so the point-in-time tests can rebuild over a
    truncated source and compare frames directly.

    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :param config: The build settings.
    :param timings: Where to record per-phase durations. Optional.
    :param through: Last month the table covers. Defaults to the last month
        seen in the source.
    :param spark: The session, for the heap samples. Optional.
    :returns: The projected table and the internal frames behind it.
    """
    timings = timings or Timings()

    with _Phase(timings, "spine", spark) as phase:
        accounts = phase.barrier(spine.account_months(frame, through))
        users = phase.barrier(spine.user_months(accounts))

    with _Phase(timings, "balances", spark) as phase:
        month_ends = balances.month_end_by_account(frame, config.balance)
        filled = phase.barrier(balances.carry_forward(accounts, month_ends))

    with _Phase(timings, "monthly_facts", spark) as phase:
        facts = phase.barrier(
            monthly_facts.build(frame, filled, users, rules, config)
        )

    with _Phase(timings, "windows", spark) as phase:
        windows.verify_grain(facts)
        lagged = phase.barrier(
            windows.build(facts, monthly_facts.lag_plan(rules), config.windows)
        )

    with _Phase(timings, "assemble", spark) as phase:
        calendar = spine.calendar_features(users)
        held = activity.accounts_held(frame, users)

        # The target reads month M, which is the one column allowed to. It
        # comes from the same fact the lags do, so the label and its own
        # history are on one scale by construction.
        target = facts.select(
            "user_id", "month", F.col(balances.BALANCE).alias(TARGET)
        )

        joined = lagged
        for part in (calendar, held, target):
            joined = joined.join(part, on=["user_id", "month"], how="left")

        # Where the diagnostics leave. They rode every frame above so the
        # report can count them and so they could not become a back door to
        # month M; they are dropped by not being selected here.
        table = phase.barrier(contract.select(joined, rules.categories))
        contract.verify(table, rules.categories)

    return Build(table=table, facts=facts, filled=filled)


def collect_report(
    build: Build,
    frame,
    rules: Rules,
    config: FeatureSettings,
    timings: Timings,
    rows_written: int | None,
) -> dict:
    """
    Counts the diagnostics and assembles the manifest.

    :param build: The assembled build.
    :param frame: The cleaned transactions it was built from.
    :param rules: The vocabularies.
    :param config: The build settings.
    :param timings: Per-phase durations and peak memory.
    :param rows_written: Rows the upsert touched, or None if it was skipped.
    :returns: The manifest, ready to serialise.
    """
    txns = diagnostics.transactions(frame, rules, config.balance)
    months = diagnostics.account_months(build.filled)
    users = diagnostics.user_months(build.facts, rules)

    return report.build(
        build.table,
        rules,
        config,
        txns,
        months,
        users,
        timings,
        rows_written,
    )


def run(
    spark,
    frame,
    rules: Rules,
    config: FeatureSettings,
    database: Database | None = None,
) -> tuple[object, dict]:
    """
    The whole build: assemble, write, measure, report.

    :param spark: The session.
    :param frame: Cleaned transactions as ``source`` returned them.
    :param rules: The vocabularies.
    :param config: The build settings.
    :param database: Where the feature table goes. None skips the write, which
        is what the tests and a dry run use -- the report is still produced.
    :returns: The feature table and the run manifest.
    :raises ConfigError: If the assembled table breaks the contract.
    """
    timings = Timings()
    tracemalloc.start()

    # Cached because six of the phases below scan it, and because without the
    # cache a JDBC source would be re-read once per scan.
    frame = frame.persist(StorageLevel.MEMORY_AND_DISK)

    build = assemble(frame, rules, config, timings, spark=spark)

    rows_written = None
    if database is not None:
        with _Phase(timings, "write_database", spark):
            rows_written = writer.write(
                build.table, database, rules, config
            )

    with _Phase(timings, "report", spark):
        manifest = collect_report(
            build, frame, rules, config, timings, rows_written
        )

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    timings.driver_peak_mb = round(peak / MEGABYTE, 2)

    # Re-read now that the last two phases have been timed and both peaks are
    # known. The report is a dict, so this is a fixup rather than a rebuild.
    manifest["performance"]["phase_seconds"] = timings.phases
    manifest["performance"]["total_seconds"] = timings.total
    manifest["performance"]["slowest_phase"] = timings.slowest
    manifest["performance"]["phase_cpu_seconds"] = timings.cpu
    manifest["performance"]["total_cpu_seconds"] = timings.total_cpu
    manifest["performance"]["phase_parallelism"] = timings.parallelism
    manifest["performance"]["jvm_peak_memory_mb"] = timings.jvm_peak_mb
    manifest["performance"]["driver_peak_memory_mb"] = timings.driver_peak_mb

    config.output.manifest.parent.mkdir(parents=True, exist_ok=True)
    config.output.manifest.write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    return build.table, manifest


def load_source(spark, database: Database):
    """
    Reads the cleaned transactions Stage 2 persisted.

    Postgres is the only source. Stage 2 writes one table, Stage 3 reads it,
    and there is no file path in between that could hold a different answer.

    :param spark: The session.
    :param database: Where the cleaned transactions live.
    :returns: The cleaned transactions, as a Spark DataFrame.
    :raises ConfigError: If no database is configured.
    """
    if database is None:
        raise ConfigError(
            "no source for the feature build: Stage 3 reads "
            f"{source.TABLE} from Postgres, so the database settings have "
            "to resolve. Check .env, or run `make verify`."
        )
    return source.from_database(spark, database)
