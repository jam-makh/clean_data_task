"""
The scaling run deliverable 4 asks for: does this build grow with the data, or
grow *against* it?

    python -m scripts.scaling_report

Runs the whole feature build over a source replicated to 1x, 2x, 3x and 5x its
size and records wall clock, CPU and heap for every phase at every size. The
headline is the pair the brief asks for -- 1x against 5x -- and the two points
in between are what turn that pair into evidence.

Why the intermediate points
---------------------------

A single ratio cannot separate growth from overhead. A build with a large fixed
cost and linear work rises 2.6x from 1x to 5x; so does one with a smaller fixed
cost and work that is getting worse. One number cannot tell them apart. Four
points can: fit a line through log(rows) against log(seconds) and the gradient
is the exponent in ``cost ~ rows ** k``. One is linear, two is quadratic.

What that exponent is, exactly
------------------------------

It is the elasticity of *measured* cost over the range sampled, and it is not
the same thing as the exponent of the work. Fixed overhead drags it down: a
perfectly linear build carrying a constant equal to 1.5x its 1x work fits at
0.59, not 1.0, because over this range most of what is being measured is not
growing at all. Taking log-log of ``c + k*rows`` does not move ``c`` into the
intercept -- only a nonlinear fit would, and four noisy points do not support
one.

So the number is biased downward, which cuts both ways. A slope near 2 is
damning and cannot be explained away by overhead. A slope near 1 is good news
about measured cost but weaker evidence about the algorithm, because enough
fixed cost will flatten anything.

``tail_slope`` is the answer to that: the same gradient over the largest two
sizes only, where the constant matters least. It is the better predictor of
production behaviour and the noisier of the two, so both are reported and a
tail that has pulled away from the full-range fit is flagged as a curve that
is bending upward -- which is what a quadratic build looks like while the
fixed cost is still hiding it.

The absolute seconds are a property of this laptop and mean little on their
own. The gradients are a property of the algorithm and survive a bigger
machine.

What is being scaled
--------------------

Users, not history. ``features.scale.replicate`` re-keys each copy to derived
uuids rather than duplicating rows, so 5x the rows is 5x the users over the
same 43 months. Plain duplication would collide on the ``(user_id, month)``
primary key -- five copies would upsert onto the same 6,450 rows -- and would
leave the group count unchanged, which is the one number a feature build's cost
actually follows.

The consequence to read the results with: the exponent below describes growth
in the number of users. Months per user is held constant by construction, so it
says nothing about a longer calendar.

Where it writes
---------------

The scale table, never the live one. Every replicated user is a valid row with
a valid uuid, so nothing downstream would flag it as synthetic -- keeping the
two apart is the only thing that stops a benchmark from quietly becoming data.
"""

import json
import math
import sys
import time
from dataclasses import replace
from pathlib import Path

from features import builder, scale, settings as feature_settings
from src.config_readers.errors import ConfigError
from src.db import settings as db_settings
from src.rules import store
from src.spark import spark_setup

APP_NAME = "stage3-scaling-report"

# Where the curve is sampled. 1 and 5 are the deliverable; 2 and 3 are what
# make the two of them mean something. Ascending, so a run that dies on memory
# dies having already recorded the smaller sizes.
FACTORS = (1, 2, 3, 5)

REPORT = Path("data/features/scaling_report.json")

# Slope thresholds. Not tight: four points on a laptop carry real noise, and
# the question is which of 1 and 2 the exponent is near, not its third decimal.
LINEAR = 1.3
SUPERLINEAR = 1.7

# How far the tail gradient may sit above the full-range one before the curve
# is called out as bending upward. Generous, because a two-point tail is noisy;
# the flag is meant to prompt a look, not to fail a build.
BEND = 0.4

# The driver holds plans, not rows. If this grows with the data then something
# has started collecting, which is the failure this whole design exists to
# prevent -- so it is asserted rather than merely reported.
DRIVER_CEILING_MB = 50.0


def _verdict(slope: float) -> str:
    """
    :param slope: Fitted exponent.
    :returns: What that exponent means for production, in one word.
    """
    if slope < LINEAR:
        return "linear"
    if slope < SUPERLINEAR:
        return "superlinear"
    return "quadratic"


def fit(sizes: list[float], values: list[float]) -> float:
    """
    The exponent relating size to cost, by least squares on the logs.

    ``value = k * size ** slope`` becomes ``log(value) = log(k) + slope *
    log(size)``, so the exponent is the gradient of an ordinary straight-line
    fit.

    Note what this does *not* do: it does not separate fixed cost from growth.
    ``c + k*size`` is not a power law, and fitting one to it returns a gradient
    below 1 -- the larger ``c`` is, the lower the answer. The result is
    therefore a lower bound on the exponent of the work, which is why the
    caller also fits the tail of the range, where ``c`` matters least.

    Written out rather than taken from numpy: it is a six-line closed form,
    and a dependency for six lines is a dependency to keep in step for six
    lines.

    :param sizes: The input sizes, in the same order as the values.
    :param values: Cost at each size. Non-positive entries are dropped -- a
        phase can round to zero seconds, and log(0) is not a number.
    :returns: The fitted exponent, or 0.0 if fewer than two points survive.
    """
    points = [
        (math.log(size), math.log(value))
        for size, value in zip(sizes, values)
        if size > 0 and value > 0
    ]
    if len(points) < 2:
        return 0.0

    count = len(points)
    mean_x = sum(x for x, _ in points) / count
    mean_y = sum(y for _, y in points) / count

    covariance = sum((x - mean_x) * (y - mean_y) for x, y in points)
    variance = sum((x - mean_x) ** 2 for x, _ in points)

    if variance == 0:
        return 0.0
    return round(covariance / variance, 3)


def _summarise(sizes: list[float], values: list[float]) -> dict:
    """
    One series, described three ways.

    All three because each is wrong on its own. The ratio is what the brief
    asks for and cannot distinguish overhead from growth. The full-range slope
    fixes that and is biased downward by the same overhead. The tail slope
    removes most of the bias and is the noisiest of the three, being two
    points. Together they say something no one of them does.

    :param sizes: Row counts, ascending.
    :param values: Cost at each size.
    :returns: The ratio, both gradients, and the verdict.
    """
    slope = fit(sizes, values)
    tail = fit(sizes[-2:], values[-2:])

    return {
        "seconds": values,
        "slope": slope,
        "tail_slope": tail,
        "ratio": round(values[-1] / values[0], 2) if values[0] > 0 else None,
        "verdict": _verdict(max(slope, tail)),
        # The case the full-range fit is blind to: work that is getting worse
        # while a large constant still holds the average down. The tail
        # pulling away from the fit is what that looks like before it becomes
        # obvious, and at production size obvious is too late.
        "bending_up": tail - slope > BEND,
    }


def _growth(runs: list[dict], key: str) -> dict:
    """
    Per-phase growth, plus the same for the build as a whole.

    :param runs: One entry per factor, ascending, each carrying ``rows`` and a
        ``performance`` block.
    :param key: Which performance map to read -- seconds or CPU seconds.
    :returns: Phase name to its growth summary, plus a ``total`` entry.
    """
    sizes = [float(run["rows"]) for run in runs]
    phases = list(runs[0]["performance"][key])

    summary = {}
    for phase in phases:
        # A phase absent from a later run cannot be fitted across the range.
        # write_database is the one that could go missing, if a run were ever
        # made without a destination.
        if not all(phase in run["performance"][key] for run in runs):
            continue

        summary[phase] = _summarise(
            sizes, [run["performance"][key][phase] for run in runs]
        )

    total_key = (
        "total_seconds" if key == "phase_seconds" else "total_cpu_seconds"
    )
    summary["total"] = _summarise(
        sizes, [run["performance"][total_key] for run in runs]
    )
    return summary


def _check(run: dict) -> list[str]:
    """
    The two invariants a scaling run has to hold, whatever the timings say.

    :param run: One completed factor.
    :returns: Human-readable warnings, empty when both hold.
    """
    warnings = []
    performance = run["performance"]

    driver = performance.get("driver_peak_memory_mb", 0.0)
    if driver > DRIVER_CEILING_MB:
        warnings.append(
            f"x{run['factor']}: driver heap reached {driver} MB, over the "
            f"{DRIVER_CEILING_MB} MB ceiling. The driver should hold plans "
            f"and not rows -- something is collecting."
        )

    serial = sorted(
        phase
        for phase, cores in performance.get("phase_parallelism", {}).items()
        if cores < 1.0 and performance["phase_seconds"].get(phase, 0) > 1.0
    )
    if serial:
        warnings.append(
            f"x{run['factor']}: {', '.join(serial)} kept under one core busy, "
            f"so they ran serially. Serial phases cap the whole build."
        )

    return warnings


def one_run(spark, frame, rules, config, database, factor: int) -> dict:
    """
    Builds once at one size and returns what it cost.

    :param spark: The shared session.
    :param frame: Cleaned transactions at 1x, already cached.
    :param rules: The vocabularies.
    :param config: The build settings, already pointed at the scale table.
    :param database: Where the scaled table goes.
    :param factor: How many times over to replicate the source.
    :returns: The factor, its source summary and its performance block.
    """
    replicated = scale.replicate(frame, factor)
    size = scale.summarise(replicated)

    manifest = config.manifest
    scaled = replace(
        config,
        manifest=manifest.with_name(
            f"{manifest.stem}.x{factor}{manifest.suffix}"
        ),
    )

    print(
        f"  x{factor}: {size['rows']:,} rows, {size['users']:,} users ...",
        end="",
        flush=True,
    )
    _, built = builder.run(spark, replicated, rules, scaled, database)
    performance = built["performance"]
    print(
        f" {performance['total_seconds']:.1f}s wall, "
        f"{performance['total_cpu_seconds']:.1f}s cpu"
    )

    return {
        "factor": factor,
        "rows": size["rows"],
        "users": size["users"],
        "accounts": size["accounts"],
        "feature_rows": built["coverage"]["feature_rows"]["value"],
        "performance": performance,
    }


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Unused; the factors are a property of the deliverable.
    :returns: Process exit code.
    """
    config = feature_settings.load()

    try:
        database = db_settings.load()
    except ConfigError as exc:
        print(f"database settings: {exc}", file=sys.stderr)
        return 2

    # Everything below writes here instead of the live table. Done once, at the
    # top, so no later branch can forget it.
    config = replace(config, table=config.scale_table)

    # One session for every factor. Four processes would mean four JVM starts,
    # and cold start would then sit inside the very numbers being compared --
    # the largest single distortion available, and the easiest to avoid.
    spark = spark_setup.session(APP_NAME)

    try:
        rules = store.from_database(database)
        frame = builder.load_source(spark, database)
    except (ConfigError, RuntimeError) as exc:
        print(f"cannot start the scaling run: {exc}", file=sys.stderr)
        return 2

    print(f"scaling {config.table} over factors {list(FACTORS)}")

    # A build the results throw away, for the sake of the ones that follow.
    #
    # The JVM interprets bytecode until a method is hot enough to compile, so
    # the first build through any code path pays for JIT and the rest do not.
    # Left in, that cost lands entirely on x1 -- the denominator of every ratio
    # here -- and inflating the denominator makes the growth look smaller than
    # it is. The bias runs in the flattering direction, which is the kind worth
    # paying a minute to remove.
    #
    # Measured on the synthetic fixture: x1 came out slower than x5 without
    # this, which is not a scaling result, it is a warm-up artefact.
    print("  warm-up (discarded) ...", end="", flush=True)
    warmup = time.perf_counter()
    builder.run(
        spark,
        frame,
        rules,
        replace(config, manifest=config.manifest.with_name(".warmup.json")),
        database,
    )
    print(f" {time.perf_counter() - warmup:.1f}s")

    runs, warnings = [], []
    for factor in FACTORS:
        # The previous factor's cached frames are dead weight against this
        # one -- they hold memory the new build wants and distort its heap
        # sample. Cleared between runs so each factor starts from the same
        # place, which is what makes the four comparable.
        spark.catalog.clearCache()

        try:
            run = one_run(spark, frame, rules, config, database, factor)
        except ConfigError as exc:
            print(f"\nx{factor} failed: {exc}", file=sys.stderr)
            return 1

        runs.append(run)
        warnings.extend(_check(run))

    report = {
        "what": "The same feature build at four source sizes.",
        "means": "Absolute seconds are a property of this machine. The slope "
                 "is a property of the algorithm: it is the exponent in "
                 "cost ~ rows ** slope, so 1 is linear and 2 is quadratic.",
        "reading_the_slope": "It is the elasticity of measured cost, not of "
                             "the work. Fixed overhead biases it downward -- "
                             "a linear build carrying a constant equal to "
                             "1.5x its 1x cost fits at 0.59 -- so it is a "
                             "lower bound on the exponent. A slope near 2 is "
                             "therefore conclusive; a slope near 1 is good "
                             "news about measured cost and weaker evidence "
                             "about the algorithm. tail_slope is the same "
                             "gradient over the largest two sizes, where the "
                             "constant matters least: noisier, and the better "
                             "predictor at production size. bending_up flags "
                             "a phase whose tail has pulled away from its "
                             "full-range fit, which is what growth hiding "
                             "behind fixed cost looks like early.",
        "scaled_by": "Users. replicate() re-keys each copy to derived uuids, "
                     "so 5x the rows is 5x the users over the same 43 months. "
                     "Duplicating rows instead would collide on the "
                     "(user_id, month) key and leave the group count -- the "
                     "thing the cost follows -- unchanged.",
        "does_not_cover": "Longer history. Months per user is held constant "
                          "by construction, so the exponent describes growth "
                          "in users and says nothing about a longer calendar.",
        "destination": config.table,
        "factors": list(FACTORS),
        "runs": runs,
        "wall_clock_growth": _growth(runs, "phase_seconds"),
        "cpu_growth": _growth(runs, "phase_cpu_seconds"),
        "warnings": warnings,
    }

    # Added after the fit because these are properties of the curve rather
    # than of any single run, and the curve does not exist until every factor
    # has been measured. Assigned back explicitly below rather than relying on
    # the list in the dict being the same object.
    for phase, growth in report["wall_clock_growth"].items():
        if growth["verdict"] != "linear":
            warnings.append(
                f"{phase}: fitted exponent {growth['slope']} -- "
                f"{growth['verdict']}. 5x the users cost {growth['ratio']}x."
            )
        elif growth["bending_up"]:
            warnings.append(
                f"{phase}: tail gradient {growth['tail_slope']} against "
                f"{growth['slope']} over the full range. The curve is bending "
                f"upward, which is what growth looks like while fixed cost is "
                f"still hiding it. Worth a second look at a larger factor."
            )

    report["warnings"] = warnings

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    growths = report["wall_clock_growth"]
    total = growths["total"]

    # Phases heaviest first, total last -- the same order the chart draws them
    # in, so the two are read together without re-sorting anything by eye.
    ordered = sorted(
        ((name, g) for name, g in growths.items() if name != "total"),
        key=lambda item: item[1]["seconds"][-1],
        reverse=True,
    ) + [("total", total)]

    print(
        f"\n{'phase':<16} {'x1':>8} {'x5':>8} {'ratio':>7} "
        f"{'slope':>7} {'tail':>7}  verdict"
    )
    for phase, growth in ordered:
        print(
            f"{phase:<16} {growth['seconds'][0]:>8.2f} "
            f"{growth['seconds'][-1]:>8.2f} {growth['ratio']:>7} "
            f"{growth['slope']:>7} {growth['tail_slope']:>7}  "
            f"{growth['verdict']}{' (bending up)' if growth['bending_up'] else ''}"
        )

    print(
        f"\n5x the users cost {total['ratio']}x the wall clock. "
        f"Fitted exponent {total['slope']} over the range, "
        f"{total['tail_slope']} over the tail -- {total['verdict']}."
    )
    print(
        "The exponent is a lower bound: fixed overhead pulls it down, so it "
        "understates the work rather than flattering it."
    )
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)

    print(f"\nwrote {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
