"""
The scaling curve as a picture.

    python -m scripts.scaling_chart

Reads what ``scripts.scaling_report`` measured and draws it on log-log axes,
which is the one presentation where the answer is a shape rather than a
calculation: on log-log, ``cost ~ rows ** k`` is a straight line of gradient k,
so a linear build runs parallel to the dashed reference and a quadratic one is
visibly twice as steep. Nobody has to trust the fitted number -- the fit is
just the caption for what the eye already sees.

Phases are drawn together on one pair of axes rather than in a grid. The
question is which of them has a different *slope* from the others, and slopes
are only comparable when they share an axis.
"""

import json
import sys
from pathlib import Path

import matplotlib

# Chosen before pyplot is imported. This script runs from make and from CI,
# neither of which has a display, and the default backend would go looking for
# one.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402  (must follow the backend choice)

REPORT = Path("data/features/scaling_report.json")
CHART = Path("data/features/scaling.png")

# Big enough to read the phase labels when the README renders it at width.
FIGURE = (10.0, 6.5)
DPI = 140


def _reference(axis, sizes: list[float], anchor: float) -> None:
    """
    The dashed slope-1 line every real phase is compared against.

    Anchored at the smallest measured point so it starts where the data does
    and the comparison is about gradient rather than offset.

    :param axis: The axes to draw on.
    :param sizes: Row counts, ascending.
    :param anchor: Cost at the smallest size.
    """
    perfect = [anchor * (size / sizes[0]) for size in sizes]
    axis.plot(
        sizes,
        perfect,
        linestyle="--",
        color="0.35",
        linewidth=1.6,
        label="perfectly linear (slope 1)",
        zorder=1,
    )


def main(argv: list[str] | None = None) -> int:
    """
    :param argv: Unused.
    :returns: Process exit code.
    """
    if not REPORT.exists():
        print(
            f"no scaling report at {REPORT}. Run `make features-scale` -- or "
            f"`python -m scripts.scaling_report` -- first.",
            file=sys.stderr,
        )
        return 2

    report = json.loads(REPORT.read_text(encoding="utf-8"))
    sizes = [float(run["rows"]) for run in report["runs"]]
    growth = report["wall_clock_growth"]

    figure, axis = plt.subplots(figsize=FIGURE)

    total = growth["total"]
    _reference(axis, sizes, total["seconds"][0])

    # The total first and heaviest, then the phases beneath it. Ordered by
    # cost at the largest size so the legend reads top-down like the lines do.
    axis.plot(
        sizes,
        total["seconds"],
        marker="o",
        linewidth=2.8,
        color="#1f2933",
        label=f"total (slope {total['slope']}, {total['verdict']})",
        zorder=3,
    )

    phases = sorted(
        ((name, data) for name, data in growth.items() if name != "total"),
        key=lambda item: item[1]["seconds"][-1],
        reverse=True,
    )
    for name, data in phases:
        axis.plot(
            sizes,
            data["seconds"],
            marker=".",
            linewidth=1.4,
            alpha=0.85,
            label=f"{name} (slope {data['slope']})",
            zorder=2,
        )

    axis.set_xscale("log")
    axis.set_yscale("log")
    axis.set_xlabel("source rows (log scale)")
    axis.set_ylabel("wall clock seconds (log scale)")
    axis.set_title(
        "Feature build cost against source size\n"
        "On log-log axes the gradient is the exponent: parallel to the dashed "
        "line is linear, twice as steep is quadratic",
        fontsize=11,
    )

    # Ticks at the sizes actually measured, labelled by factor, because "x3"
    # is what the reader is holding in their head and "795585" is not.
    axis.set_xticks(sizes)
    axis.set_xticklabels(
        [f"x{run['factor']}\n{run['rows']:,}" for run in report["runs"]],
        fontsize=9,
    )
    axis.minorticks_off()
    axis.grid(True, which="major", linestyle=":", alpha=0.4)
    axis.legend(fontsize=8, loc="upper left", framealpha=0.9)

    figure.tight_layout()
    CHART.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(CHART, dpi=DPI)
    plt.close(figure)

    print(f"wrote {CHART}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
