"""
The arithmetic behind the scaling claim.

Deliverable 4's whole argument rests on one number -- the exponent fitted
through four measurements -- so the fit is tested against series whose answer
is known in advance rather than only against the build's own timings. A fit
that quietly returned 1.0 for everything would make a quadratic build look
fine, and nothing in a passing pipeline run would say otherwise.

No Spark and no Postgres here: these are closed-form checks on the numbers.
The real timings are measured by ``scripts.scaling_report``, which cannot be
asserted on in a test -- a slope fitted through a twenty-row fixture is fixed
overhead with a rounding error on top, and a test that pretended otherwise
would pass for the wrong reason.
"""

import pytest

from scripts.scaling_report import LINEAR, SUPERLINEAR, _growth, _verdict, fit

# Row counts standing in for the four factors. Only their ratios matter.
SIZES = [100.0, 200.0, 300.0, 500.0]


def test_a_linear_series_fits_an_exponent_of_one():
    """
    The case the build is claimed to be. Cost proportional to rows is
    ``rows ** 1``, so the fitted gradient on the logs is 1.
    """
    assert fit(SIZES, [2.0 * size for size in SIZES]) == pytest.approx(1.0)


def test_a_quadratic_series_fits_an_exponent_of_two():
    """
    The case the deliverable exists to rule out. This is the failure that
    looks identical to the linear one at small size and is fatal at large --
    the fit is what tells them apart.
    """
    assert fit(SIZES, [size * size for size in SIZES]) == pytest.approx(2.0)


def test_a_constant_series_fits_an_exponent_of_zero():
    """
    Cost that does not move with the data has no exponent. Reported as 0
    rather than as an error: a phase can genuinely be fixed overhead, and the
    reader should see that rather than a missing entry.
    """
    assert fit(SIZES, [5.0, 5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_fixed_overhead_biases_the_exponent_downward():
    """
    The limitation the report has to state rather than hide.

    ``c + k*rows`` is not a power law, and fitting one to it does not move the
    constant into the intercept -- it lowers the gradient. Linear work under a
    constant equal to 1.5x its 1x cost fits at 0.59, not 1.0.

    Which direction the error goes is what makes the number still usable: it
    understates, so a slope near 2 cannot be explained away by overhead. A
    slope near 1 is the weaker claim, and ``tail_slope`` exists for it.
    """
    values = [300.0 + 2.0 * size for size in SIZES]

    assert fit(SIZES, values) == pytest.approx(0.59, abs=0.01)

    # And the tail, where the constant is proportionally smallest, recovers
    # more of the truth. Still short of 1.0 -- two points cannot undo it.
    assert fit(SIZES[-2:], values[-2:]) > fit(SIZES, values)


def test_a_quadratic_build_is_caught_even_under_heavy_overhead():
    """
    The failure mode that matters. If a large constant could drag a quadratic
    build's exponent under the linear threshold, the whole deliverable would
    be capable of passing a build that falls over in production.
    """
    values = [300.0 + 0.01 * size * size for size in SIZES]

    assert _verdict(max(fit(SIZES, values), fit(SIZES[-2:], values[-2:]))) != "linear"


@pytest.mark.parametrize(
    "slope, expected",
    [
        (0.0, "linear"),
        (1.0, "linear"),
        (LINEAR - 0.01, "linear"),
        (LINEAR, "superlinear"),
        (SUPERLINEAR - 0.01, "superlinear"),
        (SUPERLINEAR, "quadratic"),
        (2.0, "quadratic"),
    ],
)
def test_the_verdict_reads_the_exponent(slope, expected):
    """
    The thresholds are loose on purpose -- four points on a laptop carry real
    noise, and the question is which of 1 and 2 the exponent is near.
    """
    assert _verdict(slope) == expected


def test_degenerate_input_does_not_raise():
    """
    A phase can round to zero seconds, and log(0) is not a number. Dropped
    rather than crashed on: losing one point from a fit is a worse report,
    but crashing loses the whole run.
    """
    assert fit(SIZES, [0.0, 0.0, 0.0, 0.0]) == 0.0
    assert fit(SIZES, [0.0, 0.0, 0.0, 4.0]) == 0.0  # one point is not a line
    assert fit([100.0], [1.0]) == 0.0


def test_growth_reports_both_the_ratio_and_the_slope():
    """
    Two numbers because they answer different questions. The ratio is what
    the brief asks for; the slope is what makes the ratio interpretable.
    """
    runs = [
        {
            "rows": int(size),
            "performance": {
                "phase_seconds": {"spine": 2.0 * size, "windows": 10.0},
                "total_seconds": 2.0 * size + 10.0,
            },
        }
        for size in SIZES
    ]

    growth = _growth(runs, "phase_seconds")

    assert growth["spine"]["slope"] == pytest.approx(1.0)
    assert growth["spine"]["tail_slope"] == pytest.approx(1.0)
    assert growth["spine"]["ratio"] == pytest.approx(5.0)
    assert growth["spine"]["verdict"] == "linear"
    assert growth["spine"]["bending_up"] is False

    # A phase that does not move with the data is flat, not linear-with-noise.
    assert growth["windows"]["slope"] == pytest.approx(0.0)
    assert growth["windows"]["ratio"] == pytest.approx(1.0)

    assert growth["total"]["verdict"] == "linear"


def test_a_curve_bending_upward_is_flagged():
    """
    The case the full-range fit is blind to: work getting worse while a large
    constant still holds the average down. The tail pulling away from the fit
    is the early signal, and early is the only time it is cheap to act on.
    """
    runs = [
        {
            "rows": int(size),
            "performance": {
                "phase_seconds": {"spine": 400.0 + 0.004 * size * size},
                "total_seconds": 400.0 + 0.004 * size * size,
            },
        }
        for size in SIZES
    ]

    growth = _growth(runs, "phase_seconds")["spine"]

    assert growth["tail_slope"] > growth["slope"]
    assert growth["bending_up"] is True


def test_a_phase_missing_from_a_later_run_is_skipped_not_guessed():
    """
    ``write_database`` is absent from a build without a destination. A fit
    across a range where the phase only exists at one end would be a number
    with no meaning, so it is left out rather than interpolated.
    """
    runs = [
        {
            "rows": int(size),
            "performance": {
                "phase_seconds": (
                    {"spine": size, "write_database": 1.0}
                    if index == 0
                    else {"spine": size}
                ),
                "total_seconds": size,
            },
        }
        for index, size in enumerate(SIZES)
    ]

    growth = _growth(runs, "phase_seconds")

    assert "spine" in growth
    assert "write_database" not in growth
