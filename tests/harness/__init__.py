"""
The parity harness: the machinery that proves the Spark port agrees with the
pandas pipeline.

It lives under ``tests/`` rather than in ``src/spark/`` because it never runs
in production. Nothing in the pipeline imports it; only the suite does, which cuts the
sample on first use and rebuilds it when the source changes.

    sample      the subset the harness runs on, chosen rather than taken
    parity      whether two frames say the same thing
"""
