"""
The parity harness: the machinery that proves the Spark port agrees with the
pandas pipeline.

It lives under ``tests/`` rather than in ``src/spark/`` because it never runs
in production. Nothing in the pipeline imports it; only the suite and the
``make sample`` target do.

    sample      the subset the harness runs on, chosen rather than taken
    parity      whether two frames say the same thing
"""
