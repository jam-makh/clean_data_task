"""
The Spark half of the pipeline.

    spark_setup    session configuration, and reading the source as strings
    spark_utils    shared column expressions and broadcast rule tables
    pipeline       the orchestrator, and the ledger of what is ported
    cleaners/      one module per stage, mirroring src/cleaners

The parity harness that proves this agrees with the pandas half lives in
``tests/harness`` -- it never runs in production.

Deliberately re-exporting nothing. Several of these modules hold a function
named after the module, and binding both names on the package makes the
module name resolve to whichever was imported last, with no error at all.
Importing from the modules themselves costs one more word and cannot do that.
"""
