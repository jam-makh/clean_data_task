"""
The Spark cleaning stages, one module per stage in ``src/cleaners``.

Deliberately re-exporting nothing, for the reason ``src/spark/__init__.py``
gives: several of these modules hold a function named after the module, and
binding both names on the package makes ``src.spark.cleaners.timestamps``
resolve to whichever was imported last. ``src.spark.pipeline`` imports the
modules and reads ``apply`` off each, which costs one more word and cannot do
that.

Every stage here has the same signature -- ``apply(frame, policy)``, a Spark
DataFrame in and a Spark DataFrame out -- which is ``BaseCleaner.apply``'s
contract minus the report. There is no report because there is nothing to
report from: Phase 02 moved counting out of the stages entirely, so a stage
marks rows and a single pass at the end derives the totals from those marks.
The pandas ``metrics`` methods read diagnostic columns, and the columns are
what these stages produce, so the same ``metrics`` code reports on a Spark
result once it has been collected.
"""
