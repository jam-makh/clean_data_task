"""
The Spark half of the pipeline, and the harness that proves it agrees with
the pandas half.

Nothing in here cleans anything yet. What it provides is the two things the
port needs before a single stage moves: one place that decides how a Spark
session is configured, and one place that decides whether a Spark result and
a pandas result are the same answer. Both exist first on purpose -- a port
whose runtime semantics drift between entry points, or whose "it matches"
claim is unfalsifiable, cannot be debugged after the fact.

    session      how a SparkSession is configured, everywhere
    source       reading a delimited file as strings, deciding nothing
    sample       the subset the harness runs on, chosen rather than taken
    parity       whether two frames say the same thing
    pipeline     the Spark orchestrator, and the ledger of what is ported

Deliberately re-exporting nothing. ``session.py`` holds a function called
``session``, and importing it here would bind that name on the package and
shadow the module it came from -- so ``src.spark.session`` would resolve to a
function, and ``from src.spark import session as session_module`` would hand
a caller the wrong object with no error at all. Importing from the modules
themselves costs one more word and cannot do that.
"""
