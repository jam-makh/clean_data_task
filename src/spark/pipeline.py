"""
The Spark orchestrator, and the ledger of what has actually been ported.

It mirrors ``src/pipeline.py`` deliberately -- same step names, same registry
shape, same "a profile is a list of strings in YAML rather than a list of
imports in Python" indirection -- because the parity harness runs the two
side by side and any difference in how they are *driven* would show up as a
difference in what they produce.

The registry is empty. That is the Phase 01 state and it is a fact worth
being able to read off a single line rather than infer from which files exist:
``ported()`` is what the harness asks to decide how far down the profile it
can compare, so a stage becomes tested by being registered here and by
nothing else. Registering a stage that is not finished turns its parity test
red immediately, which is the intended way round.

The step contract is ``BaseCleaner.apply``'s minus the report: a function of
a DataFrame and the policy, returning a new DataFrame. What replaces the
report -- diagnostic columns the stage marks and a single collection pass
that counts them -- is Phase 02's decision, and stating it here before that
phase has made it would be inventing an interface for code that does not
exist.
"""

from src.config.policy import Policy
from src.config.policy import load as load_policy

# Step name to the thing that runs it, for the steps that have been ported.
# The names are the ones ``config/pipeline.yaml`` profiles already use, so a
# profile does not know or care which engine runs it.
#
# Phase 03 adds timestamps, codes, amounts, missing and geo.
# Phase 04 adds merchant, mcc and macro.
# Phase 05 adds duplicates, balance and consistency.
SPARK_STEP_REGISTRY: dict[str, object] = {}


def ported(names) -> list[str]:
    """
    The longest run of steps from the start of a profile that Spark can do.

    A *prefix* rather than a filter, because these stages are not independent:
    ``amounts`` signs by the type ``codes`` resolved, ``balance`` moves by the
    figure ``amounts`` parsed. Running the ported subset out of order would
    compare a Spark frame that skipped a stage against a pandas frame that
    did not, and report the skipped stage's columns as the difference.

    :param names: Step names in profile order.
    :returns: The leading names that are registered, stopping at the first
        that is not.
    """
    prefix = []
    for name in names:
        if name not in SPARK_STEP_REGISTRY:
            break
        prefix.append(name)
    return prefix


def steps_for(names) -> list:
    """
    :param names: Step names in run order.
    :returns: The registered implementations they name.
    :raises KeyError: If a name is not registered, listing what is -- and
        naming the pandas registry, because "unknown step" and "known step,
        not yet ported" are different problems with different fixes and the
        same symptom.
    """
    unknown = [n for n in names if n not in SPARK_STEP_REGISTRY]
    if unknown:
        raise KeyError(
            f"step(s) {unknown} have no Spark implementation; ported so far: "
            f"{sorted(SPARK_STEP_REGISTRY) or 'none'}. If the name is right, "
            f"the stage is simply not ported yet -- src/pipeline.py has the "
            f"pandas one."
        )
    return [SPARK_STEP_REGISTRY[n] for n in names]


def run(frame, names, policy: Policy | None = None):
    """
    Runs the named steps over a Spark frame, in order.

    :param frame: A Spark DataFrame, all columns string, from
        ``src.spark.source.read_csv``.
    :param names: Step names in run order.
    :param policy: Resolved once here and handed to every step, for the reason
        ``TransactionCleaner`` gives: an executor resolving its own policy
        would be reading a file that may not be on the machine it is running
        on. Loaded eagerly when absent so a malformed policy fails before the
        first row moves.
    :returns: The transformed frame. With no names, the frame unchanged --
        which is what makes the harness's "compare the raw read" case a real
        comparison rather than a special case.
    """
    policy = policy if policy is not None else load_policy()
    for step in steps_for(names):
        frame = step(frame, policy)
    return frame
