"""
Code columns: ISO processing codes and MCC, padded and labelled.

Engine-neutral: the column names, status vocabularies and pure helpers that
describe the cleaned schema, with every pandas-facing method removed. The
Spark cleaners in ``src/spark/cleaners/`` import from here.

Generated from the reviewed pandas module by deleting its DataFrame methods;
every line that survives is unchanged from it.
"""

from src.rules import loader

# The two code widths are a property of the source network rather than of the
# standard, so they live in config/policy.yaml with the reasoning that fixes
# them; what each code *means* is asserted in processing_codes.json.

# The one label that means money coming back to the customer; everything else
# -- purchase, cash withdrawal -- is money going out. It is spelled exactly as
# processing_codes.json spells it, and lives here, beside the lookup that
# generates it, so the rule file and the constant stay in one another's sight.
REFUND_LABEL = "Purchase Return/Refund"


class CodeNormalizer:
    """
    Restores the leading zeros that an integer column destroyed, and
    regenerates labels from the reference rather than trusting the incoming
    text.

    A code spelled with digits is not a number: arithmetic on it is meaningless
    and its leading zeros carry meaning, so its canonical form is a string.
    """

    name = "codes"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Whether a reference was supplied at all, which decides whether
        # "not in the reference" is a finding or a meaningless zero. A fact
        # about the run's configuration, known before a row is read, so
        # holding it here is not the accumulator the contract forbids -- in
        # Stage 2 the driver knows the same thing by having loaded the table.
        self.has_reference = False
