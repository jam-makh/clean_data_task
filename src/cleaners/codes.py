"""Code columns: ISO processing codes and MCC, padded and labelled."""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader
from src.utils import audit

# The two code widths are a property of the source network rather than of the
# standard, so they live in config/policy.yaml with the reasoning that fixes
# them; what each code *means* is asserted in processing_codes.json.

# The one label that means money coming back to the customer; everything else
# -- purchase, cash withdrawal -- is money going out. It is spelled exactly as
# processing_codes.json spells it, and lives here, beside the lookup that
# generates it, so the rule file and the constant stay in one another's sight.
REFUND_LABEL = "Purchase Return/Refund"


class CodeNormalizer(BaseCleaner):
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

    def apply(
        self, df: pd.DataFrame, mcc_reference: dict | None = None
    ) -> pd.DataFrame:
        df = df.copy()
        codes = loader.processing_codes()
        widths = self.policy.codes

        if "PROCESSING_CODE" in df.columns:
            df["PROCESSING_CODE_CLEANED"] = df["PROCESSING_CODE"].map(
                lambda v: self.text(v).zfill(widths.processing_code_width)
            )

            # The label is regenerated from the code, never trusted from the
            # source, so a future file spelling it differently still lands on
            # one canonical value.
            labels = df["PROCESSING_CODE_CLEANED"].map(
                lambda c: codes.get(c, "")
            )
            df["PROCESSING_TYPE_CLEANED"] = pd.Categorical(
                labels, categories=sorted(set(codes.values()))
            )

        if "MCC_CODE" in df.columns:
            df["MCC_CODE_CLEANED"] = df["MCC_CODE"].map(
                lambda v: self.text(v).zfill(widths.mcc_width)
            )
            reference = mcc_reference or {}
            self.has_reference = bool(reference)
            df["MCC_CATEGORY"] = df["MCC_CODE_CLEANED"].map(
                lambda c: reference.get(c, "")
            )

        return df

    def metrics(self, df: pd.DataFrame):
        # Every count here reads either a column this step wrote or a raw
        # column, and a raw column is never overwritten by any step, so
        # reading one at the end of the run gives exactly what reading it
        # here would have given. That is what lets this step add no
        # provenance column of its own: the provenance is the source value,
        # still sitting there.
        if "PROCESSING_CODE_CLEANED" in df.columns:
            known = loader.processing_codes()
            yield (
                "processing_code.unknown",
                audit.rows(~df["PROCESSING_CODE_CLEANED"].isin(known)),
            )
            if "PROCESSING_TYPE" in df.columns:
                resolved = df["PROCESSING_TYPE_CLEANED"].astype(str)
                yield (
                    "processing_type.disagrees_with_code",
                    audit.rows(
                        (df["PROCESSING_TYPE"].map(self.text) != resolved)
                        & resolved.ne("")
                    ),
                )

        if "MCC_CODE" in df.columns:
            if self.has_reference and "MCC_CATEGORY" in df.columns:
                yield (
                    "mcc.not_in_reference",
                    audit.rows(df["MCC_CATEGORY"].eq("")),
                )
            # From the raw column, not MCC_CODE_CLEANED: MccResolver adopts
            # its suggestions into that column later in the run, so counting
            # it at the end would answer "how many codes did we publish"
            # where this metric asks "how many did the source distinguish".
            width = self.policy.codes.mcc_width
            yield (
                "mcc.distinct",
                audit.distinct(
                    df["MCC_CODE"].map(lambda v: self.text(v).zfill(width))
                ),
            )
