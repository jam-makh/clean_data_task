"""Code columns: ISO processing codes and MCC, padded and labelled."""

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader

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
            unknown = ~df["PROCESSING_CODE_CLEANED"].isin(codes)
            self.log("processing_code.unknown", int(unknown.sum()))

            # The label is regenerated from the code, never trusted from the
            # source, so a future file spelling it differently still lands on
            # one canonical value.
            labels = df["PROCESSING_CODE_CLEANED"].map(
                lambda c: codes.get(c, "")
            )
            df["PROCESSING_TYPE_CLEANED"] = pd.Categorical(
                labels, categories=sorted(set(codes.values()))
            )
            if "PROCESSING_TYPE" in df.columns:
                disagree = (
                    df["PROCESSING_TYPE"].map(self.text) != labels
                ) & labels.ne("")
                self.log(
                    "processing_type.disagrees_with_code", int(disagree.sum())
                )

        if "MCC_CODE" in df.columns:
            df["MCC_CODE_CLEANED"] = df["MCC_CODE"].map(
                lambda v: self.text(v).zfill(widths.mcc_width)
            )
            reference = mcc_reference or {}
            df["MCC_CATEGORY"] = df["MCC_CODE_CLEANED"].map(
                lambda c: reference.get(c, "")
            )
            if reference:
                unknown = int((df["MCC_CATEGORY"] == "").sum())
                self.log("mcc.not_in_reference", unknown)
            self.log("mcc.distinct", int(df["MCC_CODE_CLEANED"].nunique()))

        return df
