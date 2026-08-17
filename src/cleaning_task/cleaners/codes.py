"""Code columns: ISO processing codes and MCC, padded and labelled."""

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner
from cleaning_task.rules import loader

# ISO 8583 field 3 is six digits in full (type, from-account, to-account); this
# source carries only the leading transaction-type pair.
PROCESSING_CODE_WIDTH = 2
MCC_WIDTH = 4


class CodeNormalizer(BaseCleaner):
    """
    Restores the leading zeros that an integer column destroyed, and
    regenerates labels from the reference rather than trusting the incoming text.

    A code spelled with digits is not a number: arithmetic on it is meaningless
    and its leading zeros carry meaning, so its canonical form is a string.
    """

    name = "codes"

    def apply(self, df: pd.DataFrame, mcc_reference: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        codes = loader.processing_codes()

        if "PROCESSING_CODE" in df.columns:
            df["PROCESSING_CODE_ISO"] = df["PROCESSING_CODE"].map(
                lambda v: self.text(v).zfill(PROCESSING_CODE_WIDTH)
            )
            unknown = ~df["PROCESSING_CODE_ISO"].isin(codes)
            self.log("processing_code.unknown", int(unknown.sum()))

            # The label is regenerated from the code, never trusted from the
            # source, so a future file spelling it differently still lands on
            # one canonical value.
            labels = df["PROCESSING_CODE_ISO"].map(lambda c: codes.get(c, ""))
            df["PROCESSING_TYPE_CLEAN"] = pd.Categorical(
                labels, categories=sorted(set(codes.values()))
            )
            if "PROCESSING_TYPE" in df.columns:
                disagree = (
                    df["PROCESSING_TYPE"].map(self.text) != labels
                ) & labels.ne("")
                self.log("processing_type.disagrees_with_code", int(disagree.sum()))

        if "MCC_CODE" in df.columns:
            df["MCC_CODE_STR"] = df["MCC_CODE"].map(
                lambda v: self.text(v).zfill(MCC_WIDTH)
            )
            reference = mcc_reference or {}
            df["MCC_CATEGORY"] = df["MCC_CODE_STR"].map(lambda c: reference.get(c, ""))
            if reference:
                unknown = int((df["MCC_CATEGORY"] == "").sum())
                self.log("mcc.not_in_reference", unknown)
            self.log("mcc.distinct", int(df["MCC_CODE_STR"].nunique()))

        return df
