"""Exact-row deduplication and transaction-ID collision sequencing."""

import pandas as pd

from cleaning_task.cleaners.base import BaseCleaner

BUSINESS_KEYS = [
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT", "MERCHANT_NAME"],
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT"],
]


class DuplicateCleaner(BaseCleaner):
    """
    Drops byte-identical rows, then sequences any remaining ``TXN_ID``
    collisions.

    The two cases are different: an identical row is a double-load, whereas
    two different rows sharing an ID is an upstream key fault and may still be
    two real transactions. The second case is never dropped.

    :param id_col: Column expected to be unique.
    :param order_col: Parsed date used to order collisions.
    """

    name = "duplicates"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        id_col = self.config.get("id_col", "TXN_ID")
        order_col = self.config.get("order_col", "TXN_DATE_TIME_CLEAN")

        before = len(df)
        df = df.drop_duplicates().copy()
        self.log("exact_duplicate_rows_dropped", before - len(df))

        for keys in BUSINESS_KEYS:
            if set(keys).issubset(df.columns):
                n = int(df.duplicated(subset=keys, keep=False).sum())
                self.log(f"business_key_repeats[{'+'.join(keys)}]", n)

        # Sequence collisions in a separate integer column rather than mutating
        # TXN_ID -- suffixing would flip the column's dtype to string, and only
        # on files that happen to contain a collision.
        df["TXN_ID_SEQ"] = 0
        if id_col in df.columns:
            collided = df[id_col].duplicated(keep=False)
            n_collisions = int(collided.sum())
            if n_collisions:
                sort_by = [id_col] + ([order_col] if order_col in df.columns else [])
                ordered = df.loc[collided].sort_values(sort_by).index
                seq = df.loc[ordered].groupby(id_col, sort=False).cumcount()
                df.loc[ordered, "TXN_ID_SEQ"] = seq.values
            self.log("txn_id_collisions", n_collisions)

        return df
