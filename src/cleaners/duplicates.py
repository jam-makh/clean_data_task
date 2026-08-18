"""Exact-row deduplication and transaction-ID collision suffixing."""

import pandas as pd

from src.cleaners.base import BaseCleaner

BUSINESS_KEYS = [
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT", "MERCHANT_NAME"],
    ["ACCOUNT_ID", "TXN_DATE_TIME", "TXN_AMOUNT"],
]


class DuplicateCleaner(BaseCleaner):
    """
    Drops byte-identical rows, then suffixes any remaining ``TXN_ID``
    collisions so the cleaned ID is unique on its own.

    The two cases are different: an identical row is a double-load, whereas
    two different rows sharing an ID is an upstream key fault and may still be
    two real transactions. The second case is never dropped.

    :param id_col: Column expected to be unique.
    :param order_col: Parsed date used to order collisions.
    """

    name = "duplicates"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        id_col = self.config.get("id_col", "TXN_ID")
        order_col = self.config.get("order_col", "TXN_DATE_TIME_CLEANED")

        before = len(df)
        df = df.drop_duplicates().copy()
        self.log("exact_duplicate_rows_dropped", before - len(df))

        for keys in BUSINESS_KEYS:
            if set(keys).issubset(df.columns):
                n = int(df.duplicated(subset=keys, keep=False).sum())
                self.log(f"business_key_repeats[{'+'.join(keys)}]", n)

        # Collisions are disambiguated in the ID itself: two real
        # transactions sharing a key get 1000123_1 and 1000123_2, ordered by
        # date, so the cleaned ID is unique on its own and every downstream
        # join keys on one column. Only the members of a collision group are
        # suffixed -- an ID that was already unique is left exactly as it was.
        if id_col in df.columns:
            df[f"{id_col}_CLEANED"] = df[id_col].map(self.text)
            collided = df[id_col].duplicated(keep=False)
            n_collisions = int(collided.sum())
            if n_collisions:
                sort_by = [id_col] + (
                    [order_col] if order_col in df.columns else []
                )
                ordered = df.loc[collided].sort_values(sort_by).index
                groups = df.loc[ordered].groupby(id_col, sort=False)
                seq = groups.cumcount() + 1
                df.loc[ordered, f"{id_col}_CLEANED"] = [
                    f"{base}_{n}"
                    for base, n in zip(
                        df.loc[ordered, f"{id_col}_CLEANED"], seq
                    )
                ]
            self.log("txn_id_collisions", n_collisions)

        return df
