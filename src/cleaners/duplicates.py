"""Exact-row deduplication and transaction-ID collision suffixing."""

import numpy as np
import pandas as pd

from src.cleaners.base import BaseCleaner
from src.utils import audit

# The key sets that identify one transaction live in config/policy.yaml: in
# Stage 2 the same definition also derives the database upsert key, and two
# notions of "the same transaction" that can drift apart is exactly the bug
# an idempotent write is supposed to prevent.

# How many byte-identical source rows this one row stands for. Normally 1.
# This is the only place a dropped row can still be counted: the rows
# themselves are gone by the end of the run, so the survivor has to carry the
# fact that it absorbed them.
COPIES = "EXACT_DUPLICATE_COPIES"

# Whether this row's TXN_ID was shared with another row and therefore had to
# be suffixed to make TXN_ID_CLEANED unique.
COLLISION = "TXN_ID_COLLISION"


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

        copies = self._copies(df)
        df = df.drop_duplicates().copy()
        df[COPIES] = copies.reindex(df.index)

        # Collisions are disambiguated in the ID itself: two real
        # transactions sharing a key get 1000123_1 and 1000123_2, ordered by
        # date, so the cleaned ID is unique on its own and every downstream
        # join keys on one column. Only the members of a collision group are
        # suffixed -- an ID that was already unique is left exactly as it was.
        if id_col in df.columns:
            df[f"{id_col}_CLEANED"] = df[id_col].map(self.text)
            collided = df[id_col].duplicated(keep=False)
            df[COLLISION] = collided
            if collided.any():
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

        return df

    def metrics(self, df: pd.DataFrame):
        if COPIES in df.columns:
            # Every source row is accounted for: each survivor says how many
            # it stands for, so the total minus the surviving rows is what was
            # dropped. Stated this way rather than as a row count taken before
            # and after, because a count taken mid-run is the reduction this
            # contract exists to remove.
            yield (
                "exact_duplicate_rows_dropped",
                int(df[COPIES].sum()) - len(df),
            )

        # Recomputed at the end rather than marked, because every column a
        # business key names is a raw source column and no step overwrites
        # one. The rows are the post-deduplication rows either way.
        for keys in self.policy.duplicates.business_keys:
            if set(keys).issubset(df.columns):
                yield (
                    f"business_key_repeats[{'+'.join(keys)}]",
                    audit.rows(df.duplicated(subset=keys, keep=False)),
                )

        if COLLISION in df.columns:
            yield "txn_id_collisions", audit.rows(df[COLLISION])

    @staticmethod
    def _copies(df: pd.DataFrame) -> pd.Series:
        """
        Counts how many rows of ``df`` each row is byte-identical to.

        Grouping on every column, which is the same equivalence
        ``drop_duplicates`` uses, so the survivor of a group carries that
        group's size exactly. In Stage 2 this is one ``groupBy(*columns)
        .count()``; here it is a factorization per column and one pass over
        the resulting code matrix, which costs a fraction of grouping on
        thirty object columns directly.

        :param df: Frame before deduplication.
        :returns: Group size per row, on ``df``'s own index.
        """
        if df.empty:
            return pd.Series(dtype="int64", index=df.index)
        codes = np.column_stack(
            [pd.factorize(df[column])[0] for column in df.columns]
        )
        _, group, sizes = np.unique(
            codes, axis=0, return_inverse=True, return_counts=True
        )
        return pd.Series(sizes[group.ravel()], index=df.index)
