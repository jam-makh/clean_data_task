"""Merchant name cleaning: processor prefixes, reference codes, noise."""

import re
import unicodedata

import pandas as pd

from src.cleaners.base import BaseCleaner
from src.rules import loader

URL_SUFFIX = re.compile(r"\.(COM|NET|ORG|CO|IO|AI|ME|SA|AE|LB|FR|DE|EG)\b")
# The space before the slash is load-bearing. Without it this also matches the
# slash inside a channel prefix and eats the first word of the merchant:
# ECOM/LULU HYPERMARKET became "ECOM HYPERMARKET". Every one of the 128
# reference codes in the source is written with a space in front of it and
# none of the channel prefixes are, so the space is what tells them apart.
REF_SUFFIX = re.compile(r"\s/[A-Z]{2,4}\b")

# Channel, terminal and settlement affixes wrapped around the merchant name.
# These are noise, not signal: ECOM/ sits on 11.8% of PURCHASE rows and 11.8%
# of ATM_WITHDRAWAL rows, so it says nothing about how the transaction was
# acquired -- a real card-not-present marker could not appear on a cash
# withdrawal at all. They are stripped rather than lifted into a column,
# because a column of noise is still noise; PROCESSING_TYPE already carries
# the channel, and carries it correctly.
CHANNEL_PREFIX = re.compile(r"^(?:ECOM\s*/|POS\b\s*|POS(?=[A-Z]))")
# Always TRM:<digits>, in 21431 of 21431 rows, and the digits run straight
# into the name as often as not (TRM:87669HANDM). Stripping the whole token is
# what stops the fused form being read as one alphanumeric reference code and
# deleted, which reduced 4150 rows to the bare string "TRM".
TERMINAL_PREFIX = re.compile(r"^TRM\s*:\s*\d+\s*")
REF_NUMBER = re.compile(r"\s*/\s*REF\s*\d+\s*$")
CARD_PMT_SUFFIX = re.compile(r"\s*-\s*CARD\s*PMT\s*-\s*$")
AFFIXES = (CHANNEL_PREFIX, TERMINAL_PREFIX, REF_NUMBER, CARD_PMT_SUFFIX)
# A second '*' carries the acquirer's reference code: WPY*DEUTSCHE BAHN *TNAS.
# Codes containing a digit already died with the other reference codes, so
# without this the purely alphabetic ones (TNAS, GTLD) survive as fake
# variants.
STAR_REF = re.compile(r"\*\s*[A-Z0-9]{2,6}\s*$")
BRANCH = re.compile(r"\b(BR|BRANCH|STORE|SHOP)\s*#?\d+\b")
# 'BR' with no number is still a branch marker, but only at the very end --
# mid-string it could be part of a name.
TRAILING_BRANCH = re.compile(r"\s+BR$")
ALNUM_REF = re.compile(r"^(?=.*[A-Z])(?=.*\d)[A-Z\d]+$")
PUNCT = re.compile(r"[^A-Z0-9&' ]")
LEGAL = re.compile(
    r"\b(INC|LLC|LTD|LIMITED|SAL|SARL|PLC|GMBH|AG|AB|BV|NV|CO)\b"
)

# Most rows carry no processor prefix at all, which is a fact about the row
# rather than a gap in it. Naming that state beats a blank cell whose meaning
# the reader has to guess.
UNKNOWN_PROCESSOR = "Unknown"

# What the merchant name means for the row's standing. A name that resolves to
# the master is a merchant we can identify, and identifying it is the whole
# match; anything left over is a new spelling nobody has adjudicated yet.
CONFIRMED, PENDING = "Confirmed", "Pending"


class MerchantCleaner(BaseCleaner):
    """
    Reduces a raw merchant string to a stable key.

    The ``*`` split is gated on a processor whitelist because the merchant sits
    on either side: ``SQ *TAKEALOT`` puts it on the right,
    ``COURSERA.COM *W2PA`` on the left. A blind split would corrupt 114
    merchants.
    """

    name = "merchant"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.config.get("input_col", "MERCHANT_NAME")
        if source not in df.columns:
            return df

        df = df.copy()
        processors = loader.processors()
        aliases = loader.merchant_aliases()

        cleaned, prefixes = zip(
            *(self.clean_one(v, processors) for v in df[source])
        )
        df["MERCHANT_PROCESSOR"] = [p or UNKNOWN_PROCESSOR for p in prefixes]

        # String cleaning gets 'AWS CS' and 'AWSCLOUDSERVICES' close but never
        # equal; only the master says they are one merchant. An unrecognised
        # name is left exactly as cleaned and flagged -- guessing a canonical
        # form is what the review queue exists to prevent.
        df["MERCHANT_NAME_CLEANED"] = [aliases.get(n, n) for n in cleaned]
        df["MERCHANT_RECOGNISED"] = [bool(n) and n in aliases for n in cleaned]

        # Recomputed from this run rather than carried over from the file: the
        # incoming status was decided against whatever master existed then, and
        # a row that resolves against the master now is matched now. Pending is
        # what is left -- a name this pipeline could not tie to a known
        # merchant, which is exactly the merchant_review queue.
        df["MATCHES_STATUS_CLEANED"] = [
            CONFIRMED if ok else PENDING for ok in df["MERCHANT_RECOGNISED"]
        ]
        self.log(
            "matches_status.pending",
            int((df["MATCHES_STATUS_CLEANED"] == PENDING).sum()),
        )

        self.log(
            "merchants_distinct", int(df["MERCHANT_NAME_CLEANED"].nunique())
        )
        self.log(
            "merchant.unrecognised_rows",
            int((~df["MERCHANT_RECOGNISED"]).sum()),
        )
        self.log(
            "merchant.unrecognised_names",
            int(
                df.loc[
                    ~df["MERCHANT_RECOGNISED"], "MERCHANT_NAME_CLEANED"
                ].nunique()
            ),
        )
        self._review = self._build_review(df, source)
        self.log(
            "processor_prefix_stripped",
            int((df["MERCHANT_PROCESSOR"] != UNKNOWN_PROCESSOR).sum()),
        )
        self.log(
            "empty_after_clean",
            int((df["MERCHANT_NAME_CLEANED"] == "").sum()),
        )
        return df

    REVIEW_COLUMNS = [
        "MERCHANT_NAME_CLEANED", "ROW_COUNT", "RAW_SPELLINGS",
        "COUNTRIES", "MCC_OBSERVED",
    ]

    def _build_review(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        One row per unrecognised merchant name, with the evidence a reviewer
        needs to decide whether it is a new merchant or a variant of a known
        one -- the raw spellings, the countries, and the MCCs seen.

        :returns: Frame ready to write as the ``merchant_review`` sheet.
        """
        unknown = df[
            ~df["MERCHANT_RECOGNISED"] & df["MERCHANT_NAME_CLEANED"].ne("")
        ]
        if unknown.empty:
            return pd.DataFrame(columns=self.REVIEW_COLUMNS)

        rows = []
        for name, group in unknown.groupby(
            "MERCHANT_NAME_CLEANED", sort=False
        ):
            rows.append({
                "MERCHANT_NAME_CLEANED": name,
                "ROW_COUNT": len(group),
                "RAW_SPELLINGS": "; ".join(
                    sorted({str(v) for v in group[source]})
                ),
                "COUNTRIES": "; ".join(
                    sorted({str(v) for v in group.get("MERCHANT_COUNTRY", [])})
                ),
                # int() deliberately: numpy scalars stringify as "np.int64(1)".
                "MCC_OBSERVED": str(
                    {
                        k: int(v)
                        for k, v in group["MCC_CODE_CLEANED"]
                        .value_counts()
                        .items()
                    }
                    if "MCC_CODE_CLEANED" in group else {}
                ),
            })
        return pd.DataFrame(rows).sort_values("ROW_COUNT", ascending=False)

    def review_queue(self) -> pd.DataFrame:
        """
        :returns: The unrecognised-merchant queue, empty before ``apply``.
        """
        return getattr(
            self, "_review", pd.DataFrame(columns=self.REVIEW_COLUMNS)
        )

    @classmethod
    def clean_one(cls, value, processors: frozenset[str]) -> tuple[str, str]:
        """
        :param value: Raw merchant string.
        :param processors: Prefixes that legitimately precede a ``*``.
        :returns: (cleaned name, processor prefix or empty string).
        """
        text = "" if value is None or value != value else str(value)
        if not text.strip():
            return "", ""

        text = unicodedata.normalize("NFD", text.upper())
        text = "".join(c for c in text if unicodedata.category(c) != "Mn")
        prefix = ""

        if "*" in text:
            left, _, right = text.partition("*")
            left_key = re.sub(r"\.[A-Z]{2,10}$", "", left.strip()).strip()
            if left_key in processors and right.strip():
                prefix, text = left_key, right
            else:
                # Merchant is on the left; the right side is a reference code.
                text = left

        # Before PUNCT, which would turn '/' and ':' into spaces and leave
        # each affix looking like an ordinary leading word.
        for affix in AFFIXES:
            text = affix.sub("", text.strip())

        text = STAR_REF.sub(" ", text)
        text = REF_SUFFIX.sub(" ", text)
        text = URL_SUFFIX.sub(" ", text)
        text = PUNCT.sub(" ", text)
        text = BRANCH.sub(" ", text)
        text = LEGAL.sub(" ", text)
        # Drop reference codes and store numbers. A token mixing letters and
        # digits is always a code; a pure number is only a store number when
        # something precedes it, which is what keeps "7 ELEVEN" intact.
        tokens = text.split()
        kept = [
            t for i, t in enumerate(tokens)
            if not (ALNUM_REF.match(t) or (t.isdigit() and i > 0))
        ]
        if any(not t.isdigit() for t in kept):
            text = " ".join(kept)

        text = re.sub(r"\s+", " ", text).strip()
        return TRAILING_BRANCH.sub("", text).strip(), prefix
