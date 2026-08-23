"""Merchant name cleaning: processor prefixes, reference codes, noise."""

import bisect
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
# The -CARD PMT- settlement suffix, cut off at the field width like everything
# else it sits behind, so every one of its lengths survived as its own
# spelling of the merchant: H AND M CARD, H AND M CARD P, H AND M CARD PM.
#
# Dashed and dashless are two different problems and get two different rules.
# A dash in that position can only be the suffix, so any length of it goes.
# Without one the tail is indistinguishable from a truncated last word --
# METRO CASH CAR is METRO CASH CARRY, not METRO CASH with a cut-off suffix --
# and reading it as the suffix deleted the real word: seven merchants came out
# short enough that nothing could recover them. So dashless, nothing shorter
# than the whole word CARD is taken to be the suffix, and what is left of the
# name is settled against the master rather than here.
_CARD_PMT = r"CARD\s*PMT|CARD\s*PM|CARD\s*P|CARD|CAR|CA|C"
DASHED_TRUNCATION = re.compile(rf"\s*-\s*(?:{_CARD_PMT})\s*-?\s*$")
DASHLESS_TRUNCATION = re.compile(r"\s+CARD(?:\s*P(?:M(?:T)?)?)?$")
AFFIXES = (
    CHANNEL_PREFIX, TERMINAL_PREFIX, REF_NUMBER,
    DASHED_TRUNCATION, DASHLESS_TRUNCATION,
)
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
# Neither of the two, because the row has no counterparty to confirm. Left as
# Pending it would put 176064 rows of settlement traffic in front of a
# reviewer whose answer is already known.
NOT_A_MERCHANT = "Not a merchant"

# What the name in MERCHANT_NAME turned out to be. Three states because the
# column holds three different kinds of thing, and collapsing any two of them
# loses the distinction that matters: MERCHANT is a counterparty we can name,
# INTERNAL is money moving inside the bank and names nobody, and UNIDENTIFIED
# is a counterparty we cannot name yet -- the only one of the three that is a
# question for a human.
MERCHANT, INTERNAL, UNIDENTIFIED = "Merchant", "Internal", "Unidentified"

# The same three states collapsed to the two a reader of the sheet is asking
# between: is there a counterparty on this row at all, or is this the bank
# moving money between the customer's own accounts? Unidentified belongs with
# MERCHANT here and not in a third state -- a name we cannot yet resolve is
# still a counterparty, and how far the resolution got is MATCHES_STATUS's
# question, asked one column along.
MERCHANT_TYPES = [MERCHANT, INTERNAL]

# The shortest prefix allowed to identify a merchant on its own. 'A' prefixes
# half the master and 'CAR' prefixes CARREFOUR, CAREEM and CARLSBERG alike; a
# prefix that short is a coincidence rather than evidence, and the ambiguity
# test below would only catch it when two masters happen to disagree.
MIN_PREFIX = 4

# A trailing fragment that is some prefix of the -CARD PMT- or /REF suffix,
# left behind when the dashless rule above declined to guess. Trimmed only as
# a last resort and only when the trimmed name then resolves, so a real last
# word cut to the same length is never mistaken for it.
TRUNCATED_TAIL = re.compile(
    r"\s+(?:CARD\s*PMT|CARD\s*PM|CARD\s*P|CARD|CAR|CA|C|REF|RE|R)$"
)
NON_KEY = re.compile(r"[^A-Z0-9]")


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
        #
        # Descriptors are asked first because the question they answer comes
        # first: a row describing money moving inside the bank has no
        # counterparty to look up, and looking one up anyway is what put
        # CARD SETTLEMENT at the top of the merchant table.
        movement = self._resolver(loader.internal_descriptors())
        labels = loader.internal_movement_labels()
        resolve = self._resolver(aliases)

        names, kinds = [], []
        for raw in cleaned:
            kind = movement(raw)
            if kind:
                # Named by the movement rather than by the descriptor: the
                # descriptor is truncated to eleven different lengths and
                # eight of them identify the kind without identifying which
                # of TRANSFER TO CURRENT or TRANSFER TO SAVINGS it was.
                # Recording the kind states exactly what is known, and the
                # direction is already on the row in the amount's sign.
                names.append(labels.get(kind, kind))
                kinds.append(INTERNAL)
                continue
            canonical = resolve(raw)
            names.append(canonical or raw)
            kinds.append(MERCHANT if canonical else UNIDENTIFIED)

        df["MERCHANT_NAME_CLEANED"] = names
        df["MERCHANT_KIND"] = kinds
        df["MERCHANT_TYPE"] = pd.Categorical(
            [INTERNAL if k == INTERNAL else MERCHANT for k in kinds],
            categories=MERCHANT_TYPES,
        )
        df["MERCHANT_RECOGNISED"] = [k == MERCHANT for k in kinds]
        df["INTERNAL_MOVEMENT"] = [k == INTERNAL for k in kinds]

        # Recomputed from this run rather than carried over from the file: the
        # incoming status was decided against whatever master existed then, and
        # a row that resolves against the master now is matched now. Pending is
        # what is left -- a name this pipeline could not tie to a known
        # merchant, which is exactly the merchant_review queue.
        status = {
            MERCHANT: CONFIRMED, INTERNAL: NOT_A_MERCHANT,
            UNIDENTIFIED: PENDING,
        }
        df["MATCHES_STATUS_CLEANED"] = [status[k] for k in kinds]
        self.log(
            "matches_status.pending",
            int((df["MATCHES_STATUS_CLEANED"] == PENDING).sum()),
        )

        self.log(
            "merchants_distinct",
            int(df.loc[df["MERCHANT_RECOGNISED"],
                       "MERCHANT_NAME_CLEANED"].nunique()),
        )
        self.log("merchant.internal_movement_rows",
                 int(df["INTERNAL_MOVEMENT"].sum()))
        internal_names = df.loc[
            df["INTERNAL_MOVEMENT"], "MERCHANT_NAME_CLEANED"
        ]
        for label, count in internal_names.value_counts().items():
            self.log(f"merchant.internal[{label}]", int(count))
        unknown = df["MERCHANT_KIND"] == UNIDENTIFIED
        self.log("merchant.unrecognised_rows", int(unknown.sum()))
        self.log(
            "merchant.unrecognised_names",
            int(df.loc[unknown, "MERCHANT_NAME_CLEANED"].nunique()),
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

        Internal movements are absent by construction. They are unrecognised
        as merchants because they are not merchants, which is a decision
        already taken rather than a question for a reviewer.

        :returns: Frame ready to write as the ``merchant_review`` sheet.
        """
        unknown = df[
            df["MERCHANT_KIND"].eq(UNIDENTIFIED)
            & df["MERCHANT_NAME_CLEANED"].ne("")
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

    @staticmethod
    def _key(name: str) -> str:
        """
        The source writes every name both with and without its spaces, and
        BLOMBANK is not a second bank -- treating it as one split 393
        merchants into twice that many. Comparing on letters and digits alone
        is what makes the two spellings the same lookup instead of two
        entries somebody has to remember to keep in step.

        :returns: The name reduced to what identifies it.
        """
        return NON_KEY.sub("", name.upper())

    @classmethod
    def _resolver(cls, known: dict[str, str]):
        """
        Builds the lookup that turns a cleaned name into what it identifies.

        Three passes, strongest evidence first. An exact spelling settles it.
        Failing that, a prefix identifies a name only when exactly one thing
        extends it: the source truncates to a field width, leaving one
        merchant as nine lengths of itself, and a prefix that two merchants
        share is evidence about neither. That is also the whole guard behind
        trap_pairs.json -- a never-merge group holds two entries by
        definition, so any prefix reaching both refuses rather than picking
        the one that sorts first. Last, a trailing fragment of the truncated
        -CARD PMT- suffix is trimmed, and only if the trimmed name resolves,
        so a real last word cut to the same length keeps its meaning.

        Ambiguity is counted over what the spellings resolve to, not over the
        spellings themselves: AMERICAN UNIVERSITY and AMERICAN UNIVERSITY
        BEIRUT are two spellings of one merchant, and a prefix of both has
        identified it.

        :param known: Any spelling to what it identifies -- the merchant
            master keyed to canonical names, or the descriptor file keyed to
            kinds of internal movement.
        :returns: A callable taking a cleaned name and returning what it
            identifies, or ``""`` when the evidence does not settle it.
        """
        index: dict[str, str] = {}
        for spelling, identity in known.items():
            index.setdefault(cls._key(spelling), identity)
        ordered = sorted(index)

        def by_prefix(key: str) -> str:
            """:returns: The one identity extending ``key``, else ``""``."""
            found = set()
            for candidate in ordered[bisect.bisect_left(ordered, key):]:
                if not candidate.startswith(key):
                    break
                found.add(index[candidate])
                if len(found) > 1:
                    return ""
            return found.pop() if found else ""

        def resolve(name: str) -> str:
            """:returns: What ``name`` identifies, or ``""``."""
            key = cls._key(name)
            if not key:
                return ""
            if key in index:
                return index[key]
            if len(key) >= MIN_PREFIX:
                hit = by_prefix(key)
                if hit:
                    return hit
            trimmed = TRUNCATED_TAIL.sub("", name).strip()
            if trimmed and cls._key(trimmed) != key:
                return resolve(trimmed)
            return ""

        return resolve

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
