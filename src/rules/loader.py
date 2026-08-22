"""Loads and caches the JSON rule files that ship with the package."""

import json
from functools import lru_cache
from pathlib import Path

_JSON_DIR = Path(__file__).parent / "json"


@lru_cache(maxsize=None)
def load(name: str) -> dict:
    """
    Reads a rule file by stem, cached so repeated calls cost nothing.

    :param name: File stem, e.g. ``"processors"``.
    :returns: Parsed JSON contents.
    :raises FileNotFoundError: If the rule file is not present.
    """
    path = _JSON_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Rule file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def processors() -> frozenset[str]:
    """:returns: Payment-processor prefixes that gate the ``*`` split."""
    return frozenset(p.upper() for p in load("processors")["prefixes"])


def processing_codes() -> dict[str, str]:
    """:returns: ISO 8583 transaction-type code to label."""
    return load("processing_codes")["codes"]


def fx_rates() -> dict[str, float]:
    """
    Inverts the rule file's "units per USD" into the rate the data states,
    which is USD per unit -- the direction ``FX_RATE`` is stored in.

    :returns: Currency code to USD per one unit of it.
    """
    units = load("fx_rates")["units_per_usd"]
    return {
        code.upper(): 1 / float(value)
        for code, value in units.items()
        if float(value) > 0
    }


def minor_units() -> dict[str, int]:
    """:returns: Currency code to the number of digits in its minor unit."""
    return {
        code.upper(): int(units)
        for code, units in load("currencies")["minor_units"].items()
    }


def zero_decimal_currencies() -> frozenset[str]:
    """
    Derived rather than listed, so the two facts cannot drift apart: a
    currency is zero-decimal exactly when it has no minor unit.

    :returns: Currencies where a trailing three-digit group can only be a
        thousands separator.
    """
    return frozenset(
        code for code, units in minor_units().items() if units == 0
    )


def trap_pairs() -> list[dict]:
    """
    :returns: The enforced never-merge groups, each with its members and the
        evidence that settled it.
    """
    return load("trap_pairs")["never_merge"]


def date_formats() -> tuple[list[dict], set[str]]:
    """:returns: The ordered format list and the set of null tokens."""
    data = load("date_formats")
    return data["formats"], set(data["null_tokens"])


def timestamp_formats() -> dict:
    """
    :returns: The forecast source's format list, the settlement format list,
        the null tokens, and the source clock the epoch column is rendered in.
    """
    return load("timestamp_formats")


def macro_series() -> dict:
    """
    Keys are strings because JSON has no tuple: the per-country series are
    stored as ``"YYYY-MM|CC"`` so one flat dict answers one lookup.

    :returns: The interest, inflation and holiday series with their coverage.
    """
    return load("macro_series")


def city_aliases() -> tuple[dict[str, str], set[str]]:
    """
    Inverts the canonical-to-variants map into variant-to-canonical, which is
    the direction lookups actually need.

    :returns: Alias map and the set of e-commerce marker tokens.
    """
    data = load("city_aliases")
    flat = {}
    for canonical, variants in data["aliases"].items():
        flat[canonical] = canonical
        for variant in variants:
            flat[variant] = canonical
    return flat, set(data["ecommerce_tokens"])


def non_geographic_cities() -> dict[str, str]:
    """
    :returns: Canonical city value to the kind of non-place it is. These
        occupy the city column without naming one, so they carry no country
        and are not UNKNOWN either.
    """
    return load("city_aliases").get("non_geographic", {})


def city_countries() -> dict[str, str]:
    """:returns: Canonical city to the ISO country it sits in."""
    return load("city_countries")["countries"]


def mcc_rules() -> dict:
    """
    :returns: Catch-all code, suspect codes, deterministic rules,
        thresholds.
    """
    return load("mcc_rules")


def merchants() -> dict[str, dict]:
    """:returns: Canonical merchant name to its master entry."""
    return load("merchants")["merchants"]


def merchant_aliases() -> dict[str, str]:
    """
    Flattens the master into the direction lookups need: any known spelling to
    its canonical name. A canonical name maps to itself, so one membership test
    answers both "do we recognise this" and "what is it really called".

    :returns: Known spelling to canonical name.
    :raises ValueError: If one alias is claimed by two merchants, which would
        make the mapping depend on dict order.
    """
    flat: dict[str, str] = {}
    for canonical, entry in merchants().items():
        flat[canonical] = canonical
        for alias in entry.get("aliases", []):
            if alias in flat and flat[alias] != canonical:
                raise ValueError(
                    f"alias {alias!r} claimed by "
                    f"{flat[alias]!r} and {canonical!r}"
                )
            flat[alias] = canonical
    return flat
