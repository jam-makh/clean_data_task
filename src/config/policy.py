"""
Cleaning policy: the tunable judgements, loaded once and frozen.

These values used to be module-level constants scattered across the cleaners.
None of them is an implementation detail -- each is a decision with an argument
behind it, and the argument now lives beside the value in
``config/policy.yaml`` instead of in a comment three files away.

Everything here is frozen and tuple-backed rather than list-backed. That is
deliberate: one policy object is read by every step, and in Stage 2 it gets
broadcast to Spark executors, where a mutable shared default is a bug waiting
for a long weekend.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from src.config.errors import ConfigError

DEFAULT_PATH = Path("config/policy.yaml")


@dataclass(frozen=True)
class FxPolicy:
    """Tolerances for the two independent FX checks."""

    reconcile_tolerance: float
    reference_tolerance: float


@dataclass(frozen=True)
class ValidationPolicy:
    """Which columns are load-bearing enough that a null justifies a drop."""

    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class CodesPolicy:
    """Fixed widths for the two positional code columns."""

    processing_code_width: int
    mcc_width: int


@dataclass(frozen=True)
class MissingPolicy:
    """Thresholds for deciding a value was planted rather than observed."""

    auth_repeat_threshold: int


@dataclass(frozen=True)
class DuplicatesPolicy:
    """Column sets that identify one transaction, most specific first."""

    business_keys: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Policy:
    """
    The whole policy file, validated.

    :param fx: Reconciliation and reference tolerances.
    :param validation: Required-column rules.
    :param codes: Code column widths.
    :param missing: Sentinel and repeat thresholds.
    :param duplicates: Business key definitions.
    """

    fx: FxPolicy
    validation: ValidationPolicy
    codes: CodesPolicy
    missing: MissingPolicy
    duplicates: DuplicatesPolicy


def _section(data: dict, name: str, path: Path) -> dict:
    """
    :param data: Parsed YAML root.
    :param name: Section key expected at the top level.
    :param path: File the data came from, for the error message.
    :returns: The section.
    :raises ConfigError: If the section is absent or is not a mapping.
    """
    if name not in data:
        raise ConfigError(f"{path}: missing required section {name!r}")
    section = data[name]
    if not isinstance(section, dict):
        raise ConfigError(
            f"{path}: section {name!r} must be a mapping, "
            f"got {type(section).__name__}"
        )
    return section


def _number(section: dict, key: str, where: str, path: Path) -> float:
    """
    :returns: The numeric value at ``where.key``.
    :raises ConfigError: If the key is absent or not numeric.
    """
    if key not in section:
        raise ConfigError(f"{path}: missing required key {where}.{key}")
    value = section[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(
            f"{path}: {where}.{key} must be a number, got {value!r}"
        )
    return value


def _positive(section: dict, key: str, where: str, path: Path) -> float:
    """
    Every threshold in this file is a magnitude, and neither degenerate value
    fails loudly on its own: a zero tolerance flags every row, a negative one
    flags none. Both are rejected here rather than discovered in the output.

    :returns: The value, guaranteed strictly positive.
    :raises ConfigError: If the value is zero or negative.
    """
    value = _number(section, key, where, path)
    if value <= 0:
        raise ConfigError(f"{path}: {where}.{key} must be > 0, got {value}")
    return value


def _string_list(
    section: dict, key: str, where: str, path: Path
) -> tuple[str, ...]:
    """
    :returns: The list as a tuple.
    :raises ConfigError: If it is not a non-empty list of strings.
    """
    if key not in section:
        raise ConfigError(f"{path}: missing required key {where}.{key}")
    value = section[key]
    if not isinstance(value, list) or not value:
        raise ConfigError(
            f"{path}: {where}.{key} must be a non-empty list, got {value!r}"
        )
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(
                f"{path}: {where}.{key} entries must be strings, got {item!r}"
            )
    return tuple(value)


def _business_keys(
    duplicates: dict, path: Path
) -> tuple[tuple[str, ...], ...]:
    """
    :returns: Each key group as a tuple, in file order.
    :raises ConfigError: If any group is empty or holds a non-string.
    """
    groups = duplicates.get("business_keys")
    if not isinstance(groups, list) or not groups:
        raise ConfigError(
            f"{path}: duplicates.business_keys must be a non-empty list"
        )
    parsed = []
    for index, group in enumerate(groups):
        if not isinstance(group, list) or not group:
            raise ConfigError(
                f"{path}: duplicates.business_keys[{index}] must be a "
                f"non-empty list of column names"
            )
        for column in group:
            if not isinstance(column, str):
                raise ConfigError(
                    f"{path}: duplicates.business_keys[{index}] entries "
                    f"must be strings, got {column!r}"
                )
        parsed.append(tuple(group))
    return tuple(parsed)


def parse(data: dict, path: Path = DEFAULT_PATH) -> Policy:
    """
    Validates a parsed policy mapping into a frozen ``Policy``.

    Kept separate from file reading so a test can build a policy from a
    literal without writing a temporary file.

    :param data: Parsed YAML contents.
    :param path: Source path, used only in error messages.
    :returns: The validated policy.
    :raises ConfigError: On any missing key, wrong type, or invalid value.
    """
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")

    fx = _section(data, "fx", path)
    validation = _section(data, "validation", path)
    codes = _section(data, "codes", path)
    missing = _section(data, "missing", path)
    duplicates = _section(data, "duplicates", path)

    return Policy(
        fx=FxPolicy(
            reconcile_tolerance=_positive(
                fx, "reconcile_tolerance", "fx", path
            ),
            reference_tolerance=_positive(
                fx, "reference_tolerance", "fx", path
            ),
        ),
        validation=ValidationPolicy(
            required_columns=_string_list(
                validation, "required_columns", "validation", path
            )
        ),
        codes=CodesPolicy(
            processing_code_width=int(
                _positive(codes, "processing_code_width", "codes", path)
            ),
            mcc_width=int(_positive(codes, "mcc_width", "codes", path)),
        ),
        missing=MissingPolicy(
            auth_repeat_threshold=int(
                _positive(missing, "auth_repeat_threshold", "missing", path)
            )
        ),
        duplicates=DuplicatesPolicy(
            business_keys=_business_keys(duplicates, path)
        ),
    )


@lru_cache(maxsize=None)
def load(path: str | Path = DEFAULT_PATH) -> Policy:
    """
    Reads and validates the policy file.

    Cached by path, so the call sites across the cleaners cost one read. The
    cache is keyed on the path rather than held as a module global, so a test
    can load a different file without poisoning the default.

    :param path: Policy YAML to read.
    :returns: The validated policy.
    :raises ConfigError: If the file is absent, unparseable, or invalid.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Policy file not found: {path.resolve()}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"{path}: could not be parsed as YAML: {exc}"
        ) from exc
    return parse(data, path)
