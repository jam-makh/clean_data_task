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
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import yaml

from src.config_readers.errors import ConfigError

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
class BalancePolicy:
    """
    How closely a stated running balance must account for its rows, and how
    much evidence it takes to conclude the source changed which column moves
    it.
    """

    reconcile_tolerance: float
    regime_switch_penalty: float


@dataclass(frozen=True)
class OutputPolicy:
    """
    How a timestamp is spelled on the way out.

    A judgement, not a fact: the source writes dates six ways and none of them
    is the right one for a reader. Day-first is the house convention here and
    a reader elsewhere would set it differently, which is exactly what makes
    it policy rather than a constant in the writer.

    Three formats because a timestamp is rendered at the precision it was
    observed at. Writing every row to the second would print 00:00:00 on the
    rows that never carried a time, which is the fabrication the precision
    column exists to prevent.
    """

    datetime: str
    minute: str
    date: str

    def as_dict(self) -> dict[str, str]:
        """:returns: The formats keyed the way the writer wants them."""
        return {
            "datetime": self.datetime,
            "minute": self.minute,
            "date": self.date,
        }


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
    :param balance: Running-balance reconciliation tolerance and the cost of
        concluding the source changed which column moves the balance.
    :param duplicates: Business key definitions.
    :param output: Display formats applied on write.
    """

    fx: FxPolicy
    validation: ValidationPolicy
    codes: CodesPolicy
    missing: MissingPolicy
    balance: BalancePolicy
    duplicates: DuplicatesPolicy
    output: OutputPolicy


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


def _format(section: dict, key: str, path: Path) -> str:
    """
    :returns: A strftime pattern, checked by using it rather than by parsing
        it -- an invalid directive raises here, at load, instead of on the
        last line of a long run.
    :raises ConfigError: If absent, not a string, or not a usable pattern.
    """
    if key not in section:
        raise ConfigError(f"{path}: missing required key output.{key}")
    value = section[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(
            f"{path}: output.{key} must be a non-empty string, got {value!r}"
        )
    try:
        datetime(2022, 1, 31, 13, 45, 56).strftime(value)
    except (ValueError, TypeError) as exc:
        raise ConfigError(
            f"{path}: output.{key} is not a usable date format: {exc}"
        ) from exc
    return value


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
    balance = _section(data, "balance", path)
    duplicates = _section(data, "duplicates", path)
    output = _section(data, "output", path)

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
        balance=BalancePolicy(
            reconcile_tolerance=_positive(
                balance, "reconcile_tolerance", "balance", path
            ),
            regime_switch_penalty=_positive(
                balance, "regime_switch_penalty", "balance", path
            ),
        ),
        duplicates=DuplicatesPolicy(
            business_keys=_business_keys(duplicates, path)
        ),
        output=OutputPolicy(
            datetime=_format(output, "datetime", path),
            minute=_format(output, "minute", path),
            date=_format(output, "date", path),
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
