"""
The Stage 3 build settings, read from ``config/features.yaml`` and frozen.

Separate from ``src.config.policy`` so a Stage 3 knob does not move the
Stage 2 config fingerprint.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from src.config.errors import ConfigError

DEFAULT_PATH = Path("config/features.yaml")


@dataclass(frozen=True)
class BalanceSettings:
    """
    :param eligible_statuses: Running-balance statuses that may supply an
        account's month-end balance.
    """

    eligible_statuses: tuple[str, ...]


@dataclass(frozen=True)
class WindowSettings:
    """
    :param rolling_months: Months in the rolling mean and standard deviation.
    :param min_periods: Non-null months required before one is published.
    """

    rolling_months: int
    min_periods: int


@dataclass(frozen=True)
class OutputSettings:
    """
    :param manifest: Where the run manifest and data-quality report is
        written. The only file this build produces -- the feature table
        itself goes to Postgres and nowhere else.
    """

    manifest: Path


@dataclass(frozen=True)
class DatabaseSettings:
    """
    :param table: The table the feature build upserts into.
    :param batch_size: Rows per JDBC round trip on the staging load.
    """

    table: str
    batch_size: int


@dataclass(frozen=True)
class FeatureSettings:
    """
    :param balance: Month-end balance eligibility.
    :param windows: Rolling window shape.
    :param output: Where the manifest lands.
    :param database: Where the feature table lands.
    :param scale_factor: Replication factor for the scaling run.
    """

    balance: BalanceSettings
    windows: WindowSettings
    output: OutputSettings
    database: DatabaseSettings
    scale_factor: int


def _section(data: dict, name: str, path: Path) -> dict:
    """
    :param data: Parsed YAML root.
    :param name: Section key expected at the top level.
    :param path: File the data came from, for the error message.
    :returns: The section.
    :raises ConfigError: If it is absent or is not a mapping.
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


def _positive_int(section: dict, key: str, where: str, path: Path) -> int:
    """
    :returns: The value at ``where.key``.
    :raises ConfigError: If it is absent, not an integer, or not positive.
    """
    if key not in section:
        raise ConfigError(f"{path}: missing required key {where}.{key}")
    value = section[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(
            f"{path}: {where}.{key} must be a positive integer, got {value!r}"
        )
    return value


@lru_cache(maxsize=None)
def load(path: Path = DEFAULT_PATH) -> FeatureSettings:
    """
    Reads and validates the feature settings.

    :param path: The YAML file to read.
    :returns: The frozen settings.
    :raises ConfigError: If the file is absent, unparseable, or a value is
        outside what the build can act on.
    """
    if not path.exists():
        raise ConfigError(f"Feature settings not found: {path}")

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: {exc}") from exc

    balance = _section(data, "balance", path)
    statuses = balance.get("eligible_statuses")
    if not isinstance(statuses, list) or not statuses:
        raise ConfigError(
            f"{path}: balance.eligible_statuses must be a non-empty list"
        )

    windows = _section(data, "windows", path)
    rolling = _positive_int(windows, "rolling_months", "windows", path)
    minimum = _positive_int(windows, "min_periods", "windows", path)
    if minimum > rolling:
        raise ConfigError(
            f"{path}: windows.min_periods ({minimum}) exceeds "
            f"windows.rolling_months ({rolling}), so no statistic can ever "
            f"be published"
        )

    output = _section(data, "output", path)
    if "manifest" not in output:
        raise ConfigError(f"{path}: missing required key output.manifest")

    database = _section(data, "database", path)
    if "table" not in database:
        raise ConfigError(f"{path}: missing required key database.table")

    scale = _section(data, "scale", path)

    return FeatureSettings(
        balance=BalanceSettings(
            eligible_statuses=tuple(str(s) for s in statuses)
        ),
        windows=WindowSettings(rolling_months=rolling, min_periods=minimum),
        output=OutputSettings(manifest=Path(str(output["manifest"]))),
        database=DatabaseSettings(
            table=str(database["table"]),
            batch_size=_positive_int(database, "batch_size", "database", path),
        ),
        scale_factor=_positive_int(scale, "factor", "scale", path),
    )
