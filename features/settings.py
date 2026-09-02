"""
settings.py reads Stage 3 configuration from config/features.yaml, validates it, and converts it into structured, 
immutable Python settings that the rest of the pipeline can safely use.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from src.config_readers.errors import ConfigError

DEFAULT_PATH = Path("config/features.yaml")


@dataclass(frozen=True)
class FeatureSettings:
    """
    Every Stage 3 setting.
    """

    # Running-balance statuses that may supply an account's month-end balance.
    eligible_statuses: tuple[str, ...]

    # Months in the rolling mean and standard deviation.
    rolling_months: int

    # Non-null months required before a rolling statistic is published.
    min_periods: int

    # Where the run manifest and data-quality report is written. The only file
    # this build produces -- the feature table itself goes to Postgres and
    # nowhere else.
    manifest: Path

    # The table the feature build upserts into.
    table: str

    # Rows per JDBC round trip on the staging load.
    batch_size: int

    # Replication factor for the scaling run.
    scale_factor: int

    # Where a scaling run's table lands, kept apart from the live one so a
    # benchmark cannot leave synthetic rows in it.
    scale_table: str


def _section(data: dict, name: str, path: Path) -> dict:
    """
    Check that a required top-level YAML section exists
    
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
    Validates values that must be positive integers, such as rolling_months and min_periods.

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
    Reads the features.yaml file and validates that the values are valid and logically usable
    Returns those values as a structured FeatureSettings object
    
    :param path: The YAML file to read.
    :returns: The frozen settings.
    :raises ConfigError: If the file is absent, unparseable, or a value is
        outside what the build can act on.
    """
    # Fail early if the configuration file does not exist.
    if not path.exists():
        raise ConfigError(f"Feature settings not found: {path}")

    # Read and safely parse the YAML configuration file.
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: {exc}") from exc

    # Read and validate the balance configuration section.
    balance = _section(data, "balance", path)
    statuses = balance.get("eligible_statuses")

    # Ensure at least one running-balance status is eligible.
    if not isinstance(statuses, list) or not statuses:
        raise ConfigError(
            f"{path}: balance.eligible_statuses must be a non-empty list"
        )

    # Read and validate the rolling-window configuration.
    windows = _section(data, "windows", path)
    rolling = _positive_int(windows, "rolling_months", "windows", path)
    minimum = _positive_int(windows, "min_periods", "windows", path)

    # Prevent requiring more observations than the rolling window contains.
    if minimum > rolling:
        raise ConfigError(
            f"{path}: windows.min_periods ({minimum}) exceeds "
            f"windows.rolling_months ({rolling}), so no statistic can ever "
            f"be published"
        )

    # Read and validate where the run manifest should be written.
    output = _section(data, "output", path)

    # Ensure the manifest output path is configured.
    if "manifest" not in output:
        raise ConfigError(f"{path}: missing required key output.manifest")

    # Read and validate the production database configuration.
    database = _section(data, "database", path)

    # Ensure the destination feature table is configured.
    if "table" not in database:
        raise ConfigError(f"{path}: missing required key database.table")

    # Read and validate the scaling benchmark configuration.
    scale = _section(data, "scale", path)

    # Ensure the scaling benchmark table is configured.
    if "table" not in scale:
        raise ConfigError(f"{path}: missing required key scale.table")

    # Keep synthetic scaling data separate from the production feature table.
    if scale["table"] == database["table"]:
        raise ConfigError(
            f"{path}: scale.table and database.table are both "
            f"{scale['table']!r}. They have to differ -- the whole point of "
            f"the separate table is that a scaling run's synthetic users "
            f"never reach the table Stage 4 reads."
        )

    # Convert the validated YAML values into one immutable settings object.
    return FeatureSettings(
        eligible_statuses=tuple(str(s) for s in statuses),
        rolling_months=rolling,
        min_periods=minimum,
        manifest=Path(str(output["manifest"])),
        table=str(database["table"]),
        batch_size=_positive_int(database, "batch_size", "database", path),
        scale_factor=_positive_int(scale, "factor", "scale", path),
        scale_table=str(scale["table"]),
    )
