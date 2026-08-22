"""
Runtime wiring: where the pipeline reads from and writes to.

Deliberately thin. This holds the settings that say *where* work happens,
which is a different kind of thing from the policy that says *what* the
pipeline does to a row -- and the difference is load-bearing, because the
config fingerprint covers policy and vocabulary but not this file. Reading
the same workbook into a different output directory is the same run.

Stage 2 grows a database section here when Postgres exists to point at, and a
Kafka section when the broker does. Environment-variable layering and secret
handling arrive with them: those are the settings that will actually need it,
and building the machinery before there is anything to configure means
shipping an untested default, which is worse than no default at all.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from src.config.errors import ConfigError

DEFAULT_PATH = Path("config/pipeline.yaml")


@dataclass(frozen=True)
class Paths:
    """
    :param source: Workbook the pipeline reads.
    :param output: Workbook it writes.
    """

    source: Path
    output: Path


@dataclass(frozen=True)
class Runtime:
    """The runtime configuration, validated."""

    paths: Paths


def parse(data: dict, path: Path = DEFAULT_PATH) -> Runtime:
    """
    :param data: Parsed YAML contents.
    :param path: Source path, used only in error messages.
    :returns: The validated runtime configuration.
    :raises ConfigError: On a missing section or key.
    """
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")

    paths = data.get("paths")
    if not isinstance(paths, dict):
        raise ConfigError(f"{path}: missing required section 'paths'")

    for key in ("source", "output"):
        if key not in paths:
            raise ConfigError(f"{path}: missing required key paths.{key}")
        if not isinstance(paths[key], str) or not paths[key].strip():
            raise ConfigError(
                f"{path}: paths.{key} must be a non-empty string, "
                f"got {paths[key]!r}"
            )

    # The source is not checked for existence here. A missing input file is a
    # runtime condition with its own error, not a malformed configuration, and
    # conflating the two would make this loader unusable for a caller that
    # means to pass a frame instead.
    return Runtime(
        paths=Paths(
            source=Path(paths["source"]), output=Path(paths["output"])
        )
    )


@lru_cache(maxsize=None)
def load(path: str | Path = DEFAULT_PATH) -> Runtime:
    """
    :param path: Runtime YAML to read.
    :returns: The validated runtime configuration.
    :raises ConfigError: If the file is absent, unparseable, or invalid.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Runtime config not found: {path.resolve()}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"{path}: could not be parsed as YAML: {exc}"
        ) from exc
    return parse(data, path)
