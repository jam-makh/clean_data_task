"""
Runtime wiring: where the pipeline reads from, writes to, and which steps run.

Deliberately thin. This holds the settings that say *where* work happens,
which is a different kind of thing from the policy that says *what* the
pipeline does to a row -- and the difference is load-bearing, because the
config fingerprint covers policy and vocabulary but not this file. Reading
the same workbook into a different output directory is the same run.

Stage 2 added ``engine`` and ``database`` here, and will add ``kafka`` with
the producer. What is deliberately NOT here is any credential: a host, a port
and a password differ per machine and live in the environment, which is where
docker compose reads them from too, so the containers and the clients cannot
disagree about which values are in force. ``src/db/settings.py`` reads them.
The split is the same one this file already draws -- what is written here is
wiring anyone may read, and a password is not that.
"""

from dataclasses import dataclass, field
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
class Profile:
    """
    One kind of source file: how to recognise it, and what to run on it.

    :param name: The name the profile is configured and requested under.
    :param detect: Columns that must all be present for a file to match.
    :param steps: Step names, in run order.
    """

    name: str
    detect: tuple[str, ...]
    steps: tuple[str, ...]

    def matches(self, columns) -> bool:
        """
        :param columns: The source's column names.
        :returns: Whether every detection column is present. All of them, not
            any: a single shared column name is a coincidence, and the whole
            point of detection is that it cannot fire on the wrong file.
        """
        return set(self.detect).issubset(set(columns))


# The engines a run can be driven by. `spark` is the Stage 2 path -- read the
# extract, clean it on Spark, upsert to Postgres. `pandas` is Stage 1 and
# writes a workbook. They are not interchangeable outputs and the flag is not
# a performance switch: choosing one chooses what the run produces.
ENGINES = ("spark", "pandas")


@dataclass(frozen=True)
class Database:
    """
    How the Spark run treats Postgres. Not *where* it is -- see the module
    docstring.

    :param enabled: Whether the run writes at all. False makes it a dry run
        that still reads, cleans and reports, which is what you want when the
        question is "does the cleaning work" and not "does the write work".
    :param batch_size: Rows per JDBC round trip. The driver defaults to 1000;
        the round trip is the expensive part and Postgres takes far larger
        batches happily. Not so large that a rejected batch's error message
        covers an unhelpfully wide range of rows.
    """

    enabled: bool = True
    batch_size: int = 10000


@dataclass(frozen=True)
class Runtime:
    """The runtime configuration, validated."""

    paths: Paths
    profiles: tuple[Profile, ...] = ()
    engine: str = "spark"
    database: Database = field(default_factory=Database)

    def profile(self, name: str) -> Profile:
        """
        :param name: A configured profile name.
        :returns: That profile.
        :raises ConfigError: If no profile has that name, listing the ones
            that do -- a typo must say what was available, not just fail.
        """
        for candidate in self.profiles:
            if candidate.name == name:
                return candidate
        raise ConfigError(
            f"unknown profile {name!r}; configured: "
            f"{', '.join(p.name for p in self.profiles) or 'none'}"
        )

    def detect(self, columns) -> Profile:
        """
        Picks the profile whose detection columns the source carries.

        First match wins, so the more specific profile is listed first in the
        file. A source matching none of them is an error rather than a
        default: the two profiles parse dates in ways that are silently wrong
        for each other's files, and a wrong month raises nothing at all.

        :param columns: The source's column names.
        :returns: The first profile that matches.
        :raises ConfigError: If none matches, naming what was looked for and
            how to override the decision.
        """
        for candidate in self.profiles:
            if candidate.matches(columns):
                return candidate
        wanted = "; ".join(
            f"{p.name} needs [{', '.join(p.detect)}]" for p in self.profiles
        )
        raise ConfigError(
            f"no profile matches this source. Looked for: {wanted or 'none'}. "
            f"Pass --profile NAME to choose one explicitly."
        )


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
        ),
        profiles=_profiles(data.get("profiles"), path),
        engine=_engine(data.get("engine"), path),
        database=_database(data.get("database"), path),
    )


def _engine(value, path: Path) -> str:
    """
    :param value: The ``engine`` key, or None when absent.
    :returns: The engine name, defaulted.
    :raises ConfigError: On a name that is not an engine, listing the ones
        that are -- a typo here would otherwise pick the default silently and
        run the wrong half of the project.
    """
    if value is None:
        return "spark"
    if value not in ENGINES:
        raise ConfigError(
            f"{path}: engine must be one of {', '.join(ENGINES)}, "
            f"got {value!r}"
        )
    return value


def _database(section, path: Path) -> Database:
    """
    :param section: The ``database`` mapping, or None when absent.
    :returns: The database run settings, defaulted.
    :raises ConfigError: On a malformed entry.
    """
    if section is None:
        return Database()
    if not isinstance(section, dict):
        raise ConfigError(
            f"{path}: 'database' must be a mapping, got "
            f"{type(section).__name__}"
        )

    enabled = section.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ConfigError(
            f"{path}: database.enabled must be true or false, got {enabled!r}"
        )

    batch = section.get("batch_size", 10000)
    # Rejecting bool explicitly because `isinstance(True, int)` is True in
    # Python, and `batch_size: yes` in YAML parses to a bool -- which would
    # otherwise become a batch size of 1.
    if isinstance(batch, bool) or not isinstance(batch, int) or batch < 1:
        raise ConfigError(
            f"{path}: database.batch_size must be a positive integer, "
            f"got {batch!r}"
        )

    return Database(enabled=enabled, batch_size=batch)


def _profiles(section, path: Path) -> tuple[Profile, ...]:
    """
    :param section: The ``profiles`` mapping, or None when absent.
    :returns: The profiles in file order, which is the order detection tries
        them in -- so the mapping's order is data, not presentation.
    :raises ConfigError: On any malformed entry.
    """
    if section is None:
        return ()
    if not isinstance(section, dict) or not section:
        raise ConfigError(
            f"{path}: 'profiles' must be a non-empty mapping, got "
            f"{type(section).__name__}"
        )

    parsed = []
    for name, body in section.items():
        where = f"profiles.{name}"
        if not isinstance(body, dict):
            raise ConfigError(f"{path}: {where} must be a mapping")
        parsed.append(
            Profile(
                name=str(name),
                detect=_names(body, "detect", where, path),
                steps=_names(body, "steps", where, path),
            )
        )
    return tuple(parsed)


def _names(body: dict, key: str, where: str, path: Path) -> tuple[str, ...]:
    """
    :returns: The list at ``where.key`` as a tuple of strings.
    :raises ConfigError: If absent, empty, or holding a non-string.
    """
    if key not in body:
        raise ConfigError(f"{path}: missing required key {where}.{key}")
    value = body[key]
    if not isinstance(value, list) or not value:
        raise ConfigError(
            f"{path}: {where}.{key} must be a non-empty list, got {value!r}"
        )
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(
                f"{path}: {where}.{key} entries must be strings, "
                f"got {item!r}"
            )
    return tuple(value)


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
