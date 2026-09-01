"""
Where the database is, read from the environment rather than from YAML.

The split follows the one ``config/pipeline.yaml`` already draws. Table names
and batch sizes are wiring and live in YAML, where a comment can explain them.
A host, a port and a password are *credentials for this machine*, they differ
between the laptop and anywhere else, and they belong in the environment --
which is also where docker compose reads them from, so the containers and the
clients cannot disagree about which values are in force.

Precedence is the real environment first, then ``.env``. That is the order
docker compose applies, and matching it means `POSTGRES_PORT=5544 make run`
does what it looks like it does.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from src.config_readers.errors import ConfigError

ENV_FILE = Path(".env")

# Matching the defaults in docker-compose.yml, which are themselves defaults
# rather than settings -- the file documents why 5433 and not 5432. Repeated
# here so that a checkout with no .env still starts, and wrong in exactly one
# way if the two ever drift, which the verify_env check catches.
DEFAULTS = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5433",
    "POSTGRES_DB": "transactions",
    "POSTGRES_USER": "pipeline",
    "POSTGRES_PASSWORD": "pipeline",
}


def read_env_file(path: Path = ENV_FILE) -> dict[str, str]:
    """
    Parses ``.env`` into a dict, ignoring comments and blank lines.

    Hand-rolled rather than python-dotenv for the reason
    ``scripts/verify_env.py`` gives: one less dependency between a broken
    environment and the message that says so.

    :param path: The env file to read.
    :returns: Its key/value pairs, empty when the file is absent.
    """
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class Database:
    """
    :param host: Host the server is reachable on from this process.
    :param port: Port on THIS machine -- not the one inside the container.
    :param database: Database name.
    :param user: Role to connect as.
    :param password: That role's password.
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def jdbc_url(self) -> str:
        """
        :returns: The URL Spark's JDBC writer connects with. Credentials are
            deliberately not in it -- they go in the properties dict instead,
            because a URL ends up in log lines and exception messages and a
            password in one of those is a password in the logs.

            ``stringtype=unspecified`` is what lets a Spark ``string`` column
            land in a Postgres ``uuid`` one. Spark has no uuid type, so the
            identifier columns travel as strings; the driver would otherwise
            bind them with an explicit varchar OID, and Postgres does not
            implicitly cast text to uuid in a parameter position. Unspecified
            leaves the type to the server, which then coerces the literal --
            and rejects a malformed id at the load, naming the value.

            This is narrower than the name suggests. Every non-string column
            is already cast on the Spark side by ``src/db/contract.py`` before
            it reaches JDBC, so those bind as Long/Date/BigDecimal and are
            untouched; the only string parameter meeting a non-text column is
            an identifier. The option is inert for the two JDBC readers.
        """
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            "?stringtype=unspecified"
        )

    @property
    def jdbc_properties(self) -> dict[str, str]:
        """:returns: The connection properties for ``DataFrame.write.jdbc``."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    @property
    def dsn(self) -> str:
        """
        :returns: A libpq connection string for psycopg2, which runs the
            statements Spark cannot: the migration, and the upsert itself.
        """
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def __str__(self) -> str:
        """:returns: The connection, with no password in it. Used in logs."""
        return f"{self.user}@{self.host}:{self.port}/{self.database}"


def connect(database: Database):
    """
    Opens a psycopg2 connection to the database these settings describe.

    Here rather than in each module that needs one, because psycopg2 is an
    optional-looking dependency whose absence produces an ImportError naming a
    module and not what wanted it -- and three modules repeating that
    translation would be three places to fix it.

    :param database: Where to connect.
    :returns: An open connection. The caller closes it, or uses it as a
        context manager -- which in psycopg2 commits the transaction and does
        NOT close the connection, a distinction worth knowing before copying
        this call.
    :raises RuntimeError: If psycopg2 is not installed, saying what needed it.
    """
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - environment failure
        raise RuntimeError(
            "psycopg2 is required to talk to Postgres. "
            "pip install -r requirements.txt"
        ) from exc

    return psycopg2.connect(database.dsn)


def load(env: dict[str, str] | None = None) -> Database:
    """
    :param env: Overrides for testing. Production passes nothing and the real
        environment plus ``.env`` is read.
    :returns: The database connection settings.
    :raises ConfigError: If the port is not a number, which is worth catching
        here rather than as a driver error forty frames into a Spark job.
    """
    if env is None:
        env = {**DEFAULTS, **read_env_file(), **os.environ}

    def value(key: str) -> str:
        return str(env.get(key, DEFAULTS[key]))

    port = value("POSTGRES_PORT")
    if not port.isdigit():
        raise ConfigError(
            f"POSTGRES_PORT must be a number, got {port!r}. It is the port on "
            f"your machine, which docker-compose.yml publishes 5432 to."
        )

    return Database(
        host=value("POSTGRES_HOST"),
        port=int(port),
        database=value("POSTGRES_DB"),
        user=value("POSTGRES_USER"),
        password=value("POSTGRES_PASSWORD"),
    )
