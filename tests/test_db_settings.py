"""Connection settings: precedence, and the errors worth catching early."""

import pytest

from src.config.errors import ConfigError
from src.db import settings


def test_defaults_stand_in_for_an_absent_environment():
    """
    A checkout with no .env still produces a usable connection rather than a
    KeyError, and the values match docker-compose.yml's own defaults.
    """
    database = settings.load(env={})

    assert database.host == "localhost"
    assert database.port == 5433
    assert database.jdbc_url == "jdbc:postgresql://localhost:5433/transactions"


def test_the_environment_is_read():
    values = {
        "POSTGRES_HOST": "db.internal",
        "POSTGRES_PORT": "6000",
        "POSTGRES_DB": "warehouse",
        "POSTGRES_USER": "loader",
        "POSTGRES_PASSWORD": "secret",
    }

    database = settings.load(env=values)

    assert database.jdbc_url == "jdbc:postgresql://db.internal:6000/warehouse"
    assert database.jdbc_properties["user"] == "loader"


def test_a_non_numeric_port_is_rejected_by_name():
    """
    Caught here rather than as a driver error forty frames into a Spark job,
    and the message says which setting and what it is for.
    """
    with pytest.raises(ConfigError) as raised:
        settings.load(env={"POSTGRES_PORT": "5433a"})

    assert "POSTGRES_PORT" in str(raised.value)


def test_the_password_is_not_in_the_url_or_the_repr():
    """
    The URL reaches log lines and exception messages. Credentials travel in
    the properties dict instead, which does not.
    """
    database = settings.load(env={"POSTGRES_PASSWORD": "hunter2"})

    assert "hunter2" not in database.jdbc_url
    assert "hunter2" not in str(database)
    assert database.jdbc_properties["password"] == "hunter2"


def test_the_env_file_parser_ignores_comments_and_quotes(tmp_path):
    path = tmp_path / ".env"
    path.write_text(
        "# a comment\n"
        "\n"
        "POSTGRES_DB=\"quoted\"\n"
        "POSTGRES_USER = spaced \n"
        "not a setting\n",
        encoding="utf-8",
    )

    values = settings.read_env_file(path)

    assert values == {"POSTGRES_DB": "quoted", "POSTGRES_USER": "spaced"}


def test_an_absent_env_file_is_not_an_error(tmp_path):
    assert settings.read_env_file(tmp_path / "nope") == {}
