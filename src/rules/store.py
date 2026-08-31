"""
The Postgres-backed rule vocabularies Stage 3 reads.

Seeded from ``src/rules/json/`` so the reviewed source stays in git, served
from the tables so the feature build has one place to look.
"""

from dataclasses import dataclass

from src.config.errors import ConfigError
from src.db.settings import Database, connect
from src.rules import loader

SCHEMA = "sql/features_schema.sql"

# The tables this module owns, in dependency order: the category vocabulary
# has to exist before the MCC map can reference it.
TABLES = (
    "rule_processing_codes",
    "rule_spending_categories",
    "rule_mcc_categories",
)


@dataclass(frozen=True)
class Rules:
    """
    The vocabularies one feature build runs against, resolved once.

    Frozen and mapping-backed rather than a live connection: the build reads
    these inside vectorised operations, and a lookup that could hit the
    database per row is the shape of the problem Stage 3 exists to avoid.

    :param directions: Processing code to ``CREDIT`` or ``DEBIT``. A code with
        no declared direction is absent, never defaulted.
    :param labels: Processing code to its human label.
    :param spend_eligible: Processing codes that count as spending.
    :param categories: Spending categories in display order.
    :param residual: The category an unmapped MCC falls into.
    :param mcc_categories: MCC to spending category.
    """

    directions: dict[str, str]
    labels: dict[str, str]
    spend_eligible: frozenset[str]
    categories: tuple[str, ...]
    residual: str
    mcc_categories: dict[str, str]

    def category_of(self, mcc: str | None) -> str:
        """
        :param mcc: The cleaned MCC, or None where the source carried none.
        :returns: Its spending category, or the residual when unmapped.
        """
        return self.mcc_categories.get(mcc or "", self.residual)


def from_json() -> Rules:
    """
    Builds the vocabularies from the rule files, with no database involved.

    This is what seeds the tables and what the parity test compares them
    against; it is also what lets the feature build run on a machine with no
    Postgres.

    :returns: The resolved rules.
    """
    categories = loader.spending_categories()
    return Rules(
        directions=dict(loader.processing_code_directions()),
        labels=dict(loader.processing_codes()),
        spend_eligible=frozenset(
            code
            for code, flag in loader.spend_eligible_codes().items()
            if flag
        ),
        categories=tuple(row["category"] for row in categories),
        residual=loader.residual_spending_category(),
        mcc_categories=dict(loader.mcc_categories()),
    )


def migrate(database: Database) -> None:
    """
    Applies ``sql/features_schema.sql``, creating the rule tables if absent.

    :param database: Where to connect.
    :raises ConfigError: If the schema file is missing.
    """
    from pathlib import Path

    path = Path(SCHEMA)
    if not path.exists():
        raise ConfigError(f"Cannot apply missing schema: {path}")

    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(path.read_text(encoding="utf-8"))


def seed(database: Database, rules: Rules | None = None) -> dict[str, int]:
    """
    Loads the rule files into the tables, replacing what is there.

    Delete-then-insert rather than upsert: these tables are a projection of
    the rule files, and a row the files no longer declare must not survive in
    the table where a build would still read it.

    :param database: Where to connect.
    :param rules: The vocabularies to write. Defaults to the rule files.
    :returns: Rows written per table.
    """
    rules = rules or from_json()
    eligible = rules.spend_eligible
    written: dict[str, int] = {}

    codes = [
        (code, label, rules.directions.get(code), code in eligible)
        for code, label in sorted(rules.labels.items())
    ]
    categories = [
        (category, order, category == rules.residual)
        for order, category in enumerate(rules.categories, start=1)
    ]
    mccs = sorted(rules.mcc_categories.items())

    with connect(database) as connection:
        with connection.cursor() as cursor:
            # Reverse dependency order: the MCC map references the category
            # vocabulary, so it has to go first and come back last.
            for table in reversed(TABLES):
                cursor.execute(f"DELETE FROM {table}")

            cursor.executemany(
                "INSERT INTO rule_processing_codes "
                "(code, label, direction, spend_eligible) "
                "VALUES (%s, %s, %s, %s)",
                codes,
            )
            written["rule_processing_codes"] = len(codes)

            cursor.executemany(
                "INSERT INTO rule_spending_categories "
                "(category, display_order, is_residual) VALUES (%s, %s, %s)",
                categories,
            )
            written["rule_spending_categories"] = len(categories)

            cursor.executemany(
                "INSERT INTO rule_mcc_categories (mcc, category) "
                "VALUES (%s, %s)",
                mccs,
            )
            written["rule_mcc_categories"] = len(mccs)

    return written


def from_database(database: Database) -> Rules:
    """
    Reads the vocabularies back out of the rule tables.

    :param database: Where to connect.
    :returns: The resolved rules.
    :raises ConfigError: If a table is empty, which means the seed never ran
        and is worth saying plainly rather than producing a feature table
        whose every spending column is zero.
    """
    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT code, label, direction, spend_eligible "
                "  FROM rule_processing_codes"
            )
            codes = cursor.fetchall()

            cursor.execute(
                "SELECT category, is_residual FROM rule_spending_categories "
                " ORDER BY display_order"
            )
            categories = cursor.fetchall()

            cursor.execute("SELECT mcc, category FROM rule_mcc_categories")
            mccs = cursor.fetchall()

    for table, rows in zip(TABLES, (codes, categories, mccs)):
        if not rows:
            raise ConfigError(
                f"{table} is empty. Run `make db-rules` to seed the Stage 3 "
                f"vocabularies from src/rules/json/."
            )

    residuals = [name for name, is_residual in categories if is_residual]
    if len(residuals) != 1:
        raise ConfigError(
            f"rule_spending_categories must declare exactly one residual, "
            f"found {len(residuals)}: {residuals}"
        )

    return Rules(
        directions={
            code: direction for code, _, direction, _ in codes if direction
        },
        labels={code: label for code, label, _, _ in codes},
        spend_eligible=frozenset(
            code for code, _, _, eligible in codes if eligible
        ),
        categories=tuple(name for name, _ in categories),
        residual=residuals[0],
        mcc_categories=dict(mccs),
    )
