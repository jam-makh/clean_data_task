"""
City normalization, country resolution, and the card-not-present flag.

Four lookups against three small tables, and the only thing worth saying
about the port is which of them is allowed to be blank.

``MERCHANT_COUNTRY_EXPECTED`` is what the city implies, and it is the empty
string -- not null and not UNKNOWN -- wherever the city implies nothing. The
distinction is load-bearing two stages later: ``ConsistencyValidator`` raises
GEO_CITY_COUNTRY_MISMATCH only where an expectation exists, so a blank that
became UNKNOWN would flag every e-commerce row in the file as a geography
error. ``MERCHANT_COUNTRY_CLEANED``, the column a reader actually sees, is
the opposite -- it is never blank, because a blank cell reads as an oversight
where UNKNOWN reads as a fact that was checked.
"""

from pyspark.sql import functions as F

from src.cleaners.geo import LOCATION_TYPES, UNKNOWN
from src.rules import loader
from src.spark import audit
from src.spark.spark_utils import chain, lookup, one_of, text


def apply(frame, policy):
    """
    Collapses transliteration variants and e-commerce markers to one spelling,
    then resolves the country the city sits in.

    :param frame: Frame; ``HAS_TERMINAL`` is read when ``missing`` has run.
    :param policy: Unused -- every table this stage reads is vocabulary.
    :returns: The frame with the city, the country, and the two readings of
        the city that later stages ask for.
    """
    if "MERCHANT_CITY" not in frame.columns:
        return frame

    aliases, ecommerce = loader.city_aliases()
    markers = loader.non_geographic_cities()
    countries = loader.city_countries()

    raw = F.upper(text("MERCHANT_CITY"))
    # A blank city is UNKNOWN; anything else is its canonical spelling, or
    # itself where the alias table has never seen it.
    frame = frame.withColumn(
        "MERCHANT_CITY_CLEANED",
        F.when(raw == "", F.lit(UNKNOWN)).otherwise(
            lookup(raw, aliases, raw)
        ),
    )

    city = F.col("MERCHANT_CITY_CLEANED")
    frame = frame.withColumn(
        "LOCATION_TYPE",
        F.when(city == F.lit(UNKNOWN), F.lit(UNKNOWN)).otherwise(
            lookup(city, markers, F.lit("PHYSICAL"))
        ),
    )

    online = one_of(city, ecommerce)
    if "HAS_TERMINAL" in frame.columns:
        online = online | ~F.col("HAS_TERMINAL")
    frame = frame.withColumn("IS_ECOMMERCE", online)

    place = F.col("LOCATION_TYPE")
    frame = frame.withColumn(
        # What the city implies, kept apart from what the file states so the
        # two can still be compared. Blank for markers and unknown cities:
        # they carry no geography, and "no expectation" must never read as
        # "mismatch".
        "MERCHANT_COUNTRY_EXPECTED",
        F.when(place != F.lit("PHYSICAL"), F.lit("")).otherwise(
            lookup(city, countries, F.lit(""))
        ),
    )

    stated = (
        F.upper(text("MERCHANT_COUNTRY"))
        if "MERCHANT_COUNTRY" in frame.columns
        else F.lit("")
    )
    expected = F.col("MERCHANT_COUNTRY_EXPECTED")
    return frame.withColumn(
        # ``expected or stated or UNKNOWN``, and the empty string is what
        # "or" falls through on in Python -- so the test here is against ""
        # rather than against null, which is a different question and would
        # publish a blank country.
        "MERCHANT_COUNTRY_CLEANED",
        chain(
            [(expected != "", expected), (stated != "", stated)],
            otherwise=F.lit(UNKNOWN),
        ),
    )


def metrics(frame, policy):
    """
    How many spellings collapsed, and how much geography survived.

    This stage adds no diagnostic column of its own, because it already had
    four. ``LOCATION_TYPE``, ``IS_ECOMMERCE``, ``MERCHANT_COUNTRY_EXPECTED``
    and the cleaned city each state per row what the totals below are totals
    of, and ``MERCHANT_CITY`` is a raw column no stage overwrites -- so the
    count of distinct spellings before cleaning is still readable at the end
    of the run.

    :param frame: The frame as the last stage left it.
    :param policy: Unused; every table this stage reads is vocabulary.
    :returns: ``(metric, request)`` pairs in report order.
    """
    if "MERCHANT_CITY_CLEANED" not in frame.columns:
        return []

    city = F.col("MERCHANT_CITY_CLEANED")
    kind = F.col("LOCATION_TYPE")
    out = [
        (
            "cities_distinct_before",
            audit.distinct(F.upper(text("MERCHANT_CITY"))),
        ),
        ("cities_distinct_after", audit.distinct(city)),
        ("city.unknown", audit.rows(city == F.lit(UNKNOWN))),
        ("ecommerce_rows", audit.rows(F.col("IS_ECOMMERCE"))),
    ]

    for value in LOCATION_TYPES:
        out.append((
            f"location_type.{value.lower()}",
            audit.rows(kind == F.lit(value), nonzero=True),
        ))

    out.append((
        "city.not_in_country_reference",
        audit.rows(
            (kind == F.lit("PHYSICAL"))
            & (F.col("MERCHANT_COUNTRY_EXPECTED") == "")
        ),
    ))
    out.append((
        "country.unknown",
        audit.rows(F.col("MERCHANT_COUNTRY_CLEANED") == F.lit(UNKNOWN)),
    ))
    return out
