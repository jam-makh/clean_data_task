"""
Configuration, in the three kinds it actually comes in.

``policy``       -- tunable judgements: tolerances, thresholds, widths.
                    YAML, because each value carries an argument for itself
                    and YAML has real comments.
``fingerprint``  -- a hash over everything that shapes the output, so
                    "identical input, identical state" is checkable.

Domain vocabulary -- merchants, cities, date formats, FX -- is a different
kind again and stays where it is, in ``src/rules/json``. It is fact about the
world rather than judgement about how to treat it, and it is loaded through
``src.rules.loader``.

Runtime wiring -- database URL, Kafka brokers, credentials -- is the third
kind and does not exist yet. It arrives with the services it points at.
"""

from src.config.errors import ConfigError
from src.config.fingerprint import config_fingerprint, short_fingerprint
from src.config.policy import Policy, load

__all__ = [
    "ConfigError",
    "Policy",
    "config_fingerprint",
    "load",
    "short_fingerprint",
]
