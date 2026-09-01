"""
A stable hash over everything that shapes a run's output.

Stage 2 requires that identical input leaves the system in an identical state.
That claim is only true of ``(input, config)`` together: the same workbook run
against an edited merchant master legitimately produces different rows, and an
idempotency check that ignores configuration will call that a violation, or
worse, will not notice it at all.

So the resolved configuration is hashed, and the hash travels with the output
-- into the database rows, into the emitted event, into the audit trail. Then
"same state" becomes a statement that can be checked instead of assumed, and a
consumer can tell that a replayed event came from a different generation of
the rules.
"""

import hashlib
import json
from pathlib import Path

from src.config_readers.errors import ConfigError

# Every file whose contents can change the cleaned output. Rule vocabularies
# and the policy file both qualify; runtime wiring does not, because pointing
# the pipeline at a different database does not change what it computes.
FINGERPRINTED = (
    Path("config/policy.yaml"),
    Path("src/rules/json/city_aliases.json"),
    Path("src/rules/json/city_countries.json"),
    Path("src/rules/json/currencies.json"),
    Path("src/rules/json/date_formats.json"),
    Path("src/rules/json/fx_rates.json"),
    Path("src/rules/json/mcc_rules.json"),
    Path("src/rules/json/merchants.json"),
    Path("src/rules/json/processing_codes.json"),
    Path("src/rules/json/processors.json"),
    Path("src/rules/json/trap_pairs.json"),
)


def _canonical(path: Path) -> str:
    """
    Renders one config file as a byte string whose value depends only on the
    meaning of its contents.

    Hashing raw bytes would make the fingerprint change when someone reflows a
    comment or converts line endings -- which on Windows happens on checkout.
    JSON is reparsed and re-serialised with sorted keys; YAML is normalised the
    same way through JSON. Two files that say the same thing hash the same.

    :param path: File to render.
    :returns: Canonical text for hashing.
    :raises ConfigError: If the file is absent or cannot be parsed.
    """
    if not path.exists():
        raise ConfigError(f"Cannot fingerprint missing file: {path}")

    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        loaded = json.loads(text)
    else:
        import yaml

        loaded = yaml.safe_load(text)
    return json.dumps(loaded, sort_keys=True, separators=(",", ":"))


def config_fingerprint(files: tuple[Path, ...] = FINGERPRINTED) -> str:
    """
    :param files: Config files to include, in a fixed order.
    :returns: A 64-character hex sha256 over the canonicalised contents.
    :raises ConfigError: If any file is missing or unparseable.
    """
    digest = hashlib.sha256()
    for path in sorted(files):
        # The path is hashed alongside the contents so that renaming a rule
        # file registers as a change, and so two files swapping contents do
        # not collide.
        digest.update(path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(_canonical(path).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def short_fingerprint(files: tuple[Path, ...] = FINGERPRINTED) -> str:
    """
    :returns: The first 12 characters of the fingerprint, for log lines.

    Twelve hex characters is ~48 bits, which is plenty to tell two generations
    of a config apart by eye. The full value is what gets stored.
    """
    return config_fingerprint(files)[:12]
