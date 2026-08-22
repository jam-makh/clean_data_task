"""
The configuration layer: that it loads, that it validates, and that the
fingerprint means what it claims.

The validation tests matter more than they look. The point of moving these
values out of code was not tidiness -- it was that a wrong value should be
caught at startup with a message naming the key, instead of silently changing
what the pipeline flags. A config loader that accepts a negative tolerance has
given up the only thing it was for.
"""

import copy

import pytest
import yaml

from src.config import policy as policy_module
from src.config import runtime as runtime_module
from src.config.errors import ConfigError
from src.config.fingerprint import (
    FINGERPRINTED,
    config_fingerprint,
    short_fingerprint,
)


@pytest.fixture
def raw_policy():
    """:returns: The real policy file, parsed, as a mutable dict."""
    return yaml.safe_load(
        policy_module.DEFAULT_PATH.read_text(encoding="utf-8")
    )


def test_the_shipped_policy_file_loads(raw_policy):
    """The file in the repo must be valid, not merely well-formed YAML."""
    parsed = policy_module.parse(raw_policy)
    assert parsed.fx.reconcile_tolerance > 0
    assert parsed.validation.required_columns
    assert parsed.duplicates.business_keys


def test_policy_is_frozen():
    """
    Every step reads one policy object, and in Stage 2 it is broadcast to
    Spark executors. A step that could mutate it would change the rules for
    every step after it, in a way no audit trail would record.
    """
    loaded = policy_module.load()
    with pytest.raises(Exception):
        loaded.fx.reconcile_tolerance = 0.5


def test_business_keys_are_tuples_not_lists():
    """
    Hashability is not cosmetic here: a frozen dataclass holding a list is
    still mutable through that list, and the broadcast in Stage 2 needs the
    whole object to be safe to share.
    """
    keys = policy_module.load().duplicates.business_keys
    assert isinstance(keys, tuple)
    assert all(isinstance(group, tuple) for group in keys)


@pytest.mark.parametrize(
    "section,key",
    [
        ("fx", "reconcile_tolerance"),
        ("fx", "reference_tolerance"),
        ("codes", "processing_code_width"),
        ("codes", "mcc_width"),
        ("missing", "auth_repeat_threshold"),
    ],
)
def test_a_missing_key_is_named_in_the_error(raw_policy, section, key):
    """The error has to say which key, or it is no better than a traceback."""
    broken = copy.deepcopy(raw_policy)
    del broken[section][key]
    with pytest.raises(ConfigError, match=f"{section}.{key}"):
        policy_module.parse(broken)


@pytest.mark.parametrize("bad", [0, -1, -0.5])
def test_a_non_positive_tolerance_is_rejected(raw_policy, bad):
    """
    Neither degenerate value announces itself in the output: a zero tolerance
    flags every row and a negative one flags none. Both look like a working
    pipeline until someone reads the numbers.
    """
    broken = copy.deepcopy(raw_policy)
    broken["fx"]["reconcile_tolerance"] = bad
    with pytest.raises(ConfigError, match="must be > 0"):
        policy_module.parse(broken)


def test_a_non_numeric_tolerance_is_rejected(raw_policy):
    """A quoted number in YAML is a string, and a common way to break this."""
    broken = copy.deepcopy(raw_policy)
    broken["fx"]["reconcile_tolerance"] = "0.01"
    with pytest.raises(ConfigError, match="must be a number"):
        policy_module.parse(broken)


def test_an_empty_business_key_group_is_rejected(raw_policy):
    """An empty key group would match every row against every other row."""
    broken = copy.deepcopy(raw_policy)
    broken["duplicates"]["business_keys"] = [[]]
    with pytest.raises(ConfigError, match="business_keys"):
        policy_module.parse(broken)


def test_a_missing_section_is_rejected(raw_policy):
    broken = copy.deepcopy(raw_policy)
    del broken["fx"]
    with pytest.raises(ConfigError, match="'fx'"):
        policy_module.parse(broken)


def test_a_missing_file_names_the_path():
    with pytest.raises(ConfigError, match="not found"):
        policy_module.load("config/does-not-exist.yaml")


def test_runtime_config_loads():
    paths = runtime_module.load().paths
    assert paths.source.suffix == ".xlsx"
    assert paths.output.suffix == ".xlsx"


def test_runtime_rejects_a_missing_path_key():
    with pytest.raises(ConfigError, match="paths.output"):
        runtime_module.parse({"paths": {"source": "a.xlsx"}})


def test_fingerprint_is_stable_across_calls():
    """Two calls in one process must agree, or nothing downstream can."""
    assert config_fingerprint() == config_fingerprint()
    assert len(config_fingerprint()) == 64
    assert short_fingerprint() == config_fingerprint()[:12]


def test_fingerprint_ignores_formatting_but_not_meaning(tmp_path):
    """
    The reason contents are canonicalised rather than hashed raw: on Windows,
    a checkout can rewrite line endings, and a reflowed comment is not a rule
    change. Neither may move the fingerprint. A changed *value* must.
    """
    original = tmp_path / "a.json"
    reformatted = tmp_path / "a.json"

    original.write_text('{"b": 2, "a": 1}', encoding="utf-8")
    first = config_fingerprint((original,))

    reformatted.write_text(
        '{\n  "a": 1,\n\n  "b": 2\n}\r\n', encoding="utf-8"
    )
    assert config_fingerprint((reformatted,)) == first

    reformatted.write_text('{"a": 1, "b": 3}', encoding="utf-8")
    assert config_fingerprint((reformatted,)) != first


def test_fingerprint_covers_policy_and_rules_but_not_runtime():
    """
    The distinction the whole idea rests on. Policy and vocabulary change what
    the pipeline computes, so they are fingerprinted. Runtime wiring changes
    only where it runs -- if pipeline.yaml were included, moving the output
    directory would read as a rule change and break the Stage 2 idempotency
    check for no reason.
    """
    covered = {path.as_posix() for path in FINGERPRINTED}
    assert "config/policy.yaml" in covered
    assert "src/rules/json/merchants.json" in covered
    assert "src/rules/json/trap_pairs.json" in covered
    assert "config/pipeline.yaml" not in covered


def test_every_fingerprinted_file_exists():
    """
    A path that has drifted would raise mid-run at exactly the moment the
    fingerprint is needed, which is after the cleaning work is done.
    """
    for path in FINGERPRINTED:
        assert path.exists(), f"fingerprinted file is missing: {path}"
