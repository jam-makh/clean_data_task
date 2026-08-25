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
from pathlib import Path

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
    """
    The suffixes are asserted as a set rather than a literal: the configured
    source moved from the v4 workbook to the forecast CSV when Spark became
    the default engine, and a test pinned to one extension would have failed
    on a change of file rather than a change of meaning. What has to hold is
    that the reader understands it -- see src/utils/io.py.
    """
    paths = runtime_module.load().paths
    assert paths.source.suffix in {".xlsx", ".csv", ".tsv", ".txt"}
    assert paths.output.suffix in {".xlsx", ".csv"}


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


# --- Stage 2: engine selection and database wiring -------------------------
#
# Both are read on every Spark run, and both fail in ways that look like data
# problems rather than configuration ones: a mistyped engine would silently
# run the other half of the project, and a batch_size of 1 would turn one
# round trip into 265,195 of them and look only like slowness.


def test_the_engine_defaults_to_spark_when_absent():
    parsed = runtime_module.parse({"paths": {"source": "a.csv", "output": "b.xlsx"}})

    assert parsed.engine == "spark"
    assert parsed.database.enabled is True


@pytest.mark.parametrize("engine", runtime_module.ENGINES)
def test_each_configured_engine_is_accepted(engine):
    parsed = runtime_module.parse(
        {"paths": {"source": "a.csv", "output": "b.xlsx"}, "engine": engine}
    )

    assert parsed.engine == engine


def test_an_unknown_engine_names_the_ones_that_exist():
    """
    Silently defaulting would run the wrong engine and produce the wrong kind
    of output, with nothing in the run to say so.
    """
    with pytest.raises(ConfigError) as raised:
        runtime_module.parse(
            {"paths": {"source": "a.csv", "output": "b.xlsx"}, "engine": "sparl"}
        )

    assert "spark" in str(raised.value)
    assert "pandas" in str(raised.value)


@pytest.mark.parametrize("bad", [0, -1, "10000", 1.5])
def test_a_bad_batch_size_is_rejected(bad):
    with pytest.raises(ConfigError, match="batch_size"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "database": {"batch_size": bad},
        })


def test_a_yaml_boolean_batch_size_is_rejected():
    """
    `batch_size: yes` parses to True in YAML, and `isinstance(True, int)` is
    True in Python -- so without an explicit check it would become a batch
    size of 1 and merely look slow.
    """
    with pytest.raises(ConfigError, match="batch_size"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "database": {"batch_size": True},
        })


def test_a_non_boolean_enabled_is_rejected():
    with pytest.raises(ConfigError, match="database.enabled"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "database": {"enabled": "yes please"},
        })


def test_the_shipped_config_selects_spark_and_writes():
    """
    What `make run` actually does, asserted rather than assumed -- this is the
    one place the Stage 2 default is stated.
    """
    config = runtime_module.load()

    assert config.engine == "spark"
    assert config.database.enabled is True
    assert config.database.batch_size >= 1000


# --- Stage 2: the event contract -------------------------------------------


def test_kafka_defaults_when_the_section_is_absent():
    parsed = runtime_module.parse({"paths": {"source": "a.csv", "output": "b.xlsx"}})

    assert parsed.kafka.enabled is True
    assert parsed.kafka.topic.endswith(".v1")
    assert parsed.kafka.replication_factor == 1


def test_the_shipped_topics_are_the_ones_the_preflight_check_expects():
    """
    ``scripts/verify_env.py`` hardcodes the topic names, deliberately -- it has
    to report a broken environment while the environment is broken, so it
    imports nothing from the project. That duplication is only safe if
    something asserts the two agree.

    Both names, because the stack is only half up with one of them: the
    completion event has nowhere to go without the first, and the cleaning
    consumer subscribes to a topic that does not exist -- which is not an
    error, it is a consumer that polls forever and reports nothing wrong.
    """
    import re

    source = Path("scripts/verify_env.py").read_text(encoding="utf-8")
    block = re.search(r"EXPECTED_TOPICS = \(([^)]+)\)", source).group(1)
    expected = re.findall(r'"([^"]+)"', block)
    kafka = runtime_module.load().kafka

    assert expected, "the preflight check names no topics"
    assert set(expected) == {kafka.topic, kafka.raw_topic}


def test_the_two_topics_cannot_be_the_same_place():
    """
    Pointing both at one topic would feed the consumer its own completion
    events. They decode as an unknown event type and are skipped, so the
    symptom is not an error -- it is a consumer quietly doing half the work it
    appears to.
    """
    with pytest.raises(ConfigError, match="cannot share a topic"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"topic": "one.v1", "raw_topic": "one.v1"},
        })


@pytest.mark.parametrize("key", ["topic", "raw_topic"])
@pytest.mark.parametrize("bad", ["", "   ", 5, None])
def test_a_bad_topic_name_is_rejected_by_name(key, bad):
    with pytest.raises(ConfigError, match=f"kafka.{key}"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {key: bad},
        })


def test_consumer_defaults_when_the_section_is_absent():
    parsed = runtime_module.parse({"paths": {"source": "a.csv", "output": "b.xlsx"}})

    assert parsed.kafka.consumer.group_id
    assert parsed.kafka.consumer.auto_offset_reset == "earliest"
    assert parsed.kafka.consumer.batch_size >= 1


def test_the_shipped_poll_interval_allows_for_a_slow_spark_batch():
    """
    Kafka's own default is 300 seconds and a cold Spark batch has been
    measured well past that on this machine. Exceeding it makes the broker
    revoke the partitions mid-batch, fail the commit, and redeliver the work
    to a consumer that will take just as long -- a livelock rather than a slow
    run, and one that looks like a Kafka problem rather than a timeout.
    """
    assert runtime_module.load().kafka.consumer.max_poll_interval > 300


@pytest.mark.parametrize("bad", ["", "   ", 5, None])
def test_a_bad_consumer_group_is_rejected(bad):
    with pytest.raises(ConfigError, match="group_id"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"consumer": {"group_id": bad}},
        })


@pytest.mark.parametrize("bad", ["newest", "beginning", "", 1, None])
def test_an_unknown_offset_reset_is_rejected(bad):
    """
    The two Kafka accepts, and no others. A typo here does not fail loudly at
    the broker -- the client rejects the config at construction with a message
    about a property name, which is a long way from "you meant earliest".
    """
    with pytest.raises(ConfigError, match="auto_offset_reset"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"consumer": {"auto_offset_reset": bad}},
        })


@pytest.mark.parametrize("bad", [0, -1, "1", None, True])
def test_a_bad_poll_timeout_is_rejected(bad):
    """
    Zero is the interesting one: it is a number, it is not negative, and it
    would spin the poll loop at full speed doing nothing.
    """
    with pytest.raises(ConfigError, match="poll_timeout"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"consumer": {"poll_timeout": bad}},
        })


def test_a_fractional_poll_timeout_is_allowed():
    """
    Unlike every other number in this section, this one is seconds of waiting
    and half a second is a reasonable thing to ask for.
    """
    parsed = runtime_module.parse({
        "paths": {"source": "a.csv", "output": "b.xlsx"},
        "kafka": {"consumer": {"poll_timeout": 0.5}},
    })

    assert parsed.kafka.consumer.poll_timeout == 0.5


@pytest.mark.parametrize("key", ["batch_size", "max_poll_interval"])
@pytest.mark.parametrize("bad", [0, -1, "1", 1.5, True])
def test_a_bad_consumer_number_is_rejected(key, bad):
    with pytest.raises(ConfigError, match=key):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"consumer": {key: bad}},
        })


def test_a_consumer_section_that_is_not_a_mapping_is_rejected():
    with pytest.raises(ConfigError, match="kafka.consumer"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"consumer": "cleaning-consumer"},
        })


@pytest.mark.parametrize("key", ["partitions", "replication_factor", "delivery_timeout"])
@pytest.mark.parametrize("bad", [0, -1, "1", 1.5, True])
def test_a_bad_kafka_number_is_rejected(key, bad):
    with pytest.raises(ConfigError, match=key):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {key: bad},
        })


def test_an_empty_topic_is_rejected():
    """
    Auto-create is off, so an empty or whitespace topic would fail at publish
    time against a broker rather than at load time against the file.
    """
    with pytest.raises(ConfigError, match="kafka.topic"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"topic": "   "},
        })


def test_a_non_boolean_kafka_enabled_is_rejected():
    with pytest.raises(ConfigError, match="kafka.enabled"):
        runtime_module.parse({
            "paths": {"source": "a.csv", "output": "b.xlsx"},
            "kafka": {"enabled": "off"},
        })
