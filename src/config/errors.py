"""The one exception every configuration problem surfaces as."""


class ConfigError(Exception):
    """
    A configuration file is missing, malformed, or internally inconsistent.

    Raised at startup, never mid-run. That is the whole point of loading
    eagerly: a typo in a YAML file fails in the first second with a message
    naming the file and the key, rather than surfacing as a ``None`` three
    steps into a pipeline that has already written half its output.
    """
