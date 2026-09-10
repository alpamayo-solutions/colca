"""The topic root: the first segment of every Colca topic.

Every node, service and SDK process of one tree uses the same root. It is
``colca`` unless ``COLCA_TOPIC_ROOT`` names another. The value is read each time
it is asked for, so a process started with a different environment, or a test
that overrides it, gets the root it asked for.
"""
import os
import re

DEFAULT_ROOT = "colca"
ROOT_ENV = "COLCA_TOPIC_ROOT"
VERSION = "v1"

# One topic segment: no MQTT wildcards, not the reserved "$" space, and not the
# leading "_" that marks a contract.
_VALID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def topic_root() -> str:
    """The configured root, or ``colca``. Raises ``ValueError`` for an invalid value."""
    value = os.environ.get(ROOT_ENV, "")
    if not value:
        return DEFAULT_ROOT
    if not _VALID.fullmatch(value):
        raise ValueError(
            f"{ROOT_ENV}={value!r} is not a topic root: one segment of at most 64 letters, "
            "digits, '-', '_' and '.', starting with a letter or digit"
        )
    return value


def topic_prefix() -> str:
    """How every topic starts: the root and the version, for example ``colca/v1/``."""
    return f"{topic_root()}/{VERSION}/"
