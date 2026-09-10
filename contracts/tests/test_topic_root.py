"""The topic root is configurable, and every topic this package builds follows it."""

import pytest

from colca_data_contracts import Metric, node_topic, topic_prefix, topic_root


def test_the_root_defaults_to_colca(monkeypatch):
    monkeypatch.delenv("COLCA_TOPIC_ROOT", raising=False)
    assert topic_root() == "colca"
    assert topic_prefix() == "colca/v1/"


def test_a_configured_root_starts_every_topic(monkeypatch):
    monkeypatch.setenv("COLCA_TOPIC_ROOT", "acme")
    monkeypatch.setenv("NODE_ID", "n-edge1")
    assert str(node_topic(Metric, "line1", "temp")) == "acme/v1/_Metric/n-edge1/line1/temp"


def test_an_explicit_prefix_is_left_alone(monkeypatch):
    monkeypatch.setenv("COLCA_TOPIC_ROOT", "acme")
    from franzmq.topic import Topic

    assert Topic(prefix="other", payload_type=Metric, node_id="n1", context=("a",)).prefix == "other"


@pytest.mark.parametrize("bad", ["-x", "_x", "a/b", "#", "+", "$SYS", "a b", "x" * 65])
def test_an_invalid_root_is_refused(monkeypatch, bad):
    monkeypatch.setenv("COLCA_TOPIC_ROOT", bad)
    with pytest.raises(ValueError):
        topic_root()
