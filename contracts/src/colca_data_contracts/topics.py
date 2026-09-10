"""Building Colca topics under the publishing node's identity.

The publisher's identity is level 4 of every v1 topic
(``colca/v1/_Contract/{node_id}/{path…}``), and every service on a node
publishes under the node's. These helpers read ``NODE_ID`` for all of them;
the topic grammar itself stays in franzmq.
"""

from decouple import config
from franzmq.data_contracts.base import Payload
from franzmq.topic import Topic

#: True when the installed franzmq uses the node-id topic scheme (>= 0.5.0).
TOPICS_CARRY_NODE_ID = "node_id" in getattr(Topic, "__dataclass_fields__", {})


def node_id() -> str:
    """The identity this process publishes under, from ``NODE_ID``.

    Raises ``RuntimeError`` when it is unset: the broker rejects records under
    a wrong identity, so there is no safe default.
    """
    value = config("NODE_ID", default="")
    if not value:
        raise RuntimeError(
            "NODE_ID is not set. Colca topics carry the publishing node's identity at "
            "level 4 (colca/v1/_Contract/{node_id}/{path…}); the broker matches it against "
            "the authenticated client, so it cannot be defaulted."
        )
    return value


def node_topic(
    payload_type: type[Payload],
    *path: str | int,
    node: str | None = None,
) -> Topic:
    """Build ``colca/v1/_{payload_type}/{node}/{path…}``.

    ``node`` defaults to :func:`node_id`. ``path`` segments are joined as topic
    levels; a single pre-joined ``"a/b/c"`` string works too, since franzmq
    renders the context with ``/`` separators either way.
    """
    if not TOPICS_CARRY_NODE_ID:
        raise RuntimeError(
            "The installed franzmq predates the node-id topic scheme (0.5.0). "
            "node_topic() cannot build a topic it would not render; pin franzmq==0.5.0 "
            "in this service to migrate it, or use franzmq.Topic directly to stay on the "
            "old scheme."
        )
    return Topic(
        payload_type=payload_type,
        node_id=node if node is not None else node_id(),
        context=tuple(str(segment) for segment in path),
    )
