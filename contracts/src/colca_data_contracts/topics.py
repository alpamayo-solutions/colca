"""Building Colca topics under the publishing node's own identity.

franzmq 0.5.0 puts the publisher's identity at level 4 of every v1 topic
(``colca/v1/_Contract/{node_id}/{path…}``). On a node, every service publishes
under the *same* identity — the node's — so each one reading ``NODE_ID`` and
threading it through its own topic construction would be the same three lines
repeated per service, with a different failure message each time it is missing.

This module is that code, once. Topic knowledge itself stays in franzmq; nothing
here is added to the payload classes.
"""
from typing import Optional, Union

from decouple import config
from franzmq.data_contracts.base import Payload
from franzmq.topic import Topic

#: True when the installed franzmq speaks the node-id topic scheme (>= 0.5.0).
#: Services migrate one at a time — their franzmq pin is the switch
#: (schema-bundle design §9.2) — so both are installable while the fleet moves.
TOPICS_CARRY_NODE_ID = "node_id" in getattr(Topic, "__dataclass_fields__", {})


def node_id() -> str:
    """The identity this process publishes under, from the ``NODE_ID`` environment.

    Raises ``RuntimeError`` naming the variable when it is unset: a topic
    published under the wrong identity is rejected by the broker's identity
    rule, so guessing a default would trade a startup error for a silent
    ingest failure.
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
    *path: Union[str, int],
    node: Optional[str] = None,
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
