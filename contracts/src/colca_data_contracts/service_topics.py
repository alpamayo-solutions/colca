"""Where a service's own records live.

Standard library only, so code without an MQTT client can use the rule.
"""

from __future__ import annotations


def service_context(mount: str, service_name: str) -> tuple[str, ...]:
    """Where a service's own records live: its mount, then its name.

    Without the name, services sharing a mount, or several unplaced services
    on one node, would publish their retained records to the same topic and
    overwrite each other.
    """
    return (*(segment for segment in mount.split("/") if segment), service_name)


def service_state_topic(root: str, node_id: str, mount: str, service_name: str, key: str) -> str:
    """The topic of one key of a service's private state (``_ServiceState``).

    It sits at the service's own position (:func:`service_context`); only
    that service writes and reads it, colcad keeps it off the bus and does not
    replicate it. The payload is the service's JSON object with a positive
    integer ``version``; an empty payload retires the key. It is for a
    service's own checkpoints and open work, not for data other readers need.
    """
    if not key.strip("/"):
        raise ValueError("a service state key needs at least one segment")
    position = "/".join(service_context(mount, service_name))
    return f"{root}/v1/_ServiceState/{node_id}/{position}/{key.strip('/')}"
