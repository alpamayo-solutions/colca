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
