"""Where a service's own records live.

Its own module, and stdlib-only, because everything that needs this rule must
be able to reach it: a Python service that publishes its own registration, the
connector, and a test suite that stubs out MQTT entirely. Sitting next to a
paho import — as this did — means a caller has to drag an MQTT client in to
ask a question about a string, and the answer to that is always a second copy
of the rule somewhere else.

"""

from __future__ import annotations


def service_context(mount: str, service_name: str) -> tuple[str, ...]:
    """Where a service's own records live: its mount, then its name.

    The name segment is not decoration. Without it every service sharing a
    mount shares one address — and an UNPLACED service has no mount at all, so
    on a node running several of them (a projector, an audit writer, a UI, a
    dataops) they would all publish their retained `_ServiceDetails` to the
    same topic and erase each other. Whichever wrote last would be the only
    service the node appeared to have; the rest would lose their projected
    row, and with it any catalogue that names one (`_DataTags.connector`),
    which then parks forever waiting for a service that will never come back.

    This is the "Position in topics" rule — a local
    service's name is appended as the final segment of its service-owned
    records — and it is stated HERE so the connector and every other local
    service cannot each keep their own version of it.
    """
    return tuple(segment for segment in mount.split("/") if segment) + (service_name,)
