"""Narrow client helpers for a service inside one Colca deployment network.

The local Colca doors use network placement as their trust boundary. A service
names itself, but carries no token, password, client certificate, or TLS mode.
This module deliberately covers only the two bootstrap operations shared by
local publishers: resolve ``GET /self`` and connect MQTT v5.
"""

from __future__ import annotations

import datetime
import json
import logging
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

import paho.mqtt.client as pahomqtt
from franzmq import Client, Topic
from franzmq.data_contracts.base import Payload
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from colca_data_contracts.payload import ServiceDetails
from colca_data_contracts.service_topics import service_context


class _LogPublishingHandler(logging.Handler):
    """Turn a log record into a `_Log` at this service's position in the tree.

    franzmq's MQTTHandler publishes at the node root, which a placed service
    has no write grant for. The record goes where the service's other records
    go instead: its mount, then its name (`service_context`). The Python
    logger name stays in the payload.
    """

    def __init__(self, client: Client, context: tuple[str, ...]):
        super().__init__()
        self.client = client
        self.context = tuple(context)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            from franzmq import Topic
            from franzmq.data_contracts.base import Log

            payload = Log(
                timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
                level=record.levelname,
                message=self.format(record),
                logger_name=record.name,
                module=record.module,
                function=record.funcName,
                line_no=record.lineno,
                exc_info=(
                    self.formatter.formatException(record.exc_info) if record.exc_info and self.formatter else None
                ),
                extra=getattr(record, "extra", None),
            )
            topic = Topic(
                payload_type=Log,
                node_id=self.client.require_node_id(),
                context=(*self.context, record.levelname),
            )
            self.client.publish(topic, payload)
        except Exception:
            self.handleError(record)


def attach_log_publisher(client: Client, context: tuple[str, ...]) -> None:
    """Publish this service's log records as ``_Log``, without taking over logging.

    `connect_local_mqtt` calls this for you; a service with its own client
    calls it directly. `context` is the service's position, from
    `service_context(mount, name)`.

    The handler is added to whatever logging the service configured, with the
    shared Colca format. franzmq's ``configure_mqtt_logger`` is not used
    because it replaces the root logger's handlers and level.
    """
    from colca_data_contracts.logging import COLCA_LOG_FORMAT

    handler = _LogPublishingHandler(client, context)
    handler.setFormatter(logging.Formatter(COLCA_LOG_FORMAT))
    logging.getLogger().addHandler(handler)


@dataclass(frozen=True)
class LocalServiceIdentity:
    service_id: str
    service_name: str
    node_id: str
    system_element_id: str
    mount: str

    @property
    def hierarchy(self) -> tuple[str, ...]:
        return service_context(self.mount, self.service_name)


def resolve_local_identity(
    service_name: str,
    *,
    host: str = "colca",
    http_port: int = 80,
    mount: str = "",
    timeout: float = 10.0,
) -> LocalServiceIdentity:
    """Resolve the service entry and current placement from Colca's local door."""

    request = urllib.request.Request(
        f"http://{host}:{http_port}/self",
        headers={
            "X-Colca-Service": service_name,
            "X-Colca-Mount": mount,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - always http://  # nosec B310
        payload = json.load(response)

    required = ("ulid", "name", "node", "element", "mount")
    if any(not isinstance(payload.get(field), str) for field in required):
        raise RuntimeError("Colca /self returned an invalid local service identity")
    if payload["name"] != service_name:
        raise RuntimeError(f"Colca /self resolved service {payload['name']!r}, expected {service_name!r}")
    if not payload["ulid"] or not payload["node"]:
        raise RuntimeError("Colca /self omitted the service or node identity")

    return LocalServiceIdentity(
        service_id=payload["ulid"],
        service_name=payload["name"],
        node_id=payload["node"],
        system_element_id=payload["element"],
        mount=payload["mount"],
    )


def connect_local_mqtt(
    service_name: str,
    *,
    host: str = "colca",
    mqtt_port: int = 1883,
    http_port: int = 80,
    mount: str = "",
    client_id: str | None = None,
    on_connect: Callable | None = None,
    on_disconnect: Callable | None = None,
    identity: LocalServiceIdentity | None = None,
    publish_logs: bool = True,
    will: tuple[Topic, Payload] | None = None,
    max_queued_messages: int = 0,
) -> tuple[Client, LocalServiceIdentity]:
    """Connect to local Colca MQTT without credentials or client TLS.

    ``publish_logs`` attaches the ``_Log`` publisher; turn it off if you
    manage that yourself. ``will`` is a ``(topic, payload)`` pair the broker
    publishes retained if the connection drops without a clean DISCONNECT.
    """

    resolved = identity or resolve_local_identity(
        service_name,
        host=host,
        http_port=http_port,
        mount=mount,
    )
    client = Client(client_id=client_id or service_name, protocol=pahomqtt.MQTTv5)
    if max_queued_messages:
        client.max_queued_messages_set(max_queued_messages)
    client.node_id = resolved.node_id
    client.username_pw_set(service_name)
    if publish_logs:
        attach_log_publisher(client, resolved.hierarchy)
    client.reconnect_on_failure = True
    client.reconnect_on_offline = True
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    if on_connect is not None:
        client.on_connect = on_connect
    if on_disconnect is not None:
        client.on_disconnect = on_disconnect
    if will is not None:
        will_topic, will_payload = will
        client.will_set(str(will_topic), will_payload.encode(), qos=1, retain=True)

    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 0xFFFFFFFF
    properties.UserProperty = ("mount", mount)
    client.connect(
        host=host,
        port=mqtt_port,
        clean_start=False,
        properties=properties,
    )
    return client, resolved


def service_details_topic(identity: LocalServiceIdentity) -> Topic:
    """Return the reserved observed-service topic at the assigned mount."""

    return Topic(
        payload_type=ServiceDetails,
        node_id=identity.node_id,
        context=(*identity.hierarchy, "_service"),
    )


def publish_local_service_details(
    client: Client,
    identity: LocalServiceIdentity,
    details: ServiceDetails,
) -> None:
    """Publish one retained service record, pinned to the resolved identity."""

    if details.id != identity.service_id or details.colca_node_id != identity.node_id:
        raise ValueError("service details must describe the resolved local identity")
    client.publish(service_details_topic(identity), details, qos=1, retain=True)
