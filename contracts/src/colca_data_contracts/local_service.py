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
from dataclasses import dataclass
from typing import Callable, Optional

import paho.mqtt.client as pahomqtt
from franzmq import Client, Topic
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from colca_data_contracts.payload import ServiceDetails
from colca_data_contracts.service_topics import service_context


class _LogPublishingHandler(logging.Handler):
    """Turn a log record into a `_Log` at THIS SERVICE'S position in the tree.

    franzmq's own MQTTHandler addresses a record as
    ``_Log/{node}/{logger}/{LEVEL}`` -- flat at the node root, with no
    position in it. A service is authorized to write its own subtree, so that
    topic is outside the scope of every PLACED service: colcad refused each
    one with `no write scope covers colca/v1/_Log/...`, and the only publisher
    that got through was dataops, which is unplaced and therefore scoped to
    the whole node. Eleven connectors published nothing and said nothing
    about it, because a logging handler that reports its own failure through
    logging is a loop.

    So the record goes where the service's other records go -- its mount, then
    its name, which is the rule `service_context` states and what `_DataTags`
    and `_ServiceDetails` already do. Being authorized is the point; carrying
    the service's position is the bonus, and the reader shows it as the path.

    The Python logger's own name (`dataops.ingest`) stays in the PAYLOAD,
    where it is not competing with the address for the same segment.
    """

    def __init__(self, client: Client, context: tuple[str, ...]):
        super().__init__()
        self.client = client
        self.context = tuple(context)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            from franzmq.data_contracts.base import Log
            from franzmq import Topic

            payload = Log(
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                level=record.levelname,
                message=self.format(record),
                logger_name=record.name,
                module=record.module,
                function=record.funcName,
                line_no=record.lineno,
                exc_info=(
                    self.formatter.formatException(record.exc_info)
                    if record.exc_info and self.formatter else None
                ),
                extra=getattr(record, "extra", None),
            )
            topic = Topic(
                payload_type=Log,
                node_id=self.client.require_node_id(),
                context=self.context + (record.levelname,),
            )
            self.client.publish(topic, payload)
        except Exception:
            self.handleError(record)


def attach_log_publisher(client: Client, context: tuple[str, ...]) -> None:
    """Publish this service's log records as ``_Log`` — without owning the log.

    This is the ONE way a service on the local MQTT door reaches the tree's
    `logs` stream, and it is public because it has two callers: services that
    connect through `connect_local_mqtt` (which calls it for them), and the
    connector, which builds its own client for reasons of its own. It was
    private, so the connector could not call it and simply never logged into
    the tree -- eleven of them in the demo, publishing nothing while an
    identically-shaped dataops line right beside them was visible.

    `context` is the service's own position, from `service_context(mount,
    name)`. It is a required argument rather than something derived here
    because only the caller knows its mount, and a publisher that guessed
    would be a second implementation of the rule `service_context` owns.

    franzmq's ``configure_mqtt_logger`` is not used for a second reason: it
    CLEARS the root logger and reinstalls its own stream handler — a dash
    format no Colca service uses, a hard INFO level that undoes
    ``LOG_LEVEL``, and no secret sanitization. Publishing is ADDED to
    whatever logging the service configured, formatted with the shared Colca
    format.
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
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)

    required = ("ulid", "name", "node", "element", "mount")
    if any(not isinstance(payload.get(field), str) for field in required):
        raise RuntimeError("Colca /self returned an invalid local service identity")
    if payload["name"] != service_name:
        raise RuntimeError(
            f"Colca /self resolved service {payload['name']!r}, expected {service_name!r}"
        )
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
    client_id: Optional[str] = None,
    on_connect: Optional[Callable] = None,
    on_disconnect: Optional[Callable] = None,
    identity: Optional[LocalServiceIdentity] = None,
) -> tuple[Client, LocalServiceIdentity]:
    """Connect to local Colca MQTT without credentials or client TLS."""

    resolved = identity or resolve_local_identity(
        service_name,
        host=host,
        http_port=http_port,
        mount=mount,
    )
    client = Client(client_id=client_id or service_name, protocol=pahomqtt.MQTTv5)
    client.node_id = resolved.node_id
    client.username_pw_set(service_name)
    attach_log_publisher(client, resolved.hierarchy)
    client.reconnect_on_failure = True
    client.reconnect_on_offline = True
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    if on_connect is not None:
        client.on_connect = on_connect
    if on_disconnect is not None:
        client.on_disconnect = on_disconnect

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
        context=identity.hierarchy + ("_service",),
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
