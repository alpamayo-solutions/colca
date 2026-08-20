"""Narrow client helpers for a service inside one Colca deployment network.

The local Colca doors use network placement as their trust boundary. A service
names itself, but carries no token, password, client certificate, or TLS mode.
This module deliberately covers only the two bootstrap operations shared by
local publishers: resolve ``GET /self`` and connect MQTT v5.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional

import paho.mqtt.client as pahomqtt
from franzmq import Client, Topic
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from colca_data_contracts.payload import ServiceDetails


@dataclass(frozen=True)
class LocalServiceIdentity:
    service_id: str
    service_name: str
    node_id: str
    system_element_id: str
    mount: str

    @property
    def hierarchy(self) -> tuple[str, ...]:
        return tuple(segment for segment in self.mount.split("/") if segment)


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
    client.configure_mqtt_logger()
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
