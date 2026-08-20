import io
import json
from unittest.mock import patch

import paho.mqtt.client as pahomqtt

from colca_data_contracts import ServiceDetails, ServiceType
from colca_data_contracts.local_service import (
    LocalServiceIdentity,
    connect_local_mqtt,
    publish_local_service_details,
    resolve_local_identity,
)


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _Client:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.node_id = None
        self.username = None
        self.connect_args = None
        self.tls_calls = []
        self.published = []

    def username_pw_set(self, username, password=None):
        self.username = (username, password)

    def configure_mqtt_logger(self):
        pass

    def reconnect_delay_set(self, **kwargs):
        self.reconnect_delay = kwargs

    def connect(self, **kwargs):
        self.connect_args = kwargs

    def publish(self, topic, payload, **kwargs):
        self.published.append((str(topic), payload, kwargs))


def _identity():
    return LocalServiceIdentity(
        service_id="svc-1",
        service_name="notifications",
        node_id="node-1",
        system_element_id="el-1",
        mount="line-1/press-1",
    )


def test_self_uses_only_local_service_and_creation_mount_headers():
    body = json.dumps({
        "ulid": "svc-1",
        "name": "notifications",
        "node": "node-1",
        "element": "el-1",
        "mount": "line-1/press-1",
    }).encode()

    with patch("urllib.request.urlopen", return_value=_Response(body)) as urlopen:
        identity = resolve_local_identity("notifications", mount="declared/path")

    request = urlopen.call_args.args[0]
    assert request.full_url == "http://colca:80/self"
    assert request.get_header("X-colca-service") == "notifications"
    assert request.get_header("X-colca-mount") == "declared/path"
    assert "Authorization" not in request.headers
    assert identity == _identity()


def test_mqtt_uses_v5_name_and_mount_without_tls_or_password():
    with patch("colca_data_contracts.local_service.Client", _Client):
        client, identity = connect_local_mqtt(
            "notifications",
            mount="line-1/press-1",
            identity=_identity(),
        )

    assert identity == _identity()
    assert client.kwargs["protocol"] == pahomqtt.MQTTv5
    assert client.username == ("notifications", None)
    assert client.node_id == "node-1"
    assert client.connect_args["host"] == "colca"
    assert client.connect_args["port"] == 1883
    assert client.connect_args["clean_start"] is False
    assert client.connect_args["properties"].UserProperty == [("mount", "line-1/press-1")]
    assert client.tls_calls == []


def test_service_details_are_published_under_node_and_assigned_mount():
    client = _Client()
    details = ServiceDetails(
        id="svc-1",
        name="notifications",
        service_type=ServiceType.NOTIFICATIONS,
        colca_node_id="node-1",
        system_element_id="el-1",
    )

    publish_local_service_details(client, _identity(), details)

    assert client.published[0][0] == (
        "colca/v1/_ServiceDetails/node-1/line-1/press-1/_service"
    )
    assert client.published[0][1] is details
    assert client.published[0][2] == {"qos": 1, "retain": True}
