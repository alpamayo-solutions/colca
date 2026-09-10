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
    body = json.dumps(
        {
            "ulid": "svc-1",
            "name": "notifications",
            "node": "node-1",
            "element": "el-1",
            "mount": "line-1/press-1",
        }
    ).encode()

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


def test_outgoing_queue_limit_is_set_before_connecting():
    calls = []

    class BoundedClient(_Client):
        def max_queued_messages_set(self, limit):
            calls.append(("queue", limit))

        def connect(self, **kwargs):
            calls.append(("connect", kwargs["host"]))
            return super().connect(**kwargs)

    with patch("colca_data_contracts.local_service.Client", BoundedClient):
        connect_local_mqtt("connector", identity=_identity(), max_queued_messages=100)
    assert calls == [("queue", 100), ("connect", "colca")]


def _details(name="notifications", service_id="svc-1"):
    return ServiceDetails(
        id=service_id,
        name=name,
        service_type=ServiceType.NOTIFICATIONS,
        colca_node_id="node-1",
        system_element_id="el-1",
    )


def test_service_details_are_published_under_node_mount_and_service_name():
    client = _Client()
    details = _details()

    publish_local_service_details(client, _identity(), details)

    assert client.published[0][0] == ("colca/v1/_ServiceDetails/node-1/line-1/press-1/notifications/_service")
    assert client.published[0][1] is details
    assert client.published[0][2] == {"qos": 1, "retain": True}


def test_two_unplaced_services_do_not_share_one_record():
    """Unplaced services have no mount, so only their names keep their retained
    registrations apart.
    """
    projector = LocalServiceIdentity(
        service_id="svc-p",
        service_name="projector",
        node_id="node-1",
        system_element_id="",
        mount="",
    )
    dataops = LocalServiceIdentity(
        service_id="svc-d",
        service_name="dataops",
        node_id="node-1",
        system_element_id="",
        mount="",
    )

    client = _Client()
    publish_local_service_details(client, projector, _details("projector", "svc-p"))
    publish_local_service_details(client, dataops, _details("dataops", "svc-d"))

    topics = [entry[0] for entry in client.published]
    assert topics == [
        "colca/v1/_ServiceDetails/node-1/projector/_service",
        "colca/v1/_ServiceDetails/node-1/dataops/_service",
    ]
    assert len(set(topics)) == 2, "one service erased the other's registration"


def test_service_context_matches_the_shared_vectors():
    """The Go implementation (`uns.ServiceContext`) reads the same vectors."""
    import json as _json
    from pathlib import Path

    from colca_data_contracts.local_service import service_context

    vectors_path = (
        Path(__file__).resolve().parents[1] / "src" / "colca_data_contracts" / "vectors" / "service_context.json"
    )
    vectors = _json.loads(vectors_path.read_text())["vectors"]
    assert vectors, "empty vectors would make this test pass proving nothing"
    for vector in vectors:
        assert list(service_context(vector["mount"], vector["service"])) == vector["context"], (
            f"service_context({vector['mount']!r}, {vector['service']!r})"
        )


def test_connecting_mqtt_adds_the_log_publisher_without_owning_the_log():
    """Connecting adds the _Log publisher and keeps the caller's handler, level
    and formatter."""
    import logging

    from colca_data_contracts.local_service import _LogPublishingHandler as MQTTHandler
    from colca_data_contracts.local_service import attach_log_publisher
    from colca_data_contracts.logging import COLCA_LOG_FORMAT

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    sentinel = logging.NullHandler()
    sentinel.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(sentinel)
    root.setLevel(logging.DEBUG)
    before = [h for h in root.handlers if isinstance(h, MQTTHandler)]
    try:
        attach_log_publisher(object(), ("line1", "svc"))

        assert sentinel in root.handlers, "the caller's handler was removed"
        assert root.level == logging.DEBUG, "the caller's level was reset"
        added = [h for h in root.handlers if isinstance(h, MQTTHandler) and h not in before]
        assert len(added) == 1, "exactly one _Log publisher must be added"
        assert added[0].formatter._fmt == COLCA_LOG_FORMAT
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)
        root.setLevel(original_level)
