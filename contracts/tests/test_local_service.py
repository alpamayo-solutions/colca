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


def test_a_published_log_message_is_the_records_own_message():
    """The payload's message is the raw message, not the formatted line: the
    timestamp, level and logger have fields of their own, and a timestamp in
    the message would keep every repeat distinct, so the node could never
    collapse an error loop."""
    import logging

    from franzmq.data_contracts.base import Log

    from colca_data_contracts.local_service import _LogPublishingHandler
    from colca_data_contracts.logging import COLCA_LOG_FORMAT

    class _Recording:
        def __init__(self):
            self.published = []

        def require_node_id(self):
            return "node-1"

        def publish(self, topic, payload):
            self.published.append((str(topic), payload))

    client = _Recording()
    handler = _LogPublishingHandler(client, ("line-1", "press"))
    handler.setFormatter(logging.Formatter(COLCA_LOG_FORMAT))
    logger = logging.getLogger("press.driver")
    record = logger.makeRecord(
        "press.driver", logging.ERROR, "driver.py", 42, "connection %s refused", ("plc-1",), None
    )

    handler.emit(record)
    handler.emit(record)

    assert len(client.published) == 2
    (topic, first), (_, second) = client.published
    assert isinstance(first, Log)
    assert topic.endswith("/line-1/press/ERROR")
    assert first.message == "connection plc-1 refused"
    assert first.message == second.message, "two identical records must carry identical messages"
    assert first.level == "ERROR"
    assert first.logger_name == "press.driver"
    assert first.line_no == 42


def test_the_traceback_and_stack_move_to_exc_info():
    """Everything the formatted line carried after the message still reaches
    the node: the traceback and the stack_info stack, in exc_info."""
    import logging

    from colca_data_contracts.local_service import _LogPublishingHandler
    from colca_data_contracts.logging import COLCA_LOG_FORMAT

    class _Recording:
        def __init__(self):
            self.published = []

        def require_node_id(self):
            return "node-1"

        def publish(self, topic, payload):
            self.published.append(payload)

    client = _Recording()
    handler = _LogPublishingHandler(client, ("line-1", "press"))
    formatter = logging.Formatter(COLCA_LOG_FORMAT)
    handler.setFormatter(formatter)
    logger = logging.getLogger("press.driver.tb")
    logger.propagate = False
    logger.addHandler(handler)
    try:
        try:
            raise ConnectionRefusedError("plc-1")
        except ConnectionRefusedError:
            logger.exception("connection refused", stack_info=True)
        logger.warning("plain line")
    finally:
        logger.removeHandler(handler)
        logger.propagate = True

    failure, plain = client.published
    assert failure.message == "connection refused"
    assert "Traceback (most recent call last)" in failure.exc_info
    assert "ConnectionRefusedError: plc-1" in failure.exc_info
    assert "Stack (most recent call last)" in failure.exc_info
    assert plain.exc_info is None, "a line without an exception carries no details"


def test_exc_info_holds_exactly_what_the_formatted_line_appended():
    """`logging.Formatter.format` appends the traceback and the stack to the
    message; `_details` must be that tail, character for character."""
    import logging

    from colca_data_contracts.local_service import _details

    formatted = logging.Formatter().format(_with_exception())
    message, tail = formatted.split("\n", 1)
    assert message == "failed"
    assert _details(_with_exception(), logging.Formatter()) == tail


def _with_exception():
    import logging
    import sys

    try:
        raise ValueError("boom")
    except ValueError:
        info = sys.exc_info()
    return logging.LogRecord(
        "x", logging.ERROR, __file__, 1, "failed", None, info, sinfo="Stack (most recent call last):\n  here"
    )
