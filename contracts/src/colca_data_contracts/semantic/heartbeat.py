"""Heartbeat — semantic contract for liveness signals.

IMPORTANT — this is a SEMANTIC DATA CONTRACT, not a topic type.

How it works:
    1. A Signal is created with data_type='boolean' and
       implements_contract='Heartbeat'.
    2. Heartbeat values (True/False) are published as STANDARD Metric payloads
       on that signal's regular _Metric topic.
    3. The producer toggles the value at a fixed interval — consumers detect
       liveness from the timestamp progression, the boolean value itself
       carries no domain meaning.
    4. Consumers discover Heartbeat signals via the Hub API:
       GET /api/v1/signals/?implements_contract=Heartbeat

Unlike the JSON-shaped contracts (EdgeExecutionContext,
ExecutionEventCorrection), Heartbeat carries a primitive bool — no
from_json/to_json needed.
"""

from typing import ClassVar


class Heartbeat:
    """Semantic data contract for liveness signals.

    The signal's data_type must be 'boolean'. The value alternates between
    True and False at a producer-defined interval (e.g. 5s for connectors).
    Consumers track the metric timestamp, not the boolean value.
    """

    expected_data_type: ClassVar[str] = "boolean"
