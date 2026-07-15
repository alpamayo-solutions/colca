"""Semantic data contracts for Colca signals.

These define the JSON schema that Metric.value_json must conform to when a
Signal has implements_contract set. They are NOT MQTT payload types and do
NOT define their own topic patterns.

Events conforming to a semantic contract are always published as standard
Metrics on the signal's regular _Metric topic. Consumers discover signals
via the Hub API (GET /signals/?implements_contract=...) and subscribe to
the returned signal topics — never to a custom topic derived from the
contract name.
"""

from colca_data_contracts.semantic.edge_execution_context import EdgeExecutionContext
from colca_data_contracts.semantic.execution_event_correction import (
    ExecutionEventCorrection,
)
from colca_data_contracts.semantic.heartbeat import Heartbeat

# Registry of all known semantic contracts.
# Key: contract name (matches Signal.implements_contract value)
# Value: the contract class defining the schema. Each class declares
#        `expected_data_type` to constrain Signal.data_type at validation time.
SEMANTIC_CONTRACTS = {
    "EdgeExecutionContext": EdgeExecutionContext,
    "ExecutionEventCorrection": ExecutionEventCorrection,
    "Heartbeat": Heartbeat,
}
