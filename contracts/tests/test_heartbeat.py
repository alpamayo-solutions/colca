"""Tests for the Heartbeat semantic data contract."""

from colca_data_contracts.semantic import SEMANTIC_CONTRACTS
from colca_data_contracts.semantic.heartbeat import Heartbeat


def test_contract_registered():
    assert SEMANTIC_CONTRACTS["Heartbeat"] is Heartbeat


def test_expected_data_type_is_boolean():
    assert Heartbeat.expected_data_type == "boolean"


def test_other_contracts_expect_json():
    """Sanity check: existing contracts keep their json data_type."""
    assert SEMANTIC_CONTRACTS["EdgeExecutionContext"].expected_data_type == "json"
    assert SEMANTIC_CONTRACTS["ExecutionEventCorrection"].expected_data_type == "json"
