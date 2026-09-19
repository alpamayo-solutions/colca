"""The routing table knows every command the payload hierarchy defines."""

from franzmq.data_contracts.base import Cmd

import colca_data_contracts  # noqa: F401 - registers every contract class
from colca_data_contracts.routing import COMMAND_CONTRACTS, streams_by_contract


def _command_contracts(cls=Cmd) -> set[str]:
    found = set()
    for sub in cls.__subclasses__():
        found.add(sub.get_identifier())
        found |= _command_contracts(sub)
    return found


def test_every_command_contract_is_routed_to_the_commands_stream():
    defined = _command_contracts()
    assert "_CmdOperate" in defined, "the hierarchy walk found no commands; this test would prove nothing"
    assert set(COMMAND_CONTRACTS) == defined
    streams = streams_by_contract()
    for contract in defined:
        assert streams.get(contract) == "commands", contract
