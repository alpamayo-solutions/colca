"""Where a contract's records land: its routing class, and that class's stream.

* contract -> class is defined here. :data:`CLASS_TABLE` covers every contract
  the payload hierarchy does not decide, and ``generate_bundle`` refuses a
  registered class it cannot classify.
* class -> stream is colca's ``StreamFor``, carried over by
  ``vectors/manifest_streams.json``, which
  ``plugins/uns/manifest_streams_test.go`` checks in both directions.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_VECTOR = Path(__file__).parent / "vectors" / "manifest_streams.json"

#: Every command contract. Each lands on the commands stream; the class
#: hierarchy decides that, and ``test_routing`` pins this list to it, because a
#: command missing here was a command ``streams_by_contract`` did not know --
#: a test double then put a ``_CmdOperate`` on the entities stream.
COMMAND_CONTRACTS: tuple[str, ...] = (
    "_CmdAcknowledge",
    "_CmdParam",
    "_CmdOperate",
    "_CmdMaintain",
    "_CmdConfigure",
    "_CmdEdit",
    "_CmdAdmin",
)

# Routing class for everything not derivable from the class hierarchy
# (Cmd subclasses -> "cmd", Ack subclasses -> "ack" — derived, not listed).
CLASS_TABLE: dict[str, str] = {
    "_Metric": "data",
    "_ClockProgress": "data",
    # A log line is an event: a later line does not replace an earlier one.
    "_Log": "log",
    "_SystemElement": "entity",
    "_Signal": "entity",
    "_Constant": "entity",
    "_Resource": "entity",
    "_EditOperation": "entity",
    "_DataTags": "entity",
    "_Node": "entity",
    "_ServiceDetails": "entity",
    "_ExternalReference": "entity",
    "_AlarmNotificationConfig": "entity",
    "_NotificationConfigStatus": "entity",
    # What a service found, republished for as long as it holds and retired by
    # tombstone: same retained shape as the alarm below, one writer earlier in
    # the chain. The service that ran the check writes this; the manager reads
    # findings and owns the alarm.
    "_Finding": "entity",
    # The standing alarm, one record per definition: it overwrites itself and
    # a tombstone retires it, so a new subscriber sees what stands.
    "_AlarmState": "entity",
    # A silence per element and reason, retained until it runs out and then
    # retired by tombstone; it outlives the alarms it covers.
    "_AlarmSilence": "entity",
    # Alarm events: append-only, not in KV, not retained, and on their own
    # stream so they never queue behind a metrics backlog.
    "_AlarmStateChange": "alarm",
    "_NotificationDispatched": "alarm",
    "_Annotation": "annotation",
    "_EnrolledIdentity": "entity",
    "_Group": "definition",
    "_ClockDefinition": "definition",
    "_MetadataType": "definition",
    "_AnnotationType": "definition",
    "_DataModel": "definition",
    "_ExternalSystem": "definition",
    "_SemanticTag": "definition",
    "_PersonalAccessToken": "definition",
    "_AuditEvent": "audit",
}


@lru_cache(maxsize=1)
def stream_of_class() -> dict[str, str]:
    """Every bundle class name mapped to the stream it routes to.

    An empty value means the class has no stream: nothing persists it.
    """
    return dict(json.loads(_VECTOR.read_text(encoding="utf-8"))["streams"])


def stream_of_contract(contract: str) -> str:
    """The stream ``contract``'s records land on, or ``""`` for none.

    Raises for a contract with no routing class, which is not the same as a
    class without a stream.
    """
    if contract.startswith("_Cmd"):
        routing_class = "cmd"
    elif contract == "_Ack":
        routing_class = "ack"
    else:
        try:
            routing_class = CLASS_TABLE[contract]
        except KeyError:
            raise KeyError(
                f"{contract} has no routing class — add it to CLASS_TABLE "
                f"(colca_data_contracts.routing), the same table generate_bundle reads"
            ) from None
    return stream_of_class().get(routing_class, "")


def streams_by_contract() -> dict[str, str]:
    """Every classified contract mapped to its stream, omitting the streamless."""
    contracts = [*CLASS_TABLE, "_Ack", *COMMAND_CONTRACTS]
    return {contract: stream_of_contract(contract) for contract in contracts if stream_of_contract(contract)}
