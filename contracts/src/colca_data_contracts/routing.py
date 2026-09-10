"""Where a contract's records land: its routing class, and that class's stream.

Two facts, each with one owner, composed here so nothing has to re-implement
the composition:

* **contract -> class** is Python's. :data:`CLASS_TABLE` states it for every
  contract not derivable from the payload hierarchy, and ``generate_bundle``
  refuses to build a bundle for a registered payload class it cannot classify
  — so a new contract cannot slip through unclassified.
* **class -> stream** is colca's ``StreamFor``. It is carried across the
  language boundary by ``vectors/manifest_streams.json``, which
  ``plugins/uns/manifest_streams_test.go`` pins against Go in both directions:
  a class Go routes but the vector omits fails, and so does the reverse.

This module exists because that composition used to be re-typed by hand
wherever Python needed it. The copy in the api's projector test double went a
whole release without ``_Log`` in it: colca had grown a class, the copy could
not see that, and no test could go red about the gap.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_VECTOR = Path(__file__).parent / "vectors" / "manifest_streams.json"

# Routing class for everything not derivable from the class hierarchy
# (Cmd subclasses -> "cmd", Ack subclasses -> "ack" — derived, not listed).
CLASS_TABLE: dict[str, str] = {
    "_Metric": "data",
    # A log line is an EVENT: the thing happened, and a later line does not
    # replace an earlier one. As "data" it was state — KV kept only the newest
    # line per logger and level, and the history rode the metrics stream where
    # a chatty service evicted the samples it shared the lane with.
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
    # Alarm EVENTS: append-only, no KV, not retained, on their own stream so
    # they never queue behind a metrics backlog. The config pair above stays
    # on entities — only the two event contracts moved
    # (the alarm stream and uplink lanes design §3).
    "_AlarmStateChange": "alarm",
    "_NotificationDispatched": "alarm",
    "_Annotation": "annotation",
    "_EnrolledIdentity": "entity",
    "_Group": "definition",
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

    Raises for a contract with no routing class, rather than answering "no
    stream" — those two are different, and conflating them is how a record
    silently goes nowhere.
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
    contracts = list(CLASS_TABLE) + ["_Ack", "_CmdConfigure", "_CmdEdit", "_CmdAdmin"]
    return {contract: stream_of_contract(contract) for contract in contracts if stream_of_contract(contract)}
