#!/usr/bin/env python3
"""Write the conformance vectors from the reference implementation.

Every client in this repository must give the same answers as
`colca-data-contracts`, the package the node itself validates against. The
expected values here are therefore never typed by hand: they are read out of
that package, so a rule that changes there fails the clients instead of
drifting quietly.

    uv run --project contracts python clients/spec/generate_vectors.py

or with any interpreter that has colca-data-contracts installed. Writes
`clients/spec/vectors.json` and prints its digest.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

try:
    import colca_data_contracts as contracts
    from colca_data_contracts.payload import derive_annotation_id
except ImportError:  # pragma: no cover - operator-facing
    sys.exit("colca-data-contracts is not importable; see the module docstring")

HERE = Path(__file__).resolve().parent

#: Annotation ids. The last two cases are the same call with the signals in a
#: different order: the set is sorted before hashing, so the id is the same.
ANNOTATION_IDS = [
    {
        "note": "a panel from the Wisewoods line",
        "annotation_type_id": "01M2M9N2GBFMRCEKT4DVJC671A",
        "source": "dataops/paneltracking-wisewoods/line1",
        "time_start": 1789535138.047,
        "signal_ids": ["01M2AB5YWM56SH9B2EQ3S5VNXF"],
    },
    {
        "note": "no signals at all, and the epoch",
        "annotation_type_id": "01M2M9N2GBVGQVEBJ7KADQY9M2",
        "source": "a",
        "time_start": 0.0,
        "signal_ids": [],
    },
    {
        "note": "microseconds are part of the identity; six decimals, no more",
        "annotation_type_id": "01M2M9N2GBN747S8858N3TXMN7",
        "source": "svc/x",
        "time_start": 1.0000005,
        "signal_ids": ["01BX5ZZKBKACTAV9WEVGEMMVRZ", "01M2AB5YWSZTAFBKYYYA5C0TE7"],
    },
    {
        "note": "the same two signals in the other order — the set is sorted, so same id",
        "annotation_type_id": "01M2M9N2GBN747S8858N3TXMN7",
        "source": "svc/x",
        "time_start": 1.0000005,
        "signal_ids": ["01M2AB5YWSZTAFBKYYYA5C0TE7", "01BX5ZZKBKACTAV9WEVGEMMVRZ"],
    },
]

#: Topics. Level 4 is the publishing node for every class — a definition is not
#: the exception a client author expects it to be, and the broker reads the node
#: off that level whatever the class (`internal/historian/record.go`).
TOPICS = [
    {"note": "a metric on a node", "root": "steine", "contract": "_Metric",
     "node": "n-technikum", "path": "wisewoods/line1/mas2/sta1/aggos/grit"},
    {"note": "an annotation", "root": "steine",
     "contract": "_Annotation", "node": "n-technikum", "path": "panel/01M2M9"},
    {"note": "a definition carries the node too", "root": "steine",
     "contract": "_AnnotationType", "node": "n-technikum", "path": "01M2M9N2GBFMRCEKT4DVJC671A"},
    {"note": "a command names its verb under the node, and the root is configurable",
     "root": "alp", "contract": "_CmdConfigure",
     "node": "n-technikum", "path": "element/upsert"},
]


def annotation_ids() -> list[dict]:
    return [{**case, "expected": derive_annotation_id(
        annotation_type_id=case["annotation_type_id"],
        source=case["source"],
        time_start=case["time_start"],
        signal_ids=case["signal_ids"],
    )} for case in ANNOTATION_IDS]


def topics() -> list[dict]:
    """Render each case through `node_topic`, the builder the services use."""
    out = []
    for case in TOPICS:
        payload_type = contracts.PAYLOAD_CLASSES[case["contract"]]
        # topic_root() reads the environment on every call, so the root is set
        # per case rather than by importing the module twice.
        os.environ[contracts.root.ROOT_ENV] = case["root"]
        topic = contracts.node_topic(payload_type, case["path"], node=case["node"])
        out.append({**case, "expected": str(topic)})
    return out


def main() -> int:
    if not contracts.TOPICS_CARRY_NODE_ID:
        sys.exit("the installed franzmq predates the node-id topic scheme (0.5.0)")
    vectors = {
        "produced_by": "clients/spec/generate_vectors.py from colca-data-contracts",
        "annotation_id": annotation_ids(),
        "topic": topics(),
    }
    body = json.dumps(vectors, indent=2, sort_keys=False) + "\n"
    (HERE / "vectors.json").write_text(body, encoding="utf-8")
    print(f"{hashlib.sha256(body.encode()).hexdigest()}  clients/spec/vectors.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
