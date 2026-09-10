"""The canonical slot data_type vocabulary, shared with Go.

`CANONICAL_DATA_TYPES` in data_models/loader.py is the definition, and
`vectors/data_model_vocabulary.json` carries it to Go (`plugins/uns.slotDataTypes`).
This test keeps the file equal to the definition; a Go test does the same on
the other side.
"""

import json
from pathlib import Path

from colca_data_contracts.data_models import CANONICAL_DATA_TYPES

VECTORS_PATH = Path(__file__).resolve().parents[1] / ("src/colca_data_contracts/vectors/data_model_vocabulary.json")


def test_golden_vocabulary_matches_the_canonical_definition():
    vectors = json.loads(VECTORS_PATH.read_text())
    assert set(vectors["canonical_data_types"]) == set(CANONICAL_DATA_TYPES)
    # Sorted, so a regenerated file is byte-stable and the Go side's failure
    # message stays readable.
    assert vectors["canonical_data_types"] == sorted(vectors["canonical_data_types"])
