"""The canonical slot data_type vocabulary, pinned across the language boundary.

`CANONICAL_DATA_TYPES` (data_models/loader.py) is the one definition. Colca's
Go door needs it natively too (`plugins/uns.slotDataTypes`), and so does any
client that validates models before submitting. Go cannot import Python, so the sides meet
at `vectors/data_model_vocabulary.json` -- the same tier-2 golden-vector
mechanism `topic_transformations.json` uses for the topic grammar.

This test is the link between the definition and that file: a sixth canonical
type added to `loader.py` fails here until the vectors are updated, and updating
the vectors fails colca's `TestSlotDataTypesMatchesTheGoldenVocabulary` until
`slotDataTypes` moves with it.
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
