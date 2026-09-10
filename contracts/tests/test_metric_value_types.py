"""``Metric.value`` is typed ``Any`` (franzmq's own ``Metric`` dataclass) and
has no schema of its own — the *signal*'s ``data_type`` is what says how a
value should be interpreted (pinned for ``json`` and ``string`` by
``test_signal_data_type.py``). This test pins the metric side of the same
claim: a ``string``-kind value is not a special case that needs its own
handling anywhere in ``Metric.encode``/``decode`` — it round-trips exactly
like ``json``, ``float``, ``int`` and ``bool`` already do (e.g. dataops's
``AnnotationOutput`` already publishes ``json`` metric values; a connector
reading a ``kind="string"`` PLC tag — see the demo's ``factory.py`` — needs
the identical guarantee for a plain ``str``).
"""

from __future__ import annotations

import pytest

from colca_data_contracts.payload import Metric


def _round_trip(value):
    metric = Metric(value=value, timestamp=1_700_000_000.0)
    decoded = Metric.decode(metric.encode(), 0)
    return decoded.value


@pytest.mark.parametrize(
    "value",
    [
        "PO-2026-00042",  # a plain string value, e.g. a work_order tag
        "",  # the idle sentinel a string signal rests at
        3.5,
        1,
        True,
        {"work_order": "PO-2026-00042", "job_card": "PO-JOB00001"},  # json
    ],
)
def test_a_metric_value_round_trips_by_type(value):
    assert _round_trip(value) == value


def test_a_string_value_keeps_its_type_not_just_its_content():
    """A number-like string ("42") must not come back as an int -- json.dumps
    quotes it, json.loads must hand the same str back."""
    decoded = _round_trip("42")
    assert decoded == "42"
    assert isinstance(decoded, str)
