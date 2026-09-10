"""Builtin quantity kinds.

Not authored, not per deployment, not on the stream. A milliamp is 1e-3 A on
every deployment that will ever exist; asking each one to re-declare that is
how two tags end up disagreeing about a unit.

Values are never converted anywhere in Colca. The factor and offset are
published so a consumer that needs a common basis -- an alarm threshold, an
export -- can do the arithmetic itself.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Unit:
    """`base = value * factor + offset`. Offset carries affine scales (°C→K)."""

    factor: float
    offset: float = 0.0


@dataclass(frozen=True)
class QuantityKind:
    base_unit: Optional[str]
    units: dict[str, Unit] = field(default_factory=dict)


QUANTITY_KINDS: dict[str, QuantityKind] = {
    "temperature": QuantityKind(
        "K",
        {
            "K": Unit(1.0),
            "°C": Unit(1.0, 273.15),
            "°F": Unit(5 / 9, 255.372),
        },
    ),
    "pressure": QuantityKind(
        "Pa",
        {
            "Pa": Unit(1.0),
            "kPa": Unit(1e3),
            "MPa": Unit(1e6),
            "bar": Unit(1e5),
            "mbar": Unit(1e2),
            "psi": Unit(6894.757),
        },
    ),
    "volume-flow": QuantityKind(
        "m³/s",
        {
            "m³/s": Unit(1.0),
            "L/min": Unit(1 / 60000),
            "L/h": Unit(1 / 3600000),
            "m³/h": Unit(1 / 3600),
        },
    ),
    "mass-flow": QuantityKind(
        "kg/s",
        {
            "kg/s": Unit(1.0),
            "kg/h": Unit(1 / 3600),
            "g/s": Unit(1e-3),
            "t/h": Unit(1000 / 3600),
        },
    ),
    "electric-current": QuantityKind(
        "A",
        {
            "A": Unit(1.0),
            "mA": Unit(1e-3),
            "kA": Unit(1e3),
        },
    ),
    "voltage": QuantityKind(
        "V",
        {
            "V": Unit(1.0),
            "mV": Unit(1e-3),
            "kV": Unit(1e3),
        },
    ),
    "power": QuantityKind(
        "W",
        {
            "W": Unit(1.0),
            "kW": Unit(1e3),
            "MW": Unit(1e6),
        },
    ),
    "energy": QuantityKind(
        "J",
        {
            "J": Unit(1.0),
            "kJ": Unit(1e3),
            "Wh": Unit(3600),
            "kWh": Unit(3.6e6),
            "MWh": Unit(3.6e9),
        },
    ),
    "rotational-speed": QuantityKind(
        "s⁻¹",
        {
            "s⁻¹": Unit(1.0),
            "rps": Unit(1.0),
            "rpm": Unit(1 / 60),
        },
    ),
    "speed": QuantityKind(
        "m/s",
        {
            "m/s": Unit(1.0),
            "mm/s": Unit(1e-3),
            "m/min": Unit(1 / 60),
            "km/h": Unit(1 / 3.6),
        },
    ),
    "force": QuantityKind("N", {"N": Unit(1.0), "kN": Unit(1e3)}),
    "torque": QuantityKind(
        "N·m",
        {
            "N·m": Unit(1.0),
            "mN·m": Unit(1e-3),
            "kN·m": Unit(1e3),
        },
    ),
    "acceleration": QuantityKind(
        "m/s²",
        {
            "m/s²": Unit(1.0),
            "mm/s²": Unit(1e-3),
            "g": Unit(9.80665),
        },
    ),
    "length": QuantityKind(
        "m",
        {
            "m": Unit(1.0),
            "mm": Unit(1e-3),
            "cm": Unit(1e-2),
            "km": Unit(1e3),
            "in": Unit(0.0254),
        },
    ),
    "mass": QuantityKind(
        "kg",
        {
            "kg": Unit(1.0),
            "g": Unit(1e-3),
            "mg": Unit(1e-6),
            "t": Unit(1e3),
            "lb": Unit(0.45359237),
        },
    ),
    "volume": QuantityKind(
        "m³",
        {
            "m³": Unit(1.0),
            "L": Unit(1e-3),
            "mL": Unit(1e-6),
        },
    ),
    "time": QuantityKind(
        "s",
        {
            "s": Unit(1.0),
            "ms": Unit(1e-3),
            "µs": Unit(1e-6),
            "min": Unit(60),
            "h": Unit(3600),
        },
    ),
    "frequency": QuantityKind(
        "Hz",
        {
            "Hz": Unit(1.0),
            "kHz": Unit(1e3),
            "MHz": Unit(1e6),
        },
    ),
    "ratio": QuantityKind(
        "1",
        {
            "1": Unit(1.0),
            "%": Unit(1e-2),
            "ppm": Unit(1e-6),
        },
    ),
    # Dimensionless counter: a part count has no unit at all.
    "count": QuantityKind(None, {}),
}


def accepts_unit(quantity_kind: str, unit: str) -> bool:
    """Whether `unit` is the kind's base unit or one of its alternates."""
    kind = QUANTITY_KINDS.get(quantity_kind)
    if kind is None:
        return False
    return unit in kind.units
