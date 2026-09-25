"""Product and recipe records of a node's catalog.

A catalog is master data held as `_Constant` records of data type ``json``: one
record per product at ``catalog/products/{sku}`` and one per recipe at
``catalog/recipes/{id}``. The node checks only that such a value is JSON, so this
module is the contract for what is inside: the standard fields, an open
``attributes`` map for whatever a plant keeps beyond them, and who wrote it.

A product names its recipe by ``recipe_id`` instead of carrying it: many products
share one recipe, and editing that recipe then changes one record.

``provenance`` says who wrote the record. ``field_provenance`` names the fields
someone else set on top of it, typically a value entered by hand where the
upstream system has none or a wrong one. Whoever writes the whole record again
keeps those fields, so a re-sync does not undo a hand entry.

`PRODUCT_SCHEMA` and `RECIPE_SCHEMA` state the same rules as JSON Schema for
consumers that are not written in Python.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

CATALOG_ROOT = "catalog"
PRODUCTS = f"{CATALOG_ROOT}/products"
RECIPES = f"{CATALOG_ROOT}/recipes"
#: A key is one topic level: no separator, no wildcard, not blank.
_KEY_PATTERN = re.compile(r"[^/+#]*[^/+#\s][^/+#]*")


def product_path(sku: str) -> str:
    return f"{PRODUCTS}/{_key(sku, 'sku')}"


def recipe_path(recipe_id: str) -> str:
    return f"{RECIPES}/{_key(recipe_id, 'id')}"


def _key(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _KEY_PATTERN.fullmatch(value):
        raise ValueError(f"`{name}` must be a non-blank string without '/', '+' or '#', got {value!r}")
    return value


@dataclass(frozen=True)
class Provenance:
    """Who wrote a record or a field. ``source`` is the writer: an external
    system's key for a connector, ``manual`` for a person."""

    source: str
    set_by: str | None = None
    #: RFC 3339.
    set_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"source": self.source}
        if self.set_by is not None:
            data["set_by"] = self.set_by
        if self.set_at is not None:
            data["set_at"] = self.set_at
        return data

    @classmethod
    def from_dict(cls, data: Any) -> Provenance:
        if not isinstance(data, dict):
            raise ValueError(f"provenance must be an object, got {data!r}")
        _only(data, ("source", "set_by", "set_at"), "provenance")
        source = data.get("source")
        if not isinstance(source, str) or not source:
            raise ValueError("provenance.source must be a non-empty string")
        for name in ("set_by", "set_at"):
            if data.get(name) is not None and not isinstance(data[name], str):
                raise ValueError(f"provenance.{name} must be a string")
        return cls(source, data.get("set_by"), data.get("set_at"))


@dataclass
class Product:
    sku: str
    name: str | None = None
    substrate: str | None = None
    thickness_mm: float | None = None
    length_mm: float | None = None
    width_mm: float | None = None
    #: What sanding takes off the board, both sides together.
    sandoff_mm: float | None = None
    density_kg_m3: float | None = None
    #: The grit of the last belt the board sees, FEPA P.
    final_grit: int | None = None
    recipe_id: str | None = None
    #: The product a line runs when nobody chose one.
    is_default: bool = False
    attributes: dict[str, Any] = field(default_factory=dict)
    provenance: Provenance | None = None
    field_provenance: dict[str, Provenance] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self, PRODUCT_FIELDS)

    @classmethod
    def from_dict(cls, data: Any) -> Product:
        fields_ = _checked(data, PRODUCT_FIELDS, "product")
        _key(fields_.get("sku"), "sku")
        for name in ("thickness_mm", "length_mm", "width_mm", "sandoff_mm", "density_kg_m3"):
            _number(fields_, name)
        _whole(fields_, "final_grit")
        _text(fields_, "name", "substrate", "recipe_id")
        return cls(**fields_)


@dataclass
class Recipe:
    id: str
    name: str | None = None
    is_default: bool = False
    #: One grit per head, keyed by the head's path below the line
    #: (``mas2/sta1/aggos``); ``null`` for a head the recipe leaves without a belt.
    grits: dict[str, int | None] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)
    provenance: Provenance | None = None
    field_provenance: dict[str, Provenance] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self, RECIPE_FIELDS)

    @classmethod
    def from_dict(cls, data: Any) -> Recipe:
        fields_ = _checked(data, RECIPE_FIELDS, "recipe")
        _key(fields_.get("id"), "id")
        _text(fields_, "name")
        grits = fields_.get("grits", {})
        if not isinstance(grits, dict):
            raise ValueError("recipe.grits must be an object")
        for head, grit in grits.items():
            if grit is not None and (isinstance(grit, bool) or not isinstance(grit, int) or grit <= 0):
                raise ValueError(f"recipe.grits[{head!r}] must be a positive whole number or null")
        return cls(**fields_)


PRODUCT_FIELDS = (
    "sku",
    "name",
    "substrate",
    "thickness_mm",
    "length_mm",
    "width_mm",
    "sandoff_mm",
    "density_kg_m3",
    "final_grit",
    "recipe_id",
    "is_default",
    "attributes",
    "provenance",
    "field_provenance",
)
RECIPE_FIELDS = ("id", "name", "is_default", "grits", "attributes", "provenance", "field_provenance")


def _only(data: dict[str, Any], allowed: tuple[str, ...], what: str) -> None:
    unknown = sorted(set(data) - set(allowed))
    if unknown:
        raise ValueError(f"{what}: unknown field(s) {unknown}; plant-specific values go in `attributes`")


def _checked(data: Any, allowed: tuple[str, ...], what: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError(f"a {what} must be an object, got {data!r}")
    _only(data, allowed, what)
    fields_ = dict(data)
    if not isinstance(fields_.get("is_default", False), bool):
        raise ValueError(f"{what}.is_default must be a boolean")
    if not isinstance(fields_.get("attributes", {}), dict):
        raise ValueError(f"{what}.attributes must be an object")
    if fields_.get("provenance") is not None:
        fields_["provenance"] = Provenance.from_dict(fields_["provenance"])
    by_field = fields_.get("field_provenance", {})
    if not isinstance(by_field, dict):
        raise ValueError(f"{what}.field_provenance must be an object")
    for name in by_field:
        if name not in allowed or name in ("provenance", "field_provenance"):
            raise ValueError(f"{what}.field_provenance names {name!r}, which is no field")
    fields_["field_provenance"] = {name: Provenance.from_dict(p) for name, p in by_field.items()}
    return fields_


def _number(data: dict[str, Any], name: str) -> None:
    value = data.get(name)
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"`{name}` must be a number >= 0 or null, got {value!r}")


def _whole(data: dict[str, Any], name: str) -> None:
    value = data.get(name)
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
        raise ValueError(f"`{name}` must be a positive whole number or null, got {value!r}")


def _text(data: dict[str, Any], *names: str) -> None:
    for name in names:
        if data.get(name) is not None and not isinstance(data[name], str):
            raise ValueError(f"`{name}` must be a string or null")


def _to_dict(record: Product | Recipe, names: tuple[str, ...]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for name in names:
        value = getattr(record, name)
        if name == "provenance":
            if value is not None:
                data[name] = value.to_dict()
        elif name == "field_provenance":
            if value:
                data[name] = {key: p.to_dict() for key, p in value.items()}
        elif name == "attributes":
            data[name] = dict(value)
        else:
            data[name] = value
    return data


_PROVENANCE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["source"],
    "additionalProperties": False,
    "properties": {
        "source": {"type": "string", "minLength": 1},
        "set_by": {"type": ["string", "null"]},
        "set_at": {"type": ["string", "null"]},
    },
}
_KEY = {"type": "string", "pattern": f"^{_KEY_PATTERN.pattern}$"}
_MEASURE = {"type": ["number", "null"], "minimum": 0}
_TEXT = {"type": ["string", "null"]}
_COMMON: dict[str, Any] = {
    "is_default": {"type": "boolean"},
    "attributes": {"type": "object"},
    "provenance": _PROVENANCE_SCHEMA,
}


def _field_provenance(names: tuple[str, ...]) -> dict[str, Any]:
    named = [n for n in names if n not in ("provenance", "field_provenance")]
    return {"type": "object", "propertyNames": {"enum": named}, "additionalProperties": _PROVENANCE_SCHEMA}


PRODUCT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Catalog product",
    "type": "object",
    "required": ["sku"],
    "additionalProperties": False,
    "properties": {
        "sku": _KEY,
        "name": _TEXT,
        "substrate": _TEXT,
        "thickness_mm": _MEASURE,
        "length_mm": _MEASURE,
        "width_mm": _MEASURE,
        "sandoff_mm": _MEASURE,
        "density_kg_m3": _MEASURE,
        "final_grit": {"type": ["integer", "null"], "minimum": 1},
        "recipe_id": _TEXT,
        **_COMMON,
        "field_provenance": _field_provenance(PRODUCT_FIELDS),
    },
}

RECIPE_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Catalog recipe",
    "type": "object",
    "required": ["id"],
    "additionalProperties": False,
    "properties": {
        "id": _KEY,
        "name": _TEXT,
        "grits": {"type": "object", "additionalProperties": {"type": ["integer", "null"], "minimum": 1}},
        **_COMMON,
        "field_provenance": _field_provenance(RECIPE_FIELDS),
    },
}
