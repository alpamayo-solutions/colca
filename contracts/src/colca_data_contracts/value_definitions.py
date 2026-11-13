"""What the values of a signal mean: the rules a value definition follows.

A value definition maps a value a signal can take to the text that says what
it means: a fault code to its message (``{"202": "Sieve blocked"}``), a
machine state to its name (``{"0": "UNKNOWN", "1": "OFFLINE", ...}``). A
``_Signal`` record carries it as ``value_definitions``, and a data-model signal
slot may declare one, which every signal filling the slot then carries.

The keys are JSON object keys, so they are text: the value written the one
canonical way for the signal's data type (``KEY_KINDS``):

- ``int``: base ten, no sign on zero, no leading zeros, no ``+``, within int64
  (``"0"``, ``"-1"``, ``"202"``).
- ``boolean``: ``"true"`` or ``"false"``.
- ``string``: the value itself, 1 to ``MAX_TEXT`` characters.

No other data type takes a definition: a float has no exact value to name, and
a datetime or json value is no code. A meaning is 1 to ``MAX_TEXT``
characters with no surrounding whitespace, and a definition holds at most
``MAX_ENTRIES`` values.

The node refuses a record that breaks a rule with
``invalid_field: value_definitions: <keyword>[ <limit>]``, the refusal shape
of ``vectors/field_refusal.json``. colca's Go suite checks its copy of these
rules against ``vectors/value_definitions.json``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

KeyKind = Literal["int", "boolean", "string"]

#: Which key a definition takes, by data type. Both vocabularies are here, the
#: signal one (``int``) and the data-model slot one (``integer``), so a slot
#: and the signals filling it are judged by one table. A data type that is not
#: a key takes no definition.
KEY_KINDS: dict[str, KeyKind] = {
    "int": "int",
    "integer": "int",
    "boolean": "boolean",
    "string": "string",
}

#: The most values one definition may name.
MAX_ENTRIES = 500

#: The most characters a meaning, or a string key, may have.
MAX_TEXT = 120

_INT_KEY = re.compile(r"^(0|-?[1-9][0-9]*)$")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class ValueDefinitionProblem:
    """One broken rule: the schema-like keyword and its limit, as the node's
    refusal names them, and the key it concerns (``None`` for the whole
    definition)."""

    keyword: str
    limit: str = ""
    key: str | None = None

    @property
    def refusal(self) -> str:
        """The refusal the node answers for this problem."""
        suffix = f" {self.limit}" if self.limit else ""
        return f"invalid_field: value_definitions: {self.keyword}{suffix}"

    @property
    def message(self) -> str:
        """A sentence for a person. Without a key (the node's refusal names
        none) it speaks of "a value" and "a meaning"."""
        value = repr(self.key) if self.key is not None else "a value"
        meaning = f"the meaning of {self.key!r}" if self.key is not None else "a meaning"
        if self.keyword == "data_type":
            return f"only signals of data type {', '.join(sorted(set(KEY_KINDS.values())))} take value definitions"
        if self.keyword == "type":
            return "value definitions must map text keys to text meanings"
        if self.keyword == "maxProperties":
            return f"at most {MAX_ENTRIES} values may be defined"
        if self.keyword == "propertyNames":
            return f"{value} is not written as a canonical {self.limit} value"
        if self.keyword == "minLength":
            return f"{meaning} is empty"
        if self.keyword == "maxLength":
            return f"{meaning} is longer than {MAX_TEXT} characters"
        return f"{meaning} has surrounding whitespace"


def key_is_canonical(kind: KeyKind, key: str) -> bool:
    """Whether ``key`` is a value of ``kind`` written the one canonical way."""
    if kind == "int":
        return bool(_INT_KEY.match(key)) and _INT64_MIN <= int(key) <= _INT64_MAX
    if kind == "boolean":
        return key in ("true", "false")
    return 0 < len(key) <= MAX_TEXT


def canonical_key(data_type: str, value: object) -> str:
    """The key a typed value has in a definition of ``data_type``.

    A plant model or a data-model YAML writes ``202: ...`` or ``true: ...``;
    this turns the parsed value into the wire key. Raises ``ValueError`` for a
    value the data type cannot take.
    """
    kind = KEY_KINDS.get(data_type)
    if kind is None:
        raise ValueError(f"data type {data_type!r} takes no value definitions")
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            if isinstance(value, str) and key_is_canonical("int", value):
                return value
            raise ValueError(f"{value!r} is not an integer")
        key = str(value)
    elif kind == "boolean":
        if not isinstance(value, bool):
            if value in ("true", "false"):
                return str(value)
            raise ValueError(f"{value!r} is not a boolean")
        key = "true" if value else "false"
    else:
        if not isinstance(value, str):
            raise ValueError(f"{value!r} is not a string")
        key = value
    if not key_is_canonical(kind, key):
        raise ValueError(f"{value!r} is not a {kind} value a definition can name")
    return key


def key_value(data_type: str, key: str) -> int | bool | str:
    """The typed value behind a canonical key: the inverse of ``canonical_key``."""
    kind = KEY_KINDS[data_type]
    if kind == "int":
        return int(key)
    if kind == "boolean":
        return key == "true"
    return key


def sorted_keys(data_type: str | None, definitions: Mapping[str, str]) -> list[str]:
    """The keys in the order of their values: numbers numerically, the rest as text."""
    kind = KEY_KINDS.get(data_type or "")
    if kind == "int" and all(key_is_canonical("int", key) for key in definitions):
        return sorted(definitions, key=int)
    return sorted(definitions)


def value_definition_problems(data_type: str | None, definitions: object) -> list[ValueDefinitionProblem]:
    """Every broken rule of ``definitions`` for a signal (or slot) of
    ``data_type``, in a stable order: the whole definition first, then each key
    in sorted order. Empty when the definition is valid. An absent or empty
    definition is valid for every data type."""
    if definitions is None:
        return []
    if not isinstance(definitions, Mapping):
        return [ValueDefinitionProblem("type")]
    if not definitions:
        return []
    kind = KEY_KINDS.get(data_type or "")
    if kind is None:
        return [ValueDefinitionProblem("data_type")]
    if len(definitions) > MAX_ENTRIES:
        return [ValueDefinitionProblem("maxProperties", str(MAX_ENTRIES))]
    problems: list[ValueDefinitionProblem] = []
    for key in sorted(definitions, key=str):
        meaning = definitions[key]
        if not isinstance(key, str) or not isinstance(meaning, str):
            problems.append(ValueDefinitionProblem("type", key=str(key)))
        elif not key_is_canonical(kind, key):
            problems.append(ValueDefinitionProblem("propertyNames", kind, key))
        elif not meaning.strip():
            problems.append(ValueDefinitionProblem("minLength", "1", key))
        elif len(meaning) > MAX_TEXT:
            problems.append(ValueDefinitionProblem("maxLength", str(MAX_TEXT), key))
        elif meaning != meaning.strip():
            problems.append(ValueDefinitionProblem("whitespace", key=key))
    return problems
