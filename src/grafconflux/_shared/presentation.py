"""Presentation-only variable helpers shared by static vars and render matrices."""

from __future__ import annotations

from typing import Any


PRESENTATION_KEYS = frozenset({"hide", "display_name", "value_aliases"})


def display_value(raw_value: Any, value_aliases: dict[str, str]) -> Any:
    """Apply exact aliases without changing the raw value shape or request identity."""
    if isinstance(raw_value, (list, tuple)):
        return [display_value(item, value_aliases) for item in raw_value]
    return value_aliases.get(str(raw_value), str(raw_value))


def default_hidden(raw_values: Any, value_aliases: dict[str, str]) -> bool:
    return bool(value_aliases) or effective_value_count(raw_values) > 1


def effective_value_count(raw_values: Any) -> int:
    if isinstance(raw_values, (list, tuple)):
        return len(raw_values)
    return 0 if raw_values is None else 1


def resolved_hidden(spec: dict[str, Any], raw_values: Any) -> bool:
    if "hide" in spec:
        return spec["hide"]
    return default_hidden(raw_values, spec.get("value_aliases") or {})


def display_name(key: str, spec: dict[str, Any]) -> str:
    return str(spec.get("display_name") or spec.get("alias") or key)
