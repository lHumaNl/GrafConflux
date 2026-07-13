"""Saved Grafana variable context used during matrix discovery."""

from __future__ import annotations

from typing import Any

ALL_SENTINELS = {"$__all", "__all", "all"}


def dashboard_current_context(
    dashboard: dict[str, Any],
    exclude: set[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Return usable non-matrix saved values and their safe source labels."""
    context: dict[str, str] = {}
    sources: dict[str, str] = {}
    for variable in dashboard.get("templating", {}).get("list", []):
        name = variable.get("name") if isinstance(variable, dict) else None
        value, source = _dashboard_variable_context_value(variable)
        if name and name not in exclude and value is not None:
            context[str(name)] = value
            sources[str(name)] = source
    return context, sources


def dashboard_variable_current_value(variable: dict[str, Any] | None) -> str | None:
    """Return a non-empty saved current value, including current text fallback."""
    current = variable.get("current") if isinstance(variable, dict) else None
    if not isinstance(current, dict):
        return None
    for field in ("value", "text"):
        value = _normalized_dashboard_value(current.get(field))
        if value is not None:
            return value
    return None


def _dashboard_variable_context_value(variable: dict[str, Any] | None) -> tuple[str | None, str]:
    current = variable.get("current") if isinstance(variable, dict) else None
    if isinstance(current, dict):
        for field in ("value", "text"):
            if field not in current:
                continue
            if current[field] == "" and variable.get("type") == "query":
                return "", f"current.{field}"
            value = _normalized_dashboard_value(current[field])
            if value is not None:
                return value, f"current.{field}"
    if isinstance(variable, dict) and variable.get("type") == "query" and variable.get("default") == "":
        return "", "default"
    return None, "unavailable"


def _normalized_dashboard_value(value: Any) -> str | None:
    values = [item for item in _normalize_values(value) if item.lower() not in ALL_SENTINELS]
    return None if not values else values[0] if len(values) == 1 else "|".join(values)


def _normalize_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [item for raw in value for item in _normalize_values(raw)]
    return [] if value in (None, "") or isinstance(value, (dict, tuple, set)) else [str(value)]
