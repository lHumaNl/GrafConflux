"""Saved Grafana variable context used during matrix discovery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ALL_SENTINELS = {"$__all", "__all", "all"}


@dataclass(frozen=True)
class DiscoveryContextAssembly:
    """Effective context plus value-free provenance suitable for diagnostics."""

    values: dict[str, Any]
    sources: dict[str, str]
    value_kinds: dict[str, str]
    dashboard_sources: dict[str, str]
    exclusions: tuple[tuple[str, str], ...]


def assemble_discovery_context(
    dashboard: dict[str, Any],
    exclude_saved: set[str],
    parent_context: dict[str, Any],
    explicit_vars: dict[str, Any],
) -> DiscoveryContextAssembly:
    """Apply dashboard < resolved parent < explicit variable precedence."""
    values, dashboard_sources, exclusions = _dashboard_context(dashboard, exclude_saved)
    sources = {name: f"dashboard.{source}" for name, source in dashboard_sources.items()}
    _overlay_context(values, sources, dashboard_sources, exclusions, parent_context, "resolved_parent")
    _overlay_context(values, sources, dashboard_sources, exclusions, explicit_vars, "explicit_vars")
    kinds = {name: context_value_kind(value) for name, value in values.items()}
    return DiscoveryContextAssembly(
        values, sources, kinds, dashboard_sources, tuple(sorted(set(exclusions))),
    )


def dashboard_current_context(
    dashboard: dict[str, Any],
    exclude: set[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Return usable non-matrix saved values and their safe source labels."""
    context, sources, _ = _dashboard_context(dashboard, exclude)
    return context, sources


def _dashboard_context(
    dashboard: dict[str, Any],
    exclude: set[str],
) -> tuple[dict[str, str], dict[str, str], list[tuple[str, str]]]:
    context: dict[str, str] = {}
    sources: dict[str, str] = {}
    exclusions: list[tuple[str, str]] = []
    for variable in _dashboard_variables(dashboard):
        name = variable.get("name") if isinstance(variable, dict) else None
        if not name:
            continue
        normalized_name = str(name)
        if normalized_name in exclude:
            exclusions.append((normalized_name, "saved_current_excluded_matrix"))
            continue
        value, source = _dashboard_variable_context_value(variable)
        if value is None:
            exclusions.append((normalized_name, source))
            continue
        context[normalized_name] = value
        sources[normalized_name] = source
    return context, sources, exclusions


def _overlay_context(
    values: dict[str, Any],
    sources: dict[str, str],
    dashboard_sources: dict[str, str],
    exclusions: list[tuple[str, str]],
    overlay: dict[str, Any],
    source: str,
) -> None:
    for raw_name, value in _public_items(overlay):
        name = str(raw_name)
        dashboard_sources.pop(name, None)
        if value is None:
            values.pop(name, None)
            sources.pop(name, None)
            exclusions.append((name, f"{source}_null"))
            continue
        values[name] = value
        sources[name] = source


def _dashboard_variables(dashboard: dict[str, Any]) -> list[Any]:
    templating = dashboard.get("templating", {}) if isinstance(dashboard, dict) else {}
    variables = templating.get("list", []) if isinstance(templating, dict) else []
    return variables if isinstance(variables, list) else []


def _public_items(context: dict[str, Any]) -> list[tuple[Any, Any]]:
    if not isinstance(context, dict):
        return []
    return [(name, value) for name, value in context.items() if not str(name).startswith("__")]


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
    return None, _unavailable_reason(current)


def _unavailable_reason(current: Any) -> str:
    if not isinstance(current, dict):
        return "saved_current_missing"
    present = [current[field] for field in ("value", "text") if field in current]
    if not present:
        return "saved_current_missing"
    if all(value is None for value in present):
        return "saved_current_null"
    return "saved_current_unusable"


def context_value_kind(value: Any) -> str:
    """Return a coarse value kind without exposing the value."""
    if value == "":
        return "empty_string"
    if isinstance(value, str):
        return "scalar_string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, (list, tuple, set)):
        return "sequence"
    if isinstance(value, dict):
        return "mapping"
    return "other"


def _normalized_dashboard_value(value: Any) -> str | None:
    values = [item for item in _normalize_values(value) if item.lower() not in ALL_SENTINELS]
    return None if not values else values[0] if len(values) == 1 else "|".join(values)


def _normalize_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [item for raw in value for item in _normalize_values(raw)]
    return [] if value in (None, "") or isinstance(value, (dict, tuple, set)) else [str(value)]
