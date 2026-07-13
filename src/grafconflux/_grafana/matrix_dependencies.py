"""Dependency inference and ordering for render-matrix variables."""

from __future__ import annotations

import re
from typing import Any

from grafconflux._shared.grafana_models import ConfigurationError

REFERENCE_PATTERN = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::[^}]+)?}|\$([A-Za-z_][A-Za-z0-9_]*)|\[\[([A-Za-z_][A-Za-z0-9_]*)]]"
)
IGNORED_VARIABLE_FIELDS = {"current", "options"}


def ordered_matrix_variables(
    dashboard_name: str,
    matrix: dict[str, Any],
    dashboard: dict[str, Any],
) -> tuple[list[str], dict[str, list[str]]]:
    """Return stable topological order and resolved dependencies."""
    variables = matrix["variables"]
    dependencies = _matrix_dependencies(variables, dashboard)
    _validate_dependencies(dashboard_name, variables, dependencies)
    return _stable_topological_order(dashboard_name, list(variables), dependencies), dependencies


def _matrix_dependencies(
    variables: dict[str, dict[str, Any]],
    dashboard: dict[str, Any],
) -> dict[str, list[str]]:
    grafana_to_key = {
        str(spec.get("grafana_variable") or key): key
        for key, spec in variables.items()
    }
    dashboard_variables = {
        item.get("name"): item
        for item in dashboard.get("templating", {}).get("list", [])
        if isinstance(item, dict) and item.get("name")
    }
    return {
        key: _dependencies_for_variable(key, spec, dashboard_variables, grafana_to_key)
        for key, spec in variables.items()
    }


def _dependencies_for_variable(
    key: str,
    spec: dict[str, Any],
    dashboard_variables: dict[str, dict[str, Any]],
    grafana_to_key: dict[str, str],
) -> list[str]:
    if "depends_on" in spec:
        return configured_dependencies(spec)
    grafana_name = str(spec.get("grafana_variable") or key)
    variable = dashboard_variables.get(grafana_name, {})
    references = _references(variable)
    return _dedupe([
        grafana_to_key[name]
        for name in references
        if name in grafana_to_key and grafana_to_key[name] != key
    ])


def configured_dependencies(spec: dict[str, Any]) -> list[str]:
    value = spec.get("depends_on")
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) and item for item in value):
        return list(value)
    return [""]


def _references(value: Any) -> list[str]:
    if isinstance(value, str):
        return [next(group for group in match.groups() if group) for match in REFERENCE_PATTERN.finditer(value)]
    if isinstance(value, list):
        return [name for item in value for name in _references(item)]
    if isinstance(value, dict):
        return [
            name
            for field, item in value.items()
            if field not in IGNORED_VARIABLE_FIELDS
            for name in _references(item)
        ]
    return []


def _validate_dependencies(
    dashboard_name: str,
    variables: dict[str, dict[str, Any]],
    dependencies: dict[str, list[str]],
) -> None:
    for key, names in dependencies.items():
        unknown = [name for name in names if name not in variables]
        if unknown:
            path = f"dashboards.{dashboard_name}.render_matrix.variables.{key}.depends_on"
            raise ConfigurationError(f"{path}: unknown dependencies {unknown}.")


def _stable_topological_order(
    dashboard_name: str,
    keys: list[str],
    dependencies: dict[str, list[str]],
) -> list[str]:
    remaining = list(keys)
    ordered: list[str] = []
    while remaining:
        ready = [key for key in remaining if all(name in ordered for name in dependencies[key])]
        if not ready:
            cycle = ", ".join(remaining)
            raise ConfigurationError(
                f"dashboards.{dashboard_name}.render_matrix.variables: dependency cycle involving [{cycle}]."
            )
        ordered.extend(ready)
        remaining = [key for key in remaining if key not in ready]
    return ordered


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
