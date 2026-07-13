"""Value-free diagnostic summaries for matrix variable discovery."""

from __future__ import annotations

import re
from typing import Any, Callable

PROMETHEUS_DATASOURCE_TYPE = "prometheus"
SUPPORTED_VARIABLE_TYPES = {
    "query", "datasource", "custom", "constant", "interval", "textbox", "adhoc", "groupby",
}


def variable_diagnostic(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    references: Callable[[Any], list[str]],
) -> dict[str, Any]:
    """Describe variable metadata without exposing its saved values or query."""
    if not isinstance(variable, dict):
        return _missing_variable_diagnostic()
    names = sorted(set(references((
        variable.get("datasource"), variable.get("query"), variable.get("definition"),
    ))))
    return {
        "found": "found", "type": _variable_type(variable.get("type")),
        "current": _saved_value_kind(variable.get("current"), "current"),
        "default": _saved_value_kind(variable, "default"),
        "references": names,
        "missing_references": [name for name in names if name not in context and not name.startswith("__")],
    }


def datasource_diagnostic(variable: dict[str, Any] | None, context: dict[str, Any]) -> dict[str, Any]:
    """Describe datasource resolution inputs without emitting its UID."""
    datasource = variable.get("datasource") if isinstance(variable, dict) else None
    datasource_type, datasource_uid, shape = _datasource_parts(datasource)
    reference = _reference_name(datasource_uid) or _reference_name(datasource_type)
    uid_value = context.get(reference) if reference else datasource_uid
    return {
        "shape": shape,
        "type": "prometheus" if str(datasource_type).lower() == PROMETHEUS_DATASOURCE_TYPE else _value_kind(datasource_type),
        "reference": "variable" if reference else "direct",
        "uid_present": uid_value not in (None, ""),
    }


def _missing_variable_diagnostic() -> dict[str, Any]:
    return {
        "found": "not_found", "type": "missing", "current": "missing", "default": "missing",
        "references": [], "missing_references": [],
    }


def _saved_value_kind(container: Any, field: str) -> str:
    if not isinstance(container, dict) or field not in container:
        return "missing"
    value = container[field]
    if field == "current" and isinstance(value, dict):
        value = value.get("value", value.get("text", _MISSING))
    return "missing" if value is _MISSING else _value_kind(value)


def _variable_type(value: Any) -> str:
    return str(value) if value in SUPPORTED_VARIABLE_TYPES else _value_kind(value)


def _datasource_parts(datasource: Any) -> tuple[Any, Any, str]:
    if isinstance(datasource, dict):
        return datasource.get("type"), datasource.get("uid"), "mapping"
    if isinstance(datasource, str):
        return datasource, datasource, "string"
    return None, None, "missing" if datasource is None else "unsupported"


def _reference_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\$\{([^}:]+)(?::[^}]+)?}|\$(\w+)", value)
    return (match.group(1) or match.group(2)) if match else None


def _value_kind(value: Any) -> str:
    if value is None:
        return "null"
    if value == "":
        return "empty_string"
    if isinstance(value, (str, int, float, bool)):
        return "nonempty_scalar"
    return "unsupported"


_MISSING = object()
