"""Prometheus query and datasource resolution for matrix discovery."""

from __future__ import annotations

import re
from typing import Any

from grafconflux._grafana.matrix_context import dashboard_variable_current_value

PROMETHEUS_DATASOURCE_TYPE = "prometheus"


def prometheus_label_values_query(
    variable: dict[str, Any],
    context: dict[str, Any],
) -> tuple[str | None, str] | None:
    query_config = variable.get("query")
    modern = _modern_label_values_query(query_config, context)
    if modern is not None:
        return modern
    for query in (_variable_query_text(query_config), variable.get("definition")):
        if isinstance(query, str):
            parsed = _parse_label_values(_substitute_query_vars(query, context))
            if parsed is not None:
                return parsed
    return None


def resolved_datasource_type_uid(
    datasource: Any,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> tuple[str | None, str | None]:
    resolved_type, resolved_uid, _ = _resolved_datasource_details(datasource, context, config, dashboard)
    return resolved_type, resolved_uid


def _resolved_datasource_details(
    datasource: Any,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> tuple[str | None, str | None, str]:
    datasource_type, datasource_uid = _datasource_type_uid(datasource)
    ref_name = _datasource_reference(datasource_type, datasource_uid, config)
    if not ref_name:
        resolved_type = _resolved_context_value(datasource_type, context)
        resolved_uid = _resolved_context_value(datasource_uid, context)
        return _direct_or_context_uid(resolved_type, resolved_uid, datasource_uid, context, dashboard)
    variable = _dashboard_variable(dashboard, ref_name)
    resolved_type = _datasource_variable_type(variable) or _resolved_context_value(datasource_type, context)
    resolved_uid = _resolved_context_value(datasource_uid, context) or dashboard_variable_current_value(variable)
    return resolved_type, resolved_uid, "direct" if resolved_uid else "missing"


def datasource_resolution(
    datasource: Any,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> dict[str, str]:
    datasource_type, datasource_uid = _datasource_type_uid(datasource)
    ref_name = _datasource_reference(datasource_type, datasource_uid, config)
    resolved_type, resolved_uid, uid_source = _resolved_datasource_details(
        datasource, context, config, dashboard,
    )
    return {
        "source": "variable_reference" if ref_name else "direct" if datasource is not None else "missing",
        "type_status": _datasource_type_status(resolved_type, bool(ref_name)),
        "uid_status": _datasource_uid_status(resolved_uid, bool(ref_name)),
        "uid_source": uid_source,
    }


def _direct_or_context_uid(
    datasource_type: str | None,
    datasource_uid: str | None,
    raw_uid: Any,
    context: dict[str, Any],
    dashboard: dict[str, Any],
) -> tuple[str | None, str | None, str]:
    if datasource_uid:
        return datasource_type, datasource_uid, "direct"
    fallback_uid = _validated_prometheus_context_uid(datasource_type, raw_uid, context, dashboard)
    return datasource_type, fallback_uid, "datasource_context_validated" if fallback_uid else "missing"


def _validated_prometheus_context_uid(
    datasource_type: str | None,
    raw_uid: Any,
    context: dict[str, Any],
    dashboard: dict[str, Any],
) -> str | None:
    if raw_uid not in (None, "") or str(datasource_type).lower() != PROMETHEUS_DATASOURCE_TYPE:
        return None
    candidates = [_context_uid(variable, context) for variable in _dashboard_datasource_variables(dashboard)]
    valid = [uid for uid in candidates if uid is not None]
    return valid[0] if len(valid) == 1 else None


def _dashboard_datasource_variables(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    variables = dashboard.get("templating", {}).get("list", [])
    return [variable for variable in variables if isinstance(variable, dict) and variable.get("type") == "datasource"]


def _context_uid(variable: dict[str, Any], context: dict[str, Any]) -> str | None:
    name = variable.get("name")
    value = context.get(name) if isinstance(name, str) else None
    if _datasource_variable_type(variable) != PROMETHEUS_DATASOURCE_TYPE:
        return None
    return value if isinstance(value, str) and value.strip() else None


def _datasource_reference(datasource_type: Any, datasource_uid: Any, config: Any) -> str | None:
    configured = _configured_datasource_reference(datasource_type, datasource_uid, config)
    return configured or _variable_reference_name(datasource_uid) or _variable_reference_name(datasource_type)


def _configured_datasource_reference(
    datasource_type: Any,
    datasource_uid: Any,
    config: Any,
) -> str | None:
    datasource_vars = getattr(config, "datasource_vars", {}) or {}
    for value in (datasource_uid, datasource_type):
        ref_name = _variable_reference_name(value)
        if ref_name in datasource_vars:
            return ref_name
    return None


def _resolved_context_value(value: Any, context: dict[str, Any]) -> str | None:
    ref_name = _variable_reference_name(value)
    if ref_name:
        return str(context[ref_name]) if ref_name in context else None
    return str(value) if value not in (None, "") else None


def _variable_reference_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\$\{([^}:]+)(?::[^}]+)?}|\$(\w+)", value)
    return (match.group(1) or match.group(2)) if match else None


def _dashboard_variable(dashboard: dict[str, Any], name: str) -> dict[str, Any] | None:
    variables = dashboard.get("templating", {}).get("list", [])
    return next((item for item in variables if isinstance(item, dict) and item.get("name") == name), None)


def _datasource_variable_type(variable: dict[str, Any] | None) -> str | None:
    if not isinstance(variable, dict) or variable.get("type") != "datasource":
        return None
    query = variable.get("query")
    value = query if isinstance(query, str) else query.get("type") if isinstance(query, dict) else None
    return value.lower() if isinstance(value, str) and value else None


def _datasource_type_status(value: Any, referenced: bool) -> str:
    if value in (None, ""):
        return "unresolved_reference" if referenced else "missing"
    return "resolved_prometheus" if str(value).lower() == PROMETHEUS_DATASOURCE_TYPE else "resolved_other"


def _datasource_uid_status(value: Any, referenced: bool) -> str:
    if value in (None, ""):
        return "unresolved_reference" if referenced else "missing"
    return "resolved"


def _datasource_type_uid(datasource: Any) -> tuple[str | None, str | None]:
    if isinstance(datasource, dict):
        return datasource.get("type"), datasource.get("uid")
    return (datasource, datasource) if isinstance(datasource, str) else (None, None)


def _modern_label_values_query(
    query_config: Any,
    context: dict[str, Any],
) -> tuple[str | None, str] | None:
    if not isinstance(query_config, dict) or query_config.get("queryType") != "label_values":
        return None
    label = _substitute_query_vars(str(query_config.get("label") or ""), context)
    metric = _substitute_query_vars(str(query_config.get("query") or ""), context) or None
    return (metric, label) if _safe_label(label) and _safe_match(metric) else None


def _parse_label_values(query: str) -> tuple[str | None, str] | None:
    stripped = query.strip()
    if not stripped.startswith("label_values(") or not stripped.endswith(")"):
        return None
    metric, label = _split_label_values_args(stripped[len("label_values("):-1].strip())
    return (metric, label) if _safe_label(label) and _safe_match(metric) else None


def _split_label_values_args(inner: str) -> tuple[str | None, str]:
    if "," not in inner:
        return None, inner.strip()
    metric, label = inner.rsplit(",", 1)
    return metric.strip(), label.strip()


def _substitute_query_vars(query: str, context: dict[str, Any]) -> str:
    pattern = r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::[^}]+)?}|\$([A-Za-z_][A-Za-z0-9_]*)|\[\[([A-Za-z_][A-Za-z0-9_]*)]]"
    def replacement(match: re.Match[str]) -> str:
        name = next(group for group in match.groups() if group)
        return str(context.get(name, match.group(0)))

    return re.sub(pattern, replacement, query)


def _variable_query_text(query_config: Any) -> str | None:
    if isinstance(query_config, str):
        return query_config
    return query_config.get("query") if isinstance(query_config, dict) and isinstance(query_config.get("query"), str) else None


def _safe_label(label: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", label or ""))


def _safe_match(metric: str | None) -> bool:
    return metric is None or (0 < len(metric) <= 2000 and not re.search(r"[$\r\n]", metric))
