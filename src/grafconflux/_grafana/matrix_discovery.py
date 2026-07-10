"""Grafana-backed value discovery for render matrix variables."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

ALL_SENTINELS = {"$__all", "__all", "all"}
PROMETHEUS_DATASOURCE_TYPE = "prometheus"


@dataclass(frozen=True)
class MatrixValueResult:
    values: list[str]
    provenance: dict[str, Any]


def resolve_values_from(
    key: str,
    spec: dict[str, Any],
    dashboard: dict[str, Any],
    timestamp: Any,
    context: dict[str, str],
    static_vars: dict[str, Any],
    session: Any = None,
    config: Any = None,
) -> MatrixValueResult:
    source_name = _values_from_variable_name(spec.get("values_from"), spec, key)
    effective_context = {**static_vars, **_public_context(context)}
    if source_name in effective_context:
        return _context_result(source_name, effective_context[source_name], timestamp)
    variable = _dashboard_variable(dashboard, source_name)
    api_result = _api_values(variable, source_name, timestamp, effective_context, session, config, dashboard)
    if api_result is not None:
        return api_result
    return _templating_result(variable, source_name, timestamp, effective_context)


def _api_values(variable: dict[str, Any] | None, source_name: str, timestamp: Any,
                context: dict[str, Any], session: Any, config: Any,
                dashboard: dict[str, Any]) -> MatrixValueResult | None:
    request = _prometheus_request(variable, timestamp, context, config, dashboard)
    if request is None or session is None:
        return None
    try:
        response = session.get(request["url"], params=request["params"], timeout=getattr(config, "timeout", None))
    except Exception as error:  # pragma: no cover - defensive logging only
        logger.warning("Matrix values_from discovery failed variable=%s error=%s", source_name, error)
        return MatrixValueResult([], _provenance(source_name, timestamp, context, "grafana_api", "request_failed"))
    values = _response_values(response)
    if values is None:
        return None
    provenance = _provenance(source_name, timestamp, context, "grafana_api", "prometheus_label_values", request)
    return MatrixValueResult(values, provenance)


def _prometheus_request(variable: dict[str, Any] | None, timestamp: Any,
                        context: dict[str, Any], config: Any,
                        dashboard: dict[str, Any]) -> dict[str, Any] | None:
    if not _is_prometheus_query_variable(variable, context, config, dashboard) or config is None:
        return None
    query = _variable_query_text(variable.get("query")) if variable else None
    parsed = _parse_label_values(_substitute_query_vars(query or "", context))
    if parsed is None:
        return None
    datasource_uid = _resolved_datasource_type_uid(variable.get("datasource"), context, config, dashboard)[1]
    params = _prometheus_params(parsed[0], timestamp, variable, context)
    return {"url": _prometheus_url(config.grafana_base_url, datasource_uid, parsed[1]), "params": params}


def _prometheus_params(metric: str | None, timestamp: Any, variable: dict[str, Any], context: dict[str, Any]) -> dict[str, str]:
    params = {"match[]": metric} if metric else {}
    params.update({"start": str(timestamp.start_time_timestamp), "end": str(timestamp.end_time_timestamp)})
    params.update({f"var-{_context_grafana_key(name, variable)}": str(value) for name, value in context.items()})
    return params


def _context_grafana_key(name: str, variable: dict[str, Any]) -> str:
    return str(name if name != variable.get("name") else variable.get("name"))


def _prometheus_url(base_url: str, datasource_uid: str, label: str) -> str:
    return f"{base_url}/api/datasources/proxy/uid/{quote(str(datasource_uid), safe='')}/api/v1/label/{quote(label, safe='')}/values"


def _response_values(response: Any) -> list[str] | None:
    if getattr(response, "status_code", None) != 200:
        return []
    try:
        payload = response.json()
    except Exception as error:  # pragma: no cover - defensive logging only
        logger.warning("Matrix values_from discovery returned invalid JSON error=%s", error)
        return None
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return [value for value in _normalize_values(data) if value.lower() not in ALL_SENTINELS]


def _templating_result(variable: dict[str, Any] | None, source_name: str, timestamp: Any,
                       context: dict[str, Any]) -> MatrixValueResult:
    values = _dashboard_variable_options(variable)
    provenance = _provenance(source_name, timestamp, context, "templating_options", "no_safe_api_endpoint")
    return MatrixValueResult([value for value in values if value.lower() not in ALL_SENTINELS], provenance)


def _context_result(source_name: str, value: Any, timestamp: Any) -> MatrixValueResult:
    values = _normalize_values(value)
    return MatrixValueResult(values, _provenance(source_name, timestamp, {source_name: value}, "configured_context", "static_or_parent"))


def _provenance(source_name: str, timestamp: Any, context: dict[str, Any], source: str,
                method: str, request: dict[str, Any] | None = None) -> dict[str, Any]:
    provenance = {"variable": source_name, "source": source, "method": method, "from": str(timestamp.start_time_timestamp),
                  "to": str(timestamp.end_time_timestamp), "context_vars": sorted(str(key) for key in context)}
    if request:
        provenance["request"] = {"url": request["url"], "params": dict(request["params"])}
    return provenance


def _values_from_variable_name(source: Any, spec: dict[str, Any], key: str) -> str:
    return str(spec.get("grafana_variable") or key)


def _dashboard_variable(dashboard: dict[str, Any], variable_name: str) -> dict[str, Any] | None:
    variables = dashboard.get("templating", {}).get("list", [])
    return next((item for item in variables if item.get("name") == variable_name), None)


def _dashboard_variable_options(variable: dict[str, Any] | None) -> list[str]:
    options = variable.get("options", []) if isinstance(variable, dict) else []
    return _dedupe([value for option in options for value in _option_values(option)])


def _option_values(option: Any) -> list[str]:
    value = option.get("value", option.get("text")) if isinstance(option, dict) else option
    return _normalize_values(value)


def _normalize_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [item for raw in value for item in _normalize_values(raw)]
    return [] if value in (None, "") or isinstance(value, (dict, tuple, set)) else [str(value)]


def _substitute_query_vars(query: str, context: dict[str, Any]) -> str:
    return re.sub(r"\$\{([^}]+)}|\$(\w+)", lambda match: str(context.get(match.group(1) or match.group(2), match.group(0))), query)


def _parse_label_values(query: str) -> tuple[str | None, str] | None:
    if not query.strip().startswith("label_values(") or not query.strip().endswith(")"):
        return None
    inner = query.strip()[len("label_values("):-1].strip()
    metric, label = _split_label_values_args(inner)
    return (metric, label) if _is_safe_prometheus_label(label) and _is_safe_prometheus_match(metric) else None


def _split_label_values_args(inner: str) -> tuple[str | None, str]:
    if "," not in inner:
        return None, inner.strip()
    metric, label = inner.rsplit(",", 1)
    return metric.strip(), label.strip()


def _is_prometheus_query_variable(variable: dict[str, Any] | None, context: dict[str, Any],
                                  config: Any, dashboard: dict[str, Any]) -> bool:
    datasource_type, datasource_uid = _resolved_datasource_type_uid(
        variable.get("datasource") if variable else None, context, config, dashboard)
    return bool(
        variable and variable.get("type") == "query"
        and str(datasource_type).lower() == PROMETHEUS_DATASOURCE_TYPE and datasource_uid
    )


def _resolved_datasource_type_uid(datasource: Any, context: dict[str, Any], config: Any,
                                  dashboard: dict[str, Any]) -> tuple[str | None, str | None]:
    datasource_type, datasource_uid = _datasource_type_uid(datasource)
    ref_name = _datasource_ref_name(datasource_type, datasource_uid, config)
    if not ref_name:
        return datasource_type, datasource_uid
    variable = _dashboard_variable(dashboard, ref_name)
    resolved_type = _datasource_variable_type(variable) or _resolved_context_value(datasource_type, context)
    resolved_uid = _resolved_context_value(datasource_uid, context)
    return resolved_type, resolved_uid


def _datasource_ref_name(datasource_type: Any, datasource_uid: Any, config: Any) -> str | None:
    datasource_vars = getattr(config, "datasource_vars", {}) or {}
    for value in (datasource_uid, datasource_type):
        ref_name = _variable_reference_name(value)
        if ref_name in datasource_vars:
            return ref_name
    return None


def _resolved_context_value(value: Any, context: dict[str, Any]) -> str | None:
    ref_name = _variable_reference_name(value)
    if ref_name and ref_name in context:
        return str(context[ref_name])
    return str(value) if value not in (None, "") else None


def _variable_reference_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\$\{([^}]+)}|\$(\w+)", value)
    if not match:
        return None
    return match.group(1) or match.group(2)


def _datasource_variable_type(variable: dict[str, Any] | None) -> str | None:
    if not isinstance(variable, dict) or variable.get("type") != "datasource":
        return None
    query = variable.get("query")
    if isinstance(query, str) and query:
        return query
    return query.get("type") if isinstance(query, dict) else None


def _variable_query_text(query_config: Any) -> str | None:
    if isinstance(query_config, str):
        return query_config
    return query_config.get("query") if isinstance(query_config, dict) and isinstance(query_config.get("query"), str) else None


def _datasource_type_uid(datasource: Any) -> tuple[str | None, str | None]:
    if isinstance(datasource, dict):
        return datasource.get("type"), datasource.get("uid")
    return (datasource, datasource) if isinstance(datasource, str) else (None, None)


def _is_safe_prometheus_label(label: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", label or ""))


def _is_safe_prometheus_match(metric: str | None) -> bool:
    return metric is None or (0 < len(metric) <= 300 and not re.search(r"[$()\r\n]", metric))


def _public_context(context: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in context.items() if not str(key).startswith("__")}


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
