"""Structured Grafana-backed discovery for render-matrix variables."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

ALL_SENTINELS = {"$__all", "__all", "all"}
PROMETHEUS_DATASOURCE_TYPE = "prometheus"
SAFE_LOG_VALUE_LIMIT = 5
SAFE_LOG_VALUE_LENGTH = 64
SENSITIVE_NAME_PATTERN = re.compile(r"(?:pass|secret|token|cookie|credential|authorization|api.?key)", re.I)
SECRET_VALUE_PATTERN = re.compile(
    r"(?:^Bearer\s+|^Basic\s+|(?:token|secret|password|api.?key)=|^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$|^[a-fA-F0-9]{32,}$)",
    re.I,
)


class MatrixDiscoveryStatus(str, Enum):
    RESOLVED = "resolved"
    EMPTY = "empty"
    UNSUPPORTED = "unsupported"
    UNRESOLVED = "unresolved"
    FAILED = "failed"


@dataclass(frozen=True)
class MatrixValueResult:
    status: MatrixDiscoveryStatus
    values: list[str]
    provenance: dict[str, Any]

    @property
    def authoritative(self) -> bool:
        return self.status in {MatrixDiscoveryStatus.RESOLVED, MatrixDiscoveryStatus.EMPTY}


class MatrixValueResolver:
    """Resolve dynamic values with timestamp/context-aware caching."""

    def __init__(
        self,
        dashboard: dict[str, Any],
        session: Any,
        config: Any,
        browser_fallback: Any = None,
        dynamic_variable_names: set[str] | None = None,
    ) -> None:
        self.dashboard = dashboard
        self.session = session
        self.config = config
        self.browser_fallback = browser_fallback
        self.dynamic_variable_names = dynamic_variable_names or set()
        self.cache: dict[tuple[Any, ...], MatrixValueResult] = {}

    def resolve(
        self,
        key: str,
        spec: dict[str, Any],
        timestamp: Any,
        context: dict[str, Any],
        static_vars: dict[str, Any],
    ) -> MatrixValueResult:
        source_name = str(spec.get("grafana_variable") or key)
        effective_context = self._effective_context(source_name, context, static_vars)
        cache_key = _cache_key(source_name, timestamp, effective_context)
        if cache_key in self.cache:
            return self.cache[cache_key]
        result = self._discover(source_name, timestamp, effective_context)
        self.cache[cache_key] = result
        _log_result(source_name, timestamp, result)
        return result

    def _effective_context(
        self,
        source_name: str,
        context: dict[str, Any],
        static_vars: dict[str, Any],
    ) -> dict[str, Any]:
        explicit = {**static_vars, **_public_context(context)}
        defaults = _dashboard_current_context(
            self.dashboard,
            self.dynamic_variable_names | {source_name},
        )
        return {**defaults, **explicit}

    def _discover(
        self,
        source_name: str,
        timestamp: Any,
        context: dict[str, Any],
    ) -> MatrixValueResult:
        variable = _dashboard_variable(self.dashboard, source_name)
        api_result = _prometheus_result(
            variable, source_name, timestamp, context, self.session, self.config, self.dashboard,
        )
        if api_result.authoritative or self.browser_fallback is None:
            return api_result
        fallback_result = self.browser_fallback.discover(source_name, variable, timestamp, context)
        if fallback_result.authoritative:
            return fallback_result
        return _combined_failure(api_result, fallback_result)


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
    """Compatibility wrapper for callers that do not need shared caching."""
    return MatrixValueResolver(dashboard, session, config).resolve(
        key, spec, timestamp, context, static_vars,
    )


def _prometheus_result(
    variable: dict[str, Any] | None,
    source_name: str,
    timestamp: Any,
    context: dict[str, Any],
    session: Any,
    config: Any,
    dashboard: dict[str, Any],
) -> MatrixValueResult:
    parsed = _prometheus_query(variable, context, config, dashboard)
    if parsed is None:
        return _result(MatrixDiscoveryStatus.UNSUPPORTED, [], source_name, timestamp, context, "grafana_api", "no_supported_adapter")
    metric, label, datasource_uid = parsed
    if not datasource_uid:
        return _result(MatrixDiscoveryStatus.UNRESOLVED, [], source_name, timestamp, context, "grafana_api", "datasource_unresolved")
    if session is None:
        return _result(MatrixDiscoveryStatus.UNRESOLVED, [], source_name, timestamp, context, "grafana_api", "session_unavailable")
    method = "prometheus_series" if metric else "prometheus_label_values"
    url = _prometheus_url(config.grafana_base_url, datasource_uid, label, bool(metric))
    params = _prometheus_params(metric, timestamp)
    try:
        response = session.get(url, params=params, timeout=getattr(config, "timeout", None))
    except Exception as error:
        logger.warning("Matrix discovery request failed variable=%s error_type=%s", safe_discovery_variable(source_name), type(error).__name__)
        return _result(MatrixDiscoveryStatus.FAILED, [], source_name, timestamp, context, "grafana_api", method)
    return _prometheus_response_result(response, source_name, timestamp, context, method, label)


def _prometheus_query(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> tuple[str | None, str, str | None] | None:
    if not isinstance(variable, dict) or variable.get("type") != "query" or config is None:
        return None
    datasource_type, datasource_uid = _resolved_datasource_type_uid(
        variable.get("datasource"), context, config, dashboard,
    )
    if str(datasource_type).lower() != PROMETHEUS_DATASOURCE_TYPE:
        return None
    parsed = _prometheus_label_values_query(variable, context)
    return None if parsed is None else (parsed[0], parsed[1], datasource_uid)


def _prometheus_response_result(
    response: Any,
    source_name: str,
    timestamp: Any,
    context: dict[str, Any],
    method: str,
    label: str,
) -> MatrixValueResult:
    if getattr(response, "status_code", None) != 200:
        return _result(MatrixDiscoveryStatus.FAILED, [], source_name, timestamp, context, "grafana_api", method)
    try:
        payload = response.json()
    except Exception as error:
        logger.warning("Matrix discovery returned invalid JSON variable=%s error_type=%s", safe_discovery_variable(source_name), type(error).__name__)
        return _result(MatrixDiscoveryStatus.FAILED, [], source_name, timestamp, context, "grafana_api", method)
    values = _prometheus_payload_values(payload, method, label)
    if values is None:
        return _result(MatrixDiscoveryStatus.UNRESOLVED, [], source_name, timestamp, context, "grafana_api", method)
    status = MatrixDiscoveryStatus.RESOLVED if values else MatrixDiscoveryStatus.EMPTY
    return _result(status, values, source_name, timestamp, context, "grafana_api", method)


def _prometheus_payload_values(payload: Any, method: str, label: str) -> list[str] | None:
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return None
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    if method == "prometheus_series":
        if not all(isinstance(item, dict) for item in data):
            return None
        values = [str(item[label]) for item in data if item.get(label) not in (None, "")]
    else:
        if any(isinstance(item, (dict, list, tuple, set)) for item in data):
            return None
        values = [str(item) for item in data if item not in (None, "")]
    return _dedupe([value for value in values if value.lower() not in ALL_SENTINELS])


def _prometheus_params(metric: str | None, timestamp: Any) -> dict[str, str]:
    params = {
        "start": _prometheus_seconds(timestamp.start_time_timestamp),
        "end": _prometheus_seconds(timestamp.end_time_timestamp),
    }
    if metric:
        params["match[]"] = metric
    return params


def _prometheus_seconds(milliseconds: Any) -> str:
    value = int(milliseconds)
    return str(value // 1000) if value % 1000 == 0 else str(value / 1000)


def _prometheus_url(base_url: str, datasource_uid: str, label: str, series: bool) -> str:
    proxy = f"{base_url}/api/datasources/proxy/uid/{quote(str(datasource_uid), safe='')}/api/v1"
    return f"{proxy}/series" if series else f"{proxy}/label/{quote(label, safe='')}/values"


def _result(
    status: MatrixDiscoveryStatus,
    values: list[str],
    variable: str,
    timestamp: Any,
    context: dict[str, Any],
    source: str,
    method: str,
) -> MatrixValueResult:
    provenance = {
        "variable": variable,
        "status": status.value,
        "source": source,
        "method": method,
        "timestamp_id": timestamp.id_time,
        "from": str(timestamp.start_time_timestamp),
        "to": str(timestamp.end_time_timestamp),
        "context_vars": sorted(str(key) for key in context),
        "count": len(values),
    }
    return MatrixValueResult(status, values, provenance)


def _combined_failure(api_result: MatrixValueResult, fallback_result: MatrixValueResult) -> MatrixValueResult:
    status = MatrixDiscoveryStatus.FAILED if MatrixDiscoveryStatus.FAILED in {
        api_result.status, fallback_result.status,
    } else MatrixDiscoveryStatus.UNRESOLVED
    provenance = dict(fallback_result.provenance)
    provenance.update({"status": status.value, "api_status": api_result.status.value})
    return MatrixValueResult(status, [], provenance)


def _log_result(variable: str, timestamp: Any, result: MatrixValueResult) -> None:
    log = logger.info if result.authoritative else logger.warning
    log(
        "Matrix discovery variable=%s timestamp_id=%s range=%s..%s status=%s source=%s method=%s count=%s values=%s",
        safe_discovery_variable(variable), timestamp.id_time, timestamp.start_time_timestamp,
        timestamp.end_time_timestamp, result.status.value, result.provenance.get("source"),
        result.provenance.get("method"), len(result.values), safe_discovery_values(variable, result.values),
    )


def safe_discovery_values(variable: str, values: list[str]) -> list[str]:
    if SENSITIVE_NAME_PATTERN.search(variable):
        return ["<redacted>"] if values else []
    safe = [_safe_discovery_value(value) for value in values[:SAFE_LOG_VALUE_LIMIT]]
    if len(values) > SAFE_LOG_VALUE_LIMIT:
        safe.append(f"...(+{len(values) - SAFE_LOG_VALUE_LIMIT})")
    return safe


def _safe_discovery_value(value: str) -> str:
    text = str(value)
    if SECRET_VALUE_PATTERN.search(text):
        return "<redacted>"
    return text if len(text) <= SAFE_LOG_VALUE_LENGTH else text[:SAFE_LOG_VALUE_LENGTH] + "..."


def safe_discovery_variable(variable: str) -> str:
    return "<sensitive-variable>" if SENSITIVE_NAME_PATTERN.search(variable) else variable


def _cache_key(source_name: str, timestamp: Any, context: dict[str, Any]) -> tuple[Any, ...]:
    normalized_context = tuple(sorted((str(key), repr(value)) for key, value in context.items()))
    return (
        source_name, timestamp.id_time, timestamp.start_time_timestamp,
        timestamp.end_time_timestamp, normalized_context,
    )


def _dashboard_variable(dashboard: dict[str, Any], variable_name: str) -> dict[str, Any] | None:
    variables = dashboard.get("templating", {}).get("list", [])
    return next((item for item in variables if item.get("name") == variable_name), None)


def _dashboard_current_context(dashboard: dict[str, Any], exclude: set[str]) -> dict[str, str]:
    context: dict[str, str] = {}
    for variable in dashboard.get("templating", {}).get("list", []):
        name = variable.get("name") if isinstance(variable, dict) else None
        value = _dashboard_variable_current_value(variable)
        if name and name not in exclude and value is not None:
            context[str(name)] = value
    return context


def _dashboard_variable_current_value(variable: dict[str, Any] | None) -> str | None:
    current = variable.get("current") if isinstance(variable, dict) else None
    if not isinstance(current, dict):
        return None
    values = _normalize_values(current.get("value") or current.get("text"))
    values = [value for value in values if value.lower() not in ALL_SENTINELS]
    return None if not values else values[0] if len(values) == 1 else "|".join(values)


def _normalize_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [item for raw in value for item in _normalize_values(raw)]
    return [] if value in (None, "") or isinstance(value, (dict, tuple, set)) else [str(value)]


def _substitute_query_vars(query: str, context: dict[str, Any]) -> str:
    pattern = r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::[^}]+)?}|\$([A-Za-z_][A-Za-z0-9_]*)|\[\[([A-Za-z_][A-Za-z0-9_]*)]]"
    return re.sub(pattern, lambda match: str(context.get(next(group for group in match.groups() if group), match.group(0))), query)


def _prometheus_label_values_query(variable: dict[str, Any], context: dict[str, Any]) -> tuple[str | None, str] | None:
    query_config = variable.get("query")
    modern = _modern_prometheus_label_values_query(query_config, context)
    if modern is not None:
        return modern
    for query in (_variable_query_text(query_config), variable.get("definition")):
        if isinstance(query, str):
            parsed = _parse_label_values(_substitute_query_vars(query, context))
            if parsed is not None:
                return parsed
    return None


def _modern_prometheus_label_values_query(query_config: Any, context: dict[str, Any]) -> tuple[str | None, str] | None:
    if not isinstance(query_config, dict) or query_config.get("queryType") != "label_values":
        return None
    label = _substitute_query_vars(str(query_config.get("label") or ""), context)
    metric = _substitute_query_vars(str(query_config.get("query") or ""), context) or None
    return (metric, label) if _is_safe_prometheus_label(label) and _is_safe_prometheus_match(metric) else None


def _parse_label_values(query: str) -> tuple[str | None, str] | None:
    stripped = query.strip()
    if not stripped.startswith("label_values(") or not stripped.endswith(")"):
        return None
    metric, label = _split_label_values_args(stripped[len("label_values("):-1].strip())
    return (metric, label) if _is_safe_prometheus_label(label) and _is_safe_prometheus_match(metric) else None


def _split_label_values_args(inner: str) -> tuple[str | None, str]:
    if "," not in inner:
        return None, inner.strip()
    metric, label = inner.rsplit(",", 1)
    return metric.strip(), label.strip()


def _resolved_datasource_type_uid(
    datasource: Any,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> tuple[str | None, str | None]:
    datasource_type, datasource_uid = _datasource_type_uid(datasource)
    ref_name = _datasource_ref_name(datasource_type, datasource_uid, config) or _variable_reference_name(datasource_uid) or _variable_reference_name(datasource_type)
    if not ref_name:
        return _resolved_context_value(datasource_type, context), _resolved_context_value(datasource_uid, context)
    variable = _dashboard_variable(dashboard, ref_name)
    resolved_type = _datasource_variable_type(variable) or _resolved_context_value(datasource_type, context)
    resolved_uid = _resolved_context_value(datasource_uid, context) or _dashboard_variable_current_value(variable)
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
    if ref_name:
        return str(context[ref_name]) if ref_name in context else None
    return str(value) if value not in (None, "") else None


def _variable_reference_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\$\{([^}:]+)(?::[^}]+)?}|\$(\w+)", value)
    return (match.group(1) or match.group(2)) if match else None


def _datasource_variable_type(variable: dict[str, Any] | None) -> str | None:
    if not isinstance(variable, dict) or variable.get("type") != "datasource":
        return None
    query = variable.get("query")
    return query if isinstance(query, str) and query else query.get("type") if isinstance(query, dict) else None


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
    return metric is None or (0 < len(metric) <= 2000 and not re.search(r"[$\r\n]", metric))


def _public_context(context: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in context.items() if not str(key).startswith("__")}


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
