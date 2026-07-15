"""Structured Grafana-backed discovery for render-matrix variables."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from grafconflux._grafana.matrix_context import (
    DiscoveryContextAssembly,
    assemble_discovery_context,
)
from grafconflux._grafana.matrix_prometheus import (
    datasource_resolution as _datasource_resolution,
    prometheus_label_values_query as _prometheus_label_values_query,
    resolved_datasource_type_uid as _resolved_datasource_type_uid,
)
from grafconflux._grafana.matrix_prometheus_metadata import (
    PROXY_UID_ROUTE,
    UID_RESOURCES_ROUTE,
    prometheus_params as _prometheus_params,
    prometheus_payload_values as _prometheus_payload_values,
    prometheus_seconds as _prometheus_seconds,
    prometheus_url as _prometheus_url,
)

logger = logging.getLogger(__name__)

PROMETHEUS_DATASOURCE_TYPE = "prometheus"
SENSITIVE_NAME_PATTERN = re.compile(r"(?:pass|secret|token|cookie|credential|authorization|api.?key)", re.I)


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
        assembly = self._effective_context(
            source_name, timestamp, context, static_vars,
        )
        cache_key = _cache_key(source_name, timestamp, assembly.values, assembly.sources)
        if cache_key in self.cache:
            return self.cache[cache_key]
        result = self._discover(source_name, timestamp, assembly.values)
        result = _with_context_assembly(result, assembly)
        self.cache[cache_key] = result
        _log_result(source_name, timestamp, _public_context(context), result)
        return result

    def _effective_context(
        self,
        source_name: str,
        timestamp: Any,
        context: dict[str, Any],
        static_vars: dict[str, Any],
    ) -> DiscoveryContextAssembly:
        assembly = assemble_discovery_context(
            self.dashboard,
            self.dynamic_variable_names | {source_name},
            _public_context(context),
            _public_context(static_vars),
        )
        return assembly

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
    return MatrixValueResolver(dashboard, session, config).resolve(key, spec, timestamp, context, static_vars)


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
    diagnostics = _adapter_diagnostics(variable, context, config, dashboard, parsed)
    if parsed is None:
        method = _prometheus_adapter_failure(variable, context, config, dashboard)
        result = _result(MatrixDiscoveryStatus.UNSUPPORTED, [], source_name, timestamp, context, "grafana_api", method)
        return _with_adapter_diagnostics(result, diagnostics)
    metric, label, datasource_uid = parsed
    if not datasource_uid:
        result = _result(MatrixDiscoveryStatus.UNRESOLVED, [], source_name, timestamp, context, "grafana_api", "invalid_or_missing_context")
        return _with_adapter_diagnostics(result, diagnostics)
    if session is None:
        result = _result(MatrixDiscoveryStatus.UNRESOLVED, [], source_name, timestamp, context, "grafana_api", "session_unavailable")
        return _with_adapter_diagnostics(result, diagnostics, "session_unavailable")
    method = "prometheus_series" if metric else "prometheus_label_values"
    params = _prometheus_params(metric, timestamp)
    response, error = _prometheus_request(
        session, config, datasource_uid, label, bool(metric), params,
        PROXY_UID_ROUTE,
    )
    if error is not None:
        result = _result(MatrixDiscoveryStatus.FAILED, [], source_name, timestamp, context, "grafana_api", method)
        return _with_adapter_diagnostics(
            result, {**diagnostics, "error_type": type(error).__name__}, "request_failed",
        )
    if method == "prometheus_series" and getattr(response, "status_code", None) == 404:
        response, error = _prometheus_request(
            session, config, datasource_uid, label, True, params,
            UID_RESOURCES_ROUTE,
        )
        if error is not None:
            result = _result(MatrixDiscoveryStatus.FAILED, [], source_name, timestamp, context, "grafana_api", method)
            return _with_adapter_diagnostics(
                result, {**diagnostics, "error_type": type(error).__name__}, "request_failed",
            )
    result = _prometheus_response_result(response, source_name, timestamp, context, method, label)
    return _with_adapter_diagnostics(result, diagnostics)


def _prometheus_request(
    session: Any, config: Any, datasource_uid: str, label: str, series: bool,
    params: dict[str, str], route_family: str,
) -> tuple[Any, Exception | None]:
    url = _prometheus_url(config.grafana_base_url, datasource_uid, label, series, route_family)
    try:
        response = session.get(url, params=params, timeout=getattr(config, "timeout", None))
    except Exception as error:
        return None, error
    return response, None


def _adapter_diagnostics(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
    parsed: tuple[str | None, str, str | None] | None,
) -> dict[str, Any]:
    datasource = variable.get("datasource") if isinstance(variable, dict) else None
    resolution = _datasource_resolution(datasource, context, config, dashboard)
    diagnosis = _adapter_diagnosis(variable, context, config, resolution, parsed)
    diagnostics: dict[str, Any] = {"diagnosis": diagnosis, "datasource_resolution": resolution}
    missing = _missing_context_names(variable, context)
    if missing:
        diagnostics["missing_context_vars"] = missing
    return diagnostics


def _adapter_diagnosis(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    config: Any,
    resolution: dict[str, str],
    parsed: tuple[str | None, str, str | None] | None,
) -> str:
    if not isinstance(variable, dict):
        return "variable_missing"
    if variable.get("type") != "query":
        return "variable_not_query"
    if config is None:
        return "config_unavailable"
    if resolution["type_status"] != "resolved_prometheus":
        return f"datasource_type_{resolution['type_status']}"
    if resolution["uid_status"] != "resolved":
        return f"datasource_uid_{resolution['uid_status']}"
    if _missing_context_names(variable, context):
        return "query_context_missing"
    return "resolved" if parsed is not None else "unsupported_query"


def _with_adapter_diagnostics(
    result: MatrixValueResult,
    diagnostics: dict[str, Any],
    diagnosis: str | None = None,
) -> MatrixValueResult:
    metadata = dict(diagnostics)
    if diagnosis is not None:
        metadata["diagnosis"] = diagnosis
    return MatrixValueResult(result.status, result.values, {**result.provenance, **metadata})


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


def _prometheus_adapter_failure(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    config: Any,
    dashboard: dict[str, Any],
) -> str:
    if not isinstance(variable, dict) or variable.get("type") != "query" or config is None:
        return "adapter_not_applicable"
    datasource_type, datasource_uid = _resolved_datasource_type_uid(
        variable.get("datasource"), context, config, dashboard,
    )
    if str(datasource_type).lower() != PROMETHEUS_DATASOURCE_TYPE:
        return "adapter_not_applicable"
    if not datasource_uid or _missing_context_names(variable, context):
        return "invalid_or_missing_context"
    return "unsupported_query"


def _missing_context_names(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
) -> list[str]:
    if not isinstance(variable, dict):
        return []
    relevant = (variable.get("datasource"), variable.get("query"), variable.get("definition"))
    references = set(_variable_references(relevant))
    return sorted(name for name in references if not name.startswith("__") and name not in context)


def _variable_references(value: Any) -> list[str]:
    if isinstance(value, str):
        pattern = r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::[^}]+)?}|\$([A-Za-z_][A-Za-z0-9_]*)|\[\[([A-Za-z_][A-Za-z0-9_]*)]]"
        return [next(group for group in match.groups() if group) for match in re.finditer(pattern, value)]
    if isinstance(value, (list, tuple)):
        return [name for item in value for name in _variable_references(item)]
    if isinstance(value, dict):
        return [name for item in value.values() for name in _variable_references(item)]
    return []


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


def _with_context_assembly(
    result: MatrixValueResult,
    assembly: DiscoveryContextAssembly,
) -> MatrixValueResult:
    provenance = {
        **result.provenance,
        "dashboard_context_sources": dict(sorted(assembly.dashboard_sources.items())),
        "context_sources": dict(sorted(assembly.sources.items())),
        "context_value_kinds": dict(sorted(assembly.value_kinds.items())),
    }
    return MatrixValueResult(result.status, result.values, provenance)


def _log_result(
    variable: str,
    timestamp: Any,
    context: dict[str, Any],
    result: MatrixValueResult,
) -> None:
    if result.authoritative:
        log = logger.warning if result.status is MatrixDiscoveryStatus.EMPTY else logger.info
        log(
            "matrix_discovery variable=%s time=%s context=%s count=%s values=%s",
            safe_discovery_variable(variable), timestamp_log_label(timestamp), context,
            len(result.values), result.values,
        )
        return
    error_type = result.provenance.get("error_type")
    suffix = " error_type=%s" if error_type else ""
    logger.warning(
        f"matrix_discovery variable=%s time=%s context=%s status=%s reason=%s{suffix}",
        safe_discovery_variable(variable), timestamp_log_label(timestamp), context,
        result.status.value, result.provenance.get("method"), *([error_type] if error_type else []),
    )


def timestamp_log_label(timestamp: Any) -> str:
    time_tag = getattr(timestamp, "time_tag", None)
    if time_tag not in (None, ""):
        return str(time_tag)
    start_human = getattr(timestamp, "start_time_human", None)
    end_human = getattr(timestamp, "end_time_human", None)
    if start_human and end_human:
        return f"{start_human} - {end_human}"
    start = getattr(timestamp, "start_time_timestamp", None)
    end = getattr(timestamp, "end_time_timestamp", None)
    if start is not None and end is not None:
        return f"{start}-{end}"
    return "unknown"


def safe_discovery_variable(variable: str) -> str:
    return "<sensitive-variable>" if SENSITIVE_NAME_PATTERN.search(variable) else variable


def _cache_key(
    source_name: str,
    timestamp: Any,
    context: dict[str, Any],
    context_sources: dict[str, str] | None = None,
) -> tuple[Any, ...]:
    normalized_context = tuple(sorted((str(key), repr(value)) for key, value in context.items()))
    return (
        source_name, timestamp.id_time, timestamp.start_time_timestamp,
        timestamp.end_time_timestamp, normalized_context, tuple(sorted((context_sources or {}).items())),
    )


def _dashboard_variable(dashboard: dict[str, Any], variable_name: str) -> dict[str, Any] | None:
    variables = dashboard.get("templating", {}).get("list", [])
    return next((
        item for item in variables
        if isinstance(item, dict) and item.get("name") == variable_name
    ), None)


def _public_context(context: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in context.items() if not str(key).startswith("__")}


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
