"""Prometheus metadata request construction and strict payload parsing."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

ALL_SENTINELS = {"$__all", "__all", "all"}
PROXY_UID_ROUTE = "proxy_uid"
UID_RESOURCES_ROUTE = "uid_resources"


def series_attempt_outcome(
    response: Any, error: Exception | None,
) -> tuple[str, str, str | int, str]:
    if error is not None:
        return "none", "none", "unknown", "request_exception"
    if getattr(response, "status_code", None) != 200:
        return _response_content_classification(response), "none", "unknown", "http_non_2xx"
    try:
        payload = response.json()
    except Exception:
        return _response_content_classification(response), "none", "unknown", "invalid_json"
    return _series_payload_outcome(payload)


def _series_payload_outcome(payload: Any) -> tuple[str, str, str | int, str]:
    if not isinstance(payload, dict):
        return "json_schema_invalid", "none", "unknown", "data_schema_invalid"
    prometheus_status = _prometheus_status_enum(payload.get("status"))
    if prometheus_status != "success":
        return "prometheus_error", prometheus_status, "unknown", "prometheus_status_not_success"
    data = payload.get("data")
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        return "json_schema_invalid", prometheus_status, "unknown", "data_schema_invalid"
    outcome = "success_nonempty" if data else "success_empty"
    return "prometheus_success_list", prometheus_status, len(data), outcome


def _response_content_classification(response: Any) -> str:
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("Content-Type", headers.get("content-type", ""))).lower()
    if "html" in content_type:
        return "html_like"
    if content_type.startswith("text/"):
        return "text_like"
    return "non_json"


def _prometheus_status_enum(value: Any) -> str:
    return str(value) if value in {"success", "error"} else "other" if value is not None else "none"


def prometheus_payload_values(payload: Any, method: str, label: str) -> list[str] | None:
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
    return list(dict.fromkeys(value for value in values if value.lower() not in ALL_SENTINELS))


def prometheus_params(metric: str | None, timestamp: Any) -> dict[str, str]:
    params = {
        "start": prometheus_seconds(timestamp.start_time_timestamp),
        "end": prometheus_seconds(timestamp.end_time_timestamp),
    }
    if metric:
        params["match[]"] = metric
    return params


def prometheus_seconds(milliseconds: Any) -> str:
    value = int(milliseconds)
    return str(value // 1000) if value % 1000 == 0 else str(value / 1000)


def prometheus_url(
    base_url: str, datasource_uid: str, label: str, series: bool,
    route_family: str = PROXY_UID_ROUTE,
) -> str:
    encoded_uid = quote(str(datasource_uid), safe="")
    if route_family == UID_RESOURCES_ROUTE:
        base = f"{base_url}/api/datasources/uid/{encoded_uid}/resources/api/v1"
    else:
        base = f"{base_url}/api/datasources/proxy/uid/{encoded_uid}/api/v1"
    return f"{base}/series" if series else f"{base}/label/{quote(label, safe='')}/values"
