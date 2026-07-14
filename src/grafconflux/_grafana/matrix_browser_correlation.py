"""Value-free request correlation helpers for browser matrix discovery."""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from grafconflux._grafana.matrix_discovery import _dedupe
from grafconflux._grafana.matrix_prometheus import resolved_datasource_type_uid
from grafconflux._grafana.matrix_prometheus_metadata import prometheus_seconds

def required_match_state(expected: Any, observed: Any) -> str:
    if expected in (None, ""):
        return "unavailable"
    return "match" if observed == expected else "mismatch"


def endpoint_match_state(
    endpoint: str | None, endpoint_prefix: str, expected: Any, observed: Any,
) -> str:
    if endpoint is None or not endpoint.startswith(endpoint_prefix):
        return "not_applicable"
    return required_match_state(expected, observed)


def observed_selector(url: str, expected: str | None) -> str | None:
    selectors = parse_qs(urlparse(url).query).get("match[]", [])
    if expected is not None and expected in selectors:
        return expected
    if len(selectors) == 1:
        return selectors[0]
    return "\0".join(selectors) if selectors else None


def candidate_endpoint(url: str) -> str | None:
    path = urlparse(url).path
    if path.endswith("/series"):
        return "series"
    match = re.search(r"/(label/[A-Za-z_][A-Za-z0-9_]*/values)$", path)
    return match.group(1) if match else None


def endpoint_label(endpoint: str | None) -> str | None:
    if endpoint is None or not endpoint.startswith("label/"):
        return None
    return endpoint.split("/", 2)[1]


def metadata_rejection(states: dict[str, str], datasource_present: bool) -> str:
    checks = (
        (states["route_match"] != "match", "rejected_route"),
        (states["method_match"] != "match", "request_method_correlation"),
        (not datasource_present, "invalid_or_missing_context"),
        (states["datasource_match"] != "match", "datasource_correlation"),
        (states["time_match"] != "match", "time_correlation"),
        (
            states["selector_match"] != "match" and states["target_label_match"] != "match",
            "selector_or_label_correlation",
        ),
    )
    return next((reason for rejected, reason in checks if rejected), "none")


def prometheus_datasource_uid(
    variable: dict[str, Any], context: dict[str, Any], config: Any, dashboard: dict[str, Any],
) -> str | None:
    datasource_type, datasource_uid = resolved_datasource_type_uid(
        variable.get("datasource"), context, config, dashboard,
    )
    if str(datasource_type).lower() != "prometheus" or not datasource_uid:
        return None
    return str(datasource_uid)


def datasource_representation(datasource: Any) -> str:
    if isinstance(datasource, dict):
        return "mapping"
    if isinstance(datasource, str):
        return "string"
    return "missing" if datasource is None else "unsupported"


def candidate_route(response: Any) -> tuple[str, str]:
    path = urlparse(response_request_url(response)).path
    if "/api/datasources/proxy/uid/" in path:
        return "proxy_uid", "proxy_uid_path"
    if "/api/datasources/uid/" in path and "/resources/" in path:
        return "uid_resources", "uid_resources_path"
    if path.endswith("/api/ds/query"):
        return "ds_query", "request_payload"
    return "other", "none"


def navigation_telemetry(response: Any, page: Any) -> tuple[int | str, str]:
    """Return non-sensitive navigation status and route classification."""
    status = getattr(response, "status", None)
    safe_status = status if isinstance(status, int) and not isinstance(status, bool) else "unavailable"
    return safe_status, _navigation_route(getattr(page, "url", ""))


def _navigation_route(url: Any) -> str:
    path = urlparse(str(url or "")).path.lower()
    segments = set(filter(None, path.split("/")))
    if segments.intersection({"auth", "login", "oauth", "oauth2", "saml", "signin", "sso"}):
        return "login_like"
    if "/d/" in path:
        return "dashboard"
    return "other"


def successful_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("status") == "success"


def response_request_url(response: Any) -> str:
    request_url = getattr(getattr(response, "request", None), "url", None)
    return str(request_url or getattr(response, "url", ""))


def prometheus_metadata_route(url: str) -> tuple[str, str] | None:
    path = urlparse(url).path
    match = re.search(
        r"/api/datasources/(?:proxy/uid/([^/]+)/api/v1|uid/([^/]+)/resources/api/v1)/"
        r"(series|label/[A-Za-z_][A-Za-z0-9_]*/values)$",
        path,
    )
    if match is None:
        return None
    return unquote(match.group(1) or match.group(2)), match.group(3)


def is_metadata_candidate(response: Any) -> bool:
    path = urlparse(response_request_url(response)).path
    return "/api/datasources/" in path and (
        path.endswith("/series") or "/label/" in path or path.endswith("/api/ds/query")
    )


def is_prometheus_metadata_candidate(response: Any) -> bool:
    path = urlparse(response_request_url(response)).path
    return "/api/datasources/" in path and (path.endswith("/series") or "/label/" in path)


def url_time_matches(url: str, timestamp: Any) -> bool:
    query = parse_qs(urlparse(url).query)
    expected = {
        "start": prometheus_seconds(timestamp.start_time_timestamp),
        "end": prometheus_seconds(timestamp.end_time_timestamp),
    }
    return all(query.get(name) == [value] for name, value in expected.items())


def payload_time_matches(payload: dict[str, Any], timestamp: Any) -> bool:
    expected = {
        "from": str(timestamp.start_time_timestamp),
        "to": str(timestamp.end_time_timestamp),
    }
    return all(name in payload and str(payload[name]) == value for name, value in expected.items())


def query_datasource_uid(query: Any) -> str | None:
    if not isinstance(query, dict):
        return None
    datasource = query.get("datasource")
    if isinstance(datasource, dict):
        return str(datasource.get("uid")) if datasource.get("uid") not in (None, "") else None
    return str(datasource) if datasource not in (None, "") else None


def request_json(request: Any) -> Any:
    value = getattr(request, "post_data_json", None)
    if callable(value):
        try:
            return value()
        except Exception:
            return None
    if value is not None:
        return value
    try:
        return json.loads(getattr(request, "post_data", "") or "")
    except (TypeError, ValueError):
        return None


def ds_variable_query_signature(
    variable: dict[str, Any], parsed_query: tuple[str | None, str] | None,
) -> tuple[str, str, str] | None:
    query = variable.get("query")
    if not isinstance(query, dict) or query.get("queryType") != "label_values" or parsed_query is None:
        return None
    ref_id = query.get("refId")
    if not isinstance(ref_id, str) or not ref_id:
        return None
    expression, label = parsed_query
    return ref_id, expression or "", label


def query_matches_signature(query: Any, signature: tuple[str, str, str]) -> bool:
    if not isinstance(query, dict):
        return False
    ref_id, expression, label = signature
    return (
        query.get("queryType") == "label_values"
        and query.get("refId") == ref_id
        and query.get("query", "") == expression
        and query.get("label") == label
    )


def ds_query_values(payload: Any, label: str, ref_id: str) -> list[str] | None:
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, dict) or _invalid_ds_metadata(payload):
        return None
    result = results.get(ref_id)
    if not isinstance(result, dict) or _invalid_ds_metadata(result):
        return None
    frames = result.get("frames")
    if not isinstance(frames, list) or not frames:
        return None
    values: list[str] = []
    for frame in frames:
        frame_values = _ds_frame_values(frame, label)
        if frame_values is None:
            return None
        values.extend(frame_values)
    return _dedupe(values)


def _invalid_ds_metadata(container: dict[str, Any]) -> bool:
    if any(container.get(key) not in (None, "") for key in ("error", "errorSource")):
        return True
    return "status" in container and str(container["status"]) != "200"


def _ds_frame_values(frame: Any, label: str) -> list[str] | None:
    if not isinstance(frame, dict):
        return None
    schema, data = frame.get("schema"), frame.get("data")
    if not isinstance(schema, dict) or not isinstance(data, dict):
        return None
    fields, columns = schema.get("fields"), data.get("values")
    if not _valid_frame_columns(fields, columns):
        return None
    indices = [index for index, field in enumerate(fields) if field.get("name") == label]
    if len(indices) != 1:
        return None
    column = columns[indices[0]]
    if any(isinstance(value, (dict, list, tuple, set)) for value in column):
        return None
    return [str(value) for value in column if value not in (None, "")]


def _valid_frame_columns(fields: Any, columns: Any) -> bool:
    if not isinstance(fields, list) or not fields or not isinstance(columns, list):
        return False
    if len(fields) != len(columns) or not all(isinstance(column, list) for column in columns):
        return False
    return all(isinstance(field, dict) and isinstance(field.get("name"), str) for field in fields)
