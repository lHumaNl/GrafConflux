"""Sequential normal-dashboard browser fallback for matrix planning."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._grafana.matrix_browser_dom import open_variable_script, read_variable_options_script
from grafconflux._grafana.matrix_discovery import (
    MatrixDiscoveryStatus,
    MatrixValueResult,
    _dedupe,
    _prometheus_label_values_query,
    _prometheus_payload_values,
    _prometheus_seconds,
    _resolved_datasource_type_uid,
    _result,
    safe_discovery_variable,
)
from grafconflux._grafana.rendering import build_dashboard_url_params

logger = logging.getLogger(__name__)

PLANNING_POLL_MS = 100
PLANNING_MAX_WAIT_MS = 5_000
ALL_DISPLAY_VALUES = {"all", "$__all", "__all"}


class BrowserMatrixFallback:
    """Own a browser used only by the sequential matrix-planning phase."""

    def __init__(
        self,
        config: Any,
        session: Any,
        dashboard_url: str,
        browser_factory: Callable[[], Any] | None = None,
        dashboard: dict[str, Any] | None = None,
    ) -> None:
        self.config = config
        self.session = session
        self.dashboard_url = dashboard_url
        self.browser_factory = browser_factory
        self.dashboard = dashboard or {}
        self.browser: Any = None

    def discover(
        self,
        variable_name: str,
        variable: dict[str, Any] | None,
        timestamp: Any,
        context: dict[str, Any],
    ) -> MatrixValueResult:
        if not self._normal_dashboard_url():
            return self._outcome(MatrixDiscoveryStatus.UNRESOLVED, variable_name, timestamp, context, "invalid_dashboard_route")
        browser = self._browser(variable_name, timestamp, context)
        if browser is None:
            return self._outcome(MatrixDiscoveryStatus.FAILED, variable_name, timestamp, context, "browser_unavailable")
        collector = _PlanningResponseCollector(
            variable_name, variable, context, timestamp, self.config, self.dashboard,
        )
        try:
            with collector.collect(browser.page):
                browser.get(self._navigation_url(timestamp, context))
                self._wait_for_response(browser, collector)
            if collector.result is not None:
                return collector.result
            logger.info(
                "Matrix planning using DOM fallback variable=%s timestamp_id=%s network_diagnostics=%s",
                safe_discovery_variable(variable_name), timestamp.id_time, collector.diagnostics(),
            )
            values = self._dom_values(browser, variable_name)
        except Exception as error:
            logger.warning(
                "Matrix planning browser failed variable=%s error_type=%s",
                safe_discovery_variable(variable_name),
                type(error).__name__,
            )
            return self._outcome(MatrixDiscoveryStatus.FAILED, variable_name, timestamp, context, "browser_error")
        status = MatrixDiscoveryStatus.RESOLVED if values else MatrixDiscoveryStatus.UNRESOLVED
        return self._outcome(status, variable_name, timestamp, context, "dashboard_dom", values)

    def close(self) -> None:
        browser, self.browser = self.browser, None
        if browser is None:
            return
        close = getattr(browser, "quit", None) or getattr(browser, "close", None)
        if callable(close):
            try:
                close()
            except Exception as error:
                logger.warning("Matrix planning browser cleanup failed error_type=%s", type(error).__name__)

    def _browser(self, variable: str, timestamp: Any, context: dict[str, Any]) -> Any:
        if self.browser is not None:
            return self.browser
        try:
            self.browser = self.browser_factory() if self.browser_factory else self._create_browser()
        except Exception as error:
            logger.warning(
                "Matrix planning browser setup failed variable=%s timestamp_id=%s context_vars=%s error_type=%s",
                safe_discovery_variable(variable),
                timestamp.id_time,
                sorted(str(key) for key in context),
                type(error).__name__,
            )
        return self.browser

    def _create_browser(self) -> Any:
        return GrafanaBrowserSession(
            self.config,
            self.session,
            require_cookie_domain=True,
        ).create_browser()

    def _navigation_url(self, timestamp: Any, context: dict[str, Any]) -> str:
        variables = {**(getattr(self.config, "vars", None) or {}), **context}
        params = build_dashboard_url_params(timestamp, self.config.orgId, variables)
        return f"{self.dashboard_url}?{urlencode(params, doseq=True)}"

    def _normal_dashboard_url(self) -> bool:
        path = urlparse(self.dashboard_url).path.lower()
        return "/d/" in path and not any(part in path for part in ("/edit", "/settings"))

    def _wait_for_response(self, browser: Any, collector: "_PlanningResponseCollector") -> None:
        page = browser.page
        wait = getattr(page, "wait_for_timeout", None)
        if not callable(wait):
            return
        timeout_ms = min(max(0, int(getattr(self.config, "timeout", 0)) * 1000), PLANNING_MAX_WAIT_MS)
        elapsed = 0
        while collector.result is None and elapsed < timeout_ms:
            wait(PLANNING_POLL_MS)
            elapsed += PLANNING_POLL_MS

    @staticmethod
    def _dom_values(browser: Any, variable_name: str) -> list[str]:
        scope = browser.page.evaluate(open_variable_script(), variable_name)
        if not isinstance(scope, dict):
            return []
        wait = getattr(browser.page, "wait_for_timeout", None)
        if callable(wait):
            wait(PLANNING_POLL_MS)
        values = browser.page.evaluate(read_variable_options_script(), scope)
        if not isinstance(values, list):
            return []
        return _dedupe([
            str(value).strip()
            for value in values
            if str(value).strip() and str(value).strip().lower() not in ALL_DISPLAY_VALUES
        ])

    @staticmethod
    def _outcome(
        status: MatrixDiscoveryStatus,
        variable: str,
        timestamp: Any,
        context: dict[str, Any],
        method: str,
        values: list[str] | None = None,
    ) -> MatrixValueResult:
        return _result(status, values or [], variable, timestamp, context, "browser", method)


class _ListenerScope:
    def __init__(self, page: Any, response_handler: Callable[[Any], None]) -> None:
        self.page = page
        self.response_handler = response_handler

    def __enter__(self) -> "_ListenerScope":
        self.page.on("response", self.response_handler)
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        try:
            self.page.remove_listener("response", self.response_handler)
        except Exception:
            try:
                self.page.off("response", self.response_handler)
            except Exception:
                pass


class _PlanningResponseCollector:
    def __init__(self, variable_name: str, variable: dict[str, Any] | None, context: dict[str, Any],
                 timestamp: Any, config: Any, dashboard: dict[str, Any]) -> None:
        self.variable_name = variable_name
        self.variable = variable or {}
        self.context = context
        self.timestamp = timestamp
        self.query = _prometheus_label_values_query(self.variable, context) if self.variable else None
        self.target_label = self.query[1] if self.query is not None else variable_name
        self.datasource_uid = _prometheus_datasource_uid(self.variable, context, config, dashboard)
        self.ds_query_signature = _ds_variable_query_signature(self.variable, self.query)
        self.ds_query_ref_id: str | None = None
        self.result: MatrixValueResult | None = None
        self.rejections: set[str] = set()

    def diagnostics(self) -> list[str]:
        if not self.datasource_uid:
            self.rejections.add("invalid_or_missing_context")
        return sorted(self.rejections) or ["no_correlated_network_candidate"]

    def collect(self, page: Any) -> _ListenerScope:
        return _ListenerScope(page, self._record_response)

    def _record_response(self, response: Any) -> None:
        if self.result is not None:
            return
        if getattr(response, "status", None) != 200:
            if _is_metadata_candidate(response):
                self.rejections.add("http_status")
            return
        method = self._correlated_method(response)
        if method is None:
            return
        try:
            payload = response.json()
        except Exception:
            self.rejections.add("invalid_response_payload")
            return
        if method in {"prometheus_label_values", "prometheus_series"} and not _successful_payload(payload):
            self.result = _result(
                MatrixDiscoveryStatus.UNRESOLVED, [], self.variable_name, self.timestamp,
                self.context, "browser_network", method,
            )
            return
        values = self._values(payload, method)
        if values is None:
            self.rejections.add("invalid_response_payload")
            return
        status = MatrixDiscoveryStatus.RESOLVED if values else MatrixDiscoveryStatus.EMPTY
        self.result = _result(
            status, values, self.variable_name, self.timestamp, self.context, "browser_network", method,
        )

    def _correlated_method(self, response: Any) -> str | None:
        request_url = _response_request_url(response)
        path = urlparse(request_url).path
        route = _prometheus_metadata_route(request_url)
        if route is not None:
            return self._correlated_metadata_method(response, request_url, route)
        if _is_metadata_candidate(response):
            self.rejections.add("rejected_route")
        if path.endswith("/api/ds/query"):
            ref_id = self._ds_query_ref_id(response)
            if ref_id is not None:
                self.ds_query_ref_id = ref_id
                return "prometheus_ds_query"
            self.rejections.add("query_correlation")
        return None

    def _correlated_metadata_method(
        self, response: Any, request_url: str, route: tuple[str, str],
    ) -> str | None:
        datasource_uid, endpoint = route
        request = getattr(response, "request", None)
        checks = (
            (str(getattr(request, "method", "")).upper() != "GET", "request_method_correlation"),
            (not self.datasource_uid, "invalid_or_missing_context"),
            (datasource_uid != self.datasource_uid, "datasource_correlation"),
            (not _url_time_matches(request_url, self.timestamp), "time_correlation"),
        )
        for rejected, reason in checks:
            if rejected:
                self.rejections.add(reason)
                return None
        if endpoint == f"label/{self.target_label}/values":
            return "prometheus_label_values"
        if endpoint == "series" and self._series_request_matches(response):
            return "prometheus_series"
        self.rejections.add("selector_or_label_correlation")
        return None

    def _series_request_matches(self, response: Any) -> bool:
        if self.query is None or not self.query[0]:
            return False
        request_url = str(getattr(getattr(response, "request", None), "url", ""))
        return self.query[0] in parse_qs(urlparse(request_url).query).get("match[]", [])

    def _ds_query_ref_id(self, response: Any) -> str | None:
        request = getattr(response, "request", None)
        if str(getattr(request, "method", "")).upper() != "POST" or self.ds_query_signature is None:
            return None
        payload = _request_json(request)
        queries = payload.get("queries") if isinstance(payload, dict) else None
        if not isinstance(queries, list) or not _payload_time_matches(payload, self.timestamp):
            return None
        matching = [
            query for query in queries
            if _query_matches_signature(query, self.ds_query_signature)
            and _query_datasource_uid(query) == self.datasource_uid
        ]
        return self.ds_query_signature[0] if len(matching) == 1 else None

    def _values(self, payload: Any, method: str) -> list[str] | None:
        if method == "prometheus_label_values":
            return _prometheus_payload_values(payload, method, self.target_label)
        if method == "prometheus_series":
            return _prometheus_payload_values(payload, method, self.target_label)
        if self.ds_query_ref_id is None:
            return None
        return _ds_query_values(payload, self.target_label, self.ds_query_ref_id)


def _prometheus_datasource_uid(
    variable: dict[str, Any], context: dict[str, Any], config: Any, dashboard: dict[str, Any],
) -> str | None:
    datasource_type, datasource_uid = _resolved_datasource_type_uid(
        variable.get("datasource"), context, config, dashboard,
    )
    if str(datasource_type).lower() != "prometheus" or not datasource_uid:
        return None
    return str(datasource_uid)


def _successful_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("status") == "success"


def _response_request_url(response: Any) -> str:
    request_url = getattr(getattr(response, "request", None), "url", None)
    return str(request_url or getattr(response, "url", ""))


def _prometheus_metadata_route(url: str) -> tuple[str, str] | None:
    path = urlparse(url).path
    match = re.search(
        r"/api/datasources/(?:proxy/uid/([^/]+)/api/v1|uid/([^/]+)/resources/api/v1)/"
        r"(series|label/[A-Za-z_][A-Za-z0-9_]*/values)$",
        path,
    )
    if match is None:
        return None
    return unquote(match.group(1) or match.group(2)), match.group(3)


def _is_metadata_candidate(response: Any) -> bool:
    path = urlparse(_response_request_url(response)).path
    return "/api/datasources/" in path and (
        path.endswith("/series") or "/label/" in path or path.endswith("/api/ds/query")
    )


def _url_time_matches(url: str, timestamp: Any) -> bool:
    query = parse_qs(urlparse(url).query)
    expected = {
        "start": _prometheus_seconds(timestamp.start_time_timestamp),
        "end": _prometheus_seconds(timestamp.end_time_timestamp),
    }
    return all(query.get(name) == [value] for name, value in expected.items())


def _payload_time_matches(payload: dict[str, Any], timestamp: Any) -> bool:
    expected = {
        "from": str(timestamp.start_time_timestamp),
        "to": str(timestamp.end_time_timestamp),
    }
    return all(name in payload and str(payload[name]) == value for name, value in expected.items())


def _query_datasource_uid(query: Any) -> str | None:
    if not isinstance(query, dict):
        return None
    datasource = query.get("datasource")
    if isinstance(datasource, dict):
        return str(datasource.get("uid")) if datasource.get("uid") not in (None, "") else None
    return str(datasource) if datasource not in (None, "") else None


def _request_json(request: Any) -> Any:
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


def _ds_variable_query_signature(
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


def _query_matches_signature(query: Any, signature: tuple[str, str, str]) -> bool:
    if not isinstance(query, dict):
        return False
    ref_id, expression, label = signature
    return (
        query.get("queryType") == "label_values"
        and query.get("refId") == ref_id
        and query.get("query", "") == expression
        and query.get("label") == label
    )


def _ds_query_values(payload: Any, label: str, ref_id: str) -> list[str] | None:
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
