"""Sequential normal-dashboard browser fallback for matrix planning."""

from __future__ import annotations

import logging
from typing import Any, Callable
from urllib.parse import urlencode, urlparse

from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._grafana.matrix_browser_correlation import (
    candidate_endpoint as _candidate_endpoint,
    ds_query_values as _ds_query_values,
    ds_variable_query_signature as _ds_variable_query_signature,
    endpoint_label as _endpoint_label,
    endpoint_match_state as _endpoint_match_state,
    is_metadata_candidate as _is_metadata_candidate,
    is_prometheus_metadata_candidate as _is_prometheus_metadata_candidate,
    metadata_rejection as _metadata_rejection,
    navigation_telemetry as _navigation_telemetry,
    observed_selector as _observed_selector,
    payload_time_matches as _payload_time_matches,
    prometheus_datasource_uid as _prometheus_datasource_uid,
    prometheus_metadata_route as _prometheus_metadata_route,
    query_datasource_uid as _query_datasource_uid,
    query_matches_signature as _query_matches_signature,
    request_json as _request_json,
    required_match_state as _required_match_state,
    response_request_url as _response_request_url,
    successful_payload as _successful_payload,
    url_time_matches as _url_time_matches,
)
from grafconflux._grafana.matrix_browser_dom import open_variable_script, read_variable_options_script
from grafconflux._grafana.matrix_discovery import (
    MatrixDiscoveryStatus,
    MatrixValueResult,
    _dedupe,
    _prometheus_label_values_query,
    _prometheus_payload_values,
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
                navigation_response = browser.get(self._navigation_url(timestamp, context))
                self._wait_for_response(browser, collector)
            status, route = _navigation_telemetry(navigation_response, browser.page)
            logger.info(
                "Matrix planning navigation variable=%s timestamp_id=%s status=%s route=%s",
                safe_discovery_variable(variable_name), timestamp.id_time, status, route,
            )
            if collector.result is not None:
                return collector.result
            logger.info(
                "Matrix planning DOM fallback variable=%s timestamp_id=%s candidates=%s rejections=%s",
                safe_discovery_variable(variable_name), timestamp.id_time,
                collector.candidate_count, collector.rejection_summary(),
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
        self.candidate_count = 0

    def rejection_summary(self) -> str:
        return ",".join(sorted(self.rejections)) or "no_correlated_network_candidate"

    def collect(self, page: Any) -> _ListenerScope:
        return _ListenerScope(page, self._record_response)

    def _record_response(self, response: Any) -> None:
        if self.result is not None:
            return
        evaluation = self._metadata_evaluation(response) if _is_prometheus_metadata_candidate(response) else None
        if getattr(response, "status", None) != 200:
            if _is_metadata_candidate(response):
                self._reject_candidate("http_status")
            return
        method = self._correlated_method(response, evaluation)
        if method is None:
            return
        try:
            payload = response.json()
        except Exception:
            self._reject_candidate("invalid_response_payload")
            return
        if method in {"prometheus_label_values", "prometheus_series"} and not _successful_payload(payload):
            self._emit_candidate()
            self.result = _result(
                MatrixDiscoveryStatus.UNRESOLVED, [], self.variable_name, self.timestamp,
                self.context, "browser_network", method,
            )
            return
        values = self._values(payload, method)
        if values is None:
            self._reject_candidate("invalid_response_payload")
            return
        self._emit_candidate()
        status = MatrixDiscoveryStatus.RESOLVED if values else MatrixDiscoveryStatus.EMPTY
        self.result = _result(
            status, values, self.variable_name, self.timestamp, self.context, "browser_network", method,
        )

    def _correlated_method(
        self, response: Any, evaluation: tuple[str | None, str, dict[str, Any]] | None,
    ) -> str | None:
        request_url = _response_request_url(response)
        path = urlparse(request_url).path
        if evaluation is not None:
            method, rejection, _states = evaluation
            if method is not None:
                return method
            self._reject_candidate(rejection)
            return None
        if path.endswith("/api/ds/query"):
            ref_id = self._ds_query_ref_id(response)
            if ref_id is not None:
                self.ds_query_ref_id = ref_id
                return "prometheus_ds_query"
            self._reject_candidate("query_correlation")
        return None

    def _metadata_evaluation(
        self, response: Any,
    ) -> tuple[str | None, str, dict[str, Any]]:
        request_url = _response_request_url(response)
        route = _prometheus_metadata_route(request_url)
        datasource_uid, endpoint = route if route is not None else (None, _candidate_endpoint(request_url))
        request = getattr(response, "request", None)
        expected_selector = self.query[0] if self.query is not None and self.query[0] else None
        observed_selector = _observed_selector(request_url, expected_selector)
        observed_label = _endpoint_label(endpoint)
        states = {
            "route_match": "match" if route is not None else "mismatch",
            "method_match": "match" if str(getattr(request, "method", "")).upper() == "GET" else "mismatch",
            "datasource_match": _required_match_state(self.datasource_uid, datasource_uid),
            "selector_match": _endpoint_match_state(endpoint, "series", expected_selector, observed_selector),
            "time_match": "match" if _url_time_matches(request_url, self.timestamp) else "mismatch",
            "target_label_match": _endpoint_match_state(endpoint, "label/", self.target_label, observed_label),
        }
        rejection = _metadata_rejection(states, bool(self.datasource_uid))
        if rejection != "none":
            return None, rejection, states
        method = "prometheus_series" if endpoint == "series" else "prometheus_label_values"
        return method, "none", states

    def _reject_candidate(self, rejection: str) -> None:
        self.rejections.add(rejection)
        self._emit_candidate()

    def _emit_candidate(self) -> None:
        self.candidate_count += 1

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
