import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlencode

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from grafconflux._grafana.matrix_browser_planning import BrowserMatrixFallback
from grafconflux._grafana.matrix_discovery import MatrixDiscoveryStatus


class TestBrowserMatrixPlanningCorrelation(unittest.TestCase):
    def test_resources_series_route_is_accepted_with_exact_correlation(self) -> None:
        response = _metadata_response(
            "resources",
            datasource_uid="prom",
            selector='kube_pod_info{namespace="app"}',
        )

        result = self.fallback([response]).discover(
            "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api-1"])

    def test_resources_label_route_is_accepted(self) -> None:
        params = urlencode({"start": "1700000000", "end": "1700003600"})
        url = (
            "https://grafana.example/grafana/api/datasources/uid/prom/"
            f"resources/api/v1/label/service/values?{params}"
        )
        variable = {
            "name": "service", "type": "query",
            "datasource": {"type": "prometheus", "uid": "prom"},
            "query": "label_values(service)",
        }

        result = self.fallback([_response(url, {"status": "success", "data": ["api"]})]).discover(
            "service", variable, _timestamp(), {},
        )

        self.assertEqual(result.values, ["api"])

    def test_resources_series_route_keeps_uid_selector_and_time_strict(self) -> None:
        candidates = (
            _metadata_response("resources", datasource_uid="other"),
            _metadata_response("resources", selector="up"),
            _metadata_response("resources", end="1700003601"),
        )

        for response in candidates:
            with self.subTest(url=response.url):
                result = self.fallback([response]).discover(
                    "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)

    def test_rejected_network_candidates_are_logged_before_dom_fallback(self) -> None:
        responses = [
            _metadata_response("invalid"),
            _metadata_response("resources", datasource_uid="other"),
            _metadata_response("resources", selector="up"),
            _metadata_response("resources", end="1700003601"),
        ]

        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            result = self.fallback(responses).discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        diagnostic = "\n".join(logs.output)
        self.assertIn("DOM fallback", diagnostic)
        self.assertIn("rejected_route", diagnostic)
        self.assertIn("datasource_correlation", diagnostic)
        self.assertIn("selector_or_label_correlation", diagnostic)
        self.assertIn("time_correlation", diagnostic)
        self.assertIn("candidates=4", diagnostic)
        self.assertNotIn("request_url", diagnostic)

    def test_uid_mismatch_logs_exact_authorized_correlation_evidence_without_headers_or_bodies(self) -> None:
        expected_uid = "expected-uid"
        observed_uid = "observed-uid"
        selector = 'kube_pod_info{namespace="sensitive-namespace"}'
        variable = {
            **_pod_variable(),
            "datasource": {"type": "prometheus", "uid": expected_uid},
            "query": f"label_values({selector}, pod)",
        }
        response = _metadata_response(
            "resources", datasource_uid=observed_uid, selector=selector,
        )
        response.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer response-token-secret",
        }
        response.body = "response-body-secret"
        response.request.headers = {"Cookie": "session-cookie-secret"}
        response.request.post_data = "request-body-secret"

        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            result = self.fallback([response]).discover("pod", variable, _timestamp(), {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        diagnostic = "\n".join(logs.output)
        self.assertIn("candidates=1", diagnostic)
        self.assertIn("rejections=datasource_correlation", diagnostic)
        self.assertNotIn(observed_uid, diagnostic)
        self.assertNotIn(expected_uid, diagnostic)
        self.assertNotIn(selector, diagnostic)
        self.assertNotIn("response-token-secret", diagnostic)
        self.assertNotIn("response-body-secret", diagnostic)
        self.assertNotIn("session-cookie-secret", diagnostic)
        self.assertNotIn("request-body-secret", diagnostic)

    def test_rejected_candidate_diagnostics_are_bounded_to_ten_blocks(self) -> None:
        responses = [
            _metadata_response("resources", datasource_uid=f"other-{index}")
            for index in range(30)
        ]

        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback(responses).discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        diagnostic = "\n".join(logs.output)
        self.assertNotIn("MATRIX BROWSER METADATA CANDIDATE", diagnostic)
        self.assertIn("candidates=30", diagnostic)
        self.assertNotIn("other-29", diagnostic)

    def test_candidate_url_redacts_userinfo_and_secret_query_values(self) -> None:
        response = _metadata_response("resources", datasource_uid="other")
        response.url = response.url.replace("https://", "https://alice:password@").replace(
            "&start=", "&token=hidden&start=",
        )
        response.request.url = response.url

        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback([response]).discover("pod", _pod_variable(), _timestamp(), {"namespace": "app"})

        diagnostic = "\n".join(logs.output)
        self.assertNotIn("request_url", diagnostic)
        self.assertNotIn("https://", diagnostic)
        self.assertNotIn("alice", diagnostic)
        self.assertNotIn("password", diagnostic)
        self.assertNotIn("hidden", diagnostic)

    def test_dom_fallback_logs_safe_dashboard_navigation_telemetry(self) -> None:
        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback([], navigation_url="https://grafana.example/grafana/d/uid/dashboard").discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        diagnostic = "\n".join(logs.output)
        self.assertIn("navigation variable=pod", diagnostic)
        self.assertIn("status=200", diagnostic)
        self.assertIn("route=dashboard", diagnostic)
        self.assertNotIn("https://", diagnostic)

    def test_dom_fallback_logs_safe_login_navigation_telemetry(self) -> None:
        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback([], navigation_url="https://identity.example/login", navigation_status=302).discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        diagnostic = "\n".join(logs.output)
        self.assertIn("status=302", diagnostic)
        self.assertIn("route=login_like", diagnostic)
        self.assertNotIn("https://", diagnostic)

    def test_navigation_url_redacts_userinfo_and_secret_query_values(self) -> None:
        navigation_url = (
            "https://alice:password@grafana.example/grafana/d/uid/dashboard"
            "?from=1700000000000&token=hidden"
        )

        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback([], navigation_url=navigation_url).discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        diagnostic = "\n".join(logs.output)
        self.assertIn("status=200", diagnostic)
        self.assertIn("route=dashboard", diagnostic)
        self.assertNotIn("https://", diagnostic)
        self.assertNotIn("alice", diagnostic)
        self.assertNotIn("password", diagnostic)
        self.assertNotIn("hidden", diagnostic)

    def test_dom_fallback_marks_unavailable_navigation_signals(self) -> None:
        with self.assertLogs(
            "grafconflux._grafana.matrix_browser_planning", level="INFO",
        ) as logs:
            self.fallback([], navigation_url=None, navigation_status=None).discover(
                "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
            )

        diagnostic = "\n".join(logs.output)
        self.assertIn("status=unavailable", diagnostic)
        self.assertIn("route=other", diagnostic)

    def test_series_response_requires_complete_period_boundaries(self) -> None:
        for missing_boundary in ("start", "end"):
            with self.subTest(missing_boundary=missing_boundary):
                response = _series_response(missing_boundary=missing_boundary)

                result = self.fallback([response]).discover(
                    "pod", _pod_variable(), _timestamp(), {"namespace": "app"},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertEqual(result.values, [])

    def test_label_response_requires_complete_period_boundaries(self) -> None:
        variable = {
            "name": "service",
            "type": "query",
            "datasource": {"type": "prometheus", "uid": "prom"},
            "query": "label_values(service)",
        }
        for missing_boundary in ("start", "end"):
            with self.subTest(missing_boundary=missing_boundary):
                response = _label_response(missing_boundary=missing_boundary)

                result = self.fallback([response]).discover(
                    "service", variable, _timestamp(), {},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertEqual(result.values, [])

    def test_ds_query_response_requires_complete_period_boundaries(self) -> None:
        for missing_boundary in ("from", "to"):
            with self.subTest(missing_boundary=missing_boundary):
                response = _ds_query_response(missing_boundary=missing_boundary)

                result = self.fallback([response]).discover(
                    "pod", _ds_query_variable(), _timestamp(), {},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertEqual(result.values, [])

    def test_ds_query_response_with_complete_period_is_accepted(self) -> None:
        response = _ds_query_response(
            queries=[_panel_query(), _variable_query()],
            results={
                "Panel": _ds_result("panel-value"),
                "Variable-pod": _ds_result("api-1"),
            },
        )

        result = self.fallback([response]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api-1"])

    def test_ds_query_rejects_panel_query_with_matching_label_words(self) -> None:
        response = _ds_query_response(
            queries=[_panel_query()],
            results={"Panel": _ds_result("panel-value")},
        )

        result = self.fallback([response]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        self.assertEqual(result.values, [])

    def test_ds_query_requires_exact_query_and_ref_id(self) -> None:
        candidates = (
            {**_variable_query(), "query": "up "},
            {**_variable_query(), "label": "pod_name"},
            {**_variable_query(), "refId": "Other"},
            {**_variable_query(), "datasource": {"uid": "other"}},
            {key: value for key, value in _variable_query().items() if key != "refId"},
        )
        for query in candidates:
            with self.subTest(query=query):
                result = self.fallback([_ds_query_response(queries=[query])]).discover(
                    "pod", _ds_query_variable(), _timestamp(), {},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertEqual(result.values, [])

    def test_ds_query_rejects_ambiguous_exact_query_candidates(self) -> None:
        response = _ds_query_response(queries=[_variable_query(), _variable_query()])

        result = self.fallback([response]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)

    def test_ds_query_extracts_only_the_matched_ref_id(self) -> None:
        response = _ds_query_response(results={"Other": _ds_result("sole-wrong-result")})

        result = self.fallback([response]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        self.assertEqual(result.values, [])

    def test_ds_query_rejects_result_errors_and_non_200_metadata(self) -> None:
        invalid_metadata = (
            {"error": "query failed"},
            {"errorSource": "downstream"},
            {"status": 500},
            {"status": "success"},
        )
        for metadata in invalid_metadata:
            with self.subTest(metadata=metadata):
                result_data = {**_ds_result("must-not-be-used"), **metadata}
                response = _ds_query_response(results={"Variable-pod": result_data})

                result = self.fallback([response]).discover(
                    "pod", _ds_query_variable(), _timestamp(), {},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertNotEqual(result.status, MatrixDiscoveryStatus.EMPTY)

    def test_ds_query_rejects_non_200_http_response(self) -> None:
        result = self.fallback([_ds_query_response(response_status=503)]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)

    def test_ds_query_rejects_malformed_frame_schema(self) -> None:
        malformed_frames = (
            {"schema": {"fields": "pod"}, "data": {"values": [["bad"]]}},
            {"schema": {"fields": [{"name": "pod"}]}, "data": {"values": []}},
            {"schema": {"fields": [{"name": "pod"}]}, "data": {"values": ["bad"]}},
            {"schema": {"fields": [{"name": "other"}]}, "data": {"values": [["bad"]]}},
        )
        for frame in malformed_frames:
            with self.subTest(frame=frame):
                response = _ds_query_response(results={"Variable-pod": {"frames": [frame]}})

                result = self.fallback([response]).discover(
                    "pod", _ds_query_variable(), _timestamp(), {},
                )

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertNotEqual(result.status, MatrixDiscoveryStatus.EMPTY)

    def test_ds_query_without_proven_variable_query_semantics_is_rejected(self) -> None:
        legacy_variable = {**_ds_query_variable(), "query": "label_values(up, pod)"}

        result = self.fallback([_ds_query_response()]).discover(
            "pod", legacy_variable, _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)

    def test_ds_query_valid_empty_label_column_is_authoritative_empty(self) -> None:
        response = _ds_query_response(results={"Variable-pod": _ds_result(None)})

        result = self.fallback([response]).discover(
            "pod", _ds_query_variable(), _timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.EMPTY)
        self.assertEqual(result.values, [])

    def fallback(
        self, responses: list[object], navigation_url: str | None = None, navigation_status: int | None = 200,
    ) -> BrowserMatrixFallback:
        browser = _PlanningBrowser(_PlanningPage(navigation_url), responses, navigation_status)
        return BrowserMatrixFallback(
            SimpleNamespace(vars={}, orgId=2, timeout=0, datasource_vars={}),
            Mock(),
            "https://grafana.example/grafana/d/uid/dashboard",
            browser_factory=lambda: browser,
        )


class TestBrowserMatrixPlanningDomScope(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.playwright = sync_playwright().start()
        try:
            cls.browser = cls.playwright.chromium.launch(headless=True)
        except PlaywrightError as error:
            cls.playwright.stop()
            raise unittest.SkipTest("Playwright Chromium is unavailable") from error

    @classmethod
    def tearDownClass(cls) -> None:
        cls.browser.close()
        cls.playwright.stop()

    def test_unrelated_visible_popup_is_rejected_without_target_linkage(self) -> None:
        page = self.browser.new_page()
        page.set_content(_dom_fixture(link_target=False))

        try:
            result = self.fallback(page).discover(
                "service", {"name": "service", "type": "custom"}, _timestamp(), {},
            )
        finally:
            page.close()

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        self.assertEqual(result.values, [])

    def test_exact_control_opens_and_reads_only_its_linked_popup(self) -> None:
        page = self.browser.new_page()
        page.set_content(_dom_fixture(link_target=True))

        try:
            result = self.fallback(page).discover(
                "service", {"name": "service", "type": "custom"}, _timestamp(), {},
            )
        finally:
            page.close()

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api", "worker"])

    @staticmethod
    def fallback(page: object) -> BrowserMatrixFallback:
        browser = _DomPlanningBrowser(page)
        return BrowserMatrixFallback(
            SimpleNamespace(vars={}, orgId=2, timeout=0, datasource_vars={}),
            Mock(),
            "https://grafana.example/grafana/d/uid/dashboard",
            browser_factory=lambda: browser,
        )


class _PlanningPage:
    def __init__(self, url: str | None = None) -> None:
        self.listeners: dict[str, object] = {}
        self.url = url

    def on(self, event: str, handler: object) -> None:
        self.listeners[event] = handler

    def remove_listener(self, event: str, _handler: object) -> None:
        self.listeners.pop(event, None)

    def evaluate(self, _script: str, *_args: object) -> object:
        return []


class _PlanningBrowser:
    def __init__(self, page: _PlanningPage, responses: list[object], navigation_status: int | None = 200) -> None:
        self.page = page
        self.responses = responses
        self.navigation_status = navigation_status

    def get(self, _url: str) -> SimpleNamespace | None:
        for response in self.responses:
            self.page.listeners["response"](response)
        return SimpleNamespace(status=self.navigation_status) if self.navigation_status is not None else None


class _DomPlanningBrowser:
    def __init__(self, page: object) -> None:
        self.page = page

    def get(self, _url: str) -> None:
        return None


def _timestamp() -> SimpleNamespace:
    return SimpleNamespace(
        id_time=3,
        start_time_timestamp=1_700_000_000_000,
        end_time_timestamp=1_700_003_600_000,
    )


def _pod_variable() -> dict:
    return {
        "name": "pod",
        "type": "query",
        "datasource": {"type": "prometheus", "uid": "prom"},
        "query": 'label_values(kube_pod_info{namespace="$namespace"}, pod)',
    }


def _ds_query_variable() -> dict:
    return {
        "name": "pod",
        "type": "query",
        "datasource": {"type": "prometheus", "uid": "prom"},
        "query": {
            "queryType": "label_values",
            "query": "up",
            "label": "pod",
            "refId": "Variable-pod",
        },
    }


def _series_response(missing_boundary: str) -> SimpleNamespace:
    params = {
        "match[]": 'kube_pod_info{namespace="app"}',
        "start": "1700000000",
        "end": "1700003600",
    }
    params.pop(missing_boundary)
    url = _proxy_url("/api/v1/series", params)
    return _response(url, {"status": "success", "data": [{"pod": "wrong-period"}]})


def _label_response(missing_boundary: str) -> SimpleNamespace:
    params = {"start": "1700000000", "end": "1700003600"}
    params.pop(missing_boundary)
    url = _proxy_url("/api/v1/label/service/values", params)
    return _response(url, {"status": "success", "data": ["wrong-period"]})


def _ds_query_response(
    missing_boundary: str | None = None,
    queries: list[dict] | None = None,
    results: dict | None = None,
    response_status: int = 200,
) -> SimpleNamespace:
    request_payload = {
        "from": "1700000000000",
        "to": "1700003600000",
        "queries": [_variable_query()] if queries is None else queries,
    }
    if missing_boundary is not None:
        request_payload.pop(missing_boundary)
    url = "https://grafana.example/grafana/api/ds/query"
    request = SimpleNamespace(method="POST", url=url, post_data_json=request_payload)
    payload = {
        "results": {"Variable-pod": _ds_result("api-1")} if results is None else results,
    }
    return SimpleNamespace(status=response_status, url=url, request=request, json=lambda: payload)


def _variable_query() -> dict:
    return {
        "refId": "Variable-pod",
        "datasource": {"uid": "prom"},
        "queryType": "label_values",
        "query": "up",
        "label": "pod",
    }


def _panel_query() -> dict:
    return {
        "refId": "Panel",
        "datasource": {"uid": "prom"},
        "expr": 'sum(up{pod=~"api|worker"})',
        "legendFormat": "pod",
    }


def _ds_result(value: str | None) -> dict:
    values = [] if value is None else [value]
    return {
        "status": 200,
        "frames": [{
            "schema": {"fields": [{"name": "pod"}]},
            "data": {"values": [values]},
        }],
    }


def _dom_fixture(link_target: bool) -> str:
    controls = 'aria-controls="service-options"' if link_target else ""
    onclick = "document.getElementById('service-options').style.display='block'" if link_target else ""
    return f"""
    <button id="service-control" aria-label="Dashboard variable service"
            {controls} onclick="{onclick}">service</button>
    <div id="namespace-options" role="listbox">
      <div role="option">unrelated-option</div>
    </div>
    <div id="service-options" role="listbox" style="display:none">
      <div role="option">api</div>
      <div role="option">worker</div>
      <div role="option">All</div>
    </div>
    """


def _proxy_url(path: str, params: dict[str, str]) -> str:
    base = "https://grafana.example/grafana/api/datasources/proxy/uid/prom"
    return f"{base}{path}?{urlencode(params)}"


def _metadata_response(
    route: str,
    datasource_uid: str = "prom",
    selector: str = 'kube_pod_info{namespace="app"}',
    end: str = "1700003600",
) -> SimpleNamespace:
    if route == "resources":
        route_path = f"/api/datasources/uid/{datasource_uid}/resources/api/v1/series"
    elif route == "proxy":
        route_path = f"/api/datasources/proxy/uid/{datasource_uid}/api/v1/series"
    else:
        route_path = f"/api/datasources/uid/{datasource_uid}/api/v1/series"
    params = {"match[]": selector, "start": "1700000000", "end": end}
    url = f"https://grafana.example/grafana{route_path}?{urlencode(params)}"
    return _response(url, {"status": "success", "data": [{"pod": "api-1"}]})


def _response(url: str, payload: dict) -> SimpleNamespace:
    request = SimpleNamespace(method="GET", url=url)
    return SimpleNamespace(
        status=200,
        url=url,
        request=request,
        headers={"Content-Type": "application/json"},
        json=lambda: payload,
    )


if __name__ == "__main__":
    unittest.main()
