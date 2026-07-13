import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlencode

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from grafconflux._grafana.matrix_browser_planning import BrowserMatrixFallback
from grafconflux._grafana.matrix_discovery import MatrixDiscoveryStatus


class TestBrowserMatrixPlanningCorrelation(unittest.TestCase):
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

    def fallback(self, responses: list[object]) -> BrowserMatrixFallback:
        browser = _PlanningBrowser(_PlanningPage(), responses)
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
    def __init__(self) -> None:
        self.listeners: dict[str, object] = {}

    def on(self, event: str, handler: object) -> None:
        self.listeners[event] = handler

    def remove_listener(self, event: str, _handler: object) -> None:
        self.listeners.pop(event, None)

    def evaluate(self, _script: str, *_args: object) -> object:
        return []


class _PlanningBrowser:
    def __init__(self, page: _PlanningPage, responses: list[object]) -> None:
        self.page = page
        self.responses = responses

    def get(self, _url: str) -> None:
        for response in self.responses:
            self.page.listeners["response"](response)


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


def _response(url: str, payload: dict) -> SimpleNamespace:
    request = SimpleNamespace(method="GET", url=url)
    return SimpleNamespace(status=200, url=url, request=request, json=lambda: payload)


if __name__ == "__main__":
    unittest.main()
