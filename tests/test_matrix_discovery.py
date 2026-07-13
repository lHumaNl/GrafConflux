import logging
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from grafconflux._grafana.matrix import _discovery_context, append_matrix_tasks
from grafconflux._grafana.matrix_browser_planning import BrowserMatrixFallback
from grafconflux._grafana.matrix_dependencies import ordered_matrix_variables
from grafconflux._grafana.matrix_discovery import (
    MatrixDiscoveryStatus,
    MatrixValueResolver,
    safe_discovery_values,
)
from grafconflux._shared.grafana_models import ConfigurationError


class TestMatrixDependencies(unittest.TestCase):
    def test_infers_all_supported_reference_forms_and_topologically_orders(self) -> None:
        matrix = {
            "variables": {
                "pod": {"values_from": {}},
                "cluster": {"values": ["prod"]},
                "namespace": {"values_from": {}},
            }
        }
        dashboard = self.dashboard([
            {"name": "cluster", "query": "constant"},
            {"name": "namespace", "query": 'label_values(up{cluster="$cluster"}, namespace)'},
            {
                "name": "pod",
                "query": 'label_values(up{a="${cluster}", b="${namespace:regex}", c="[[namespace]]"}, pod)',
            },
        ])

        ordered, dependencies = ordered_matrix_variables("Demo", matrix, dashboard)

        self.assertEqual(ordered, ["cluster", "namespace", "pod"])
        self.assertEqual(dependencies["namespace"], ["cluster"])
        self.assertEqual(dependencies["pod"], ["cluster", "namespace"])

    def test_explicit_dependencies_are_authoritative(self) -> None:
        matrix = {
            "variables": {
                "cluster": {"values": ["prod"]},
                "namespace": {"values": ["system"]},
                "pod": {"depends_on": ["cluster"], "values_from": {}},
            }
        }
        dashboard = self.dashboard([
            {"name": "pod", "query": 'label_values(up{namespace="$namespace"}, pod)'},
        ])

        _, dependencies = ordered_matrix_variables("Demo", matrix, dashboard)

        self.assertEqual(dependencies["pod"], ["cluster"])

    def test_cycle_fails_with_safe_configuration_error(self) -> None:
        matrix = {"variables": {"left": {"values_from": {}}, "right": {"values_from": {}}}}
        dashboard = self.dashboard([
            {"name": "left", "query": 'label_values(up{right="$right"}, left)'},
            {"name": "right", "query": 'label_values(up{left="$left"}, right)'},
        ])

        with self.assertRaisesRegex(ConfigurationError, "dependency cycle"):
            ordered_matrix_variables("Demo", matrix, dashboard)

    @staticmethod
    def dashboard(variables: list[dict]) -> dict:
        return {"templating": {"list": variables}}


class TestMatrixValueResolver(unittest.TestCase):
    def setUp(self) -> None:
        self.timestamp = SimpleNamespace(
            id_time=7,
            time_tag="period",
            start_time_timestamp=1_700_000_000_000,
            end_time_timestamp=1_700_003_600_000,
        )
        self.config = SimpleNamespace(
            grafana_base_url="https://grafana.example/grafana",
            timeout=30,
            datasource_vars={},
        )

    def test_prometheus_selector_uses_series_seconds_and_extracts_label_maps(self) -> None:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={
                "status": "success",
                "data": [{"pod": "api-1", "namespace": "app"}, {"pod": "api-2"}],
            }),
        )
        resolver = self.resolver(session, 'label_values(kube_pod_info{namespace="$namespace"}, pod)')

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {"namespace": "app"}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api-1", "api-2"])
        request = session.get.call_args
        self.assertTrue(request.args[0].endswith("/api/v1/series"))
        self.assertEqual(request.kwargs["params"], {
            "match[]": 'kube_pod_info{namespace="app"}',
            "start": "1700000000",
            "end": "1700003600",
        })

    def test_label_only_query_uses_label_values_endpoint(self) -> None:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": ["api", "worker"]}),
        )
        resolver = self.resolver(session, "label_values(service)")

        result = resolver.resolve("service", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertTrue(session.get.call_args.args[0].endswith("/api/v1/label/service/values"))

    def test_authoritative_empty_is_distinct_from_failure(self) -> None:
        empty_session = Mock()
        empty_session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": []}),
        )
        failed_session = Mock()
        failed_session.get.return_value = Mock(status_code=503)

        empty = self.resolver(empty_session, "label_values(service)").resolve(
            "service", {"values_from": {}}, self.timestamp, {}, {},
        )
        failed = self.resolver(failed_session, "label_values(service)").resolve(
            "service", {"values_from": {}}, self.timestamp, {}, {},
        )

        self.assertEqual(empty.status, MatrixDiscoveryStatus.EMPTY)
        self.assertEqual(failed.status, MatrixDiscoveryStatus.FAILED)

    def test_invalid_json_does_not_use_saved_dashboard_options(self) -> None:
        session = Mock()
        session.get.return_value = Mock(status_code=200, json=Mock(side_effect=ValueError("invalid")))
        resolver = self.resolver(session, "label_values(service)", options=[{"value": "stale"}])

        result = resolver.resolve("service", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.FAILED)
        self.assertEqual(result.values, [])

    def test_series_failure_logs_one_safe_compact_primary_diagnostic(self) -> None:
        session = Mock()
        session.get.return_value = Mock(status_code=503, headers={})
        resolver = self.resolver(session, 'label_values(up{namespace="$namespace"}, pod)')

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="WARNING") as logs:
            resolver.resolve("pod", {"values_from": {}}, self.timestamp, {"namespace": "private"}, {})

        events = [line for line in logs.output if "primary_series" in line]
        self.assertEqual(len(events), 1)
        diagnostic = events[0]
        self.assertIn("route_family=proxy_uid", diagnostic)
        self.assertIn("http_status=503", diagnostic)
        self.assertIn("response_classification=non_json", diagnostic)
        self.assertIn("outcome=http_non_2xx", diagnostic)
        self.assertIn("reference_vars=['namespace']", diagnostic)
        self.assertNotIn("private", diagnostic)
        self.assertNotIn("up{", diagnostic)

    def test_saved_current_is_not_used_for_another_dynamic_matrix_variable(self) -> None:
        session = Mock()
        dashboard = {"templating": {"list": [
            {"name": "namespace", "current": {"value": "stale-namespace"}},
            {
                "name": "service", "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(up{namespace="$namespace"}, service)',
            },
        ]}}
        resolver = MatrixValueResolver(
            dashboard,
            session,
            self.config,
            dynamic_variable_names={"namespace", "service"},
        )

        result = resolver.resolve("service", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        session.get.assert_not_called()

    def test_cache_key_separates_timestamp_and_context(self) -> None:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": ["api"]}),
        )
        resolver = self.resolver(session, 'label_values(up{namespace="$namespace"}, service)')

        resolver.resolve("service", {"values_from": {}}, self.timestamp, {"namespace": "a"}, {})
        resolver.resolve("service", {"values_from": {}}, self.timestamp, {"namespace": "a"}, {})
        resolver.resolve("service", {"values_from": {}}, self.timestamp, {"namespace": "b"}, {})
        later = SimpleNamespace(**{**vars(self.timestamp), "id_time": 8, "end_time_timestamp": 1_700_007_200_000})
        resolver.resolve("service", {"values_from": {}}, later, {"namespace": "a"}, {})

        self.assertEqual(session.get.call_count, 3)

    def test_logical_key_does_not_replace_distinct_dashboard_variable(self) -> None:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": [{"service": "api"}]}),
        )
        dashboard = {"templating": {"list": [
            {"name": "environment", "type": "custom", "current": {"value": "shared"}},
            {
                "name": "service", "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(up{env="$env", environment="$environment"}, service)',
            },
        ]}}
        matrix = {"variables": {
            "environment": {"grafana_variable": "env", "values": ["prod"]},
            "application": {"grafana_variable": "service", "values_from": {}},
        }}
        context = _discovery_context(matrix, {"environment": "prod"})
        resolver = MatrixValueResolver(
            dashboard, session, self.config, dynamic_variable_names={"env", "service"},
        )

        result = resolver.resolve(
            "application", matrix["variables"]["application"], self.timestamp, context, {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(context, {"env": "prod"})
        self.assertEqual(
            session.get.call_args.kwargs["params"]["match[]"],
            'up{env="prod", environment="shared"}',
        )

    def test_prometheus_payload_requires_explicit_success_status(self) -> None:
        payloads = (
            {"data": []}, {"data": ["api"]},
            {"status": "unknown", "data": []}, {"status": "error", "data": []},
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                session = Mock()
                session.get.return_value = Mock(status_code=200, json=Mock(return_value=payload))

                result = self.resolver(session, "label_values(service)").resolve(
                    "service", {"values_from": {}}, self.timestamp, {}, {},
                )

                self.assertFalse(result.authoritative)
                self.assertNotEqual(result.status, MatrixDiscoveryStatus.EMPTY)

    def test_safe_values_redact_sensitive_names_and_secret_like_content(self) -> None:
        self.assertEqual(safe_discovery_values("api_token", ["plain"]), ["<redacted>"])
        values = safe_discovery_values("service", ["api", "Bearer abcdefghijklmnopqrstuvwxyz", "x" * 100])
        self.assertEqual(values[0], "api")
        self.assertEqual(values[1], "<redacted>")
        self.assertTrue(values[2].endswith("..."))

    def resolver(self, session: Mock, query: str, options: list[dict] | None = None) -> MatrixValueResolver:
        dashboard = {
            "templating": {"list": [{
                "name": "service" if "pod" not in query else "pod",
                "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": query,
                "options": options or [],
            }]}
        }
        return MatrixValueResolver(dashboard, session, self.config)


class TestBrowserMatrixFallback(unittest.TestCase):
    def test_uses_normal_dashboard_period_context_and_correlated_network_response(self) -> None:
        timestamp = SimpleNamespace(
            id_time=3,
            start_time_timestamp=1_700_000_000_000,
            end_time_timestamp=1_700_003_600_000,
        )
        page = _FakePlanningPage()
        browser = _FakePlanningBrowser(page)
        config = SimpleNamespace(vars={}, orgId=2, timeout=1, datasource_vars={})
        fallback = BrowserMatrixFallback(
            config,
            Mock(),
            "https://grafana.example/grafana/d/uid/dashboard",
            browser_factory=lambda: browser,
        )
        variable = {
            "name": "pod",
            "type": "query",
            "datasource": {"type": "prometheus", "uid": "prom"},
            "query": 'label_values(kube_pod_info{namespace="$namespace"}, pod)',
        }

        result = fallback.discover("pod", variable, timestamp, {"namespace": "app"})
        fallback.close()

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api-1"])
        self.assertIn("/d/uid/dashboard?", browser.opened_url)
        self.assertIn("from=1700000000000", browser.opened_url)
        self.assertIn("to=1700003600000", browser.opened_url)
        self.assertIn("var-namespace=app", browser.opened_url)
        self.assertNotIn("settings", browser.opened_url.lower())
        self.assertNotIn("edit", browser.opened_url.lower())
        self.assertTrue(browser.closed)

    def test_network_payload_without_success_status_is_not_authoritative_empty(self) -> None:
        for status in (None, "unknown", "error"):
            with self.subTest(status=status):
                payload = {"data": []}
                if status is not None:
                    payload["status"] = status
                page = _FakePlanningPage()
                browser = _FakePlanningBrowser(page, payload=payload)
                fallback = self.fallback(browser)

                result = fallback.discover("pod", self.variable(), self.timestamp(), {"namespace": "app"})

                self.assertFalse(result.authoritative)
                self.assertNotEqual(result.status, MatrixDiscoveryStatus.EMPTY)

    def test_network_response_rejects_wrong_datasource_and_time_candidates(self) -> None:
        page = _FakePlanningPage()
        responses = [
            _series_response("other", "1700000000", "1700003600", "wrong-datasource"),
            _series_response("prom", "1700000001", "1700003600", "wrong-time"),
            _series_response("prom", "1700000000", "1700003600", "api-1"),
        ]
        browser = _FakePlanningBrowser(page, responses=responses)
        fallback = self.fallback(browser)

        result = fallback.discover("pod", self.variable(), self.timestamp(), {"namespace": "app"})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api-1"])

    def test_dom_fallback_aggregates_safe_network_rejections(self) -> None:
        page = _FakePlanningPage()
        browser = _FakePlanningBrowser(page, responses=[
            _series_response("other", "1700000000", "1700003600", "wrong"),
            _series_response("prom", "1700000001", "1700003600", "wrong"),
        ])
        fallback = self.fallback(browser)

        with self.assertLogs("grafconflux._grafana.matrix_browser_planning", level="INFO") as logs:
            result = fallback.discover("pod", self.variable(), self.timestamp(), {"namespace": "private"})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        diagnostic = "\n".join(logs.output)
        self.assertIn("navigation_datasource_present=True", diagnostic)
        self.assertIn("navigation_datasource_source=direct", diagnostic)
        self.assertIn("route_family': 'proxy_uid'", diagnostic)
        self.assertIn("rejection': 'datasource_correlation'", diagnostic)
        self.assertNotIn("private", diagnostic)
        self.assertNotIn("https://", diagnostic)

    def test_label_response_rejects_same_label_from_another_datasource(self) -> None:
        page = _FakePlanningPage()
        browser = _FakePlanningBrowser(page, responses=[
            _label_response("other", "wrong"),
            _label_response("prom", "api"),
        ])
        fallback = self.fallback(browser)
        variable = {
            "name": "service", "type": "query",
            "datasource": {"type": "prometheus", "uid": "prom"},
            "query": "label_values(service)",
        }

        result = fallback.discover("service", variable, self.timestamp(), {})

        self.assertEqual(result.values, ["api"])

    def test_dom_options_are_read_from_the_exact_opened_variable_control(self) -> None:
        page = _ScopedDomPlanningPage()
        browser = _FakePlanningBrowser(page, responses=[])
        fallback = self.fallback(browser)

        result = fallback.discover(
            "service", {"name": "service", "type": "custom"}, self.timestamp(), {},
        )

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.values, ["api", "worker"])
        self.assertEqual(page.opened_name, "service")
        self.assertEqual(page.read_scope, {"popupId": "service-options", "controlId": "service-control"})

    def test_close_failure_is_best_effort(self) -> None:
        browser = Mock()
        browser.quit.side_effect = RuntimeError("close failed")
        fallback = self.fallback(browser)
        fallback.browser = browser

        fallback.close()

        self.assertIsNone(fallback.browser)

    def test_planning_cleanup_does_not_mask_original_discovery_error(self) -> None:
        config = SimpleNamespace(
            name="Demo", render_matrix={"variables": {"service": {"values_from": {}}}}, vars={},
        )
        fallback = Mock()
        fallback.close.side_effect = RuntimeError("close failed")
        original = ConfigurationError("discovery failed")
        browser_factory = Mock(return_value=fallback)
        rows = Mock(side_effect=original)
        with patch.dict(append_matrix_tasks.__globals__, {
            "BrowserMatrixFallback": browser_factory,
            "_rows_by_timestamp": rows,
        }):
            with self.assertRaisesRegex(ConfigurationError, "discovery failed"):
                append_matrix_tasks(config, {}, [], [], [], [], Mock(), "/d/uid/demo")

        fallback.close.assert_called_once_with()

    @staticmethod
    def timestamp() -> SimpleNamespace:
        return SimpleNamespace(
            id_time=3,
            start_time_timestamp=1_700_000_000_000,
            end_time_timestamp=1_700_003_600_000,
        )

    @staticmethod
    def variable() -> dict:
        return {
            "name": "pod", "type": "query",
            "datasource": {"type": "prometheus", "uid": "prom"},
            "query": 'label_values(kube_pod_info{namespace="$namespace"}, pod)',
        }

    def fallback(self, browser: object) -> BrowserMatrixFallback:
        return BrowserMatrixFallback(
            SimpleNamespace(vars={}, orgId=2, timeout=1, datasource_vars={}),
            Mock(),
            "https://grafana.example/grafana/d/uid/dashboard",
            browser_factory=lambda: browser,
        )


class _FakePlanningPage:
    def __init__(self) -> None:
        self.listeners: dict[str, object] = {}

    def on(self, event: str, handler: object) -> None:
        self.listeners[event] = handler

    def remove_listener(self, event: str, _handler: object) -> None:
        self.listeners.pop(event, None)

    def wait_for_timeout(self, _milliseconds: int) -> None:
        return None

    def evaluate(self, _script: str, *_args: object) -> object:
        return []


class _ScopedDomPlanningPage(_FakePlanningPage):
    def __init__(self) -> None:
        super().__init__()
        self.opened_name = None
        self.read_scope = None

    def evaluate(self, script: str, *args: object) -> object:
        if "target.click()" in script:
            self.opened_name = args[0]
            return {"popupId": "service-options", "controlId": "service-control"}
        self.read_scope = args[0]
        return ["api", "worker", "All"]


class _FakePlanningBrowser:
    def __init__(
        self,
        page: _FakePlanningPage,
        payload: dict | None = None,
        responses: list[object] | None = None,
    ) -> None:
        self.page = page
        self.payload = payload or {"status": "success", "data": [{"pod": "api-1"}]}
        self.responses = responses
        self.opened_url = ""
        self.closed = False

    def get(self, url: str) -> None:
        self.opened_url = url
        responses = self.responses
        if responses is None:
            responses = [_series_response("prom", "1700000000", "1700003600", payload=self.payload)]
        for response in responses:
            self.page.listeners["response"](response)

    def close(self) -> None:
        self.closed = True


def _series_response(
    datasource_uid: str,
    start: str,
    end: str,
    pod: str = "api-1",
    payload: dict | None = None,
) -> SimpleNamespace:
    url = (
        f"https://grafana.example/grafana/api/datasources/proxy/uid/{datasource_uid}/api/v1/series"
        f"?match%5B%5D=kube_pod_info%7Bnamespace%3D%22app%22%7D&start={start}&end={end}"
    )
    body = payload or {"status": "success", "data": [{"pod": pod}]}
    request = SimpleNamespace(method="GET", url=url)
    return SimpleNamespace(status=200, url=url, request=request, json=lambda: body)


def _label_response(datasource_uid: str, value: str) -> SimpleNamespace:
    url = (
        f"https://grafana.example/grafana/api/datasources/proxy/uid/{datasource_uid}"
        "/api/v1/label/service/values?start=1700000000&end=1700003600"
    )
    request = SimpleNamespace(method="GET", url=url)
    return SimpleNamespace(
        status=200, url=url, request=request,
        json=lambda: {"status": "success", "data": [value]},
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
