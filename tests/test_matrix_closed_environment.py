import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._grafana.matrix_discovery import MatrixDiscoveryStatus, MatrixValueResolver
from grafconflux.grafana import GrafanaConfigDownloader, GrafanaManager


class TestEmptyDashboardContextDefaults(unittest.TestCase):
    def setUp(self) -> None:
        self.timestamp = SimpleNamespace(
            id_time=9,
            start_time_timestamp=1_700_000_000_000,
            end_time_timestamp=1_700_003_600_000,
        )
        self.config = SimpleNamespace(
            name="Kubernetes",
            grafana_base_url="https://grafana.example/grafana",
            timeout=30,
            datasource_vars={},
        )

    def test_empty_current_query_default_is_valid_non_matrix_context(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, {"value": "", "text": None})

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="INFO") as logs:
            result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(
            session.get.call_args.kwargs["params"]["match[]"],
            'kube_pod_info{cluster=""}',
        )
        diagnostic = "\n".join(logs.output)
        self.assertIn("variable=pod", diagnostic)
        self.assertIn("time=1700000000000-1700003600000", diagnostic)
        self.assertIn("context={}", diagnostic)
        self.assertEqual(len(logs.output), 1)
        self.assertIn(
            "matrix_discovery variable=pod time=1700000000000-1700003600000 "
            "context={} count=1",
            diagnostic,
        )
        self.assertIn("values=['api-1']", diagnostic)

    def test_empty_saved_default_is_used_when_current_is_absent(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, None, default="")

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.provenance["dashboard_context_sources"], {"cluster": "default"})

    def test_empty_default_never_replaces_dynamic_or_explicit_context(self) -> None:
        for dynamic_names, context, static_vars, expected in (
            ({"cluster", "pod"}, {"cluster": "parent"}, {}, "parent"),
            ({"pod"}, {}, {"cluster": "configured"}, "configured"),
            ({"pod"}, {}, {"cluster": ""}, ""),
        ):
            with self.subTest(expected=expected):
                session = self.successful_session()
                resolver = self.resolver(session, {"value": ""}, dynamic_names=dynamic_names)

                resolver.resolve("pod", {"values_from": {}}, self.timestamp, context, static_vars)

                self.assertEqual(
                    session.get.call_args.kwargs["params"]["match[]"],
                    f'kube_pod_info{{cluster="{expected}"}}',
                )

    def test_explicit_vars_override_parent_rows_and_dashboard_current(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, {"value": "dashboard"})

        result = resolver.resolve(
            "pod",
            {"values_from": {}},
            self.timestamp,
            {"cluster": "parent"},
            {"cluster": "configured"},
        )

        self.assertEqual(
            session.get.call_args.kwargs["params"]["match[]"],
            'kube_pod_info{cluster="configured"}',
        )
        self.assertEqual(result.provenance["context_sources"]["cluster"], "explicit_vars")
        self.assertNotIn("cluster", result.provenance["dashboard_context_sources"])

    def test_full_planning_path_preserves_empty_cluster_and_direct_datasource(self) -> None:
        dashboard = self.dashboard_with_exact_closed_environment_variables()
        config = GrafanaConfigDownloader("Kubernetes", {
            "grafana_url": "https://grafana.example/grafana",
            "dash_title": "Kubernetes",
            "render_matrix": {
                "variables": {
                    "namespace": {"values": ["team-a"]},
                    "pod": {"values_from": {}},
                },
            },
        })
        manager = GrafanaManager(config)
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/kubernetes"
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})),
            self.successful_session().get.return_value,
        ])

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="INFO") as logs:
            manager.get_panels([self.timestamp])

        request = manager.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom-main/api/v1/series", request.args[0])
        self.assertEqual(
            request.kwargs["params"]["match[]"],
            'kube_pod_info{cluster="", namespace="team-a"}',
        )
        discovery = manager.config.render_matrix_rows_by_timestamp[9][0]["discovery"]["pod"]
        self.assertEqual(discovery["dashboard_context_sources"], {
            "cluster": "current.value",
            "datasource": "current.value",
        })
        self.assertEqual(discovery["datasource_resolution"], {
            "source": "direct",
            "type_status": "resolved_prometheus",
            "uid_status": "resolved",
            "uid_source": "direct",
        })
        diagnostic = "\n".join(logs.output)
        self.assertEqual(len(logs.output), 1)
        self.assertIn(
            "matrix_discovery variable=pod time=1700000000000-1700003600000 "
            "context={'namespace': 'team-a'} count=1",
            diagnostic,
        )
        self.assertIn("values=['api-1']", diagnostic)

    def test_unresolved_discovery_logs_one_safe_final_warning(self) -> None:
        dashboard = {"templating": {"list": [{
            "name": "pod",
            "type": "query",
            "datasource": {"type": "prometheus", "uid": "fake-uid-should-not-log"},
            "query": 'label_values(up{cluster="$cluster", token="fake-query-secret"}, pod)',
        }]}}
        resolver = MatrixValueResolver(dashboard, Mock(), self.config, dynamic_variable_names={"pod"})

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="WARNING") as logs:
            result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        diagnostic = "\n".join(logs.output)
        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(len(logs.output), 1)
        self.assertIn(
            "matrix_discovery variable=pod time=1700000000000-1700003600000 "
            "context={} status=unsupported",
            diagnostic,
        )
        self.assertIn("reason=invalid_or_missing_context", diagnostic)
        self.assertNotIn("fake-uid-should-not-log", diagnostic)
        self.assertNotIn("fake-query-secret", diagnostic)

    def test_null_current_and_default_are_not_coerced_to_empty(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, {"value": None, "text": None}, default=None)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(result.provenance["method"], "invalid_or_missing_context")
        self.assertEqual(result.provenance["diagnosis"], "query_context_missing")
        self.assertEqual(result.provenance["missing_context_vars"], ["cluster"])
        session.get.assert_not_called()

    def test_missing_current_is_not_coerced_to_empty(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, None)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(result.provenance["diagnosis"], "query_context_missing")
        session.get.assert_not_called()

    def test_non_query_variable_reports_adapter_not_applicable(self) -> None:
        dashboard = {"templating": {"list": [{"name": "pod", "type": "custom"}]}}
        resolver = MatrixValueResolver(dashboard, Mock(), self.config)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(result.provenance["method"], "adapter_not_applicable")

    def test_missing_direct_datasource_uid_has_precise_safe_diagnosis(self) -> None:
        dashboard = {"templating": {"list": [{
            "name": "pod",
            "type": "query",
            "datasource": {"type": "prometheus"},
            "query": "label_values(pod)",
        }]}}
        session = Mock()
        resolver = MatrixValueResolver(dashboard, session, self.config)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
        self.assertEqual(result.provenance["diagnosis"], "datasource_uid_missing")
        self.assertEqual(result.provenance["datasource_resolution"], {
            "source": "direct",
            "type_status": "resolved_prometheus",
            "uid_status": "missing",
            "uid_source": "missing",
        })
        session.get.assert_not_called()

    def test_explicit_empty_cluster_override_is_used_when_saved_current_is_missing(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, None)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {"cluster": ""})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(result.provenance["context_sources"]["cluster"], "explicit_vars")
        self.assertEqual(session.get.call_args.kwargs["params"]["match[]"], 'kube_pod_info{cluster=""}')

    def test_prometheus_mapping_without_uid_uses_validated_datasource_context(self) -> None:
        dashboard = self.dashboard_with_missing_pod_uid({"query": "prometheus", "current": {"value": "prom-main"}})
        session = self.successful_session()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": ["api-1"]}),
        )
        resolver = MatrixValueResolver(dashboard, session, self.config, dynamic_variable_names={"pod"})

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertIn("/uid/prom-main/", session.get.call_args.args[0])
        self.assertEqual(result.provenance["datasource_resolution"]["uid_source"], "datasource_context_validated")

    def test_missing_uid_rejects_invalid_or_ambiguous_datasource_context(self) -> None:
        cases = (
            (({"query": "loki", "current": {"value": "loki-main"}},), {}),
            (({"query": "prometheus", "current": {"value": "prom-main"}},), {"datasource": []}),
            (({"query": "prometheus", "current": {"value": "prom-one"}}, {"query": "prometheus", "current": {"value": "prom-two"}}), {}),
        )
        for datasource_variables, static_vars in cases:
            with self.subTest(datasource_variables=datasource_variables):
                dashboard = self.dashboard_with_missing_pod_uid(*datasource_variables)
                session = Mock()
                resolver = MatrixValueResolver(dashboard, session, self.config, dynamic_variable_names={"pod"})

                result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, static_vars)

                self.assertEqual(result.status, MatrixDiscoveryStatus.UNRESOLVED)
                self.assertEqual(result.provenance["datasource_resolution"]["uid_source"], "missing")
                session.get.assert_not_called()

    def resolver(
        self,
        session: Mock,
        current: dict | None,
        default: object = None,
        dynamic_names: set[str] | None = None,
    ) -> MatrixValueResolver:
        cluster = {"name": "cluster", "type": "query"}
        if current is not None:
            cluster["current"] = current
        if default is not None or default == "":
            cluster["default"] = default
        dashboard = {
            "title": "Kubernetes",
            "templating": {"list": [cluster, {
                "name": "pod",
                "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(kube_pod_info{cluster="$cluster"}, pod)',
            }]},
        }
        return MatrixValueResolver(
            dashboard,
            session,
            self.config,
            dynamic_variable_names=dynamic_names or {"pod"},
        )

    @staticmethod
    def dashboard_with_exact_closed_environment_variables() -> dict:
        return {
            "title": "Kubernetes",
            "panels": [],
            "templating": {"list": [
                {
                    "name": "datasource",
                    "type": "datasource",
                    "query": "prometheus",
                    "current": {"text": "Prometheus", "value": "prom-main"},
                },
                {
                    "name": "cluster",
                    "type": "query",
                    "current": {"text": None, "value": ""},
                    "datasource": {"type": "prometheus", "uid": "prom-main"},
                    "query": "label_values(kube_node_info,cluster)",
                },
                {
                    "name": "namespace",
                    "type": "query",
                    "current": {"text": "stale", "value": "stale"},
                },
                {
                    "name": "pod",
                    "type": "query",
                    "datasource": {"type": "prometheus", "uid": "prom-main"},
                    "query": 'label_values(kube_pod_info{cluster="$cluster", namespace="$namespace"}, pod)',
                },
            ]},
        }

    @staticmethod
    def dashboard_with_missing_pod_uid(*datasource_variables: dict) -> dict:
        variables = [
            {"name": f"datasource{index}", "type": "datasource", **value}
            for index, value in enumerate(datasource_variables or ({"query": "prometheus", "current": {"value": "prom-main"}},))
        ]
        if len(variables) == 1:
            variables[0]["name"] = "datasource"
        variables.append({
            "name": "pod",
            "type": "query",
            "datasource": {"type": "prometheus"},
            "query": "label_values(pod)",
        })
        return {"templating": {"list": variables}}

    @staticmethod
    def successful_session() -> Mock:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": [{"pod": "api-1"}]}),
        )
        return session


if __name__ == "__main__":
    unittest.main()
