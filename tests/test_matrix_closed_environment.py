import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._grafana.matrix_discovery import MatrixDiscoveryStatus, MatrixValueResolver


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
        resolver = self.resolver(session, {"value": "", "text": "None"})

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="INFO") as logs:
            result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.RESOLVED)
        self.assertEqual(
            session.get.call_args.kwargs["params"]["match[]"],
            'kube_pod_info{cluster=""}',
        )
        diagnostic = "\n".join(logs.output)
        self.assertIn("dashboard=Kubernetes", diagnostic)
        self.assertIn("variable=cluster", diagnostic)
        self.assertIn("timestamp_id=9", diagnostic)
        self.assertIn("source=current.value", diagnostic)
        self.assertNotIn("None", diagnostic)

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

    def test_null_current_and_default_are_not_coerced_to_empty(self) -> None:
        session = self.successful_session()
        resolver = self.resolver(session, {"value": None, "text": None}, default=None)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(result.provenance["method"], "invalid_or_missing_context")
        session.get.assert_not_called()

    def test_non_query_variable_reports_adapter_not_applicable(self) -> None:
        dashboard = {"templating": {"list": [{"name": "pod", "type": "custom"}]}}
        resolver = MatrixValueResolver(dashboard, Mock(), self.config)

        result = resolver.resolve("pod", {"values_from": {}}, self.timestamp, {}, {})

        self.assertEqual(result.status, MatrixDiscoveryStatus.UNSUPPORTED)
        self.assertEqual(result.provenance["method"], "adapter_not_applicable")

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
    def successful_session() -> Mock:
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": [{"pod": "api-1"}]}),
        )
        return session


if __name__ == "__main__":
    unittest.main()
