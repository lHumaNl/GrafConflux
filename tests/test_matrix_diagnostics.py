import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._grafana.matrix_discovery import MatrixValueResolver


class TestMatrixOperationalDiagnostics(unittest.TestCase):
    def test_empty_result_log_includes_an_empty_values_list(self) -> None:
        dashboard = {"templating": {"list": [{
            "name": "service", "type": "query",
            "datasource": {"type": "prometheus", "uid": "private-uid"},
            "query": "label_values(service)",
        }]}}
        session = Mock()
        session.get.return_value = Mock(status_code=200, json=Mock(return_value={"status": "success", "data": []}))
        config = SimpleNamespace(grafana_base_url="https://grafana.example/grafana", timeout=30, datasource_vars={})
        timestamp = SimpleNamespace(id_time=1, start_time_timestamp=1_700_000_000_000, end_time_timestamp=1_700_003_600_000)

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="WARNING") as logs:
            MatrixValueResolver(dashboard, session, config).resolve("service", {"values_from": {}}, timestamp, {}, {})

        self.assertEqual(logs.output, ["WARNING:grafconflux._grafana.matrix_discovery:matrix_discovery variable=service period=1700000000000..1700003600000 count=0 values=[]"])

    def test_result_log_includes_complete_discovered_values_without_request_details(self) -> None:
        dashboard = {"templating": {"list": [{
            "name": "service",
            "type": "query",
            "datasource": {"type": "prometheus", "uid": "private-uid"},
            "query": "label_values(service)",
        }]}}
        session = Mock()
        session.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={
                "status": "success",
                "data": ["token=known-secret", "opaque-private-material-8675309"],
            }),
        )
        config = SimpleNamespace(
            grafana_base_url="https://grafana.example/grafana",
            timeout=30,
            datasource_vars={},
        )
        timestamp = SimpleNamespace(
            id_time=1,
            start_time_timestamp=1_700_000_000_000,
            end_time_timestamp=1_700_003_600_000,
        )

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="INFO") as logs:
            MatrixValueResolver(dashboard, session, config).resolve(
                "service", {"values_from": {}}, timestamp, {}, {},
            )

        diagnostic = "\n".join(logs.output)
        self.assertEqual(len(logs.output), 1)
        self.assertIn("matrix_discovery variable=service period=1700000000000..1700003600000 count=2", diagnostic)
        self.assertIn("values=['token=known-secret', 'opaque-private-material-8675309']", diagnostic)
        self.assertNotIn("https://", diagnostic)
        self.assertNotIn("private-uid", diagnostic)
        self.assertNotIn("request_url", diagnostic)
        self.assertNotIn("normalized_selector", diagnostic)


if __name__ == "__main__":
    unittest.main()
