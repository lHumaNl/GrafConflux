import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._grafana.matrix_discovery import MatrixValueResolver


class TestMatrixOperationalDiagnostics(unittest.TestCase):
    def test_adapter_log_has_outcome_without_request_dump(self) -> None:
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
        self.assertIn("status=200", diagnostic)
        self.assertIn("value_count=2", diagnostic)
        self.assertNotIn("token=known-secret", diagnostic)
        self.assertNotIn("opaque-private-material-8675309", diagnostic)
        self.assertNotIn("https://", diagnostic)
        self.assertNotIn("private-uid", diagnostic)
        self.assertNotIn("request_url", diagnostic)
        self.assertNotIn("normalized_selector", diagnostic)


if __name__ == "__main__":
    unittest.main()
