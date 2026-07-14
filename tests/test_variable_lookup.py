import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._grafana.variable_lookup import (
    resolve_dashboard_variable_lookups,
    resolve_configured_datasource_names,
    resolve_matrix_variable_lookups,
)
from grafconflux._grafana.rendering import build_dashboard_url_params
from grafconflux._shared.grafana_models import ConfigurationError, GrafanaConfigDownloader


class TestDashboardVariableLookup(unittest.TestCase):
    def test_static_lookup_uses_label_but_sends_technical_name(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "cluster_selection": {
                "lookup": "Cluster selector",
                "value": "east",
                "display_name": "Cluster",
            },
        }))
        dashboard = self.dashboard({"name": "cluster_uri", "label": "Cluster selector", "type": "query"})

        resolve_dashboard_variable_lookups(config, dashboard)
        params = build_dashboard_url_params(self.timestamp(), 1, config.vars)

        self.assertEqual(config.vars, {"cluster_uri": "east"})
        self.assertEqual(config.vars_presentation["cluster_uri"]["display_name"], "Cluster")
        self.assertEqual(params["var-cluster_uri"], "east")
        self.assertNotIn("var-cluster_selection", params)
        self.assertNotIn("var-Cluster selector", params)

    def test_datasource_lookup_can_use_saved_raw_current_uid(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "metrics_source": {
                "lookup": "Metrics source",
                "is_datasource": True,
            },
        }))
        dashboard = self.dashboard({
            "name": "ds_internal",
            "label": "Metrics source",
            "description": "Primary metrics",
            "type": "datasource",
            "query": "prometheus",
            "current": {"text": "Prometheus", "value": "prom-main"},
        })

        resolve_dashboard_variable_lookups(config, dashboard)

        self.assertEqual(config.vars, {"ds_internal": "prom-main"})
        self.assertEqual(config.datasource_vars, {"ds_internal": "prom-main"})
        self.assertEqual(config.vars_presentation["ds_internal"]["raw_value"], "prom-main")

    def test_datasource_lookup_rejects_text_only_saved_current(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "metrics_source": {
                "lookup": "Metrics source",
                "is_datasource": True,
            },
        }))
        dashboard = self.dashboard({
            "name": "ds_internal",
            "label": "Metrics source",
            "type": "datasource",
            "current": {"text": "Prometheus"},
        })

        with self.assertRaisesRegex(ConfigurationError, "no usable current raw value"):
            resolve_dashboard_variable_lookups(config, dashboard)

    def test_datasource_lookup_rejects_non_scalar_saved_raw_current(self) -> None:
        invalid_values = (None, ["prom-main"], {"uid": "prom-main"})
        for raw_value in invalid_values:
            with self.subTest(raw_value=raw_value):
                config = GrafanaConfigDownloader("Demo", self.config({
                    "metrics_source": {
                        "lookup": "Metrics source",
                        "is_datasource": True,
                    },
                }))
                dashboard = self.dashboard({
                    "name": "ds_internal",
                    "label": "Metrics source",
                    "type": "datasource",
                    "current": {"value": raw_value, "text": "Prometheus"},
                })

                with self.assertRaisesRegex(ConfigurationError, "no usable current raw value"):
                    resolve_dashboard_variable_lookups(config, dashboard)

    def test_datasource_marker_filters_non_datasource_matches(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "metrics_source": {
                "lookup": "Metrics source",
                "is_datasource": True,
            },
        }))
        dashboard = self.dashboard(
            {"name": "query_source", "label": "Metrics source", "type": "query"},
            {
                "name": "ds_internal",
                "label": "Metrics source",
                "type": "datasource",
                "current": {"value": "prom-main"},
            },
        )

        resolve_dashboard_variable_lookups(config, dashboard)

        self.assertEqual(config.vars, {"ds_internal": "prom-main"})

    def test_datasource_name_resolves_exact_match_to_uid_for_technical_url(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "metrics_source": {
                "lookup": "Metrics source", "is_datasource": True, "name": "ICAPMock",
            },
        }))
        dashboard = self.dashboard({
            "name": "ds_internal", "label": "Metrics source", "type": "datasource",
            "current": {"text": "Wrong display value"},
        })
        session = Mock()
        session.get.return_value = Mock(status_code=200, json=Mock(return_value=[
            {"name": "ICAPMock", "uid": "icap-uid"},
        ]))

        resolve_dashboard_variable_lookups(config, dashboard)
        resolve_configured_datasource_names(config, session, "https://grafana.example/api/datasources")
        params = build_dashboard_url_params(self.timestamp(), 1, config.vars)

        self.assertEqual(config.vars, {"ds_internal": "icap-uid"})
        self.assertEqual(config.datasource_vars, {"ds_internal": "icap-uid"})
        self.assertEqual(config.vars_presentation["ds_internal"]["datasource_name"], "ICAPMock")
        self.assertEqual(params["var-ds_internal"], "icap-uid")
        self.assertNotIn("Wrong display value", params.values())
        session.get.assert_called_once_with("https://grafana.example/api/datasources", timeout=config.timeout)

    def test_datasource_name_case_and_space_mismatch_fails_safely(self) -> None:
        for configured_name in ("icapmock", "ICAPMock "):
            with self.subTest(configured_name=configured_name):
                config = GrafanaConfigDownloader("Demo", self.config({
                    "ds": {"is_datasource": True, "name": configured_name},
                }))
                session = Mock()
                session.get.return_value = Mock(status_code=200, json=Mock(return_value=[
                    {"name": "ICAPMock", "uid": "icap-uid"},
                ]))

                with self.assertRaisesRegex(ConfigurationError, "expected exactly one available datasource"):
                    resolve_configured_datasource_names(config, session, "https://grafana.example/api/datasources")

    def test_datasource_name_no_match_and_ambiguity_fail_safely(self) -> None:
        for records in ([], [{"name": "ICAPMock", "uid": "first"}, {"name": "ICAPMock", "uid": "second"}]):
            with self.subTest(records=records):
                config = GrafanaConfigDownloader("Demo", self.config({
                    "ds": {"is_datasource": True, "name": "ICAPMock"},
                }))
                session = Mock()
                session.get.return_value = Mock(status_code=200, json=Mock(return_value=records))

                with self.assertRaisesRegex(ConfigurationError, "expected exactly one available datasource"):
                    resolve_configured_datasource_names(config, session, "https://grafana.example/api/datasources")

    def test_existing_raw_datasource_uid_does_not_request_datasource_list(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "ds": {"is_datasource": True, "value": "prom-main"},
        }))
        session = Mock()

        resolve_configured_datasource_names(config, session, "https://grafana.example/api/datasources")

        self.assertEqual(config.vars, {"ds": "prom-main"})
        session.get.assert_not_called()

    def test_lookup_zero_and_multiple_matches_fail_safely(self) -> None:
        cases = (
            (self.dashboard({"name": "other", "label": "Other"}), "matched 0"),
            (
                self.dashboard(
                    {"name": "one", "label": "Shared"},
                    {"name": "two", "description": "Shared"},
                ),
                "matched 2",
            ),
        )
        for dashboard, message in cases:
            with self.subTest(message=message):
                config = GrafanaConfigDownloader("Demo", self.config({
                    "selection": {"lookup": "Shared", "value": "value"},
                }))

                with self.assertRaisesRegex(ConfigurationError, message):
                    resolve_dashboard_variable_lookups(config, dashboard)

    def test_display_name_does_not_participate_in_lookup(self) -> None:
        config = GrafanaConfigDownloader("Demo", self.config({
            "cluster": {"value": "east", "display_name": "Cluster selector"},
        }))

        resolve_dashboard_variable_lookups(
            config,
            self.dashboard({"name": "technical", "label": "Cluster selector"}),
        )

        self.assertEqual(config.vars, {"cluster": "east"})

    @staticmethod
    def config(variables: dict) -> dict:
        return {
            "grafana_url": "https://grafana.example/grafana",
            "dash_title": "Demo",
            "vars": variables,
        }

    @staticmethod
    def dashboard(*variables: dict) -> dict:
        return {"templating": {"list": list(variables)}}

    @staticmethod
    def timestamp() -> SimpleNamespace:
        return SimpleNamespace(start_time_timestamp=1, end_time_timestamp=2)


class TestMatrixVariableLookup(unittest.TestCase):
    def test_matrix_lookup_resolves_to_raw_name_without_changing_display_name(self) -> None:
        matrix = {
            "variables": {
                "workload": {
                    "lookup": "Service selector",
                    "display_name": "Application",
                    "values": ["api"],
                },
            },
        }
        dashboard = {"templating": {"list": [{
            "name": "service_uri",
            "label": "Service selector",
            "description": "Workload service",
            "type": "query",
        }]}}

        resolved = resolve_matrix_variable_lookups("Demo", matrix, dashboard)

        self.assertEqual(resolved["variables"]["workload"]["grafana_variable"], "service_uri")
        self.assertEqual(resolved["variables"]["workload"]["display_name"], "Application")

    def test_matrix_lookup_rejects_explicit_technical_name(self) -> None:
        config = {
            "grafana_url": "https://grafana.example/grafana",
            "dash_title": "Demo",
            "render_matrix": {
                "variables": {
                    "service": {
                        "lookup": "Service selector",
                        "grafana_variable": "service_uri",
                        "values": ["api"],
                    },
                },
            },
        }

        with self.assertRaisesRegex(ValueError, "lookup.*grafana_variable"):
            GrafanaConfigDownloader("Demo", config)


if __name__ == "__main__":
    unittest.main()
