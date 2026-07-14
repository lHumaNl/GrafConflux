import os
import tempfile
import textwrap
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import yaml

from grafconflux._confluence.content import build_confluence_storage_content
from grafconflux._confluence.content import _grouped_matrix_artifacts
from grafconflux._orchestration.upload_merge import _shift_matrix_dashboard_links, transform_grafana_configs
from grafconflux._shared.confluence_settings import ConfluenceRenderingSettings
from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux._grafana.matrix_discovery import MatrixDiscoveryStatus, MatrixValueResult
from grafconflux.grafana import ConfigurationError, GrafanaConfigUploader, GrafanaManager, Panel


class TestRenderMatrixPlanning(unittest.TestCase):
    @staticmethod
    def timestamp(index: int = 0) -> GrafanaTimeDownloader:
        return GrafanaTimeDownloader(f"smoke{index}__&from=170000000{index}&to=170000360{index}", index, "UTC")

    def test_explicit_and_values_by_matrix_create_hashed_tasks(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                  row_grouping: [environment]
                  environment:
                    alias: Environment
                    hide: false
                    grafana_variable: env
                    values: [prod, stage]
                  service:
                    alias: Service
                    depends_on: environment
                    values_by:
                      prod: [api]
                      stage: [worker]
            """
        )

        panels = grafana.get_panels([self.timestamp()])
        tasks = grafana.render_tasks
        self.assertEqual([task.variables["env"] for task in tasks], ["prod", "stage"])
        self.assertEqual([task.variables["service"] for task in tasks], ["api", "worker"])
        self.assertTrue(all(task.variables["region"] == "us" for task in tasks))
        self.assertTrue(all("prod" not in task.file_name and "stage" not in task.file_name for task in tasks))
        self.assertTrue(all("matrix-" in task.file_name for task in tasks))
        self.assertEqual(panels[0].artifacts[0]["matrix"]["variables"], {"Environment": "prod", "Service": "api"})
        self.assertEqual(panels[0].artifacts[0]["matrix"]["grafana_variables"], {"env": "prod", "service": "api"})
        self.assertFalse(panels[0].artifacts[0]["matrix"]["context_path"][1]["hidden"])
        self.assertEqual(panels[0].artifacts[0]["matrix"]["group"], "Environment: prod")
        self.assertEqual(panels[0].artifacts[0]["display_title"], "Requests (Environment: prod, Service: api)")

    def test_values_from_uses_api_values_with_regex_and_max(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                vars: {region: us, env: prod}
                render_matrix:
                  service:
                    values_from:
                      regex: "^(api|worker|db)$"
                      max_values: 2
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard = self.dashboard_with_prometheus_variable()
        grafana.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})),
            Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [
                {"service": "api"}, {"service": "worker"}, {"service": "db"}, {"service": "cache"},
            ]})),
        ])

        grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])

    def test_values_from_queries_prometheus_with_time_and_parent_context(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                  environment:
                    grafana_variable: env
                    values: [prod]
                  service:
                    depends_on: environment
                    values_from: {}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard_with_prometheus_variable()}))
        values_response = Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"service": "api"}, {"service": "worker"}]}))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])
        api_call = grafana.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom/api/v1/series", api_call.args[0])
        self.assertEqual(api_call.kwargs["params"]["start"], "1700000000")
        self.assertEqual(api_call.kwargs["params"]["end"], "1700003600")
        self.assertEqual(api_call.kwargs["params"]["match[]"], 'up{region="us", env="prod"}')
        discovery = grafana.config.render_matrix_rows_by_timestamp[0][0]["discovery"]["service"]
        self.assertEqual(discovery["source"], "grafana_api")
        self.assertEqual(discovery["method"], "prometheus_series")

    def test_dependent_variable_without_source_uses_implicit_values_from(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                  environment:
                    grafana_variable: env
                    values: [prod]
                  application:
                    grafana_variable: service
                    depends_on: environment
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard_with_prometheus_variable()}))
        values_response = Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"service": "api"}, {"service": "worker"}]}))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        grafana.get_panels([self.timestamp()])

        service_spec = grafana.config.render_matrix["variables"]["application"]
        self.assertEqual(service_spec["values_from"], {})
        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])
        api_call = grafana.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom/api/v1/series", api_call.args[0])

    def test_implicit_values_from_uses_modern_prometheus_query_with_dashboard_context(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us, env: prod}
                render_matrix:
                  namespace:
                    values: [team-a, team-b]
                  pod:
                    depends_on: namespace
            """,
            self.dashboard_with_modern_prometheus_pod_variable(),
        )
        dashboard_response = Mock(
            status_code=200,
            json=Mock(return_value={"dashboard": self.dashboard_with_modern_prometheus_pod_variable()}),
        )

        def response_for(url, **kwargs):
            if "/api/datasources/proxy/uid/prom-main/api/v1/series" in url:
                namespace = "team-a" if 'namespace="team-a"' in kwargs["params"]["match[]"] else "team-b"
                return Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"pod": f"{namespace}-pod"}]}))
            return dashboard_response

        grafana.session.get = Mock(side_effect=response_for)

        grafana.get_panels([self.timestamp()])

        self.assertEqual(
            [(task.variables["namespace"], task.variables["pod"]) for task in grafana.render_tasks],
            [("team-a", "team-a-pod"), ("team-b", "team-b-pod")],
        )
        first_values_call = grafana.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom-main/api/v1/series", first_values_call.args[0])
        self.assertEqual(
            first_values_call.kwargs["params"]["match[]"],
            'kube_pod_info{cluster="prod", namespace="team-a", job="kube-state-metrics"}',
        )

    def test_automatic_dependencies_resolve_in_topological_order_for_each_timestamp(self) -> None:
        dashboard = self.dashboard()
        dashboard["templating"] = {"list": [
            {
                "name": "namespace", "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(kube_namespace_labels{cluster="$cluster"}, namespace)',
            },
            {
                "name": "pod", "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(kube_pod_info{namespace="${namespace:regex}"}, pod)',
            },
        ]}
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                render_matrix:
                  pod: {}
                  namespace: {}
                  cluster: {values: [prod]}
            """,
            dashboard,
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))

        def response_for(url, **kwargs):
            if "/api/v1/series" not in url:
                return dashboard_response
            selector = kwargs["params"]["match[]"]
            if selector.startswith("kube_namespace_labels"):
                period = kwargs["params"]["start"]
                namespace = "period-a" if period == "1700000000" else "period-b"
                return Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"namespace": namespace}]}))
            namespace = "period-a" if 'namespace="period-a"' in selector else "period-b"
            return Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"pod": f"{namespace}-pod"}]}))

        grafana.session.get = Mock(side_effect=response_for)

        grafana.get_panels([self.timestamp(0), self.timestamp(1)])

        self.assertEqual(
            [(task.timestamp.id_time, task.variables["namespace"], task.variables["pod"]) for task in grafana.render_tasks],
            [(0, "period-a", "period-a-pod"), (1, "period-b", "period-b-pod")],
        )
        discovery_calls = [call for call in grafana.session.get.call_args_list if "/api/v1/series" in call.args[0]]
        self.assertEqual(len(discovery_calls), 4)
        self.assertEqual(
            [call.kwargs["params"]["start"] for call in discovery_calls],
            ["1700000000", "1700000000", "1700000001", "1700000001"],
        )

    def test_matrix_dashboard_links_include_static_and_matrix_vars(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                    service: {values: [api]}
            """
        )
        timestamp = self.timestamp()
        grafana.get_panels([timestamp])
        grafana.config.full_links = grafana._GrafanaManager__get_full_links([timestamp])

        links = grafana._GrafanaManager__get_matrix_full_links([timestamp])

        self.assertIn("var-region=us", links[0]["url"])
        self.assertIn("var-service=api", links[0]["url"])
        self.assertIn("/grafana/d/uid/demo", links[0]["url"])

    def test_panel_variants_merge_after_matrix_and_override_variables(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    service: {values: [api], alias: Matrix Service}
                panel_variants:
                  - name: by_service
                    selectors: {panel_id: 17}
                    variables:
                      service: {values: [worker]}
                    label_template: "Service: {service}"
            """
        )

        panels = grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])
        self.assertEqual(panels[0].artifacts[1]["variant"]["variables"], {"service": "worker"})
        self.assertEqual(panels[0].artifacts[1]["matrix"]["grafana_variables"]["service"], "worker")
        self.assertEqual(panels[0].artifacts[1]["matrix"]["variables"], {"Matrix Service": "worker"})
        self.assertEqual(panels[0].artifacts[1]["matrix"]["context_path"][0]["value"], "worker")
        self.assertEqual(panels[0].artifacts[1]["matrix"]["label"], "Matrix Service: worker")
        self.assertEqual(panels[0].artifacts[1]["display_title"], "Requests (Matrix Service: worker)")
        self.assertIn("Service: worker", panels[0].artifacts[1]["variant"]["label"])

    def test_values_from_empty_branch_skips_only_affected_context(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                  environment:
                    grafana_variable: env
                    values: [prod, stage]
                  service:
                    depends_on: environment
                    values_from: {}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard = self.dashboard_with_prometheus_variable()
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))

        def response_for(url, **kwargs):
            if "/api/datasources/proxy/uid/prom/api/v1/series" in url:
                value = "api" if 'env="prod"' in kwargs["params"]["match[]"] else None
                return Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [] if value is None else [{"service": value}]}))
            return dashboard_response

        grafana.session.get = Mock(side_effect=response_for)

        with self.assertLogs("grafconflux._grafana.matrix", level="WARNING") as captured:
            grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["env"] for task in grafana.render_tasks], ["prod"])
        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api"])
        self.assertIn("Render matrix branch skipped", "\n".join(captured.output))

    def test_values_from_all_empty_branches_fail(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                vars: {region: us}
                render_matrix:
                  environment:
                    grafana_variable: env
                    values: [prod]
                  service:
                    depends_on: environment
                    values_from: {}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard = self.dashboard_with_prometheus_variable()
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))
        empty_values_response = Mock(status_code=200, json=Mock(return_value={"status": "success", "data": []}))
        grafana.session.get = Mock(side_effect=[dashboard_response, empty_values_response])

        with self.assertRaisesRegex(ConfigurationError, "render_matrix.variables: no rows resolved"):
            grafana.get_panels([self.timestamp()])

    def test_invalid_variant_label_template_placeholder_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "label_template.*unknown placeholders"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    panel_variants:
                      - selectors: {panel_id: 17}
                        variables:
                          service: {values: [api]}
                        label_template: "Service: {missing}"
            """))

    def test_matrix_label_template_supports_variable_keys_and_aliases(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  label_template: "{environment} / {Service}"
                  environment:
                    alias: Service
                    values: [prod]
            """
        )

        grafana.get_panels([self.timestamp()])

        self.assertEqual(grafana.render_tasks[0].artifact["matrix"]["label"], "prod / prod")

    def test_values_from_invalid_json_and_failed_browser_does_not_use_saved_options(self) -> None:
        dashboard = self.dashboard()
        dashboard["templating"] = {"list": [{
            "name": "service", "type": "query", "datasource": {"type": "prometheus", "uid": "prom"},
            "query": "label_values(up, service)", "options": [{"value": "api"}, {"value": "worker"}],
        }]}
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                render_matrix:
                  service:
                    values_from: {}
            """,
            dashboard,
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))
        broken_values_response = Mock(status_code=200, json=Mock(side_effect=ValueError("bad json")))
        grafana.session.get = Mock(side_effect=[dashboard_response, broken_values_response])

        fallback_result = MatrixValueResult(
            MatrixDiscoveryStatus.FAILED,
            [],
            {
                "status": "failed", "source": "browser", "method": "test_failure", "timestamp_id": 0,
                "from": "1700000000000", "to": "1700003600000", "context_vars": [],
            },
        )
        with patch(
            "grafconflux._grafana.matrix_browser_planning.BrowserMatrixFallback.discover",
            return_value=fallback_result,
        ), self.assertLogs("grafconflux._grafana.matrix_discovery", level="WARNING") as captured:
            with self.assertRaisesRegex(ConfigurationError, "dynamic discovery did not resolve"):
                grafana.get_panels([self.timestamp()])

        self.assertIn("invalid JSON", "\n".join(captured.output))

    def test_absent_render_matrix_keeps_flat_task_filename(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
            """
        )

        grafana.get_panels([self.timestamp()])

        self.assertEqual(len(grafana.render_tasks), 1)
        self.assertEqual(grafana.render_tasks[0].file_name, "Demo__17__0.png")
        self.assertIsNone(grafana.config.render_matrix)

    def test_matrix_values_first_layout_is_accepted(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options:
                    layout: matrix_values_first
                  variables:
                    service: {values: [api]}
            """
        )

        grafana.get_panels([self.timestamp()])

        self.assertEqual(grafana.config.render_matrix["layout"], "matrix_values_first")
        self.assertEqual(grafana.render_tasks[0].artifact["matrix"]["label"], "service: api")

    def test_matrix_options_layout_and_variables_take_precedence_over_flat_variables(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  service: {values: [legacy]}
                  options:
                    layout: matrix_values_first
                  variables:
                    service: {values: [api]}
            """
        )

        grafana.get_panels([self.timestamp()])

        self.assertEqual(grafana.config.render_matrix["layout"], "matrix_values_first")
        self.assertEqual(grafana.config.render_matrix["variables"]["service"]["values"], ["api"])
        self.assertEqual(grafana.render_tasks[0].artifact["matrix"]["label"], "service: api")

    def test_matrix_options_layout_accepts_legacy_flat_variables(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options:
                    layout: matrix_values_first
                  service: {values: [api]}
            """
        )

        grafana.get_panels([self.timestamp()])

        self.assertEqual(grafana.config.render_matrix["layout"], "matrix_values_first")
        self.assertEqual(grafana.render_tasks[0].artifact["matrix"]["label"], "service: api")

    def test_invalid_matrix_layout_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "render_matrix.layout"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      options:
                        layout: invalid
                      service: {values: [api]}
            """))

    def test_flat_matrix_layout_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "render_matrix.layout.*options.layout"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      layout: matrix_values_first
                      service: {values: [api]}
            """))

    def test_invalid_matrix_validation_fails_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, "render_matrix.variables"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix: {}
            """))

    def test_matrix_presentation_fields_keep_raw_request_identity(self) -> None:
        grafana = self.manager_from_config("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    environment:
                      grafana_variable: env
                      display_name: Environment
                      hide: false
                      value_aliases: {prod: Production}
                      values: [prod, stage]
                    service:
                      display_name: Service
                      hide: true
                      value_aliases: {api: Public API}
                      values: [api]
        """)

        panels = grafana.get_panels([self.timestamp()])
        grafana.config.full_links = grafana._GrafanaManager__get_full_links([self.timestamp()])
        grafana.config.matrix_dashboard_links = grafana._GrafanaManager__get_matrix_full_links([self.timestamp()])

        first_task = grafana.render_tasks[0]
        matrix = panels[0].artifacts[0]["matrix"]
        self.assertEqual(first_task.variables, {"env": "prod", "service": "api"})
        self.assertEqual(matrix["raw_variables"], {"environment": "prod", "service": "api"})
        self.assertEqual(matrix["variables"], {"Environment": "Production"})
        self.assertEqual(matrix["label"], "Environment: Production")
        self.assertEqual(matrix["context_path"][0]["raw_value"], "prod")
        self.assertEqual(matrix["context_path"][0]["display_value"], "Production")
        self.assertFalse(matrix["context_path"][0]["hidden"])
        self.assertTrue(matrix["context_path"][1]["hidden"])
        self.assertIn("var-env=prod", grafana.config.matrix_dashboard_links[0]["url"])
        self.assertNotIn("Production", grafana.config.matrix_dashboard_links[0]["url"])

    def test_matrix_default_hide_uses_effective_value_count_and_alias_presence(self) -> None:
        grafana = self.manager_from_config("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    single: {values: [one]}
                    multiple: {values: [first, second]}
                    aliased:
                      values: [raw]
                      value_aliases: {raw: Display}
        """)

        panels = grafana.get_panels([self.timestamp()])

        first_matrix = panels[0].artifacts[0]["matrix"]
        self.assertEqual(first_matrix["label"], "single: one")
        self.assertEqual(
            [item["hidden"] for item in first_matrix["context_path"]],
            [False, True, True],
        )
        self.assertEqual(first_matrix["variables"], {"single": "one"})

    def test_all_hidden_matrix_context_uses_neutral_variant_labels(self) -> None:
        grafana = self.manager_from_config("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options:
                    layout: matrix_values_first
                  variables:
                    environment: {values: [prod, stage]}
        """)

        panels = grafana.get_panels([self.timestamp()])

        artifacts = panels[0].artifacts
        self.assertEqual([artifact["matrix"]["label"] for artifact in artifacts], ["Variant 1", "Variant 2"])
        self.assertEqual([artifact["display_title"] for artifact in artifacts], [
            "Requests (Variant 1)", "Requests (Variant 2)",
        ])
        grafana.config.full_links = grafana._GrafanaManager__get_full_links([self.timestamp()])
        grafana.config.matrix_dashboard_links = grafana._GrafanaManager__get_matrix_full_links([self.timestamp()])
        grafana.config.panels = panels
        content = build_confluence_storage_content([grafana.config], [self.timestamp()], 600)
        self.assertIn('ac:parameter ac:name="title">Panels</ac:parameter>', content)
        self.assertIn('ac:parameter ac:name="title">Requests</ac:parameter>', content)
        self.assertIn(">Variant 1</a>", content)
        self.assertIn(">Variant 2</a>", content)
        self.assertNotIn("Environment: prod", content)
        self.assertNotIn("Environment: stage", content)
        self.assertNotIn("environment:", content)

    def test_matrix_presentation_does_not_change_hash_or_filename(self) -> None:
        base = """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    environment:
                      values: [prod]
                      %s
        """
        plain = self.manager_from_config(base % "hide: false")
        presented = self.manager_from_config(
            base % "display_name: Environment\n                      value_aliases: {prod: Production}\n                      hide: false"
        )

        plain.get_panels([self.timestamp()])
        presented.get_panels([self.timestamp()])

        self.assertEqual(plain.render_tasks[0].file_name, presented.render_tasks[0].file_name)
        self.assertEqual(
            plain.render_tasks[0].artifact["matrix"]["hash"],
            presented.render_tasks[0].artifact["matrix"]["hash"],
        )

    def test_matrix_alias_and_display_name_must_not_conflict(self) -> None:
        with self.assertRaisesRegex(ValueError, "alias.*display_name.*different"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      variables:
                        environment:
                          alias: Environment
                          display_name: Deployment
                          values: [prod]
            """))

    def test_matrix_display_name_cannot_collide_with_another_raw_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "variables.environment.alias.*collides with raw matrix variable 'service'"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      variables:
                        environment: {display_name: service, values: [prod]}
                        service: {values: [api]}
            """))

    def test_hidden_matrix_alias_cannot_collide_with_another_raw_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "variables.environment.alias.*collides with raw matrix variable 'service'"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      variables:
                        environment: {alias: service, hide: true, values: [prod]}
                        service: {values: [api]}
            """))

    def test_matrix_rejects_template_reference_to_hidden_variable(self) -> None:
        with self.assertRaisesRegex(ValueError, "label_template.*hidden"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      options:
                        label_template: "Environment: {environment}"
                      variables:
                        environment: {values: [prod], hide: true}
            """))

    def test_matrix_presentation_validation_is_strict(self) -> None:
        invalid_fields = (
            ("hide: hidden", "variables.environment.hide"),
            ("display_name: ''", "variables.environment.display_name"),
            ("value_aliases: []", "variables.environment.value_aliases"),
            ("value_aliases: {prod: 1}", "variables.environment.value_aliases"),
        )
        for field, message in invalid_fields:
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, message):
                    GrafanaManager.load_grafana_config(self.config_path(f"""
                        dashboards:
                          Demo:
                            grafana_url: https://grafana.example
                            dash_title: Demo
                            render_matrix:
                              variables:
                                environment:
                                  values: [prod]
                                  {field}
                    """))

    def test_dynamic_single_value_is_visible_when_hide_is_omitted(self) -> None:
        grafana = self.manager_from_config("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us, env: prod}
                render_matrix:
                  service: {values_from: {}}
        """, self.dashboard_with_prometheus_variable())
        dashboard_response = Mock(
            status_code=200,
            json=Mock(return_value={"dashboard": self.dashboard_with_prometheus_variable()}),
        )
        values_response = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": [{"service": "api"}]}),
        )
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        panels = grafana.get_panels([self.timestamp()])

        matrix = panels[0].artifacts[0]["matrix"]
        self.assertEqual(matrix["label"], "service: api")
        self.assertFalse(matrix["context_path"][0]["hidden"])

    def test_dynamic_default_hide_is_resolved_after_discovery(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars: {region: us, env: prod}
                render_matrix:
                  service: {values_from: {}}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard_response = Mock(
            status_code=200,
            json=Mock(return_value={"dashboard": self.dashboard_with_prometheus_variable()}),
        )
        values_response = Mock(
            status_code=200,
            json=Mock(return_value={"status": "success", "data": [{"service": "api"}, {"service": "worker"}]}),
        )
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        panels = grafana.get_panels([self.timestamp()])

        self.assertEqual(
            [artifact["matrix"]["label"] for artifact in panels[0].artifacts],
            ["Variant 1", "Variant 2"],
        )

    def test_values_by_requires_previous_dependency(self) -> None:
        with self.assertRaisesRegex(ValueError, "depends_on.*required"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      service:
                        values_by:
                          prod: [api]
            """))

    def test_later_explicit_dependency_is_accepted_for_topological_planning(self) -> None:
        configs = GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      service:
                        depends_on: pod
                        values_from: {}
                      pod:
                        values: [pod-a]
            """))

        self.assertEqual(configs[0].render_matrix["variables"]["service"]["depends_on"], "pod")

    def test_values_from_variable_override_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "variables.service.values_from"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      service:
                        values_from: {variable: other_service}
            """))

    def test_values_from_string_override_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "variables.service.values_from"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      service:
                        values_from: other_service
            """))

    def test_datasource_static_var_object_resolves_query_datasource(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example/grafana
                dash_title: Demo
                vars:
                  ds: {is_datasource: true, value: Prometheus}
                render_matrix:
                  service:
                    values_from: {max_values: 50}
            """,
            self.dashboard_with_datasource_variable(),
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard_with_datasource_variable()}))
        values_response = Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [{"service": "api"}]}))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        grafana.get_panels([self.timestamp()])
        grafana.config.full_links = grafana._GrafanaManager__get_full_links([self.timestamp()])

        api_call = grafana.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/Prometheus/api/v1/series", api_call.args[0])
        self.assertEqual(grafana.render_tasks[0].variables["ds"], "Prometheus")
        self.assertIn("var-ds=Prometheus", grafana.config.full_links[0])

    def test_expansion_limit_is_enforced(self) -> None:
        grafana = self.manager_from_config("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  max_rows: 1
                  service: {values: [api, worker]}
        """)

        with self.assertRaisesRegex(ConfigurationError, "expansion produced 2 rows"):
            grafana.get_panels([self.timestamp()])

    def manager_from_config(self, content: str, dashboard: dict | None = None) -> GrafanaManager:
        config = GrafanaManager.load_grafana_config(self.config_path(content))[0]
        grafana = GrafanaManager(config)
        grafana.dashboard_uid = "uid"
        grafana.dashboard_url = "/d/uid/demo"
        grafana.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard or self.dashboard()})))
        return grafana

    def config_path(self, content: str) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent(content))
        return config_path

    @staticmethod
    def dashboard() -> dict:
        return {"panels": [{"id": 17, "type": "timeseries", "title": "Requests"}]}

    @staticmethod
    def dashboard_with_variables() -> dict:
        dashboard = TestRenderMatrixPlanning.dashboard()
        dashboard["templating"] = {"list": [{"name": "service", "options": [
            {"value": "api"}, {"value": "worker"}, {"value": "db"}, {"value": "cache"},
        ]}]}
        return dashboard

    @staticmethod
    def dashboard_with_prometheus_variable() -> dict:
        dashboard = TestRenderMatrixPlanning.dashboard()
        dashboard["templating"] = {"list": [{
            "name": "service", "type": "query", "datasource": {"type": "prometheus", "uid": "prom"},
            "query": 'label_values(up{region="$region", env="$env"}, service)', "options": [{"value": "api"}, {"value": "worker"}],
        }]}
        return dashboard

    @staticmethod
    def dashboard_with_datasource_variable() -> dict:
        dashboard = TestRenderMatrixPlanning.dashboard()
        dashboard["templating"] = {"list": [
            {"name": "ds", "type": "datasource", "query": "prometheus"},
            {
                "name": "service", "type": "query", "datasource": {"type": "prometheus", "uid": "$ds"},
                "query": "label_values(up, service)", "options": [],
            },
        ]}
        return dashboard

    @staticmethod
    def dashboard_with_modern_prometheus_pod_variable() -> dict:
        dashboard = TestRenderMatrixPlanning.dashboard()
        dashboard["templating"] = {"list": [
            {
                "name": "datasource", "type": "datasource", "query": "prometheus",
                "current": {"text": "Prometheus", "value": "prom-main"},
            },
            {"name": "cluster", "type": "custom", "current": {"text": "prod", "value": "prod"}},
            {"name": "job", "type": "custom", "current": {"text": "kube-state-metrics", "value": "kube-state-metrics"}},
            {
                "name": "pod", "type": "query", "datasource": {"type": "prometheus", "uid": "$datasource"},
                "query": {
                    "queryType": "label_values",
                    "query": 'kube_pod_info{cluster="$cluster", namespace="$namespace", job="$job"}',
                    "label": "pod",
                },
                "options": [],
            },
        ]}
        return dashboard


class TestRenderMatrixReplayAndConfluence(unittest.TestCase):
    def test_upload_merge_shifts_matrix_dashboard_link_timestamps(self) -> None:
        links = [{"timestamp_id": 0, "label": "Service: api", "context_path": [{"key": "service", "value": "api"}]}]

        shifted = _shift_matrix_dashboard_links(links, 2)

        self.assertEqual(shifted[0]["timestamp_id"], 2)
        self.assertEqual(shifted[0]["context_path"], links[0]["context_path"])
        self.assertIsNot(shifted[0], links[0])

    def test_upload_merge_preserves_multi_folder_matrix_dashboard_links(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder, second_folder = os.path.join(temp_dir, "first"), os.path.join(temp_dir, "second")
            os.makedirs(os.path.join(first_folder, "Demo")); os.makedirs(os.path.join(second_folder, "Demo"))
            configs = [self.upload_config(first_folder, "prod"), self.upload_config(second_folder, "stage")]
            args = SimpleNamespace(test_root_folder=temp_dir, test_id="merged", test_upload_folders=[first_folder, second_folder],
                                   config_file="config.yaml")

            merged_configs, _ = transform_grafana_configs(configs, args)

        links = merged_configs[0].matrix_dashboard_links
        self.assertEqual([link["label"] for link in links], ["Environment: prod", "Environment: stage"])
        self.assertEqual([link["timestamp_id"] for link in links], [0, 1])
        self.assertEqual([timestamp.id_time for timestamp in merged_configs[0].timestamps], [0, 1])

    def test_upload_merge_accepts_identical_static_presentation_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder, second_folder = os.path.join(temp_dir, "first"), os.path.join(temp_dir, "second")
            os.makedirs(os.path.join(first_folder, "Demo")); os.makedirs(os.path.join(second_folder, "Demo"))
            presentation = {"region": {"display_name": "Region", "hide": False}}
            configs = [
                self.upload_config(first_folder, "prod", presentation),
                self.upload_config(second_folder, "stage", presentation),
            ]
            args = SimpleNamespace(test_root_folder=temp_dir, test_id="merged", test_upload_folders=[first_folder, second_folder])

            merged_configs, _ = transform_grafana_configs(configs, args)

        self.assertEqual(merged_configs[0].vars_presentation, presentation)

    def test_upload_merge_rejects_divergent_static_presentation_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder, second_folder = os.path.join(temp_dir, "first"), os.path.join(temp_dir, "second")
            os.makedirs(os.path.join(first_folder, "Demo")); os.makedirs(os.path.join(second_folder, "Demo"))
            configs = [
                self.upload_config(first_folder, "prod", {"region": {"display_name": "Region"}}),
                self.upload_config(second_folder, "stage", {"region": {"display_name": "Location"}}),
            ]
            args = SimpleNamespace(test_root_folder=temp_dir, test_id="merged", test_upload_folders=[first_folder, second_folder])

            with self.assertRaisesRegex(ConfigurationError, "vars_presentation metadata differs across folders"):
                transform_grafana_configs(configs, args)

    def test_upload_merge_accepts_legacy_metadata_without_presentation_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder, second_folder = os.path.join(temp_dir, "first"), os.path.join(temp_dir, "second")
            os.makedirs(os.path.join(first_folder, "Demo")); os.makedirs(os.path.join(second_folder, "Demo"))
            presentation = {"region": {"display_name": "Region", "hide": False}}
            configs = [self.upload_config(first_folder, "prod"), self.upload_config(second_folder, "stage", presentation)]
            args = SimpleNamespace(test_root_folder=temp_dir, test_id="merged", test_upload_folders=[first_folder, second_folder])

            merged_configs, _ = transform_grafana_configs(configs, args)

        self.assertEqual(merged_configs[0].vars_presentation, presentation)

    def test_upload_merge_legacy_metadata_remains_compatible_with_later_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder, second_folder = os.path.join(temp_dir, "first"), os.path.join(temp_dir, "second")
            snapshot_folder = os.path.join(temp_dir, "snapshot")
            for folder in (first_folder, second_folder, snapshot_folder):
                os.makedirs(os.path.join(folder, "Demo"))
            legacy_args = SimpleNamespace(
                test_root_folder=temp_dir, test_id="legacy", test_upload_folders=[first_folder, second_folder]
            )
            _, legacy_output = transform_grafana_configs(
                [self.upload_config(first_folder, "prod"), self.upload_config(second_folder, "stage")], legacy_args
            )
            with open(os.path.join(legacy_output, "Demo.yaml"), encoding="utf-8") as metadata_file:
                legacy_metadata = yaml.safe_load(metadata_file)
            self.assertNotIn("vars_presentation", legacy_metadata)

            snapshot = {"region": {"display_name": "Region", "hide": False}}
            final_args = SimpleNamespace(
                test_root_folder=temp_dir, test_id="final", test_upload_folders=[legacy_output, snapshot_folder]
            )
            final_configs, _ = transform_grafana_configs(
                [GrafanaConfigUploader("Demo", legacy_metadata), self.upload_config(snapshot_folder, "dev", snapshot)],
                final_args,
            )

        self.assertEqual(final_configs[0].vars_presentation, snapshot)

    def test_uploader_replays_matrix_metadata_and_nested_confluence_groups(self) -> None:
        config = {
            "name": "Demo",
            "charts_path": "unused",
            "full_links": ["https://grafana.example/d/demo?from=1&to=2"],
            "render_matrix": {"layout": "dashboard_first"},
            "matrix_dashboard_links": [{"timestamp_id": 0, "label": "Environment: prod, Service: api", "url": "https://grafana.example/d/demo?var-env=prod", "context_path": [{"key": "environment", "label": "Environment", "value": "prod", "grafana_variable": "env"}]}],
            "timestamps": [{
                "time_tag": "smoke", "id_time": 0, "start_time_timestamp": 1, "end_time_timestamp": 2,
                "start_time_human": "start", "end_time_human": "end",
            }],
            "panels": [{
                "panel_id": 17, "type": "timeseries", "title": "Requests", "links": [],
                "artifacts": [{
                    "artifact_type": "matrix", "timestamp_tag": "smoke", "render_status": "rendered",
                    "png_file": "Demo__17__matrix-000-deadbeef__0.png",
                    "link": "https://grafana.example/panel?var-env=prod",
                    "matrix": {
                        "label": "Environment: prod, Service: api",
                        "group": "Environment: prod",
                        "variables": {"Environment": "prod", "Service": "api"},
                        "context_path": [{"key": "environment", "label": "Environment", "value": "prod", "grafana_variable": "env"}, {"key": "service", "label": "Service", "value": "api", "grafana_variable": "service"}],
                    },
                }],
            }],
        }

        uploader = GrafanaConfigUploader("Demo", config)
        content = build_confluence_storage_content([uploader], uploader.timestamps, 900)

        self.assertEqual(uploader.matrix_dashboard_links[0]["label"], "Environment: prod, Service: api")
        self.assertNotIn("<p>Dashboard links</p>", content)
        self.assertIn("https://grafana.example/d/demo?var-env=prod", content)
        self.assertIn("Demo (Environment: prod)", content)
        self.assertIn("Requests (Environment: prod, Service: api)", content)
        self.assertIn("Demo__17__matrix-000-deadbeef__0.png", content)
        self.assertIn("ac:structured-macro ac:name=\"expand\"", content)

    def test_uploader_replays_hidden_and_aliased_presentation_snapshot(self) -> None:
        config = {
            "charts_path": "unused",
            "full_links": [],
            "matrix_dashboard_links": [{
                "label": "Region: United States",
                "url": "https://grafana.example/d/demo?var-region=us&var-service=api",
                "context_path": [
                    {"key": "region", "label": "Region", "value": "us", "raw_value": "us",
                     "display_value": "United States", "hidden": False},
                    {"key": "service", "label": "Service", "value": "api", "raw_value": "api",
                     "display_value": "Public API", "hidden": True},
                ],
            }],
            "timestamps": [{"time_tag": "smoke", "id_time": 0, "start_time_timestamp": 1,
                            "end_time_timestamp": 2, "start_time_human": "start", "end_time_human": "end"}],
            "panels": [{
                "panel_id": 17, "type": "timeseries", "title": "Requests", "links": [],
                "artifacts": [{
                    "artifact_type": "matrix", "render_status": "rendered", "png_file": "matrix.png",
                    "matrix": {
                        "label": "Region: United States",
                        "context_path": [
                            {"key": "region", "label": "Region", "value": "us", "raw_value": "us",
                             "display_value": "United States", "hidden": False},
                            {"key": "service", "label": "Service", "value": "api", "raw_value": "api",
                             "display_value": "Public API", "hidden": True},
                        ],
                    },
                }],
            }],
            "render_matrix": {"layout": "dashboard_first"},
        }

        uploader = GrafanaConfigUploader("Demo", config)
        content = build_confluence_storage_content([uploader], uploader.timestamps, 900)

        self.assertIn("Demo (Region: United States)", content)
        self.assertNotIn("Service: Public API", content)
        self.assertIn("var-region=us&amp;var-service=api", content)

    @staticmethod
    def upload_config(folder: str, environment: str, vars_presentation: dict | None = None) -> GrafanaConfigUploader:
        config = {
            "charts_path": os.path.join(folder, "Demo"),
            "full_links": [f"https://grafana.example/d?var-env={environment}"],
            "matrix_dashboard_links": [{"timestamp_id": 0, "label": f"Environment: {environment}",
                                        "url": f"https://grafana.example/d?var-env={environment}",
                                        "context_path": [{"key": "environment", "value": environment}]}],
            "timestamps": [{"time_tag": environment, "id_time": 0, "start_time_timestamp": 1,
                            "end_time_timestamp": 2, "start_time_human": "start", "end_time_human": "end"}],
            "panels": [],
        }
        if vars_presentation is not None:
            config["vars_presentation"] = vars_presentation
        return GrafanaConfigUploader("Demo", config)

    def test_confluence_default_render_matrix_layout_is_panel_first(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "display_title": "Requests (Service: api)",
            "matrix": {"label": "Service: api", "context_path": [{"key": "service", "label": "Service", "value": "api"}]},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[],
                                 render_matrix={"variables": {"service": {"values": ["api"]}}})

        content = build_confluence_storage_content([config], timestamps, 600)

        expected_order = [
            '<h2>Demo</h2>',
            'ac:parameter ac:name="title">Demo</ac:parameter>',
            'ac:parameter ac:name="title">Requests</ac:parameter>',
            'https://grafana.example/panel/17',
            'Demo__17__matrix-000-hash__0.png',
        ]
        indexes = [content.index(fragment) for fragment in expected_order]
        self.assertEqual(indexes, sorted(indexes))
        self.assertIn("Requests (Service: api)", content)
        self.assertNotIn('ac:parameter ac:name="title">Panels</ac:parameter>', content)
        self.assertNotIn("Demo (Service: api)", content)

    def test_confluence_explicit_dashboard_first_preserves_context_hierarchy(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "variables": {"Service": "api"}},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[],
                                 render_matrix={"layout": "dashboard_first"})

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertIn("Demo (Service: api)", content)
        self.assertLess(content.index("Demo (Service: api)"), content.index("Requests (Service: api)"))

    def test_confluence_matrix_values_first_uses_exact_context_hierarchy(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "context_path": [{"key": "service", "label": "Service", "value": "api"}]},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[{"label": "Service: api", "url": "https://grafana.example/d?var-service=api", "context_path": [{"key": "service", "value": "api"}]}],
            render_matrix={"layout": "matrix_values_first"}, confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        expected_order = [
            'ac:parameter ac:name="title">Test times</ac:parameter>',
            '<h2>Demo</h2>',
            'ac:parameter ac:name="title">Demo</ac:parameter>',
            'ac:parameter ac:name="title">Panels</ac:parameter>',
            'ac:parameter ac:name="title">Requests</ac:parameter>',
            'https://grafana.example/d?var-service=api',
            '>Service: api</a>',
            'Demo__17__matrix-000-hash__0.png',
        ]
        indexes = [content.index(fragment) for fragment in expected_order]
        self.assertEqual(indexes, sorted(indexes))
        self.assertNotIn('<h4>Requests</h4>', content)
        self.assertNotIn('<h5>Requests', content)
        self.assertNotIn('<p>Panels</p>', content)
        self.assertNotIn('<p>Dashboard links</p>', content)
        self.assertNotIn('ac:parameter ac:name="title">Service: api</ac:parameter>', content)
        self.assertNotIn("<h3>Service: api</h3>", content)
        self.assertIn("https://grafana.example/panel/17", content)
        self.assertIn("https://grafana.example/d?var-service=api", content)

    def test_confluence_matrix_values_first_renders_each_context_level(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        context_path = [
            {"key": "dc", "label": "DC", "value": "prod"},
            {"key": "host", "label": "Host", "value": "app-01"},
        ]
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-host__0.png",
            "matrix": {"label": "DC: prod, Host: app-01", "context_path": context_path},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[{"label": "DC: prod, Host: app-01", "url": "https://grafana.example/d?var-host=app-01", "context_path": context_path}],
            render_matrix={"layout": "matrix_values_first"}, confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        expected_order = [
            '<h3>DC: prod</h3>',
            'ac:parameter ac:name="title">DC: prod</ac:parameter>',
            'ac:parameter ac:name="title">Panels</ac:parameter>',
            'ac:parameter ac:name="title">Requests</ac:parameter>',
            'https://grafana.example/d?var-host=app-01',
            '>Host: app-01</a>',
        ]
        indexes = [content.index(fragment) for fragment in expected_order]
        self.assertEqual(indexes, sorted(indexes))
        self.assertNotIn('<h3>Host: app-01</h3>', content)
        self.assertNotIn('ac:parameter ac:name="title">Host: app-01</ac:parameter>', content)
        self.assertNotIn('<p>Dashboard links</p>', content)

    def test_confluence_matrix_values_first_skips_empty_leaf_dashboard_links_label(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, [])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "context_path": [{"key": "service", "label": "Service", "value": "api"}]},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[], render_matrix={"layout": "matrix_values_first"},
            confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertNotIn("<p>Dashboard links</p>", content)
        self.assertIn("Service: api (Grafana link unavailable)", content)
        self.assertNotIn("<h3>Service: api</h3>", content)

    def test_matrix_dashboard_link_without_context_does_not_match_matrix_section(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, [])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "context_path": [{"key": "service", "label": "Service", "value": "api"}]},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[{"label": "Stale link", "url": "https://grafana.example/d?stale=1"}],
            render_matrix={"layout": "dashboard_first"}, confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertNotIn("https://grafana.example/d?stale=1", content)

    def test_matrix_dashboard_link_without_context_matches_empty_matrix_section(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, [])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Matrix"},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[{"label": "Matrix", "url": "https://grafana.example/d?matrix=1"}],
            render_matrix={"layout": "dashboard_first"}, confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertIn("https://grafana.example/d?matrix=1", content)

    def test_confluence_dashboard_link_location_dashboard_keeps_matrix_dashboard_links(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, [])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "context_path": [{"key": "service", "label": "Service", "value": "api"}]},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo",
            full_links=["https://grafana.example/d?from=1&to=2"],
            backup_dashboard_links=[],
            snapshot_urls=None,
            panels=[panel],
            matrix_dashboard_links=[{"label": "Service: api", "url": "https://grafana.example/d?var-service=api", "context_path": [{"key": "service", "value": "api"}]}],
            confluence_rendering=ConfluenceRenderingSettings(dashboard_links_location="dashboard"),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertIn("https://grafana.example/d?var-service=api", content)
        self.assertNotIn("<p>Dashboard links</p>", content)

    def test_confluence_matrix_values_first_leaf_keeps_links_without_leaf_label_or_expand(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        context_path = [{"key": "service", "label": "Service", "value": "api"}]
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "context_path": context_path},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(
            name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[], snapshot_urls=None,
            panels=[panel], matrix_dashboard_links=[{"label": "Service: api", "url": "https://grafana.example/d?var-service=api", "context_path": context_path}],
            render_matrix={"layout": "matrix_values_first"}, confluence_rendering=ConfluenceRenderingSettings(),
        )

        content = build_confluence_storage_content([config], timestamps, 600)

        leaf_link = '>Service: api</a>'
        self.assertIn(leaf_link, content)
        self.assertNotIn('<h3>Service: api</h3>', content)
        self.assertNotIn('ac:parameter ac:name="title">Service: api</ac:parameter>', content)
        self.assertNotIn('<p>Dashboard links</p>', content)
        self.assertLess(content.index('ac:parameter ac:name="title">Panels</ac:parameter>'), content.index('ac:parameter ac:name="title">Requests</ac:parameter>'))
        self.assertLess(content.index('ac:parameter ac:name="title">Requests</ac:parameter>'), content.index('https://grafana.example/d?var-service=api'))
        self.assertLess(content.index('https://grafana.example/d?var-service=api'), content.index(leaf_link))

    def test_confluence_groups_same_panel_artifacts_under_one_expand_per_row(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"], row_title="Pods")
        panel.artifacts = [
            {
                "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-001.png",
                "matrix": {
                    "label": "Environment: prod, Pod: pod-a",
                    "group": "Pod: pod-a",
                    "context_path": [
                        {"key": "environment", "label": "Environment", "value": "prod", "grafana_variable": "environment"},
                        {"key": "pod", "label": "Pod", "value": "pod-a", "grafana_variable": "pod"},
                    ],
                },
            },
            {
                "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-002.png",
                "matrix": {
                    "label": "Environment: prod, Pod: pod-a, Container: app",
                    "group": "Pod: pod-a",
                    "context_path": [
                        {"key": "environment", "label": "Environment", "value": "prod", "grafana_variable": "environment"},
                        {"key": "pod", "label": "Pod", "value": "pod-a", "grafana_variable": "pod"},
                        {"key": "container", "label": "Container", "value": "app", "grafana_variable": "container"},
                    ],
                },
            },
        ]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[],
                                 render_matrix={"layout": "dashboard_first"})

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertNotIn('<h4>Requests</h4>', content)
        self.assertEqual(content.count('ac:parameter ac:name="title">Requests</ac:parameter>'), 1)
        self.assertIn("Demo__17__matrix-001.png", content)
        self.assertIn("Demo__17__matrix-002.png", content)

    def test_flat_confluence_matrix_grouping_excludes_non_matrix_artifacts(self) -> None:
        artifacts = [
            {
                "artifact_type": "matrix", "render_status": "rendered", "png_file": "matrix.png",
                "matrix": {"label": "Environment: prod", "group": "Environment: prod"},
            },
            {
                "artifact_type": "variant", "render_status": "rendered", "png_file": "variant.png",
                "variant": {"label": "Variant"},
            },
        ]

        grouped = _grouped_matrix_artifacts(artifacts)

        self.assertEqual(list(grouped), ["Environment: prod"])
        self.assertEqual([artifact["png_file"] for artifact in grouped["Environment: prod"]], ["matrix.png"])

    def test_flat_confluence_groups_panels_by_dashboard_rows(self) -> None:
        first = Panel(17, "timeseries", "CPU", 1, ["https://grafana.example/panel/17"], row_title="Compute")
        second = Panel(18, "timeseries", "Memory", 1, ["https://grafana.example/panel/18"], row_title="Compute")
        third = Panel(19, "timeseries", "Latency", 1, ["https://grafana.example/panel/19"], row_title="API")
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[first, second, third], matrix_dashboard_links=[])

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertLess(content.index('title">Compute</ac:parameter>'), content.index('title">API</ac:parameter>'))
        self.assertLess(content.index("CPU"), content.index("Memory"))
        self.assertIn('ac:parameter ac:name="title">Compute</ac:parameter>', content)
        self.assertIn('ac:parameter ac:name="title">API</ac:parameter>', content)

    def test_flat_confluence_omits_row_wrapper_for_single_row(self) -> None:
        panel = Panel(17, "timeseries", "CPU", 1, ["https://grafana.example/panel/17"], row_title="Compute")
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[])

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertNotIn('ac:parameter ac:name="title">Compute</ac:parameter>', content)


if __name__ == "__main__":
    unittest.main()
