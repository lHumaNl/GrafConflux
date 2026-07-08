import os
import tempfile
import textwrap
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from grafconflux._confluence.content import build_confluence_storage_content
from grafconflux._confluence.content import _grouped_matrix_artifacts
from grafconflux._orchestration.upload_merge import _shift_matrix_dashboard_links, transform_grafana_configs
from grafconflux.args_parser import GrafanaTimeDownloader
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
        self.assertEqual(panels[0].artifacts[0]["matrix"]["group"], "Environment: prod")
        self.assertEqual(panels[0].artifacts[0]["display_title"], "Requests (Environment: prod, Service: api)")

    def test_values_from_uses_dashboard_options_with_regex_and_max(self) -> None:
        grafana = self.manager_from_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  service:
                    values_from:
                      variable: service
                      regex: "^(api|worker|db)$"
                      max_values: 2
            """,
            self.dashboard_with_variables(),
        )

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
                    values_from: {variable: service}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard_with_prometheus_variable()}))
        values_response = Mock(status_code=200, json=Mock(return_value={"data": ["api", "worker"]}))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response])

        grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])
        api_call = grafana.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom/api/v1/label/service/values", api_call.args[0])
        self.assertEqual(api_call.kwargs["params"]["start"], "1700000000000")
        self.assertEqual(api_call.kwargs["params"]["end"], "1700003600000")
        self.assertEqual(api_call.kwargs["params"]["match[]"], 'up{region="us", env="prod"}')
        self.assertEqual(api_call.kwargs["params"]["var-region"], "us")
        self.assertEqual(api_call.kwargs["params"]["var-env"], "prod")
        discovery = grafana.config.render_matrix_rows_by_timestamp[0][0]["discovery"]["service"]
        self.assertEqual(discovery["source"], "grafana_api")
        self.assertEqual(discovery["method"], "prometheus_label_values")

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
                    values_from: {variable: service}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard = self.dashboard_with_prometheus_variable()
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))

        def response_for(url, **kwargs):
            if "/api/datasources/proxy/uid/prom/api/v1/label/service/values" in url:
                value = "api" if kwargs["params"].get("var-env") == "prod" else None
                return Mock(status_code=200, json=Mock(return_value={"data": [] if value is None else [value]}))
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
                    values_from: {variable: service}
            """,
            self.dashboard_with_prometheus_variable(),
        )
        dashboard = self.dashboard_with_prometheus_variable()
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))
        empty_values_response = Mock(status_code=200, json=Mock(return_value={"data": []}))
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

    def test_values_from_invalid_json_response_falls_back_to_dashboard_options(self) -> None:
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
                    values_from: {variable: service}
            """,
            dashboard,
        )
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard}))
        broken_values_response = Mock(status_code=200, json=Mock(side_effect=ValueError("bad json")))
        grafana.session.get = Mock(side_effect=[dashboard_response, broken_values_response])

        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="WARNING") as captured:
            grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["api", "worker"])
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

    def test_invalid_matrix_validation_fails_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, "render_matrix.variables"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix: {}
            """))

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

    def test_later_dependency_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown or later dependencies"):
            GrafanaManager.load_grafana_config(self.config_path("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render_matrix:
                      service:
                        depends_on: pod
                        values_from: {source: grafana_variable}
                      pod:
                        values: [pod-a]
            """))

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

    def test_uploader_replays_matrix_metadata_and_nested_confluence_groups(self) -> None:
        config = {
            "name": "Demo",
            "charts_path": "unused",
            "full_links": ["https://grafana.example/d/demo?from=1&to=2"],
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
        self.assertIn("Dashboard links", content)
        self.assertIn("Demo (Environment: prod)", content)
        self.assertIn("Requests (Environment: prod, Service: api)", content)
        self.assertIn("Demo__17__matrix-000-deadbeef__0.png", content)
        self.assertIn("ac:structured-macro ac:name=\"expand\"", content)

    @staticmethod
    def upload_config(folder: str, environment: str) -> GrafanaConfigUploader:
        return GrafanaConfigUploader("Demo", {
            "charts_path": os.path.join(folder, "Demo"),
            "full_links": [f"https://grafana.example/d?var-env={environment}"],
            "matrix_dashboard_links": [{"timestamp_id": 0, "label": f"Environment: {environment}",
                                        "url": f"https://grafana.example/d?var-env={environment}",
                                        "context_path": [{"key": "environment", "value": environment}]}],
            "timestamps": [{"time_tag": environment, "id_time": 0, "start_time_timestamp": 1,
                            "end_time_timestamp": 2, "start_time_human": "start", "end_time_human": "end"}],
            "panels": [],
        })

    def test_confluence_renders_matrix_grouping_from_live_panel(self) -> None:
        panel = Panel(17, "timeseries", "Requests", 1, [])
        panel.artifacts = [{
            "artifact_type": "matrix", "render_status": "rendered", "png_file": "Demo__17__matrix-000-hash__0.png",
            "matrix": {"label": "Service: api", "variables": {"Service": "api"}},
        }]
        timestamps = [SimpleNamespace(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")]
        config = SimpleNamespace(name="Demo", full_links=["https://grafana.example/d"], backup_dashboard_links=[],
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[])

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertIn("Demo (Service: api)", content)
        self.assertIn("Requests (Service: api)", content)

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
                                 snapshot_urls=None, panels=[panel], matrix_dashboard_links=[])

        content = build_confluence_storage_content([config], timestamps, 600)

        self.assertEqual(content.count('<h4>Requests</h4>'), 1)
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
