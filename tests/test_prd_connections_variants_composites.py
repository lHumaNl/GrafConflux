import os
import tempfile
import textwrap
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import yaml
from PIL import Image

from grafconflux._grafana.composites import generate_composites
from grafconflux._grafana.credentials import GrafanaSessionPool
from grafconflux._orchestration.manifest import dashboard_metadata_files, write_run_manifest
from grafconflux._orchestration.runner import process_grafana_dashboard
from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import ConfigurationError, GrafanaManager, Panel


class TestNamedGrafanaCredentials(unittest.TestCase):
    def write_config(self, content: str) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent(content))
        return config_path

    def test_named_credentials_merge_and_preserve_dashboard_order(self) -> None:
        config_path = self.write_config(
            """
            grafana_credentials:
              prod:
                grafana_url: https://grafana.example/grafana
                login: env:GRAFANA_TEST_LOGIN
                password: env:GRAFANA_TEST_PASSWORD
            dashboards:
              First:
                credentials: prod
                dash_title: First
              Second:
                credentials: prod
                dash_title: Second
                session_mode: isolated
            """
        )

        with patch.dict(os.environ, {"GRAFANA_TEST_LOGIN": "user", "GRAFANA_TEST_PASSWORD": "secret"}):
            configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual([config.name for config in configs], ["First", "Second"])
        self.assertEqual(configs[0].grafana_base_url, "https://grafana.example/grafana")
        self.assertEqual(configs[0].login, "user")
        self.assertEqual(configs[0].credential_ref, "prod")
        self.assertEqual(configs[0].session_mode, "shared")
        self.assertIsNone(configs[1].session_key)
        self.assertEqual(configs[1].order_index, 1)

    def test_default_credentials_apply_only_when_dashboard_has_no_inline_connection_fields(self) -> None:
        config_path = self.write_config(
            """
            default_grafana_credentials:
              grafana_url: https://grafana.example/grafana
              login: env:GRAFANA_TEST_LOGIN
              password: env:GRAFANA_TEST_PASSWORD
            dashboards:
              UsesDefault:
                dash_title: Uses Default
              InlineWins:
                dash_title: Inline Wins
                grafana_url: https://other.example/grafana
            """
        )

        with patch.dict(os.environ, {"GRAFANA_TEST_LOGIN": "user", "GRAFANA_TEST_PASSWORD": "secret"}):
            configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(configs[0].grafana_base_url, "https://grafana.example/grafana")
        self.assertEqual(configs[0].config_source, "default_credentials")
        self.assertEqual(configs[0].session_mode, "shared")
        self.assertEqual(configs[0].credential_ref, "__default__")
        self.assertEqual(configs[1].grafana_base_url, "https://other.example/grafana")
        self.assertEqual(configs[1].config_source, "inline")

    def test_default_credentials_render_false_applies_to_dashboard(self) -> None:
        config_path = self.write_config(
            """
            default_grafana_credentials:
              grafana_url: https://grafana.example/grafana
              token: env:GRAFANA_TEST_TOKEN
              render: false
            dashboards:
              UsesDefault:
                dash_title: Uses Default
            """
        )

        with patch.dict(os.environ, {"GRAFANA_TEST_TOKEN": "token"}):
            config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertFalse(config.render)
        self.assertEqual(config.config_source, "default_credentials")

    def test_named_credentials_render_false_can_be_overridden_by_dashboard(self) -> None:
        config_path = self.write_config(
            """
            grafana_credentials:
              prod:
                grafana_url: https://grafana.example/grafana
                token: env:GRAFANA_TEST_TOKEN
                render: false
            dashboards:
              UsesCredentialDefault:
                credentials: prod
                dash_title: Uses Credential Default
              DashboardOverride:
                credentials: prod
                dash_title: Dashboard Override
                render: true
            """
        )

        with patch.dict(os.environ, {"GRAFANA_TEST_TOKEN": "token"}):
            configs = GrafanaManager.load_grafana_config(config_path)

        self.assertFalse(configs[0].render)
        self.assertTrue(configs[1].render)
        self.assertEqual(configs[0].config_source, "named_credentials")
        self.assertEqual(configs[1].config_source, "named_credentials")

    def test_named_credentials_take_precedence_over_default_credentials(self) -> None:
        config_path = self.write_config(
            """
            default_grafana_credentials:
              grafana_url: https://default.example/grafana
              token: env:DEFAULT_GRAFANA_TOKEN
            grafana_credentials:
              prod:
                grafana_url: https://named.example/grafana
                token: env:NAMED_GRAFANA_TOKEN
            dashboards:
              UsesNamed:
                credentials: prod
                dash_title: Uses Named
            """
        )

        with patch.dict(os.environ, {"DEFAULT_GRAFANA_TOKEN": "default-token", "NAMED_GRAFANA_TOKEN": "named-token"}):
            config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.grafana_base_url, "https://named.example/grafana")
        self.assertEqual(config.credential_ref, "prod")
        self.assertEqual(config.config_source, "named_credentials")

    def test_missing_named_credential_fails_with_clear_path(self) -> None:
        config_path = self.write_config(
            """
            grafana_credentials: {}
            dashboards:
              Demo:
                credentials: missing
                dash_title: Demo
            """
        )

        with self.assertRaisesRegex(ConfigurationError, "dashboards.Demo.credentials"):
            GrafanaManager.load_grafana_config(config_path)

    def test_shared_session_pool_authenticates_once(self) -> None:
        first = SimpleNamespace(name="A", dash_title="A", session_mode="shared", session_key="prod|grafana")
        second = SimpleNamespace(name="B", dash_title="B", session_mode="shared", session_key="prod|grafana")
        args = SimpleNamespace(
            confluence_login="user", confluence_password="secret", timestamps=[], only_graphs=True,
            _grafana_session_pool=GrafanaSessionPool(), confluence_continue_on_error=False,
        )

        FakeManager.auth_count = 0
        FakeManager.sessions = []
        process_grafana_dashboard(first, "folder", args, Mock(), FakeManager)
        process_grafana_dashboard(second, "folder", args, Mock(), FakeManager)

        self.assertEqual(FakeManager.auth_count, 1)
        self.assertIs(FakeManager.sessions[0], FakeManager.sessions[1])


class FakeManager:
    auth_count = 0
    sessions = []

    def __init__(self, config, session=None):
        self.config = config
        self.session = session
        self.charts_path = f"graphs/{config.name}"
        self.sessions.append(session)

    def authenticate(self, login, password):
        self.__class__.auth_count += 1

    def download_charts(self, test_folder, timestamps):
        return None


class TestPanelVariants(unittest.TestCase):
    @staticmethod
    def timestamp() -> GrafanaTimeDownloader:
        return GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")

    def test_explicit_panel_variants_create_hashed_tasks_and_metadata(self) -> None:
        manager = GrafanaManager.load_grafana_config(self.config_path())[0]
        grafana = GrafanaManager(manager)
        grafana.dashboard_uid = "uid"
        grafana.dashboard_url = "/d/uid/demo"
        grafana.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard()})))

        panels = grafana.get_panels([self.timestamp()])

        variant_tasks = [task for task in grafana.render_tasks if task.artifact and task.artifact.get("artifact_type") == "variant"]
        self.assertEqual(len(variant_tasks), 2)
        self.assertEqual(variant_tasks[0].variables["service"], "api")
        self.assertIn("variant-00-000-", variant_tasks[0].file_name)
        self.assertNotIn("api", variant_tasks[0].file_name)
        self.assertEqual(panels[0].artifacts[1]["variant"]["variables"], {"service": "api"})
        self.assertEqual(panels[0].artifacts[1]["variant"]["label"], "Service: api")
        self.assertEqual(panels[0].artifacts[1]["source_panel_id"], 17)
        self.assertEqual(panels[0].artifacts[1]["source_panel_type"], "timeseries")
        self.assertEqual(panels[0].artifacts[1]["source_panel_title"], "Requests")
        self.assertEqual(panels[0].artifacts[1]["source_panel_display_title"], "Requests")
        self.assertEqual(panels[0].artifacts[1]["source_timestamp_id"], 0)

    def test_regex_variable_expansion_uses_filtered_values_and_safe_filenames(self) -> None:
        manager = GrafanaManager.load_grafana_config(self.regex_config_path())[0]
        grafana = GrafanaManager(manager)
        grafana.dashboard_uid = "uid"
        grafana.dashboard_url = "/d/uid/demo"
        grafana.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard_with_variables()})))

        panels = grafana.get_panels([self.timestamp()])

        variant_tasks = [task for task in grafana.render_tasks if task.artifact and task.artifact.get("artifact_type") == "variant"]
        self.assertEqual([task.variables["service"] for task in variant_tasks], ["api", "worker"])
        self.assertTrue(all("/" not in task.file_name for task in variant_tasks))
        self.assertTrue(all("api" not in task.file_name and "worker" not in task.file_name for task in variant_tasks))
        self.assertEqual(
            [artifact["variant"]["variables"] for artifact in panels[0].artifacts[1:]],
            [{"service": "api"}, {"service": "worker"}],
        )

    def config_path(self) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    panel_variants:
                      - name: by_service
                        selectors: {panel_id: 17}
                        variables:
                          service: {values: [api, worker]}
                        label_template: "Service: {service}"
                """))
        return config_path

    def regex_config_path(self) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent("""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    panel_variants:
                      - name: filtered_services
                        selectors: {panel_id: 17}
                        variables:
                          service:
                            match_values:
                              regex: "^(api|worker)$"
                """))
        return config_path

    @staticmethod
    def dashboard() -> dict:
        return {"panels": [{"id": 17, "type": "timeseries", "title": "Requests"}]}

    @staticmethod
    def dashboard_with_variables() -> dict:
        return {
            "panels": [{"id": 17, "type": "timeseries", "title": "Requests"}],
            "templating": {
                "list": [{
                    "name": "service",
                    "options": [
                        {"value": "api"},
                        {"value": "worker"},
                        {"value": "db/admin"},
                    ],
                }],
            },
        }


class TestCompositeArtifacts(unittest.TestCase):
    def write_config(self, content: str) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent(content))
        return config_path

    def test_vertical_composite_writes_png_and_hides_sources(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        panel = Panel(17, "timeseries", "CPU", 1, ["https://grafana.example/panel/17"])
        config = SimpleNamespace(name="Demo", panels=[panel], composites=[{
            "name": "overview", "layout": "vertical", "include_sources": False,
            "sources": [{"panel_id": 17}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "Demo"), exist_ok=True)
            image_path = os.path.join(temp_dir, "Demo", "Demo__17__0.png")
            Image.new("RGB", (10, 8), "red").save(image_path)

            generate_composites(config, os.path.join(temp_dir, "Demo"), [timestamp])

            composite = config.panels[-1].artifacts[0]
            output_path = os.path.join(temp_dir, "Demo", composite["png_file"])
            self.assertTrue(os.path.isfile(output_path))
            self.assertEqual(composite["artifact_type"], "composite")
            self.assertEqual(panel.artifacts[0]["confluence"]["visible"], False)
            self.assertIsNotNone(composite["composite"]["sources"][0]["artifact_id"])
            self.assertEqual(composite["composite"]["sources"][0]["artifact_id"], panel.artifacts[0]["artifact_id"])

    def test_missing_source_policies_fail_skip_and_placeholder(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            with self.assertRaises(FileNotFoundError):
                generate_composites(self.config_with_missing_source("fail"), charts_path, [timestamp])

            skip_config = self.config_with_missing_source("skip")
            generate_composites(skip_config, charts_path, [timestamp])
            skip_artifact = skip_config.panels[-1].artifacts[0]
            self.assertEqual(skip_artifact["render_status"], "skipped")
            self.assertIsNone(skip_artifact["png_file"])
            self.assertEqual(skip_artifact["composite"]["sources"][0]["missing_reason"], "selector_matched_no_panels")
            self.assertTrue(skip_artifact["composite"]["sources"][0]["missing"])
            self.assertTrue(skip_artifact["composite"]["sources"][0]["placeholder"])
            self.assertEqual(skip_artifact["composite"]["sources"][0]["selector"], {"panel_id": 999})

            placeholder_config = self.config_with_missing_source("placeholder")
            generate_composites(placeholder_config, charts_path, [timestamp])
            placeholder_artifact = placeholder_config.panels[-1].artifacts[0]
            output_file = os.path.join(charts_path, placeholder_artifact["png_file"])
            self.assertTrue(os.path.isfile(output_file))
            self.assertEqual(placeholder_artifact["composite"]["sources"][0]["missing_reason"], "selector_matched_no_panels")

    def test_missing_source_metadata_tracks_missing_png_for_matched_panel(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        config = SimpleNamespace(name="Demo", panels=[Panel(17, "timeseries", "CPU", 1)], composites=[{
            "name": "missing-png", "layout": "vertical", "missing_source": "skip",
            "sources": [{"panel_id": 17}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            generate_composites(config, charts_path, [timestamp])

        source = config.panels[-1].artifacts[0]["composite"]["sources"][0]
        self.assertEqual(source["panel_id"], 17)
        self.assertEqual(source["missing_reason"], "missing_png_file")
        self.assertEqual(source["selector"], {"panel_id": 17})

    def test_dashboard_grid_uses_grid_rectangles_with_letterboxing(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        first = Panel(1, "timeseries", "Wide", 1, grid_pos={"x": 0, "y": 0, "w": 12, "h": 8})
        second = Panel(2, "timeseries", "Short", 1, grid_pos={"x": 12, "y": 0, "w": 12, "h": 4})
        config = SimpleNamespace(name="Demo", panels=[first, second], composites=[{
            "name": "grid", "layout": "dashboard_grid", "gap_px": 0,
            "background": "#000000", "sources": [{"panel_id": 1}, {"panel_id": 2}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            Image.new("RGB", (120, 80), "red").save(os.path.join(charts_path, "Demo__1__0.png"))
            Image.new("RGB", (120, 20), "blue").save(os.path.join(charts_path, "Demo__2__0.png"))
            generate_composites(config, charts_path, [timestamp])

            with Image.open(os.path.join(charts_path, config.panels[-1].artifacts[0]["png_file"])) as image:
                self.assertEqual(image.size, (240, 80))
                self.assertEqual(image.getpixel((130, 5))[:3], (0, 0, 0))
                self.assertEqual(image.getpixel((130, 15))[:3], (0, 0, 255))
            self.assertEqual(config.panels[-1].artifacts[0]["composite"]["three_panel_policy"], "preserve")

    def test_matrix_composites_are_generated_per_matrix_context(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        first = Panel(1, "timeseries", "CPU", 1)
        second = Panel(2, "timeseries", "Memory", 1)
        first.artifacts = [self.matrix_artifact("Demo__1__matrix-prod__0.png", "prod")]
        second.artifacts = [self.matrix_artifact("Demo__2__matrix-prod__0.png", "prod"),
                            self.matrix_artifact("Demo__2__matrix-stage__0.png", "stage")]
        config = SimpleNamespace(name="Demo", panels=[first, second], composites=[{
            "name": "matrix-overview", "layout": "vertical", "sources": [{"panel_id": 1}, {"panel_id": 2}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            for file_name in ["Demo__1__matrix-prod__0.png", "Demo__2__matrix-prod__0.png", "Demo__2__matrix-stage__0.png"]:
                Image.new("RGB", (10, 8), "red").save(os.path.join(charts_path, file_name))
            generate_composites(config, charts_path, [timestamp])

        artifacts = config.panels[-1].artifacts
        self.assertEqual(len(artifacts), 2)
        contexts = [artifact["composite"]["matrix_context"][0]["value"] for artifact in artifacts]
        self.assertEqual(contexts, ["prod", "stage"])
        self.assertEqual(artifacts[0]["composite"]["sources"][0]["matrix_context"][0]["value"], "prod")
        self.assertIn("__matrix-", artifacts[0]["png_file"])

    @staticmethod
    def matrix_artifact(file_name: str, environment: str) -> dict:
        return {
            "artifact_type": "matrix", "timestamp_id": 0, "render_status": "rendered", "png_file": file_name,
            "matrix": {"context_path": [{"key": "environment", "label": "Environment", "value": environment}]},
        }

    def test_dashboard_grid_preserve_compacts_unselected_empty_grid_bands(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        top_right = Panel(4, "timeseries", "Load", 1, grid_pos={"x": 12, "y": 0, "w": 12, "h": 9})
        bottom_left = Panel(9, "timeseries", "CPU", 1, grid_pos={"x": 0, "y": 17, "w": 12, "h": 10})
        bottom_right = Panel(10, "timeseries", "Memory", 1, grid_pos={"x": 12, "y": 17, "w": 12, "h": 10})
        config = SimpleNamespace(name="Demo", panels=[top_right, bottom_left, bottom_right], composites=[{
            "name": "compact-grid", "layout": "dashboard_grid", "gap_px": 0,
            "background": "#000000", "sources": [{"panel_id": 4}, {"panel_id": 9}, {"panel_id": 10}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            Image.new("RGB", (120, 90), "red").save(os.path.join(charts_path, "Demo__4__0.png"))
            Image.new("RGB", (120, 100), "green").save(os.path.join(charts_path, "Demo__9__0.png"))
            Image.new("RGB", (120, 100), "blue").save(os.path.join(charts_path, "Demo__10__0.png"))
            generate_composites(config, charts_path, [timestamp])

            with Image.open(os.path.join(charts_path, config.panels[-1].artifacts[0]["png_file"])) as image:
                self.assertEqual(image.size, (240, 190))
                self.assertEqual(image.getpixel((125, 5))[:3], (255, 0, 0))
                self.assertEqual(image.getpixel((5, 95))[:3], (0, 128, 0))
                self.assertEqual(image.getpixel((125, 95))[:3], (0, 0, 255))
            self.assertEqual(config.panels[-1].artifacts[0]["composite"]["three_panel_policy"], "preserve")

    def test_dashboard_grid_top_wide_uses_presentation_layout_for_three_panels(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        panels = [
            Panel(4, "timeseries", "Load", 1, grid_pos={"x": 12, "y": 0, "w": 12, "h": 9}),
            Panel(9, "timeseries", "CPU", 1, grid_pos={"x": 0, "y": 17, "w": 12, "h": 10}),
            Panel(10, "timeseries", "Memory", 1, grid_pos={"x": 12, "y": 17, "w": 12, "h": 10}),
        ]
        config = SimpleNamespace(name="Demo", panels=panels, composites=[{
            "name": "top-wide", "layout": "dashboard_grid", "three_panel_policy": "top_wide",
            "gap_px": 0, "background": "#000000", "sources": [{"panel_id": 4}, {"panel_id": 9}, {"panel_id": 10}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            Image.new("RGB", (120, 90), "red").save(os.path.join(charts_path, "Demo__4__0.png"))
            Image.new("RGB", (120, 100), "green").save(os.path.join(charts_path, "Demo__9__0.png"))
            Image.new("RGB", (120, 100), "blue").save(os.path.join(charts_path, "Demo__10__0.png"))
            generate_composites(config, charts_path, [timestamp])

            with Image.open(os.path.join(charts_path, config.panels[-1].artifacts[0]["png_file"])) as image:
                self.assertEqual(image.size, (240, 190))
                self.assertEqual(image.getpixel((120, 5))[:3], (255, 0, 0))
                self.assertEqual(image.getpixel((5, 95))[:3], (0, 128, 0))
                self.assertEqual(image.getpixel((125, 95))[:3], (0, 0, 255))
            self.assertEqual(config.panels[-1].artifacts[0]["composite"]["three_panel_policy"], "top_wide")

    def test_dashboard_grid_bottom_half_keeps_top_natural_width(self) -> None:
        timestamp = GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")
        panels = [
            Panel(4, "timeseries", "Load", 1, grid_pos={"x": 12, "y": 0, "w": 12, "h": 9}),
            Panel(9, "timeseries", "CPU", 1, grid_pos={"x": 0, "y": 17, "w": 12, "h": 10}),
            Panel(10, "timeseries", "Memory", 1, grid_pos={"x": 12, "y": 17, "w": 12, "h": 10}),
        ]
        config = SimpleNamespace(name="Demo", panels=panels, composites=[{
            "name": "bottom-half", "layout": "dashboard_grid", "three_panel_policy": "bottom_half",
            "gap_px": 0, "background": "#000000", "sources": [{"panel_id": 4}, {"panel_id": 9}, {"panel_id": 10}],
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            charts_path = os.path.join(temp_dir, "Demo")
            os.makedirs(charts_path)
            Image.new("RGB", (120, 90), "red").save(os.path.join(charts_path, "Demo__4__0.png"))
            Image.new("RGB", (120, 100), "green").save(os.path.join(charts_path, "Demo__9__0.png"))
            Image.new("RGB", (120, 100), "blue").save(os.path.join(charts_path, "Demo__10__0.png"))
            generate_composites(config, charts_path, [timestamp])

            with Image.open(os.path.join(charts_path, config.panels[-1].artifacts[0]["png_file"])) as image:
                self.assertEqual(image.size, (120, 140))
                self.assertEqual(image.getpixel((60, 5))[:3], (255, 0, 0))
                self.assertEqual(image.getpixel((5, 95))[:3], (0, 128, 0))
                self.assertEqual(image.getpixel((65, 95))[:3], (0, 0, 255))
            self.assertEqual(config.panels[-1].artifacts[0]["composite"]["three_panel_policy"], "bottom_half")

    def test_invalid_three_panel_policy_fails_validation(self) -> None:
        config_path = self.write_config(
            """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                composites:
                  - name: invalid
                    layout: dashboard_grid
                    three_panel_policy: diagonal
                    sources:
                      - panel_id: 17
            """
        )

        with self.assertRaisesRegex(ConfigurationError, "three_panel_policy"):
            GrafanaManager.load_grafana_config(config_path)

    @staticmethod
    def config_with_missing_source(policy: str) -> SimpleNamespace:
        return SimpleNamespace(name="Demo", panels=[Panel(17, "timeseries", "CPU", 1)], composites=[{
            "name": f"missing-{policy}", "layout": "vertical", "missing_source": policy,
            "sources": [{"panel_id": 999}],
        }])

    def test_manifest_order_prefers_manifest_metadata_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            for name in ["B", "A"]:
                with open(os.path.join(temp_dir, f"{name}.yaml"), "w", encoding="utf-8") as metadata_file:
                    yaml.safe_dump({"name": name}, metadata_file)
            write_run_manifest(temp_dir, [SimpleNamespace(name="B"), SimpleNamespace(name="A")], "config.yaml")

            files = [os.path.basename(path) for path in dashboard_metadata_files(temp_dir)]

        self.assertEqual(files, ["B.yaml", "A.yaml"])


if __name__ == "__main__":
    unittest.main()
