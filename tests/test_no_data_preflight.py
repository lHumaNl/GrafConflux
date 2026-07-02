import tempfile
import unittest
from unittest.mock import Mock, patch

import grafconflux.grafana as grafana_facade
import grafconflux.no_data as no_data
from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.confluence import build_confluence_storage_content
from grafconflux.grafana import (
    ConfigurationError,
    GrafanaConfigDownloader,
    GrafanaConfigUploader,
    GrafanaManager,
    NoDataDetectorRegistry,
    Panel,
    interpret_no_data_response,
    infer_no_data_datasource,
)


class TestNoDataPreflight(unittest.TestCase):
    def create_config(self, **overrides):
        config = {"dash_title": "Dashboard", "host": "https://grafana.example"}
        config.update(overrides)
        return GrafanaConfigDownloader("demo", config)

    def create_manager(self, **overrides):
        manager = GrafanaManager(self.create_config(**overrides))
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.get_dashboard_uid = Mock(return_value=("dashboard-uid", "/d/dashboard-uid/dashboard"))
        return manager

    def create_timestamps(self, count=1):
        return [
            GrafanaTimeDownloader(f"tag{index}__&from=1700000000&to=1700003600", index, "UTC")
            for index in range(count)
        ]

    def dashboard(self, panel):
        return {"panels": [panel], "templating": {"list": []}}

    def prepare_dashboard_response(self, manager, dashboard):
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})))

    def test_no_data_symbols_remain_available_from_grafana_facade(self):
        self.assertIs(grafana_facade.NoDataDetectorRegistry, no_data.NoDataDetectorRegistry)
        self.assertIs(grafana_facade.infer_no_data_datasource, no_data.infer_no_data_datasource)
        self.assertIs(grafana_facade.interpret_no_data_response, no_data.interpret_no_data_response)

    def test_config_defaults_keep_no_data_collection_enabled(self):
        config = self.create_config()

        self.assertTrue(config.collect_no_data_panels)
        self.assertEqual(config.no_data_preflight.mode, "conservative")
        self.assertEqual(config.no_data_preflight.on_error, "render_anyway")
        self.assertEqual(config.no_data_preflight.min_non_empty_frames, 1)

    def test_invalid_no_data_preflight_values_fail_validation(self):
        invalid_configs = [
            ({"no_data_preflight": {"mode": "aggressive"}}, "no_data_preflight.mode.*conservative"),
            ({"no_data_preflight": {"on_error": "skip_panel"}}, "no_data_preflight.on_error.*render_anyway"),
            ({"no_data_preflight": {"min_non_empty_frames": 2}}, "min_non_empty_frames.*expected 1"),
            ({"collect_no_data_panels": "false"}, "collect_no_data_panels.*expected bool"),
        ]

        for overrides, pattern in invalid_configs:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ConfigurationError, pattern):
                    self.create_config(**overrides)

    def test_detector_registry_and_datasource_inference_are_conservative(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }

        inference = infer_no_data_datasource(panel, NoDataDetectorRegistry.default())

        self.assertTrue(inference.applicable)
        self.assertEqual(inference.detector_name, "prometheus")
        self.assertEqual(inference.datasource_uid, "prom-uid")
        target_panel = dict(panel, datasource=None)
        target_panel["targets"] = [{"refId": "A", "datasource": {"type": "prometheus", "uid": "target-uid"}}]
        self.assertEqual(
            infer_no_data_datasource(target_panel, NoDataDetectorRegistry.default()).datasource_uid,
            "target-uid",
        )

    def test_mixed_transform_expression_and_unsupported_datasources_render_anyway(self):
        registry = NoDataDetectorRegistry.default()
        unsupported = {"id": 1, "type": "stat", "targets": [{"refId": "A", "datasource": {"type": "unknown", "uid": "x"}}]}
        transformed = {"id": 2, "type": "stat", "transformations": [{"id": "reduce"}], "targets": [{"refId": "A", "datasource": {"type": "prometheus", "uid": "x"}}]}
        expression = {"id": 3, "type": "stat", "targets": [{"refId": "A", "datasource": {"type": "__expr__", "uid": "expr"}}]}
        mixed = {"id": 4, "type": "stat", "targets": [{"refId": "A", "datasource": {"type": "prometheus", "uid": "a"}}, {"refId": "B", "datasource": {"type": "prometheus", "uid": "b"}}]}

        for panel in (unsupported, transformed, expression, mixed):
            with self.subTest(panel_id=panel["id"]):
                self.assertFalse(infer_no_data_datasource(panel, registry).applicable)

    def test_response_interpreter_confirms_empty_frames_only(self):
        empty = {"results": {"A": {"frames": [{"schema": {}, "data": {"values": []}}]}}}
        non_empty = {"results": {"A": {"frames": [{"data": {"values": [[1], [2]]}}]}}}
        missing_ref = {"results": {}}
        plugin_error = {"results": {"A": {"error": "bad query", "frames": []}}}

        self.assertEqual(interpret_no_data_response(empty, ["A"]).status, "confirmed_no_data")
        self.assertEqual(interpret_no_data_response(non_empty, ["A"]).status, "confirmed_has_data")
        self.assertEqual(interpret_no_data_response(missing_ref, ["A"]).status, "inconclusive")
        self.assertEqual(interpret_no_data_response(plugin_error, ["A"]).status, "error")

    def test_collect_false_skips_confirmed_no_data_without_rendering(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }
        manager = self.create_manager(collect_no_data_panels=False)
        manager.session.post = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})))
        self.prepare_dashboard_response(manager, self.dashboard(panel))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        download_chart.assert_not_called()
        artifact = manager.config.panels[0].artifacts[0]
        self.assertEqual(artifact["render_status"], "skipped_no_data")
        self.assertIsNone(artifact["png_file"])
        self.assertEqual(artifact["skip_reason"], "datasource_query_returned_empty_frames")

    def test_store_skip_metadata_false_skips_without_no_data_metadata(self):
        panel = {"id": 7, "type": "timeseries", "title": "CPU",
                 "datasource": {"type": "prometheus", "uid": "prom-uid"},
                 "targets": [{"refId": "A", "expr": "up"}]}
        manager = self.create_manager(
            collect_no_data_panels=False,
            no_data_preflight={"store_skip_metadata": False},
        )
        manager.session.post = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})))
        self.prepare_dashboard_response(manager, self.dashboard(panel))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        download_chart.assert_not_called()
        artifact = manager.config.panels[0].artifacts[0]
        self.assertEqual(artifact["render_status"], "missing")
        self.assertIsNone(artifact["png_file"])
        self.assertIsNone(artifact["skip_reason"])
        self.assertNotIn("preflight_status", artifact)

    def test_default_collection_enabled_does_not_run_preflight(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }
        manager = self.create_manager()
        manager.session.post = Mock()
        self.prepare_dashboard_response(manager, self.dashboard(panel))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        manager.session.post.assert_not_called()
        download_chart.assert_called_once()
        self.assertFalse(manager.config.panels[0].artifacts)

    def test_non_empty_and_error_preflight_render_anyway(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }
        responses = [
            Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": [{"data": {"values": [[1]]}}]}}})),
            Mock(status_code=500, json=Mock(return_value={})),
        ]

        for response in responses:
            with self.subTest(status_code=response.status_code):
                manager = self.create_manager(collect_no_data_panels=False)
                manager.session.post = Mock(return_value=response)
                self.prepare_dashboard_response(manager, self.dashboard(panel))
                with tempfile.TemporaryDirectory() as temp_dir:
                    with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                        manager.download_charts(temp_dir, self.create_timestamps())
                download_chart.assert_called_once()
                self.assertEqual(manager.config.panels[0].artifacts[0]["render_status"], "rendered")

    def test_multiple_timestamps_can_mix_rendered_and_skipped_statuses(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }
        responses = [
            Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})),
            Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": [{"data": {"values": [[1]]}}]}}})),
        ]
        manager = self.create_manager(collect_no_data_panels=False)
        manager.session.post = Mock(side_effect=responses)
        self.prepare_dashboard_response(manager, self.dashboard(panel))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps(count=2))

        statuses = [artifact["render_status"] for artifact in manager.config.panels[0].artifacts]
        self.assertEqual(statuses, ["skipped_no_data", "rendered"])
        self.assertEqual(download_chart.call_count, 1)

    def test_parse_error_and_timeout_render_anyway(self):
        panel = {
            "id": 7,
            "type": "timeseries",
            "title": "CPU",
            "datasource": {"type": "prometheus", "uid": "prom-uid"},
            "targets": [{"refId": "A", "expr": "up"}],
        }
        cases = [
            Mock(status_code=200, json=Mock(side_effect=ValueError("invalid json"))),
            TimeoutError("slow query"),
        ]

        for outcome in cases:
            with self.subTest(outcome=type(outcome).__name__):
                manager = self.create_manager(collect_no_data_panels=False)
                manager.session.post = Mock(side_effect=outcome if isinstance(outcome, Exception) else None)
                if not isinstance(outcome, Exception):
                    manager.session.post.return_value = outcome
                self.prepare_dashboard_response(manager, self.dashboard(panel))
                with tempfile.TemporaryDirectory() as temp_dir:
                    with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                        manager.download_charts(temp_dir, self.create_timestamps())
                download_chart.assert_called_once()
                self.assertEqual(manager.config.panels[0].artifacts[0]["render_status"], "rendered")

    def test_repeating_rule_override_can_disable_dashboard_no_data_skip(self):
        dashboard = {
            "templating": {"list": [{"name": "host", "current": {"value": "prod-1"}}]},
            "panels": [{
                "id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "repeat": "host",
                "datasource": {"type": "prometheus", "uid": "prom-uid"},
                "targets": [{"refId": "A", "expr": "up"}],
            }],
        }
        manager = self.create_manager(
            collect_no_data_panels=False,
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 17, "collect_no_data_panels": True}],
        )
        manager.session.post = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})))
        self.prepare_dashboard_response(manager, dashboard)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        manager.session.post.assert_not_called()
        download_chart.assert_called_once()
        self.assertEqual(manager.config.panels[0].artifacts[0]["render_status"], "rendered")

    def test_repeating_panel_preflight_applies_per_repeat_value(self):
        dashboard = {
            "templating": {"list": [{"name": "host", "options": [
                {"value": "prod-1"},
                {"value": "prod-2"},
            ]}]},
            "panels": [{
                "id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "repeat": "host",
                "datasource": {"type": "prometheus", "uid": "prom-uid"},
                "targets": [{"refId": "A", "expr": "up"}],
            }],
        }
        responses = [
            Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})),
            Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": [{"data": {"values": [[1]]}}]}}})),
        ]
        manager = self.create_manager(
            collect_no_data_panels=False,
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "all"}}],
        )
        manager.session.post = Mock(side_effect=responses)
        self.prepare_dashboard_response(manager, dashboard)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        statuses = [artifact["render_status"] for artifact in manager.config.panels[0].artifacts]
        self.assertEqual(statuses, ["skipped_no_data", "rendered"])
        self.assertEqual(download_chart.call_count, 1)
        first_payload = manager.session.post.call_args_list[0].kwargs["json"]
        self.assertEqual(first_payload["queries"][0]["scopedVars"]["host"]["value"], "prod-1")

    def test_auto_repeating_panel_preflight_skips_no_data_task(self):
        dashboard = {
            "templating": {"list": [{"name": "host", "current": {"value": "prod-1"}}]},
            "panels": [{
                "id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "repeat": "host",
                "datasource": {"type": "prometheus", "uid": "prom-uid"},
                "targets": [{"refId": "A", "expr": "up"}],
            }],
        }
        manager = self.create_manager(collect_no_data_panels=False)
        manager.session.post = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": []}}})))
        self.prepare_dashboard_response(manager, dashboard)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart") as download_chart:
                manager.download_charts(temp_dir, self.create_timestamps())

        download_chart.assert_not_called()
        artifact = manager.config.panels[0].artifacts[0]
        self.assertEqual(artifact["render_status"], "skipped_no_data")
        self.assertEqual(artifact["repeat_value"], "prod-1")

    def test_confluence_hides_skipped_no_data_artifacts(self):
        panel = Panel(7, "timeseries", "CPU", 1, ["legacy-link"], artifacts=[
            {"timestamp_tag": "smoke", "render_status": "skipped_no_data", "png_file": None, "repeat_value": None},
            {"timestamp_tag": "smoke", "render_status": "rendered", "png_file": "demo__7__0.png", "link": "panel-link"},
        ])
        config = Mock(name="demo", full_links=["dashboard-link"], snapshot_urls=None, panels=[panel], backup_dashboard_links=[])
        timestamp = Mock(id_time=0, time_tag="smoke", start_time_human="start", end_time_human="end")

        content = build_confluence_storage_content([config], [timestamp], 900)

        self.assertIn("demo__7__0.png", content)
        self.assertNotIn("skipped_no_data", content)

    def test_upload_only_new_and_legacy_metadata_do_not_infer_no_data_from_missing_png(self):
        config = {
            "panels": [
                {"panel_id": 1, "type": "stat", "title": "Legacy", "links": ["legacy-link"]},
                {"panel_id": 2, "type": "stat", "title": "New", "links": [], "artifacts": [{"timestamp_tag": "tag", "render_status": "missing", "png_file": None}]},
            ],
            "full_links": ["dashboard-link"],
            "snapshot_urls": [],
            "charts_path": "unused",
            "timestamps": [{
                "time_tag": "tag",
                "id_time": 0,
                "start_time_timestamp": 1,
                "end_time_timestamp": 2,
                "start_time_human": "start",
                "end_time_human": "end",
            }],
        }

        uploader = GrafanaConfigUploader("demo", config)

        self.assertFalse(uploader.panels[0].artifacts)
        self.assertEqual(uploader.panels[1].artifacts[0]["render_status"], "missing")
