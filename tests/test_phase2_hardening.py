import os
import tempfile
import unittest
from dataclasses import fields
from unittest.mock import Mock, patch

import yaml

from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import GrafanaConfigDownloader, GrafanaConfigUploader, GrafanaManager
from grafconflux.options import GrafConfluxRunOptions


class TestPhase2Diagnostics(unittest.TestCase):
    def create_manager(self, **overrides: object) -> GrafanaManager:
        config = {"dash_title": "Dashboard", "host": "https://grafana.example"}
        config.update(overrides)
        return GrafanaManager(GrafanaConfigDownloader("demo", config))

    def create_timestamp(self) -> list[GrafanaTimeDownloader]:
        return [GrafanaTimeDownloader("tag__&from=1700000000&to=1700003600", 0, "UTC")]

    def prepare_dashboard(self, manager: GrafanaManager, dashboard: dict) -> None:
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})))

    def test_folder_mismatch_error_lists_available_folders_and_case_sensitive_contract(self) -> None:
        manager = self.create_manager(folder="Production")
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=[
            {"uid": "uid-1", "title": "Dashboard", "folderTitle": "production", "url": "/d/uid-1"},
            {"uid": "uid-2", "title": "Dashboard", "folderTitle": "Staging", "url": "/d/uid-2"},
        ])))

        with self.assertRaisesRegex(ValueError, "available folders.*production.*Staging.*case-sensitive"):
            manager.get_dashboard_uid()

    def test_folder_uid_mismatch_error_names_folder_uid_selector(self) -> None:
        manager = self.create_manager(folder_uid="folder-prod")
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=[
            {
                "uid": "uid-1",
                "title": "Dashboard",
                "folderUid": "folder-stage",
                "folderTitle": "Staging",
                "url": "/d/uid-1",
            },
        ])))

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            with self.assertRaisesRegex(ValueError, "folder_uid \"folder-prod\".*folder-stage"):
                manager.get_dashboard_uid()

        self.assertTrue(any("folder mismatch" in message for message in logs.output))

    def test_dashboard_lookup_ambiguity_lists_folder_uid(self) -> None:
        manager = self.create_manager()
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=[
            {
                "uid": "uid-1",
                "title": "Dashboard",
                "folderUid": "prod",
                "folderTitle": "Production",
                "url": "/d/uid-1",
            },
            {
                "uid": "uid-2",
                "title": "Dashboard",
                "folderUid": "stage",
                "folderTitle": "Staging",
                "url": "/d/uid-2",
            },
        ])))

        with self.assertRaisesRegex(ValueError, "uid-1.*folder_uid=prod.*uid-2.*folder_uid=stage"):
            manager.get_dashboard_uid()

    def test_download_metadata_persists_dashboard_identity_fields(self) -> None:
        manager = self.create_manager(folder="Production")
        manager.session.get = Mock(side_effect=self.fake_lookup_and_dashboard_payload)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart"):
                manager.download_charts(temp_dir, self.create_timestamp())
            with open(os.path.join(temp_dir, "demo.yaml"), "r", encoding="utf-8") as metadata_file:
                metadata = yaml.safe_load(metadata_file)

        self.assertEqual(metadata["dashboard_uid"], "uid-1")
        self.assertEqual(metadata["dashboard_title"], "Dashboard")
        self.assertEqual(metadata["folder_uid"], "folder-prod")
        self.assertEqual(metadata["folder_title"], "Production")

    @staticmethod
    def fake_lookup_and_dashboard_payload(url: str, **kwargs: object) -> Mock:
        if "/api/search" in url:
            return Mock(status_code=200, json=Mock(return_value=[{
                "uid": "uid-1",
                "title": "Dashboard",
                "folderUid": "folder-prod",
                "folderTitle": "Production",
                "url": "/d/uid-1/dashboard",
            }]))
        return Mock(status_code=200, json=Mock(return_value={"dashboard": {
            "panels": [{"id": 7, "type": "timeseries", "title": "CPU"}],
        }}))

    def test_panel_filtering_logs_specific_include_and_exclude_reasons(self) -> None:
        manager = self.create_manager(
            panel_filtering={
                "mode": "include_only_selected",
                "include_rows": {"titles": ["Production"]},
                "exclude_panels": {"ids": [2]},
            },
        )
        self.prepare_dashboard(manager, {"panels": [
            {"id": 10, "type": "row", "title": "Production", "panels": [
                {"id": 1, "type": "timeseries", "title": "CPU"},
                {"id": 2, "type": "timeseries", "title": "Debug"},
            ]},
        ]})

        with self.assertLogs("grafconflux.grafana", level="INFO") as logs:
            panels = manager.get_panels(self.create_timestamp())

        self.assertEqual([panel.panel_id for panel in panels], [1])
        self.assertTrue(any(
            "included panel_id=1" in message and "reason=include_rows.titles" in message
            for message in logs.output
        ))
        self.assertTrue(any(
            "excluded panel_id=2" in message and "reason=exclude_panels.ids" in message
            for message in logs.output
        ))

    def test_repeating_selector_no_match_error_lists_available_source_panels(self) -> None:
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"title": "Missing panel"}],
        )
        self.prepare_dashboard(manager, {
            "templating": {"list": []},
            "panels": [{"id": 17, "type": "timeseries", "title": "CPU by host", "repeat": "host"}],
        })

        with self.assertRaisesRegex(ValueError, "title=\"Missing panel\".*available source panels.*panel_id=17.*CPU by host"):
            manager.get_panels(self.create_timestamp())

    def test_panel_selector_no_match_warning_lists_available_panel_ids(self) -> None:
        manager = self.create_manager(
            panel_filtering={"mode": "include_only_selected", "include_panels": {"ids": [999]}},
        )
        self.prepare_dashboard(manager, {"panels": [{"id": 1, "type": "timeseries", "title": "CPU"}]})

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            manager.get_panels(self.create_timestamp())

        self.assertTrue(any("available_panel_ids=[1]" in message for message in logs.output))

    def test_collapsed_alias_conflict_warning_includes_values_and_yaml_path(self) -> None:
        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            GrafanaConfigDownloader("demo", {
                "dash_title": "Dashboard",
                "host": "https://grafana.example",
                "download_collapsed_rows": False,
                "download_collapse_panels": True,
            })

        self.assertTrue(any("dashboards.demo.download_collapsed_rows=false" in message for message in logs.output))
        self.assertTrue(any("download_collapse_panels=true" in message for message in logs.output))


class TestPhase2Compatibility(unittest.TestCase):
    def test_upload_only_legacy_metadata_without_snapshot_urls_is_accepted(self) -> None:
        uploader = GrafanaConfigUploader("demo", {
            "panels": [{"panel_id": 1, "type": "graph", "title": "CPU", "links": ["panel-link"]}],
            "full_links": ["dashboard-link"],
            "charts_path": "unused",
            "timestamps": [{
                "time_tag": "tag",
                "id_time": 0,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            }],
        })

        self.assertEqual(uploader.snapshot_urls, [])
        self.assertEqual(uploader.panels[0].links, ["panel-link"])

    def test_public_run_options_contract_fields_remain_stable(self) -> None:
        self.assertEqual(
            [field.name for field in fields(GrafConfluxRunOptions)],
            [
                "wiki_url", "confluence_page_id", "confluence_login", "confluence_password",
                "timestamps", "config_file", "test_root_folder", "test_upload_folders",
                "graph_width", "test_id", "threads", "only_graphs", "tz",
                "confluence_verify_ssl", "confluence_upload_threads", "confluence_upload_delay",
                "confluence_upload_rate_per_second", "confluence_retry", "confluence_retry_count",
                "confluence_retry_delay", "confluence_retry_backoff_multiplier",
                "confluence_retry_max_delay", "confluence_retry_jitter", "confluence_continue_on_error",
            ],
        )


class TestPhase2PipelineOrdering(unittest.TestCase):
    @staticmethod
    def create_timestamp() -> list[GrafanaTimeDownloader]:
        return [GrafanaTimeDownloader("tag__&from=1700000000&to=1700003600", 0, "UTC")]

    def test_download_charts_orchestration_order_is_stable(self) -> None:
        calls = []
        manager = GrafanaManager(GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
            "snapshot": True,
        }))
        task_panel = Mock(panel_id=7, links=[None], artifacts=[])
        task = Mock(panel=task_panel, timestamp=self.create_timestamp()[0])
        manager.browser_list = [Mock(quit=Mock(side_effect=lambda: calls.append("browser_quit")))]

        manager.get_dashboard_uid = Mock(side_effect=lambda: calls.append("lookup") or ("uid-1", "/d/uid-1"))
        manager.get_panels = Mock(side_effect=lambda timestamps: self.fake_panels(manager, task, calls))
        manager._GrafanaManager__get_full_links = Mock(side_effect=lambda timestamps: calls.append("full_links") or ["dashboard-link"])
        manager._skip_no_data_task = Mock(side_effect=lambda render_task: calls.append("preflight") or False)
        manager._GrafanaManager__download_chart = Mock(side_effect=lambda render_task: calls.append("download"))
        manager.take_snapshot = Mock(side_effect=lambda timestamps, folder: calls.append("snapshot"))
        manager._GrafanaManager__save_params_to_file = Mock(side_effect=lambda timestamps, folder: calls.append("metadata"))

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.download_charts(temp_dir, [task.timestamp])

        self.assertEqual(
            calls,
            ["lookup", "panels", "full_links", "preflight", "download", "browser_quit", "snapshot", "metadata"],
        )

    def test_download_charts_closes_browsers_when_download_future_raises(self) -> None:
        calls = []
        manager = GrafanaManager(GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
            "snapshot": True,
        }))
        task_panel = Mock(panel_id=7, links=[None], artifacts=[])
        task = Mock(panel=task_panel, timestamp=self.create_timestamp()[0])
        manager.browser_list = [Mock(quit=Mock(side_effect=lambda: calls.append("browser_quit")))]

        manager.get_dashboard_uid = Mock(return_value=("uid-1", "/d/uid-1"))
        manager.get_panels = Mock(side_effect=lambda timestamps: self.fake_panels(manager, task, calls))
        manager._GrafanaManager__get_full_links = Mock(return_value=["dashboard-link"])
        manager._skip_no_data_task = Mock(return_value=False)
        manager._GrafanaManager__download_chart = Mock(side_effect=RuntimeError("download failed"))
        manager.take_snapshot = Mock(side_effect=lambda timestamps, folder: calls.append("snapshot"))
        manager._GrafanaManager__save_params_to_file = Mock(side_effect=lambda timestamps, folder: calls.append("metadata"))

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.download_charts(temp_dir, [task.timestamp])

        self.assertEqual(calls, ["panels", "browser_quit", "snapshot", "metadata"])
        self.assertEqual(manager.browser_list, [])

    @staticmethod
    def fake_panels(manager: GrafanaManager, task: Mock, calls: list[str]) -> list[Mock]:
        calls.append("panels")
        manager._render_tasks = [task]
        return [task.panel]

    def test_pipeline_order_lookup_collapsed_filtering_repeating_no_data_snapshots(self) -> None:
        calls = []
        manager = GrafanaManager(GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
            "download_collapsed_rows": True,
            "panel_filtering": {"mode": "include_only_selected", "include_rows": {"titles": ["Production"]}},
            "enable_repeating_panels": True,
            "repeating_panels": [{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["prod-1", "prod-2"]}}],
            "collect_no_data_panels": False,
            "snapshot": True,
            "snapshot_store_dashboard_json": False,
        }))
        manager.session.get = Mock(side_effect=lambda url, **kwargs: self.fake_get(url, calls))
        manager.session.post = Mock(side_effect=lambda url, **kwargs: self.fake_post(url, kwargs, calls))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(manager, "_GrafanaManager__download_chart", side_effect=lambda task: calls.append(f"render:{task.repeat_value}")):
                with patch.object(manager, "_take_snapshot_ui", side_effect=lambda timestamps, folder: calls.append("snapshot")):
                    manager.download_charts(temp_dir, [GrafanaTimeDownloader("tag__&from=1700000000&to=1700003600", 0, "UTC")])

        self.assertLess(calls.index("lookup"), calls.index("dashboard_payload"))
        self.assertEqual([task.repeat_value for task in manager.render_tasks], ["prod-1", "prod-2"])
        self.assertEqual([panel.panel_id for panel in manager.config.panels], [17])
        self.assertLess(calls.index("no_data:prod-1"), calls.index("no_data:prod-2"))
        self.assertLess(calls.index("no_data:prod-2"), calls.index("render:prod-2"))
        self.assertLess(calls.index("render:prod-2"), calls.index("snapshot"))

    @staticmethod
    def fake_get(url: str, calls: list[str]) -> Mock:
        if "/api/search" in url:
            calls.append("lookup")
            return Mock(status_code=200, json=Mock(return_value=[{"uid": "uid-1", "title": "Dashboard", "url": "/d/uid-1"}]))
        calls.append("dashboard_payload")
        return Mock(status_code=200, json=Mock(return_value={"dashboard": {
            "templating": {"list": []},
            "panels": [
                {"id": 1, "type": "timeseries", "title": "Outside"},
                {"id": 10, "type": "row", "title": "Production", "collapsed": True, "panels": [
                    {"id": 17, "type": "timeseries", "title": "CPU by host", "repeat": "host",
                     "datasource": {"type": "prometheus", "uid": "prom"}, "targets": [{"refId": "A", "expr": "up"}]},
                ]},
            ],
        }}))

    @staticmethod
    def fake_post(url: str, kwargs: dict, calls: list[str]) -> Mock:
        host = kwargs["json"]["queries"][0]["scopedVars"]["host"]["value"]
        calls.append(f"no_data:{host}")
        frames = [] if host == "prod-1" else [{"data": {"values": [[1]]}}]
        return Mock(status_code=200, json=Mock(return_value={"results": {"A": {"frames": frames}}}))


if __name__ == "__main__":
    unittest.main()
