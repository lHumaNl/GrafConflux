import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main
from grafconflux._orchestration.paths import sanitize_run_folder_segment
from grafconflux._orchestration.runner import _build_test_folder
from grafconflux.orchestration import _create_confluence_manager
from grafconflux.grafana import GrafanaConfigUploader


class TestMainHelpers(unittest.TestCase):
    @staticmethod
    def make_run_args(test_upload_folders=None, only_graphs=False):
        retry_options = {
            "confluence_retry": True,
            "confluence_retry_backoff_multiplier": 1.0,
            "confluence_retry_count": 3,
            "confluence_retry_delay": 1.0,
            "confluence_retry_jitter": 0,
            "confluence_retry_max_delay": None,
        }
        upload_options = {
            "confluence_upload_delay": 0,
            "confluence_upload_rate_per_second": None,
            "confluence_upload_threads": 1,
        }
        defaults = dict(
            config_file="config.yaml",
            confluence_continue_on_error=True,
            confluence_login=None,
            confluence_page_id=123,
            confluence_password=None,
            confluence_token=None,
            confluence_verify_ssl=True,
            graph_width=1000,
            only_graphs=only_graphs,
            test_id="demo",
            test_root_folder="root",
            test_upload_folders=test_upload_folders,
            threads=1,
            timestamps=["ts1"],
            wiki_url="https://wiki.example",
        )
        return SimpleNamespace(**defaults, **retry_options, **upload_options)

    def make_upload_config(self, name, charts_path, repeat_value):
        png_file = f"{name}__17__repeat-{repeat_value}__0.png"
        return GrafanaConfigUploader(name, {
            "charts_path": charts_path,
            "full_links": [f"dashboard-{repeat_value}"],
            "snapshot_urls": [],
            "timestamps": [{
                "time_tag": repeat_value,
                "id_time": 0,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            }],
            "panels": [{
                "panel_id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "links": [f"link-{repeat_value}"],
                "is_repeating": True,
                "source_panel_id": 17,
                "repeat_var": "host",
                "artifacts": [{
                    "timestamp_id": 0,
                    "timestamp_tag": repeat_value,
                    "from": "1700000000000",
                    "to": "1700003600000",
                    "render_status": "rendered",
                    "png_file": png_file,
                    "skip_reason": None,
                    "repeat_var": "host",
                    "repeat_value": repeat_value,
                    "repeat_value_slug": repeat_value,
                    "link": f"link-{repeat_value}",
                    "artifact_id": f"stale-{repeat_value}",
                    "order_index": 500,
                }],
            }],
        })

    def make_upload_folders(self, temp_dir):
        folders = []
        configs = []
        for folder_name, repeat_value in (("one", "prod-1"), ("two", "prod-2")):
            folder = os.path.join(temp_dir, folder_name)
            charts_path = os.path.join(folder, "demo")
            os.makedirs(charts_path)
            png_file = f"demo__17__repeat-{repeat_value}__0.png"
            with open(os.path.join(charts_path, png_file), "wb") as image_file:
                image_file.write(b"png")
            folders.append(folder)
            configs.append(self.make_upload_config("demo", charts_path, repeat_value))
        return folders, configs

    def make_upload_config_with_timestamps(self, name, charts_path, timestamp_tags):
        artifacts = []
        timestamps = []
        links = []
        for index, tag in enumerate(timestamp_tags):
            png_file = f"{name}__17__repeat-{tag}__{index}.png"
            artifacts.append({
                "timestamp_id": index,
                "timestamp_tag": tag,
                "from": "1700000000000",
                "to": "1700003600000",
                "render_status": "rendered",
                "png_file": png_file,
                "skip_reason": None,
                "repeat_var": "host",
                "repeat_value": tag,
                "repeat_value_slug": tag,
                "link": f"link-{tag}",
                "artifact_id": f"stale-{name}-{tag}",
                "order_index": 500 + index,
            })
            timestamps.append({
                "time_tag": tag,
                "id_time": index,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            })
            links.append(f"link-{tag}")
            with open(os.path.join(charts_path, png_file), "wb") as image_file:
                image_file.write(b"png")

        return GrafanaConfigUploader(name, {
            "charts_path": charts_path,
            "full_links": [f"dashboard-{name}"],
            "snapshot_urls": [],
            "timestamps": timestamps,
            "panels": [{
                "panel_id": 17,
                "type": "timeseries",
                "title": f"{name} CPU by host",
                "links": links,
                "is_repeating": True,
                "source_panel_id": 17,
                "repeat_var": "host",
                "artifacts": artifacts,
            }],
        })

    def make_multi_dashboard_upload_folders(self, temp_dir):
        folders = []
        configs = []
        folder_specs = (
            ("one", {"alpha": ["alpha-a", "alpha-b"], "beta": ["beta-a"]}),
            ("two", {"alpha": ["alpha-c"], "beta": ["beta-b"]}),
        )
        for folder_name, dashboard_specs in folder_specs:
            folder = os.path.join(temp_dir, folder_name)
            folders.append(folder)
            for dashboard_name, timestamp_tags in dashboard_specs.items():
                charts_path = os.path.join(folder, dashboard_name)
                os.makedirs(charts_path)
                configs.append(self.make_upload_config_with_timestamps(dashboard_name, charts_path, timestamp_tags))
        return folders, configs

    def test_multi_folder_upload_only_merge_preserves_repeated_artifacts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_upload_folders(temp_dir)
            args = SimpleNamespace(test_upload_folders=folders, test_root_folder=temp_dir, test_id="merged")

            merged_configs, _ = main.transform_grafana_configs(configs, args)

        artifacts = merged_configs[0].panels[0].artifacts
        self.assertEqual([artifact["repeat_value"] for artifact in artifacts], ["prod-1", "prod-2"])
        self.assertEqual([artifact["timestamp_id"] for artifact in artifacts], [0, 1])
        self.assertEqual([artifact["order_index"] for artifact in artifacts], [0, 1])
        self.assertTrue(all(not artifact["artifact_id"].startswith("stale-") for artifact in artifacts))

    def test_multi_folder_filename_shift_rewrites_repeated_artifact_png_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_upload_folders(temp_dir)
            args = SimpleNamespace(test_upload_folders=folders, test_root_folder=temp_dir, test_id="merged")

            merged_configs, folder_graphs = main.transform_grafana_configs(configs, args)

            shifted_png = merged_configs[0].panels[0].artifacts[1]["png_file"]
            shifted_path = os.path.join(folder_graphs, "demo", shifted_png)
            self.assertEqual(shifted_png, "demo__17__repeat-prod-2__1.png")
            self.assertTrue(os.path.isfile(shifted_path))

    def test_multi_folder_merge_keeps_timestamp_offsets_per_dashboard_name(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_multi_dashboard_upload_folders(temp_dir)
            args = SimpleNamespace(test_upload_folders=folders, test_root_folder=temp_dir, test_id="merged")

            merged_configs, folder_graphs = main.transform_grafana_configs(configs, args)
            configs_by_name = {config.name: config for config in merged_configs}
            alpha_artifacts = configs_by_name["alpha"].panels[0].artifacts
            beta_artifacts = configs_by_name["beta"].panels[0].artifacts
            alpha_path = os.path.join(folder_graphs, "alpha", alpha_artifacts[2]["png_file"])
            beta_path = os.path.join(folder_graphs, "beta", beta_artifacts[1]["png_file"])

            self.assertEqual(alpha_artifacts[2]["png_file"], "alpha__17__repeat-alpha-c__2.png")
            self.assertEqual(beta_artifacts[1]["png_file"], "beta__17__repeat-beta-b__1.png")
            self.assertTrue(os.path.isfile(alpha_path))
            self.assertTrue(os.path.isfile(beta_path))

    def test_multi_folder_merge_rewrites_composite_source_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_composite_upload_folders(temp_dir)
            args = SimpleNamespace(test_upload_folders=folders, test_root_folder=temp_dir, test_id="merged")

            merged_configs, folder_graphs = main.transform_grafana_configs(configs, args)

            panel = merged_configs[0].panels[0]
            composite_panel = merged_configs[0].panels[1]
            composite_artifact = composite_panel.artifacts[1]
            source = composite_artifact["composite"]["sources"][0]
            shifted_path = os.path.join(folder_graphs, "demo", composite_artifact["png_file"])

            self.assertTrue(os.path.isfile(shifted_path))

        self.assertEqual(composite_artifact["timestamp_id"], 1)
        self.assertEqual(composite_artifact["png_file"], "demo__composite-overview__1.png")
        self.assertEqual(source["png_file"], "demo__17__1.png")
        self.assertEqual(source["artifact_id"], panel.artifacts[1]["artifact_id"])

    def test_multi_folder_merge_shifts_new_panel_links_and_variant_source_timestamp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first_folder = os.path.join(temp_dir, "one")
            second_folder = os.path.join(temp_dir, "two")
            os.makedirs(os.path.join(first_folder, "demo"))
            os.makedirs(os.path.join(second_folder, "demo"))
            with open(os.path.join(first_folder, "demo", "demo__17__0.png"), "wb") as image_file:
                image_file.write(b"png")
            with open(os.path.join(second_folder, "demo", "demo__18__0.png"), "wb") as image_file:
                image_file.write(b"png")
            configs = [
                GrafanaConfigUploader("demo", {
                    "charts_path": os.path.join(first_folder, "demo"), "full_links": ["dash-a"], "snapshot_urls": [],
                    "timestamps": [{"time_tag": "a", "id_time": 0, "start_time_timestamp": 1, "end_time_timestamp": 2,
                                    "start_time_human": "a", "end_time_human": "b"}],
                    "panels": [{"panel_id": 17, "type": "timeseries", "title": "CPU", "links": ["link-a"],
                                "artifacts": [{"artifact_type": "normal", "timestamp_id": 0, "timestamp_tag": "a",
                                               "render_status": "rendered", "png_file": "demo__17__0.png"}]}],
                }),
                GrafanaConfigUploader("demo", {
                    "charts_path": os.path.join(second_folder, "demo"), "full_links": ["dash-b"], "snapshot_urls": [],
                    "timestamps": [{"time_tag": "b", "id_time": 0, "start_time_timestamp": 3, "end_time_timestamp": 4,
                                    "start_time_human": "c", "end_time_human": "d"}],
                    "panels": [{"panel_id": 18, "type": "timeseries", "title": "Memory", "links": ["link-b"],
                                "artifacts": [{"artifact_type": "variant", "timestamp_id": 0, "timestamp_tag": "b",
                                               "source_timestamp_id": 0, "render_status": "rendered", "png_file": "demo__18__0.png"}]}],
                }),
            ]
            args = SimpleNamespace(test_upload_folders=[first_folder, second_folder], test_root_folder=temp_dir, test_id="merged")

            merged_configs, _ = main.transform_grafana_configs(configs, args)

        first_panel, second_panel = merged_configs[0].panels
        self.assertEqual(first_panel.links, ["link-a"])
        self.assertEqual(second_panel.links, [None, "link-b"])
        self.assertEqual(second_panel.artifacts[0]["timestamp_id"], 1)
        self.assertEqual(second_panel.artifacts[0]["source_timestamp_id"], 1)

    def test_multi_folder_upload_copies_snapshot_json_once_and_ignores_unrelated_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_upload_folders(temp_dir)
            for folder in folders:
                with open(os.path.join(folder, "demo__snapshot.json"), "w", encoding="utf-8") as json_file:
                    json_file.write("{}")
                with open(os.path.join(folder, "notes.txt"), "w", encoding="utf-8") as text_file:
                    text_file.write("ignore")
            args = SimpleNamespace(test_upload_folders=folders, test_root_folder=temp_dir, test_id="merged")

            _, folder_graphs = main.transform_grafana_configs(configs, args)
            root_entries = os.listdir(folder_graphs)

        self.assertEqual(root_entries.count("demo__snapshot.json"), 1)
        self.assertNotIn("notes.txt", root_entries)

    def test_transform_grafana_configs_sanitizes_test_id_in_output_folder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folders, configs = self.make_upload_folders(temp_dir)
            args = SimpleNamespace(
                test_upload_folders=folders,
                test_root_folder=temp_dir,
                test_id=r"Release / 1.1 Гбит\с",
            )

            _, folder_graphs = main.transform_grafana_configs(configs, args)

        self.assertEqual(os.path.dirname(folder_graphs), temp_dir)
        self.assertIn(sanitize_run_folder_segment(args.test_id), os.path.basename(folder_graphs))

    def test_build_test_folder_sanitizes_test_id(self):
        args = SimpleNamespace(test_root_folder=r"C:\tmp", test_id=r"Release / 1.1 Гбит\с")

        folder = _build_test_folder(args)

        self.assertEqual(os.path.dirname(folder), args.test_root_folder)
        self.assertIn(sanitize_run_folder_segment(args.test_id), os.path.basename(folder))

    def make_composite_upload_folders(self, temp_dir):
        folders = []
        configs = []
        for folder_name, tag in (("one", "a"), ("two", "b")):
            folder = os.path.join(temp_dir, folder_name)
            charts_path = os.path.join(folder, "demo")
            os.makedirs(charts_path)
            for png_file in ("demo__17__0.png", "demo__composite-overview__0.png"):
                with open(os.path.join(charts_path, png_file), "wb") as image_file:
                    image_file.write(b"png")
            folders.append(folder)
            configs.append(self.make_composite_upload_config(charts_path, tag))
        return folders, configs

    @staticmethod
    def make_composite_upload_config(charts_path, tag):
        return GrafanaConfigUploader("demo", {
            "charts_path": charts_path,
            "full_links": [f"dashboard-{tag}"],
            "snapshot_urls": [],
            "timestamps": [{
                "time_tag": tag,
                "id_time": 0,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            }],
            "panels": [
                {"panel_id": 17, "type": "timeseries", "title": "CPU", "links": [f"link-{tag}"],
                 "artifacts": [{"artifact_type": "normal", "timestamp_id": 0, "timestamp_tag": tag,
                                "render_status": "rendered", "png_file": "demo__17__0.png",
                                "artifact_id": f"stale-source-{tag}", "order_index": 99}]},
                {"panel_id": 0, "type": "composite", "title": "Overview", "links": [None],
                 "artifacts": [{"artifact_type": "composite", "timestamp_id": 0, "timestamp_tag": tag,
                                "render_status": "rendered", "png_file": "demo__composite-overview__0.png",
                                "artifact_id": f"stale-composite-{tag}", "order_index": 100,
                                "composite": {"name": "overview", "sources": [{"panel_id": 17,
                                    "png_file": "demo__17__0.png", "artifact_id": f"stale-source-{tag}"}]}}]},
            ],
        })

    def test_get_yaml_files_returns_only_yaml_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = os.path.join(temp_dir, "keep.yaml")
            yml_path = os.path.join(temp_dir, "skip.yml")
            txt_path = os.path.join(temp_dir, "skip.txt")

            for path in (yaml_path, yml_path, txt_path):
                with open(path, "w", encoding="utf-8") as file_obj:
                    file_obj.write("data")

            result = main.get_yaml_files(temp_dir)

        self.assertEqual(result, [yaml_path])

    def test_process_grafana_dashboard_authenticates_downloads_and_uploads(self):
        grafana_manager_class = Mock()
        grafana_manager = grafana_manager_class.return_value
        grafana_manager.charts_path = "graphs/demo"
        grafana_config = SimpleNamespace(dash_title="Demo dashboard")
        args = SimpleNamespace(
            confluence_login="user",
            confluence_password="secret",
            timestamps=["ts1"],
            only_graphs=False,
        )
        confluence_manager = Mock()

        with patch.dict(main.process_grafana_dashboard.__globals__, {"GrafanaManager": grafana_manager_class}):
            main.process_grafana_dashboard(grafana_config, "test-folder", args, confluence_manager)

        grafana_manager_class.assert_called_once_with(config=grafana_config)
        grafana_manager.authenticate.assert_called_once_with("user", "secret")
        grafana_manager.download_charts.assert_called_once_with(test_folder="test-folder", timestamps=["ts1"])
        confluence_manager.upload_charts.assert_called_once_with("graphs/demo")

    def test_process_grafana_dashboard_skips_upload_when_only_graphs_enabled(self):
        grafana_manager_class = Mock()
        grafana_manager = grafana_manager_class.return_value
        grafana_manager.charts_path = "graphs/demo"
        grafana_config = SimpleNamespace(dash_title="Demo dashboard")
        args = SimpleNamespace(
            confluence_login="user",
            confluence_password="secret",
            timestamps=["ts1"],
            only_graphs=True,
        )
        confluence_manager = Mock()

        with patch.dict(main.process_grafana_dashboard.__globals__, {"GrafanaManager": grafana_manager_class}):
            main.process_grafana_dashboard(grafana_config, "test-folder", args, confluence_manager)

        grafana_manager.download_charts.assert_called_once_with(test_folder="test-folder", timestamps=["ts1"])
        confluence_manager.upload_charts.assert_not_called()

    def test_process_grafana_dashboard_logs_failures_without_raising(self):
        grafana_manager_class = Mock()
        grafana_manager_class.return_value.authenticate.side_effect = RuntimeError("boom")
        grafana_config = SimpleNamespace(dash_title="Broken dashboard")
        args = SimpleNamespace(
            confluence_login="user",
            confluence_password="secret",
            timestamps=["ts1"],
            only_graphs=False,
        )

        helper_globals = main.process_grafana_dashboard.__globals__
        with patch.dict(helper_globals, {"GrafanaManager": grafana_manager_class}):
            with self.assertLogs(helper_globals["logger"], level="ERROR") as logs:
                main.process_grafana_dashboard(grafana_config, "test-folder", args, Mock())

        self.assertTrue(any("Failed to process dashboard Broken dashboard: boom" in message for message in logs.output))

    def test_run_uses_patchable_manager_and_process_globals(self):
        args = self.make_run_args()
        grafana_config = SimpleNamespace(dash_title="Demo dashboard")
        grafana_manager_class = Mock()
        grafana_manager_class.load_grafana_config.return_value = [grafana_config]
        confluence_manager_class = Mock()
        confluence_manager = confluence_manager_class.return_value
        process_dashboard = Mock()

        run_globals = main.run.__globals__
        with patch.dict(run_globals, {
            "ConfluenceManager": confluence_manager_class,
            "GrafanaManager": grafana_manager_class,
            "process_grafana_dashboard": process_dashboard,
        }):
            main.run(args)

        test_folder = process_dashboard.call_args.args[1]
        grafana_manager_class.load_grafana_config.assert_called_once_with("config.yaml")
        process_dashboard.assert_called_once_with(grafana_config, test_folder, args, confluence_manager)
        confluence_manager.upload_charts.assert_called_once_with(test_folder, [['.json', 'application/json']])
        confluence_manager.update_page_content.assert_called_once_with(
            [grafana_config], args.timestamps, args.graph_width, test_folder,
        )

    def test_run_applies_global_playwright_options_to_grafana_configs(self):
        args = self.make_run_args()
        args.playwright_browser = "chromium"
        args.playwright_browser_channel = "chrome"
        args.playwright_browser_executable_path = "C:/Browsers/chrome.exe"
        grafana_config = SimpleNamespace(dash_title="Demo dashboard")
        grafana_manager_class = Mock()
        grafana_manager_class.load_grafana_config.return_value = [grafana_config]
        process_dashboard = Mock()

        with patch.dict(main.run.__globals__, {
            "ConfluenceManager": Mock(),
            "GrafanaManager": grafana_manager_class,
            "process_grafana_dashboard": process_dashboard,
        }):
            main.run(args)

        self.assertEqual(grafana_config.playwright_browser, "chromium")
        self.assertEqual(grafana_config.playwright_browser_channel, "chrome")
        self.assertEqual(grafana_config.playwright_browser_executable_path, "C:/Browsers/chrome.exe")

    def test_create_confluence_manager_propagates_all_confluence_options(self):
        manager_class = Mock()
        args = SimpleNamespace(
            confluence_login="user",
            confluence_password="secret",
            confluence_token="token",
            confluence_page_id=123,
            confluence_upload_threads=4,
            wiki_url="https://wiki.example.test",
            confluence_verify_ssl=False,
            confluence_upload_delay=1.5,
            confluence_upload_rate_per_second=2.5,
            confluence_retry=True,
            confluence_retry_count=7,
            confluence_retry_delay=3.0,
            confluence_retry_backoff_multiplier=2.0,
            confluence_retry_max_delay=30.0,
            confluence_retry_jitter=0.25,
            confluence_continue_on_error=True,
        )

        with patch.dict(_create_confluence_manager.__globals__, {"ConfluenceManager": manager_class}):
            result = _create_confluence_manager(args)

        self.assertIs(result, manager_class.return_value)
        manager_class.assert_called_once_with(
            login="user",
            password="secret",
            token="token",
            page_id=123,
            upload_threads=4,
            wiki_url="https://wiki.example.test",
            verify_ssl=False,
            upload_delay=1.5,
            upload_rate_per_second=2.5,
            retry_enabled=True,
            retry_count=7,
            retry_delay=3.0,
            retry_backoff_multiplier=2.0,
            retry_max_delay=30.0,
            retry_jitter=0.25,
            continue_on_error=True,
        )
