import os
import tempfile
import textwrap
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import GrafanaConfigDownloader, GrafanaManager


class TestGrafanaDashboardLookupConfig(unittest.TestCase):
    def write_config(self, content: str) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(self.new_config(content))
        self.addCleanup(temp_dir.cleanup)
        return config_path

    @staticmethod
    def new_config(dashboards_yaml: str) -> str:
        if dashboards_yaml.lstrip().startswith("settings:"):
            return dashboards_yaml
        return "settings: {}\ndashboards:\n" + textwrap.indent(dashboards_yaml, "  ")

    def test_config_loading_accepts_dashboard_uid(self) -> None:
        config_path = self.write_config(
            "dash_by_uid:\n"
            "  grafana_url: https://grafana.example\n"
            "  dashboard_uid: uid-123\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.dashboard_uid, "uid-123")
        self.assertIsNone(config.dash_title)

    def test_config_loading_accepts_folder_uid(self) -> None:
        config_path = self.write_config(
            "dash_by_title:\n"
            "  grafana_url: https://grafana.example\n"
            "  dash_title: Payments\n"
            "  folder_uid: folder-123\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.dash_title, "Payments")
        self.assertEqual(config.folder_uid, "folder-123")

    def test_title_only_dashboard_config_remains_valid(self) -> None:
        config_path = self.write_config(
            "legacy_dashboard:\n"
            "  grafana_url: https://grafana.example\n"
            "  dash_title: Legacy\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.dash_title, "Legacy")
        self.assertIsNone(config.dashboard_uid)

    def test_dashboard_uid_and_title_are_configuration_error(self) -> None:
        config_path = self.write_config(
            "bad_dashboard:\n"
            "  grafana_url: https://grafana.example\n"
            "  dashboard_uid: uid-123\n"
            "  dash_title: Payments\n"
        )

        with self.assertRaisesRegex(ValueError, "dashboard_uid and dash_title"):
            GrafanaManager.load_grafana_config(config_path)

    def test_folder_fields_without_dash_title_are_configuration_error(self) -> None:
        cases = [
            "  dashboard_uid: uid-123\n  folder_uid: folder-123\n",
            "  folder: Production\n",
        ]

        for config_body in cases:
            with self.subTest(config_body=config_body):
                config_path = self.write_config(
                    "bad_dashboard:\n"
                    "  grafana_url: https://grafana.example\n"
                    f"{config_body}"
                )
                with self.assertRaisesRegex(ValueError, "folder.*dash_title"):
                    GrafanaManager.load_grafana_config(config_path)


class TestGrafanaDashboardLookup(unittest.TestCase):
    def create_manager(self, **overrides) -> GrafanaManager:
        config = {
            "grafana_url": "https://grafana.example",
            "dash_title": "Payments",
        }
        config.update(overrides)
        return GrafanaManager(GrafanaConfigDownloader("demo", config))

    def mock_search(self, manager: GrafanaManager, status_code: int, payload: list[dict]) -> None:
        response = Mock(status_code=status_code)
        response.json = Mock(return_value=payload)
        manager.session.get = Mock(return_value=response)

    def test_uid_lookup_uses_dash_db_type_and_dashboard_uids_param(self) -> None:
        manager = self.create_manager(dashboard_uid="uid-123", dash_title=None)
        self.mock_search(manager, 200, [{"uid": "uid-123", "title": "Payments", "url": "/d/uid-123/payments"}])

        uid, _ = manager.get_dashboard_uid()

        self.assertEqual(uid, "uid-123")
        self.assertEqual(
            manager.session.get.call_args.kwargs["params"],
            {"type": "dash-db", "dashboardUIDs": "uid-123"},
        )

    def test_uid_lookup_returns_matching_result(self) -> None:
        manager = self.create_manager(dashboard_uid="uid-123", dash_title=None)
        self.mock_search(manager, 200, [{"uid": "uid-123", "title": "Payments", "url": "/d/uid-123/payments"}])

        uid, url = manager.get_dashboard_uid()

        self.assertEqual((uid, url), ("uid-123", "/d/uid-123/payments"))

    def test_dashboard_url_from_api_is_normalized_to_app_route(self) -> None:
        cases = [
            "/d/uid-123/payments",
            "/grafana/d/uid-123/payments",
            "https://grafana.example/grafana/d/uid-123/payments",
        ]
        for api_url in cases:
            with self.subTest(api_url=api_url):
                manager = self.create_manager(
                    grafana_url="https://grafana.example/grafana",
                    dashboard_uid="uid-123",
                    dash_title=None,
                )
                self.mock_search(manager, 200, [{"uid": "uid-123", "title": "Payments", "url": api_url}])

                _, url = manager.get_dashboard_uid()

                self.assertEqual(url, "/d/uid-123/payments")

    def test_uid_lookup_raises_when_not_found(self) -> None:
        manager = self.create_manager(dashboard_uid="uid-123", dash_title=None)
        self.mock_search(manager, 200, [])

        with self.assertRaisesRegex(ValueError, "dashboard uid.*not found"):
            manager.get_dashboard_uid()

    def test_uid_lookup_raises_on_duplicate_unexpected_result(self) -> None:
        manager = self.create_manager(dashboard_uid="uid-123", dash_title=None)
        self.mock_search(manager, 200, [
            {"uid": "uid-123", "title": "Payments", "url": "/d/uid-123/a"},
            {"uid": "uid-123", "title": "Payments Copy", "url": "/d/uid-123/b"},
        ])

        with self.assertRaisesRegex(ValueError, "ambiguous|multiple"):
            manager.get_dashboard_uid()

    def test_title_lookup_uses_dash_db_type_and_query_param(self) -> None:
        manager = self.create_manager()
        self.mock_search(manager, 200, [{"uid": "uid-1", "title": "Payments", "url": "/d/uid-1/payments"}])

        manager.get_dashboard_uid()

        self.assertEqual(
            manager.session.get.call_args.kwargs["params"],
            {"type": "dash-db", "query": "Payments"},
        )

    def test_title_exact_single_match_returns_result(self) -> None:
        manager = self.create_manager()
        self.mock_search(manager, 200, [
            {"uid": "uid-other", "title": "Other", "url": "/d/uid-other/other"},
            {"uid": "uid-1", "title": "Payments", "url": "/d/uid-1/payments"},
        ])

        uid, url = manager.get_dashboard_uid()

        self.assertEqual((uid, url), ("uid-1", "/d/uid-1/payments"))

    def test_title_lookup_raises_when_no_exact_match(self) -> None:
        manager = self.create_manager()
        self.mock_search(manager, 200, [{"uid": "uid-1", "title": "payments", "url": "/d/uid-1/payments"}])

        with self.assertRaisesRegex(ValueError, "not found"):
            manager.get_dashboard_uid()

    def test_title_lookup_raises_on_ambiguity_and_logs_it(self) -> None:
        manager = self.create_manager()
        self.mock_search(manager, 200, [
            {"uid": "uid-1", "title": "Payments", "folderTitle": "Production", "url": "/d/uid-1/a"},
            {"uid": "uid-2", "title": "Payments", "folderTitle": "Staging", "url": "/d/uid-2/b"},
        ])

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            with self.assertRaisesRegex(ValueError, "ambiguous.*uid-1.*Production.*uid-2.*Staging"):
                manager.get_dashboard_uid()

        self.assertTrue(any("Dashboard lookup ambiguity" in message for message in logs.output))

    def test_folder_exact_folder_title_filter(self) -> None:
        manager = self.create_manager(folder="Production")
        self.mock_search(manager, 200, [
            {"uid": "uid-1", "title": "Payments", "folderTitle": "Staging", "url": "/d/uid-1/a"},
            {"uid": "uid-2", "title": "Payments", "folderTitle": "Production", "url": "/d/uid-2/b"},
        ])

        uid, url = manager.get_dashboard_uid()

        self.assertEqual((uid, url), ("uid-2", "/d/uid-2/b"))

    def test_folder_uid_is_sent_as_api_param(self) -> None:
        manager = self.create_manager(folder_uid="folder-123")
        self.mock_search(manager, 200, [{"uid": "uid-1", "title": "Payments", "folderUid": "folder-123", "url": "/d/uid-1"}])

        manager.get_dashboard_uid()

        self.assertEqual(
            manager.session.get.call_args.kwargs["params"],
            {"type": "dash-db", "query": "Payments", "folderUIDs": "folder-123"},
        )

    def test_title_and_folder_matching_is_case_sensitive(self) -> None:
        manager = self.create_manager(folder="Production")
        self.mock_search(manager, 200, [
            {"uid": "uid-1", "title": "payments", "folderTitle": "Production", "url": "/d/uid-1"},
            {"uid": "uid-2", "title": "Payments", "folderTitle": "production", "url": "/d/uid-2"},
        ])

        with self.assertRaisesRegex(ValueError, "not in folder"):
            manager.get_dashboard_uid()

    def test_http_error_remains_connection_error(self) -> None:
        manager = self.create_manager()
        self.mock_search(manager, 500, [])

        with self.assertRaises(ConnectionError):
            manager.get_dashboard_uid()

    def test_lookup_mode_is_logged(self) -> None:
        manager = self.create_manager(dashboard_uid="uid-123", dash_title=None)
        self.mock_search(manager, 200, [{"uid": "uid-123", "title": "Payments", "url": "/d/uid-123"}])

        with self.assertLogs("grafconflux.grafana", level="INFO") as logs:
            manager.get_dashboard_uid()

        self.assertTrue(any("Dashboard lookup using dashboard_uid=uid-123" in message for message in logs.output))


class TestGrafanaLookupIntegration(unittest.TestCase):
    def create_manager(self, **overrides) -> GrafanaManager:
        config = {"grafana_url": "https://grafana.example", "dash_title": "Payments"}
        config.update(overrides)
        return GrafanaManager(GrafanaConfigDownloader("demo", config))

    def create_timestamps(self) -> list[GrafanaTimeDownloader]:
        return [GrafanaTimeDownloader("tag__&from=1700000000&to=1700003600", 0, "UTC")]

    def test_download_charts_calls_lookup_before_get_panels(self) -> None:
        manager = self.create_manager()
        calls = []
        manager.get_dashboard_uid = Mock(side_effect=lambda: calls.append("lookup") or ("uid-1", "/d/uid-1/payments"))
        manager.get_panels = Mock(side_effect=lambda timestamps: calls.append("panels") or [])

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.download_charts(temp_dir, self.create_timestamps())

        self.assertEqual(calls, ["lookup", "panels"])

    def test_lookup_failure_stops_before_panel_download(self) -> None:
        manager = self.create_manager()
        manager.get_dashboard_uid = Mock(side_effect=ValueError("ambiguous"))
        manager.get_panels = Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                manager.download_charts(temp_dir, self.create_timestamps())

        manager.get_panels.assert_not_called()

    def test_successful_lookup_links_use_grafana_base_url_without_double_subpath(self) -> None:
        manager = self.create_manager(grafana_url="https://grafana.example/grafana")
        manager.get_dashboard_uid = Mock(return_value=("uid-1", "/d/uid-1/payments-from-search"))
        manager.get_panels = Mock(return_value=[])

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.download_charts(temp_dir, self.create_timestamps())

        self.assertEqual(manager.dashboard_uid, "uid-1")
        self.assertEqual(manager.dashboard_url, "/d/uid-1/payments-from-search")
        self.assertTrue(manager.config.full_links[0].startswith("https://grafana.example/grafana/d/uid-1/payments-from-search"))
        self.assertNotIn("/grafana/grafana/", manager.config.full_links[0])

    def test_grafana_url_subpath_applies_to_search_endpoint(self) -> None:
        manager = self.create_manager(grafana_url="https://grafana.example/grafana")
        response = Mock(status_code=200)
        response.json = Mock(return_value=[{"uid": "uid-1", "title": "Payments", "url": "/grafana/d/uid-1"}])
        manager.session.get = Mock(return_value=response)

        _, dashboard_url = manager.get_dashboard_uid()

        self.assertEqual(manager.session.get.call_args.args[0], "https://grafana.example/grafana/api/search")
        self.assertEqual(dashboard_url, "/d/uid-1")

    def test_upload_only_run_does_not_load_grafana_config_or_lookup(self) -> None:
        args = SimpleNamespace(
            confluence_login="user",
            confluence_password="secret",
            confluence_page_id=1,
            confluence_upload_threads=1,
            wiki_url="https://wiki.example",
            confluence_verify_ssl=True,
            confluence_upload_delay=0,
            confluence_upload_rate_per_second=None,
            confluence_retry=True,
            confluence_retry_count=1,
            confluence_retry_delay=0,
            confluence_retry_backoff_multiplier=1.0,
            confluence_retry_max_delay=None,
            confluence_retry_jitter=0,
            confluence_continue_on_error=False,
            test_upload_folders=None,
            graph_width=900,
            test_root_folder="unused",
            test_id="unused",
            timestamps=[],
            only_graphs=False,
        )
        with tempfile.TemporaryDirectory() as upload_dir:
            args.test_upload_folders = [upload_dir]
            self.write_upload_metadata(upload_dir)
            confluence_manager = Mock()

            with patch("grafconflux.orchestration.ConfluenceManager", return_value=confluence_manager):
                with patch("grafconflux.orchestration.GrafanaManager.load_grafana_config", side_effect=AssertionError):
                    from grafconflux.orchestration import run

                    run(args)

        confluence_manager.update_page_content.assert_called_once()

    def write_upload_metadata(self, upload_dir: str) -> None:
        metadata_path = os.path.join(upload_dir, "demo.yaml")
        with open(metadata_path, "w", encoding="utf-8") as metadata:
            metadata.write(
                "name: demo\n"
                f"charts_path: {upload_dir}\n"
                "full_links:\n"
                "  - https://grafana.example/d/uid-1/demo\n"
                "snapshot_urls: []\n"
                "timestamps:\n"
                "  - time_tag: tag\n"
                "    id_time: 0\n"
                "    start_time_timestamp: 1700000000000\n"
                "    end_time_timestamp: 1700003600000\n"
                "    start_time_human: '2023/11/14 22:13:20'\n"
                "    end_time_human: '2023/11/14 23:13:20'\n"
                "panels:\n"
                "  - panel_id: 1\n"
                "    type: graph\n"
                "    title: CPU\n"
                "    links:\n"
                "      - https://grafana.example/d-solo/uid-1/demo?panelId=1\n"
            )


class TestGrafanaLookupCliApiSmoke(unittest.TestCase):
    def write_config(self, content: str) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(content)
        self.addCleanup(temp_dir.cleanup)
        return config_path

    def test_existing_cli_args_still_parse_without_new_flags(self) -> None:
        config_path = self.write_config(
            "settings: {}\n"
            "dashboards:\n"
            "  demo:\n"
            "    dash_title: Demo\n"
            "    grafana_url: https://grafana.example\n"
        )

        from grafconflux.args_parser import ArgsParser

        args = ArgsParser([
            "--config", config_path,
            "--wiki_url", "https://wiki.example",
            "--confluence_page_id", "1",
            "--confluence_login", "user",
            "--confluence_password", "secret",
            "--timestamps", "tag__&from=1700000000&to=1700003600",
            "--only_graphs",
        ])

        self.assertTrue(args.only_graphs)

    def test_uid_only_yaml_can_be_loaded_through_library_config_path(self) -> None:
        config_path = self.write_config(
            "settings: {}\n"
            "dashboards:\n"
            "  uid_dashboard:\n"
            "    grafana_url: https://grafana.example\n"
            "    dashboard_uid: uid-123\n"
        )

        configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(configs[0].dashboard_uid, "uid-123")

    def test_public_exports_remain_unchanged(self) -> None:
        import grafconflux

        self.assertEqual(
            grafconflux.__all__,
            [
                "GrafConfluxRunOptions",
                "options_from_config_file",
                "parse_timestamps",
                "run",
                "run_from_config_file",
            ],
        )


if __name__ == "__main__":
    unittest.main()
