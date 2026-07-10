import os
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from grafconflux.grafana import ConfigurationError, GrafanaConfigDownloader, GrafanaManager


class TestGrafanaConfigLoading(unittest.TestCase):
    def write_config(self, content: str, raw: bool = False):
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(content if raw else self.new_config(content))
        self.addCleanup(temp_dir.cleanup)
        return config_path

    @staticmethod
    def new_config(dashboards_yaml: str) -> str:
        if dashboards_yaml.lstrip().startswith("settings:"):
            return dashboards_yaml
        return "settings: {}\ndashboards:\n" + textwrap.indent(dashboards_yaml, "  ")

    def test_rejects_legacy_top_level_dashboards(self):
        config_path = self.write_config(
            "legacy_dashboard:\n"
            "  dash_title: Legacy\n"
            "  grafana_url: https://grafana.example\n",
            raw=True,
        )

        with self.assertRaisesRegex(ConfigurationError, "Legacy top-level dashboard YAML format"):
            GrafanaManager.load_grafana_config(config_path)

    def test_loads_new_format_dashboards_when_settings_present(self):
        config_path = self.write_config(
            "settings:\n"
            "  wiki_url: https://wiki.example\n"
            "dashboards:\n"
            "  new_dashboard:\n"
            "    dash_title: New\n"
            "    grafana_url: https://grafana.example\n"
        )

        configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0].name, "new_dashboard")
        self.assertEqual(configs[0].dash_title, "New")

    def test_loads_new_format_dashboards_when_settings_are_absent(self):
        config_path = self.write_config(
            "dashboards:\n"
            "  new_dashboard:\n"
            "    dash_title: New\n"
            "    grafana_url: https://grafana.example\n",
            raw=True,
        )

        configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0].name, "new_dashboard")

    def test_default_grafana_credentials_must_be_mapping(self):
        config_path = self.write_config(
            "settings: {}\n"
            "default_grafana_credentials: prod\n"
            "dashboards:\n"
            "  demo:\n"
            "    dash_title: Demo\n",
            raw=True,
        )

        with self.assertRaisesRegex(ConfigurationError, "default_grafana_credentials"):
            GrafanaManager.load_grafana_config(config_path)

    def test_default_grafana_credentials_allow_dashboard_session_override(self):
        config_path = self.write_config(
            "settings: {}\n"
            "default_grafana_credentials:\n"
            "  grafana_url: https://grafana.example\n"
            "  token: env:GRAFANA_TOKEN\n"
            "dashboards:\n"
            "  demo:\n"
            "    dash_title: Demo\n"
            "    session_mode: isolated\n",
            raw=True,
        )

        with patch.dict(os.environ, {"GRAFANA_TOKEN": "token"}):
            config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.config_source, "default_credentials")
        self.assertEqual(config.session_mode, "isolated")
        self.assertIsNone(config.session_key)

    def test_grafana_url_with_subpath_is_normalized(self):
        config = GrafanaConfigDownloader(
            "demo",
            {"dash_title": "Dashboard", "grafana_url": "https://grafana.example/grafana/"},
        )

        self.assertEqual(config.grafana_origin, "https://grafana.example")
        self.assertEqual(config.grafana_app_path, "/grafana")
        self.assertEqual(config.grafana_base_url, "https://grafana.example/grafana")

    def test_auth_url_loads_without_changing_grafana_base_url(self):
        config = GrafanaConfigDownloader(
            "demo",
            {
                "dash_title": "Dashboard",
                "grafana_url": "https://grafana.example/grafana",
                "auth_url": "https://auth.example/bootstrap?target=grafana",
            },
        )

        self.assertEqual(config.auth_url, "https://auth.example/bootstrap?target=grafana")
        self.assertEqual(config.grafana_base_url, "https://grafana.example/grafana")

    def test_missing_grafana_url_is_rejected(self):
        with self.assertRaisesRegex(ConfigurationError, "grafana_url"):
            GrafanaConfigDownloader("demo", {"dash_title": "Dashboard"})

    def test_removed_grafana_url_keys_are_rejected(self):
        for removed_key in ("host", "nginx_prefix", "login_url"):
            with self.subTest(removed_key=removed_key):
                config = {"dash_title": "Dashboard", "grafana_url": "https://grafana.example", removed_key: "legacy"}
                with self.assertRaisesRegex(ConfigurationError, removed_key):
                    GrafanaConfigDownloader("demo", config)

    def test_invalid_grafana_url_shape_is_rejected(self):
        for value in ("grafana.example", "https://grafana.example/grafana?orgId=1", "https://grafana.example/#/d/x"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ConfigurationError, "grafana_url"):
                    GrafanaConfigDownloader("demo", {"dash_title": "Dashboard", "grafana_url": value})

    def test_grafana_and_auth_urls_reject_userinfo_without_leaking_values(self):
        configs = [
            {"dash_title": "Dashboard", "grafana_url": "https://user:secret@grafana.example"},
            {
                "dash_title": "Dashboard",
                "grafana_url": "https://grafana.example",
                "auth_url": "https://user:secret@auth.example/bootstrap",
            },
        ]

        for config in configs:
            with self.subTest(config=config):
                with self.assertRaisesRegex(ConfigurationError, "userinfo") as context:
                    GrafanaConfigDownloader("demo", config)
                self.assertNotIn("secret", str(context.exception))

    def test_sensitive_query_values_are_redacted_in_url_validation_errors(self):
        with self.assertRaises(ConfigurationError) as context:
            GrafanaConfigDownloader("demo", {
                "dash_title": "Dashboard",
                "grafana_url": "https://grafana.example/grafana?token=secret&orgId=1",
            })

        self.assertIn("token=REDACTED", str(context.exception))
        self.assertNotIn("secret", str(context.exception))

    def test_rejects_settings_without_dashboards(self):
        config_path = self.write_config(
            "settings:\n"
            "  wiki_url: https://wiki.example\n",
            raw=True,
        )

        with self.assertRaisesRegex(ConfigurationError, "non-empty top-level 'dashboards' mapping"):
            GrafanaManager.load_grafana_config(config_path)

    def test_applies_documented_default_values(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertTrue(config.render)
        self.assertEqual(config.threads, 4)
        self.assertEqual(config.width, 1920)
        self.assertEqual(config.height, 1080)
        self.assertEqual(config.orgId, 1)
        self.assertFalse(config.download_collapse_panels)
        self.assertFalse(config.download_collapsed_rows)
        self.assertEqual(config.disable_graph_types, [])
        self.assertIsNone(config.playwright_browser)
        self.assertIsNone(config.playwright_browser_channel)
        self.assertIsNone(config.playwright_browser_executable_path)
        self.assertEqual(config.screenshot_readiness.network_idle_ms, 750)
        self.assertEqual(config.screenshot_readiness.no_network_grace_ms, 1000)
        self.assertEqual(config.screenshot_readiness.min_settle_ms, 200)
        self.assertEqual(config.screenshot_readiness.poll_interval_ms, 100)
        self.assertFalse(config.screenshot_readiness.strict_datasource_fragments)

    def test_static_datasource_var_object_normalizes_to_value(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  vars:\n"
            "    ds: {is_datasource: true, value: Prometheus}\n"
            "    region: us\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.vars, {"ds": "Prometheus", "region": "us"})
        self.assertEqual(config.datasource_vars, {"ds": "Prometheus"})

    def test_static_var_object_requires_datasource_value(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  vars:\n"
            "    ds: {is_datasource: true}\n"
        )

        with self.assertRaisesRegex(ConfigurationError, "vars.ds.value"):
            GrafanaManager.load_grafana_config(config_path)

    def test_loads_dashboard_playwright_browser_options(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  playwright_browser: chromium\n"
            "  playwright_browser_channel: chrome\n"
            "  playwright_browser_executable_path: C:/Browsers/chrome.exe\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.playwright_browser, "chromium")
        self.assertEqual(config.playwright_browser_channel, "chrome")
        self.assertEqual(config.playwright_browser_executable_path, "C:/Browsers/chrome.exe")

    def test_loads_dashboard_screenshot_readiness_options(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  screenshot_readiness:\n"
            "    network_idle_ms: 500\n"
            "    no_network_grace_ms: 250\n"
            "    min_settle_ms: 50\n"
            "    poll_interval_ms: 25\n"
            "    strict_datasource_fragments: true\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.screenshot_readiness.network_idle_ms, 500)
        self.assertEqual(config.screenshot_readiness.no_network_grace_ms, 250)
        self.assertEqual(config.screenshot_readiness.min_settle_ms, 50)
        self.assertEqual(config.screenshot_readiness.poll_interval_ms, 25)
        self.assertTrue(config.screenshot_readiness.strict_datasource_fragments)

    def test_new_download_collapsed_rows_config_key_loads(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  download_collapsed_rows: true\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)

    def test_legacy_download_collapse_panels_alias_still_loads(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  download_collapse_panels: true\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)

    def test_new_download_collapsed_rows_overrides_legacy_alias(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  download_collapsed_rows: false\n"
            "  download_collapse_panels: true\n"
        )

        with self.assertLogs("grafconflux.grafana", level="WARNING"):
            config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertFalse(config.download_collapsed_rows)
        self.assertFalse(config.download_collapse_panels)

    def test_conflicting_collapsed_row_keys_emit_warning(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  download_collapsed_rows: true\n"
            "  download_collapse_panels: false\n"
        )

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            GrafanaManager.load_grafana_config(config_path)

        self.assertTrue(any("download_collapsed_rows overrides legacy" in message for message in logs.output))

    def test_download_hidden_panels_is_rejected_as_unsupported_config(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  download_hidden_panels: true\n"
        )

        with self.assertRaisesRegex(ValueError, "download_hidden_panels.*unsupported in phase 1"):
            GrafanaManager.load_grafana_config(config_path)

    def test_collapsed_row_validation_preserves_attributes_messages_and_logs(self):
        hidden_config = {"dash_title": "Dashboard", "grafana_url": "https://grafana.example", "download_hidden_panels": True}

        with self.assertRaises(ConfigurationError) as context:
            GrafanaConfigDownloader("demo", hidden_config)

        self.assertEqual(
            str(context.exception),
            "dashboards.demo.download_hidden_panels: invalid value=True, expected unsupported in phase 1, "
            "suggested fix: remove this key and use download_collapsed_rows",
        )

        alias_config = {"dash_title": "Dashboard", "grafana_url": "https://grafana.example", "download_collapse_panels": True}
        with self.assertLogs("grafconflux.grafana", level="INFO") as logs:
            config = GrafanaConfigDownloader("demo", alias_config)

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)
        self.assertIn("Using legacy config key download_collapse_panels=True", "\n".join(logs.output))

    def test_config_validation_groups_preserve_constructor_visible_defaults(self):
        config = GrafanaConfigDownloader("demo", {"dash_title": "Dashboard", "grafana_url": "https://grafana.example"})

        self.assertFalse(config.download_collapsed_rows)
        self.assertFalse(config.download_collapse_panels)
        self.assertEqual(config.panel_filtering.mode, "include_all_except_excluded")
        self.assertFalse(config.enable_repeating_panels)
        self.assertEqual(config.repeating_panels, [])
        self.assertTrue(config.collect_no_data_panels)
        self.assertEqual(config.no_data_preflight.timeout, 10)
        self.assertEqual(config.snapshot_mode, "ui")
        self.assertTrue(config.snapshot_fallback_to_ui)
        self.assertEqual(config.snapshot_expires, 0)

    def test_invalid_panel_filtering_mode_is_rejected(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    mode: selected_only\n"
        )

        with self.assertRaisesRegex(ValueError, "panel_filtering.mode.*selected_only.*include_only_selected"):
            GrafanaManager.load_grafana_config(config_path)

    def test_invalid_panel_filtering_regex_is_rejected(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    exclude_panels:\n"
            "      title_regex:\n"
            "        - '('\n"
        )

        with self.assertRaisesRegex(ValueError, "exclude_panels.title_regex\[0\].*invalid value"):
            GrafanaManager.load_grafana_config(config_path)

    def test_invalid_panel_filtering_typed_title_shape_is_rejected(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    include_panels:\n"
            "      titles:\n"
            "        - Packet Drops: timeseries\n"
            "          Speed: stat\n"
        )

        with self.assertRaisesRegex(ValueError, r"include_panels.titles\[0\].*one-item mapping"):
            GrafanaManager.load_grafana_config(config_path)

    def test_include_only_selected_requires_include_selectors(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    mode: include_only_selected\n"
            "    exclude_panels:\n"
            "      ids: [1]\n"
        )

        with self.assertRaisesRegex(ValueError, "include_only_selected.*include selectors"):
            GrafanaManager.load_grafana_config(config_path)

    def test_typed_title_mapping_is_rejected_for_row_selectors(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    mode: include_only_selected\n"
            "    include_rows:\n"
            "      titles:\n"
            "        - Production: timeseries\n"
        )

        with self.assertRaisesRegex(ValueError, "include_rows.titles\[0\].*one-item mapping"):
            GrafanaManager.load_grafana_config(config_path)

    def test_include_panels_rename_shapes_load(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    mode: include_only_selected\n"
            "    include_panels:\n"
            "      titles:\n"
            "        - Total drops:\n"
            "            rename: Общие дропы\n"
            "        - Packet Drops:\n"
            "            type: timeseries\n"
            "            rename: some_name\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.panel_filtering.include_panels.inline_renames[("Total drops", None)], "Общие дропы")
        self.assertEqual(config.panel_filtering.include_panels.inline_renames[("Packet Drops", "timeseries")], "some_name")

    def test_exclude_panels_rename_is_rejected(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  panel_filtering:\n"
            "    exclude_panels:\n"
            "      titles:\n"
            "        - Packet Drops:\n"
            "            rename: bad\n"
        )

        with self.assertRaisesRegex(ValueError, "exclude_panels.titles\[0\].*one-item mapping"):
            GrafanaManager.load_grafana_config(config_path)

    def test_rename_panels_loads(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  rename_panels:\n"
            "    - id: 20\n"
            "      rename: X\n"
            "    - title: Packet Drops\n"
            "      type: timeseries\n"
            "      rename: Y\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.rename_panels[0]["id"], 20)
        self.assertEqual(config.rename_panels[1]["type"], "timeseries")

    def test_invalid_repeating_config_field_shapes_are_rejected(self):
        cases = [
            ("  enable_repeating_panels: 'true'\n", "enable_repeating_panels.*expected bool"),
            ("  repeating_panels:\n    panel_id: 17\n", "repeating_panels.*expected list"),
            ("  repeating_panels:\n    - 17\n", "repeating_panels.*expected list"),
        ]

        for body, pattern in cases:
            with self.subTest(body=body):
                config_path = self.write_config(
                    "dashboard:\n"
                    "  dash_title: Dashboard\n"
                    "  grafana_url: https://grafana.example\n"
                    f"{body}"
                )

                with self.assertRaisesRegex(ValueError, pattern):
                    GrafanaManager.load_grafana_config(config_path)

    def test_snapshot_validation_keeps_constructor_visible_defaults(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  snapshot_store_dashboard_json: false\n"
            "  snapshot_expires: 0\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(config.snapshot_mode, "ui")
        self.assertTrue(config.snapshot_fallback_to_ui)
        self.assertEqual(config.snapshot_expires, 0)
        self.assertFalse(config.snapshot_store_dashboard_json)

    def test_backup_dashboard_links_loads_from_yaml(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  backup_dashboard_links:\n"
            "    - https://backup.example/d/demo?orgId=1\n"
            "    - https://backup2.example/d/demo\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertEqual(
            config.backup_dashboard_links,
            [
                "https://backup.example/d/demo?orgId=1",
                "https://backup2.example/d/demo",
            ],
        )

    def test_backup_dashboard_links_rejects_non_string_items(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  backup_dashboard_links:\n"
            "    - https://backup.example/d/demo\n"
            "    - 17\n"
        )

        with self.assertRaisesRegex(ValueError, "backup_dashboard_links.*expected list\\[str\\]"):
            GrafanaManager.load_grafana_config(config_path)

    def test_backup_dashboard_links_rejects_scalar_value(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  grafana_url: https://grafana.example\n"
            "  backup_dashboard_links: https://backup.example/d/demo\n"
        )

        with self.assertRaisesRegex(ValueError, "backup_dashboard_links.*expected list\\[str\\]"):
            GrafanaManager.load_grafana_config(config_path)
