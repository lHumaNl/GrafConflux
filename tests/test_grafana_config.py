import os
import tempfile
import unittest

from grafconflux.grafana import ConfigurationError, GrafanaConfigDownloader, GrafanaManager


class TestGrafanaConfigLoading(unittest.TestCase):
    def write_config(self, content: str):
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(content)
        self.addCleanup(temp_dir.cleanup)
        return config_path

    def test_loads_legacy_top_level_dashboards(self):
        config_path = self.write_config(
            "legacy_dashboard:\n"
            "  dash_title: Legacy\n"
            "  host: https://grafana.example\n"
        )

        configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0].name, "legacy_dashboard")
        self.assertEqual(configs[0].dash_title, "Legacy")

    def test_loads_new_format_dashboards_when_settings_present(self):
        config_path = self.write_config(
            "settings:\n"
            "  wiki_url: https://wiki.example\n"
            "dashboards:\n"
            "  new_dashboard:\n"
            "    dash_title: New\n"
            "    host: https://grafana.example\n"
        )

        configs = GrafanaManager.load_grafana_config(config_path)

        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0].name, "new_dashboard")
        self.assertEqual(configs[0].dash_title, "New")

    def test_applies_documented_default_values(self):
        config_path = self.write_config(
            "legacy_dashboard:\n"
            "  dash_title: Legacy\n"
            "  host: https://grafana.example\n"
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

    def test_new_download_collapsed_rows_config_key_loads(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  host: https://grafana.example\n"
            "  download_collapsed_rows: true\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)

    def test_legacy_download_collapse_panels_alias_still_loads(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  host: https://grafana.example\n"
            "  download_collapse_panels: true\n"
        )

        config = GrafanaManager.load_grafana_config(config_path)[0]

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)

    def test_new_download_collapsed_rows_overrides_legacy_alias(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
            "  download_hidden_panels: true\n"
        )

        with self.assertRaisesRegex(ValueError, "download_hidden_panels.*unsupported in phase 1"):
            GrafanaManager.load_grafana_config(config_path)

    def test_collapsed_row_validation_preserves_attributes_messages_and_logs(self):
        hidden_config = {"dash_title": "Dashboard", "host": "https://grafana.example", "download_hidden_panels": True}

        with self.assertRaises(ConfigurationError) as context:
            GrafanaConfigDownloader("demo", hidden_config)

        self.assertEqual(
            str(context.exception),
            "dashboards.demo.download_hidden_panels: invalid value=True, expected unsupported in phase 1, "
            "suggested fix: remove this key and use download_collapsed_rows",
        )

        alias_config = {"dash_title": "Dashboard", "host": "https://grafana.example", "download_collapse_panels": True}
        with self.assertLogs("grafconflux.grafana", level="INFO") as logs:
            config = GrafanaConfigDownloader("demo", alias_config)

        self.assertTrue(config.download_collapsed_rows)
        self.assertTrue(config.download_collapse_panels)
        self.assertIn("Using legacy config key download_collapse_panels=True", "\n".join(logs.output))

    def test_config_validation_groups_preserve_constructor_visible_defaults(self):
        config = GrafanaConfigDownloader("demo", {"dash_title": "Dashboard", "host": "https://grafana.example"})

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
            "  host: https://grafana.example\n"
            "  panel_filtering:\n"
            "    mode: selected_only\n"
        )

        with self.assertRaisesRegex(ValueError, "panel_filtering.mode.*selected_only.*include_only_selected"):
            GrafanaManager.load_grafana_config(config_path)

    def test_invalid_panel_filtering_regex_is_rejected(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
                    "  host: https://grafana.example\n"
                    f"{body}"
                )

                with self.assertRaisesRegex(ValueError, pattern):
                    GrafanaManager.load_grafana_config(config_path)

    def test_snapshot_validation_keeps_constructor_visible_defaults(self):
        config_path = self.write_config(
            "dashboard:\n"
            "  dash_title: Dashboard\n"
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
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
            "  host: https://grafana.example\n"
            "  backup_dashboard_links: https://backup.example/d/demo\n"
        )

        with self.assertRaisesRegex(ValueError, "backup_dashboard_links.*expected list\\[str\\]"):
            GrafanaManager.load_grafana_config(config_path)
