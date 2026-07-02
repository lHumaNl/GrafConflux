import os
import tempfile
import unittest
from unittest.mock import Mock
from urllib.parse import parse_qs, urlparse

import grafconflux.grafana as grafana_facade
import grafconflux.grafana_models as grafana_models
import grafconflux.grafana_rendering as grafana_rendering
from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import (
    ConfigurationError,
    GrafanaConfigDownloader,
    GrafanaConfigUploader,
    GrafanaManager,
    Panel,
    PanelDefinition,
    PanelDescriptor,
    PanelRenderTask,
    build_dashboard_url_params,
    build_panel_url_params,
    extract_dashboard_panels,
)


class TestGrafanaPanels(unittest.TestCase):
    def create_manager(self, **overrides):
        config = {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
        }
        config.update(overrides)
        return GrafanaManager(GrafanaConfigDownloader("demo", config))

    def create_timestamps(self, count=2):
        return [
            GrafanaTimeDownloader(f"tag{index}__&from=1700000000&to=1700003600", index, "UTC")
            for index in range(count)
        ]

    def create_screenshot_task(self):
        return PanelRenderTask(
            Panel(17, "timeseries", "CPU", 1),
            self.create_timestamps(count=1)[0],
            artifact={},
        )

    def create_distinct_timestamps(self):
        return [
            GrafanaTimeDownloader("tag0__&from=1700000000&to=1700003600", 0, "UTC"),
            GrafanaTimeDownloader("tag1__&from=1700007200&to=1700010800", 1, "UTC"),
        ]

    def get_panel_ids(self, dashboard_panels, **config_overrides):
        manager = self.create_manager(**config_overrides)
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {"panels": dashboard_panels}
        })))

        return [panel.panel_id for panel in manager.get_panels(self.create_timestamps(count=1))]

    def get_repeating_panels(self, dashboard, **config_overrides):
        config_overrides.setdefault("enable_repeating_panels", True)
        manager = self.create_manager(**config_overrides)
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": dashboard
        })))
        return manager, manager.get_panels(self.create_timestamps(count=2))

    def get_auto_repeating_panels(self, dashboard, **config_overrides):
        manager = self.create_manager(**config_overrides)
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": dashboard
        })))
        return manager, manager.get_panels(self.create_timestamps(count=2))

    def repeating_dashboard(self):
        return {
            "templating": {"list": [
                {"name": "host", "options": [
                    {"text": "All", "value": "$__all"},
                    {"text": "Prod One", "value": "prod-1"},
                    {"text": "Prod Two", "value": "prod-2"},
                    {"text": "DB One", "value": "db-1"},
                ], "current": {"value": "prod-1"}},
                {"name": "cluster", "current": {"value": "cluster-a"}},
            ]},
            "panels": [
                {"id": 17, "type": "timeseries", "title": "CPU by host", "repeat": "host"},
                {"id": 21, "type": "stat", "title": "Memory"},
            ],
        }

    def test_extract_panels_is_recursive_and_skips_collapsed_by_default(self):
        manager = self.create_manager()
        nested_panels = [
            {"id": 1, "type": "timeseries", "title": "Top panel"},
            {
                "collapsed": False,
                "panels": [
                    {"id": 2, "type": "stat", "title": "Nested panel"},
                    {"collapsed": True, "panels": [{"id": 3, "type": "graph", "title": "Hidden panel"}]},
                ],
            },
        ]

        extracted = manager.extract_panels(nested_panels)

        self.assertEqual([panel["id"] for panel in extracted], [1, 2])

    def test_extract_panels_includes_collapsed_panels_when_enabled(self):
        manager = self.create_manager(download_collapse_panels=True)
        nested_panels = [
            {"collapsed": True, "panels": [{"id": 10, "type": "graph", "title": "Collapsed panel"}]}
        ]

        extracted = manager.extract_panels(nested_panels)

        self.assertEqual([panel["id"] for panel in extracted], [10])

    def test_extract_panels_includes_collapsed_rows_with_new_key(self):
        manager = self.create_manager(download_collapsed_rows=True)
        nested_panels = [
            {"collapsed": True, "panels": [{"id": 11, "type": "graph", "title": "Collapsed panel"}]}
        ]

        extracted = manager.extract_panels(nested_panels)

        self.assertEqual([panel["id"] for panel in extracted], [11])

    def test_extract_panels_marks_collapsed_row_metadata(self):
        nested_panels = [
            {
                "type": "row",
                "title": "Collapsed Row",
                "collapsed": True,
                "panels": [{"id": 12, "type": "graph", "title": "Nested panel"}],
            }
        ]

        extracted = extract_dashboard_panels(nested_panels, include_collapsed_rows=True)

        self.assertEqual(extracted[0]["row_title"], "Collapsed Row")
        self.assertTrue(extracted[0]["from_collapsed_row"])

    def test_get_panels_filters_disabled_graph_types_and_sets_links_per_timestamp(self):
        manager = self.create_manager(disable_graph_types=["stat"])
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "panels": [
                    {"id": 1, "type": "timeseries", "title": "Allowed"},
                    {"id": 2, "type": "stat", "title": "Filtered"},
                    {"panels": [{"id": 3, "type": "graph", "title": "Nested allowed"}]},
                ]
            }
        })))

        timestamps = self.create_timestamps(count=3)
        panels = manager.get_panels(timestamps)

        self.assertEqual([panel.panel_id for panel in panels], [1, 3])
        for panel in panels:
            self.assertEqual(len(panel.links), 3)
            self.assertEqual(panel.links, [None, None, None])
        self.assertEqual(manager.session.get.call_args.kwargs["timeout"], manager.config.timeout)

    def test_include_only_selected_by_panel_ids(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "CPU"},
                {"id": 2, "type": "graph", "title": "Memory"},
                {"id": 3, "type": "stat", "title": "Latency"},
            ],
            panel_filtering={"mode": "include_only_selected", "include_panels": {"ids": [1, 3]}},
        )

        self.assertEqual(panel_ids, [1, 3])

    def test_exclude_panels_by_title(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "CPU"},
                {"id": 2, "type": "graph", "title": "Debug Stat"},
            ],
            panel_filtering={"exclude_panels": {"titles": ["Debug Stat"]}},
        )

        self.assertEqual(panel_ids, [1])

    def test_exclude_panels_by_title_regex(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "CPU"},
                {"id": 2, "type": "graph", "title": "temporary debug"},
            ],
            panel_filtering={"exclude_panels": {"title_regex": [".*temporary.*"]}},
        )

        self.assertEqual(panel_ids, [1])

    def test_include_panels_by_mixed_string_and_typed_titles(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "Speed"},
                {"id": 2, "type": "stat", "title": "Packet Drops"},
                {"id": 3, "type": "timeseries", "title": "Packet Drops"},
            ],
            panel_filtering={"mode": "include_only_selected", "include_panels": {"titles": ["Speed", {"Packet Drops": "timeseries"}]}},
        )

        self.assertEqual(panel_ids, [1, 3])

    def test_exclude_panels_by_typed_title(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "Packet Drops"},
                {"id": 2, "type": "stat", "title": "Packet Drops"},
            ],
            panel_filtering={"exclude_panels": {"titles": [{"Packet Drops": "timeseries"}]}},
        )

        self.assertEqual(panel_ids, [2])

    def test_include_panels_inline_rename_sets_display_title(self):
        manager = self.create_manager(
            panel_filtering={
                "mode": "include_only_selected",
                "include_panels": {"titles": [{"Total drops": {"rename": "Общие дропы"}}]},
            },
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {"panels": [{"id": 1, "type": "timeseries", "title": "Total drops"}]}
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].title, "Total drops")
        self.assertEqual(panels[0].display_title, "Общие дропы")

    def test_rename_panels_precedence_and_var_substitution(self):
        manager = self.create_manager(
            vars={"iface": ["xe0", "xe1"]},
            rename_panels=[
                {"title": "Packet Drops", "rename": "By title"},
                {"title": "Packet Drops", "type": "timeseries", "rename": "By type"},
                {"id": 20, "rename": "Траф $iface"},
            ],
            panel_filtering={
                "mode": "include_only_selected",
                "include_panels": {"titles": [
                    {"Packet Drops": {"type": "timeseries", "rename": "Inline"}},
                    "Packet Drops",
                    "DPI traffic (iface: $iface)",
                ]},
            },
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {"panels": [
                {"id": 10, "type": "timeseries", "title": "Packet Drops"},
                {"id": 11, "type": "stat", "title": "Packet Drops"},
                {"id": 20, "type": "timeseries", "title": "DPI traffic (iface: $iface)"},
            ]}
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([panel.display_title for panel in panels], ["By type", "By title", "Траф xe0, xe1"])

    def test_rename_panels_without_filtering_still_applies(self):
        manager = self.create_manager(rename_panels=[{"title": "CPU", "rename": "CPU ${missing}"}])
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {"panels": [{"id": 1, "type": "timeseries", "title": "CPU"}]}
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].display_title, "CPU ${missing}")

    def test_panel_title_vars_fall_back_to_dashboard_current_values(self):
        manager = self.create_manager()
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "templating": {"list": [{"name": "iface", "current": {"value": "All"}}]},
                "panels": [{"id": 1, "type": "timeseries", "title": "DPI traffic (iface: $iface)"}],
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].display_title, "DPI traffic (iface: All)")

    def test_panel_title_vars_normalize_grafana_all_sentinel(self):
        manager = self.create_manager(vars={"iface": "$__all"})
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "templating": {"list": [{"name": "iface", "current": {"value": "$__all"}}]},
                "panels": [{"id": 1, "type": "timeseries", "title": "DPI traffic (iface: $iface)"}],
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].display_title, "DPI traffic (iface: All)")

    def test_config_vars_override_dashboard_current_values_in_panel_titles(self):
        manager = self.create_manager(vars={"iface": "Configured"})
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "templating": {"list": [{"name": "iface", "current": {"value": "Dashboard"}}]},
                "panels": [{"id": 1, "type": "timeseries", "title": "DPI traffic (iface: $iface)"}],
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].display_title, "DPI traffic (iface: Configured)")

    def test_panel_title_vars_fall_back_to_dashboard_default_and_current_text(self):
        manager = self.create_manager()
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "templating": {"list": [
                    {"name": "iface", "default": "DefaultIface"},
                    {"name": "host", "current": {"text": "Host Text"}},
                ]},
                "panels": [{"id": 1, "type": "timeseries", "title": "Traffic $iface on ${host}"}],
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].display_title, "Traffic DefaultIface on Host Text")

    def test_include_rows_filters_nested_panels_by_row_title(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "Outside"},
                {"id": 10, "type": "row", "title": "Production", "panels": [
                    {"id": 2, "type": "graph", "title": "Errors"},
                    {"id": 3, "type": "stat", "title": "Latency"},
                ]},
            ],
            panel_filtering={"mode": "include_only_selected", "include_rows": {"titles": ["Production"]}},
        )

        self.assertEqual(panel_ids, [2, 3])

    def test_include_rows_filters_nested_panels_by_row_id_and_regex(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 10, "type": "row", "title": "Production", "panels": [
                    {"id": 1, "type": "graph", "title": "Errors"},
                ]},
                {"id": 20, "type": "row", "title": "Critical API", "panels": [
                    {"id": 2, "type": "graph", "title": "Latency"},
                ]},
            ],
            panel_filtering={
                "mode": "include_only_selected",
                "include_rows": {"ids": [10], "title_regex": ["^Critical.*"]},
            },
        )

        self.assertEqual(panel_ids, [1, 2])

    def test_exclude_rows_removes_all_nested_panels(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "Outside"},
                {"id": 10, "type": "row", "title": "Deprecated", "panels": [
                    {"id": 2, "type": "graph", "title": "Errors"},
                ]},
            ],
            panel_filtering={"exclude_rows": {"titles": ["Deprecated"]}},
        )

        self.assertEqual(panel_ids, [1])

    def test_disable_graph_types_still_applies_before_filtering(self):
        panel_ids = self.get_panel_ids(
            [
                {"id": 1, "type": "timeseries", "title": "CPU"},
                {"id": 2, "type": "stat", "title": "Latency"},
            ],
            disable_graph_types=["stat"],
            panel_filtering={"mode": "include_only_selected", "include_panels": {"ids": [1, 2]}},
        )

        self.assertEqual(panel_ids, [1])

    def test_title_selector_matching_multiple_panels_logs_warning(self):
        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            panel_ids = self.get_panel_ids(
                [
                    {"id": 1, "type": "timeseries", "title": "CPU"},
                    {"id": 2, "type": "graph", "title": "CPU"},
                ],
                panel_filtering={"mode": "include_only_selected", "include_panels": {"titles": ["CPU"]}},
            )

        self.assertEqual(panel_ids, [1, 2])
        self.assertTrue(any("matched_multiple" in message for message in logs.output))

    def test_missing_panel_title_is_not_treated_as_row_selector_title(self):
        with self.assertNoLogs("grafconflux.grafana", level="WARNING"):
            panel_ids = self.get_panel_ids(
                [
                    {"id": 1, "type": "timeseries"},
                    {"id": 2, "type": "graph", "title": "Row"},
                ],
                panel_filtering={"mode": "include_only_selected", "include_panels": {"titles": ["Row"]}},
            )

        self.assertEqual(panel_ids, [2])

    def test_selector_matching_no_panels_logs_warning(self):
        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            panel_ids = self.get_panel_ids(
                [{"id": 1, "type": "timeseries", "title": "CPU"}],
                panel_filtering={"mode": "include_only_selected", "include_panels": {"ids": [999]}},
            )

        self.assertEqual(panel_ids, [])
        self.assertTrue(any("matched_nothing" in message for message in logs.output))

    def test_filtering_runs_after_collapsed_row_resolution(self):
        dashboard_panels = [
            {"id": 10, "type": "row", "title": "Production", "collapsed": True, "panels": [
                {"id": 1, "type": "graph", "title": "Errors"},
            ]},
        ]
        filtering = {"mode": "include_only_selected", "include_rows": {"titles": ["Production"]}}

        hidden_ids = self.get_panel_ids(dashboard_panels, download_collapsed_rows=False, panel_filtering=filtering)
        visible_ids = self.get_panel_ids(dashboard_panels, download_collapsed_rows=True, panel_filtering=filtering)

        self.assertEqual(hidden_ids, [])
        self.assertEqual(visible_ids, [1])

    def test_panel_metadata_persists_row_context_grid_pos_and_row_id(self):
        manager = self.create_manager()
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "panels": [
                    {"id": 10, "type": "row", "title": "Production", "panels": [
                        {"id": 1, "type": "graph", "title": "Errors", "gridPos": {"x": 0, "y": 1}},
                    ]},
                ]
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].row_title, "Production")
        self.assertEqual(panels[0].row_id, 10)
        self.assertEqual(panels[0].grid_pos, {"x": 0, "y": 1})

    def test_get_panels_skips_collapsed_subtree_when_disabled(self):
        manager = self.create_manager(download_collapsed_rows=False)
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "panels": [
                    {"id": 1, "type": "graph", "title": "Visible"},
                    {"collapsed": True, "panels": [{"id": 2, "type": "graph", "title": "Hidden"}]},
                ]
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([panel.panel_id for panel in panels], [1])

    def test_get_panels_carries_collapsed_row_metadata_when_enabled(self):
        manager = self.create_manager(download_collapsed_rows=True)
        manager.dashboard_uid = "dashboard-uid"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={
            "dashboard": {
                "panels": [
                    {
                        "type": "row",
                        "title": "Collapsed Row",
                        "collapsed": True,
                        "panels": [{"id": 2, "type": "graph", "title": "Hidden"}],
                    }
                ]
            }
        })))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual(panels[0].row_title, "Collapsed Row")
        self.assertTrue(panels[0].from_collapsed_row)

    def test_authenticate_with_login_url_uses_timeout(self):
        manager = self.create_manager(login="user", password="secret", login_url="https://grafana.example/login")
        manager.session.get = Mock(return_value=Mock(status_code=200))

        manager.authenticate("ignored", "ignored")

        self.assertEqual(manager.session.get.call_args.kwargs["timeout"], manager.config.timeout)

    def test_authenticate_with_password_uses_nginx_prefix_login_url(self):
        manager = self.create_manager(login="user", password="secret", nginx_prefix="/monitoring")
        manager.session.post = Mock(return_value=Mock(status_code=200))

        manager.authenticate("ignored", "ignored")

        self.assertEqual(manager.session.post.call_args.args[0], "https://grafana.example/monitoring/login")

    def test_domain_auth_requires_confluence_login_and_password(self):
        manager = self.create_manager(domain=True)

        with self.assertRaisesRegex(ValueError, "Confluence login/password"):
            manager.authenticate(None, None)

    def test_reauthenticate_uses_grafana_credentials_without_confluence_creds(self):
        manager = self.create_manager(login="grafana-user", password="grafana-secret")
        manager.session.post = Mock(return_value=Mock(status_code=200))

        reauthenticated = manager._reauthenticate_grafana()

        self.assertTrue(reauthenticated)
        payload = manager.session.post.call_args.kwargs["data"]
        self.assertIn('"user": "grafana-user"', payload)
        self.assertIn('"password": "grafana-secret"', payload)

    def test_reauthenticate_requires_confluence_credentials_for_domain_auth(self):
        manager = self.create_manager(domain=True)
        manager.session.post = Mock(return_value=Mock(status_code=200))

        with self.assertLogs("grafconflux.grafana", level="ERROR") as logs:
            reauthenticated = manager._reauthenticate_grafana()

        self.assertFalse(reauthenticated)
        manager.session.post.assert_not_called()
        self.assertIn("required credentials", "\n".join(logs.output))

    def test_get_dashboard_uid_uses_timeout(self):
        manager = self.create_manager()
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=[
            {"title": "Dashboard", "uid": "uid-1", "url": "/d/uid-1/dashboard"}
        ])))

        uid, url = manager.get_dashboard_uid()

        self.assertEqual(uid, "uid-1")
        self.assertEqual(url, "/d/uid-1/dashboard")
        self.assertEqual(manager.session.get.call_args.kwargs["timeout"], manager.config.timeout)

    def test_extract_dashboard_panels_helper_matches_collapsed_behavior(self):
        nested_panels = [
            {"id": 1, "type": "timeseries"},
            {"collapsed": True, "panels": [{"id": 2, "type": "graph"}]},
        ]

        self.assertEqual([panel["id"] for panel in extract_dashboard_panels(nested_panels)], [1])
        self.assertEqual([panel["id"] for panel in extract_dashboard_panels(nested_panels, True)], [1, 2])

    def test_url_param_helpers_include_theme_timezone_and_variables(self):
        timestamp = self.create_timestamps(count=1)[0]

        panel_params = build_panel_url_params(7, timestamp, 2, True, "UTC", {"env": "prod"})
        dashboard_params = build_dashboard_url_params(timestamp, 2, {"env": "prod"})

        self.assertEqual(panel_params["theme"], "light")
        self.assertEqual(panel_params["tz"], "UTC")
        self.assertEqual(panel_params["var-env"], "prod")
        self.assertEqual(dashboard_params["var-env"], "prod")

    def test_upload_only_legacy_metadata_gets_default_row_metadata(self):
        config = {
            "panels": [{"panel_id": 1, "type": "graph", "title": "CPU", "links": ["https://grafana.example/panel"]}],
            "full_links": ["https://grafana.example/dashboard"],
            "snapshot_urls": [],
            "charts_path": "unused",
            "timestamps": [
                {
                    "time_tag": "tag",
                    "id_time": 0,
                    "start_time_timestamp": 1700000000000,
                    "end_time_timestamp": 1700003600000,
                    "start_time_human": "2023/11/14 22:13:20",
                    "end_time_human": "2023/11/14 23:13:20",
                }
            ],
        }

        uploader = GrafanaConfigUploader("demo", config)

        self.assertIsNone(uploader.panels[0].row_title)
        self.assertFalse(uploader.panels[0].from_collapsed_row)

    def test_upload_only_new_metadata_loads_optional_row_fields(self):
        config = {
            "panels": [
                {
                    "panel_id": 1,
                    "type": "graph",
                    "title": "CPU",
                    "links": ["https://grafana.example/panel"],
                    "row_title": "Production",
                    "row_id": 10,
                    "grid_pos": {"x": 0, "y": 1},
                }
            ],
            "full_links": ["https://grafana.example/dashboard"],
            "snapshot_urls": [],
            "charts_path": "unused",
            "timestamps": [
                {
                    "time_tag": "tag",
                    "id_time": 0,
                    "start_time_timestamp": 1700000000000,
                    "end_time_timestamp": 1700003600000,
                    "start_time_human": "2023/11/14 22:13:20",
                    "end_time_human": "2023/11/14 23:13:20",
                }
            ],
        }

        uploader = GrafanaConfigUploader("demo", config)

        self.assertEqual(uploader.panels[0].row_title, "Production")
        self.assertEqual(uploader.panels[0].row_id, 10)
        self.assertEqual(uploader.panels[0].grid_pos, {"x": 0, "y": 1})

    def test_internal_repeating_foundation_models_are_available(self):
        definition = PanelDefinition(panel_id=17, graph_type="timeseries", title="CPU", repeat="host")
        task = PanelRenderTask(panel=Panel(17, "timeseries", "CPU", 1), timestamp=self.create_timestamps(1)[0])

        self.assertEqual(definition.repeat, "host")
        self.assertEqual(task.panel.panel_id, 17)

    def test_grafana_model_imports_remain_facade_compatible(self):
        self.assertIs(grafana_facade.GrafanaConfigDownloader, grafana_models.GrafanaConfigDownloader)
        self.assertIs(grafana_facade.Panel, grafana_models.Panel)
        self.assertIs(grafana_facade.PanelRenderTask, grafana_models.PanelRenderTask)
        self.assertIs(grafana_facade._SelectorConfig, grafana_models._SelectorConfig)

    def test_grafana_rendering_helpers_remain_facade_compatible(self):
        self.assertIs(grafana_facade.build_dashboard_url_params, grafana_rendering.build_dashboard_url_params)
        self.assertIs(grafana_facade.build_panel_url_params, grafana_rendering.build_panel_url_params)

    def test_resolve_repeating_rules_shim_preserves_planner_behavior(self):
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["a"]}}],
        )
        dashboard = self.repeating_dashboard()
        descriptors = [PanelDescriptor.from_raw_panel(panel) for panel in dashboard["panels"]]

        rules = manager._resolve_repeating_rules(dashboard, descriptors, self.create_timestamps(count=1))

        self.assertEqual(rules[17]["repeat_var"], "host")
        self.assertEqual(rules[17]["values"], ["a"])
        self.assertEqual(rules[17]["values_by_timestamp"], {0: ["a"]})

    def test_append_panel_tasks_shim_preserves_planner_behavior(self):
        manager = self.create_manager(enable_repeating_panels=True)
        manager._render_tasks = []
        descriptor = PanelDescriptor.from_raw_panel({"id": 17, "type": "timeseries", "title": "CPU", "repeat": "host"})
        panel = Panel(17, "timeseries", "CPU", 1)
        rule = {"repeat_var": "host", "values": ["prod/one"], "values_by_timestamp": {0: ["prod/one"]}}

        manager._append_panel_tasks(panel, descriptor, self.create_timestamps(count=1), rule)

        self.assertEqual([task.repeat_value for task in manager.render_tasks], ["prod/one"])
        self.assertEqual(panel.artifacts[0]["repeat_value_slug"], "prod-one")
        self.assertEqual(manager.render_tasks[0].file_name, "demo__17__repeat-prod-one__0.png")

    def test_repeating_panel_manual_values_materialize_render_tasks(self):
        manager, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["a", "b"]}}],
        )

        self.assertEqual([panel.panel_id for panel in panels], [17, 21])
        repeated_panel = panels[0]
        self.assertTrue(repeated_panel.is_repeating)
        self.assertEqual(repeated_panel.repeat_var, "host")
        self.assertEqual(len(repeated_panel.artifacts), 4)
        self.assertEqual([task.repeat_value for task in manager.render_tasks if task.repeat_value], ["a", "b", "a", "b"])

    def test_auto_repeating_panel_materializes_without_explicit_config(self):
        manager, panels = self.get_auto_repeating_panels(self.repeating_dashboard())

        self.assertEqual([panel.panel_id for panel in panels], [17, 21])
        self.assertTrue(panels[0].is_repeating)
        self.assertEqual(panels[0].repeat_var, "host")
        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["prod-1", "prod-1"])
        self.assertEqual([task.repeat_value for task in manager.render_tasks if task.repeat_value], ["prod-1", "prod-1"])

    def test_auto_repeating_row_materializes_child_panel_tasks(self):
        dashboard = {
            "templating": {"list": [{"name": "cluster", "current": {"value": "cluster-a"}}]},
            "panels": [{
                "id": 10,
                "type": "row",
                "title": "Cluster Row",
                "repeat": "cluster",
                "panels": [{"id": 22, "type": "timeseries", "title": "CPU"}],
            }],
        }

        manager, panels = self.get_auto_repeating_panels(dashboard)

        self.assertEqual([panel.panel_id for panel in panels], [22])
        self.assertTrue(panels[0].is_repeating)
        self.assertEqual(panels[0].repeat_var, "cluster")
        self.assertEqual([task.repeat_value for task in manager.render_tasks], ["cluster-a", "cluster-a"])

    def test_auto_repeating_uses_yaml_vars_before_dashboard_values(self):
        _, panels = self.get_auto_repeating_panels(
            self.repeating_dashboard(),
            vars={"host": ["from-config"]},
        )

        self.assertEqual(panels[0].artifacts[0]["repeat_value"], "from-config")

    def test_auto_repeating_uses_dashboard_default_when_current_absent(self):
        dashboard = self.repeating_dashboard()
        del dashboard["templating"]["list"][0]["current"]
        dashboard["templating"]["list"][0]["default"] = "default-host"

        _, panels = self.get_auto_repeating_panels(dashboard)

        self.assertEqual(panels[0].artifacts[0]["repeat_value"], "default-host")

    def test_explicit_repeating_config_overrides_auto_values(self):
        _, panels = self.get_auto_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["manual-host"]}}],
        )

        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["manual-host", "manual-host"])

    def test_repeating_panel_render_task_uses_original_panel_id_and_var_parameter(self):
        manager, _ = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["prod-1"]}}],
        )

        task = manager.render_tasks[0]
        url, params = manager._GrafanaManager__build_panel_url(task.panel, task.timestamp, task.variables)

        self.assertEqual(task.panel.panel_id, 17)
        self.assertIn("/d/", url)
        self.assertEqual(params["panelId"], 17)
        self.assertEqual(params["viewPanel"], 17)
        self.assertEqual(params["var-host"], "prod-1")

    def test_repeating_panel_ignores_runtime_clone_ids(self):
        dashboard = self.repeating_dashboard()
        dashboard["panels"].append({
            "id": 99,
            "type": "timeseries",
            "title": "CPU by host",
            "repeatPanelId": 17,
        })

        manager, panels = self.get_repeating_panels(
            dashboard,
            repeating_panels=[{"title": "CPU by host", "repeat_values": {"mode": "manual", "values": ["prod-1"]}}],
        )

        self.assertEqual([panel.panel_id for panel in panels], [17, 21])
        self.assertNotIn(99, [task.panel.panel_id for task in manager.render_tasks])
        self.assertEqual([task.panel.panel_id for task in manager.render_tasks].count(17), 2)

    def test_repeating_panel_filename_includes_deterministic_repeat_slug(self):
        manager, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["prod/one"]}}],
        )

        repeated_artifact = panels[0].artifacts[0]

        self.assertEqual(manager.render_tasks[0].file_name, repeated_artifact["png_file"])
        self.assertEqual(repeated_artifact["repeat_value_slug"], "prod-one")
        self.assertEqual(repeated_artifact["png_file"], "demo__17__repeat-prod-one__0.png")

    def test_repeating_panel_slug_collision_uses_stable_hash_suffix(self):
        manager, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{
                "panel_id": 17,
                "repeat_values": {"mode": "manual", "values": ["prod/one", "prod one"]},
            }],
        )

        first_artifacts = [
            artifact for artifact in panels[0].artifacts
            if artifact["timestamp_tag"] == "tag0"
        ]
        task_file_names = [
            task.file_name for task in manager.render_tasks
            if task.panel.panel_id == 17 and task.timestamp.time_tag == "tag0"
        ]

        self.assertEqual(
            [(artifact["repeat_value"], artifact["repeat_value_slug"]) for artifact in first_artifacts],
            [("prod/one", "prod-one"), ("prod one", "prod-one-3335cf06")],
        )
        self.assertEqual(
            [artifact["png_file"] for artifact in first_artifacts],
            ["demo__17__repeat-prod-one__0.png", "demo__17__repeat-prod-one-3335cf06__0.png"],
        )
        self.assertEqual(task_file_names, [artifact["png_file"] for artifact in first_artifacts])

    def test_download_chart_render_mode_uses_render_api_params_and_records_fullscreen_link(self):
        manager = self.create_manager(width=800, height=600, timeout=45, tz="UTC", vars={"env": "prod"})
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        timestamp = self.create_timestamps(count=1)[0]
        artifact = {}
        task = PanelRenderTask(
            Panel(17, "timeseries", "CPU", 1),
            timestamp,
            variables={"env": "prod", "host": "web-1"},
            file_name="demo__17__repeat-web-1__0.png",
            artifact=artifact,
        )
        calls = []

        def fake_get(url, **kwargs):
            calls.append((url, kwargs))
            response = Mock(status_code=200, content=b"png-bytes")
            response.raise_for_status = Mock()
            return response

        manager.session.get = Mock(side_effect=fake_get)

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.charts_path = temp_dir
            manager._GrafanaManager__download_chart(task)
            output_file = os.path.join(temp_dir, "demo__17__repeat-web-1__0.png")
            with open(output_file, "rb") as image_file:
                image_bytes = image_file.read()

        render_url, render_kwargs = calls[0]
        link_probe_url, link_probe_kwargs = calls[1]

        self.assertEqual(render_url, "https://grafana.example/render/d-solo/dashboard-uid/dashboard")
        self.assertEqual(render_kwargs["timeout"], 45)
        self.assertNotIn("viewPanel", render_kwargs["params"])
        self.assertEqual(render_kwargs["params"]["panelId"], 17)
        self.assertEqual(render_kwargs["params"]["width"], 800)
        self.assertEqual(render_kwargs["params"]["height"], 600)
        self.assertEqual(render_kwargs["params"]["timeout"], 45)
        self.assertEqual(render_kwargs["params"]["var-host"], "web-1")
        self.assertIn("viewPanel=17", link_probe_url)
        self.assertTrue(link_probe_url.endswith("&fullscreen"))
        self.assertEqual(link_probe_kwargs["timeout"], 45)
        self.assertEqual(task.panel.links[0], link_probe_url)
        self.assertEqual(artifact["link"], link_probe_url)
        self.assertEqual(image_bytes, b"png-bytes")

    def test_download_chart_render_mode_falls_back_to_non_fullscreen_link_probe(self):
        manager = self.create_manager(timeout=45)
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        timestamp = self.create_timestamps(count=1)[0]
        task = PanelRenderTask(Panel(21, "graph", "Memory", 1), timestamp)
        calls = []

        def fake_get(url, **kwargs):
            calls.append((url, kwargs))
            if "/render/d-solo/" in url:
                response = Mock(status_code=200, content=b"png-bytes")
                response.raise_for_status = Mock()
                return response
            if url.endswith("&fullscreen"):
                raise RuntimeError("fullscreen unavailable")
            return Mock(status_code=200)

        manager.session.get = Mock(side_effect=fake_get)

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.charts_path = temp_dir
            manager._GrafanaManager__download_chart(task)
            output_path = os.path.join(temp_dir, "demo__21__0.png")
            self.assertTrue(os.path.exists(output_path))

        fullscreen_probe = calls[1][0]
        fallback_probe = calls[2][0]
        fallback_query = parse_qs(urlparse(fallback_probe).query)

        self.assertTrue(fullscreen_probe.endswith("&fullscreen"))
        self.assertFalse(fallback_probe.endswith("&fullscreen"))
        self.assertEqual(fallback_query["viewPanel"], ["21"])
        self.assertEqual(task.panel.links[0], fallback_probe)

    def test_take_screenshot_first_use_records_fullscreen_route(self):
        manager = self.create_manager(render=False, timeout=45)
        task = self.create_screenshot_task()
        browser = FakeScreenshotBrowser({"https://grafana.example/panel?viewPanel=17&fullscreen": 200})
        manager.thread_local.is_fullscreen = None
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=["/api/ds/query"])

        manager._GrafanaManager__take_screenshot(
            browser,
            task,
            "https://grafana.example/panel?viewPanel=17",
            "panel.png",
        )

        self.assertEqual(browser.visited_urls, ["https://grafana.example/panel?viewPanel=17&fullscreen"])
        self.assertTrue(manager.thread_local.is_fullscreen)
        self.assertEqual(task.panel.links[0], "https://grafana.example/panel?viewPanel=17&fullscreen")
        self.assertEqual(task.artifact["link"], "https://grafana.example/panel?viewPanel=17&fullscreen")
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_take_screenshot_first_use_falls_back_to_non_fullscreen_route(self):
        manager = self.create_manager(render=False, timeout=45)
        task = self.create_screenshot_task()
        final_url = "https://grafana.example/panel?viewPanel=17"
        browser = FakeScreenshotBrowser({f"{final_url}&fullscreen": 500, final_url: 200})
        manager.thread_local.is_fullscreen = None
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=[])

        manager._GrafanaManager__take_screenshot(browser, task, final_url, "panel.png")

        self.assertEqual(browser.visited_urls, [f"{final_url}&fullscreen", final_url])
        self.assertEqual(browser.refresh_count, 1)
        self.assertFalse(manager.thread_local.is_fullscreen)
        self.assertEqual(task.panel.links[0], final_url)
        self.assertEqual(task.artifact["link"], final_url)
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_take_screenshot_resets_context_after_fullscreen_navigation_error(self):
        manager = self.create_manager(render=False, timeout=45)
        task = self.create_screenshot_task()
        final_url = "https://grafana.example/panel?viewPanel=17"
        browser = FakeScreenshotBrowser({f"{final_url}&fullscreen": RuntimeError("connection refused"), final_url: 200})
        manager.thread_local.is_fullscreen = None
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=[])

        manager._GrafanaManager__take_screenshot(browser, task, final_url, "panel.png")

        self.assertEqual(browser.visited_urls, [f"{final_url}&fullscreen", final_url])
        self.assertEqual(browser.refresh_count, 1)
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_take_screenshot_navigation_error_keeps_original_failure_message(self):
        manager = self.create_manager(render=False, timeout=45)
        manager.thread_local.is_fullscreen = True
        task = self.create_screenshot_task()
        final_url = "https://grafana.example/panel?viewPanel=17"
        browser = FakeScreenshotBrowser({f"{final_url}&fullscreen": RuntimeError("connection refused"), final_url: 200})
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=[])

        with self.assertLogs("grafconflux.grafana", level="ERROR") as logs:
            manager._GrafanaManager__take_screenshot(browser, task, final_url, "panel.png")

        self.assertIn("connection refused", "\n".join(logs.output))
        self.assertNotIn("no captured HTTP response", "\n".join(logs.output))
        self.assertEqual(browser.visited_urls, [f"{final_url}&fullscreen", final_url])
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_take_screenshot_reuses_detected_fullscreen_route(self):
        manager = self.create_manager(render=False, timeout=45)
        manager.thread_local.is_fullscreen = True
        task = self.create_screenshot_task()
        final_url = "https://grafana.example/panel?viewPanel=17"
        browser = FakeScreenshotBrowser({f"{final_url}&fullscreen": 200, final_url: 200})
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=[])

        manager._GrafanaManager__take_screenshot(browser, task, final_url, "panel.png")

        self.assertEqual(browser.visited_urls, [f"{final_url}&fullscreen"])
        self.assertTrue(manager.thread_local.is_fullscreen)
        self.assertEqual(task.panel.links[0], f"{final_url}&fullscreen")
        self.assertEqual(task.artifact["link"], f"{final_url}&fullscreen")
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_take_screenshot_reuses_detected_non_fullscreen_route(self):
        manager = self.create_manager(render=False, timeout=45)
        manager.thread_local.is_fullscreen = False
        task = self.create_screenshot_task()
        final_url = "https://grafana.example/panel?viewPanel=17"
        browser = FakeScreenshotBrowser({f"{final_url}&fullscreen": 200, final_url: 200})
        manager._GrafanaManager__get_panel_data_sources = Mock(return_value=[])

        manager._GrafanaManager__take_screenshot(browser, task, final_url, "panel.png")

        self.assertEqual(browser.visited_urls, [final_url])
        self.assertFalse(manager.thread_local.is_fullscreen)
        self.assertEqual(task.panel.links[0], final_url)
        self.assertEqual(task.artifact["link"], final_url)
        self.assertEqual(browser.saved_paths, ["panel.png"])

    def test_normal_panel_filename_format_is_unchanged(self):
        manager, _ = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": ["prod-1"]}}],
        )

        normal_task = [task for task in manager.render_tasks if task.panel.panel_id == 21][0]

        self.assertEqual(normal_task.file_name, "demo__21__0.png")

    def test_repeating_panel_regex_list_uses_or_semantics_and_deduplicates(self):
        _, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{
                "panel_id": 17,
                "repeat_values": {"mode": "regex", "regex": ["^prod-.*", "^db-.*", "^prod-.*"]},
            }],
        )

        values = [artifact["repeat_value"] for artifact in panels[0].artifacts if artifact["timestamp_tag"] == "tag0"]

        self.assertEqual(values, ["prod-1", "prod-2", "db-1"])

    def test_download_chart_browser_mode_closes_browser_in_worker_task(self):
        manager = self.create_manager(render=False)
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        task = self.create_screenshot_task()
        browser = Mock()
        manager._GrafanaManager__init_browser = Mock(return_value=browser)
        manager._GrafanaManager__take_screenshot = Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.charts_path = temp_dir
            manager._GrafanaManager__download_chart(task)

        browser.quit.assert_called_once_with()
        self.assertEqual(manager.browser_list, [])

    def test_download_chart_browser_mode_closes_browser_when_screenshot_fails(self):
        manager = self.create_manager(render=False)
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        task = self.create_screenshot_task()
        browser = Mock()
        manager._GrafanaManager__init_browser = Mock(return_value=browser)
        manager._GrafanaManager__take_screenshot = Mock(side_effect=RuntimeError("boom"))

        with tempfile.TemporaryDirectory() as temp_dir:
            manager.charts_path = temp_dir
            with self.assertRaisesRegex(RuntimeError, "boom"):
                manager._GrafanaManager__download_chart(task)

        browser.quit.assert_called_once_with()

    def test_repeating_panel_regex_string_is_single_pattern_shorthand(self):
        _, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "regex", "regex": "^db-.*"}}],
        )

        values = [artifact["repeat_value"] for artifact in panels[0].artifacts if artifact["timestamp_tag"] == "tag0"]

        self.assertEqual(values, ["db-1"])

    def test_repeating_panel_all_excludes_all_sentinel_and_respects_max_values(self):
        _, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "all"}, "max_values": 3}],
        )

        values = [artifact["repeat_value"] for artifact in panels[0].artifacts if artifact["timestamp_tag"] == "tag0"]

        self.assertEqual(values, ["prod-1", "prod-2", "db-1"])

    def test_repeating_panel_fallback_uses_config_vars_before_current(self):
        _, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            vars={"host": ["from-config"]},
            repeating_panels=[{"panel_id": 17}],
        )

        self.assertEqual(panels[0].artifacts[0]["repeat_value"], "from-config")

    def test_repeating_panel_fallback_uses_dashboard_current(self):
        _, panels = self.get_repeating_panels(
            self.repeating_dashboard(),
            repeating_panels=[{"panel_id": 17}],
        )

        self.assertEqual(panels[0].artifacts[0]["repeat_value"], "prod-1")

    def test_repeating_panel_prometheus_label_values_discovers_all_current_values(self):
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": self.prometheus_repeating_dashboard()})),
            Mock(status_code=200, json=Mock(return_value={"status": "success", "data": [
                "ad", "global", "nad", "postgres",
            ]})),
        ])

        panels = manager.get_panels(self.create_timestamps(count=1))

        values = [artifact["repeat_value"] for artifact in panels[0].artifacts]
        self.assertEqual(values, ["ad", "global", "nad", "postgres"])
        datasource_call = manager.session.get.call_args_list[1]
        self.assertIn("/api/datasources/proxy/uid/prom-main/api/v1/label/database/values", datasource_call.args[0])
        self.assertEqual(datasource_call.kwargs["params"], {
            "match[]": "pg_database_info",
            "start": "1700000000000",
            "end": "1700003600000",
        })
        self.assertEqual(datasource_call.kwargs["timeout"], manager.config.timeout)

    def test_repeating_panel_prometheus_values_resolve_per_timestamp(self):
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": self.prometheus_repeating_dashboard()})),
            Mock(status_code=200, json=Mock(return_value={"status": "success", "data": ["first-range"]})),
            Mock(status_code=200, json=Mock(return_value={"status": "success", "data": ["second-range"]})),
        ])

        panels = manager.get_panels(self.create_distinct_timestamps())

        self.assertEqual(
            [(artifact["timestamp_tag"], artifact["repeat_value"]) for artifact in panels[0].artifacts],
            [("tag0", "first-range"), ("tag1", "second-range")],
        )
        self.assertEqual(manager.session.get.call_count, 3)
        self.assertEqual(manager.session.get.call_args_list[1].kwargs["params"]["start"], "1700000000000")
        self.assertEqual(manager.session.get.call_args_list[2].kwargs["params"]["start"], "1700007200000")

    def test_repeating_panel_prometheus_values_used_when_current_default_and_options_absent(self):
        dashboard = self.prometheus_repeating_dashboard()
        variable = dashboard["templating"]["list"][0]
        variable.pop("current")
        variable.pop("options")
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})),
            Mock(status_code=200, json=Mock(return_value={"status": "success", "data": ["discovered"]})),
        ])

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["discovered"])
        self.assertEqual(manager.session.get.call_count, 2)

    def test_explicit_all_and_regex_modes_do_not_call_datasource_without_options(self):
        for repeat_values in ({"mode": "all"}, {"mode": "regex", "regex": ".*"}):
            with self.subTest(repeat_values=repeat_values):
                manager = self.create_manager(
                    enable_repeating_panels=True,
                    repeating_panels=[{"panel_id": 19, "repeat_values": repeat_values}],
                )
                manager.dashboard_uid = "dashboard-uid"
                manager.dashboard_url = "/d/dashboard-uid/dashboard"
                manager.session.get = Mock(return_value=Mock(
                    status_code=200,
                    json=Mock(return_value={"dashboard": self.prometheus_repeating_dashboard()}),
                ))

                with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
                    panels = manager.get_panels(self.create_timestamps(count=1))

                self.assertEqual(panels, [])
                self.assertEqual(manager.session.get.call_count, 1)
                self.assertTrue(any("variable_values_unresolved" in message for message in logs.output))

    def test_repeating_panel_prometheus_discovery_preserves_current_fallback_on_error(self):
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": self.prometheus_repeating_dashboard()})),
            Mock(status_code=500, json=Mock(return_value={})),
        ])

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["$__all"])

    def test_repeating_panel_unsupported_query_variable_keeps_current_fallback(self):
        dashboard = self.prometheus_repeating_dashboard()
        dashboard["templating"]["list"][0]["query"] = "metrics(database)"
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["$__all"])
        self.assertEqual(manager.session.get.call_count, 1)

    def test_repeating_panel_non_prometheus_query_variable_keeps_current_fallback(self):
        dashboard = self.prometheus_repeating_dashboard()
        dashboard["templating"]["list"][0]["datasource"] = {"type": "loki", "uid": "logs"}
        manager = self.create_manager(
            enable_repeating_panels=True,
            repeating_panels=[{"panel_id": 19}],
        )
        manager.dashboard_uid = "dashboard-uid"
        manager.dashboard_url = "/d/dashboard-uid/dashboard"
        manager.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})))

        panels = manager.get_panels(self.create_timestamps(count=1))

        self.assertEqual([artifact["repeat_value"] for artifact in panels[0].artifacts], ["$__all"])
        self.assertEqual(manager.session.get.call_count, 1)

    def test_panel_and_row_repeat_combination_is_skipped_with_warning(self):
        dashboard = {
            "templating": {"list": [
                {"name": "cluster", "current": {"value": "cluster-a"}},
                {"name": "host", "current": {"value": "host-a"}},
            ]},
            "panels": [{
                "id": 10,
                "type": "row",
                "title": "Cluster Row",
                "repeat": "cluster",
                "panels": [{"id": 22, "type": "timeseries", "title": "CPU", "repeat": "host"}],
            }],
        }

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            manager, panels = self.get_auto_repeating_panels(dashboard)

        self.assertEqual(panels, [])
        self.assertEqual(manager.render_tasks, [])
        self.assertTrue(any("multi_variable_repeat_unsupported" in message for message in logs.output))

    def test_unresolved_repeating_values_skip_panel_without_legacy_fallback(self):
        dashboard = self.repeating_dashboard()
        dashboard["templating"] = {"list": [{"name": "host", "options": []}]}

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            manager, panels = self.get_repeating_panels(dashboard, repeating_panels=[{"panel_id": 17}])

        self.assertEqual([panel.panel_id for panel in panels], [21])
        self.assertEqual([task.panel.panel_id for task in manager.render_tasks], [21, 21])
        self.assertTrue(any("panel_id=17" in message for message in logs.output))
        self.assertTrue(any("variable_values_unresolved" in message for message in logs.output))

    def test_repeating_panel_non_scalar_option_value_warns_and_skips(self):
        dashboard = self.repeating_dashboard()
        dashboard["templating"]["list"][0]["options"] = [
            {"text": "Unsupported", "value": {"host": "bad"}},
            {"text": "Prod One", "value": "prod-1"},
        ]

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            _, panels = self.get_repeating_panels(
                dashboard,
                repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "all"}}],
            )

        values = [artifact["repeat_value"] for artifact in panels[0].artifacts if artifact["timestamp_tag"] == "tag0"]
        self.assertEqual(values, ["prod-1"])
        self.assertTrue(any("panel_id=17" in message for message in logs.output))
        self.assertTrue(any("repeat_var=host" in message for message in logs.output))
        self.assertTrue(any("non-scalar" in message for message in logs.output))

    def test_repeating_panel_repeat_var_mismatch_fails_validation(self):
        with self.assertRaisesRegex(ConfigurationError, "repeat_var.*cluster.*host"):
            self.get_repeating_panels(
                self.repeating_dashboard(),
                repeating_panels=[{"panel_id": 17, "repeat_var": "cluster"}],
            )

    def test_repeating_panel_invalid_manual_values_fail_validation(self):
        with self.assertRaisesRegex(ConfigurationError, "repeat_values.values\[0\].*non-empty string"):
            self.get_repeating_panels(
                self.repeating_dashboard(),
                repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "manual", "values": [7]}}],
            )

    def test_repeating_panel_invalid_regex_reports_yaml_path(self):
        with self.assertRaisesRegex(ConfigurationError, "repeat_values.regex\[1\].*valid regex"):
            self.get_repeating_panels(
                self.repeating_dashboard(),
                repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "regex", "regex": ["ok", "("]}}],
            )

    def test_repeating_panel_empty_regex_list_fails_validation(self):
        with self.assertRaisesRegex(ConfigurationError, "repeat_values.regex.*non-empty"):
            self.get_repeating_panels(
                self.repeating_dashboard(),
                repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "regex", "regex": []}}],
            )

    def test_repeating_panel_selector_must_resolve_exactly_one_source_panel(self):
        with self.assertRaisesRegex(ConfigurationError, "resolved 0 panels.*exactly 1"):
            self.get_repeating_panels(self.repeating_dashboard(), repeating_panels=[{"panel_id": 999}])

    def test_repeating_panel_title_regex_multiple_matches_fail_validation(self):
        dashboard = self.repeating_dashboard()
        dashboard["panels"].append({"id": 18, "type": "timeseries", "title": "CPU extra", "repeat": "host"})

        with self.assertRaisesRegex(ConfigurationError, "resolved 2 panels.*exactly 1"):
            self.get_repeating_panels(dashboard, repeating_panels=[{"title_regex": "^CPU"}])

    def test_repeating_panel_non_repeating_source_fails_validation(self):
        with self.assertRaisesRegex(ConfigurationError, "not a repeating panel"):
            self.get_repeating_panels(self.repeating_dashboard(), repeating_panels=[{"panel_id": 21}])

    def test_repeating_panel_max_values_exceeded_fails_validation(self):
        with self.assertRaisesRegex(ConfigurationError, "resolved 3 values.*max_values=2"):
            self.get_repeating_panels(
                self.repeating_dashboard(),
                repeating_panels=[{"panel_id": 17, "repeat_values": {"mode": "all"}, "max_values": 2}],
            )

    def test_upload_only_accepts_repeating_artifacts_metadata(self):
        config = {
            "panels": [{
                "panel_id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "links": ["legacy-link"],
                "is_repeating": True,
                "source_panel_id": 17,
                "repeat_var": "host",
                "artifacts": [{
                    "timestamp_tag": "tag",
                    "from": "1700000000000",
                    "to": "1700003600000",
                    "render_status": "rendered",
                    "png_file": "demo__17__repeat-prod-1__tag.png",
                    "skip_reason": None,
                    "repeat_value": "prod-1",
                    "repeat_value_slug": "prod-1",
                    "link": "artifact-link",
                }],
            }],
            "full_links": ["dashboard-link"],
            "snapshot_urls": [],
            "charts_path": "unused",
            "timestamps": [{
                "time_tag": "tag",
                "id_time": 0,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            }],
        }

        uploader = GrafanaConfigUploader("demo", config)

        self.assertTrue(uploader.panels[0].is_repeating)
        self.assertEqual(uploader.panels[0].artifacts[0]["repeat_value"], "prod-1")
        self.assertEqual(uploader.panels[0].links, ["artifact-link"])

    @staticmethod
    def prometheus_repeating_dashboard():
        return {
            "templating": {"list": [{
                "name": "database",
                "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom-main"},
                "query": "label_values(pg_database_info, database)",
                "multi": True,
                "includeAll": True,
                "options": [],
                "current": {"text": "All", "value": "$__all"},
            }]},
            "panels": [
                {"id": 19, "type": "timeseries", "title": "Calls $database", "repeat": "database"},
            ],
        }


class FakeScreenshotBrowser:
    def __init__(self, response_statuses):
        self.response_statuses = response_statuses
        self.page = FakeScreenshotPage(self)
        self.visited_urls = []
        self.saved_paths = []
        self.current_url = ""
        self.broken = False
        self.refresh_count = 0

    def get(self, url):
        return self.page.goto(url)

    def save_screenshot(self, file_path):
        self.saved_paths.append(file_path)

    def refresh_authentication(self):
        self.refresh_count += 1
        self.broken = False
        self.page = FakeScreenshotPage(self)


class FakeScreenshotPage:
    def __init__(self, browser):
        self.browser = browser
        self.response_handlers = []

    def goto(self, url):
        self.browser.current_url = url
        self.browser.visited_urls.append(url)
        if self.browser.broken:
            status_code = 599
        else:
            status_code = self.browser.response_statuses.get(url, 404)
        if isinstance(status_code, Exception):
            self.browser.broken = True
            raise status_code
        response = Mock(url=url, status=status_code)
        for handler in list(self.response_handlers):
            handler(response)
        return response

    def on(self, event, handler):
        if event == "response":
            self.response_handlers.append(handler)

    def remove_listener(self, event, handler):
        if event == "response" and handler in self.response_handlers:
            self.response_handlers.remove(handler)

    def evaluate(self, _script, expected):
        parsed = parse_qs(urlparse(self.browser.current_url).query)
        return expected in parsed.get("panelId", []) or expected in parsed.get("viewPanel", [])

    def locator(self, _selector):
        return FakeScreenshotLocator()

    def wait_for_timeout(self, _milliseconds):
        return None


class FakeScreenshotLocator:
    @property
    def first(self):
        return self

    def wait_for(self, timeout=None):
        return None

    def count(self):
        return 0
