import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from grafconflux import confluence, confluence_content
from grafconflux.confluence import ConfluenceManager, apply_graphs_placeholder, build_confluence_storage_content
from grafconflux._confluence.content import (
    ChildPageInclude,
    build_child_page_title,
    build_parent_include_content,
    sanitize_confluence_page_title,
)
from grafconflux.grafana import GrafanaConfigUploader, Panel


class TestConfluenceContent(unittest.TestCase):
    def setUp(self):
        self.timestamps = [
            SimpleNamespace(
                id_time=0,
                time_tag="smoke",
                start_time_human="2025/01/01 00:00:00",
                end_time_human="2025/01/01 01:00:00",
            )
        ]
        self.grafana_configs = [
            SimpleNamespace(
                name="Demo dashboard",
                full_links=["https://grafana.example/d/demo?from=1&to=2"],
                backup_dashboard_links=[],
                snapshot_urls=None,
                panels=[Panel(7, "timeseries", "CPU", 1, ["https://grafana.example/panel/7"])],
            )
        ]

    def create_manager(self, mocked_confluence):
        confluence_class = Mock(return_value=mocked_confluence)
        patcher = patch.dict(ConfluenceManager.__init__.__globals__, {"Confluence": confluence_class})
        patcher.start()
        self.addCleanup(patcher.stop)
        return ConfluenceManager(
            login="user",
            password="secret",
            page_id=123,
            upload_threads=1,
            wiki_url="https://wiki.example",
            verify_ssl=True,
        )

    @patch("grafconflux.confluence.Confluence")
    def test_replaces_graphs_placeholder_when_present(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Page",
            "body": {"storage": {"value": "before %%%graphs%%% after"}},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = self.create_manager(confluence)
            manager.update_page_content(self.grafana_configs, self.timestamps, 777, temp_dir)

        updated_body = confluence.update_page.call_args.kwargs["body"]
        self.assertIn("before ", updated_body)
        self.assertIn(" after", updated_body)
        self.assertIn('<ac:image ac:width="777">', updated_body)
        self.assertNotIn("%%%graphs%%%", updated_body)

    @patch("grafconflux.confluence.Confluence")
    def test_replaces_entire_body_when_placeholder_is_missing(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Page",
            "body": {"storage": {"value": "existing body without placeholder"}},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = self.create_manager(confluence)
            manager.update_page_content(self.grafana_configs, self.timestamps, 640, temp_dir)

        updated_body = confluence.update_page.call_args.kwargs["body"]
        self.assertNotEqual(updated_body, "existing body without placeholder")
        self.assertIn("Demo dashboard", updated_body)
        self.assertIn('<ac:image ac:width="640">', updated_body)

    def test_apply_graphs_placeholder_preserves_surrounding_body(self):
        result = apply_graphs_placeholder("before %%%graphs%%% after", "generated")

        self.assertEqual(result, "before generated after")

    def test_content_helpers_remain_available_from_confluence_module(self):
        self.assertIs(confluence.apply_graphs_placeholder, confluence_content.apply_graphs_placeholder)
        self.assertIs(confluence.build_confluence_storage_content, confluence_content.build_confluence_storage_content)

    def test_build_confluence_storage_content_escapes_dynamic_values(self):
        timestamps = [
            SimpleNamespace(
                id_time=0,
                time_tag="tag&one",
                start_time_human="2025/01/01 00:00:00",
                end_time_human="2025/01/01 01:00:00",
            )
        ]
        configs = [
            SimpleNamespace(
                name="Demo & dashboard",
                full_links=["https://grafana.example/d?a=1&b=2"],
                backup_dashboard_links=[],
                snapshot_urls=["https://grafana.example/snap?a=1&b=2"],
                panels=[Panel(7, "timeseries", "CPU & Memory", 1, ["https://grafana.example/panel?a=1&b=2"])],
            )
        ]

        content = build_confluence_storage_content(configs, timestamps, 900, ["snap&one.json"])

        self.assertIn("Demo &amp; dashboard", content)
        self.assertIn("CPU &amp; Memory", content)
        self.assertIn("snap&amp;one.json", content)
        self.assertIn("tag&amp;one", content)
        self.assertIn("https://grafana.example/panel?a=1&amp;b=2", content)

    def test_build_confluence_storage_content_escapes_timestamp_table_cells(self):
        timestamps = [
            SimpleNamespace(
                id_time=0,
                time_tag='<script>alert("tag")</script>',
                start_time_human='2025/01/01 <start> & "quoted"',
                end_time_human="2025/01/01 </end> & 'quoted'",
            )
        ]

        content = build_confluence_storage_content(self.grafana_configs, timestamps, 900)

        self.assertIn('&lt;script&gt;alert(&quot;tag&quot;)&lt;/script&gt;', content)
        self.assertIn('2025/01/01 &lt;start&gt; &amp; &quot;quoted&quot;', content)
        self.assertIn('2025/01/01 &lt;/end&gt; &amp; &#x27;quoted&#x27;', content)
        self.assertNotIn('<script>alert("tag")</script>', content)

    def test_build_confluence_storage_content_groups_repeating_artifacts(self):
        panel = Panel(17, "timeseries", "CPU by host", 1, ["legacy-link"])
        panel.is_repeating = True
        panel.source_panel_id = 17
        panel.repeat_var = "host"
        panel.artifacts = [
            {
                "timestamp_tag": "smoke",
                "render_status": "rendered",
                "png_file": "Demo dashboard__17__repeat-prod-1__smoke.png",
                "repeat_value": "prod-1",
                "repeat_value_slug": "prod-1",
                "link": "https://grafana.example/panel/17?var-host=prod-1",
            },
            {
                "timestamp_tag": "smoke",
                "render_status": "rendered",
                "png_file": "Demo dashboard__17__repeat-prod-2__smoke.png",
                "repeat_value": "prod-2",
                "repeat_value_slug": "prod-2",
                "link": "https://grafana.example/panel/17?var-host=prod-2",
            },
        ]
        configs = [SimpleNamespace(
            name="Demo dashboard",
            full_links=["https://grafana.example/d/demo?from=1&to=2"],
            snapshot_urls=None,
            panels=[panel],
        )]

        content = build_confluence_storage_content(configs, self.timestamps, 900)

        self.assertIn("CPU by host", content)
        self.assertIn("CPU by host [host=prod-1]", content)
        self.assertIn("CPU by host [host=prod-2]", content)
        self.assertIn("Demo dashboard__17__repeat-prod-1__smoke.png", content)
        self.assertIn("Demo dashboard__17__repeat-prod-2__smoke.png", content)

    def test_build_confluence_storage_content_uses_display_title(self):
        panel = Panel(7, "timeseries", "CPU", 1, ["https://grafana.example/panel/7"], display_title="Renamed CPU")
        configs = [SimpleNamespace(
            name="Demo dashboard",
            full_links=["https://grafana.example/d/demo?from=1&to=2"],
            backup_dashboard_links=[],
            snapshot_urls=None,
            panels=[panel],
        )]

        content = build_confluence_storage_content(configs, self.timestamps, 900)

        self.assertIn("<h3>Renamed CPU</h3>", content)
        self.assertIn("<ac:parameter ac:name=\"title\">Renamed CPU</ac:parameter>", content)

    def test_build_confluence_storage_content_orders_panels_and_variant_composite_artifacts(self):
        panel = Panel(17, "timeseries", "CPU", 1, ["https://grafana.example/panel/17"])
        panel.order_index = 2
        panel.artifacts = [
            {
                "artifact_type": "composite",
                "order_index": 3,
                "render_status": "rendered",
                "png_file": "Demo__composite-overview__0.png",
                "link": "https://grafana.example/composite",
                "composite": {"title": "Overview"},
            },
            {
                "artifact_type": "variant",
                "order_index": 1,
                "render_status": "rendered",
                "png_file": "Demo__17__variant-00-000-deadbeef__0.png",
                "link": None,
                "variant": {"label": "Service: api"},
            },
        ]
        earlier_panel = Panel(5, "timeseries", "Memory", 1, ["https://grafana.example/panel/5"])
        earlier_panel.order_index = 1
        configs = [SimpleNamespace(
            name="Demo dashboard",
            full_links=["https://grafana.example/d/demo?from=1&to=2"],
            backup_dashboard_links=[],
            snapshot_urls=None,
            panels=[panel, earlier_panel],
        )]

        content = build_confluence_storage_content(configs, self.timestamps, 900)

        self.assertLess(content.index("<h3>Memory</h3>"), content.index("<h3>CPU</h3>"))
        self.assertLess(content.index("Service: api"), content.index("Overview"))
        self.assertIn("<a href=\"https://grafana.example/panel/17\">Service: api</a>", content)
        self.assertIn("<a href=\"https://grafana.example/composite\">Overview</a>", content)

    def test_build_confluence_storage_content_normalizes_all_repeat_value_in_titles(self):
        panel = Panel(17, "timeseries", "CPU by iface", 1, ["legacy-link"])
        panel.is_repeating = True
        panel.repeat_var = "iface"
        panel.artifacts = [
            {
                "timestamp_tag": "smoke",
                "render_status": "rendered",
                "png_file": "Demo dashboard__17__repeat-all__smoke.png",
                "repeat_value": "$__all",
                "link": "https://grafana.example/panel/17?var-iface=$__all",
            }
        ]
        configs = [SimpleNamespace(
            name="Demo dashboard",
            full_links=["https://grafana.example/d/demo?from=1&to=2"],
            backup_dashboard_links=[],
            snapshot_urls=None,
            panels=[panel],
        )]

        content = build_confluence_storage_content(configs, self.timestamps, 900)

        self.assertIn("CPU by iface [iface=All]", content)
        self.assertNotIn("CPU by iface [iface=$__all]", content)

    def test_upload_only_legacy_flat_repeat_metadata_renders_repeat_value(self):
        config = {
            "panels": [{
                "panel_id": 17,
                "type": "timeseries",
                "title": "CPU by host",
                "links": ["https://grafana.example/panel/17?var-host=prod-1"],
                "is_repeating": True,
                "source_panel_id": 17,
                "repeat_var": "host",
                "repeat_value": "prod-1",
                "repeat_value_slug": "prod-1",
                "png_file": "Demo dashboard__17__repeat-prod-1__0.png",
            }],
            "full_links": ["https://grafana.example/d/demo?from=1&to=2"],
            "snapshot_urls": [],
            "charts_path": "unused",
            "timestamps": [{
                "time_tag": "smoke",
                "id_time": 0,
                "start_time_timestamp": 1700000000000,
                "end_time_timestamp": 1700003600000,
                "start_time_human": "2023/11/14 22:13:20",
                "end_time_human": "2023/11/14 23:13:20",
            }],
        }
        uploader = GrafanaConfigUploader("Demo dashboard", config)

        content = build_confluence_storage_content([uploader], uploader.timestamps, 900)

        self.assertEqual(uploader.panels[0].artifacts[0]["repeat_value"], "prod-1")
        self.assertIn("CPU by host [host=prod-1]", content)
        self.assertIn("Demo dashboard__17__repeat-prod-1__0.png", content)

    def test_build_confluence_storage_content_renders_backup_dashboard_links_with_replaced_time_range(self):
        timestamps = [
            SimpleNamespace(
                id_time=0,
                time_tag="smoke",
                start_time_timestamp=1700000000000,
                end_time_timestamp=1700003600000,
                start_time_human="2023/11/14 22:13:20",
                end_time_human="2023/11/14 23:13:20",
            )
        ]
        configs = [SimpleNamespace(
            name="Demo dashboard",
            full_links=["https://grafana.example/d/demo?from=1&to=2"],
            backup_dashboard_links=[
                "https://backup.example/d/demo?orgId=7&from=1&to=2&var-x=abc#view",
                "https://backup.example/d/demo?orgId=7&var-x=abc",
            ],
            snapshot_urls=None,
            panels=[Panel(7, "timeseries", "CPU", 1, ["https://grafana.example/panel/7"])],
        )]

        content = build_confluence_storage_content(configs, timestamps, 900)

        self.assertIn("Backup dashboard links", content)
        self.assertIn(
            "https://backup.example/d/demo?orgId=7&amp;var-x=abc&amp;from=1700000000000&amp;to=1700003600000#view",
            content,
        )
        self.assertIn(
            "https://backup.example/d/demo?orgId=7&amp;var-x=abc&amp;from=1700000000000&amp;to=1700003600000",
            content,
        )

    def test_child_page_title_uses_default_formula_and_sanitizes(self):
        args = SimpleNamespace(
            confluence_child_title=None,
            confluence_child_title_prefix="GrafConflux: ",
            confluence_child_title_from_test_id=False,
            test_id="Release / A",
            timestamps=self.timestamps,
        )

        title = build_child_page_title("Parent", args)

        self.assertEqual(title, "Parent — GrafConflux: Release - A")

    def test_child_page_title_can_use_test_id_directly(self):
        args = SimpleNamespace(
            confluence_child_title=None,
            confluence_child_title_prefix="ignored",
            confluence_child_title_from_test_id=True,
            test_id="Run 42",
            timestamps=self.timestamps,
        )

        self.assertEqual(build_child_page_title("Parent", args), "Run 42")

    def test_sanitize_confluence_page_title_has_fallback(self):
        self.assertEqual(sanitize_confluence_page_title(" / <> "), "GrafConflux child page")
        self.assertEqual(sanitize_confluence_page_title("   "), "GrafConflux child page")

    def test_build_parent_include_content_escapes_titles_and_space(self):
        content = build_parent_include_content([ChildPageInclude('Child "A" & B', 'S&P')])

        self.assertIn('<ac:structured-macro ac:name="expand">', content)
        self.assertIn('Child &quot;A&quot; &amp; B', content)
        self.assertIn('ri:content-title="Child &quot;A&quot; &amp; B"', content)
        self.assertIn('ri:space-key="S&amp;P"', content)

    @patch("grafconflux.confluence.Confluence")
    def test_parent_include_replaces_marker_when_present(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Parent",
            "space": {"key": "OPS"},
            "body": {"storage": {"value": "before %%%graphs%%% after"}},
        }
        manager = self.create_manager(confluence)

        updated = manager.update_parent_include_block(123, [ChildPageInclude("Child", "OPS")])

        self.assertTrue(updated)
        updated_body = confluence.update_page.call_args.kwargs["body"]
        self.assertIn("before ", updated_body)
        self.assertIn("ri:content-title=\"Child\"", updated_body)
        self.assertIn(" after", updated_body)
        self.assertNotIn("%%%graphs%%%", updated_body)

    @patch("grafconflux.confluence.Confluence")
    def test_parent_include_skips_update_when_marker_missing(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Parent",
            "space": {"key": "OPS"},
            "body": {"storage": {"value": "existing body"}},
        }
        manager = self.create_manager(confluence)

        updated = manager.update_parent_include_block(123, [ChildPageInclude("Child", "OPS")])

        self.assertFalse(updated)
        confluence.update_page.assert_not_called()

    @patch("grafconflux.confluence.Confluence")
    def test_create_or_get_child_page_creates_missing_page_under_parent(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Parent",
            "space": {"key": "OPS"},
            "body": {"storage": {"value": "%%%graphs%%%"}},
        }
        confluence.get_page_child_by_type.return_value = {"results": []}
        confluence.create_page.return_value = {"id": "456", "title": "Child"}
        args = SimpleNamespace(
            confluence_child_title="Child",
            confluence_child_title_prefix="GrafConflux: ",
            confluence_child_title_from_test_id=False,
            test_id="ignored",
            timestamps=self.timestamps,
        )
        manager = self.create_manager(confluence)

        child_page = manager.create_or_get_child_page(123, args)

        self.assertEqual(args.confluence_page_id, 456)
        self.assertEqual(
            child_page,
            ChildPageInclude("Child", "OPS", 456, "https://wiki.example/pages/viewpage.action?pageId=456"),
        )
        confluence.create_page.assert_called_once_with(
            space="OPS",
            title="Child",
            body="%%%graphs%%%",
            parent_id=123,
            representation="storage",
        )

    @patch("grafconflux.confluence.Confluence")
    def test_create_or_get_child_page_reuses_existing_child(self, confluence_class):
        confluence = confluence_class.return_value
        confluence.get_page_by_id.return_value = {
            "title": "Parent",
            "space": {"key": "OPS"},
            "body": {"storage": {"value": "%%%graphs%%%"}},
        }
        confluence.get_page_child_by_type.return_value = {"results": [{"id": "456", "title": "Child"}]}
        args = SimpleNamespace(
            confluence_child_title="Child",
            confluence_child_title_prefix="GrafConflux: ",
            confluence_child_title_from_test_id=False,
            test_id="ignored",
            timestamps=self.timestamps,
        )
        manager = self.create_manager(confluence)

        child_page = manager.create_or_get_child_page(123, args)

        self.assertEqual(args.confluence_page_id, 456)
        self.assertEqual(
            child_page,
            ChildPageInclude("Child", "OPS", 456, "https://wiki.example/pages/viewpage.action?pageId=456"),
        )
        confluence.create_page.assert_not_called()

    def test_build_confluence_page_url_preserves_subpath_and_trailing_slash(self):
        url = confluence.build_confluence_page_url("https://wiki.example/confluence/", 789)

        self.assertEqual(url, "https://wiki.example/confluence/pages/viewpage.action?pageId=789")

    def test_build_confluence_page_url_prefers_metadata_webui(self):
        page = {"_links": {"base": "https://wiki.example/confluence", "webui": "/display/OPS/Page"}}

        url = confluence.build_confluence_page_url("https://fallback.example", 789, page)

        self.assertEqual(url, "https://wiki.example/confluence/display/OPS/Page")
