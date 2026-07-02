import json
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import Mock, patch

import yaml

from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import SNAPSHOT_HYDRATION_SCROLL_LIMIT, ConfigurationError, GrafanaConfigDownloader, GrafanaConfigUploader, GrafanaManager

class TestGrafanaSnapshotConfig(unittest.TestCase):
    def create_config(self, **overrides):
        config = {"dash_title": "Dashboard", "host": "https://grafana.example"}
        config.update(overrides)
        return GrafanaConfigDownloader("demo", config)

    def test_snapshot_config_defaults_to_single_ui_solution(self):
        config = self.create_config()

        self.assertEqual(config.snapshot_mode, "ui")
        self.assertTrue(config.snapshot_store_dashboard_json)

    def test_deprecated_snapshot_modes_are_ignored_with_warning(self):
        for mode in ("ui", "legacy_api", "auto"):
            with self.subTest(mode=mode):
                with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
                    config = self.create_config(snapshot_mode=mode)

                self.assertEqual(config.snapshot_mode, "ui")
                self.assertIn("deprecated", "\n".join(logs.output))
                self.assertIn("UI snapshot flow", "\n".join(logs.output))

    def test_snapshot_mode_rejects_unknown_value_with_yaml_path(self):
        with self.assertRaisesRegex(ConfigurationError, "dashboards.demo.snapshot_mode.*api.*deprecated"):
            self.create_config(snapshot_mode="api")

    def test_deprecated_snapshot_fallback_is_ignored_with_warning(self):
        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            config = self.create_config(snapshot_fallback_to_ui=False)

        self.assertTrue(config.snapshot_fallback_to_ui)
        self.assertIn("snapshot_fallback_to_ui", "\n".join(logs.output))
        self.assertIn("ignored", "\n".join(logs.output))


class TestGrafanaSnapshotUi(unittest.TestCase):
    def create_manager(self, browser: "RecordingSnapshotBrowser", **overrides):
        config = {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
            "snapshot": True,
        }
        config.update(overrides)
        manager = GrafanaManager(GrafanaConfigDownloader("demo", config))
        manager.dashboard_uid = "uid-1"
        manager.dashboard_model = self.dashboard_payload()
        manager.config.full_links = ["https://grafana.example/d/uid-1/dashboard"]
        manager.session.cookies.set("grafana_session", "cookie", domain="grafana.example", path="/")
        self.browser = browser
        return manager

    @staticmethod
    def timestamp(tag="tag"):
        return GrafanaTimeDownloader(f"{tag}__&from=1700000000&to=1700003600", 0, "UTC")

    @staticmethod
    def dashboard_payload():
        return {
            "uid": "uid-1",
            "title": "Dashboard",
            "time": {"from": "now-6h", "to": "now"},
            "templating": {"list": [{"name": "env", "current": {"value": "prod"}}]},
            "panels": [{"id": 1, "type": "timeseries", "title": "CPU"}],
        }

    @staticmethod
    def response(status_code=200, body=None):
        response = Mock(status_code=status_code)
        response.json.return_value = body if body is not None else {}
        return response

    def test_take_snapshot_always_uses_ui_flow_and_never_posts_snapshot_api(self):
        manager = self.create_manager(ModernSnapshotBrowser(), snapshot_mode="legacy_api")
        manager.session.post = Mock()

        with patch.object(manager, "_take_snapshot_ui") as ui_flow:
            manager.take_snapshot([self.timestamp("tag")], "unused")

        ui_flow.assert_called_once()
        manager.session.post.assert_not_called()

    def test_modern_grafana_flow_expands_rows_hydrates_and_saves_backup(self):
        manager = self.create_manager(ModernSnapshotBrowser())
        manager.session.get = Mock(return_value=self.response(body={"dashboard": {"title": "Dashboard"}}))

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)
            with open(os.path.join(temp_dir, "demo__tag.json"), "r", encoding="utf-8") as backup_file:
                backup = json.load(backup_file)

        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/abc"])
        self.assertEqual(backup, {"dashboard": {"title": "Dashboard"}})
        self.assertIn("expand-collapsed-rows", self.browser.scripts)
        self.assertIn("reset-scroll-top", self.browser.scripts)
        self.assertIn("hydrate-scroll", self.browser.scripts)
        self.assertLess(self.browser.scripts.index("expand-collapsed-rows"), self.browser.scripts.index("reset-scroll-top"))
        self.assertLess(self.browser.scripts.index("reset-scroll-top"), self.browser.scripts.index("hydrate-scroll"))
        self.assertIn('button[aria-label="Toggle share menu"]', self.browser.clicked_targets)
        self.assertIn('[data-testid="data-testid new share button share snapshot"]', self.browser.clicked_targets)
        self.assertIn('[data-testid="data-testid share snapshot publish button"]', self.browser.clicked_targets)
        self.assertNotIn('a[aria-label="Tab Snapshot"]', self.browser.clicked_targets)
        self.assertNotIn('input[id="timeout-input"]', self.browser.cleared_targets)
        self.assertNotIn('input[id="snapshot-url-input"]', self.browser.clicked_targets)

    def test_classic_flow_is_used_when_modern_share_snapshot_is_unavailable(self):
        manager = self.create_manager(ClassicSnapshotBrowser())
        manager.session.get = Mock(return_value=self.response(body={"dashboard": {}}))

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)

        self.assertIn('button[aria-label="Share dashboard"]', self.browser.clicked_targets)
        self.assertIn('a[aria-label="Tab Snapshot"]', self.browser.clicked_targets)
        self.assertIn("Local Snapshot", self.browser.clicked_targets)

    def test_snapshot_dialog_facade_preserves_modern_and_classic_return_values(self):
        modern_browser = ModernSnapshotBrowser()
        modern_manager = self.create_manager(modern_browser)
        with patch("grafconflux.grafana.time.sleep"):
            self.assertTrue(modern_manager._open_snapshot_dialog(modern_browser))
        self.assertIn('button[aria-label="Toggle share menu"]', modern_browser.clicked_targets)
        self.assertIn('[data-testid="data-testid new share button share snapshot"]', modern_browser.clicked_targets)
        self.assertNotIn('button[aria-label="Share dashboard"]', modern_browser.clicked_targets)

        classic_browser = ClassicSnapshotBrowser()
        classic_manager = self.create_manager(classic_browser)
        with patch("grafconflux.grafana.time.sleep"):
            self.assertFalse(classic_manager._open_snapshot_dialog(classic_browser))
        self.assertIn('button[aria-label="Share dashboard"]', classic_browser.clicked_targets)
        self.assertIn('a[aria-label="Tab Snapshot"]', classic_browser.clicked_targets)

    def test_hydration_scroll_uses_bounded_iterations(self):
        manager = self.create_manager(NeverSettlingSnapshotBrowser())
        manager.session.get = Mock(return_value=self.response(body={"dashboard": {}}))

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)

        self.assertLessEqual(self.browser.hydration_scroll_count, 40)
        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/abc"])

    def test_collapsed_rows_are_expanded_across_bidirectional_full_sweeps(self):
        manager = self.create_manager(FullDashboardSweepSnapshotBrowser())
        manager.dashboard_model = {
            "panels": [
                {"type": "row", "title": "Top", "collapsed": True},
                {"type": "row", "title": "Middle", "collapsed": True},
                {"type": "row", "title": "Bottom", "collapsed": True},
            ]
        }

        manager._expand_collapsed_rows(self.browser)

        self.assertEqual(self.browser.expanded_row_titles, ["Bottom", "Middle", "Top"])
        self.assertEqual(self.browser.scripts[0], "scroll-edge-bottom")
        self.assertIn("row-sweep-up", self.browser.scripts)
        self.assertIn("row-sweep-down", self.browser.scripts)
        self.assertIn('button[aria-label="Expand row"]', self.browser.expansion_scripts[0])
        self.assertIn('button[data-testid^="data-testid dashboard-row-title-"]', self.browser.expansion_scripts[0])
        self.assertIn("dashboardRow(button)", self.browser.expansion_scripts[0])
        self.assertIn("isInViewport", self.browser.expansion_scripts[0])

    def test_remaining_collapsed_rows_are_retried_after_hydration_warning(self):
        manager = self.create_manager(RemainingCollapsedRowsBrowser())

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            manager._prepare_dashboard_for_snapshot(self.browser)

        self.assertGreaterEqual(self.browser.scripts.count("expand-collapsed-rows"), 2)
        self.assertEqual(self.browser.expanded_row_titles, ["Late row"])
        self.assertGreaterEqual(self.browser.hydration_scroll_count, 2)
        self.assertIn("Collapsed rows remain", "\n".join(logs.output))
        self.assertNotIn("'button[aria-expanded=\"false\"]'", manager._remaining_collapsed_rows_script())
        self.assertIn('[data-testid="dashboard-row-title"] button[aria-expanded="false"]', manager._remaining_collapsed_rows_script())

    def test_hydration_scroll_uses_monotonic_javascript_scroll(self):
        manager = self.create_manager(ModernSnapshotBrowser())

        manager._hydrate_dashboard_panels(self.browser)

        self.assertEqual(self.browser.sent_keys, [])
        self.assertIn("reset-scroll-top", self.browser.scripts)
        self.assertGreater(self.browser.hydration_scroll_count, 0)
        self.assertTrue(any("grafconfluxHydrateDashboard" in script for script in self.browser.executed_scripts))

    def test_hydration_prefers_scrollbar_view_page_down_with_bounded_loop(self):
        browser = ModernSnapshotBrowser()
        browser.scrollbar_view_positions = [50, 100, 100]
        manager = self.create_manager(browser)

        with patch("grafconflux.grafana.time.sleep"):
            manager._hydrate_dashboard_panels(browser)

        page_down_keys = [args[0] for target, args in browser.sent_keys if target == ".scrollbar-view"]
        self.assertEqual(page_down_keys, ["PageDown"] * 4)
        self.assertEqual(browser.hydration_scroll_count, 0)

        never_settling = ModernSnapshotBrowser()
        never_settling.scrollbar_view_positions = list(range(1, SNAPSHOT_HYDRATION_SCROLL_LIMIT + 5))
        with patch("grafconflux.grafana.time.sleep"):
            manager._hydrate_dashboard_panels(never_settling)
        page_down_count = sum(1 for target, _ in never_settling.sent_keys if target == ".scrollbar-view")
        self.assertLessEqual(page_down_count, SNAPSHOT_HYDRATION_SCROLL_LIMIT)

    def test_scrollbar_position_uses_playwright_element_evaluate(self):
        manager = self.create_manager(ModernSnapshotBrowser())
        browser = NoElementArgumentBrowser()
        element = EvaluatingSnapshotElement(123)

        position = manager._scrollbar_view_scroll_top(browser, element)

        self.assertEqual(position, 123)
        self.assertEqual(browser.script_calls, 0)
        self.assertEqual(element.expressions, ['(element) => Math.floor(element.scrollTop || 0)'])

    def test_hydration_scroll_uses_slow_incremental_steps_and_dwell(self):
        browser = LoaderBusySnapshotBrowser()
        manager = self.create_manager(browser)

        with patch("grafconflux.grafana.time.sleep") as sleep:
            manager._hydrate_dashboard_panels(browser)

        hydrate_scripts = [script for script in browser.executed_scripts if "grafconfluxHydrateDashboard" in script]
        self.assertIn("* 0.45", hydrate_scripts[0])
        self.assertNotIn("* 0.85", hydrate_scripts[0])
        sleep.assert_any_call(0.8)
        sleep.assert_any_call(1.0)
        self.assertGreater(browser.loader_wait_checks, 0)
        self.assertLessEqual(browser.loader_wait_checks, 120)
        self.assertGreater(browser.hydration_scroll_count, 0)

    def test_ui_backup_download_failure_keeps_snapshot_link(self):
        manager = self.create_manager(ModernSnapshotBrowser())
        manager.session.get = Mock(return_value=self.response(status_code=500, body={"message": "failed"}))

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertLogs("grafconflux.grafana", level="WARNING"):
                self.run_ui_snapshot(manager, temp_dir)
            backup_path = os.path.join(temp_dir, "demo__tag.json")

        self.assertFalse(os.path.exists(backup_path))
        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/abc"])

    def test_ui_store_dashboard_json_false_skips_backup_download(self):
        manager = self.create_manager(ModernSnapshotBrowser(), snapshot_store_dashboard_json=False)
        manager.session.get = Mock()

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)
            backup_path = os.path.join(temp_dir, "demo__tag.json")

        manager.session.get.assert_not_called()
        self.assertFalse(os.path.exists(backup_path))
        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/abc"])

    def test_snapshot_url_is_read_from_playwright_response_payload(self):
        browser = NetworkSnapshotBrowser({"url": "https://grafana.example/dashboard/snapshot/netkey"})
        manager = self.create_manager(browser)
        manager.session.get = Mock(return_value=self.response(body={"dashboard": {"title": "Dashboard"}}))

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)

        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/netkey"])
        manager.session.get.assert_called_with(
            "https://grafana.example/api/snapshots/netkey",
            verify=True,
            timeout=30,
        )

    def test_snapshot_url_lookup_by_unique_name_when_ui_has_no_link_or_network_url(self):
        manager = self.create_manager(NoSnapshotUrlBrowser())
        manager.session.get = Mock(side_effect=[
            self.response(body=[{"name": "demo__tag", "key": "lookupkey"}]),
            self.response(body={"dashboard": {"title": "Dashboard"}}),
        ])

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)

        self.assertEqual(manager.config.snapshot_urls, ["https://grafana.example/dashboard/snapshot/lookupkey"])
        self.assertEqual(manager.session.get.call_args_list[0].kwargs["params"], {"query": "demo__tag"})

    def test_delete_fields_are_not_persisted_in_ui_backup_or_metadata(self):
        manager = self.create_manager(ModernSnapshotBrowser())
        manager.session.get = Mock(return_value=self.response(body={
            "dashboard": {"title": "Dashboard"}, "deleteKey": "secret", "deleteUrl": "https://delete",
        }))

        with tempfile.TemporaryDirectory() as temp_dir:
            self.run_ui_snapshot(manager, temp_dir)
            manager._GrafanaManager__save_params_to_file([self.timestamp("tag")], temp_dir)
            with open(os.path.join(temp_dir, "demo__tag.json"), "r", encoding="utf-8") as backup_file:
                backup = json.load(backup_file)
            with open(os.path.join(temp_dir, "demo.yaml"), "r", encoding="utf-8") as metadata_file:
                metadata = yaml.safe_load(metadata_file)

        self.assertNotIn("deleteKey", backup)
        self.assertNotIn("deleteUrl", backup)
        self.assertNotIn("deleteKey", metadata)
        self.assertNotIn("deleteUrl", metadata)

    def test_snapshot_helper_facade_preserves_url_payload_and_file_behavior(self):
        manager = self.create_manager(ModernSnapshotBrowser(), nginx_prefix="/grafana")
        timestamp = self.timestamp("nightly")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertEqual(manager._snapshot_name(timestamp), "demo__nightly")
            self.assertEqual(
                manager._snapshot_backup_file(temp_dir, timestamp),
                os.path.join(temp_dir, "demo__nightly.json"),
            )

        self.assertEqual(
            manager._snapshot_api_url("/api/snapshots/key"),
            "https://grafana.example/grafana/api/snapshots/key",
        )
        self.assertEqual(manager._snapshot_url_from_key("key"), "https://grafana.example/dashboard/snapshot/key")
        self.assertEqual(
            manager._normalize_snapshot_url("/dashboard/snapshot/key"),
            "https://grafana.example/dashboard/snapshot/key",
        )
        self.assertIsNone(manager._normalize_snapshot_url("/not-a-snapshot/key"))
        self.assertEqual(
            manager._snapshot_url_from_payload({"externalUrl": "/dashboard/snapshot/ext"}),
            "https://grafana.example/dashboard/snapshot/ext",
        )
        self.assertEqual(
            manager._snapshot_url_from_payload({"key": "payload-key"}),
            "https://grafana.example/dashboard/snapshot/payload-key",
        )
        self.assertEqual(manager._snapshot_key_from_url("https://snapshots.local/dashboard/snapshot/key/"), "key")
        with self.assertRaisesRegex(ValueError, "Snapshot URL has no key"):
            manager._snapshot_key_from_url("")

    def run_ui_snapshot(self, manager: GrafanaManager, temp_dir: str) -> None:
        with patch("grafconflux._grafana.snapshots.GrafanaBrowserSession.create_browser", return_value=self.browser):
            with patch("grafconflux.grafana.time.sleep"):
                manager.take_snapshot([self.timestamp("tag")], temp_dir)


class FakeSnapshotElement:
    def __init__(self, browser: "RecordingSnapshotBrowser", target: str, value: str | None = None) -> None:
        self.browser = browser
        self.target = target
        self.value = value
    def click(self) -> None:
        self.browser.clicked_targets.append(self.target)
    def clear(self) -> None:
        self.browser.cleared_targets.append(self.target)
    def send_keys(self, *args: Any) -> None:
        self.browser.sent_keys.append((self.target, args))
    def get_attribute(self, name: str) -> str | None:
        return self.value


class EvaluatingSnapshotElement:
    def __init__(self, scroll_top: int) -> None:
        self.scroll_top = scroll_top
        self.expressions: list[str] = []

    def evaluate(self, expression: str) -> int:
        self.expressions.append(expression)
        return self.scroll_top


class NoElementArgumentBrowser:
    def __init__(self) -> None:
        self.script_calls = 0

    def execute_script(self, script: str, *args: Any) -> int:
        self.script_calls += 1
        if args:
            raise AssertionError("Playwright elements must not be passed through execute_script")
        return 0


class RecordingSnapshotBrowser:
    def __init__(self) -> None:
        self.page = FakeSnapshotPage(self)
        self.snapshot_payloads: list[dict[str, Any]] = []
        self.clicked_targets: list[str] = []
        self.cleared_targets: list[str] = []
        self.sent_keys: list[tuple[str, tuple[Any, ...]]] = []
        self.scripts: list[str] = []
        self.executed_scripts: list[str] = []
        self.expansion_scripts: list[str] = []
        self.expanded_row_titles: list[str] = []
        self.hydration_scroll_count = 0
        self.scrollbar_view_positions: list[int] | None = None
        self.quit_calls = 0
    def get(self, url: str) -> None:
        pass
    def add_cookie(self, cookie: dict[str, Any]) -> None:
        pass
    def set_page_load_timeout(self, timeout: int) -> None:
        pass
    def execute_script(self, script: str, *args: Any) -> int:
        self.executed_scripts.append(script)
        if "grafconfluxDashboardLoadedForSnapshot" in script:
            return True
        if "grafconfluxScrollDashboardToEdge" in script:
            edge = "bottom" if "'bottom'" in script else "top"
            self.scripts.append(f"scroll-edge-{edge}")
            return 0
        if "grafconfluxRowSweepScroll" in script:
            direction = "up" if "const direction = 'up'" in script else "down"
            self.scripts.append(f"row-sweep-{direction}")
            return {"atEdge": True, "before": 0, "after": 0, "maximum": 0}
        if "grafconfluxResetDashboardScrollTop" in script:
            self.scripts.append("reset-scroll-top")
            return 0
        if args and getattr(args[0], "target", None) == ".scrollbar-view" and "scrollTop" in script:
            if self.scrollbar_view_positions:
                return self.scrollbar_view_positions.pop(0)
            return self.hydration_scroll_count + 1
        if "grafconfluxExpandCollapsedRows" in script or "grafconfluxExpandVisibleCollapsedRows" in script:
            self.scripts.append("expand-collapsed-rows")
            self.expansion_scripts.append(script)
            if self.scripts.count("expand-collapsed-rows") == 1:
                self.expanded_row_titles.extend(["Middle", "Top"])
                return {"count": 2, "titles": ["Middle", "Top"]}
            return {"count": 0, "titles": []}
        if "grafconfluxRemainingCollapsedRows" in script:
            return {"count": 0, "titles": []}
        if "grafconfluxSnapshotLoadersBusy" in script:
            return False
        if "grafconfluxReadSnapshotUrl" in script:
            return "https://grafana.example/dashboard/snapshot/abc"
        self.scripts.append("hydrate-scroll")
        self.hydration_scroll_count += 1
        return 0
    def find_element(self, by: str | None = None, value: str | None = None) -> FakeSnapshotElement:
        target = self.target_label(value or str(by))
        if value == ".scrollbar-view" and self.scrollbar_view_positions is None:
            raise LookupError(value)
        if value == 'input[id="snapshot-url-input"]':
            return FakeSnapshotElement(self, target, "https://grafana.example/dashboard/snapshot/abc")
        return FakeSnapshotElement(self, target)
    @staticmethod
    def target_label(target: str) -> str:
        if "Share snapshot" in target:
            return "Share snapshot"
        if "Local Snapshot" in target:
            return "Local Snapshot"
        return target
    def quit(self) -> None:
        self.quit_calls += 1


class FakeSnapshotPage:
    def __init__(self, browser: RecordingSnapshotBrowser) -> None:
        self.browser = browser

    def locator(self, selector: str) -> "FakeSnapshotLocator":
        return FakeSnapshotLocator(self.browser, selector)

    def expect_response(self, _predicate: Any, timeout: int | None = None) -> "FakeSnapshotResponseContext":
        return FakeSnapshotResponseContext(self.browser)


class FakeSnapshotLocator:
    def __init__(self, browser: RecordingSnapshotBrowser, selector: str) -> None:
        self.browser = browser
        self.selector = selector.replace("xpath=", "")
        self.target = self.selector

    @property
    def first(self) -> "FakeSnapshotLocator":
        return self

    def wait_for(self, timeout: int | None = None) -> None:
        self.browser.find_element(value=self.selector)

    def click(self, timeout: int | None = None) -> None:
        self.browser.find_element(value=self.selector).click()

    def fill(self, value: str) -> None:
        element = self.browser.find_element(value=self.selector)
        if value == "":
            element.clear()
        else:
            element.send_keys(value)

    def press(self, key: str) -> None:
        self.browser.find_element(value=self.selector).send_keys(key)

    def get_attribute(self, name: str) -> str | None:
        return self.browser.find_element(value=self.selector).get_attribute(name)

    def evaluate(self, _expression: str) -> int:
        if self.selector != ".scrollbar-view":
            return 0
        if self.browser.scrollbar_view_positions:
            return self.browser.scrollbar_view_positions.pop(0)
        return self.browser.hydration_scroll_count + 1


class FakeSnapshotResponseContext:
    def __init__(self, browser: RecordingSnapshotBrowser) -> None:
        self.browser = browser

    def __enter__(self) -> "FakeSnapshotResponseContext":
        if getattr(self.browser, "skip_snapshot_response_wait", False):
            raise TimeoutError("no snapshot response")
        self.value = Mock(json=Mock(return_value={"url": "https://grafana.example/dashboard/snapshot/abc"}))
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        return None


class ModernSnapshotBrowser(RecordingSnapshotBrowser):
    def find_element(self, by: str | None = None, value: str | None = None) -> FakeSnapshotElement:
        old_selectors = ('input[id="snapshot-url-input"]', 'input[id="timeout-input"]')
        old_xpaths = ("Local Snapshot",)
        if value in old_selectors or any(xpath in (value or "") for xpath in old_xpaths):
            raise LookupError(value)
        return super().find_element(by, value)
class FailingSetupSnapshotBrowser(RecordingSnapshotBrowser):
    def __init__(self, failing_step: str) -> None:
        super().__init__()
        self.failing_step = failing_step

    def get(self, url: str) -> None:
        if self.failing_step == "get":
            raise RuntimeError("get")

    def add_cookie(self, cookie: dict[str, Any]) -> None:
        if self.failing_step == "add_cookie":
            raise RuntimeError("add_cookie")
class ClassicSnapshotBrowser(RecordingSnapshotBrowser):
    def find_element(self, by: str | None = None, value: str | None = None) -> FakeSnapshotElement:
        modern_selectors = (
            'button[aria-label="Share dashboard menu"]',
            'button[aria-label="Share dashboard options"]',
            'button[aria-label="Open share menu"]',
            'button[aria-label="Toggle share menu"]',
            'button[data-testid="data-testid new share button arrow menu"]',
            '[data-testid="data-testid new share button arrow menu"]',
        )
        if value in modern_selectors or (value and "share snapshot" in value.lower()):
            raise LookupError(value)
        return super().find_element(by, value)
class NeverSettlingSnapshotBrowser(ModernSnapshotBrowser):
    def execute_script(self, script: str, *args: Any) -> int:
        self.executed_scripts.append(script)
        if "grafconfluxDashboardLoadedForSnapshot" in script:
            return True
        if "grafconfluxScrollDashboardToEdge" in script:
            self.scripts.append("scroll-edge-bottom")
            return 0
        if "grafconfluxRowSweepScroll" in script:
            self.scripts.append("row-sweep-up" if "const direction = 'up'" in script else "row-sweep-down")
            return {"atEdge": True, "before": 0, "after": 0, "maximum": 0}
        if "grafconfluxResetDashboardScrollTop" in script:
            self.scripts.append("reset-scroll-top")
            return 0
        if "grafconfluxExpandCollapsedRows" in script or "grafconfluxExpandVisibleCollapsedRows" in script:
            self.scripts.append("expand-collapsed-rows")
            self.expansion_scripts.append(script)
            return {"count": 0, "titles": []}
        if "grafconfluxRemainingCollapsedRows" in script:
            return {"count": 0, "titles": []}
        if "grafconfluxSnapshotLoadersBusy" in script:
            return False
        if "grafconfluxReadSnapshotUrl" in script:
            return "https://grafana.example/dashboard/snapshot/abc"
        self.scripts.append("hydrate-scroll")
        self.hydration_scroll_count += 1
        return self.hydration_scroll_count
class FullDashboardSweepSnapshotBrowser(ModernSnapshotBrowser):
    def execute_script(self, script: str, *args: Any) -> int:
        self.executed_scripts.append(script)
        if "grafconfluxExpandVisibleCollapsedRows" in script:
            return self.expand_next_visible_row(script)
        if "grafconfluxRowSweepScroll" in script:
            return self.record_sweep_scroll(script)
        return super().execute_script(script, *args)

    def expand_next_visible_row(self, script: str) -> dict[str, Any]:
        self.scripts.append("expand-collapsed-rows")
        self.expansion_scripts.append(script)
        batches = [["Bottom"], ["Middle"], ["Top"]]
        batch_index = len(self.expanded_row_titles)
        titles = batches[batch_index] if batch_index < len(batches) else []
        self.expanded_row_titles.extend(titles)
        return {"count": len(titles), "titles": titles}

    def record_sweep_scroll(self, script: str) -> dict[str, Any]:
        direction = "up" if "const direction = 'up'" in script else "down"
        self.scripts.append(f"row-sweep-{direction}")
        return {"atEdge": len(self.expanded_row_titles) >= 3, "before": 1, "after": 0, "maximum": 1}
class RemainingCollapsedRowsBrowser(ModernSnapshotBrowser):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_checks = 0

    def execute_script(self, script: str, *args: Any) -> int:
        self.executed_scripts.append(script)
        if "grafconfluxScrollDashboardToEdge" in script:
            self.scripts.append("scroll-edge-bottom")
            return 0
        if "grafconfluxRowSweepScroll" in script:
            self.scripts.append("row-sweep-up" if "const direction = 'up'" in script else "row-sweep-down")
            return {"atEdge": True, "before": 0, "after": 0, "maximum": 0}
        if "grafconfluxExpandCollapsedRows" in script or "grafconfluxExpandVisibleCollapsedRows" in script:
            self.scripts.append("expand-collapsed-rows")
            self.expansion_scripts.append(script)
            if self.remaining_checks > 0 and not self.expanded_row_titles:
                self.expanded_row_titles.append("Late row")
                return {"count": 1, "titles": ["Late row"]}
            return {"count": 0, "titles": []}
        if "grafconfluxRemainingCollapsedRows" in script:
            self.remaining_checks += 1
            if self.remaining_checks == 1:
                return {"count": 1, "titles": ["Late row"]}
            return {"count": 0, "titles": []}
        if "grafconfluxSnapshotLoadersBusy" in script:
            return False
        if "grafconfluxReadSnapshotUrl" in script:
            return "https://grafana.example/dashboard/snapshot/abc"
        self.scripts.append("hydrate-scroll")
        self.hydration_scroll_count += 1
        return 0
class LoaderBusySnapshotBrowser(ModernSnapshotBrowser):
    def __init__(self) -> None:
        super().__init__()
        self.loader_wait_checks = 0
    def execute_script(self, script: str, *args: Any) -> int:
        if "grafconfluxSnapshotLoadersBusy" in script:
            self.executed_scripts.append(script)
            self.loader_wait_checks += 1
            return self.loader_wait_checks < 50
        return super().execute_script(script, *args)
class FakeSnapshotResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.body = json.dumps(payload).encode("utf-8")
        self.headers = {}
class FakeSnapshotRequest:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.method = "POST"
        self.url = "https://grafana.example/api/snapshots"
        self.response = FakeSnapshotResponse(payload)
class NoSnapshotUrlBrowser(ModernSnapshotBrowser):
    def __init__(self) -> None:
        super().__init__()
        self.skip_snapshot_response_wait = True

    def execute_script(self, script: str, *args: Any) -> int:
        if "grafconfluxReadSnapshotUrl" in script:
            self.executed_scripts.append(script)
            return None
        return super().execute_script(script, *args)
class NetworkSnapshotBrowser(NoSnapshotUrlBrowser):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__()
        self.requests = [FakeSnapshotRequest(payload)]
class TestGrafanaSnapshotUploadCompatibility(unittest.TestCase):
    def test_upload_existing_handles_ui_created_snapshot_json_metadata(self):
        config = {
            "panels": [{"panel_id": 1, "type": "graph", "title": "CPU", "links": ["panel-link"]}],
            "full_links": ["dashboard-link"],
            "snapshot_urls": ["https://snap/abc"],
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
        self.assertEqual(uploader.snapshot_urls, ["https://snap/abc"])
    def test_missing_backup_after_successful_snapshot_link_does_not_break_upload_only_config(self):
        config = {
            "panels": [{"panel_id": 1, "type": "graph", "title": "CPU", "links": ["panel-link"]}],
            "full_links": ["dashboard-link"],
            "snapshot_urls": ["https://snap/abc"],
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
        self.assertEqual(uploader.snapshot_urls[0], "https://snap/abc")
