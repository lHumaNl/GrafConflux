import unittest
from dataclasses import dataclass

from grafconflux._grafana import playwright_screenshots
from grafconflux._grafana.playwright_screenshots import (
    PlaywrightPanelScreenshotRunner,
    PlaywrightResponseCollector,
    is_grafana_login_url,
)
from grafconflux._shared.grafana_models import sanitize_url_for_log
from grafconflux.grafana import GrafanaConfigDownloader


@dataclass
class FakeResponse:
    url: str
    status: int


class FakeLocator:
    @property
    def first(self):
        return self

    def wait_for(self, timeout):
        return None


class FakePage:
    def __init__(self, loading_states=None):
        self.current_ms = 0
        self.waited_ms = 0
        self.loading_states = list(loading_states or [False])

    def locator(self, _selector):
        return FakeLocator()

    def wait_for_timeout(self, timeout_ms):
        self.waited_ms += timeout_ms
        self.current_ms += timeout_ms

    def evaluate(self, _script):
        if len(self.loading_states) > 1:
            return self.loading_states.pop(0)
        return self.loading_states[0]


class FakeBrowser:
    def __init__(self, page):
        self.page = page


class FakeManager:
    def __init__(self, config):
        self.config = config


class TestPlaywrightScreenshotReadiness(unittest.TestCase):
    def setUp(self):
        self.original_monotonic = playwright_screenshots.time.monotonic

    def tearDown(self):
        playwright_screenshots.time.monotonic = self.original_monotonic

    def test_fragment_mismatch_does_not_wait_full_timeout_by_default(self):
        runner, browser, collector = self.create_runner(no_network_grace_ms=200, timeout=5)

        status = runner._wait_for_network_settle(browser, collector, ["unmatched-datasource"])

        self.assertIsNone(status)
        self.assertEqual(browser.page.waited_ms, 200)
        self.assertLess(browser.page.waited_ms, runner.manager.config.timeout * 1000)

    def test_no_relevant_network_uses_configured_grace(self):
        runner, browser, collector = self.create_runner(no_network_grace_ms=300, timeout=5)

        status = runner._wait_for_network_settle(browser, collector, [])

        self.assertIsNone(status)
        self.assertEqual(browser.page.waited_ms, 300)

    def test_visible_loading_indicator_delays_until_hidden(self):
        runner, browser, collector = self.create_runner(
            no_network_grace_ms=0,
            min_settle_ms=0,
            poll_interval_ms=100,
            loading_states=[True, False],
        )

        status = runner._wait_for_network_settle(browser, collector, [])

        self.assertIsNone(status)
        self.assertEqual(browser.page.waited_ms, 100)

    def test_relevant_unauthorized_response_is_returned_despite_fragment_mismatch(self):
        runner, browser, collector = self.create_runner(no_network_grace_ms=100, timeout=5)
        collector.responses.append(FakeResponse("https://grafana.example/api/ds/query", 401))

        status = runner._wait_for_network_settle(browser, collector, ["unmatched-datasource"])

        self.assertEqual(status, 401)
        self.assertEqual(browser.page.waited_ms, 0)

    def test_sanitize_url_for_log_redacts_query_and_fragment(self):
        sanitized = sanitize_url_for_log("https://grafana.example/d/demo?token=secret&viewPanel=7#frag")

        self.assertEqual(sanitized, "https://grafana.example/d/demo?token=REDACTED&viewPanel=7")

    def test_sanitize_url_for_log_preserves_bare_fullscreen_flag(self):
        sanitized = sanitize_url_for_log(
            "https://grafana.example/d/demo?viewPanel=7&fullscreen"
        )

        self.assertEqual(
            sanitized,
            "https://grafana.example/d/demo?viewPanel=7&fullscreen",
        )
        self.assertNotIn("fullscreen=", sanitized)

    def test_login_detection_supports_grafana_subpath_and_rejects_external_origin(self):
        config = GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "grafana_url": "https://grafana.example/grafana",
        })

        self.assertTrue(is_grafana_login_url(
            "https://grafana.example/grafana/login?redirectTo=%2Fgrafana%2Fd%2Fdemo",
            config,
        ))
        self.assertFalse(is_grafana_login_url(
            "https://identity.example/login",
            config,
        ))

    def test_login_detection_normalizes_default_ports(self):
        https_config = GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "grafana_url": "https://grafana.example/grafana",
        })
        http_config = GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "grafana_url": "http://grafana.example/grafana",
        })

        self.assertTrue(is_grafana_login_url(
            "https://grafana.example:443/grafana/login",
            https_config,
        ))
        self.assertTrue(is_grafana_login_url(
            "http://grafana.example:80/grafana/login",
            http_config,
        ))
        self.assertFalse(is_grafana_login_url(
            "https://grafana.example:444/grafana/login",
            https_config,
        ))

        ipv6_config = GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "grafana_url": "http://[::1]/grafana",
        })
        self.assertTrue(is_grafana_login_url(
            "http://[::1]:80/grafana/login",
            ipv6_config,
        ))

    def create_runner(self, loading_states=None, timeout=5, **readiness_overrides):
        config = self.create_config(timeout=timeout, **readiness_overrides)
        page = FakePage(loading_states)
        playwright_screenshots.time.monotonic = lambda: page.current_ms / 1000
        browser = FakeBrowser(page)
        collector = PlaywrightResponseCollector(page)
        return PlaywrightPanelScreenshotRunner(FakeManager(config)), browser, collector

    @staticmethod
    def create_config(timeout=5, **readiness_overrides):
        readiness = {
            "network_idle_ms": 750,
            "no_network_grace_ms": 1000,
            "min_settle_ms": 0,
            "poll_interval_ms": 100,
        }
        readiness.update(readiness_overrides)
        return GrafanaConfigDownloader("demo", {
            "dash_title": "Dashboard",
            "grafana_url": "https://grafana.example",
            "timeout": timeout,
            "screenshot_readiness": readiness,
        })


if __name__ == "__main__":
    unittest.main()
