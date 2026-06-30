import unittest
from unittest.mock import Mock

from grafconflux.browser_session import GrafanaBrowserSession
from grafconflux.grafana import GrafanaConfigDownloader


class FakeOptions:
    def __init__(self) -> None:
        self.arguments: list[str] = []
        self.accept_insecure_certs = False

    def add_argument(self, argument: str) -> None:
        self.arguments.append(argument)


class RecordingBrowser:
    def __init__(self, failing_step: str | None = None) -> None:
        self.failing_step = failing_step
        self.get_calls: list[str] = []
        self.cookies: list[dict] = []
        self.timeouts: list[int] = []
        self.quit_calls = 0

    def get(self, url: str) -> None:
        self.get_calls.append(url)
        if self.failing_step == "get":
            raise RuntimeError("get")

    def add_cookie(self, cookie: dict) -> None:
        self.cookies.append(cookie)
        if self.failing_step == "add_cookie":
            raise RuntimeError("add_cookie")

    def set_page_load_timeout(self, timeout: int) -> None:
        self.timeouts.append(timeout)

    def quit(self) -> None:
        self.quit_calls += 1


class TestGrafanaBrowserSession(unittest.TestCase):
    def create_config(self, **overrides):
        config = {
            "dash_title": "Dashboard",
            "host": "https://grafana.example",
            "width": 1234,
            "height": 567,
            "timeout": 42,
            "verify_ssl": False,
        }
        config.update(overrides)
        return GrafanaConfigDownloader("demo", config)

    def test_browser_session_configures_firefox_wire_cookies_and_timeout(self):
        browser = RecordingBrowser()
        browser_factory = Mock(return_value=browser)
        session = Mock()
        session.cookies = self.cookie_jar()

        result = GrafanaBrowserSession(
            self.create_config(),
            session,
            browser_factory,
            FakeOptions,
        ).create_browser()

        self.assertIs(result, browser)
        options = browser_factory.call_args.kwargs["options"]
        self.assertEqual(options.arguments, ["--headless", "--disable-gpu", "--width=1234", "--height=567"])
        self.assertTrue(options.accept_insecure_certs)
        self.assertEqual(
            browser_factory.call_args.kwargs["seleniumwire_options"],
            {
                "network.stricttransportsecurity.preloadlist": False,
                "network.stricttransportsecurity.enabled": False,
            },
        )
        self.assertEqual(browser.get_calls, ["https://grafana.example"])
        self.assertEqual([cookie["name"] for cookie in browser.cookies], ["grafana_session"])
        self.assertEqual(browser.timeouts, [42])

    def test_browser_session_quits_browser_when_authentication_fails(self):
        browser = RecordingBrowser(failing_step="add_cookie")
        browser_factory = Mock(return_value=browser)
        session = Mock()
        session.cookies = self.cookie_jar()

        with self.assertRaisesRegex(RuntimeError, "add_cookie"):
            GrafanaBrowserSession(
                self.create_config(),
                session,
                browser_factory,
                FakeOptions,
            ).create_browser()

        self.assertEqual(browser.quit_calls, 1)

    def test_browser_session_suppresses_browser_factory_failure(self):
        browser_factory = Mock(side_effect=RuntimeError("factory"))
        session = Mock()
        session.cookies = self.cookie_jar()

        with self.assertLogs("grafconflux.grafana", level="ERROR"):
            result = GrafanaBrowserSession(
                self.create_config(),
                session,
                browser_factory,
                FakeOptions,
                suppress_setup_errors=True,
            ).create_browser()

        self.assertIsNone(result)

    def test_snapshot_style_browser_session_raises_factory_failure(self):
        browser_factory = Mock(side_effect=RuntimeError("factory"))
        session = Mock()
        session.cookies = self.cookie_jar()

        with self.assertRaisesRegex(RuntimeError, "factory"):
            GrafanaBrowserSession(
                self.create_config(),
                session,
                browser_factory,
                FakeOptions,
            ).create_browser()

    def test_browser_session_suppresses_missing_required_cookie_domain(self):
        browser_factory = Mock()
        session = Mock()
        session.cookies = self.cookie_jar(domain="other.example")

        with self.assertLogs("grafconflux.grafana", level="ERROR"):
            result = GrafanaBrowserSession(
                self.create_config(),
                session,
                browser_factory,
                FakeOptions,
                suppress_setup_errors=True,
                require_cookie_domain=True,
            ).create_browser()

        self.assertIsNone(result)
        browser_factory.assert_not_called()

    @staticmethod
    def cookie_jar(domain="grafana.example"):
        import requests

        jar = requests.cookies.RequestsCookieJar()
        jar.set("grafana_session", "cookie", domain=domain, path="/")
        jar.set("other_session", "ignored", domain="other.example", path="/")
        return jar


if __name__ == "__main__":
    unittest.main()
