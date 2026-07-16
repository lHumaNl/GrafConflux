import unittest
from http.cookiejar import Cookie
from unittest.mock import Mock

import requests

from grafconflux.browser_session import GrafanaBrowserSession
from grafconflux.grafana import GrafanaConfigDownloader


class TestGrafanaBrowserSession(unittest.TestCase):
    def create_config(self, **overrides):
        config = {
            "dash_title": "Dashboard",
            "grafana_url": "https://grafana.example",
            "width": 1234,
            "height": 567,
            "timeout": 42,
            "verify_ssl": False,
        }
        config.update(overrides)
        return GrafanaConfigDownloader("demo", config)

    def test_context_options_include_viewport_ssl_and_authorization_header(self):
        session = Mock()
        session.headers = {"Authorization": "Bearer token"}
        session.cookies = self.cookie_jar()

        browser_session = GrafanaBrowserSession(self.create_config(), session)

        self.assertEqual(browser_session.context_options(), {
            "viewport": {"width": 1234, "height": 567},
            "ignore_https_errors": True,
            "extra_http_headers": {"Authorization": "Bearer token"},
        })

    def test_context_options_omit_authorization_header_when_absent(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()

        browser_session = GrafanaBrowserSession(self.create_config(), session)

        self.assertNotIn("extra_http_headers", browser_session.context_options())

    def test_launch_options_include_configured_browser_channel_and_path(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()
        config = self.create_config(
            playwright_browser_channel="chrome",
            playwright_browser_executable_path="C:/Browsers/chrome.exe",
        )

        browser_session = GrafanaBrowserSession(config, session)

        self.assertEqual(browser_session.launch_options(), {
            "headless": True,
            "channel": "chrome",
            "executable_path": "C:/Browsers/chrome.exe",
        })

    def test_configured_browser_name_selects_launcher(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()
        config = self.create_config(playwright_browser="firefox")
        browser_session = GrafanaBrowserSession(config, session)
        browser_session.playwright = Mock()

        launcher = browser_session._browser_launcher()

        self.assertIs(launcher, browser_session.playwright.firefox)

    def test_invalid_configured_browser_name_raises_clear_error(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()
        config = self.create_config(playwright_browser="opera")
        browser_session = GrafanaBrowserSession(config, session)

        with self.assertRaisesRegex(ValueError, "playwright_browser"):
            browser_session._effective_browser_name()

    def test_playwright_cookies_convert_domain_host_only_and_flags(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()

        cookies = GrafanaBrowserSession(self.create_config(), session).playwright_cookies()

        self.assertEqual({cookie["name"] for cookie in cookies}, {"grafana_session", "host_only"})
        cookies_by_name = {cookie["name"]: cookie for cookie in cookies}
        domain_cookie = cookies_by_name["grafana_session"]
        self.assertEqual(domain_cookie["domain"], ".grafana.example")
        self.assertEqual(domain_cookie["path"], "/")
        self.assertTrue(domain_cookie["secure"])
        self.assertTrue(domain_cookie["httpOnly"])
        self.assertEqual(domain_cookie["sameSite"], "Lax")
        host_only_cookie = cookies_by_name["host_only"]
        self.assertEqual(host_only_cookie["url"], "https://grafana.example/monitoring")
        self.assertNotIn("domain", host_only_cookie)
        self.assertNotIn("path", host_only_cookie)

    def test_host_only_cookie_url_uses_grafana_app_path_when_cookie_path_is_missing(self):
        session = Mock()
        session.headers = {}
        session.cookies = requests.cookies.RequestsCookieJar()
        session.cookies.set_cookie(self.host_only_cookie(path=""))

        cookies = GrafanaBrowserSession(
            self.create_config(grafana_url="https://grafana.example/monitoring"),
            session,
        ).playwright_cookies()

        self.assertEqual(cookies[0]["url"], "https://grafana.example/monitoring")

    def test_cookie_paths_are_scoped_to_grafana_app_path(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar(include_host_only=False)

        cookies = GrafanaBrowserSession(
            self.create_config(grafana_url="https://grafana.example/grafana"),
            session,
        ).playwright_cookies()

        self.assertEqual(cookies[0]["path"], "/grafana")

    def test_single_label_host_local_cookies_are_exported_as_url_scoped(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.single_label_cookie_jar()

        cookies = GrafanaBrowserSession(
            self.create_config(grafana_url="https://grafana-host/grafana"),
            session,
        ).playwright_cookies()

        self.assertEqual(
            {cookie["name"] for cookie in cookies},
            {"grafana_session", "grafana_session_expiry"},
        )
        for cookie in cookies:
            self.assertEqual(cookie["url"], "https://grafana-host/grafana")
            self.assertNotIn("domain", cookie)
            self.assertNotIn("path", cookie)
            self.assertEqual(cookie["sameSite"], "Lax")
            self.assertTrue(cookie["httpOnly"])

    def test_single_label_domain_cookie_is_url_scoped_when_host_matches(self):
        session = Mock()
        session.headers = {}
        session.cookies = requests.cookies.RequestsCookieJar()
        session.cookies.set(
            "grafana_session",
            "cookie",
            domain="grafana-host",
            path="/grafana",
            secure=True,
            rest={"SameSite": "Lax", "HttpOnly": None},
        )

        cookies = GrafanaBrowserSession(
            self.create_config(grafana_url="https://grafana-host/grafana"),
            session,
        ).playwright_cookies()

        self.assertEqual(cookies[0]["url"], "https://grafana-host/grafana")
        self.assertNotIn("domain", cookies[0])
        self.assertNotIn("path", cookies[0])

    def test_secure_cookie_on_http_origin_logs_warning(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar(include_host_only=False)

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            GrafanaBrowserSession(self.create_config(grafana_url="http://grafana.example"), session).playwright_cookies()

        self.assertIn("Secure Grafana cookies", "\n".join(logs.output))

    def test_required_cookie_domain_logs_warning_without_failing_conversion(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar(domain="other.example", include_host_only=False)

        with self.assertLogs("grafconflux.grafana", level="WARNING") as logs:
            cookies = GrafanaBrowserSession(
                self.create_config(),
                session,
                require_cookie_domain=True,
            ).playwright_cookies()

        self.assertEqual(cookies, [])
        self.assertIn("No Grafana cookies found", "\n".join(logs.output))

    def test_refresh_authentication_keeps_old_context_when_candidate_setup_fails(self):
        session = Mock()
        session.headers = {}
        session.cookies = self.cookie_jar()
        browser_session = GrafanaBrowserSession(self.create_config(), session)
        old_context = Mock()
        old_page = Mock()
        candidate_context = Mock()
        candidate_context.add_cookies.side_effect = RuntimeError("cookie setup failed")
        browser_session.browser = Mock()
        browser_session.browser.new_context.return_value = candidate_context
        browser_session.context = old_context
        browser_session.page = old_page

        with self.assertRaisesRegex(RuntimeError, "cookie setup failed"):
            browser_session.refresh_authentication()

        self.assertIs(browser_session.context, old_context)
        self.assertIs(browser_session.page, old_page)
        old_context.close.assert_not_called()
        candidate_context.close.assert_called_once()

    def test_refresh_authentication_publishes_generation_then_closes_old_context(self):
        session = Mock()
        session.headers = {"Authorization": "Bearer refreshed-token"}
        session.cookies = self.cookie_jar()
        browser_session = GrafanaBrowserSession(self.create_config(), session, auth_generation=2)
        old_context = Mock()
        old_page = Mock()
        candidate_context = Mock()
        candidate_page = Mock()
        candidate_context.new_page.return_value = candidate_page
        browser_session.browser = Mock()
        browser_session.browser.new_context.return_value = candidate_context
        browser_session.context = old_context
        browser_session.page = old_page

        browser_session.refresh_authentication(auth_generation=3)

        self.assertIs(browser_session.context, candidate_context)
        self.assertIs(browser_session.page, candidate_page)
        self.assertEqual(browser_session.auth_generation, 3)
        self.assertEqual(
            browser_session.browser.new_context.call_args.kwargs["extra_http_headers"],
            {"Authorization": "Bearer refreshed-token"},
        )
        old_context.close.assert_called_once()

    @staticmethod
    def cookie_jar(domain=".grafana.example", include_host_only=True):
        jar = requests.cookies.RequestsCookieJar()
        jar.set(
            "grafana_session",
            "cookie",
            domain=domain,
            path="/",
            secure=True,
            rest={"SameSite": "Lax", "HttpOnly": None},
        )
        jar.set("other_session", "ignored", domain="other.example", path="/")
        if include_host_only:
            jar.set("host_only", "cookie", path="/monitoring")
        return jar

    @staticmethod
    def host_only_cookie(path):
        return Cookie(
            0, "host_only", "cookie", None, False, "", False, False,
            path, bool(path), False, None, True, None, None, {}, False,
        )

    @staticmethod
    def single_label_cookie_jar():
        jar = requests.cookies.RequestsCookieJar()
        cookie_args = {
            "domain": "grafana-host.local",
            "path": "/grafana",
            "secure": True,
            "rest": {"SameSite": "Lax", "HttpOnly": None},
        }
        jar.set("grafana_session", "cookie", **cookie_args)
        jar.set("grafana_session_expiry", "cookie", **cookie_args)
        return jar


if __name__ == "__main__":
    unittest.main()
