import logging
from typing import Callable, Type
from urllib.parse import urlparse

from selenium.webdriver.firefox.options import Options

logger = logging.getLogger('grafconflux.grafana')


class GrafanaBrowserSession:
    """Create and authenticate a Selenium browser for Grafana UI flows."""

    def __init__(
        self,
        config,
        session,
        browser_factory: Callable,
        options_factory: Type[Options],
        *,
        close_on_setup_failure: bool = True,
        suppress_setup_errors: bool = False,
        require_cookie_domain: bool = False,
    ) -> None:
        self.config = config
        self.session = session
        self.browser_factory = browser_factory
        self.options_factory = options_factory
        self.close_on_setup_failure = close_on_setup_failure
        self.suppress_setup_errors = suppress_setup_errors
        self.require_cookie_domain = require_cookie_domain

    def create_browser(self):
        browser = None
        try:
            cookies = self.grafana_cookies()
            browser = self.browser_factory(
                options=self.firefox_options(),
                seleniumwire_options=self.selenium_wire_options(),
            )
            self.apply_session_headers(browser)
            self.authenticate_browser(browser, cookies)
        except Exception as error:
            if browser is not None and self.close_on_setup_failure:
                browser.quit()
            if self.suppress_setup_errors:
                logger.error(f'Failed to configure browser: {error}')
                return None
            raise
        return browser

    def firefox_options(self) -> Options:
        firefox_options = self.options_factory()
        firefox_options.page_load_strategy = 'eager'
        firefox_options.add_argument('--headless')
        firefox_options.add_argument('--disable-gpu')
        firefox_options.add_argument(f'--width={self.config.width}')
        firefox_options.add_argument(f'--height={self.config.height}')
        if not self.config.verify_ssl:
            firefox_options.accept_insecure_certs = True
        return firefox_options

    @staticmethod
    def selenium_wire_options() -> dict:
        return {
            'network.stricttransportsecurity.preloadlist': False,
            'network.stricttransportsecurity.enabled': False,
        }

    def authenticate_browser(self, browser, cookies: dict | None = None) -> None:
        cookies = self.grafana_cookies() if cookies is None else cookies
        browser.get(self.config.host)
        for cookie in cookies.values():
            browser.add_cookie(cookie)
        browser.set_page_load_timeout(self.config.timeout)

    def apply_session_headers(self, browser) -> None:
        authorization = self.session.headers.get('Authorization')
        if authorization:
            browser.header_overrides = {'Authorization': authorization}

    def grafana_cookies(self) -> dict:
        grafana_host = urlparse(self.config.host).hostname
        cookie_domains = self.session.cookies._cookies
        host_cookies = {
            path: cookies
            for domain, paths in cookie_domains.items()
            if self._cookie_domain_matches_host(domain, grafana_host)
            for path, cookies in paths.items()
        }
        if self.require_cookie_domain and not host_cookies:
            logger.warning(f'No Grafana cookies found for host={grafana_host}')
        return {
            name: cookie.__dict__
            for cookies in host_cookies.values()
            for name, cookie in cookies.items()
        }

    @staticmethod
    def _cookie_domain_matches_host(domain: str, host: str | None) -> bool:
        if host is None:
            return False
        normalized_domain = domain.lstrip('.')
        return host == normalized_domain or host.endswith(f'.{normalized_domain}')
