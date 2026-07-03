import logging
from http.cookiejar import Cookie
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger('grafconflux.grafana')

DEFAULT_PLAYWRIGHT_BROWSER = 'chromium'
SUPPORTED_PLAYWRIGHT_BROWSERS = {'chromium', 'firefox', 'webkit'}


class GrafanaBrowserSession:
    """Create and authenticate a Playwright browser for Grafana UI flows."""

    def __init__(
        self,
        config,
        session,
        browser_factory: Optional[Callable] = None,
        options_factory: Optional[Callable] = None,
        *,
        close_on_setup_failure: bool = True,
        suppress_setup_errors: bool = False,
        require_cookie_domain: bool = False,
        browser_name: str = DEFAULT_PLAYWRIGHT_BROWSER,
    ) -> None:
        self.config = config
        self.session = session
        self.browser_factory = browser_factory
        self.options_factory = options_factory
        self.close_on_setup_failure = close_on_setup_failure
        self.suppress_setup_errors = suppress_setup_errors
        self.require_cookie_domain = require_cookie_domain
        self.browser_name = browser_name
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.snapshot_payloads: List[Dict[str, Any]] = []

    def create_browser(self):
        try:
            return self._create_browser()
        except Exception as error:
            self._close_after_setup_failure()
            if self.suppress_setup_errors:
                logger.error(f'Failed to configure browser: {error}')
                return None
            raise

    def _create_browser(self):
        if self.browser_factory is not None:
            return self._create_factory_browser()
        self.playwright = self._sync_playwright().start()
        self.browser = self._browser_launcher().launch(**self.launch_options())
        self._create_authenticated_context()
        return self

    def launch_options(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {'headless': True}
        self._append_launch_option(options, 'channel', self._config_value('playwright_browser_channel'))
        self._append_launch_option(
            options,
            'executable_path',
            self._config_value('playwright_browser_executable_path'),
        )
        return options

    def _create_factory_browser(self):
        browser = self.browser_factory()
        self.browser = browser
        self.authenticate_browser(browser)
        return browser

    def _browser_launcher(self):
        browser_name = self._effective_browser_name()
        if browser_name == 'firefox':
            return self.playwright.firefox
        if browser_name == 'webkit':
            return self.playwright.webkit
        return self.playwright.chromium

    def _effective_browser_name(self) -> str:
        browser_name = self._config_value('playwright_browser') or self.browser_name
        if browser_name not in SUPPORTED_PLAYWRIGHT_BROWSERS:
            raise ValueError('playwright_browser must be one of: chromium, firefox, webkit')
        return browser_name

    def _config_value(self, option_name: str) -> Optional[str]:
        value = getattr(self.config, option_name, None)
        return value if value != '' else None

    @staticmethod
    def _append_launch_option(options: Dict[str, Any], option_name: str, value: Optional[str]) -> None:
        if value is not None:
            options[option_name] = value

    @staticmethod
    def _sync_playwright():
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:  # pragma: no cover - exercised only without dependency installed.
            raise RuntimeError(
                'Playwright is required for render:false browser screenshots. '
                'Install dependencies and run: python -m playwright install chromium'
            ) from error
        return sync_playwright()

    def refresh_authentication(self) -> None:
        if self.browser_factory is not None:
            self.authenticate_browser(self.browser)
            return
        old_context = self.context
        self._create_authenticated_context()
        if old_context is not None:
            old_context.close()

    def _create_authenticated_context(self) -> None:
        cookies = self.playwright_cookies()
        self.context = self.browser.new_context(**self.context_options())
        if cookies:
            self.context.add_cookies(cookies)
        self.page = self.context.new_page()
        self.page.on('response', self._record_snapshot_response)

    def context_options(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            'viewport': {'width': self.config.width, 'height': self.config.height},
            'ignore_https_errors': not self.config.verify_ssl,
        }
        headers = self.session_headers()
        if headers:
            options['extra_http_headers'] = headers
        return options

    def session_headers(self) -> Dict[str, str]:
        authorization = self.session.headers.get('Authorization')
        return {'Authorization': authorization} if authorization else {}

    def playwright_cookies(self) -> List[Dict[str, Any]]:
        parsed_host = urlparse(self.config.grafana_base_url)
        host = parsed_host.hostname
        matching_cookies = [
            cookie for cookie in self.session.cookies
            if self._cookie_domain_matches_host(cookie.domain, host)
        ]
        self._warn_secure_cookies_on_http(parsed_host, matching_cookies)
        cookies = [
            self._playwright_cookie(cookie, self.config.grafana_base_url, self.config.grafana_app_path)
            for cookie in matching_cookies
        ]
        if self.require_cookie_domain and not cookies:
            logger.warning(f'No Grafana cookies found for host={host}')
        return cookies

    @classmethod
    def _playwright_cookie(
        cls,
        cookie: Cookie,
        grafana_base_url: str,
        grafana_app_path: str = '',
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            'name': cookie.name,
            'value': cookie.value,
        }
        if cookie.domain_specified:
            result['domain'] = cookie.domain
            result['path'] = cls._cookie_path(cookie.path, grafana_app_path)
        else:
            result['url'] = cls._host_only_cookie_url(grafana_base_url, cookie.path, grafana_app_path)
        if cookie.expires is not None:
            result['expires'] = int(cookie.expires)
        cls._append_cookie_flags(cookie, result)
        return result

    @staticmethod
    def _warn_secure_cookies_on_http(parsed_host, cookies: List[Cookie]) -> None:
        if parsed_host.scheme.lower() != 'http' or not any(cookie.secure for cookie in cookies):
            return
        logger.warning(
            f'Secure Grafana cookies are scoped to an http origin and may be ignored host={parsed_host.hostname}'
        )

    @staticmethod
    def _host_only_cookie_url(grafana_base_url: str, cookie_path: str, grafana_app_path: str) -> str:
        normalized_path = GrafanaBrowserSession._cookie_path(cookie_path, grafana_app_path)
        if not normalized_path.startswith('/'):
            normalized_path = f'/{normalized_path}'
        parsed = urlparse(grafana_base_url)
        return urlunparse((parsed.scheme, parsed.netloc, normalized_path, '', '', ''))

    @staticmethod
    def _cookie_path(cookie_path: Optional[str], grafana_app_path: str) -> str:
        if grafana_app_path and cookie_path in (None, '', '/'):
            return grafana_app_path
        return cookie_path or '/'

    @staticmethod
    def _append_cookie_flags(cookie: Cookie, result: Dict[str, Any]) -> None:
        result['secure'] = bool(cookie.secure)
        if cookie.has_nonstandard_attr('HttpOnly'):
            result['httpOnly'] = True
        same_site = cookie.get_nonstandard_attr('SameSite')
        if same_site in ('Strict', 'Lax', 'None'):
            result['sameSite'] = same_site

    @staticmethod
    def _cookie_domain_matches_host(domain: str, host: Optional[str]) -> bool:
        if host is None:
            return False
        if not domain:
            return True
        normalized_domain = domain.lstrip('.')
        return host == normalized_domain or host.endswith(f'.{normalized_domain}')

    def authenticate_browser(self, browser=None, cookies: Optional[dict] = None) -> None:
        if browser is None or browser is self:
            self.refresh_authentication()
            return
        browser.get(self.config.grafana_base_url)
        for cookie in self.grafana_cookies().values() if cookies is None else cookies.values():
            browser.add_cookie(cookie)
        browser.set_page_load_timeout(self.config.timeout)

    def apply_session_headers(self, browser=None) -> None:
        if browser is not None and browser is not self:
            headers = self.session_headers()
            if headers:
                browser.header_overrides = headers

    def grafana_cookies(self) -> Dict[str, Dict[str, Any]]:
        return {cookie['name']: cookie for cookie in self.playwright_cookies()}

    def get(self, url: str):
        return self.page.goto(url, wait_until='domcontentloaded', timeout=self.config.timeout * 1000)

    def execute_script(self, script: str, *args: Any) -> Any:
        if args:
            return self.page.evaluate('(payload) => Function("arguments", payload.script)(payload.args)', {
                'script': script,
                'args': list(args),
            })
        return self.page.evaluate(f'() => {{ {script} }}')

    def save_screenshot(self, file_path: str) -> None:
        self.page.screenshot(path=file_path, full_page=False)

    @property
    def current_url(self) -> str:
        return self.page.url if self.page is not None else ''

    def quit(self) -> None:
        self.close()

    def close(self) -> None:
        for resource in (self.context, self.browser):
            if resource is not None:
                resource.close()
        if self.playwright is not None:
            self.playwright.stop()

    def _close_after_setup_failure(self) -> None:
        if self.close_on_setup_failure:
            self.close()

    def _record_snapshot_response(self, response: Any) -> None:
        if not _is_snapshot_response(response):
            return
        try:
            payload = response.json()
        except Exception:
            return
        if isinstance(payload, dict):
            self.snapshot_payloads.append(payload)


def _is_snapshot_response(response: Any) -> bool:
    try:
        request = response.request
        return request.method.upper() == 'POST' and urlparse(response.url).path.rstrip('/').endswith('/api/snapshots')
    except Exception:
        return False
