import json
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple, Type
from urllib.parse import urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
try:
    from seleniumwire.utils import decode as seleniumwire_decode
except ImportError:  # pragma: no cover - selenium-wire always provides this in supported installs.
    seleniumwire_decode = None

from grafconflux._shared.time import GrafanaTimeDownloader
from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._shared.grafana_models import SNAPSHOT_DELETE_FIELDS

logger = logging.getLogger('grafconflux.grafana')


def snapshot_api_url(host: str, nginx_prefix: str, path: str) -> str:
    prefix = nginx_prefix if nginx_prefix else ''
    return f'{host}{prefix}{path}'


def snapshot_name(dashboard_name: str, timestamp: GrafanaTimeDownloader) -> str:
    return f'{dashboard_name}__{timestamp.time_tag}'


def snapshot_backup_file(test_folder: str, dashboard_name: str, timestamp: GrafanaTimeDownloader) -> str:
    return os.path.join(test_folder, f'{snapshot_name(dashboard_name, timestamp)}.json')


def without_delete_fields(snapshot_json: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(snapshot_json, dict):
        return snapshot_json
    return {key: value for key, value in snapshot_json.items() if key not in SNAPSHOT_DELETE_FIELDS}


def write_json_file(output_file: str, snapshot_json: Dict[str, Any]) -> None:
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(json.dumps(snapshot_json, ensure_ascii=False, sort_keys=False))


def snapshot_key_from_url(snapshot_link: str) -> str:
    parsed = urlparse(snapshot_link)
    snapshot_path = parsed.path or snapshot_link
    snapshot_key = snapshot_path.rstrip('/').split('/')[-1]
    if not snapshot_key:
        raise ValueError(f'Snapshot URL has no key: {snapshot_link}')
    return snapshot_key


def is_snapshot_post_request(request: Any) -> bool:
    method = str(getattr(request, 'method', '')).upper()
    path = urlparse(str(getattr(request, 'url', ''))).path.rstrip('/')
    return method == 'POST' and path.endswith('/api/snapshots')


def snapshot_response_payload(response: Any) -> Dict[str, Any]:
    if response is None:
        return {}
    try:
        payload = json.loads(snapshot_response_text(response))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def snapshot_response_text(response: Any) -> str:
    body = getattr(response, 'body', b'') or b''
    if isinstance(body, str):
        return body
    headers = getattr(response, 'headers', {}) or {}
    encoding = headers.get('Content-Encoding') if hasattr(headers, 'get') else None
    if seleniumwire_decode is not None and encoding:
        try:
            body = seleniumwire_decode(body, encoding)
        except Exception:
            pass
    return body.decode('utf-8', errors='replace')


def snapshot_url_from_payload(payload: Dict[str, Any], host: str) -> Optional[str]:
    for key in ('url', 'externalUrl', 'snapshotUrl'):
        snapshot_link = normalize_snapshot_url(str(payload.get(key) or ''), host)
        if snapshot_link:
            return snapshot_link
    snapshot_key = payload.get('key')
    return snapshot_url_from_key(str(snapshot_key), host) if snapshot_key else None


def normalize_snapshot_url(snapshot_link: str, host: str) -> Optional[str]:
    if '/dashboard/snapshot/' not in snapshot_link:
        return None
    if snapshot_link.startswith('http'):
        return snapshot_link
    return f'{host}{snapshot_link}' if snapshot_link.startswith('/') else None


def snapshot_url_from_key(snapshot_key: str, host: str) -> str:
    return f'{host}/dashboard/snapshot/{snapshot_key}'


def snapshot_url_from_lookup_response(response: Any, snapshot_name_value: str, host: str,
                                      dashboard_name: str) -> Optional[str]:
    if response.status_code != 200:
        logger.warning(f'Snapshot lookup failed dashboard={dashboard_name} status={response.status_code}')
        return None
    try:
        snapshots = response.json()
    except Exception as error:
        logger.warning(f'Snapshot lookup returned invalid JSON dashboard={dashboard_name} error={error}')
        return None
    if not isinstance(snapshots, list):
        logger.warning(f'Snapshot lookup returned invalid payload dashboard={dashboard_name}')
        return None
    matches = [
        item for item in snapshots
        if isinstance(item, dict) and item.get('name') == snapshot_name_value
    ]
    if len(matches) != 1:
        logger.warning(
            f'Snapshot lookup did not find unique match dashboard={dashboard_name} matches={len(matches)}'
        )
        return None
    return snapshot_url_from_payload(matches[0], host)


class SnapshotUiRunner:
    """Run the Selenium UI snapshot flow for a Grafana manager."""

    def __init__(self, manager, browser_factory: Callable, options_factory: Type[Options]) -> None:
        self.manager = manager
        self.browser_factory = browser_factory
        self.options_factory = options_factory

    def take_snapshots(self, timestamps: List[GrafanaTimeDownloader], test_folder: str) -> None:
        browser = self._create_browser()
        try:
            for timestamp in timestamps:
                self._take_snapshot(browser, timestamp, test_folder)
        finally:
            browser.quit()

    def _create_browser(self):
        return self._browser_session().create_browser()

    def _browser_session(self) -> GrafanaBrowserSession:
        return GrafanaBrowserSession(
            self.manager.config,
            self.manager.session,
            self.browser_factory,
            self.options_factory,
        )

    def _firefox_options(self) -> Options:
        return self._browser_session().firefox_options()

    @staticmethod
    def _selenium_wire_options() -> dict:
        return GrafanaBrowserSession.selenium_wire_options()

    def _authenticate_browser(self, browser) -> None:
        self._browser_session().authenticate_browser(browser)

    def _grafana_cookies(self) -> dict:
        return self._browser_session().grafana_cookies()

    def _take_snapshot(self, browser, timestamp: GrafanaTimeDownloader, test_folder: str) -> None:
        try:
            self.manager._create_ui_snapshot(browser, timestamp, test_folder)
        except Exception as error:
            logger.error(f'Failed on dashboard {self.manager.config.name}: {error}', exc_info=True)

    def _open_snapshot_dialog(self, browser) -> bool:
        if self._open_modern_snapshot_dialog(browser):
            return True
        self._open_classic_snapshot_dialog(browser)
        return False

    def _open_modern_snapshot_dialog(self, browser) -> bool:
        if not self._click_first_css(browser, self._modern_share_menu_selectors()):
            return False
        self._sleep(1)
        if self._click_first_css(browser, self._share_snapshot_selectors()):
            self._sleep(1)
            return True
        if self._click_first_xpath(browser, self._share_snapshot_xpaths()):
            self._sleep(1)
            return True
        logger.warning(f'Modern snapshot menu unavailable dashboard={self.manager.config.name} fallback=classic')
        return False

    @staticmethod
    def _modern_share_menu_selectors() -> List[str]:
        return [
            'button[aria-label="Toggle share menu"]',
            'button[data-testid="data-testid new share button arrow menu"]',
            '[data-testid="data-testid new share button arrow menu"]',
            'button[aria-label="Share dashboard menu"]',
            'button[aria-label="Share dashboard options"]',
            'button[aria-label="Open share menu"]',
        ]

    @staticmethod
    def _share_snapshot_selectors() -> List[str]:
        return [
            '[data-testid="data-testid new share button share snapshot"]',
            'button[data-testid="data-testid new share button share snapshot"]',
            'button[aria-label="Share snapshot"]',
        ]

    @staticmethod
    def _share_snapshot_xpaths() -> List[str]:
        return [
            "//button[.//span[normalize-space()='Share snapshot']]",
            "//*[@role='menuitem' and .//*[normalize-space()='Share snapshot']]",
            "//*[self::button or @role='menuitem'][normalize-space()='Share snapshot']",
        ]

    def _open_classic_snapshot_dialog(self, browser) -> None:
        self._click_required_css(browser, 'button[aria-label="Share dashboard"]')
        self._sleep(1)
        self._click_required_css(browser, 'a[aria-label="Tab Snapshot"]')
        self._sleep(1)

    def _click_first_css(self, browser, selectors: List[str]) -> bool:
        return self._click_first(browser, [(By.CSS_SELECTOR, selector) for selector in selectors])

    def _click_first_xpath(self, browser, xpaths: List[str]) -> bool:
        return self._click_first(browser, [(By.XPATH, xpath) for xpath in xpaths])

    def _click_first(self, browser, locators: List[Tuple[str, str]]) -> bool:
        for by, value in locators:
            element = self._find_element(browser, by, value)
            if element is None:
                continue
            element.click()
            return True
        return False

    def _click_required_css(self, browser, selector: str) -> None:
        if not self._click_first_css(browser, [selector]):
            raise ValueError(f'Required snapshot control not found selector={selector}')

    def _click_required_xpath(self, browser, xpath: str) -> None:
        if not self._click_first_xpath(browser, [xpath]):
            raise ValueError(f'Required snapshot control not found xpath={xpath}')

    def _find_css(self, browser, selector: str):
        return self._find_element(browser, By.CSS_SELECTOR, selector)

    @staticmethod
    def _find_element(browser, by: str, value: str):
        try:
            return browser.find_element(by, value)
        except Exception:
            return None

    def _sleep(self, seconds: int) -> None:
        self.manager._snapshot_sleep(seconds)
