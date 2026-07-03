import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from grafconflux._shared.time import GrafanaTimeDownloader
from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._shared.grafana_models import SNAPSHOT_DELETE_FIELDS

logger = logging.getLogger('grafconflux.grafana')
CSS_SELECTOR = 'css'
XPATH = 'xpath'


def snapshot_api_url(grafana_base_url: str, path: str) -> str:
    return f'{grafana_base_url}{path}'


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
    body = _response_body(response)
    if isinstance(body, str):
        return body
    return body.decode('utf-8', errors='replace')


def _response_body(response: Any) -> bytes | str:
    body = getattr(response, 'body', b'')
    if callable(body):
        try:
            return body()
        except Exception:
            return b''
    return body or b''


def snapshot_url_from_payload(payload: Dict[str, Any], grafana_base_url: str) -> Optional[str]:
    for key in ('url', 'externalUrl', 'snapshotUrl'):
        snapshot_link = normalize_snapshot_url(str(payload.get(key) or ''), grafana_base_url)
        if snapshot_link:
            return snapshot_link
    snapshot_key = payload.get('key')
    return snapshot_url_from_key(str(snapshot_key), grafana_base_url) if snapshot_key else None


def normalize_snapshot_url(snapshot_link: str, grafana_base_url: str) -> Optional[str]:
    if '/dashboard/snapshot/' not in snapshot_link:
        return None
    if snapshot_link.startswith('http'):
        return _normalize_absolute_snapshot_url(snapshot_link, grafana_base_url)
    if not snapshot_link.startswith('/'):
        return None
    return f'{grafana_base_url}{_snapshot_app_route(snapshot_link, grafana_base_url)}'


def snapshot_url_from_key(snapshot_key: str, grafana_base_url: str) -> str:
    return f'{grafana_base_url}/dashboard/snapshot/{snapshot_key}'


def snapshot_url_from_lookup_response(response: Any, snapshot_name_value: str, grafana_base_url: str,
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
    return snapshot_url_from_payload(matches[0], grafana_base_url)


def _normalize_absolute_snapshot_url(snapshot_link: str, grafana_base_url: str) -> Optional[str]:
    parsed = urlparse(snapshot_link)
    base = urlparse(grafana_base_url)
    if (parsed.scheme, parsed.netloc) != (base.scheme, base.netloc):
        return snapshot_link
    return f'{grafana_base_url}{_snapshot_app_route(parsed.path, grafana_base_url)}'


def _snapshot_app_route(path: str, grafana_base_url: str) -> str:
    app_path = urlparse(grafana_base_url).path.rstrip('/')
    if app_path and (path == app_path or path.startswith(f'{app_path}/')):
        return path[len(app_path):] or '/'
    return path


class SnapshotUiRunner:
    """Run the Playwright UI snapshot flow for a Grafana manager."""

    def __init__(self, manager) -> None:
        self.manager = manager

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
        return GrafanaBrowserSession(self.manager.config, self.manager.session)

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
        return self._click_first(browser, [(CSS_SELECTOR, selector) for selector in selectors])

    def _click_first_xpath(self, browser, xpaths: List[str]) -> bool:
        return self._click_first(browser, [(XPATH, xpath) for xpath in xpaths])

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
        return self._find_element(browser, CSS_SELECTOR, selector)

    @staticmethod
    def _find_element(browser, by: str, value: str):
        try:
            selector = value if by == CSS_SELECTOR else f'xpath={value}'
            locator = browser.page.locator(selector).first
            locator.wait_for(timeout=1000)
            return PlaywrightElement(locator)
        except Exception:
            return None

    def _submit_snapshot_form(self, browser, timestamp: GrafanaTimeDownloader, modern_dialog: bool) -> None:
        self.manager._fill_optional_snapshot_field(browser, 'input[id="snapshot-name-input"]', self.manager._snapshot_name(timestamp))
        timeout_ms = (self.manager.config.snapshot_timeout + 5) * 1000
        if modern_dialog:
            self._click_with_optional_snapshot_wait(browser, '[data-testid="data-testid share snapshot publish button"]', timeout_ms)
        else:
            self.manager._fill_optional_snapshot_field(browser, 'input[id="timeout-input"]', f'{self.manager.config.snapshot_timeout}')
            self._click_xpath_with_optional_snapshot_wait(browser, "//button[.//span[text()='Local Snapshot']]", timeout_ms)
        self._sleep(self.manager.config.snapshot_timeout + 2)

    def _click_with_optional_snapshot_wait(self, browser, selector: str, timeout_ms: int) -> None:
        self._click_control_with_optional_snapshot_wait(browser, CSS_SELECTOR, selector, timeout_ms)

    def _click_xpath_with_optional_snapshot_wait(self, browser, xpath: str, timeout_ms: int) -> None:
        self._click_control_with_optional_snapshot_wait(browser, XPATH, xpath, timeout_ms)

    def _click_control_with_optional_snapshot_wait(self, browser, by: str, value: str, timeout_ms: int) -> None:
        control = self._find_element(browser, by, value)
        if control is None:
            raise ValueError(f'Required snapshot control not found selector={value}')
        clicked = False
        try:
            with browser.page.expect_response(self._is_snapshot_response, timeout=timeout_ms) as response_info:
                control.click()
                clicked = True
            self._record_snapshot_payload(browser, response_info.value)
        except Exception:
            if not clicked:
                control.click()

    @staticmethod
    def _is_snapshot_response(response: Any) -> bool:
        try:
            return response.request.method.upper() == 'POST' and urlparse(response.url).path.rstrip('/').endswith('/api/snapshots')
        except Exception:
            return False

    @staticmethod
    def _record_snapshot_payload(browser: Any, response: Any) -> None:
        try:
            payload = response.json()
        except Exception:
            payload = snapshot_response_payload(response)
        if isinstance(payload, dict):
            if not hasattr(browser, 'snapshot_payloads'):
                browser.snapshot_payloads = []
            browser.snapshot_payloads.append(payload)

    def _sleep(self, seconds: int) -> None:
        self.manager._snapshot_sleep(seconds)


class PlaywrightElement:
    """Small compatibility wrapper for manager snapshot helper methods."""

    def __init__(self, locator: Any) -> None:
        self.locator = locator
        self.target = getattr(locator, 'target', None)

    def click(self) -> None:
        self.locator.click(timeout=3000)

    def clear(self) -> None:
        self.locator.fill('')

    def send_keys(self, *args: Any) -> None:
        for value in args:
            if value == 'PageDown':
                self.locator.press('PageDown')
            else:
                self.locator.fill(str(value))

    def evaluate(self, expression: str) -> Any:
        return self.locator.evaluate(expression)

    def get_attribute(self, name: str) -> Optional[str]:
        return self.locator.get_attribute(name)
