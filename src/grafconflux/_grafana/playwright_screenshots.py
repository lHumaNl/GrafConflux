import logging
import re
import time
from typing import Any, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

from grafconflux._shared.grafana_models import sanitize_url_for_log

logger = logging.getLogger('grafconflux.grafana')
URL_IN_TEXT_RE = re.compile(r'https?://\S+')


def _safe_exception_message(error: Exception) -> str:
    message = str(error)
    if not message:
        return ''
    return URL_IN_TEXT_RE.sub(lambda match: sanitize_url_for_log(match.group(0).rstrip('.,;:')),
                              message)

UNAUTHORIZED_STATUSES = {401, 403}
RELEVANT_GRAFANA_API_FRAGMENTS: Tuple[str, ...] = (
    '/api/ds/query',
    '/api/datasources/proxy',
    '/api/datasources/proxy/uid',
    '/api/annotations',
    '/api/dashboard',
    '/api/dashboards/uid',
    '/api/dashboards/db',
    '/api/tsdb/query',
)
PANEL_READY_SELECTOR = ', '.join([
    '[data-testid^="data-testid Panel header"]',
    '[data-testid="data-testid Panel chrome"]',
    '[data-testid="data-testid panel content"]',
    '.panel-container',
    '.react-grid-item',
])


class GrafanaAuthenticationError(RuntimeError):
    """Raised when bounded browser authentication recovery is exhausted."""
SIDEBAR_BUTTON_SELECTORS = [
    'button[aria-label*="Collapse"]',
    'button[aria-label*="Close"]',
    'button[title*="Collapse"]',
    'button[title*="Close"]',
]
LOADING_INDICATOR_SCRIPT = """
() => {
  const selectors = [
    '[data-testid*="Loading"]',
    '[aria-label*="Loading"]',
    '.panel-loading',
    '[class*="spinner"]'
  ];
  return selectors.some((selector) => Array.from(document.querySelectorAll(selector)).some((element) => {
    if (element.closest('[aria-hidden="true"]')) {
      return false;
    }
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== 'none'
      && style.visibility !== 'hidden'
      && Number(style.opacity || '1') > 0
      && rect.width > 0
      && rect.height > 0;
  }));
}
"""


class PlaywrightResponseCollector:
    """Collect network responses scoped to one Playwright navigation attempt."""

    def __init__(self, page: Any) -> None:
        self.page = page
        self.responses: List[Any] = []
        self._inflight_relevant: Set[int] = set()
        self._seen_relevant = False
        self._last_relevant_activity_ms: Optional[int] = None

    def __enter__(self):
        self.page.on('request', self._record_request)
        self.page.on('response', self._record)
        self.page.on('requestfinished', self._record_request_done)
        self.page.on('requestfailed', self._record_request_done)
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        try:
            self.page.remove_listener('request', self._record_request)
            self.page.remove_listener('response', self._record)
            self.page.remove_listener('requestfinished', self._record_request_done)
            self.page.remove_listener('requestfailed', self._record_request_done)
        except Exception:
            try:
                self.page.off('request', self._record_request)
                self.page.off('response', self._record)
                self.page.off('requestfinished', self._record_request_done)
                self.page.off('requestfailed', self._record_request_done)
            except Exception:
                pass

    def _record(self, response: Any) -> None:
        self.responses.append(response)
        if is_relevant_grafana_api(response_url(response)):
            self._mark_relevant_activity()

    def _record_request(self, request: Any) -> None:
        if not is_relevant_grafana_api(request_url(request)):
            return
        self._inflight_relevant.add(id(request))
        self._mark_relevant_activity()

    def _record_request_done(self, request: Any) -> None:
        request_id = id(request)
        if request_id not in self._inflight_relevant:
            return
        self._inflight_relevant.remove(request_id)
        self._mark_relevant_activity()

    def _mark_relevant_activity(self) -> None:
        self._seen_relevant = True
        self._last_relevant_activity_ms = monotonic_ms()

    def all_fragments_loaded(self, fragments: List[str]) -> bool:
        return all(self._has_successful_fragment(fragment) for fragment in fragments)

    def unauthorized_status(self, fragments: List[str]) -> Optional[int]:
        for response in self.responses:
            status = response_status(response)
            if status not in UNAUTHORIZED_STATUSES:
                continue
            url = response_url(response)
            if not fragments or any(fragment in url for fragment in fragments) or is_relevant_grafana_api(url):
                return status
        return None

    def has_relevant_requests(self) -> bool:
        return self._seen_relevant

    def relevant_network_idle(self, idle_ms: int) -> bool:
        if self._inflight_relevant or self._last_relevant_activity_ms is None:
            return False
        return monotonic_ms() - self._last_relevant_activity_ms >= idle_ms

    def _has_successful_fragment(self, fragment: str) -> bool:
        return any(fragment in response_url(response) and response_status(response) == 200 for response in self.responses)


class PlaywrightPanelScreenshotRunner:
    """Take Grafana panel screenshots through the Playwright sync API."""

    def __init__(self, manager) -> None:
        self.manager = manager
        self._auth_recovery_used = False

    def take_screenshot(self, browser: Any, task: Any, final_url: str, file_path: str) -> None:
        self._refresh_browser_if_stale(browser)
        auth_generation = getattr(self.manager, '_auth_generation', 0)
        data_sources = self.manager._GrafanaManager__get_panel_data_sources(final_url)
        if getattr(self.manager, '_auth_generation', 0) > auth_generation:
            self._refresh_browser_if_stale(browser)
        fullscreen_state = self._fullscreen_state()
        if fullscreen_state is False:
            self._take_required_route(browser, task, final_url, file_path, data_sources, False)
            return
        if self._try_route(
            browser,
            task,
            f'{final_url}&fullscreen',
            file_path,
            data_sources,
            fullscreen_state is True,
            True,
        ):
            return
        self._recover_browser_for_fallback(browser, task)
        self._take_required_route(browser, task, final_url, file_path, data_sources, False)

    def _take_required_route(self, browser: Any, task: Any, route_url: str, file_path: str,
                             data_sources: List[str], fullscreen_state: Optional[bool]) -> None:
        if self._try_route(browser, task, route_url, file_path, data_sources, True, fullscreen_state):
            return
        raise RuntimeError(f'Failed to take screenshot for panel_id={task.panel.panel_id}')

    def _try_route(self, browser: Any, task: Any, route_url: str, file_path: str,
                   data_sources: List[str], log_errors: bool, fullscreen_state: Optional[bool]) -> bool:
        try:
            self._refresh_browser_if_stale(browser)
            observed_generation = getattr(
                browser,
                'auth_generation',
                getattr(self.manager, '_auth_generation', 0),
            )
            status_code = self._open_validate_and_settle(browser, task, route_url, data_sources)
            if status_code in UNAUTHORIZED_STATUSES:
                if self._auth_recovery_used:
                    raise GrafanaAuthenticationError(
                        f'Grafana authentication recovery exhausted for panel_id={task.panel.panel_id}'
                    )
                self._auth_recovery_used = True
                if not self.manager._reauthenticate_grafana(
                    browser,
                    observed_generation=observed_generation,
                ):
                    raise GrafanaAuthenticationError(
                        f'Grafana authentication recovery failed for panel_id={task.panel.panel_id}'
                    )
                logger.info(f'Retrying panel after re-authentication panel_id={task.panel.panel_id}')
                self._refresh_browser_if_stale(browser)
                status_code = self._open_validate_and_settle(browser, task, route_url, data_sources)
                if status_code in UNAUTHORIZED_STATUSES:
                    raise GrafanaAuthenticationError(
                        f'Grafana authentication recovery exhausted for panel_id={task.panel.panel_id}'
                    )
            if status_code != 200:
                raise RuntimeError(self._route_error_message(route_url, status_code))
            self._set_fullscreen_state(fullscreen_state)
            self._close_sidebar(browser)
            browser.save_screenshot(file_path)
            self.manager._GrafanaManager__record_task_link(task, route_url)
            logger.info(f'Screenshot saved to {file_path}')
            return True
        except Exception as error:
            if log_errors:
                logger.error(
                    f'Failed to take screenshot panel_id={task.panel.panel_id} '
                    f'error_type={type(error).__name__} error={_safe_exception_message(error)}'
                )
            if isinstance(error, GrafanaAuthenticationError):
                raise
            return False

    def _open_validate_and_settle(self, browser: Any, task: Any, route_url: str, data_sources: List[str]) -> Optional[int]:
        collector = PlaywrightResponseCollector(browser.page)
        with collector:
            status_code = self._open_route(browser, task, route_url)
            if status_code in UNAUTHORIZED_STATUSES:
                return status_code
            if status_code != 200:
                return status_code
            if is_grafana_login_url(browser.current_url, self.manager.config):
                return 401
            if not self._loaded_expected_panel(browser, task.panel.panel_id):
                raise RuntimeError(
                    f'Browser did not load expected panel_id={task.panel.panel_id}; '
                    f'current_url={sanitize_url_for_log(browser.current_url)}'
                )
            settle_status = self._wait_for_network_settle(browser, collector, data_sources)
            return settle_status or status_code

    def _open_route(self, browser: Any, task: Any, route_url: str) -> Optional[int]:
        logger.info(f'Opening panel for screenshot panel_id={task.panel.panel_id} url={route_url}')
        try:
            response = browser.get(route_url)
        except Exception as error:
            logger.warning(
                f'Panel navigation failed panel_id={task.panel.panel_id} '
                f'url={route_url} error_type={type(error).__name__} '
                f'error={_safe_exception_message(error)}'
            )
            raise RuntimeError(
                f'Panel navigation failed panel_id={task.panel.panel_id} '
                f'url={route_url} error_type={type(error).__name__} '
                f'error={_safe_exception_message(error)}'
            ) from error
        status_code = response_status(response)
        logger.info(
            f'Panel navigation response panel_id={task.panel.panel_id} '
            f'status={status_code} url={route_url}'
        )
        return status_code

    @staticmethod
    def _recover_browser_for_fallback(browser: Any, task: Any) -> None:
        refresh = getattr(browser, 'refresh_authentication', None)
        if not callable(refresh):
            logger.debug(f'Browser context reset is unavailable panel_id={task.panel.panel_id}')
            return
        try:
            logger.warning(f'Resetting browser context before panel fallback panel_id={task.panel.panel_id}')
            refresh()
        except Exception as error:
            logger.warning(
                f'Browser context reset before panel fallback failed '
                f'panel_id={task.panel.panel_id} error_type={type(error).__name__} '
                f'error={_safe_exception_message(error)}'
            )

    def _wait_for_network_settle(self, browser: Any, collector: PlaywrightResponseCollector,
                                 fragments: List[str]) -> Optional[int]:
        readiness = self.manager.config.screenshot_readiness
        self._wait_for_panel_dom(browser)
        start_ms = monotonic_ms()
        deadline_ms = start_ms + max(0, self.manager.config.timeout) * 1000
        while monotonic_ms() < deadline_ms:
            unauthorized_status = collector.unauthorized_status(fragments)
            if unauthorized_status is not None:
                return unauthorized_status
            if self._readiness_reached(browser, collector, fragments, start_ms):
                return None
            browser.page.wait_for_timeout(readiness.poll_interval_ms)
        return collector.unauthorized_status(fragments)

    def _readiness_reached(self, browser: Any, collector: PlaywrightResponseCollector,
                           fragments: List[str], start_ms: int) -> bool:
        readiness = self.manager.config.screenshot_readiness
        if not self._minimum_settle_elapsed(readiness.min_settle_ms, start_ms):
            return False
        if not self._datasource_fragments_ready(collector, fragments):
            return False
        if not self._loading_indicators_hidden(browser):
            return False
        if collector.has_relevant_requests():
            return collector.relevant_network_idle(readiness.network_idle_ms)
        return monotonic_ms() - start_ms >= readiness.no_network_grace_ms

    @staticmethod
    def _minimum_settle_elapsed(min_settle_ms: int, start_ms: int) -> bool:
        return monotonic_ms() - start_ms >= min_settle_ms

    def _datasource_fragments_ready(self, collector: PlaywrightResponseCollector, fragments: List[str]) -> bool:
        readiness = self.manager.config.screenshot_readiness
        return not readiness.strict_datasource_fragments or collector.all_fragments_loaded(fragments)

    @staticmethod
    def _loading_indicators_hidden(browser: Any) -> bool:
        try:
            return not bool(browser.page.evaluate(LOADING_INDICATOR_SCRIPT))
        except Exception as error:
            logger.debug(f'Grafana loading indicator check skipped: {error}')
            return True

    @staticmethod
    def _wait_for_panel_dom(browser: Any) -> None:
        try:
            browser.page.locator(PANEL_READY_SELECTOR).first.wait_for(timeout=5000)
        except Exception:
            logger.debug('Panel readiness selector wait reached timeout')

    @staticmethod
    def _loaded_expected_panel(browser: Any, panel_id: int) -> bool:
        try:
            return bool(browser.page.evaluate(
                """
                (expected) => {
                  const current = new URL(window.location.href);
                  return current.searchParams.get('panelId') === expected
                    || current.searchParams.get('viewPanel') === expected;
                }
                """,
                str(panel_id),
            ))
        except Exception:
            return url_has_panel_id(browser.current_url, panel_id)

    @staticmethod
    def _close_sidebar(browser: Any) -> None:
        try:
            for selector in SIDEBAR_BUTTON_SELECTORS:
                locator = browser.page.locator(selector).first
                if locator.count():
                    locator.click(timeout=1000)
                    return
            browser.page.evaluate(_hide_sidebar_script())
        except Exception as error:
            logger.debug(f'Grafana sidebar was not closed: {error}')

    def _fullscreen_state(self) -> Optional[bool]:
        return getattr(self.manager.thread_local, 'is_fullscreen', None)

    def _refresh_browser_if_stale(self, browser: Any) -> None:
        manager_generation = getattr(self.manager, '_auth_generation', 0)
        browser_generation = getattr(browser, 'auth_generation', manager_generation)
        if browser_generation >= manager_generation:
            return
        self.manager._refresh_browser_authentication(browser)

    def _set_fullscreen_state(self, fullscreen_state: Optional[bool]) -> None:
        if fullscreen_state is not None:
            self.manager.thread_local.is_fullscreen = fullscreen_state

    @staticmethod
    def _route_error_message(route_url: str, status_code: Optional[int]) -> str:
        if status_code is None:
            return f'Request to {route_url} completed without an HTTP response'
        return f'Request to {route_url} returned HTTP {status_code}'


def response_status(response: Any) -> Optional[int]:
    if response is None:
        return None
    status = getattr(response, 'status', None)
    if status is None:
        status = getattr(response, 'status_code', None)
    return int(status) if status is not None else None


def response_url(response: Any) -> str:
    return str(getattr(response, 'url', ''))


def request_url(request: Any) -> str:
    return str(getattr(request, 'url', ''))


def is_relevant_grafana_api(url: str) -> bool:
    return any(fragment in url for fragment in RELEVANT_GRAFANA_API_FRAGMENTS)


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def url_has_panel_id(current_url: str, panel_id: int) -> bool:
    query = parse_qs(urlparse(current_url).query)
    expected = str(panel_id)
    return expected in query.get('panelId', []) or expected in query.get('viewPanel', [])


def is_grafana_login_url(current_url: Any, config: Any) -> bool:
    if not isinstance(current_url, str) or not current_url:
        return False
    current = urlparse(current_url)
    expected = urlparse(config.grafana_base_url)
    app_path = (getattr(config, 'grafana_app_path', '') or '').rstrip('/')
    expected_path = f'{app_path}/login' if app_path else '/login'
    return (
        current.scheme.lower() == expected.scheme.lower()
        and (current.hostname or '').lower() == (expected.hostname or '').lower()
        and _effective_port(current) == _effective_port(expected)
        and current.path.rstrip('/') == expected_path.rstrip('/')
    )


def _effective_port(parsed: Any) -> Optional[int]:
    if parsed.port is not None:
        return parsed.port
    return 443 if parsed.scheme.lower() == 'https' else 80 if parsed.scheme.lower() == 'http' else None


def _hide_sidebar_script() -> str:
    return """
    () => {
      const sidebar = document.querySelector('nav, aside, [aria-label="Navigation"], [data-testid*="sidemenu"]');
      if (sidebar && sidebar.getBoundingClientRect().width > 120) {
        sidebar.style.display = 'none';
      }
    }
    """
