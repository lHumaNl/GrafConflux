import base64
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, wait

import requests
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from lxml import html
import demjson3
import yaml

from grafconflux._shared.time import GrafanaTimeDownloader, GrafanaTimeUploader
from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._grafana.lookup import log_lookup_mode, search_params, select_dashboard
from grafconflux._shared.grafana_models import (
    DEFAULT_INTERVAL_MS,
    DEFAULT_MAX_DATA_POINTS,
    DEFAULT_NO_DATA_TIMEOUT,
    DOWNLOAD_COLLAPSE_PANELS_KEY,
    DOWNLOAD_COLLAPSED_ROWS_KEY,
    DOWNLOAD_HIDDEN_PANELS_KEY,
    INCLUDE_ALL_EXCEPT_EXCLUDED,
    INCLUDE_ONLY_SELECTED,
    NO_DATA_MODE_CONSERVATIVE,
    NO_DATA_ON_ERROR_RENDER,
    NO_DATA_PREFLIGHT_KEY,
    PANEL_FILTERING_KEY,
    PANEL_FILTERING_MODES,
    SKIP_REASON_EMPTY_FRAMES,
    SNAPSHOT_HYDRATION_DWELL_SECONDS,
    SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS,
    SNAPSHOT_HYDRATION_MIN_STEP_PX,
    SNAPSHOT_HYDRATION_SCROLL_LIMIT,
    SNAPSHOT_HYDRATION_STEP_VIEWPORT_FRACTION,
    SNAPSHOT_LOADER_WAIT_INTERVAL_SECONDS,
    SNAPSHOT_LOADER_WAIT_LIMIT,
    SNAPSHOT_MODE_AUTO,
    SNAPSHOT_MODE_LEGACY_API,
    SNAPSHOT_MODE_UI,
    SNAPSHOT_MODES,
    SNAPSHOT_PAGE_DOWN_DWELL_SECONDS,
    SNAPSHOT_ROW_EXPAND_LIMIT,
    SNAPSHOT_ROW_SETTLE_SECONDS,
    SNAPSHOT_ROW_SWEEP_LIMIT,
    SNAPSHOT_ROW_SWEEP_STEP_LIMIT,
    SNAPSHOT_ROW_SWEEP_STEP_VIEWPORT_FRACTION,
    SNAPSHOT_STABLE_SCROLL_LIMIT,
    SUPPORTED_PHASE1_DATASOURCES,
    ConfigurationError,
    DashboardLookupRequest,
    DashboardLookupResult,
    GrafanaConfigBase,
    GrafanaConfigDownloader,
    GrafanaConfigUploader,
    NoDataDatasourceInference,
    NoDataPreflightConfig,
    NoDataPreflightResult,
    Panel,
    PanelDefinition,
    PanelDescriptor,
    PanelFilteringConfig,
    PanelRenderTask,
    _SelectorConfig,
)
from grafconflux._grafana.panel_selection import (
    apply_disabled_graph_type_filter,
    extract_dashboard_panels,
    filter_panel_descriptors,
    filter_runtime_repeat_clones,
    panel_from_descriptor,
    warn_unmatched_filter_selectors,
)
from grafconflux._grafana.no_data import (
    NoDataDetectorRegistry,
    NoDataPreflightRunner,
    _GenericNoDataDetector,
    _datasource_type_uid,
    _has_ambiguous_datasource_refs,
    _has_no_data_unsupported_panel_features,
    _interpret_fields,
    _interpret_frame,
    _interpret_frames,
    _interpret_ref_result,
    _interpret_values,
    _preflight_targets,
    _scoped_vars,
    _target_datasource_ref,
    _value_has_data,
    infer_no_data_datasource,
    interpret_no_data_response,
)
from grafconflux._grafana.repeating import RepeatingPlanner, is_unresolved_repeating_rule
from grafconflux._grafana.rendering import (
    _append_grafana_variables as _append_grafana_variables,
    build_dashboard_url_params as build_dashboard_url_params,
    build_panel_url_params as build_panel_url_params,
    build_render_api_params,
    build_render_api_url,
    build_render_file_path,
)
from grafconflux._grafana.snapshots import (
    SnapshotUiRunner,
    is_snapshot_post_request,
    normalize_snapshot_url,
    snapshot_api_url,
    snapshot_backup_file,
    snapshot_key_from_url,
    snapshot_name,
    snapshot_response_payload,
    snapshot_response_text,
    snapshot_url_from_key,
    snapshot_url_from_lookup_response,
    snapshot_url_from_payload,
    without_delete_fields,
    write_json_file,
)

logger = logging.getLogger(__name__)
logging.getLogger('seleniumwire').setLevel(logging.ERROR)

class GrafanaManager:
    """
    Manages interactions with a Grafana instance.
    """

    def __init__(self, config: GrafanaConfigDownloader):
        self.thread_local = threading.local()
        self.browser_list: Optional[List[webdriver.Firefox]] = []
        self.dashboard_url = ''
        self.config = config
        self.session = requests.Session()
        self.session.verify = self.config.verify_ssl
        self.session.timeout = self.config.timeout
        self.charts_path = ''
        self.dashboard_uid = ''
        self.dashboard_identity: Optional[DashboardLookupResult] = None
        self.dashboard_model: Optional[Dict[str, Any]] = None
        self._render_tasks: List[PanelRenderTask] = []
        self._confluence_login: Optional[str] = None
        self._confluence_password: Optional[str] = None

    @property
    def render_tasks(self) -> List[PanelRenderTask]:
        return self._render_tasks

    def authenticate(self, confluence_login: str, confluence_password: str):
        """
        Authenticate with Grafana using the specified method.
        """
        self._confluence_login = confluence_login
        self._confluence_password = confluence_password
        if not self.config.auth:
            logger.info('Authentication disabled for this Grafana instance.')
            return

        if self.config.domain:
            login = confluence_login.split('@')[0]
            password = confluence_password
        elif (self.config.login and self.config.password) and not self.config.login_url:
            login = self.config.login
            password = self.config.password
        elif self.config.token:
            self.session.headers.update({'Authorization': f'Bearer {self.config.token}'})
            return
        elif self.config.login_url and (self.config.login and self.config.password):
            auth_string = base64.b64encode(f'{self.config.login}:{self.config.password}'.encode()).decode()
            self.session.headers.update({'Authorization': f"Basic {auth_string}"})

            response = self.session.get(self.config.login_url, timeout=self.config.timeout)

            if response.status_code != 200:
                raise ConnectionError('Failed to authenticate with Grafana.')
            logger.info('Successfully authenticated with Grafana.')

            return
        else:
            raise ValueError('No valid authentication method provided.')

        # Authenticate
        payload = {
            'user': login,
            'password': password
        }

        response = self.session.post(
            f'{self.config.host}/login',
            headers={'Content-type': 'application/json'},
            data=json.dumps(payload),
            timeout=self.config.timeout,
        )

        if response.status_code != 200:
            raise ConnectionError('Failed to authenticate with Grafana.')
        logger.info('Successfully authenticated with Grafana.')

    def _reauthenticate_grafana(self, browser: Optional[webdriver.Firefox] = None) -> bool:
        if not self.config.auth:
            return False
        if self._confluence_login is None or self._confluence_password is None:
            logger.error('Cannot re-authenticate Grafana: original credentials are not available')
            return False
        try:
            logger.warning('Grafana session expired; re-authenticating')
            self.session.cookies.clear()
            self.authenticate(self._confluence_login, self._confluence_password)
            if browser is not None:
                self._refresh_browser_authentication(browser)
            return True
        except Exception as error:
            logger.error(f'Grafana re-authentication failed: {error}')
            return False

    def _refresh_browser_authentication(self, browser: webdriver.Firefox) -> None:
        browser.delete_all_cookies()
        browser_session = GrafanaBrowserSession(
            self.config,
            self.session,
            webdriver.Firefox,
            Options,
            require_cookie_domain=True,
        )
        browser_session.apply_session_headers(browser)
        browser_session.authenticate_browser(browser)

    def download_charts(self, test_folder: str, timestamps: List[GrafanaTimeDownloader]):
        """
        Download charts from Grafana.
        """
        self.charts_path = os.path.join(test_folder, self.config.name)
        os.makedirs(self.charts_path, exist_ok=True)
        logger.info(f'Downloading charts to {self.charts_path}')

        # Get dashboard UID,URL
        self.dashboard_uid, self.dashboard_url = self.get_dashboard_uid()

        # Get panels
        self.config.panels = self.get_panels(timestamps)

        self.config.full_links = self.__get_full_links(timestamps)

        self._download_render_tasks()

        if self.config.snapshot:
            self.take_snapshot(timestamps, test_folder)

        self.__save_params_to_file(timestamps, test_folder)

    def _download_render_tasks(self) -> None:
        executor = ThreadPoolExecutor(max_workers=self.config.threads)
        try:
            wait(self._submit_render_tasks(executor))
        finally:
            self._close_browsers()
            executor.shutdown()

    def _submit_render_tasks(self, executor: ThreadPoolExecutor) -> List[Any]:
        futures = []
        for task in self._render_tasks:
            if self._skip_no_data_task(task):
                continue
            futures.append(executor.submit(self.__download_chart, task))
        return futures

    def _close_browsers(self) -> None:
        if not self.browser_list:
            return
        for browser in self.browser_list:
            browser.quit()
        self.browser_list = []

    def _skip_no_data_task(self, task: PanelRenderTask) -> bool:
        return self._no_data_preflight_runner().skip_task(task)

    def _no_data_preflight_runner(self) -> NoDataPreflightRunner:
        return NoDataPreflightRunner(self.config, self.session, self.__build_panel_url, self.__record_task_link)

    def _no_data_preflight_result(self, task: PanelRenderTask) -> NoDataPreflightResult:
        return self._no_data_preflight_runner().result(task)

    def _record_no_data_preflight(self, task: PanelRenderTask, result: NoDataPreflightResult) -> None:
        self._no_data_preflight_runner().record_preflight(task, result)

    def _record_skipped_task_link(self, task: PanelRenderTask) -> None:
        self._no_data_preflight_runner().record_skipped_task_link(task)

    def _ds_query_url(self) -> str:
        return self._no_data_preflight_runner().ds_query_url()

    def take_snapshot(self, timestamps: List[GrafanaTimeDownloader], test_folder: str):
        self._take_snapshot_ui(timestamps, test_folder)

    def _save_snapshot_backup(self, snapshot_key: str, timestamp: GrafanaTimeDownloader, test_folder: str) -> None:
        if not self.config.snapshot_store_dashboard_json:
            return
        try:
            self._write_snapshot_backup(snapshot_key, timestamp, test_folder)
        except Exception as error:
            logger.warning(f'Snapshot backup not saved dashboard={self.config.name} error={error}')

    def _write_snapshot_backup(self, snapshot_key: str, timestamp: GrafanaTimeDownloader, test_folder: str) -> None:
        response = self.session.get(self._snapshot_api_url(f'/api/snapshots/{snapshot_key}'),
                                    verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code != 200:
            raise ConnectionError(f'Failed to download snapshot backup status={response.status_code}')
        output_file = self._snapshot_backup_file(test_folder, timestamp)
        self._write_json_file(output_file, self._without_delete_fields(response.json()))
        logger.info(f'Snapshot backup for {self.config.name} saved in {output_file}')

    def _snapshot_api_url(self, path: str) -> str:
        return snapshot_api_url(self.config.host, self.config.nginx_prefix, path)

    def _snapshot_name(self, timestamp: GrafanaTimeDownloader) -> str:
        return snapshot_name(self.config.name, timestamp)

    def _snapshot_backup_file(self, test_folder: str, timestamp: GrafanaTimeDownloader) -> str:
        return snapshot_backup_file(test_folder, self.config.name, timestamp)

    def _record_snapshot_url(self, snapshot_url: str) -> None:
        if self.config.snapshot_urls is None:
            self.config.snapshot_urls = []
        self.config.snapshot_urls.append(snapshot_url)
        logger.info(f'Link to snapshot {self.config.name}: {snapshot_url}')

    @staticmethod
    def _without_delete_fields(snapshot_json: Dict[str, Any]) -> Dict[str, Any]:
        return without_delete_fields(snapshot_json)

    @staticmethod
    def _write_json_file(output_file: str, snapshot_json: Dict[str, Any]) -> None:
        write_json_file(output_file, snapshot_json)

    def _take_snapshot_ui(self, timestamps: List[GrafanaTimeDownloader], test_folder: str):
        self._snapshot_ui_runner().take_snapshots(timestamps, test_folder)

    def _snapshot_ui_runner(self) -> SnapshotUiRunner:
        return SnapshotUiRunner(self, webdriver.Firefox, Options)

    @staticmethod
    def _snapshot_sleep(seconds: int) -> None:
        time.sleep(seconds)

    def _create_ui_snapshot(self, browser: webdriver.Firefox, timestamp: GrafanaTimeDownloader,
                            test_folder: str) -> None:
        self._open_dashboard_for_snapshot(browser, self.config.full_links[timestamp.id_time])
        self._prepare_dashboard_for_snapshot(browser)
        modern_dialog = self._open_snapshot_dialog(browser)
        self._submit_snapshot_form(browser, timestamp, modern_dialog)
        snapshot_key = self._read_snapshot_key(browser, timestamp)
        self._record_snapshot_url(f'{self.config.host}/dashboard/snapshot/{snapshot_key}')
        self._save_snapshot_backup(snapshot_key, timestamp, test_folder)

    def _open_dashboard_for_snapshot(self, browser: webdriver.Firefox, dashboard_link: str) -> None:
        browser.get(dashboard_link)
        time.sleep(5)
        if self._dashboard_loaded_for_snapshot(browser):
            return
        logger.warning(f'Dashboard route fallback via browser UI navigation dashboard={self.config.name}')
        browser.get(self.config.host)
        time.sleep(2)
        browser.execute_script('window.location.href = arguments[0]; return window.location.href;', dashboard_link)
        time.sleep(5)

    def _dashboard_loaded_for_snapshot(self, browser: webdriver.Firefox) -> bool:
        return bool(browser.execute_script(self._dashboard_loaded_for_snapshot_script()))

    @staticmethod
    def _dashboard_loaded_for_snapshot_script() -> str:
        return """
        return (function grafconfluxDashboardLoadedForSnapshot() {
          const dashboardPath = window.location.pathname.includes('/d/');
          const selectors = [
            'button[aria-label="Toggle share menu"]',
            '[data-testid="data-testid dashboard controls"]',
            'button[data-testid^="data-testid dashboard-row-title-"]',
            'button[data-testid^="dashboard-row-title-"]',
            '[data-testid^="data-testid Panel header"]'
          ];
          return dashboardPath && selectors.some((selector) => document.querySelector(selector));
        })();
        """

    def _prepare_dashboard_for_snapshot(self, browser: webdriver.Firefox) -> None:
        for attempt in range(2):
            self._expand_collapsed_rows(browser)
            self._hydrate_dashboard_panels(browser)
            remaining_count, remaining_titles = self._remaining_collapsed_rows(browser)
            if not remaining_count:
                return
            logger.warning(
                f'Collapsed rows remain dashboard={self.config.name} '
                f'count={remaining_count} rows={remaining_titles} retry={attempt == 0}'
            )

    def _expand_collapsed_rows(self, browser: webdriver.Firefox) -> None:
        expanded_titles = []
        expanded_count = 0
        expected_count = self._expected_collapsed_row_count()
        self._scroll_dashboard_to_edge(browser, 'bottom')
        for _ in range(SNAPSHOT_ROW_SWEEP_LIMIT):
            sweep_count, sweep_titles = self._expand_collapsed_rows_full_sweep(browser)
            expanded_count += sweep_count
            expanded_titles.extend(sweep_titles)
            if sweep_count == 0 and self._expected_rows_expanded(expanded_count, expected_count):
                self._log_expanded_rows(expanded_count, expanded_titles, expected_count)
                return
            time.sleep(SNAPSHOT_ROW_SETTLE_SECONDS)
        self._warn_incomplete_row_expansion(expanded_count, expanded_titles, expected_count)

    def _expand_collapsed_rows_full_sweep(self, browser: webdriver.Firefox) -> Tuple[int, List[str]]:
        up_count, up_titles = self._expand_collapsed_rows_sweep(browser, 'up')
        down_count, down_titles = self._expand_collapsed_rows_sweep(browser, 'down')
        return up_count + down_count, up_titles + down_titles

    def _expand_collapsed_rows_sweep(self, browser: webdriver.Firefox, direction: str) -> Tuple[int, List[str]]:
        expanded_count = 0
        expanded_titles = []
        for _ in range(SNAPSHOT_ROW_SWEEP_STEP_LIMIT):
            count, titles = self._expand_visible_collapsed_rows(browser)
            expanded_count += count
            expanded_titles.extend(titles)
            if count:
                time.sleep(SNAPSHOT_ROW_SETTLE_SECONDS)
            if self._row_sweep_reached_edge(browser, direction):
                break
        return expanded_count, expanded_titles

    def _expand_visible_collapsed_rows(self, browser: webdriver.Firefox) -> Tuple[int, List[str]]:
        result = browser.execute_script(self._expand_visible_collapsed_rows_script())
        return self._expanded_row_result(result)

    def _row_sweep_reached_edge(self, browser: webdriver.Firefox, direction: str) -> bool:
        result = browser.execute_script(self._row_sweep_scroll_script(direction))
        return bool(isinstance(result, dict) and result.get('atEdge'))

    def _scroll_dashboard_to_edge(self, browser: webdriver.Firefox, edge: str) -> None:
        browser.execute_script(self._scroll_dashboard_to_edge_script(edge))

    def _expected_collapsed_row_count(self) -> Optional[int]:
        if not isinstance(self.dashboard_model, dict):
            return None
        panels = self.dashboard_model.get('panels')
        if not isinstance(panels, list):
            return None
        return sum(1 for panel in panels if self._is_collapsed_row_model(panel))

    @staticmethod
    def _is_collapsed_row_model(panel: Any) -> bool:
        return isinstance(panel, dict) and panel.get('type') == 'row' and panel.get('collapsed') is True

    @staticmethod
    def _expected_rows_expanded(expanded_count: int, expected_count: Optional[int]) -> bool:
        return expected_count is None or expanded_count >= expected_count

    def _log_expanded_rows(self, expanded_count: int, row_titles: List[str], expected_count: Optional[int]) -> None:
        if expanded_count:
            logger.info(
                f'Expanded collapsed rows dashboard={self.config.name} count={expanded_count} '
                f'expected={expected_count} rows={row_titles}'
            )

    def _warn_incomplete_row_expansion(self, expanded_count: int, row_titles: List[str],
                                       expected_count: Optional[int]) -> None:
        logger.warning(
            f'Collapsed row expansion reached limit dashboard={self.config.name} '
            f'count={expanded_count} expected={expected_count} rows={row_titles}'
        )

    @staticmethod
    def _expanded_row_result(result: Any) -> Tuple[int, List[str]]:
        if isinstance(result, dict):
            count = int(result.get('count') or 0)
            titles = result.get('titles') or []
            return count, [str(title) for title in titles]
        return int(result or 0), []

    @staticmethod
    def _expand_visible_collapsed_rows_script() -> str:
        return """
        return (function grafconfluxExpandVisibleCollapsedRows() {
          const selectors = [
            'button[data-testid^="data-testid dashboard-row-title-"]',
            'button[data-testid^="dashboard-row-title-"]',
            'button[aria-label="Expand row"]',
            'button[aria-label^="Expand row"]',
            'button[aria-label*="Expand row"]',
            'button[aria-expanded="false"][aria-label^="Expand row"]',
            'button[aria-expanded="false"][aria-label*="Expand row"]',
            '[data-testid="dashboard-row-title"] button[aria-expanded="false"]',
            '[data-testid="dashboard-row-title"] button[aria-label*="Expand row"]',
            '.dashboard-row button[aria-expanded="false"]'
          ];
          const buttons = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
          const collapsed = Array.from(new Set(buttons)).map((button) => ({
            button,
            rect: button.getBoundingClientRect(),
            title: rowTitle(button),
          })).filter((item) => isVisibleCollapsedButton(item));
          collapsed.sort((left, right) => right.rect.top - left.rect.top);
          const titles = [];
          collapsed.forEach((item) => {
            item.button.scrollIntoView({block: 'center', inline: 'nearest'});
            item.button.click();
            titles.push(item.title);
          });
          return {count: collapsed.length, titles};

          function isVisibleCollapsedButton(item) {
            const label = item.button.getAttribute('aria-label') || '';
            const expanded = item.button.getAttribute('aria-expanded');
            const collapsedByLabel = label.includes('Expand row');
            const collapsedByState = expanded === 'false';
            return dashboardRow(item.button) && (collapsedByLabel || collapsedByState)
              && !label.includes('Collapse row') && !item.button.disabled
              && item.rect.width > 0 && item.rect.height > 0 && isInViewport(item.rect);
          }

          function rowTitle(button) {
            const row = dashboardRow(button) || button.parentElement;
            const text = row ? row.textContent.trim().replace(/\s+/g, ' ') : '';
            return text || button.getAttribute('aria-label') || 'collapsed row';
          }

          function dashboardRow(button) {
            if (isDashboardRowTitleButton(button)) {
              return button.closest('.react-grid-item') || button.parentElement;
            }
            return button.closest('[data-testid="dashboard-row-title"], .dashboard-row');
          }

          function isDashboardRowTitleButton(button) {
            const testId = button.getAttribute('data-testid') || '';
            return testId.startsWith('data-testid dashboard-row-title-') || testId.startsWith('dashboard-row-title-');
          }

          function isInViewport(rect) {
            const height = window.innerHeight || document.documentElement.clientHeight || 0;
            return rect.bottom >= 0 && rect.top <= height;
          }
        })();
        """

    def _row_sweep_scroll_script(self, direction: str) -> str:
        step_fraction = str(SNAPSHOT_ROW_SWEEP_STEP_VIEWPORT_FRACTION)
        min_step_px = str(SNAPSHOT_HYDRATION_MIN_STEP_PX)
        return self._row_sweep_scroll_script_template(direction, step_fraction, min_step_px)

    @staticmethod
    def _row_sweep_scroll_script_template(direction: str, step_fraction: str, min_step_px: str) -> str:
        return """
        return (function grafconfluxRowSweepScroll() {
          const direction = 'DIRECTION';
          const container = findScrollContainer();
          const before = scrollTop(container);
          const maximum = Math.max(0, container.scrollHeight - container.clientHeight);
          const step = Math.max(Math.floor((window.innerHeight || container.clientHeight || 800) * STEP_FRACTION), MIN_STEP_PX);
          const target = direction === 'up' ? Math.max(0, before - step) : Math.min(maximum, before + step);
          scrollToPosition(container, target);
          const after = scrollTop(container);
          const atEdge = direction === 'up' ? after <= 0 || after === before : after >= maximum || after === before;
          return {before, after, maximum, direction, atEdge};

          function findScrollContainer() {
            const selectors = ['[data-testid="data-testid Dashboard scroll container"]', '.scrollbar-view'];
            for (const selector of selectors) {
              const element = document.querySelector(selector);
              if (element && element.scrollHeight > element.clientHeight) {
                return element;
              }
            }
            return document.scrollingElement || document.documentElement;
          }

          function scrollTop(element) {
            return Math.floor(element.scrollTop || window.scrollY || 0);
          }

          function scrollToPosition(element, position) {
            element.scrollTo(0, position);
            if (element === document.scrollingElement || element === document.documentElement) {
              window.scrollTo(0, position);
            }
            window.dispatchEvent(new Event('scroll'));
          }
        })();
        """.replace('DIRECTION', direction).replace('STEP_FRACTION', step_fraction).replace('MIN_STEP_PX', min_step_px)

    def _scroll_dashboard_to_edge_script(self, edge: str) -> str:
        return self._scroll_dashboard_to_edge_script_template(edge)

    @staticmethod
    def _scroll_dashboard_to_edge_script_template(edge: str) -> str:
        return """
        return (function grafconfluxScrollDashboardToEdge() {
          const edge = 'EDGE';
          const container = findScrollContainer();
          const position = edge === 'bottom' ? Math.max(0, container.scrollHeight - container.clientHeight) : 0;
          container.scrollTo(0, position);
          if (container === document.scrollingElement || container === document.documentElement) {
            window.scrollTo(0, position);
          }
          window.dispatchEvent(new Event('scroll'));
          return Math.floor(container.scrollTop || window.scrollY || 0);

          function findScrollContainer() {
            const selectors = ['[data-testid="data-testid Dashboard scroll container"]', '.scrollbar-view'];
            for (const selector of selectors) {
              const element = document.querySelector(selector);
              if (element && element.scrollHeight > element.clientHeight) {
                return element;
              }
            }
            return document.scrollingElement || document.documentElement;
          }
        })();
        """.replace('EDGE', edge)

    def _remaining_collapsed_rows(self, browser: webdriver.Firefox) -> Tuple[int, List[str]]:
        result = browser.execute_script(self._remaining_collapsed_rows_script())
        return self._expanded_row_result(result)

    @staticmethod
    def _remaining_collapsed_rows_script() -> str:
        return """
        return (function grafconfluxRemainingCollapsedRows() {
          const selectors = [
            'button[data-testid^="data-testid dashboard-row-title-"][aria-label*="Expand row"]',
            'button[data-testid^="dashboard-row-title-"][aria-label*="Expand row"]',
            'button[aria-label="Expand row"]',
            'button[aria-label^="Expand row"]',
            'button[aria-label*="Expand row"]',
            '[data-testid="dashboard-row-title"] button[aria-expanded="false"]',
            '[data-testid="dashboard-row-title"] button[aria-label*="Expand row"]',
            '.dashboard-row button[aria-expanded="false"]',
            '.dashboard-row button[aria-label*="Expand row"]'
          ];
          const buttons = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
          const collapsed = Array.from(new Set(buttons)).filter((button) => isVisibleCollapsed(button));
          return {count: collapsed.length, titles: collapsed.map((button) => rowTitle(button))};

          function isVisibleCollapsed(button) {
            const rect = button.getBoundingClientRect();
            const label = button.getAttribute('aria-label') || '';
            const collapsedByLabel = label.includes('Expand row');
            const collapsedByState = button.getAttribute('aria-expanded') === 'false';
            return dashboardRow(button) && (collapsedByLabel || collapsedByState)
              && !label.includes('Collapse row') && !button.disabled
              && rect.width > 0 && rect.height > 0;
          }

          function rowTitle(button) {
            const row = dashboardRow(button) || button.parentElement;
            const text = row ? row.textContent.trim().replace(/\s+/g, ' ') : '';
            return text || button.getAttribute('aria-label') || 'collapsed row';
          }

          function dashboardRow(button) {
            if (isDashboardRowTitleButton(button)) {
              return button.closest('.react-grid-item') || button.parentElement;
            }
            return button.closest('[data-testid="dashboard-row-title"], .dashboard-row');
          }

          function isDashboardRowTitleButton(button) {
            const testId = button.getAttribute('data-testid') || '';
            return testId.startsWith('data-testid dashboard-row-title-') || testId.startsWith('dashboard-row-title-');
          }
        })();
        """

    def _hydrate_dashboard_panels(self, browser: webdriver.Firefox) -> None:
        self._reset_dashboard_scroll_to_top(browser)
        if self._hydrate_dashboard_panels_with_page_down(browser):
            return
        self._hydrate_dashboard_panels_with_js(browser)

    def _reset_dashboard_scroll_to_top(self, browser: webdriver.Firefox) -> None:
        browser.execute_script(self._reset_dashboard_scroll_top_script())

    @staticmethod
    def _reset_dashboard_scroll_top_script() -> str:
        return """
        return (function grafconfluxResetDashboardScrollTop() {
          const selectors = ['.scrollbar-view', '[data-testid="data-testid Dashboard scroll container"]'];
          const containers = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
          Array.from(new Set(containers)).forEach((container) => container.scrollTo(0, 0));
          window.scrollTo(0, 0);
          const scrollingElement = document.scrollingElement || document.documentElement;
          if (scrollingElement) {
            scrollingElement.scrollTo(0, 0);
          }
          return 0;
        })();
        """

    def _hydrate_dashboard_panels_with_page_down(self, browser: webdriver.Firefox) -> bool:
        scrollable_div = self._find_css(browser, '.scrollbar-view')
        if scrollable_div is None:
            return False
        previous_scroll_position = -1
        for _ in range(SNAPSHOT_HYDRATION_SCROLL_LIMIT):
            scrollable_div.send_keys(Keys.PAGE_DOWN)
            time.sleep(SNAPSHOT_PAGE_DOWN_DWELL_SECONDS)
            self._wait_for_snapshot_loaders(browser)
            current_scroll_position = self._scrollbar_view_scroll_top(browser, scrollable_div)
            if current_scroll_position == previous_scroll_position:
                scrollable_div.send_keys(Keys.PAGE_DOWN)
                time.sleep(SNAPSHOT_PAGE_DOWN_DWELL_SECONDS)
                self._wait_for_snapshot_loaders(browser)
                break
            previous_scroll_position = current_scroll_position
        time.sleep(SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS)
        return True

    @staticmethod
    def _scrollbar_view_scroll_top(browser: webdriver.Firefox, scrollable_div: Any) -> int:
        return int(browser.execute_script('return Math.floor(arguments[0].scrollTop || 0)', scrollable_div) or 0)

    def _hydrate_dashboard_panels_with_js(self, browser: webdriver.Firefox) -> None:
        previous_scroll_position = -1
        stable_scrolls = 0
        for _ in range(SNAPSHOT_HYDRATION_SCROLL_LIMIT):
            result = browser.execute_script(self._hydrate_dashboard_scroll_script())
            time.sleep(SNAPSHOT_HYDRATION_DWELL_SECONDS)
            self._wait_for_snapshot_loaders(browser)
            current_scroll_position = self._scroll_position_result(result)
            stable_scrolls = stable_scrolls + 1 if current_scroll_position == previous_scroll_position else 0
            if stable_scrolls >= SNAPSHOT_STABLE_SCROLL_LIMIT:
                break
            previous_scroll_position = current_scroll_position
        time.sleep(SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS)

    def _wait_for_snapshot_loaders(self, browser: webdriver.Firefox) -> None:
        for _ in range(SNAPSHOT_LOADER_WAIT_LIMIT):
            if not self._snapshot_loaders_busy(browser):
                return
            time.sleep(SNAPSHOT_LOADER_WAIT_INTERVAL_SECONDS)
        logger.debug(f'Snapshot loader wait reached limit dashboard={self.config.name}')

    def _snapshot_loaders_busy(self, browser: webdriver.Firefox) -> bool:
        return bool(browser.execute_script(self._snapshot_loaders_busy_script()))

    @staticmethod
    def _scroll_position_result(result: Any) -> int:
        if isinstance(result, dict):
            return int(result.get('after') or 0)
        return int(result or 0)

    @staticmethod
    def _hydrate_dashboard_scroll_script() -> str:
        step_fraction = str(SNAPSHOT_HYDRATION_STEP_VIEWPORT_FRACTION)
        min_step_px = str(SNAPSHOT_HYDRATION_MIN_STEP_PX)
        return """
        return (function grafconfluxHydrateDashboard() {
          const container = findScrollContainer();
          const before = scrollTop(container);
          const maximum = Math.max(0, container.scrollHeight - container.clientHeight);
          const step = Math.max(
            Math.floor((window.innerHeight || container.clientHeight || 800) * STEP_FRACTION),
            MIN_STEP_PX
          );
          const after = Math.min(before + step, maximum);
          container.scrollTo(0, after);
          window.dispatchEvent(new Event('scroll'));
          return {before, after: scrollTop(container), maximum};

          function findScrollContainer() {
            const selectors = ['[data-testid="data-testid Dashboard scroll container"]', '.scrollbar-view'];
            for (const selector of selectors) {
              const element = document.querySelector(selector);
              if (element && element.scrollHeight > element.clientHeight) {
                return element;
              }
            }
            return document.scrollingElement || document.documentElement;
          }

          function scrollTop(element) {
            return Math.floor(element.scrollTop || window.scrollY || 0);
          }
        })();
        """.replace('STEP_FRACTION', step_fraction).replace('MIN_STEP_PX', min_step_px)

    @staticmethod
    def _snapshot_loaders_busy_script() -> str:
        return """
        return (function grafconfluxSnapshotLoadersBusy() {
          const selectors = [
            '[aria-busy="true"]', '[data-testid*="loading"]', '[data-testid*="Loading"]',
            '[class*="spinner"]', '[class*="Spinner"]', '[class*="loading"]', '[class*="Loading"]',
            '.panel-loading', '.fa-spinner'
          ];
          const elements = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
          return elements.some((element) => {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden'
              && style.display !== 'none' && style.opacity !== '0';
          });
        })();
        """

    def _snapshot_scroll_container(self, browser: webdriver.Firefox):
        for selector in ('.scrollbar-view', '[data-testid="data-testid Dashboard scroll container"]'):
            element = self._find_css(browser, selector)
            if element is not None:
                return element
        return browser.find_element(By.TAG_NAME, 'body')

    def _open_snapshot_dialog(self, browser: webdriver.Firefox) -> bool:
        return self._snapshot_ui_runner()._open_snapshot_dialog(browser)

    def _open_modern_snapshot_dialog(self, browser: webdriver.Firefox) -> bool:
        return self._snapshot_ui_runner()._open_modern_snapshot_dialog(browser)

    @staticmethod
    def _modern_share_menu_selectors() -> List[str]:
        return SnapshotUiRunner._modern_share_menu_selectors()

    @staticmethod
    def _share_snapshot_selectors() -> List[str]:
        return SnapshotUiRunner._share_snapshot_selectors()

    @staticmethod
    def _share_snapshot_xpaths() -> List[str]:
        return SnapshotUiRunner._share_snapshot_xpaths()

    def _open_classic_snapshot_dialog(self, browser: webdriver.Firefox) -> None:
        self._snapshot_ui_runner()._open_classic_snapshot_dialog(browser)

    def _submit_snapshot_form(self, browser: webdriver.Firefox, timestamp: GrafanaTimeDownloader,
                              modern_dialog: bool) -> None:
        self._fill_optional_snapshot_field(browser, 'input[id="snapshot-name-input"]', self._snapshot_name(timestamp))
        if modern_dialog:
            self._click_required_css(browser, '[data-testid="data-testid share snapshot publish button"]')
        else:
            self._fill_optional_snapshot_field(browser, 'input[id="timeout-input"]', f'{self.config.snapshot_timeout}')
            self._click_required_xpath(browser, "//button[.//span[text()='Local Snapshot']]")
        time.sleep(self.config.snapshot_timeout + 2)

    def _fill_optional_snapshot_field(self, browser: webdriver.Firefox, selector: str, value: str) -> None:
        element = self._find_css(browser, selector)
        if element is None:
            return
        element.clear()
        element.send_keys(value)

    def _read_snapshot_key(self, browser: webdriver.Firefox, timestamp: GrafanaTimeDownloader) -> str:
        snapshot_link = self._read_snapshot_url(browser, timestamp)
        if not snapshot_link:
            raise ValueError('Snapshot URL was not found after publishing.')
        return self._snapshot_key_from_url(snapshot_link)

    @staticmethod
    def _snapshot_key_from_url(snapshot_link: str) -> str:
        return snapshot_key_from_url(snapshot_link)

    def _read_snapshot_url(self, browser: webdriver.Firefox,
                           timestamp: Optional[GrafanaTimeDownloader] = None) -> Optional[str]:
        for selector in self._snapshot_url_selectors():
            snapshot_link = self._snapshot_url_from_element(browser, selector)
            if snapshot_link:
                return snapshot_link
        snapshot_link = browser.execute_script(self._read_snapshot_url_script())
        if snapshot_link:
            return str(snapshot_link)
        snapshot_link = self._snapshot_url_from_browser_requests(browser)
        if snapshot_link or timestamp is None:
            return snapshot_link
        return self._lookup_snapshot_url_by_name(timestamp)

    def _snapshot_url_from_browser_requests(self, browser: webdriver.Firefox) -> Optional[str]:
        for request in reversed(list(getattr(browser, 'requests', []))):
            if not self._is_snapshot_post_request(request):
                continue
            payload = self._snapshot_response_payload(getattr(request, 'response', None))
            snapshot_link = self._snapshot_url_from_payload(payload)
            if snapshot_link:
                return snapshot_link
        return None

    @staticmethod
    def _is_snapshot_post_request(request: Any) -> bool:
        return is_snapshot_post_request(request)

    def _snapshot_response_payload(self, response: Any) -> Dict[str, Any]:
        return snapshot_response_payload(response)

    @staticmethod
    def _snapshot_response_text(response: Any) -> str:
        return snapshot_response_text(response)

    def _snapshot_url_from_payload(self, payload: Dict[str, Any]) -> Optional[str]:
        return snapshot_url_from_payload(payload, self.config.host)

    def _normalize_snapshot_url(self, snapshot_link: str) -> Optional[str]:
        return normalize_snapshot_url(snapshot_link, self.config.host)

    def _snapshot_url_from_key(self, snapshot_key: str) -> str:
        return snapshot_url_from_key(snapshot_key, self.config.host)

    def _lookup_snapshot_url_by_name(self, timestamp: GrafanaTimeDownloader) -> Optional[str]:
        snapshot_name = self._snapshot_name(timestamp)
        try:
            response = self.session.get(
                self._snapshot_api_url('/api/dashboard/snapshots'),
                params={'query': snapshot_name},
                timeout=self.config.timeout,
            )
        except Exception as error:
            logger.warning(f'Snapshot lookup failed dashboard={self.config.name} error={error}')
            return None
        return self._snapshot_url_from_lookup_response(response, snapshot_name)

    def _snapshot_url_from_lookup_response(self, response: Any, snapshot_name: str) -> Optional[str]:
        return snapshot_url_from_lookup_response(response, snapshot_name, self.config.host, self.config.name)

    def _snapshot_url_from_element(self, browser: webdriver.Firefox, selector: str) -> Optional[str]:
        element = self._find_css(browser, selector)
        if element is None:
            return None
        for attribute in ('value', 'href'):
            snapshot_link = element.get_attribute(attribute)
            if snapshot_link and '/dashboard/snapshot/' in snapshot_link:
                return snapshot_link
        return None

    @staticmethod
    def _snapshot_url_selectors() -> List[str]:
        return [
            'input[id="snapshot-url-input"]',
            'input[value*="/dashboard/snapshot/"]',
            'textarea[value*="/dashboard/snapshot/"]',
            'a[href*="/dashboard/snapshot/"]',
        ]

    @staticmethod
    def _read_snapshot_url_script() -> str:
        return """
        return (function grafconfluxReadSnapshotUrl() {
          const elements = Array.from(document.querySelectorAll('input, textarea, a[href]'));
          const values = elements.flatMap((element) => [element.value, element.href, element.textContent]);
          values.push(window.location.href);
          return values.find((value) => typeof value === 'string' && value.includes('/dashboard/snapshot/')) || null;
        })();
        """

    def _click_first_css(self, browser: webdriver.Firefox, selectors: List[str]) -> bool:
        return self._snapshot_ui_runner()._click_first_css(browser, selectors)

    def _click_first_xpath(self, browser: webdriver.Firefox, xpaths: List[str]) -> bool:
        return self._snapshot_ui_runner()._click_first_xpath(browser, xpaths)

    def _click_first(self, browser: webdriver.Firefox, locators: List[Tuple[str, str]]) -> bool:
        return self._snapshot_ui_runner()._click_first(browser, locators)

    def _click_required_css(self, browser: webdriver.Firefox, selector: str) -> None:
        self._snapshot_ui_runner()._click_required_css(browser, selector)

    def _click_required_xpath(self, browser: webdriver.Firefox, xpath: str) -> None:
        self._snapshot_ui_runner()._click_required_xpath(browser, xpath)

    def _find_css(self, browser: webdriver.Firefox, selector: str):
        return self._snapshot_ui_runner()._find_css(browser, selector)

    @staticmethod
    def _find_element(browser: webdriver.Firefox, by: str, value: str):
        return SnapshotUiRunner._find_element(browser, by, value)

    @classmethod
    def convert_to_dict(cls, obj):
        if isinstance(obj, list):
            return [cls.convert_to_dict(item) for item in obj]
        elif isinstance(obj, Panel):
            return cls.__panel_to_dict(obj)
        elif hasattr(obj, '__dict__'):
            return {key: cls.convert_to_dict(value) for key, value in obj.__dict__.items()}
        else:
            return obj

    @classmethod
    def __panel_to_dict(cls, panel: Panel) -> Dict[str, Any]:
        data = {key: cls.convert_to_dict(getattr(panel, key)) for key in cls.__legacy_panel_keys()}
        if panel.is_repeating or panel.artifacts:
            data.update(cls.__repeating_panel_dict(panel))
        return data

    @staticmethod
    def __legacy_panel_keys() -> List[str]:
        return ['panel_id', 'type', 'title', 'display_title', 'row_title', 'from_collapsed_row', 'row_id', 'grid_pos', 'links']

    @staticmethod
    def __repeating_panel_dict(panel: Panel) -> Dict[str, Any]:
        return {
            'is_repeating': panel.is_repeating,
            'source_panel_id': panel.source_panel_id,
            'repeat_var': panel.repeat_var,
            'artifacts': panel.artifacts,
        }

    def __save_params_to_file(self, timestamps: List[GrafanaTimeDownloader], test_folder: str):
        save_data = {
            'name': self.config.name,
            'charts_path': self.charts_path,
            'full_links': self.config.full_links,
            'backup_dashboard_links': self.config.backup_dashboard_links,
            'timestamps': self.convert_to_dict(timestamps),
            'panels': self.convert_to_dict(self.config.panels)
        }

        if self.config.snapshot_urls:
            save_data.update({'snapshot_urls': self.config.snapshot_urls})
        save_data.update(self.__dashboard_identity_metadata())

        with open(os.path.join(test_folder, f'{self.config.name}.yaml'), 'w+', encoding='utf-8') as yaml_file:
            yaml_file.write(yaml.safe_dump(save_data, sort_keys=False, allow_unicode=True))

    def __dashboard_identity_metadata(self) -> Dict[str, Any]:
        identity = self.__current_dashboard_identity()
        return {key: value for key, value in identity.items() if value is not None}

    def __current_dashboard_identity(self) -> Dict[str, Any]:
        if self.dashboard_identity is not None:
            return {
                'dashboard_uid': self.dashboard_identity.dashboard_uid,
                'dashboard_title': self.dashboard_identity.dashboard_title,
                'folder_uid': self.dashboard_identity.folder_uid,
                'folder_title': self.dashboard_identity.folder_title,
            }
        identity = {
            'dashboard_uid': self.dashboard_uid or self.config.dashboard_uid,
            'dashboard_title': self.config.dashboard_title,
            'folder_uid': self.config.folder_uid,
            'folder_title': self.config.folder_title,
        }
        return identity

    def __get_full_links(self, timestamps: List[GrafanaTimeDownloader]):
        url = f'{self.config.host}{self.dashboard_url}'
        links = []

        for timestamp in timestamps:
            params = build_dashboard_url_params(timestamp, self.config.orgId, self.config.vars)
            links.append(f"{url}?{urlencode(params, doseq=True)}")

        return links

    def get_dashboard_uid(self):
        """
        Retrieve a deterministic dashboard identity from Grafana search.
        """
        request = self.__build_lookup_request()
        log_lookup_mode(request)
        response = self.session.get(
            f'{self.config.host}{self.config.nginx_prefix if self.config.nginx_prefix else ""}/api/search',
            params=search_params(request),
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard list.')

        result = select_dashboard(request, response.json())
        self.__record_dashboard_identity(result)
        logger.debug(f'Found dashboard UID: {result.dashboard_uid}')
        return result.dashboard_uid, result.url

    def __record_dashboard_identity(self, result: DashboardLookupResult) -> None:
        self.dashboard_identity = result

    def __build_lookup_request(self) -> DashboardLookupRequest:
        return DashboardLookupRequest(
            dashboard_uid=self.config.dashboard_uid,
            dash_title=self.config.dash_title,
            folder=self.config.folder,
            folder_uid=self.config.folder_uid,
        )

    def get_panels(self, timestamps: List[GrafanaTimeDownloader]):
        """
        Retrieve panel information from the dashboard.
        """
        response = self.session.get(
            f'{self.config.host}{self.config.nginx_prefix if self.config.nginx_prefix else ""}'
            f'/api/dashboards/uid/{self.dashboard_uid}',
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard details.')

        dashboard = response.json()['dashboard']
        self.dashboard_model = dashboard
        raw_panels = self.extract_panels(dashboard['panels'])
        descriptors = [PanelDescriptor.from_raw_panel(raw_panel) for raw_panel in raw_panels]
        descriptors = apply_disabled_graph_type_filter(descriptors, self.config.disable_graph_types)
        descriptors = filter_runtime_repeat_clones(
            descriptors, self.config.enable_repeating_panels, self.config.repeating_panels)
        warn_unmatched_filter_selectors(self.config.name, self.config.panel_filtering, descriptors)
        descriptors = filter_panel_descriptors(self.config.name, self.config.panel_filtering, descriptors)

        return self._build_panels_and_tasks(dashboard, descriptors, timestamps)

    def _build_panels_and_tasks(self, dashboard: Dict, descriptors: List[PanelDescriptor],
                                timestamps: List[GrafanaTimeDownloader]) -> List[Panel]:
        panels = []
        self._render_tasks = []
        planner = self._repeating_planner()
        rules = planner.resolve_repeating_rules(dashboard, descriptors, timestamps)
        for descriptor in descriptors:
            rule = rules.get(descriptor.panel_id)
            if is_unresolved_repeating_rule(rule):
                continue
            panel = panel_from_descriptor(
                descriptor,
                len(timestamps),
                self._panel_display_title(dashboard, descriptor),
            )
            panels.append(panel)
            planner.append_panel_tasks(self._render_tasks, panel, descriptor, timestamps, rule)
        return panels

    def _panel_display_title(self, dashboard: Dict, descriptor: PanelDescriptor) -> str:
        title = self._panel_rename_from_rules(descriptor)
        if title is None:
            title = self.config.panel_filtering.include_panels.inline_renames.get((descriptor.title or '', descriptor.graph_type))
        if title is None:
            title = self.config.panel_filtering.include_panels.inline_renames.get((descriptor.title or '', None))
        return self._substitute_panel_title_vars(title or descriptor.title or '', dashboard)

    def _panel_rename_from_rules(self, descriptor: PanelDescriptor) -> Optional[str]:
        title = descriptor.title or ''
        for rule in self.config.rename_panels:
            if rule['id'] == descriptor.panel_id:
                return rule['rename']
        for rule in self.config.rename_panels:
            if rule['title'] == title and rule['type'] == descriptor.graph_type:
                return rule['rename']
        for rule in self.config.rename_panels:
            if rule['title'] == title and rule['type'] is None:
                return rule['rename']
        return None

    def _substitute_panel_title_vars(self, value: str, dashboard: Dict) -> str:
        return re.sub(
            r'\$\{([^}]+)\}|\$(\w+)',
            lambda match: self._panel_title_var_value(dashboard, match.group(1) or match.group(2), match.group(0)),
            value,
        )

    def _panel_title_var_value(self, dashboard: Dict, name: str, original: str) -> str:
        configured_vars = self.config.vars or {}
        if name in configured_vars:
            return self._stringify_panel_title_var(configured_vars[name])
        dashboard_value = self._dashboard_template_var_value(dashboard, name)
        if dashboard_value is None:
            return original
        return self._stringify_panel_title_var(dashboard_value)

    def _dashboard_template_var_value(self, dashboard: Dict, name: str):
        variable = self._dashboard_template_var(dashboard, name)
        if not variable:
            return None
        current = variable.get('current') if isinstance(variable.get('current'), dict) else {}
        for value in (current.get('value'), variable.get('default'), current.get('text')):
            if value is not None and value != '':
                return value
        return None

    @staticmethod
    def _dashboard_template_var(dashboard: Dict, name: str) -> Optional[Dict[str, Any]]:
        variables = dashboard.get('templating', {}).get('list', [])
        return next((variable for variable in variables if variable.get('name') == name), None)

    def _config_var_value(self, name: str, original: str) -> str:
        if name not in (self.config.vars or {}):
            return original
        value = self.config.vars[name]
        return self._stringify_panel_title_var(value)

    @staticmethod
    def _stringify_panel_title_var(value) -> str:
        if isinstance(value, (list, tuple, set)):
            return ', '.join(str(item) for item in value)
        return str(value)

    @staticmethod
    def _is_unresolved_repeating_rule(rule: Optional[Dict[str, Any]]) -> bool:
        return is_unresolved_repeating_rule(rule)

    def _repeating_planner(self) -> RepeatingPlanner:
        return RepeatingPlanner(self.config, self.session)

    def _resolve_repeating_rules(self, dashboard: Dict, descriptors: List[PanelDescriptor],
                                 timestamps: List[GrafanaTimeDownloader]) -> Dict[int, Dict[str, Any]]:
        return self._repeating_planner().resolve_repeating_rules(dashboard, descriptors, timestamps)

    def _append_panel_tasks(self, panel: Panel, descriptor: PanelDescriptor,
                            timestamps: List[GrafanaTimeDownloader], rule: Optional[Dict[str, Any]]) -> None:
        self._repeating_planner().append_panel_tasks(self._render_tasks, panel, descriptor, timestamps, rule)

    def extract_panels(self, panels):
        """
        Recursively extract panels from dashboard panels.
        """
        return extract_dashboard_panels(panels, include_collapsed_rows=self.config.download_collapsed_rows)

    def __download_chart(self, task: PanelRenderTask):
        """
        Download or render a single chart.
        """
        if not self.config.render:
            browser = getattr(self.thread_local, 'browser', None)

            if browser is None:
                browser = self.__init_browser()
                if browser:
                    self.thread_local.browser = browser
                    self.thread_local.is_fullscreen = None
                    self.browser_list.append(browser)
                else:
                    logger.error('Failed to initialize browser')
                    return
        else:
            browser = None

        panel = task.panel
        timestamp = task.timestamp
        file_path = build_render_file_path(self.charts_path, self.config.name, panel.panel_id, timestamp, task.file_name)
        url, params = self.__build_panel_url(panel, timestamp, task.variables)
        final_url = f"{url}?{urlencode(params, doseq=True)}"

        if self.config.render:
            # Use Grafana rendering API
            render_params = build_render_api_params(params, self.config.width, self.config.height, self.config.timeout)
            render_url = build_render_api_url(self.config.host, self.config.nginx_prefix, self.dashboard_url)
            try:
                response = self.session.get(render_url, params=render_params, timeout=self.config.timeout)
                response.raise_for_status()
            except Exception as e:
                logger.error(f'Failed to download chart for panel {panel.panel_id}: {e}')
                response = None

            try:
                self.session.get(f"{final_url}&fullscreen", timeout=self.config.timeout)
                self.__record_task_link(task, f"{final_url}&fullscreen")
            except Exception:
                self.session.get(final_url, timeout=self.config.timeout)
                self.__record_task_link(task, final_url)

            if response and response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)

                logger.info(f'Downloaded chart to {file_path}')
            else:
                logger.error(f'Failed to download chart for panel {panel.panel_id}')
        else:
            # Use headless browser
            self.__take_screenshot(browser, task, final_url, file_path)

    def __record_task_link(self, task: PanelRenderTask, link: str) -> None:
        task.panel.links[task.timestamp.id_time] = link
        if task.artifact is not None:
            task.artifact['link'] = link

    def __build_panel_url(self, panel: Panel, timestamp: GrafanaTimeDownloader, variables: Optional[Dict] = None):
        """
        Build the URL for a panel in view mode.
        """
        url = f'{self.config.host}{self.dashboard_url}'
        variables = self.config.vars if variables is None else variables
        params = build_panel_url_params(
            panel.panel_id,
            timestamp,
            self.config.orgId,
            self.config.white_theme,
            self.config.tz,
            variables,
        )
        return url, params

    def __init_browser(self):
        return GrafanaBrowserSession(
            self.config,
            self.session,
            webdriver.Firefox,
            Options,
            suppress_setup_errors=True,
            require_cookie_domain=True,
        ).create_browser()

    def __take_screenshot(self, browser: webdriver.Firefox, task: PanelRenderTask, final_url, file_path):
        """
        Use a headless browser to take a screenshot of the panel.
        """
        panel_data_sources = self.__get_panel_data_sources(final_url)

        if self.thread_local.is_fullscreen is None:
            if self.__try_screenshot_route(
                browser, task, f"{final_url}&fullscreen", file_path, panel_data_sources, False, fullscreen_state=True,
            ):
                return
            self.__try_screenshot_route(
                browser, task, final_url, file_path, panel_data_sources, True,
                request_error_message=f'Request to {final_url} does not return 200 OK!', fullscreen_state=False,
            )
            return

        route_url = f"{final_url}&fullscreen" if self.thread_local.is_fullscreen else final_url
        request_error_message = None if self.thread_local.is_fullscreen else f'Request to {final_url} does not return 200 OK!'
        self.__try_screenshot_route(browser, task, route_url, file_path, panel_data_sources, True, request_error_message)

    def __try_screenshot_route(self, browser: webdriver.Firefox, task: PanelRenderTask, route_url: str,
                               file_path: str, panel_data_sources: List[str], log_errors: bool,
                               request_error_message: Optional[str] = None,
                               fullscreen_state: Optional[bool] = None) -> bool:
        try:
            status_code = self.__open_screenshot_route(browser, task, route_url)
            if status_code in (401, 403) and self._reauthenticate_grafana(browser):
                logger.info(f'Retrying panel after re-authentication panel_id={task.panel.panel_id}')
                status_code = self.__open_screenshot_route(browser, task, route_url)
            if status_code != 200:
                message = self.__route_error_message(route_url, status_code, request_error_message)
                raise Exception(message)
            if fullscreen_state is not None:
                self.thread_local.is_fullscreen = fullscreen_state
            if not self.__browser_loaded_panel(browser, task.panel.panel_id):
                raise Exception(f'Browser did not load expected panel_id={task.panel.panel_id}; current_url={browser.current_url}')
            self.__record_task_link(task, route_url)
            self.__wait_for_network_request(browser, panel_data_sources, self.config.timeout)
            self.__close_grafana_sidebar(browser)
            browser.save_screenshot(file_path)
            logger.info(f'Screenshot saved to {file_path}')
            return True
        except Exception as e:
            if log_errors:
                logger.error(f'Failed to take screenshot: {e}')
            return False

    def __open_screenshot_route(self, browser: webdriver.Firefox, task: PanelRenderTask, route_url: str) -> Optional[int]:
        logger.info(f'Opening panel for screenshot panel_id={task.panel.panel_id} url={route_url}')
        self.__clear_browser_requests(browser)
        try:
            browser.get(route_url)
        except TimeoutException as error:
            logger.warning(f'Panel navigation timed out, checking loaded response panel_id={task.panel.panel_id}: {error}')
        status_code = self.__route_status_code(browser, route_url)
        logger.info(f'Panel navigation response panel_id={task.panel.panel_id} status={status_code} url={route_url}')
        return status_code

    @staticmethod
    def __browser_loaded_panel(browser: webdriver.Firefox, panel_id: int) -> bool:
        expected_values = {str(panel_id)}
        try:
            return bool(browser.execute_script("""
            const expected = arguments[0];
            const current = new URL(window.location.href);
            return current.searchParams.get('panelId') === expected
              || current.searchParams.get('viewPanel') === expected;
            """, str(panel_id)))
        except Exception:
            current_url = getattr(browser, 'current_url', None)
            if current_url is None:
                return True
            return any(f'panelId={value}' in current_url or f'viewPanel={value}' in current_url for value in expected_values)

    @staticmethod
    def __clear_browser_requests(browser: webdriver.Firefox) -> None:
        try:
            requests = getattr(browser, 'requests')
            if hasattr(requests, 'clear'):
                requests.clear()
            else:
                del browser.requests
        except Exception as error:
            logger.warning(f'Failed to clear browser request history: {error}')

    @staticmethod
    def __route_error_message(route_url: str, status_code: Optional[int], fallback_message: Optional[str]) -> str:
        if status_code is None:
            return fallback_message or f'Request to {route_url} has no captured HTTP response'
        if fallback_message:
            return f'{fallback_message} HTTP status={status_code}'
        return f'Request to {route_url} returned HTTP {status_code}'

    @staticmethod
    def __close_grafana_sidebar(browser: webdriver.Firefox) -> None:
        try:
            browser.execute_script("""
            const buttons = Array.from(document.querySelectorAll('button'));
            const closeButton = buttons.find((button) => {
              const label = (button.getAttribute('aria-label') || button.getAttribute('title') || '').toLowerCase();
              const text = (button.textContent || '').trim().toLowerCase();
              return label.includes('close') || label.includes('collapse') || text === '×' || text === 'x';
            });
            if (closeButton) {
              closeButton.click();
              return;
            }
            const sidebar = document.querySelector('nav, aside, [aria-label="Navigation"], [data-testid*="sidemenu"]');
            if (sidebar && sidebar.getBoundingClientRect().width > 120) {
              sidebar.style.display = 'none';
            }
            """)
        except Exception as error:
            logger.debug(f'Grafana sidebar was not closed: {error}')

    @staticmethod
    def __route_status_code(browser: webdriver.Firefox, route_url: str) -> Optional[int]:
        matching_requests = [
            request
            for request in browser.requests
            if request.url == route_url and request.response is not None
        ]
        if not matching_requests:
            return None
        return matching_requests[-1].response.status_code

    def __wait_for_network_request(self, browser: webdriver.Firefox, url_part: List[str], timeout):
        """
        Wait until a network request containing `url_part` has completed.
        """
        if url_part:
            time.sleep(self.config.firefox_driver_preload_time)
            start_time = time.time()

            while True:
                if self.__all_network_parts_loaded(browser, url_part):
                    return

                if time.time() - start_time > timeout - self.config.firefox_driver_preload_time:
                    return

                time.sleep(0.1)
        else:
            time.sleep(self.config.timeout)

    @staticmethod
    def __all_network_parts_loaded(browser: webdriver.Firefox, url_part: List[str]) -> bool:
        for url in url_part:
            if not any(
                url in request.url
                and request.response is not None
                and request.response.status_code == 200
                for request in browser.requests
            ):
                return False
        return True

    def __get_panel_data_sources(self, final_url):
        response = self.session.get(final_url, verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code in (401, 403) and self._reauthenticate_grafana():
            logger.info(f'Retrying panel datasource discovery after re-authentication url={final_url}')
            response = self.session.get(final_url, verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code != 200:
            logger.warning(f'Panel datasource discovery returned HTTP {response.status_code} url={final_url}')
        response = response.text
        panel_data_sources = []
        tree = html.fromstring(response)

        script_content = tree.xpath('//script[contains(text(), "window.grafanaBootData")]/text()')

        if script_content:
            data_script = script_content[0]
            match = re.search(r'window\.grafanaBootData\s*=\s*({.*?})\s*;', data_script, re.DOTALL)
            if match:
                data_object = match.group(1)
                data_object_json = demjson3.decode(data_object)
                panel_data_sources = [
                    datasource['url']
                    for datasource in data_object_json['settings']['datasources'].values()
                    if 'url' in datasource
                ]

        return panel_data_sources

    @staticmethod
    def load_grafana_config(path: str) -> List[GrafanaConfigDownloader]:
        with open(path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        grafana_configs = []
        
        if 'settings' in config:
            for config_name, config_data in config['dashboards'].items():
                grafana_configs.append(GrafanaConfigDownloader(config_name, config_data))
        else:
            for config_name, config_data in config.items():
                grafana_configs.append(GrafanaConfigDownloader(config_name, config_data))
        
        return grafana_configs
