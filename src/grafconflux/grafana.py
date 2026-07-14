import base64
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

import requests
from lxml import html
import demjson3
import yaml

from grafconflux._shared.time import GrafanaTimeDownloader, GrafanaTimeUploader
from grafconflux._shared.confluence_settings import confluence_rendering_settings_from_config
from grafconflux._grafana.browser_session import GrafanaBrowserSession
from grafconflux._grafana.lookup import log_lookup_mode, search_params, select_dashboard
from grafconflux._grafana.playwright_screenshots import PlaywrightPanelScreenshotRunner
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
    normalize_grafana_dashboard_route,
    sanitize_url_for_log,
)
from grafconflux._shared.display import normalize_grafana_display_value
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
from grafconflux._grafana.composites import generate_composites
from grafconflux._grafana.credentials import resolve_dashboard_configs
from grafconflux._grafana.variants import append_variant_tasks
from grafconflux._grafana.matrix import append_matrix_tasks, build_matrix_dashboard_links
from grafconflux._grafana.variable_lookup import (
    resolve_configured_datasource_names,
    resolve_dashboard_variable_lookups,
)
from grafconflux._orchestration.manifest import assign_artifact_order, dashboard_manifest_metadata
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
URL_IN_TEXT_RE = re.compile(r'https?://\S+')


def _exception_type(error: Exception) -> str:
    return type(error).__name__


def _safe_exception_message(error: Exception) -> str:
    message = str(error)
    if not message:
        return ''
    return URL_IN_TEXT_RE.sub(lambda match: sanitize_url_for_log(match.group(0).rstrip('.,;:')),
                              message)

class GrafanaManager:
    """
    Manages interactions with a Grafana instance.
    """

    def __init__(self, config: GrafanaConfigDownloader, session: Optional[requests.Session] = None):
        self.thread_local = threading.local()
        self.browser_list: Optional[List[Any]] = []
        self.dashboard_url = ''
        self.config = config
        self.session = session or requests.Session()
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

    def authenticate(self, confluence_login: Optional[str], confluence_password: Optional[str]):
        """
        Authenticate with Grafana using the specified method.
        """
        self._confluence_login = confluence_login
        self._confluence_password = confluence_password
        if not self.config.auth:
            logger.info('Authentication disabled for this Grafana instance.')
            return

        if self.config.domain:
            if confluence_login in (None, '') or confluence_password in (None, ''):
                raise ValueError('Confluence login/password are required for Grafana domain authentication.')
            login = confluence_login.split('@')[0]
            password = confluence_password
        elif (self.config.login and self.config.password) and not self.config.auth_url:
            login = self.config.login
            password = self.config.password
        elif self.config.token:
            self.session.headers.update({'Authorization': f'Bearer {self.config.token}'})
            return
        elif self.config.auth_url and (self.config.login and self.config.password):
            auth_string = base64.b64encode(f'{self.config.login}:{self.config.password}'.encode()).decode()
            self.session.headers.update({'Authorization': f"Basic {auth_string}"})

            response = self.session.get(self.config.auth_url, timeout=self.config.timeout)

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
            self._grafana_login_url(),
            headers={'Content-type': 'application/json'},
            data=json.dumps(payload),
            timeout=self.config.timeout,
        )

        if response.status_code != 200:
            raise ConnectionError('Failed to authenticate with Grafana.')
        logger.info('Successfully authenticated with Grafana.')

    def _grafana_login_url(self) -> str:
        return self._grafana_url('/login')

    def _reauthenticate_grafana(self, browser: Optional[Any] = None) -> bool:
        if not self.config.auth:
            return False
        if not self._has_reauthentication_credentials():
            logger.error('Cannot re-authenticate Grafana: required credentials are not available')
            return False
        try:
            logger.warning('Grafana session expired; re-authenticating')
            self.session.cookies.clear()
            self.authenticate(self._confluence_login, self._confluence_password)
            if browser is not None:
                self._refresh_browser_authentication(browser)
            return True
        except Exception as error:
            logger.error(
                f'Grafana re-authentication failed error_type={_exception_type(error)} '
                f'error={_safe_exception_message(error)}'
            )
            return False

    def _has_reauthentication_credentials(self) -> bool:
        if self.config.domain:
            return self._has_values(self._confluence_login, self._confluence_password)
        if self.config.token:
            return True
        return self._has_values(self.config.login, self.config.password)

    @staticmethod
    def _has_values(*values: Optional[str]) -> bool:
        return all(value not in (None, '') for value in values)

    def _refresh_browser_authentication(self, browser: Any) -> None:
        if hasattr(browser, 'refresh_authentication'):
            browser.refresh_authentication()

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
        self.config.matrix_dashboard_links = self.__get_matrix_full_links(timestamps)

        self._download_render_tasks()

        assign_artifact_order(self.config)
        generate_composites(self.config, self.charts_path, timestamps)
        assign_artifact_order(self.config)

        if self.config.snapshot:
            self.take_snapshot(timestamps, test_folder)

        self.__save_params_to_file(timestamps, test_folder)

    def _download_render_tasks(self) -> None:
        executor = ThreadPoolExecutor(max_workers=self.config.threads)
        try:
            for future in self._submit_render_tasks(executor):
                future.result()
        finally:
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
            logger.warning(
                f'Snapshot backup not saved dashboard={self.config.name} '
                f'error_type={_exception_type(error)} error={_safe_exception_message(error)}'
            )

    def _write_snapshot_backup(self, snapshot_key: str, timestamp: GrafanaTimeDownloader, test_folder: str) -> None:
        response = self.session.get(self._snapshot_api_url(f'/api/snapshots/{snapshot_key}'),
                                    verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code != 200:
            raise ConnectionError(f'Failed to download snapshot backup status={response.status_code}')
        output_file = self._snapshot_backup_file(test_folder, timestamp)
        self._write_json_file(output_file, self._without_delete_fields(response.json()))
        logger.info(f'Snapshot backup for {self.config.name} saved in {output_file}')

    def _snapshot_api_url(self, path: str) -> str:
        return snapshot_api_url(self.config.grafana_base_url, path)

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
        return SnapshotUiRunner(self)

    @staticmethod
    def _snapshot_sleep(seconds: int) -> None:
        time.sleep(seconds)

    def _create_ui_snapshot(self, browser: Any, timestamp: GrafanaTimeDownloader,
                            test_folder: str) -> None:
        self._open_dashboard_for_snapshot(browser, self.config.full_links[timestamp.id_time])
        self._prepare_dashboard_for_snapshot(browser)
        modern_dialog = self._open_snapshot_dialog(browser)
        self._submit_snapshot_form(browser, timestamp, modern_dialog)
        snapshot_key = self._read_snapshot_key(browser, timestamp)
        self._record_snapshot_url(f'{self.config.grafana_base_url}/dashboard/snapshot/{snapshot_key}')
        self._save_snapshot_backup(snapshot_key, timestamp, test_folder)

    def _open_dashboard_for_snapshot(self, browser: Any, dashboard_link: str) -> None:
        browser.get(dashboard_link)
        time.sleep(5)
        if self._dashboard_loaded_for_snapshot(browser):
            return
        logger.warning(f'Dashboard route fallback via browser UI navigation dashboard={self.config.name}')
        browser.get(self.config.grafana_base_url)
        time.sleep(2)
        browser.execute_script('window.location.href = arguments[0]; return window.location.href;', dashboard_link)
        time.sleep(5)

    def _dashboard_loaded_for_snapshot(self, browser: Any) -> bool:
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

    def _prepare_dashboard_for_snapshot(self, browser: Any) -> None:
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

    def _expand_collapsed_rows(self, browser: Any) -> None:
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

    def _expand_collapsed_rows_full_sweep(self, browser: Any) -> Tuple[int, List[str]]:
        up_count, up_titles = self._expand_collapsed_rows_sweep(browser, 'up')
        down_count, down_titles = self._expand_collapsed_rows_sweep(browser, 'down')
        return up_count + down_count, up_titles + down_titles

    def _expand_collapsed_rows_sweep(self, browser: Any, direction: str) -> Tuple[int, List[str]]:
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

    def _expand_visible_collapsed_rows(self, browser: Any) -> Tuple[int, List[str]]:
        result = browser.execute_script(self._expand_visible_collapsed_rows_script())
        return self._expanded_row_result(result)

    def _row_sweep_reached_edge(self, browser: Any, direction: str) -> bool:
        result = browser.execute_script(self._row_sweep_scroll_script(direction))
        return bool(isinstance(result, dict) and result.get('atEdge'))

    def _scroll_dashboard_to_edge(self, browser: Any, edge: str) -> None:
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

    def _remaining_collapsed_rows(self, browser: Any) -> Tuple[int, List[str]]:
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

    def _hydrate_dashboard_panels(self, browser: Any) -> None:
        self._reset_dashboard_scroll_to_top(browser)
        if self._hydrate_dashboard_panels_with_page_down(browser):
            return
        self._hydrate_dashboard_panels_with_js(browser)

    def _reset_dashboard_scroll_to_top(self, browser: Any) -> None:
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

    def _hydrate_dashboard_panels_with_page_down(self, browser: Any) -> bool:
        scrollable_div = self._find_css(browser, '.scrollbar-view')
        if scrollable_div is None:
            return False
        previous_scroll_position = -1
        for _ in range(SNAPSHOT_HYDRATION_SCROLL_LIMIT):
            scrollable_div.send_keys('PageDown')
            time.sleep(SNAPSHOT_PAGE_DOWN_DWELL_SECONDS)
            self._wait_for_snapshot_loaders(browser)
            current_scroll_position = self._scrollbar_view_scroll_top(browser, scrollable_div)
            if current_scroll_position == previous_scroll_position:
                scrollable_div.send_keys('PageDown')
                time.sleep(SNAPSHOT_PAGE_DOWN_DWELL_SECONDS)
                self._wait_for_snapshot_loaders(browser)
                break
            previous_scroll_position = current_scroll_position
        time.sleep(SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS)
        return True

    @staticmethod
    def _scrollbar_view_scroll_top(browser: Any, scrollable_div: Any) -> int:
        if hasattr(scrollable_div, 'evaluate'):
            return int(scrollable_div.evaluate('(element) => Math.floor(element.scrollTop || 0)') or 0)
        return int(browser.execute_script('return Math.floor(arguments[0].scrollTop || 0)', scrollable_div) or 0)

    def _hydrate_dashboard_panels_with_js(self, browser: Any) -> None:
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

    def _wait_for_snapshot_loaders(self, browser: Any) -> None:
        for _ in range(SNAPSHOT_LOADER_WAIT_LIMIT):
            if not self._snapshot_loaders_busy(browser):
                return
            time.sleep(SNAPSHOT_LOADER_WAIT_INTERVAL_SECONDS)
        logger.debug(f'Snapshot loader wait reached limit dashboard={self.config.name}')

    def _snapshot_loaders_busy(self, browser: Any) -> bool:
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

    def _snapshot_scroll_container(self, browser: Any):
        for selector in ('.scrollbar-view', '[data-testid="data-testid Dashboard scroll container"]'):
            element = self._find_css(browser, selector)
            if element is not None:
                return element
        return self._find_css(browser, 'body')

    def _open_snapshot_dialog(self, browser: Any) -> bool:
        return self._snapshot_ui_runner()._open_snapshot_dialog(browser)

    def _open_modern_snapshot_dialog(self, browser: Any) -> bool:
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

    def _open_classic_snapshot_dialog(self, browser: Any) -> None:
        self._snapshot_ui_runner()._open_classic_snapshot_dialog(browser)

    def _submit_snapshot_form(self, browser: Any, timestamp: GrafanaTimeDownloader,
                              modern_dialog: bool) -> None:
        self._snapshot_ui_runner()._submit_snapshot_form(browser, timestamp, modern_dialog)

    def _fill_optional_snapshot_field(self, browser: Any, selector: str, value: str) -> None:
        element = self._find_css(browser, selector)
        if element is None:
            return
        element.clear()
        element.send_keys(value)

    def _read_snapshot_key(self, browser: Any, timestamp: GrafanaTimeDownloader) -> str:
        snapshot_link = self._read_snapshot_url(browser, timestamp)
        if not snapshot_link:
            raise ValueError('Snapshot URL was not found after publishing.')
        return self._snapshot_key_from_url(snapshot_link)

    @staticmethod
    def _snapshot_key_from_url(snapshot_link: str) -> str:
        return snapshot_key_from_url(snapshot_link)

    def _read_snapshot_url(self, browser: Any,
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

    def _snapshot_url_from_browser_requests(self, browser: Any) -> Optional[str]:
        for payload in reversed(list(getattr(browser, 'snapshot_payloads', []))):
            snapshot_link = self._snapshot_url_from_payload(payload)
            if snapshot_link:
                return snapshot_link
        for request in reversed(list(getattr(browser, 'requests', []))):
            if self._is_snapshot_post_request(request):
                snapshot_link = self._snapshot_url_from_payload(
                    self._snapshot_response_payload(getattr(request, 'response', None))
                )
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
        return snapshot_url_from_payload(payload, self.config.grafana_base_url)

    def _normalize_snapshot_url(self, snapshot_link: str) -> Optional[str]:
        return normalize_snapshot_url(snapshot_link, self.config.grafana_base_url)

    def _snapshot_url_from_key(self, snapshot_key: str) -> str:
        return snapshot_url_from_key(snapshot_key, self.config.grafana_base_url)

    def _lookup_snapshot_url_by_name(self, timestamp: GrafanaTimeDownloader) -> Optional[str]:
        snapshot_name = self._snapshot_name(timestamp)
        try:
            response = self.session.get(
                self._snapshot_api_url('/api/dashboard/snapshots'),
                params={'query': snapshot_name},
                timeout=self.config.timeout,
            )
        except Exception as error:
            logger.warning(
                f'Snapshot lookup failed dashboard={self.config.name} '
                f'error_type={_exception_type(error)} error={_safe_exception_message(error)}'
            )
            return None
        return self._snapshot_url_from_lookup_response(response, snapshot_name)

    def _snapshot_url_from_lookup_response(self, response: Any, snapshot_name: str) -> Optional[str]:
        return snapshot_url_from_lookup_response(response, snapshot_name, self.config.grafana_base_url, self.config.name)

    def _snapshot_url_from_element(self, browser: Any, selector: str) -> Optional[str]:
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

    def _click_first_css(self, browser: Any, selectors: List[str]) -> bool:
        return self._snapshot_ui_runner()._click_first_css(browser, selectors)

    def _click_first_xpath(self, browser: Any, xpaths: List[str]) -> bool:
        return self._snapshot_ui_runner()._click_first_xpath(browser, xpaths)

    def _click_first(self, browser: Any, locators: List[Tuple[str, str]]) -> bool:
        return self._snapshot_ui_runner()._click_first(browser, locators)

    def _click_required_css(self, browser: Any, selector: str) -> None:
        self._snapshot_ui_runner()._click_required_css(browser, selector)

    def _click_required_xpath(self, browser: Any, xpath: str) -> None:
        self._snapshot_ui_runner()._click_required_xpath(browser, xpath)

    def _find_css(self, browser: Any, selector: str):
        return self._snapshot_ui_runner()._find_css(browser, selector)

    @staticmethod
    def _find_element(browser: Any, by: str, value: str):
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
        assign_artifact_order(self.config)
        save_data = {
            'schema_version': 2,
            'name': self.config.name,
            'charts_path': self.charts_path,
            'full_links': self.config.full_links,
            'backup_dashboard_links': self.config.backup_dashboard_links,
            'matrix_dashboard_links': self.config.matrix_dashboard_links,
            'timestamps': self.convert_to_dict(timestamps),
            'panels': self.convert_to_dict(self.config.panels),
            'manifest': dashboard_manifest_metadata(self.config),
            'confluence_rendering': self.config.confluence_rendering.to_metadata(),
        }

        if self.config.render_matrix:
            save_data.update({'render_matrix': self.config.render_matrix})
        if self.config.vars_presentation:
            save_data.update({'vars_presentation': self.config.vars_presentation})

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
        url = self._dashboard_public_url()
        links = []

        for timestamp in timestamps:
            params = build_dashboard_url_params(timestamp, self.config.orgId, self.config.vars)
            links.append(f"{url}?{urlencode(params, doseq=True)}")

        return links

    def __get_matrix_full_links(self, timestamps: List[GrafanaTimeDownloader]) -> List[Dict[str, Any]]:
        return build_matrix_dashboard_links(
            self.config,
            timestamps,
            self._dashboard_public_url(),
            build_dashboard_url_params,
        )

    def get_dashboard_uid(self):
        """
        Retrieve a deterministic dashboard identity from Grafana search.
        """
        request = self.__build_lookup_request()
        log_lookup_mode(request)
        response = self.session.get(
            self._grafana_url('/api/search'),
            params=search_params(request),
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard list.')

        result = select_dashboard(request, response.json())
        self.__record_dashboard_identity(result)
        logger.debug(f'Found dashboard UID: {result.dashboard_uid}')
        return result.dashboard_uid, self._normalize_dashboard_route(result.url)

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
            self._grafana_url(f'/api/dashboards/uid/{self.dashboard_uid}'),
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard details.')

        dashboard = response.json()['dashboard']
        self.dashboard_model = dashboard
        resolve_dashboard_variable_lookups(self.config, dashboard)
        resolve_configured_datasource_names(self.config, self.session, self._grafana_url('/api/datasources'))
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
        self._render_tasks = append_matrix_tasks(
            self.config,
            dashboard,
            descriptors,
            panels,
            self._render_tasks,
            timestamps,
            self.session,
            self._dashboard_public_url(),
        )
        self._render_tasks = append_variant_tasks(self.config, dashboard, descriptors, panels, self._render_tasks, timestamps)
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
            return ', '.join(normalize_grafana_display_value(item) for item in value)
        return normalize_grafana_display_value(value)

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
        browser = None
        if not self.config.render:
            browser = self.__init_screenshot_browser()
            if browser is None:
                raise RuntimeError('Failed to initialize screenshot browser')

        try:
            panel = task.panel
            timestamp = task.timestamp
            file_path = build_render_file_path(self.charts_path, self.config.name, panel.panel_id, timestamp, task.file_name)
            url, params = self.__build_panel_url(panel, timestamp, task.variables)
            final_url = f"{url}?{urlencode(params, doseq=True)}"

            if self.config.render:
                # Use Grafana rendering API
                render_params = build_render_api_params(params, self.config.width, self.config.height, self.config.timeout)
                render_url = build_render_api_url(self.config.grafana_base_url, self.dashboard_url)
                response = self._download_render_api_response(panel.panel_id, render_url, render_params)

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
                    raise RuntimeError(f'Failed to download chart for panel {panel.panel_id}')
            else:
                # Use headless browser
                self.__take_screenshot(browser, task, final_url, file_path)
        finally:
            if not self.config.render:
                self._close_worker_browser(browser)

    def _download_render_api_response(self, panel_id: int, render_url: str, render_params: Dict[str, Any]):
        try:
            response = self.session.get(render_url, params=render_params, timeout=self.config.timeout)
            response.raise_for_status()
            if response.status_code != 200:
                raise RuntimeError(f'HTTP {response.status_code}')
            return response
        except Exception as error:
            safe_url = sanitize_url_for_log(render_url)
            logger.error(
                f'Failed to download chart for panel {panel_id} '
                f'url={safe_url} error_type={_exception_type(error)} error={_safe_exception_message(error)}'
            )
            raise RuntimeError(f'Failed to download chart for panel {panel_id}') from error

    def __init_screenshot_browser(self):
        browser = self.__init_browser()
        if browser is None:
            logger.error('Failed to initialize browser')
            return None
        if not hasattr(self.thread_local, 'is_fullscreen'):
            self.thread_local.is_fullscreen = None
        return browser

    @staticmethod
    def _close_worker_browser(browser: Any) -> None:
        if browser is None:
            return
        close = getattr(browser, 'quit', None) or getattr(browser, 'close', None)
        if callable(close):
            close()

    def __record_task_link(self, task: PanelRenderTask, link: str) -> None:
        task.panel.links[task.timestamp.id_time] = link
        if task.artifact is not None:
            task.artifact['link'] = link

    def __build_panel_url(self, panel: Panel, timestamp: GrafanaTimeDownloader, variables: Optional[Dict] = None):
        """
        Build the URL for a panel in view mode.
        """
        url = self._dashboard_public_url()
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

    def _dashboard_public_url(self) -> str:
        return self._grafana_url(self.dashboard_url)

    def _grafana_url(self, path: str) -> str:
        return f'{self.config.grafana_base_url}{path}'

    def _normalize_dashboard_route(self, dashboard_url: str) -> str:
        return normalize_grafana_dashboard_route(
            self.config.name,
            dashboard_url,
            self.config.grafana_origin,
            self.config.grafana_app_path,
        )

    def __init_browser(self):
        return GrafanaBrowserSession(
            self.config,
            self.session,
            suppress_setup_errors=True,
            require_cookie_domain=True,
        ).create_browser()

    def __take_screenshot(self, browser: Any, task: PanelRenderTask, final_url, file_path):
        """
        Use a Playwright headless browser to take a screenshot of the panel.
        """
        PlaywrightPanelScreenshotRunner(self).take_screenshot(browser, task, final_url, file_path)

    def __get_panel_data_sources(self, final_url):
        response = self.session.get(final_url, verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code in (401, 403) and self._reauthenticate_grafana():
            logger.info(f'Retrying panel datasource discovery after re-authentication url={sanitize_url_for_log(final_url)}')
            response = self.session.get(final_url, verify=self.config.verify_ssl, timeout=self.config.timeout)
        if response.status_code != 200:
            logger.warning(f'Panel datasource discovery returned HTTP {response.status_code} url={sanitize_url_for_log(final_url)}')
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
            config = yaml.safe_load(file) or {}

        dashboards = GrafanaManager._dashboard_configs_from_yaml(config)
        confluence_rendering = confluence_rendering_settings_from_config(config).to_metadata()
        configs = []
        for order_index, (config_name, config_data) in enumerate(dashboards.items()):
            config_data = dict(config_data)
            config_data['order_index'] = order_index
            config_data['confluence_rendering'] = confluence_rendering
            configs.append(GrafanaConfigDownloader(config_name, config_data))
        return configs

    @staticmethod
    def _dashboard_configs_from_yaml(config: Any) -> Dict[str, Any]:
        return resolve_dashboard_configs(config)
