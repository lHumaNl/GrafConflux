import logging
import re
from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Pattern, Tuple
from urllib.parse import urlparse, urlunparse
from grafconflux._shared.time import GrafanaTimeDownloader, GrafanaTimeUploader

logger = logging.getLogger('grafconflux.grafana')

DOWNLOAD_COLLAPSED_ROWS_KEY = 'download_collapsed_rows'
DOWNLOAD_COLLAPSE_PANELS_KEY = 'download_collapse_panels'
DOWNLOAD_HIDDEN_PANELS_KEY = 'download_hidden_panels'
PANEL_FILTERING_KEY = 'panel_filtering'
RENAME_PANELS_KEY = 'rename_panels'
ENABLE_REPEATING_PANELS_KEY = 'enable_repeating_panels'
REPEATING_PANELS_KEY = 'repeating_panels'
REPEAT_VALUES_KEY = 'repeat_values'
INCLUDE_ALL_EXCEPT_EXCLUDED = 'include_all_except_excluded'
INCLUDE_ONLY_SELECTED = 'include_only_selected'
PANEL_FILTERING_MODES = [INCLUDE_ALL_EXCEPT_EXCLUDED, INCLUDE_ONLY_SELECTED]
REPEAT_VALUE_MODES = ['manual', 'regex', 'all']
ALL_REPEAT_SENTINELS = {'$__all', '__all', 'all'}
COLLECT_NO_DATA_PANELS_KEY = 'collect_no_data_panels'
NO_DATA_PREFLIGHT_KEY = 'no_data_preflight'
NO_DATA_MODE_CONSERVATIVE = 'conservative'
NO_DATA_ON_ERROR_RENDER = 'render_anyway'
DEFAULT_NO_DATA_TIMEOUT = 10
DEFAULT_MAX_DATA_POINTS = 1000
DEFAULT_INTERVAL_MS = 60000
PROMETHEUS_DATASOURCE_TYPE = 'prometheus'
SUPPORTED_PHASE1_DATASOURCES = {PROMETHEUS_DATASOURCE_TYPE}
SKIP_REASON_EMPTY_FRAMES = 'datasource_query_returned_empty_frames'
SNAPSHOT_MODE_UI = 'ui'
SNAPSHOT_MODE_LEGACY_API = 'legacy_api'
SNAPSHOT_MODE_AUTO = 'auto'
SNAPSHOT_MODES = [SNAPSHOT_MODE_UI, SNAPSHOT_MODE_LEGACY_API, SNAPSHOT_MODE_AUTO]
SNAPSHOT_DELETE_FIELDS = {'deleteKey', 'deleteUrl'}
SNAPSHOT_HYDRATION_SCROLL_LIMIT = 40
SNAPSHOT_HYDRATION_STEP_VIEWPORT_FRACTION = 0.45
SNAPSHOT_HYDRATION_MIN_STEP_PX = 320
SNAPSHOT_HYDRATION_DWELL_SECONDS = 0.8
SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS = 1.0
SNAPSHOT_PAGE_DOWN_DWELL_SECONDS = 0.2
SNAPSHOT_LOADER_WAIT_LIMIT = 16
SNAPSHOT_LOADER_WAIT_INTERVAL_SECONDS = 0.25
SNAPSHOT_ROW_EXPAND_LIMIT = 10
SNAPSHOT_ROW_SWEEP_LIMIT = 4
SNAPSHOT_ROW_SWEEP_STEP_LIMIT = 40
SNAPSHOT_ROW_SWEEP_STEP_VIEWPORT_FRACTION = 0.75
SNAPSHOT_ROW_SETTLE_SECONDS = 0.5
SNAPSHOT_STABLE_SCROLL_LIMIT = 2
SCREENSHOT_READINESS_KEY = 'screenshot_readiness'
GRAFANA_URL_KEY = 'grafana_url'
AUTH_URL_KEY = 'auth_url'
REMOVED_GRAFANA_URL_KEYS = ('host', 'nginx_prefix', 'login_url')
DEFAULT_SCREENSHOT_NETWORK_IDLE_MS = 750
DEFAULT_SCREENSHOT_NO_NETWORK_GRACE_MS = 1000
DEFAULT_SCREENSHOT_MIN_SETTLE_MS = 200
DEFAULT_SCREENSHOT_POLL_INTERVAL_MS = 100


class ConfigurationError(ValueError):
    """Raised when a dashboard lookup configuration is invalid."""


@dataclass(frozen=True)
class NoDataPreflightConfig:
    mode: str = NO_DATA_MODE_CONSERVATIVE
    timeout: int = DEFAULT_NO_DATA_TIMEOUT
    on_error: str = NO_DATA_ON_ERROR_RENDER
    store_skip_metadata: bool = True
    min_non_empty_frames: int = 1

    @classmethod
    def from_config(cls, dashboard_name: str, raw_config: Optional[Dict]) -> 'NoDataPreflightConfig':
        if raw_config is None:
            raw_config = {}
        if not isinstance(raw_config, dict):
            _raise_no_data_config_error(dashboard_name, '', raw_config, 'mapping')
        return cls(
            mode=_validated_no_data_choice(dashboard_name, raw_config, 'mode', NO_DATA_MODE_CONSERVATIVE),
            timeout=_validated_no_data_timeout(dashboard_name, raw_config.get('timeout', DEFAULT_NO_DATA_TIMEOUT)),
            on_error=_validated_no_data_choice(dashboard_name, raw_config, 'on_error', NO_DATA_ON_ERROR_RENDER),
            store_skip_metadata=_validated_no_data_bool(
                dashboard_name, raw_config.get('store_skip_metadata', True), 'store_skip_metadata'),
            min_non_empty_frames=_validated_min_non_empty_frames(dashboard_name, raw_config.get('min_non_empty_frames', 1)),
        )


@dataclass(frozen=True)
class ScreenshotReadinessConfig:
    network_idle_ms: int = DEFAULT_SCREENSHOT_NETWORK_IDLE_MS
    no_network_grace_ms: int = DEFAULT_SCREENSHOT_NO_NETWORK_GRACE_MS
    min_settle_ms: int = DEFAULT_SCREENSHOT_MIN_SETTLE_MS
    poll_interval_ms: int = DEFAULT_SCREENSHOT_POLL_INTERVAL_MS
    strict_datasource_fragments: bool = False

    @classmethod
    def from_config(cls, dashboard_name: str, raw_config: Optional[Dict]) -> 'ScreenshotReadinessConfig':
        if raw_config is None:
            raw_config = {}
        if not isinstance(raw_config, dict):
            _raise_screenshot_readiness_error(dashboard_name, '', raw_config, 'mapping')
        return cls(
            network_idle_ms=_validated_readiness_ms(
                dashboard_name, raw_config, 'network_idle_ms', DEFAULT_SCREENSHOT_NETWORK_IDLE_MS),
            no_network_grace_ms=_validated_readiness_ms(
                dashboard_name, raw_config, 'no_network_grace_ms', DEFAULT_SCREENSHOT_NO_NETWORK_GRACE_MS),
            min_settle_ms=_validated_readiness_ms(
                dashboard_name, raw_config, 'min_settle_ms', DEFAULT_SCREENSHOT_MIN_SETTLE_MS),
            poll_interval_ms=_validated_readiness_ms(
                dashboard_name, raw_config, 'poll_interval_ms', DEFAULT_SCREENSHOT_POLL_INTERVAL_MS, minimum=1),
            strict_datasource_fragments=_validated_readiness_bool(
                dashboard_name, raw_config.get('strict_datasource_fragments', False), 'strict_datasource_fragments'),
        )


@dataclass(frozen=True)
class NoDataDatasourceInference:
    applicable: bool
    datasource_type: Optional[str] = None
    datasource_uid: Optional[str] = None
    detector_name: Optional[str] = None
    reason: str = 'not_applicable'
    targets: List[Dict[str, Any]] = field(default_factory=list)
    ref_ids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class NoDataPreflightResult:
    should_skip: bool; status: str; reason: Optional[str] = None
    datasource_uid: Optional[str] = None; detector_name: Optional[str] = None


@dataclass(frozen=True)
class DashboardLookupRequest:
    dashboard_uid: Optional[str] = None; dash_title: Optional[str] = None
    folder: Optional[str] = None; folder_uid: Optional[str] = None


@dataclass(frozen=True)
class DashboardLookupResult:
    dashboard_uid: str; dashboard_title: Optional[str]
    folder_uid: Optional[str]; folder_title: Optional[str]; url: str

    @classmethod
    def from_search_result(cls, dashboard: Dict) -> 'DashboardLookupResult':
        return cls(dashboard['uid'], dashboard.get('title'), dashboard.get('folderUid'),
                   dashboard.get('folderTitle'), dashboard['url'])


@dataclass(frozen=True)
class GrafanaUrlParts:
    origin: str
    app_path: str
    base_url: str


@dataclass(frozen=True)
class _SelectorConfig:
    ids: List[int]; titles: List[str]; typed_titles: List[Tuple[str, str]]
    title_regex: List[Pattern[str]]; title_regex_values: List[str]
    inline_renames: Dict[Tuple[str, Optional[str]], str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, dashboard_name: str, section_path: str, raw_config: Optional[Dict], allow_typed_titles: bool = False) -> '_SelectorConfig':
        if raw_config is None:
            raw_config = {}
        if not isinstance(raw_config, dict):
            _raise_panel_filtering_error(dashboard_name, f'.{section_path}', raw_config, 'mapping')
        regex_values = _validated_selector_values(dashboard_name, section_path, raw_config, 'title_regex', str)
        titles, typed_titles, inline_renames = _validated_title_selector_values(
            dashboard_name, section_path, raw_config, allow_typed_titles)
        return cls(_validated_selector_values(dashboard_name, section_path, raw_config, 'ids', int),
                   titles, typed_titles,
                   _compile_selector_regexes(dashboard_name, section_path, regex_values), regex_values, inline_renames)

    def has_selectors(self) -> bool:
        return bool(self.ids or self.titles or self.typed_titles or self.title_regex)


@dataclass(frozen=True)
class PanelFilteringConfig:
    mode: str = INCLUDE_ALL_EXCEPT_EXCLUDED
    include_panels: _SelectorConfig = field(default_factory=lambda: _SelectorConfig([], [], [], [], []))
    exclude_panels: _SelectorConfig = field(default_factory=lambda: _SelectorConfig([], [], [], [], []))
    include_rows: _SelectorConfig = field(default_factory=lambda: _SelectorConfig([], [], [], [], []))
    exclude_rows: _SelectorConfig = field(default_factory=lambda: _SelectorConfig([], [], [], [], []))

    @classmethod
    def from_config(cls, dashboard_name: str, raw_config: Optional[Dict]) -> 'PanelFilteringConfig':
        if raw_config is None:
            return cls()
        if not isinstance(raw_config, dict):
            _raise_panel_filtering_error(dashboard_name, '', raw_config, 'mapping')
        filtering_config = cls(
            mode=_validated_filtering_mode(dashboard_name, raw_config.get('mode', INCLUDE_ALL_EXCEPT_EXCLUDED)),
            include_panels=_SelectorConfig.from_config(dashboard_name, 'include_panels', raw_config.get('include_panels'), True),
            exclude_panels=_SelectorConfig.from_config(dashboard_name, 'exclude_panels', raw_config.get('exclude_panels'), True),
            include_rows=_SelectorConfig.from_config(dashboard_name, 'include_rows', raw_config.get('include_rows')),
            exclude_rows=_SelectorConfig.from_config(dashboard_name, 'exclude_rows', raw_config.get('exclude_rows')))
        filtering_config.validate_include_rules(dashboard_name)
        return filtering_config

    def validate_include_rules(self, dashboard_name: str) -> None:
        if self.mode != INCLUDE_ONLY_SELECTED:
            return
        if self.include_panels.has_selectors() or self.include_rows.has_selectors():
            return
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{PANEL_FILTERING_KEY}.mode: invalid value="{INCLUDE_ONLY_SELECTED}", '
            'expected include selectors in include_panels or include_rows, suggested fix: add ids, titles, '
            'or title_regex under include_panels/include_rows')


@dataclass(frozen=True)
class PanelDescriptor:
    panel_id: int; graph_type: str; title: Optional[str]
    row_title: Optional[str]; row_id: Optional[int]; grid_pos: Optional[Dict]
    from_collapsed_row: bool; repeat: Optional[str] = None; row_repeat: Optional[str] = None
    repeat_panel_id: Optional[int] = None; raw_panel: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw_panel(cls, raw_panel: Dict) -> 'PanelDescriptor':
        return cls(raw_panel['id'], raw_panel['type'], raw_panel.get('title') or None,
                   raw_panel.get('row_title'), raw_panel.get('row_id'), raw_panel.get('gridPos'),
                   raw_panel.get('from_collapsed_row', False), raw_panel.get('repeat') or None,
                   raw_panel.get('row_repeat') or None, raw_panel.get('repeatPanelId'), raw_panel)

    def effective_repeat(self) -> Optional[str]:
        return self.repeat or self.row_repeat


@dataclass(frozen=True)
class PanelDefinition:
    panel_id: int; graph_type: str; title: Optional[str]
    repeat: Optional[str] = None; row_title: Optional[str] = None; row_id: Optional[int] = None


class Panel:
    def __init__(self, panel_id: int, graph_type: str, title: str, timestamps_count: int,
                  links: Optional[List[str]] = None, row_title: Optional[str] = None,
                  from_collapsed_row: bool = False, row_id: Optional[int] = None,
                  grid_pos: Optional[Dict] = None, is_repeating: bool = False,
                  source_panel_id: Optional[int] = None, repeat_var: Optional[str] = None,
                  artifacts: Optional[List[Dict[str, Any]]] = None,
                  display_title: Optional[str] = None):
        self.panel_id: int = panel_id; self.type: str = graph_type; self.title: str = title
        self.display_title: str = display_title or title
        self.row_title: Optional[str] = row_title; self.from_collapsed_row: bool = from_collapsed_row
        self.row_id: Optional[int] = row_id; self.grid_pos: Optional[Dict] = grid_pos
        self.is_repeating: bool = is_repeating; self.source_panel_id: Optional[int] = source_panel_id
        self.repeat_var: Optional[str] = repeat_var; self.artifacts: List[Dict[str, Any]] = list(artifacts or [])
        self.links: List[Optional[str]] = links if links else [None for _ in range(timestamps_count)]


@dataclass(frozen=True)
class PanelRenderTask:
    panel: Panel; timestamp: GrafanaTimeDownloader; variables: Optional[Dict[str, Any]] = None
    file_name: Optional[str] = None; artifact: Optional[Dict[str, Any]] = None
    repeat_var: Optional[str] = None; repeat_value: Optional[str] = None
    raw_panel: Optional[Dict[str, Any]] = None; collect_no_data_panels: bool = True


class GrafanaConfigBase(ABC):
    def __init__(self, name: str):
        self.name: str = name
        self.panels: Optional[List[Panel]] = None; self.full_links: Optional[List[str]] = None
        self.backup_dashboard_links: List[str] = []
        self.snapshot_urls: Optional[List[str]] = None; self.dashboard_uid: Optional[str] = None
        self.dashboard_title: Optional[str] = None; self.folder_uid: Optional[str] = None
        self.folder_title: Optional[str] = None


class GrafanaConfigUploader(GrafanaConfigBase):
    def __init__(self, name: str, config: Dict):
        super().__init__(name)
        self.dashboard_uid = config.get('dashboard_uid'); self.dashboard_title = config.get('dashboard_title')
        self.folder_uid = config.get('folder_uid'); self.folder_title = config.get('folder_title')
        self.panels: Optional[List[Panel]] = []
        if isinstance(config['panels'][0], Panel):
            self.panels = config['panels']
        else:
            for panel in config['panels']:
                panel = self.__with_legacy_repeat_artifact(panel, config['timestamps'])
                self.panels.append(self.__panel_from_metadata(panel, config['timestamps']))
        self.full_links: Optional[List[str]] = config['full_links']; self.snapshot_urls: Optional[List[str]] = config.get('snapshot_urls', [])
        self.backup_dashboard_links: List[str] = config.get('backup_dashboard_links', []) or []
        self.charts_path: str = config['charts_path']; self.timestamps: List[GrafanaTimeUploader] = []
        for timestamp in config['timestamps']:
            self.timestamps.append(timestamp if isinstance(timestamp, GrafanaTimeUploader) else GrafanaTimeUploader(timestamp))

    def __panel_from_metadata(self, panel: Dict, timestamps: List) -> Panel:
        return Panel(panel['panel_id'], panel['type'], panel['title'], len(timestamps),
                     self.__panel_links(panel, timestamps), panel.get('row_title'), panel.get('from_collapsed_row', False),
                     panel.get('row_id'), panel.get('grid_pos'), panel.get('is_repeating', False),
                     panel.get('source_panel_id'), panel.get('repeat_var'), panel.get('artifacts'),
                     panel.get('display_title'))

    def __panel_links(self, panel: Dict, timestamps: List) -> List[Optional[str]]:
        if panel.get('artifacts'):
            return self.__artifact_links(panel['artifacts'], len(timestamps), panel.get('links', []))
        return panel.get('links', [])

    def __with_legacy_repeat_artifact(self, panel: Dict, timestamps: List) -> Dict:
        if panel.get('artifacts') or not self.__has_flat_repeat_metadata(panel):
            return panel
        panel_with_artifact = dict(panel); panel_with_artifact['artifacts'] = [self.__legacy_repeat_artifact(panel, timestamps)]
        return panel_with_artifact

    @staticmethod
    def __has_flat_repeat_metadata(panel: Dict) -> bool:
        return bool(panel.get('is_repeating')) and panel.get('repeat_value') is not None

    def __legacy_repeat_artifact(self, panel: Dict, timestamps: List) -> Dict[str, Any]:
        timestamp = timestamps[0] if timestamps else {}; id_time = self.__timestamp_value(timestamp, 'id_time', 0)
        links = panel.get('links') or []
        return {
            'timestamp_tag': self.__timestamp_value(timestamp, 'time_tag'),
            'from': str(self.__timestamp_value(timestamp, 'start_time_timestamp', '')),
            'to': str(self.__timestamp_value(timestamp, 'end_time_timestamp', '')),
            'render_status': panel.get('render_status', 'rendered'),
            'png_file': panel.get('png_file') or f'{self.name}__{panel["panel_id"]}__{id_time}.png',
            'skip_reason': panel.get('skip_reason'), 'repeat_var': panel.get('repeat_var'),
            'repeat_value': panel.get('repeat_value'), 'repeat_value_slug': panel.get('repeat_value_slug'),
            'link': links[0] if links else None,
        }

    @staticmethod
    def __timestamp_value(timestamp, field_name: str, default=None):
        return timestamp.get(field_name, default) if isinstance(timestamp, dict) else getattr(timestamp, field_name, default)

    @staticmethod
    def __artifact_links(artifacts: List[Dict], timestamps_count: int, fallback: List) -> List[Optional[str]]:
        links = list(fallback) if fallback else [None for _ in range(timestamps_count)]
        for index, artifact in enumerate(artifacts):
            timestamp_index = min(index, timestamps_count - 1)
            if artifact.get('link') and timestamp_index < len(links):
                links[timestamp_index] = artifact['link']
        return links


class GrafanaConfigDownloader(GrafanaConfigBase):
    def __init__(self, name: str, config: Dict):
        super().__init__(name)
        _reject_removed_grafana_url_keys(name, config)
        grafana_url = _validated_grafana_url(name, config)
        self.dash_title: Optional[str] = config.get('dash_title'); self.dashboard_uid: Optional[str] = config.get('dashboard_uid')
        self.grafana_url: str = grafana_url.base_url; self.grafana_origin: str = grafana_url.origin
        self.grafana_app_path: str = grafana_url.app_path; self.grafana_base_url: str = grafana_url.base_url
        self.auth_url: Optional[str] = _validated_auth_url(name, config.get(AUTH_URL_KEY))
        self.width: int = config.get('width', 1920); self.height: int = config.get('height', 1080)
        self.render: bool = config.get('render', True); self.snapshot: bool = config.get('snapshot', False)
        self.snapshot_timeout: int = config.get('snapshot_timeout', 30); self.snapshot_mode: str = _validated_snapshot_mode(self.name, config)
        self.snapshot_fallback_to_ui: bool = _validated_snapshot_fallback_to_ui(self.name, config)
        self.snapshot_expires: int = _validated_snapshot_expires(self.name, config)
        self.snapshot_store_dashboard_json: bool = _validated_bool_config(self.name, config, 'snapshot_store_dashboard_json', True)
        self.firefox_driver_preload_time: float = config.get('firefox_driver_preload_time', 2.5)
        self.timeout: int = config.get('timeout', 30); self.tz: Optional[str] = config.get('tz', None)
        self.threads: int = config.get('threads', 4); self.vars: Optional[Dict[str, str]] = config.get('vars', None)
        self.playwright_browser: Optional[str] = config.get('playwright_browser', None)
        self.playwright_browser_channel: Optional[str] = config.get('playwright_browser_channel', None)
        self.playwright_browser_executable_path: Optional[str] = config.get('playwright_browser_executable_path', None)
        self.enable_repeating_panels: bool = _validated_bool_config(self.name, config, ENABLE_REPEATING_PANELS_KEY, False)
        self.repeating_panels: List[Dict] = _validated_repeating_panels(self.name, config)
        self.collect_no_data_panels: bool = _validated_bool_config(self.name, config, COLLECT_NO_DATA_PANELS_KEY, True)
        self.no_data_preflight: NoDataPreflightConfig = NoDataPreflightConfig.from_config(self.name, config.get(NO_DATA_PREFLIGHT_KEY))
        self.screenshot_readiness: ScreenshotReadinessConfig = ScreenshotReadinessConfig.from_config(
            self.name, config.get(SCREENSHOT_READINESS_KEY))
        self.white_theme: bool = config.get('white_theme', False); self.orgId: int = config.get('orgId', 1)
        self.login: Optional[str] = config.get('login', None); self.password: Optional[str] = config.get('password', None)
        self.token: Optional[str] = config.get('token', None); self.auth: bool = config.get('auth', True)
        self.domain: bool = config.get('domain', False); self.verify_ssl: bool = config.get('verify_ssl', True)
        self.folder: Optional[str] = config.get('folder', None); self.folder_uid: Optional[str] = config.get('folder_uid', None)
        self.folder_title: Optional[str] = config.get('folder_title', None); self.download_collapsed_rows: bool = _validated_collapsed_rows(self.name, config)
        self.backup_dashboard_links: List[str] = _validated_backup_dashboard_links(self.name, config)
        self.download_collapse_panels: bool = self.download_collapsed_rows; self.disable_graph_types: List = config.get('disable_graph_types', [])
        self.panel_filtering: PanelFilteringConfig = PanelFilteringConfig.from_config(self.name, config.get(PANEL_FILTERING_KEY))
        self.rename_panels: List[Dict[str, Any]] = _validated_rename_panels(self.name, config)
        _validate_dashboard_lookup(self.name, self.dashboard_uid, self.dash_title, self.folder, self.folder_uid)


def _reject_removed_grafana_url_keys(dashboard_name: str, config: Dict) -> None:
    removed_keys = [key for key in REMOVED_GRAFANA_URL_KEYS if key in config]
    if removed_keys:
        keys = ', '.join(removed_keys)
        raise ConfigurationError(
            f'dashboards.{dashboard_name}: removed Grafana URL key(s): {keys}; '
            f'expected {GRAFANA_URL_KEY} and optional {AUTH_URL_KEY}'
        )


def _validated_grafana_url(dashboard_name: str, config: Dict) -> GrafanaUrlParts:
    value = config.get(GRAFANA_URL_KEY)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{GRAFANA_URL_KEY}: invalid value="{value}", '
            'expected full Grafana base URL with http(s) scheme'
        )
    parsed = urlparse(value.strip())
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{GRAFANA_URL_KEY}: invalid value="{value}", '
            'expected absolute URL with http(s) scheme'
        )
    if parsed.query or parsed.fragment:
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{GRAFANA_URL_KEY}: invalid value="{value}", '
            'expected Grafana base URL without query or fragment'
        )
    app_path = _normalized_app_path(parsed.path)
    origin = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
    return GrafanaUrlParts(origin, app_path, f'{origin}{app_path}')


def _validated_auth_url(dashboard_name: str, value: Any) -> Optional[str]:
    if value in (None, ''):
        return None
    if not isinstance(value, str):
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{AUTH_URL_KEY}: invalid value="{value}", expected absolute URL'
        )
    parsed = urlparse(value)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{AUTH_URL_KEY}: invalid value="{value}", '
            'expected absolute URL with http(s) scheme'
        )
    return value


def _normalized_app_path(path: str) -> str:
    normalized_path = (path or '').rstrip('/')
    if normalized_path in ('', '/'):
        return ''
    return normalized_path if normalized_path.startswith('/') else f'/{normalized_path}'


def normalize_grafana_dashboard_route(dashboard_name: str, raw_url: str,
                                      grafana_origin: str, grafana_app_path: str) -> str:
    return _normalize_grafana_app_route(dashboard_name, raw_url, grafana_origin, grafana_app_path, '/d/')


def _normalize_grafana_app_route(dashboard_name: str, raw_url: str, grafana_origin: str,
                                 grafana_app_path: str, route_prefix: str) -> str:
    if not isinstance(raw_url, str) or not raw_url:
        raise ConfigurationError(f'dashboards.{dashboard_name}: Grafana API returned empty dashboard url')
    parsed = urlparse(raw_url)
    if parsed.scheme or parsed.netloc:
        _validate_same_grafana_origin(dashboard_name, raw_url, parsed, grafana_origin)
    path = parsed.path or raw_url
    path = path if path.startswith('/') else f'/{path}'
    route = _strip_grafana_app_path(path, grafana_app_path)
    if not route.startswith(route_prefix):
        raise ConfigurationError(
            f'dashboards.{dashboard_name}: Grafana API returned unsupported dashboard url="{raw_url}", '
            f'expected route starting with {route_prefix}'
        )
    return route


def _validate_same_grafana_origin(dashboard_name: str, raw_url: str, parsed, grafana_origin: str) -> None:
    origin = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
    if origin != grafana_origin:
        raise ConfigurationError(
            f'dashboards.{dashboard_name}: Grafana API returned cross-origin dashboard url="{raw_url}"'
        )


def _strip_grafana_app_path(path: str, grafana_app_path: str) -> str:
    if grafana_app_path and (path == grafana_app_path or path.startswith(f'{grafana_app_path}/')):
        stripped = path[len(grafana_app_path):]
        return stripped or '/'
    return path


def _validated_collapsed_rows(dashboard_name: str, config: Dict) -> bool:
    _reject_hidden_panels(dashboard_name, config)
    new_present = DOWNLOAD_COLLAPSED_ROWS_KEY in config; legacy_present = DOWNLOAD_COLLAPSE_PANELS_KEY in config
    if new_present and legacy_present:
        _warn_if_collapsed_row_keys_conflict(dashboard_name, config); return config[DOWNLOAD_COLLAPSED_ROWS_KEY]
    if new_present:
        return config[DOWNLOAD_COLLAPSED_ROWS_KEY]
    if legacy_present:
        logger.info(f'Using legacy config key {DOWNLOAD_COLLAPSE_PANELS_KEY}={config[DOWNLOAD_COLLAPSE_PANELS_KEY]}')
        return config[DOWNLOAD_COLLAPSE_PANELS_KEY]
    return False


def _validated_backup_dashboard_links(dashboard_name: str, config: Dict) -> List[str]:
    value = config.get('backup_dashboard_links', [])
    if value is None:
        return []
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    raise ConfigurationError(
        f'dashboards.{dashboard_name}.backup_dashboard_links: invalid value="{value}", '
        'expected list[str], suggested fix: configure backup dashboard URLs as a YAML list'
    )


def _reject_hidden_panels(dashboard_name: str, config: Dict) -> None:
    if DOWNLOAD_HIDDEN_PANELS_KEY not in config:
        return
    raise ConfigurationError(f'dashboards.{dashboard_name}.{DOWNLOAD_HIDDEN_PANELS_KEY}: invalid value={config[DOWNLOAD_HIDDEN_PANELS_KEY]}, expected unsupported in phase 1, suggested fix: remove this key and use {DOWNLOAD_COLLAPSED_ROWS_KEY}')


def _warn_if_collapsed_row_keys_conflict(dashboard_name: str, config: Dict) -> None:
    if config[DOWNLOAD_COLLAPSED_ROWS_KEY] == config[DOWNLOAD_COLLAPSE_PANELS_KEY]:
        return
    logger.warning(f'Config key {DOWNLOAD_COLLAPSED_ROWS_KEY} overrides legacy {DOWNLOAD_COLLAPSE_PANELS_KEY}: dashboards.{dashboard_name}.{DOWNLOAD_COLLAPSED_ROWS_KEY}={_yaml_bool(config[DOWNLOAD_COLLAPSED_ROWS_KEY])}, {DOWNLOAD_COLLAPSE_PANELS_KEY}={_yaml_bool(config[DOWNLOAD_COLLAPSE_PANELS_KEY])}')


def _yaml_bool(value: bool) -> str:
    return str(value).lower()

def _validated_snapshot_mode(dashboard_name: str, config: Dict) -> str:
    if 'snapshot_mode' not in config:
        return SNAPSHOT_MODE_UI
    value = config.get('snapshot_mode')
    if value in SNAPSHOT_MODES:
        logger.warning(f'dashboards.{dashboard_name}.snapshot_mode is deprecated and ignored; GrafConflux now uses the automatic UI snapshot flow')
        return SNAPSHOT_MODE_UI
    raise ConfigurationError(f'dashboards.{dashboard_name}.snapshot_mode: invalid value="{value}", expected a deprecated value in {SNAPSHOT_MODES} or no key, suggested fix: remove snapshot_mode')


def _validated_snapshot_fallback_to_ui(dashboard_name: str, config: Dict) -> bool:
    if 'snapshot_fallback_to_ui' not in config:
        return True
    value = config.get('snapshot_fallback_to_ui')
    if not isinstance(value, bool):
        raise ConfigurationError(f'dashboards.{dashboard_name}.snapshot_fallback_to_ui: invalid value="{value}", expected bool, suggested fix: remove snapshot_fallback_to_ui')
    logger.warning(f'dashboards.{dashboard_name}.snapshot_fallback_to_ui is deprecated and ignored; the automatic UI snapshot flow has no API fallback mode')
    return True


def _validated_bool_config(dashboard_name: str, config: Dict, key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    raise ConfigurationError(f'dashboards.{dashboard_name}.{key}: invalid value="{value}", expected bool, suggested fix: set true or false')


def _validated_snapshot_expires(dashboard_name: str, config: Dict) -> int:
    value = config.get('snapshot_expires', 0)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        if 'snapshot_expires' in config:
            logger.warning(f'dashboards.{dashboard_name}.snapshot_expires is deprecated and ignored; UI snapshots use Grafana snapshot form settings')
        return value
    raise ConfigurationError(f'dashboards.{dashboard_name}.snapshot_expires: invalid value={value}, expected integer >= 0, suggested fix: remove the key or set 0')


def _validated_repeating_panels(dashboard_name: str, config: Dict) -> List[Dict]:
    value = config.get(REPEATING_PANELS_KEY, [])
    if value is None:
        return []
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return list(value)
    raise ConfigurationError(f'dashboards.{dashboard_name}.{REPEATING_PANELS_KEY}: invalid value="{value}", expected list[object], suggested fix: configure explicit repeating panel rules')


def _validate_dashboard_lookup(dashboard_name: str, dashboard_uid: Optional[str], dash_title: Optional[str],
                               folder: Optional[str], folder_uid: Optional[str]) -> None:
    has_uid = bool(dashboard_uid); has_title = bool(dash_title)
    if has_uid and has_title:
        raise ConfigurationError(f'dashboards.{dashboard_name}: dashboard_uid and dash_title are mutually exclusive in phase 1, suggested fix: keep exactly one lookup variant')
    if not has_title and (folder or folder_uid):
        raise ConfigurationError(f'dashboards.{dashboard_name}.folder: invalid value without dash_title, expected title lookup, suggested fix: remove folder fields or configure dash_title')
    if not has_uid and not has_title:
        raise ConfigurationError(f'dashboards.{dashboard_name}: invalid lookup value=None, expected dashboard_uid or dash_title, suggested fix: configure exactly one lookup variant')


def _compile_selector_regexes(dashboard_name: str, section_path: str, regex_values: List[str]) -> List[Pattern[str]]:
    return [_compile_selector_regex(dashboard_name, section_path, regex_value, index) for index, regex_value in enumerate(regex_values)]


def _compile_selector_regex(dashboard_name: str, section_path: str, regex_value: str, index: int) -> Pattern[str]:
    try:
        return re.compile(regex_value)
    except re.error as error:
        raise ConfigurationError(f'dashboards.{dashboard_name}.{PANEL_FILTERING_KEY}.{section_path}.title_regex[{index}]: invalid value="{regex_value}", expected valid regular expression, suggested fix: escape special characters or use a valid regex ({error})') from error


def _validated_filtering_mode(dashboard_name: str, mode: str) -> str:
    if mode in PANEL_FILTERING_MODES:
        return mode
    expected = ', '.join(PANEL_FILTERING_MODES)
    raise ConfigurationError(f'dashboards.{dashboard_name}.{PANEL_FILTERING_KEY}.mode: invalid value="{mode}", expected one of [{expected}], suggested fix: use {INCLUDE_ONLY_SELECTED}')


def _validated_selector_values(dashboard_name: str, section_path: str, raw_config: Dict, selector_name: str, expected_type: type) -> List:
    values = raw_config.get(selector_name, [])
    if values is None:
        return []
    if not isinstance(values, list):
        _raise_panel_filtering_error(dashboard_name, f'.{section_path}.{selector_name}', values, 'list')
    _validate_selector_item_types(dashboard_name, section_path, selector_name, values, expected_type)
    return list(values)

def _validated_title_selector_values(dashboard_name: str, section_path: str, raw_config: Dict,
                                     allow_typed_titles: bool) -> Tuple[List[str], List[Tuple[str, str]], Dict[Tuple[str, Optional[str]], str]]:
    values = raw_config.get('titles', [])
    if values is None:
        return [], [], {}
    if not isinstance(values, list):
        _raise_panel_filtering_error(dashboard_name, f'.{section_path}.titles', values, 'list')
    titles, typed_titles, inline_renames = [], [], {}
    for index, value in enumerate(values):
        if isinstance(value, str): titles.append(value); continue
        if allow_typed_titles and isinstance(value, dict) and len(value) == 1:
            [(title, panel_type)] = value.items()
            if isinstance(title, str) and isinstance(panel_type, str):
                typed_titles.append((title, panel_type))
                continue
            if isinstance(title, str) and isinstance(panel_type, dict):
                rename = panel_type.get('rename')
                typed_name = panel_type.get('type')
                if section_path == 'exclude_panels' and rename is not None:
                    _raise_panel_filtering_error(dashboard_name, f'.{section_path}.titles[{index}]', value, 'string or one-item mapping {title: panel_type}')
                if isinstance(rename, str) and typed_name is None:
                    titles.append(title)
                    inline_renames[(title, None)] = rename
                    continue
                if isinstance(rename, str) and isinstance(typed_name, str):
                    typed_titles.append((title, typed_name))
                    inline_renames[(title, typed_name)] = rename
                    continue
        _raise_panel_filtering_error(
            dashboard_name, f'.{section_path}.titles[{index}]', value,
            'string or one-item mapping {title: panel_type} or {title: {rename: display_title[, type: panel_type]}}')
    return titles, typed_titles, inline_renames


def _validated_rename_panels(dashboard_name: str, config: Dict) -> List[Dict[str, Any]]:
    value = config.get(RENAME_PANELS_KEY, [])
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigurationError(
            f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}: invalid value="{value}", '
            'expected list[object], suggested fix: configure rename_panels as a YAML list of selector mappings')
    renamed = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}]: invalid value="{item}", '
                'expected mapping, suggested fix: use id/title/type/rename fields')
        rename = item.get('rename')
        title = item.get('title')
        panel_id = item.get('id')
        panel_type = item.get('type')
        if not isinstance(rename, str) or not rename:
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}].rename: invalid value="{rename}", '
                'expected non-empty string, suggested fix: set rename to the display title')
        if panel_id is None and title is None:
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}]: invalid value="{item}", '
                'expected id or title, suggested fix: configure at least one selector field')
        if panel_id is not None and (not isinstance(panel_id, int) or isinstance(panel_id, bool)):
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}].id: invalid value="{panel_id}", '
                'expected int, suggested fix: use a numeric panel id')
        if title is not None and not isinstance(title, str):
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}].title: invalid value="{title}", '
                'expected str, suggested fix: use the panel title')
        if panel_type is not None and not isinstance(panel_type, str):
            raise ConfigurationError(
                f'dashboards.{dashboard_name}.{RENAME_PANELS_KEY}[{index}].type: invalid value="{panel_type}", '
                'expected str, suggested fix: use the Grafana panel type')
        renamed.append({'id': panel_id, 'title': title, 'type': panel_type, 'rename': rename})
    return renamed


def _validate_selector_item_types(dashboard_name: str, section_path: str, selector_name: str, values: List, expected_type: type) -> None:
    for index, value in enumerate(values):
        if isinstance(value, expected_type) and not isinstance(value, bool):
            continue
        _raise_panel_filtering_error(dashboard_name, f'.{section_path}.{selector_name}[{index}]', value, expected_type.__name__)


def _raise_panel_filtering_error(dashboard_name: str, path: str, value, expected: str) -> None:
    raise ConfigurationError(f'dashboards.{dashboard_name}.{PANEL_FILTERING_KEY}{path}: invalid value="{value}", expected {expected}, suggested fix: use documented panel_filtering mapping fields')


def _validated_no_data_choice(dashboard_name: str, raw_config: Dict, key: str, expected: str) -> str:
    value = raw_config.get(key, expected)
    if value == expected:
        return value
    _raise_no_data_config_error(dashboard_name, f'.{key}', value, expected)


def _validated_no_data_timeout(dashboard_name: str, value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    _raise_no_data_config_error(dashboard_name, '.timeout', value, 'positive integer')


def _validated_readiness_ms(dashboard_name: str, raw_config: Dict, key: str, default: int,
                            minimum: int = 0) -> int:
    value = raw_config.get(key, default)
    if isinstance(value, int) and not isinstance(value, bool) and value >= minimum:
        return value
    _raise_screenshot_readiness_error(dashboard_name, f'.{key}', value, f'integer >= {minimum}')


def _validated_readiness_bool(dashboard_name: str, value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    _raise_screenshot_readiness_error(dashboard_name, f'.{key}', value, 'bool')


def _validated_no_data_bool(dashboard_name: str, value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    _raise_no_data_config_error(dashboard_name, f'.{key}', value, 'bool')


def _validated_min_non_empty_frames(dashboard_name: str, value: Any) -> int:
    if value == 1 and not isinstance(value, bool):
        return value
    _raise_no_data_config_error(dashboard_name, '.min_non_empty_frames', value, '1 in phase 1')


def _raise_no_data_config_error(dashboard_name: str, path: str, value: Any, expected: str) -> None:
    raise ConfigurationError(f'dashboards.{dashboard_name}.{NO_DATA_PREFLIGHT_KEY}{path}: invalid value="{value}", expected {expected}, suggested fix: remove the key or use {expected}')


def _raise_screenshot_readiness_error(dashboard_name: str, path: str, value: Any, expected: str) -> None:
    raise ConfigurationError(
        f'dashboards.{dashboard_name}.{SCREENSHOT_READINESS_KEY}{path}: invalid value="{value}", '
        f'expected {expected}, suggested fix: remove the key or use documented screenshot readiness fields')
