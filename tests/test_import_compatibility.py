import ast
import importlib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


class TestImportCompatibility(unittest.TestCase):
    def assert_exports(self, module_name, names):
        module = importlib.import_module(module_name)
        missing = [name for name in names if not hasattr(module, name)]
        self.assertEqual([], missing, f"Missing exports from {module_name}: {missing}")
        return module

    def assert_identity_exports(self, facade, implementation, names):
        for name in names:
            with self.subTest(facade=facade.__name__, name=name):
                self.assertIs(getattr(facade, name), getattr(implementation, name))

    def test_grafana_facade_exports_model_symbols(self):
        grafana = self.assert_exports("grafconflux.grafana", GRAFANA_MODEL_SYMBOLS)
        models = importlib.import_module("grafconflux.grafana_models")

        self.assert_identity_exports(grafana, models, GRAFANA_MODEL_SYMBOLS)

    def test_grafana_models_facade_re_exports_shared_model_symbols(self):
        models = self.assert_exports("grafconflux.grafana_models", GRAFANA_SHARED_MODEL_SYMBOLS)
        shared_models = importlib.import_module("grafconflux._shared.grafana_models")

        self.assert_identity_exports(models, shared_models, GRAFANA_SHARED_MODEL_SYMBOLS)

    def test_grafana_facade_exports_helper_symbols(self):
        grafana = self.assert_exports("grafconflux.grafana", GRAFANA_HELPER_SYMBOLS)
        modules = {
            "grafconflux.dashboard_lookup": DASHBOARD_LOOKUP_SYMBOLS,
            "grafconflux.panel_selection": PANEL_SELECTION_SYMBOLS,
            "grafconflux.no_data": NO_DATA_SYMBOLS,
            "grafconflux.grafana_rendering": GRAFANA_RENDERING_SYMBOLS,
            "grafconflux.repeating": REPEATING_SYMBOLS,
            "grafconflux.snapshots": SNAPSHOT_SYMBOLS,
            "grafconflux.browser_session": BROWSER_SESSION_SYMBOLS,
        }
        for module_name, names in modules.items():
            self.assert_identity_exports(grafana, importlib.import_module(module_name), names)

    def test_grafana_helper_facades_re_export_private_symbols(self):
        modules = {
            "grafconflux.dashboard_lookup": ("grafconflux._grafana.lookup", DASHBOARD_LOOKUP_SYMBOLS),
            "grafconflux.panel_selection": ("grafconflux._grafana.panel_selection", PANEL_SELECTION_SYMBOLS),
            "grafconflux.grafana_rendering": ("grafconflux._grafana.rendering", GRAFANA_RENDERING_SYMBOLS),
            "grafconflux.no_data": ("grafconflux._grafana.no_data", NO_DATA_SYMBOLS),
            "grafconflux.repeating": ("grafconflux._grafana.repeating", REPEATING_FACADE_SYMBOLS),
            "grafconflux.repeat_tasks": ("grafconflux._grafana.repeat_tasks", REPEAT_TASK_SYMBOLS),
            "grafconflux.snapshots": ("grafconflux._grafana.snapshots", SNAPSHOT_SYMBOLS),
            "grafconflux.browser_session": ("grafconflux._grafana.browser_session", BROWSER_SESSION_SYMBOLS),
        }
        for facade_name, (private_name, names) in modules.items():
            with self.subTest(facade=facade_name):
                facade = self.assert_exports(facade_name, names)
                private_module = importlib.import_module(private_name)

                self.assert_identity_exports(facade, private_module, names)

    def test_confluence_facade_exports_public_symbols(self):
        confluence = self.assert_exports("grafconflux.confluence", CONFLUENCE_SYMBOLS)
        content = importlib.import_module("grafconflux.confluence_content")
        uploads = importlib.import_module("grafconflux.confluence_uploads")

        self.assert_identity_exports(confluence, content, CONFLUENCE_CONTENT_SYMBOLS)
        self.assert_identity_exports(confluence, uploads, CONFLUENCE_UPLOAD_SYMBOLS)

    def test_confluence_helper_facades_re_export_private_symbols(self):
        modules = {
            "grafconflux.confluence_content": ("grafconflux._confluence.content", CONFLUENCE_CONTENT_FACADE_SYMBOLS),
            "grafconflux.confluence_uploads": ("grafconflux._confluence.uploads", CONFLUENCE_UPLOAD_FACADE_SYMBOLS),
        }
        for facade_name, (private_name, names) in modules.items():
            with self.subTest(facade=facade_name):
                facade = self.assert_exports(facade_name, names)
                private_module = importlib.import_module(private_name)

                self.assert_identity_exports(facade, private_module, names)

    def test_config_facades_export_compatibility_symbols(self):
        for module_name, names in CONFIG_FACADE_SYMBOLS.items():
            with self.subTest(module=module_name):
                self.assert_exports(module_name, names)

    def test_config_facades_re_export_private_symbols(self):
        modules = {
            "grafconflux.args_parser": ("grafconflux._config.args_parser", ["ArgsParser"]),
            "grafconflux.config": ("grafconflux._config.file_options", CONFIG_PRIVATE_HELPER_SYMBOLS),
            "grafconflux.yaml_settings": ("grafconflux._config.yaml_settings", YAML_SETTINGS_SYMBOLS),
            "grafconflux.options": ("grafconflux._config.options", ["GrafConfluxRunOptions"]),
        }
        for facade_name, (private_name, names) in modules.items():
            with self.subTest(facade=facade_name):
                facade = self.assert_exports(facade_name, names)
                private_module = importlib.import_module(private_name)

                self.assert_identity_exports(facade, private_module, names)

    def test_orchestration_facade_exports_private_symbols(self):
        facade = self.assert_exports("grafconflux.orchestration", ORCHESTRATION_FACADE_SYMBOLS)
        runner = self.assert_exports("grafconflux._orchestration.runner", ORCHESTRATION_RUNNER_SYMBOLS)
        upload_merge = self.assert_exports(
            "grafconflux._orchestration.upload_merge",
            ORCHESTRATION_UPLOAD_MERGE_SYMBOLS,
        )

        self.assertIs(facade.get_yaml_files, runner.get_yaml_files)
        self.assertIs(facade.transform_grafana_configs, upload_merge.transform_grafana_configs)
        self.assertIs(facade._shift_png_file_name, upload_merge._shift_png_file_name)

    def test_args_parser_re_exports_shared_time_symbols(self):
        args_parser = self.assert_exports(
            "grafconflux.args_parser",
            CONFIG_FACADE_SYMBOLS["grafconflux.args_parser"],
        )
        shared_time = importlib.import_module("grafconflux._shared.time")

        self.assert_identity_exports(
            args_parser,
            shared_time,
            ["GrafanaTimeBase", "GrafanaTimeDownloader", "GrafanaTimeUploader"],
        )

    def test_main_exports_legacy_compatibility_symbols(self):
        main = self.assert_exports("main", MAIN_EXPORT_SYMBOLS)
        modules = {
            "grafconflux.args_parser": ["ArgsParser"],
            "grafconflux.confluence": ["ConfluenceManager"],
            "grafconflux.grafana": ["GrafanaConfigDownloader", "GrafanaConfigUploader", "GrafanaManager"],
            "grafconflux.orchestration": ORCHESTRATION_SYMBOLS,
        }
        for module_name, names in modules.items():
            self.assert_identity_exports(main, importlib.import_module(module_name), names)

    def test_grafana_manager_preserves_name_mangled_methods(self):
        grafana = importlib.import_module("grafconflux.grafana")
        config = grafana.GrafanaConfigDownloader("demo", GRAFANA_CONFIG)
        manager = grafana.GrafanaManager(config)

        self.assertEqual("GrafanaManager", grafana.GrafanaManager.__name__)
        for method_name in GRAFANA_MANAGER_MANGLED_METHODS:
            with self.subTest(method_name=method_name):
                self.assertTrue(callable(getattr(manager, method_name, None)))

    def test_grafana_manager_private_methods_remain_patchable(self):
        grafana = importlib.import_module("grafconflux.grafana")
        config = grafana.GrafanaConfigDownloader("demo", GRAFANA_CONFIG)
        manager = grafana.GrafanaManager(config)

        for method_name in GRAFANA_MANAGER_MANGLED_METHODS:
            with self.subTest(method_name=method_name):
                replacement = Mock(name=method_name)
                with patch.object(grafana.GrafanaManager, method_name, replacement):
                    self.assertIs(getattr(manager, method_name), replacement)

    def test_manager_globals_remain_stable_across_facades(self):
        grafana = importlib.import_module("grafconflux.grafana")
        confluence = importlib.import_module("grafconflux.confluence")
        orchestration = importlib.import_module("grafconflux.orchestration")
        main = importlib.import_module("main")

        self.assertIs(orchestration.GrafanaManager, grafana.GrafanaManager)
        self.assertIs(main.GrafanaManager, grafana.GrafanaManager)
        self.assertIs(orchestration.ConfluenceManager, confluence.ConfluenceManager)
        self.assertIs(main.ConfluenceManager, confluence.ConfluenceManager)


class TestPrivatePackageDependencyGuard(unittest.TestCase):
    def test_private_package_namespaces_are_importable(self):
        for package_name in PRIVATE_PACKAGE_NAMES:
            with self.subTest(package=package_name):
                module = importlib.import_module(package_name)

                self.assertEqual(package_name, module.__name__)

    def test_private_packages_do_not_import_public_facades(self):
        violations = []
        for source_file in self.private_package_files():
            violations.extend(self.forbidden_imports(source_file))

        self.assertEqual([], violations)

    def test_dependency_guard_detects_forbidden_import_forms(self):
        cases = {
            "import grafconflux.grafana": "grafconflux.grafana",
            "from grafconflux.grafana import GrafanaManager": "grafconflux.grafana",
            "from grafconflux.grafana_models import Panel": "grafconflux.grafana_models",
            "from grafconflux.no_data import NoDataDetectorRegistry": "grafconflux.no_data",
            "from grafconflux.repeating import RepeatingPlanner": "grafconflux.repeating",
            "from grafconflux.repeat_tasks import RepeatTaskBuilder": "grafconflux.repeat_tasks",
            "from grafconflux.snapshots import SnapshotUiRunner": "grafconflux.snapshots",
            "from grafconflux.browser_session import GrafanaBrowserSession": "grafconflux.browser_session",
            "from grafconflux.confluence_content import apply_graphs_placeholder": "grafconflux.confluence_content",
            "from grafconflux.confluence_uploads import _effective_upload_interval": "grafconflux.confluence_uploads",
            "import grafconflux.args_parser": "grafconflux.args_parser",
            "from grafconflux.args_parser import ArgsParser": "grafconflux.args_parser",
            "import grafconflux.config": "grafconflux.config",
            "from grafconflux.config import parse_timestamps": "grafconflux.config",
            "from grafconflux.options import GrafConfluxRunOptions": "grafconflux.options",
            "from grafconflux.yaml_settings import YamlSettings": "grafconflux.yaml_settings",
            "from grafconflux.orchestration import run": "grafconflux.orchestration",
            "from grafconflux import grafana": "grafconflux.grafana",
            "from grafconflux import grafana_models": "grafconflux.grafana_models",
            "from grafconflux import args_parser": "grafconflux.args_parser",
            "from grafconflux import config": "grafconflux.config",
            "from grafconflux import options": "grafconflux.options",
            "from grafconflux import yaml_settings": "grafconflux.yaml_settings",
            "from grafconflux import orchestration": "grafconflux.orchestration",
            "from grafconflux import no_data": "grafconflux.no_data",
            "from grafconflux import snapshots": "grafconflux.snapshots",
            "from grafconflux import browser_session": "grafconflux.browser_session",
            "from grafconflux import confluence_content": "grafconflux.confluence_content",
            "from grafconflux import confluence_uploads": "grafconflux.confluence_uploads",
            "from .. import grafana": "grafconflux.grafana",
            "from .. import grafana_models": "grafconflux.grafana_models",
            "from .. import repeating": "grafconflux.repeating",
            "from .. import snapshots": "grafconflux.snapshots",
            "from .. import browser_session": "grafconflux.browser_session",
            "from .. import confluence_content": "grafconflux.confluence_content",
            "from .. import confluence_uploads": "grafconflux.confluence_uploads",
            "from ..grafana import GrafanaManager": "grafconflux.grafana",
            "from ..grafana_models import Panel": "grafconflux.grafana_models",
            "from ..repeat_tasks import RepeatTaskBuilder": "grafconflux.repeat_tasks",
            "from ..snapshots import SnapshotUiRunner": "grafconflux.snapshots",
            "from ..browser_session import GrafanaBrowserSession": "grafconflux.browser_session",
            "from ..confluence_content import build_confluence_storage_content": "grafconflux.confluence_content",
            "from ..confluence_uploads import _retry_after_seconds": "grafconflux.confluence_uploads",
            "from ..args_parser import ArgsParser": "grafconflux.args_parser",
            "from ..config import parse_timestamps": "grafconflux.config",
            "from ..options import GrafConfluxRunOptions": "grafconflux.options",
            "from ..yaml_settings import YamlSettings": "grafconflux.yaml_settings",
            "from ..orchestration import run": "grafconflux.orchestration",
            "import main": "main",
        }
        for source, expected_module in cases.items():
            with self.subTest(source=source):
                violations = self.forbidden_imports_from_text(source)

                self.assertTrue(violations, "Guard must not pass vacuously")
                self.assertTrue(
                    any(expected_module in violation for violation in violations),
                    f"Expected forbidden import for {expected_module}, got {violations}",
                )

    @staticmethod
    def private_package_files():
        package_dir = Path(__file__).resolve().parents[1] / "src" / "grafconflux"
        for private_dir in package_dir.glob("_*"):
            if private_dir.is_dir():
                yield from private_dir.rglob("*.py")

    def forbidden_imports(self, source_file):
        source = source_file.read_text(encoding="utf-8")
        return self.forbidden_imports_from_text(source, source_file)

    def forbidden_imports_from_text(self, source, source_file=None):
        if source_file is None:
            source_file = self.synthetic_private_source_file()
        tree = ast.parse(source, filename=str(source_file))
        return [
            f"{source_file}: forbidden import {module_name}"
            for module_name in self.imported_modules(tree, source_file)
            if self.is_forbidden_import(module_name)
        ]

    @staticmethod
    def synthetic_private_source_file():
        return Path(__file__).resolve().parents[1] / "src" / "grafconflux" / "_synthetic" / "module.py"

    @staticmethod
    def imported_modules(tree, source_file):
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                yield from (alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                yield from TestPrivatePackageDependencyGuard.import_from_modules(node, source_file)

    @staticmethod
    def import_from_modules(node, source_file):
        module_name = TestPrivatePackageDependencyGuard.resolve_import_from_module(node, source_file)
        if module_name:
            yield module_name
        if TestPrivatePackageDependencyGuard.should_include_imported_names(node, module_name):
            for alias in node.names:
                yield f"{module_name}.{alias.name}" if module_name else alias.name

    @staticmethod
    def resolve_import_from_module(node, source_file):
        if node.level == 0:
            return node.module
        base_parts = TestPrivatePackageDependencyGuard.relative_import_base_parts(node, source_file)
        module_parts = node.module.split(".") if node.module else []
        return ".".join(base_parts + module_parts)

    @staticmethod
    def relative_import_base_parts(node, source_file):
        package_parts = TestPrivatePackageDependencyGuard.source_package_parts(source_file)
        if node.level <= 1:
            return package_parts
        return package_parts[: -(node.level - 1)]

    @staticmethod
    def source_package_parts(source_file):
        package_dir = Path(__file__).resolve().parents[1] / "src"
        module_parts = source_file.relative_to(package_dir).with_suffix("").parts
        if module_parts[-1] == "__init__":
            return list(module_parts[:-1])
        return list(module_parts[:-1])

    @staticmethod
    def should_include_imported_names(node, module_name):
        return node.module is None or module_name == "grafconflux"

    @staticmethod
    def is_forbidden_import(module_name):
        return any(
            module_name == forbidden or module_name.startswith(f"{forbidden}.")
            for forbidden in FORBIDDEN_INTERNAL_IMPORTS
        )


GRAFANA_CONFIG = {"dash_title": "Dashboard", "host": "https://grafana.example"}

GRAFANA_MODEL_SYMBOLS = [
    "ConfigurationError",
    "_SelectorConfig",
    "DEFAULT_INTERVAL_MS",
    "DEFAULT_MAX_DATA_POINTS",
    "DEFAULT_NO_DATA_TIMEOUT",
    "DOWNLOAD_COLLAPSE_PANELS_KEY",
    "DOWNLOAD_COLLAPSED_ROWS_KEY",
    "DOWNLOAD_HIDDEN_PANELS_KEY",
    "DashboardLookupRequest",
    "DashboardLookupResult",
    "GrafanaConfigBase",
    "GrafanaConfigDownloader",
    "GrafanaConfigUploader",
    "INCLUDE_ALL_EXCEPT_EXCLUDED",
    "INCLUDE_ONLY_SELECTED",
    "NO_DATA_MODE_CONSERVATIVE",
    "NO_DATA_ON_ERROR_RENDER",
    "NO_DATA_PREFLIGHT_KEY",
    "NoDataDatasourceInference",
    "NoDataPreflightConfig",
    "NoDataPreflightResult",
    "PANEL_FILTERING_KEY",
    "PANEL_FILTERING_MODES",
    "Panel",
    "PanelDefinition",
    "PanelDescriptor",
    "PanelFilteringConfig",
    "PanelRenderTask",
    "SKIP_REASON_EMPTY_FRAMES",
    "SNAPSHOT_HYDRATION_DWELL_SECONDS",
    "SNAPSHOT_HYDRATION_FINAL_DWELL_SECONDS",
    "SNAPSHOT_HYDRATION_MIN_STEP_PX",
    "SNAPSHOT_HYDRATION_SCROLL_LIMIT",
    "SNAPSHOT_HYDRATION_STEP_VIEWPORT_FRACTION",
    "SNAPSHOT_LOADER_WAIT_INTERVAL_SECONDS",
    "SNAPSHOT_LOADER_WAIT_LIMIT",
    "SNAPSHOT_MODE_AUTO",
    "SNAPSHOT_MODE_LEGACY_API",
    "SNAPSHOT_MODE_UI",
    "SNAPSHOT_MODES",
    "SNAPSHOT_PAGE_DOWN_DWELL_SECONDS",
    "SNAPSHOT_ROW_EXPAND_LIMIT",
    "SNAPSHOT_ROW_SETTLE_SECONDS",
    "SNAPSHOT_ROW_SWEEP_LIMIT",
    "SNAPSHOT_ROW_SWEEP_STEP_LIMIT",
    "SNAPSHOT_ROW_SWEEP_STEP_VIEWPORT_FRACTION",
    "SNAPSHOT_STABLE_SCROLL_LIMIT",
    "SUPPORTED_PHASE1_DATASOURCES",
]

GRAFANA_SHARED_MODEL_SYMBOLS = GRAFANA_MODEL_SYMBOLS + [
    "ALL_REPEAT_SENTINELS",
    "COLLECT_NO_DATA_PANELS_KEY",
    "ENABLE_REPEATING_PANELS_KEY",
    "PROMETHEUS_DATASOURCE_TYPE",
    "REPEATING_PANELS_KEY",
    "REPEAT_VALUES_KEY",
    "REPEAT_VALUE_MODES",
    "SNAPSHOT_DELETE_FIELDS",
]

DASHBOARD_LOOKUP_SYMBOLS = ["log_lookup_mode", "search_params", "select_dashboard"]
PANEL_SELECTION_SYMBOLS = [
    "apply_disabled_graph_type_filter",
    "extract_dashboard_panels",
    "filter_panel_descriptors",
    "filter_runtime_repeat_clones",
    "panel_from_descriptor",
    "warn_unmatched_filter_selectors",
]
NO_DATA_SYMBOLS = [
    "NoDataDetectorRegistry",
    "NoDataPreflightRunner",
    "_GenericNoDataDetector",
    "_datasource_type_uid",
    "_has_ambiguous_datasource_refs",
    "_has_no_data_unsupported_panel_features",
    "_interpret_fields",
    "_interpret_frame",
    "_interpret_frames",
    "_interpret_ref_result",
    "_interpret_values",
    "_preflight_targets",
    "_scoped_vars",
    "_target_datasource_ref",
    "_value_has_data",
    "infer_no_data_datasource",
    "interpret_no_data_response",
]
GRAFANA_RENDERING_SYMBOLS = [
    "build_dashboard_url_params",
    "build_panel_url_params",
    "build_render_api_params",
    "build_render_api_url",
    "build_render_file_path",
]
REPEATING_SYMBOLS = ["RepeatingPlanner", "is_unresolved_repeating_rule"]
REPEATING_FACADE_SYMBOLS = REPEATING_SYMBOLS + ["_datasource_type_uid"]
REPEAT_TASK_SYMBOLS = ["RepeatTaskBuilder"]
SNAPSHOT_SYMBOLS = [
    "SnapshotUiRunner",
    "is_snapshot_post_request",
    "normalize_snapshot_url",
    "snapshot_api_url",
    "snapshot_backup_file",
    "snapshot_key_from_url",
    "snapshot_name",
    "snapshot_response_payload",
    "snapshot_response_text",
    "snapshot_url_from_key",
    "snapshot_url_from_lookup_response",
    "snapshot_url_from_payload",
    "without_delete_fields",
    "write_json_file",
]
BROWSER_SESSION_SYMBOLS = ["GrafanaBrowserSession"]
GRAFANA_HELPER_SYMBOLS = (
    DASHBOARD_LOOKUP_SYMBOLS
    + PANEL_SELECTION_SYMBOLS
    + NO_DATA_SYMBOLS
    + GRAFANA_RENDERING_SYMBOLS
    + REPEATING_SYMBOLS
    + SNAPSHOT_SYMBOLS
    + BROWSER_SESSION_SYMBOLS
)

CONFLUENCE_CONTENT_SYMBOLS = [
    "GRAPHS_PLACEHOLDER",
    "apply_graphs_placeholder",
    "build_confluence_storage_content",
]
CONFLUENCE_CONTENT_FACADE_SYMBOLS = CONFLUENCE_CONTENT_SYMBOLS + [
    "_artifact_has_rendered_png",
    "_artifact_title",
    "_dashboard_period",
    "_first_panel_link",
    "_non_repeating_artifact_title",
    "_panel_period",
    "_render_dashboard_links",
    "_render_dashboards_section",
    "_render_panel_artifacts",
    "_render_panel_timestamps",
    "_render_panels",
    "_render_snapshot_backup_section",
    "_render_test_times_section",
]
CONFLUENCE_UPLOAD_SYMBOLS = [
    "RETRY_AFTER_HEADER",
    "RETRYABLE_STATUS_CODES",
    "_ConfluenceUploadRateLimiter",
    "_effective_upload_interval",
    "_extract_status_code",
    "_retry_after_seconds",
]
CONFLUENCE_UPLOAD_FACADE_SYMBOLS = CONFLUENCE_UPLOAD_SYMBOLS + [
    "_coerce_status_code",
    "_direct_header_value",
    "_header_value",
    "_is_retryable_upload_error",
    "_iterated_header_value",
    "_parse_retry_after",
    "_parse_retry_after_date",
    "_retry_after_header_value",
    "_status_code_from_source",
]
CONFLUENCE_SYMBOLS = (
    ["ConfluenceManager", "DEFAULT_CONTENT_TYPE"]
    + CONFLUENCE_CONTENT_SYMBOLS
    + CONFLUENCE_UPLOAD_SYMBOLS
)

CONFIG_FACADE_SYMBOLS = {
    "grafconflux.args_parser": ["ArgsParser", "GrafanaTimeBase", "GrafanaTimeDownloader", "GrafanaTimeUploader"],
    "grafconflux.config": ["options_from_config_file", "parse_timestamps", "run_from_config_file"],
    "grafconflux.yaml_settings": [
        "DEFAULT_CONFLUENCE_UPLOAD_THREADS",
        "DEFAULT_GRAPH_WIDTH",
        "DEFAULT_THREADS",
        "YamlSettings",
        "ignore_verify_ssl_or_current",
        "setting_or_current",
        "verify_ssl_or_current",
        "wiki_url_or_current",
        "yaml_settings_from_config",
    ],
    "grafconflux.options": ["GrafConfluxRunOptions"],
    "grafconflux.orchestration": ["_create_confluence_manager"] + [
        "RunArgs",
        "get_yaml_files",
        "process_grafana_dashboard",
        "run",
        "transform_grafana_configs",
        "upload_already_downloaded_graphs",
    ],
}
CONFIG_PRIVATE_HELPER_SYMBOLS = ["options_from_config_file", "parse_timestamps"]
YAML_SETTINGS_SYMBOLS = [
    "DEFAULT_CONFLUENCE_UPLOAD_THREADS",
    "DEFAULT_GRAPH_WIDTH",
    "DEFAULT_THREADS",
    "YamlSettings",
    "ignore_verify_ssl_or_current",
    "setting_or_current",
    "verify_ssl_or_current",
    "wiki_url_or_current",
    "yaml_settings_from_config",
]

ORCHESTRATION_SYMBOLS = [
    "RunArgs",
    "get_yaml_files",
    "process_grafana_dashboard",
    "run",
    "transform_grafana_configs",
    "upload_already_downloaded_graphs",
]
ORCHESTRATION_FACADE_SYMBOLS = ["_create_confluence_manager"] + ORCHESTRATION_SYMBOLS
ORCHESTRATION_RUNNER_SYMBOLS = [
    "RunArgs",
    "_build_test_folder",
    "_create_confluence_manager",
    "_load_upload_configs",
    "_raise_failed_futures",
    "get_yaml_files",
    "process_grafana_dashboard",
    "run",
    "upload_already_downloaded_graphs",
]
ORCHESTRATION_UPLOAD_MERGE_SYMBOLS = [
    "_UploadMergeState",
    "_copy_snapshot_backups",
    "_copy_upload_graph_files",
    "_merge_upload_config",
    "_merge_upload_panel",
    "_merge_upload_panel_data",
    "_shift_artifact",
    "_shift_artifacts",
    "_shift_png_file_name",
    "_upload_config_matches_folder",
    "_upload_match_key",
    "_write_merged_upload_config",
    "_write_merged_upload_configs",
    "transform_grafana_configs",
]
MAIN_EXPORT_SYMBOLS = [
    "ArgsParser",
    "ConfluenceManager",
    "GrafanaConfigDownloader",
    "GrafanaConfigUploader",
    "GrafanaManager",
    "RunArgs",
    "get_yaml_files",
    "logger",
    "main",
    "process_grafana_dashboard",
    "run",
    "run_cli",
    "transform_grafana_configs",
    "upload_already_downloaded_graphs",
]
GRAFANA_MANAGER_MANGLED_METHODS = [
    "_GrafanaManager__build_panel_url",
    "_GrafanaManager__download_chart",
    "_GrafanaManager__get_full_links",
    "_GrafanaManager__get_panel_data_sources",
    "_GrafanaManager__save_params_to_file",
    "_GrafanaManager__take_screenshot",
]
FORBIDDEN_INTERNAL_IMPORTS = [
    "grafconflux.args_parser",
    "grafconflux.browser_session",
    "grafconflux.config",
    "grafconflux.confluence",
    "grafconflux.confluence_content",
    "grafconflux.confluence_uploads",
    "grafconflux.dashboard_lookup",
    "grafconflux.grafana",
    "grafconflux.grafana_models",
    "grafconflux.grafana_rendering",
    "grafconflux.no_data",
    "grafconflux.options",
    "grafconflux.orchestration",
    "grafconflux.panel_selection",
    "grafconflux.repeating",
    "grafconflux.repeat_tasks",
    "grafconflux.snapshots",
    "grafconflux.yaml_settings",
    "main",
]
PRIVATE_PACKAGE_NAMES = [
    "grafconflux._shared",
    "grafconflux._grafana",
    "grafconflux._confluence",
    "grafconflux._config",
    "grafconflux._orchestration",
]
