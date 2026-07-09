import os
from typing import Any, Optional, TypeVar

from grafconflux._shared.confluence_settings import (
    ConfluenceRenderingSettings,
    confluence_rendering_settings_from_config,
)


DEFAULT_GRAPH_WIDTH = 1500
DEFAULT_THREADS = 4
DEFAULT_CONFLUENCE_UPLOAD_THREADS = 1
DEFAULT_UPLOAD_DELAY = 0
DEFAULT_UPLOAD_RATE_PER_SECOND = None
DEFAULT_RETRY = True
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_DELAY = 5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 1.0
DEFAULT_RETRY_MAX_DELAY = None
DEFAULT_RETRY_JITTER = 0
DEFAULT_CONTINUE_ON_ERROR = False
DEFAULT_IGNORE_VERIFY_SSL = False
DEFAULT_PLAYWRIGHT_BROWSER = 'chromium'
DEFAULT_CHILD_TITLE_PREFIX = 'GrafConflux: '

YAML_SETTING_NAMES = (
    "confluence_login",
    "confluence_password",
    "confluence_token",
    "confluence_upload_threads",
    "confluence_upload_delay",
    "confluence_upload_rate_per_second",
    "confluence_retry",
    "confluence_retry_count",
    "confluence_retry_delay",
    "confluence_retry_backoff_multiplier",
    "confluence_retry_max_delay",
    "confluence_retry_jitter",
    "confluence_continue_on_error",
    "wiki_url",
    "confluence_verify_ssl",
    "confluence_ignore_verify_ssl",
    "graph_width",
    "threads",
    "playwright_browser",
    "playwright_browser_channel",
    "playwright_browser_executable_path",
    "confluence_child_title_prefix",
    "confluence_child_title_from_test_id",
)

YAML_DEFAULT_SETTING_DEFAULTS: tuple[tuple[str, Any], ...] = (
    ("confluence_upload_threads", DEFAULT_CONFLUENCE_UPLOAD_THREADS),
    ("confluence_upload_delay", DEFAULT_UPLOAD_DELAY),
    ("confluence_upload_rate_per_second", DEFAULT_UPLOAD_RATE_PER_SECOND),
    ("confluence_retry", DEFAULT_RETRY),
    ("confluence_retry_count", DEFAULT_RETRY_COUNT),
    ("confluence_retry_delay", DEFAULT_RETRY_DELAY),
    ("confluence_retry_backoff_multiplier", DEFAULT_RETRY_BACKOFF_MULTIPLIER),
    ("confluence_retry_max_delay", DEFAULT_RETRY_MAX_DELAY),
    ("confluence_retry_jitter", DEFAULT_RETRY_JITTER),
    ("confluence_continue_on_error", DEFAULT_CONTINUE_ON_ERROR),
    ("graph_width", DEFAULT_GRAPH_WIDTH),
    ("threads", DEFAULT_THREADS),
    ("playwright_browser", None),
    ("playwright_browser_channel", None),
    ("playwright_browser_executable_path", None),
    ("confluence_child_title_prefix", DEFAULT_CHILD_TITLE_PREFIX),
    ("confluence_child_title_from_test_id", False),
)

T = TypeVar("T")
AUTH_SETTING_NAMES = {"confluence_login", "confluence_password", "confluence_token"}
ENV_REFERENCE_PREFIX = "env:"


class YamlSettings:
    confluence_login: Optional[str] = None
    confluence_password: Optional[str] = None
    confluence_token: Optional[str] = None
    confluence_upload_threads: Optional[int] = None
    confluence_upload_delay: Optional[float] = None
    confluence_upload_rate_per_second: Optional[float] = None
    confluence_retry: Optional[bool] = None
    confluence_retry_count: Optional[int] = None
    confluence_retry_delay: Optional[float] = None
    confluence_retry_backoff_multiplier: Optional[float] = None
    confluence_retry_max_delay: Optional[float] = None
    confluence_retry_jitter: Optional[float] = None
    confluence_continue_on_error: Optional[bool] = None
    wiki_url: Optional[str] = None
    confluence_verify_ssl: Optional[bool] = None
    confluence_ignore_verify_ssl: Optional[bool] = None
    graph_width: Optional[int] = None
    threads: Optional[int] = None
    playwright_browser: Optional[str] = None
    playwright_browser_channel: Optional[str] = None
    playwright_browser_executable_path: Optional[str] = None
    confluence_child_title_prefix: Optional[str] = None
    confluence_child_title_from_test_id: Optional[bool] = None
    confluence_rendering: ConfluenceRenderingSettings = ConfluenceRenderingSettings()


def yaml_settings_from_config(config_data: dict) -> YamlSettings:
    _validate_new_yaml_settings(config_data)
    settings = YamlSettings()
    settings_data = config_data.get("settings", {})
    for setting_name in YAML_SETTING_NAMES:
        if setting_name in settings_data:
            value = _yaml_setting_value(setting_name, settings_data[setting_name])
            setattr(settings, setting_name, value)
    settings.confluence_rendering = confluence_rendering_settings_from_config(config_data)
    return settings


def _validate_new_yaml_settings(config_data: dict) -> None:
    if not isinstance(config_data, dict):
        raise ValueError("YAML config must be a mapping with top-level 'dashboards' and optional 'settings'.")
    if _looks_like_legacy_dashboard_config(config_data):
        raise ValueError(
            "Legacy top-level dashboard YAML format is not supported; "
            "move dashboard entries under top-level 'dashboards'."
        )
    dashboards = config_data.get("dashboards")
    if not isinstance(dashboards, dict) or not dashboards:
        raise ValueError("YAML config must contain a non-empty top-level 'dashboards' mapping.")
    if "settings" in config_data and not isinstance(config_data["settings"], dict):
        raise ValueError("YAML top-level 'settings' must be a mapping.")


def _looks_like_legacy_dashboard_config(config_data: dict[str, Any]) -> bool:
    return "dashboards" not in config_data and any(
        key != "settings" and isinstance(value, dict)
        for key, value in config_data.items()
    )


def _yaml_setting_value(setting_name: str, value: Any) -> Any:
    if setting_name in AUTH_SETTING_NAMES:
        return _resolve_auth_setting(setting_name, value)
    return value


def _resolve_auth_setting(setting_name: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"YAML setting '{setting_name}' must be a string or env:VAR reference.")
    if not value.startswith(ENV_REFERENCE_PREFIX):
        return value
    return _env_value_from_reference(setting_name, value)


def _env_value_from_reference(setting_name: str, value: str) -> str:
    env_name = value[len(ENV_REFERENCE_PREFIX):]
    if env_name == "":
        raise ValueError(f"YAML setting '{setting_name}' has an empty env variable reference.")
    if env_name not in os.environ:
        raise ValueError(
            f"YAML setting '{setting_name}' references missing environment variable '{env_name}'."
        )
    return os.environ[env_name]


def wiki_url_or_current(settings: YamlSettings, current_value: str | None) -> str | None:
    if settings.wiki_url is not None:
        return settings.wiki_url
    return current_value


def setting_or_current(setting_value: T | None, current_value: T, default_value: T) -> T:
    if setting_value is not None and current_value == default_value:
        return setting_value
    return current_value


def _yaml_default_setting_values(settings: YamlSettings, values: dict[str, Any]) -> dict[str, Any]:
    return {
        setting_name: setting_or_current(
            getattr(settings, setting_name),
            values[setting_name],
            default_value,
        )
        for setting_name, default_value in YAML_DEFAULT_SETTING_DEFAULTS
    }


def ignore_verify_ssl_or_current(settings: YamlSettings, current_value: bool) -> bool:
    if settings.confluence_verify_ssl is not None and current_value is False:
        return not settings.confluence_verify_ssl
    return setting_or_current(
        settings.confluence_ignore_verify_ssl,
        current_value,
        DEFAULT_IGNORE_VERIFY_SSL,
    )


def verify_ssl_or_current(settings: YamlSettings, current_value: bool | None) -> bool:
    if current_value is not None:
        return current_value
    if settings.confluence_verify_ssl is not None:
        return settings.confluence_verify_ssl
    if settings.confluence_ignore_verify_ssl is not None:
        return not settings.confluence_ignore_verify_ssl
    return True
