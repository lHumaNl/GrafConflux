from typing import Any, Optional, TypeVar


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

YAML_SETTING_NAMES = (
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
    "confluence_ignore_verify_ssl",
    "graph_width",
    "threads",
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
)

T = TypeVar("T")


class YamlSettings:
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
    confluence_ignore_verify_ssl: Optional[bool] = None
    graph_width: Optional[int] = None
    threads: Optional[int] = None


def yaml_settings_from_config(config_data: dict) -> YamlSettings:
    settings = YamlSettings()
    if "settings" in config_data:
        settings_data = config_data["settings"]
        for setting_name in YAML_SETTING_NAMES:
            if setting_name in settings_data:
                setattr(settings, setting_name, settings_data[setting_name])
    return settings


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
    return setting_or_current(
        settings.confluence_ignore_verify_ssl,
        current_value,
        DEFAULT_IGNORE_VERIFY_SSL,
    )


def verify_ssl_or_current(settings: YamlSettings, current_value: bool) -> bool:
    if settings.confluence_ignore_verify_ssl is not None and current_value is True:
        return not settings.confluence_ignore_verify_ssl
    return current_value
