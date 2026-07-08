"""Compatibility facade for library configuration helpers."""

from grafconflux._config.file_options import (
    _build_options,
    _ensure_config_file_exists,
    _is_negative_optional,
    _is_non_positive_optional,
    _optional_list,
    _validate_options,
    _value_or_env,
    options_from_config_file,
    parse_timestamps,
)
from grafconflux._config.options import GrafConfluxRunOptions
from grafconflux._config.yaml_settings import (
    DEFAULT_CONFLUENCE_UPLOAD_THREADS,
    DEFAULT_CONTINUE_ON_ERROR,
    DEFAULT_GRAPH_WIDTH,
    DEFAULT_IGNORE_VERIFY_SSL,
    DEFAULT_RETRY,
    DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETRY_JITTER,
    DEFAULT_RETRY_MAX_DELAY,
    DEFAULT_THREADS,
    DEFAULT_UPLOAD_DELAY,
    DEFAULT_UPLOAD_RATE_PER_SECOND,
    YAML_SETTING_NAMES,
    YamlSettings,
    ignore_verify_ssl_or_current,
    setting_or_current,
    verify_ssl_or_current,
    wiki_url_or_current,
    yaml_settings_from_config,
)
from grafconflux._shared.time import GrafanaTimeDownloader


def run_from_config_file(config_file="config.yaml", **kwargs):
    """Run GrafConflux from a YAML config and library-provided options."""
    from grafconflux.orchestration import run as orchestration_run

    return orchestration_run(options_from_config_file(config_file, **kwargs))

__all__ = [
    "DEFAULT_CONFLUENCE_UPLOAD_THREADS",
    "DEFAULT_CONTINUE_ON_ERROR",
    "DEFAULT_GRAPH_WIDTH",
    "DEFAULT_IGNORE_VERIFY_SSL",
    "DEFAULT_RETRY",
    "DEFAULT_RETRY_BACKOFF_MULTIPLIER",
    "DEFAULT_RETRY_COUNT",
    "DEFAULT_RETRY_DELAY",
    "DEFAULT_RETRY_JITTER",
    "DEFAULT_RETRY_MAX_DELAY",
    "DEFAULT_THREADS",
    "DEFAULT_UPLOAD_DELAY",
    "DEFAULT_UPLOAD_RATE_PER_SECOND",
    "GrafConfluxRunOptions",
    "GrafanaTimeDownloader",
    "YAML_SETTING_NAMES",
    "YamlSettings",
    "_build_options",
    "_ensure_config_file_exists",
    "_is_negative_optional",
    "_is_non_positive_optional",
    "_optional_list",
    "_validate_options",
    "_value_or_env",
    "ignore_verify_ssl_or_current",
    "options_from_config_file",
    "parse_timestamps",
    "run_from_config_file",
    "setting_or_current",
    "verify_ssl_or_current",
    "wiki_url_or_current",
    "yaml_settings_from_config",
]
