"""Configuration helpers for the GrafConflux library API."""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

from grafconflux._config.args_parser import ArgsParser
from grafconflux._config.options import GrafConfluxRunOptions
from grafconflux._config.yaml_settings import (
    DEFAULT_CONFLUENCE_UPLOAD_THREADS,
    DEFAULT_CONTINUE_ON_ERROR,
    DEFAULT_GRAPH_WIDTH,
    DEFAULT_RETRY,
    DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETRY_JITTER,
    DEFAULT_RETRY_MAX_DELAY,
    DEFAULT_THREADS,
    DEFAULT_UPLOAD_DELAY,
    DEFAULT_UPLOAD_RATE_PER_SECOND,
    YamlSettings,
    _yaml_default_setting_values,
    verify_ssl_or_current,
    wiki_url_or_current,
)
from grafconflux._shared.time import GrafanaTimeDownloader


def parse_timestamps(timestamp_strings: Iterable[str], tz: str = "UTC") -> list[GrafanaTimeDownloader]:
    """Parse Grafana timestamp range strings using existing CLI semantics."""
    return [
        GrafanaTimeDownloader(timestamp_str, id_time, tz)
        for id_time, timestamp_str in enumerate(timestamp_strings)
    ]


def options_from_config_file(
    config_file: str | os.PathLike[str] = "config.yaml",
    *,
    wiki_url: str | None = None,
    confluence_page_id: int | None = None,
    confluence_login: str | None = None,
    confluence_password: str | None = None,
    timestamps: Iterable[str] | None = None,
    test_root_folder: str = "graphs",
    test_upload_folders: Iterable[str] | None = None,
    graph_width: int = DEFAULT_GRAPH_WIDTH,
    test_id: str = "-1",
    threads: int = DEFAULT_THREADS,
    only_graphs: bool = False,
    tz: str = "UTC",
    confluence_verify_ssl: bool = True,
    confluence_upload_threads: int = DEFAULT_CONFLUENCE_UPLOAD_THREADS,
    confluence_upload_delay: float = DEFAULT_UPLOAD_DELAY,
    confluence_upload_rate_per_second: float | None = DEFAULT_UPLOAD_RATE_PER_SECOND,
    confluence_retry: bool = DEFAULT_RETRY,
    confluence_retry_count: int = DEFAULT_RETRY_COUNT,
    confluence_retry_delay: float = DEFAULT_RETRY_DELAY,
    confluence_retry_backoff_multiplier: float = DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    confluence_retry_max_delay: float | None = DEFAULT_RETRY_MAX_DELAY,
    confluence_retry_jitter: float = DEFAULT_RETRY_JITTER,
    confluence_continue_on_error: bool = DEFAULT_CONTINUE_ON_ERROR,
) -> GrafConfluxRunOptions:
    """Build library run options from a YAML config without parsing CLI args."""
    config_path = os.fspath(config_file)
    _ensure_config_file_exists(config_path)
    settings = ArgsParser._load_yaml_settings(config_path)
    options = _build_options(config_path, settings, locals())
    _validate_options(options)
    return options


def _build_options(
    config_file: str,
    settings: YamlSettings,
    values: dict[str, Any],
) -> GrafConfluxRunOptions:
    default_values = _yaml_default_setting_values(settings, values)
    return GrafConfluxRunOptions(
        wiki_url=wiki_url_or_current(settings, values["wiki_url"]),
        confluence_page_id=values["confluence_page_id"],
        confluence_login=_value_or_env(values["confluence_login"], "CONFLUENCE_LOGIN"),
        confluence_password=_value_or_env(values["confluence_password"], "CONFLUENCE_PASSWORD"),
        timestamps=parse_timestamps(values["timestamps"] or [], values["tz"]),
        config_file=config_file,
        test_root_folder=values["test_root_folder"],
        test_upload_folders=_optional_list(values["test_upload_folders"]),
        graph_width=default_values["graph_width"],
        test_id=values["test_id"],
        threads=default_values["threads"],
        only_graphs=values["only_graphs"],
        tz=values["tz"],
        confluence_verify_ssl=verify_ssl_or_current(settings, values["confluence_verify_ssl"]),
        confluence_upload_threads=default_values["confluence_upload_threads"],
        confluence_upload_delay=default_values["confluence_upload_delay"],
        confluence_upload_rate_per_second=default_values["confluence_upload_rate_per_second"],
        confluence_retry=default_values["confluence_retry"],
        confluence_retry_count=default_values["confluence_retry_count"],
        confluence_retry_delay=default_values["confluence_retry_delay"],
        confluence_retry_backoff_multiplier=default_values["confluence_retry_backoff_multiplier"],
        confluence_retry_max_delay=default_values["confluence_retry_max_delay"],
        confluence_retry_jitter=default_values["confluence_retry_jitter"],
        confluence_continue_on_error=default_values["confluence_continue_on_error"],
    )


def _value_or_env(value: str | None, env_name: str) -> str | None:
    if value is not None:
        return value
    return os.getenv(env_name, None)


def _optional_list(values: Iterable[str] | None) -> list[str] | None:
    if values is None:
        return None
    return list(values)


def _ensure_config_file_exists(config_file: str) -> None:
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"Configuration file {config_file} not found.")


def _validate_options(options: GrafConfluxRunOptions) -> None:
    if options.confluence_page_id is None:
        raise ValueError('Library arg "confluence_page_id" is NULL')
    if options.confluence_login is None or options.confluence_login == "":
        raise ValueError('CLI arg "--confluence_login" is NULL')
    if options.confluence_password is None or options.confluence_password == "":
        raise ValueError('CLI arg "--confluence_password" is NULL')
    if options.confluence_upload_threads < 1:
        raise ValueError('Library arg "confluence_upload_threads" must be greater than 0')
    if _is_non_positive_optional(options.confluence_upload_rate_per_second):
        raise ValueError('Library arg "confluence_upload_rate_per_second" must be greater than 0 when set')
    if options.confluence_retry_backoff_multiplier < 1:
        raise ValueError('Library arg "confluence_retry_backoff_multiplier" must be greater than or equal to 1')
    if _is_negative_optional(options.confluence_retry_max_delay):
        raise ValueError('Library arg "confluence_retry_max_delay" must be greater than or equal to 0')
    if options.confluence_retry_jitter < 0:
        raise ValueError('Library arg "confluence_retry_jitter" must be greater than or equal to 0')
    if not options.timestamps and not options.test_upload_folders:
        raise ValueError("At least one timestamp must be provided.")


def _is_negative_optional(value: float | None) -> bool:
    return value is not None and value < 0


def _is_non_positive_optional(value: float | None) -> bool:
    return value is not None and value <= 0
