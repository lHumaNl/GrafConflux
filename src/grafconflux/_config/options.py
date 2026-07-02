"""Library-facing run options for GrafConflux."""

from __future__ import annotations

from dataclasses import dataclass, field

from grafconflux._shared.time import GrafanaTimeDownloader


@dataclass
class GrafConfluxRunOptions:
    """ArgsParser-shaped options accepted by the library run API."""

    wiki_url: str | None = None
    confluence_page_id: int | None = None
    confluence_parent_page_id: int | None = None
    confluence_child_title: str | None = None
    confluence_child_title_prefix: str = "GrafConflux: "
    confluence_child_title_from_test_id: bool = False
    confluence_login: str | None = None
    confluence_password: str | None = None
    confluence_token: str | None = None
    timestamps: list[GrafanaTimeDownloader] = field(default_factory=list)
    config_file: str = "config.yaml"
    test_root_folder: str = "graphs"
    test_upload_folders: list[str] | None = None
    graph_width: int = 1500
    test_id: str = "-1"
    threads: int = 4
    only_graphs: bool = False
    tz: str = "UTC"
    confluence_verify_ssl: bool = True
    confluence_upload_threads: int = 1
    confluence_upload_delay: float = 0
    confluence_upload_rate_per_second: float | None = None
    confluence_retry: bool = True
    confluence_retry_count: int = 3
    confluence_retry_delay: float = 5
    confluence_retry_backoff_multiplier: float = 1.0
    confluence_retry_max_delay: float | None = None
    confluence_retry_jitter: float = 0
    confluence_continue_on_error: bool = False
    playwright_browser: str | None = None
    playwright_browser_channel: str | None = None
    playwright_browser_executable_path: str | None = None
