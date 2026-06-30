"""Operational runner internals for GrafConflux."""

from __future__ import annotations

import datetime
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import List, Protocol

import urllib3
import yaml

from grafconflux._orchestration.upload_merge import transform_grafana_configs
from grafconflux._shared.grafana_models import GrafanaConfigDownloader, GrafanaConfigUploader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("grafconflux.orchestration")
urllib3.disable_warnings()


class RunArgs(Protocol):
    """ArgsParser-shaped object consumed by the operational runner."""

    wiki_url: str
    config_file: str
    confluence_verify_ssl: bool
    confluence_login: str | None
    confluence_password: str | None
    confluence_page_id: int | None
    test_root_folder: str
    test_upload_folders: List[str] | None
    graph_width: int
    test_id: str
    threads: int
    only_graphs: bool
    confluence_upload_threads: int
    confluence_upload_delay: float
    confluence_upload_rate_per_second: float | None
    confluence_retry: bool
    confluence_retry_count: int
    confluence_retry_delay: float
    confluence_retry_backoff_multiplier: float
    confluence_retry_max_delay: float | None
    confluence_retry_jitter: float
    confluence_continue_on_error: bool
    timestamps: List[object]


def run(args: RunArgs, confluence_manager_class, grafana_manager_class, process_dashboard) -> None:
    if args.test_upload_folders:
        upload_already_downloaded_graphs(args, confluence_manager_class)
        return

    test_folder = _build_test_folder(args)

    # Load Grafana configurations
    grafana_configs = grafana_manager_class.load_grafana_config(args.config_file)

    # Initialize Confluence manager
    confluence_manager = _create_confluence_manager(args, confluence_manager_class)

    # Process each Grafana config
    executor = ThreadPoolExecutor(max_workers=args.threads)
    futures = []

    for grafana_config in grafana_configs:
        futures.append(
            executor.submit(process_dashboard, grafana_config, test_folder, args, confluence_manager)
        )

    wait(futures)
    executor.shutdown()
    _raise_failed_futures(futures)

    # Update Confluence page content
    if not args.only_graphs:
        confluence_manager.upload_charts(test_folder, [['.json', 'application/json']])
        confluence_manager.update_page_content(grafana_configs, args.timestamps, args.graph_width, test_folder)


def upload_already_downloaded_graphs(args: RunArgs, confluence_manager_class):
    confluence_manager = _create_confluence_manager(args, confluence_manager_class)
    grafana_configs = _load_upload_configs(args.test_upload_folders)

    if len(args.test_upload_folders) > 1:
        grafana_configs, folder_graphs = transform_grafana_configs(grafana_configs, args)
    else:
        folder_graphs = args.test_upload_folders[0]

    for grafana_config in grafana_configs:
        confluence_manager.upload_charts(grafana_config.charts_path)

    confluence_manager.upload_charts(folder_graphs, [['.json', 'application/json']])
    confluence_manager.update_page_content(grafana_configs, grafana_configs[0].timestamps, args.graph_width, folder_graphs)


def _create_confluence_manager(args: RunArgs, confluence_manager_class):
    return confluence_manager_class(
        login=args.confluence_login,
        password=args.confluence_password,
        page_id=args.confluence_page_id,
        upload_threads=args.confluence_upload_threads,
        wiki_url=args.wiki_url,
        verify_ssl=args.confluence_verify_ssl,
        upload_delay=args.confluence_upload_delay,
        upload_rate_per_second=getattr(args, 'confluence_upload_rate_per_second', None),
        retry_enabled=args.confluence_retry,
        retry_count=args.confluence_retry_count,
        retry_delay=args.confluence_retry_delay,
        retry_backoff_multiplier=getattr(args, 'confluence_retry_backoff_multiplier', 1.0),
        retry_max_delay=getattr(args, 'confluence_retry_max_delay', None),
        retry_jitter=getattr(args, 'confluence_retry_jitter', 0),
        continue_on_error=args.confluence_continue_on_error,
    )


def _build_test_folder(args: RunArgs) -> str:
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return os.path.join(args.test_root_folder, f'{args.test_id}__{current_time}')


def _load_upload_configs(upload_folders: List[str]) -> List[GrafanaConfigUploader]:
    grafana_configs = []
    for folder in upload_folders:
        for file in get_yaml_files(folder):
            with open(file, 'r', encoding='utf-8') as yaml_file:
                config = yaml.safe_load(yaml_file)

            grafana_configs.append(GrafanaConfigUploader(config['name'], config))
    return grafana_configs


def get_yaml_files(directory):
    yaml_files = []

    for entry in os.listdir(directory):
        full_path = os.path.join(directory, entry)

        if os.path.isfile(full_path) and entry.endswith('.yaml'):
            yaml_files.append(full_path)

    return yaml_files


def process_grafana_dashboard(
    grafana_config: GrafanaConfigDownloader,
    test_folder: str,
    args: RunArgs,
    confluence_manager,
    grafana_manager_class,
):
    """
    Process a single Grafana dashboard: authenticate, download charts, and upload to Confluence.
    """
    try:
        grafana_manager = grafana_manager_class(config=grafana_config)
        grafana_manager.authenticate(args.confluence_login, args.confluence_password)

        # Download charts
        grafana_manager.download_charts(
            test_folder=test_folder,
            timestamps=args.timestamps,
        )

        # Upload to Confluence
        if not args.only_graphs:
            confluence_manager.upload_charts(
                grafana_manager.charts_path,
            )
    except Exception as e:
        logger.error(f'Failed to process dashboard {grafana_config.dash_title}: {e}')
        if not getattr(args, 'confluence_continue_on_error', True):
            raise


def _raise_failed_futures(futures: List[Future]) -> None:
    for future in futures:
        future.result()


__all__ = [
    "RunArgs",
    "_build_test_folder",
    "_create_confluence_manager",
    "_load_upload_configs",
    "_raise_failed_futures",
    "get_yaml_files",
    "logger",
    "process_grafana_dashboard",
    "run",
    "upload_already_downloaded_graphs",
]
