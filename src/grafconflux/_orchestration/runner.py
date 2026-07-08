"""Operational runner internals for GrafConflux."""

from __future__ import annotations

import copy
import datetime
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import List, Protocol

import urllib3
import yaml

from grafconflux._orchestration.upload_merge import transform_grafana_configs
from grafconflux._orchestration.manifest import dashboard_metadata_files, write_run_manifest
from grafconflux._orchestration.paths import build_run_folder_name
from grafconflux._confluence.content import build_child_page_title
from grafconflux._confluence.links import build_confluence_page_url
from grafconflux._shared.grafana_models import GrafanaConfigDownloader, GrafanaConfigUploader
from grafconflux._grafana.credentials import GrafanaSessionPool

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
    confluence_token: str | None
    confluence_page_id: int | None
    confluence_parent_page_id: int | None
    confluence_child_title: str | None
    confluence_child_title_prefix: str
    confluence_child_title_from_test_id: bool
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
    playwright_browser: str | None
    playwright_browser_channel: str | None
    playwright_browser_executable_path: str | None
    timestamps: List[object]


@dataclass(frozen=True)
class ConfluencePageLink:
    """Confluence page link emitted after a successful run."""

    page_id: int
    url: str
    role: str


@dataclass(frozen=True)
class RunResult:
    """Structured result for completed orchestration work."""

    page_links: tuple[ConfluencePageLink, ...] = ()


def run(args: RunArgs, confluence_manager_class, grafana_manager_class, process_dashboard) -> RunResult:
    result = _run(args, confluence_manager_class, grafana_manager_class, process_dashboard)
    _log_page_links(result)
    return result


def _run(args: RunArgs, confluence_manager_class, grafana_manager_class, process_dashboard) -> RunResult:
    batch_run_args = getattr(args, 'batch_run_args', None)
    if batch_run_args:
        if _is_child_batch(batch_run_args):
            return run_child_page_batch(args, confluence_manager_class, grafana_manager_class, process_dashboard)
        return _run_direct_page_batch(batch_run_args, confluence_manager_class, grafana_manager_class, process_dashboard)

    if _is_child_mode(args):
        return run_child_page(args, confluence_manager_class, grafana_manager_class, process_dashboard)

    return run_direct_page(args, confluence_manager_class, grafana_manager_class, process_dashboard)


def run_direct_page(
    args: RunArgs,
    confluence_manager_class,
    grafana_manager_class,
    process_dashboard,
    page_role: str = 'target',
) -> RunResult:
    """Run the existing direct Confluence page workflow."""

    if args.test_upload_folders:
        page_url = upload_already_downloaded_graphs(args, confluence_manager_class)
        return _single_page_result(args, args.confluence_page_id, page_role, page_url)

    test_folder = _build_test_folder(args)

    # Load Grafana configurations
    grafana_configs = grafana_manager_class.load_grafana_config(args.config_file)
    _apply_runtime_playwright_options(grafana_configs, args)
    args._grafana_session_pool = GrafanaSessionPool()

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
    write_run_manifest(test_folder, grafana_configs, args.config_file)

    # Update Confluence page content
    page_url = None
    if not args.only_graphs:
        confluence_manager.upload_charts(test_folder, [['.json', 'application/json']])
        page_url = confluence_manager.update_page_content(
            grafana_configs, args.timestamps, args.graph_width, test_folder
        )
    result = _single_page_result(args, args.confluence_page_id, page_role, page_url)
    _log_immediate_page_link(result, page_url is not None)
    return result


def run_child_page(args: RunArgs, confluence_manager_class, grafana_manager_class, process_dashboard) -> RunResult:
    """Publish a run to a child page and optionally update the parent marker."""
    child_args = _clone_without_child_mode(args)
    parent_id = _required_parent_page_id(args)
    if args.only_graphs:
        child_args.confluence_page_id = parent_id
        return run_direct_page(child_args, confluence_manager_class, grafana_manager_class, process_dashboard)
    parent_manager = _create_confluence_manager_for_page(args, parent_id, confluence_manager_class)
    child_page = parent_manager.create_or_get_child_page(parent_id, child_args)
    child_result = run_direct_page(child_args, confluence_manager_class, grafana_manager_class, process_dashboard, 'child')
    parent_updated = parent_manager.update_parent_include_block(parent_id, [child_page])
    result = _with_parent_link(child_result, args, parent_id, parent_manager, parent_updated)
    _log_parent_page_link(result, parent_updated)
    return result


def run_child_page_batch(args: RunArgs, confluence_manager_class, grafana_manager_class, process_dashboard) -> RunResult:
    """Publish batch runs to child pages and update the parent once."""
    batch_run_args = getattr(args, 'batch_run_args', [])
    parent_id = _common_parent_page_id(batch_run_args)
    if args.only_graphs:
        return _run_child_batch_without_publishing(
            batch_run_args, parent_id, confluence_manager_class, grafana_manager_class, process_dashboard
        )
    parent_manager = _create_confluence_manager_for_page(args, parent_id, confluence_manager_class)
    parent_page = parent_manager.get_parent_page(parent_id)
    _validate_unique_child_titles(parent_page['title'], batch_run_args)
    child_pages = []
    child_results = []
    for run_args in batch_run_args:
        child_args = _clone_without_child_mode(run_args)
        child_page = parent_manager.create_or_get_child_page(parent_id, child_args)
        child_results.append(
            run_direct_page(child_args, confluence_manager_class, grafana_manager_class, process_dashboard, 'child')
        )
        child_pages.append(child_page)
    parent_updated = parent_manager.update_parent_include_block(parent_id, child_pages)
    result = _merge_run_results(child_results)
    result = _with_parent_link(result, args, parent_id, parent_manager, parent_updated)
    _log_parent_page_link(result, parent_updated)
    return result


def _run_child_batch_without_publishing(batch_run_args, parent_id, confluence_manager_class, grafana_manager_class,
                                        process_dashboard) -> RunResult:
    results = []
    for run_args in batch_run_args:
        child_args = _clone_without_child_mode(run_args)
        child_args.confluence_page_id = parent_id
        results.append(run_direct_page(child_args, confluence_manager_class, grafana_manager_class, process_dashboard))
    return _merge_run_results(results)


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
    return confluence_manager.update_page_content(grafana_configs, grafana_configs[0].timestamps, args.graph_width,
                                                  folder_graphs)


def _run_direct_page_batch(batch_run_args, confluence_manager_class, grafana_manager_class, process_dashboard):
    results = []
    for run_args in batch_run_args:
        results.append(_run(run_args, confluence_manager_class, grafana_manager_class, process_dashboard))
    return _merge_run_results(results)


def _merge_run_results(results: list[RunResult]) -> RunResult:
    page_links = []
    for result in results:
        page_links.extend(result.page_links)
    return RunResult(tuple(page_links))


def _single_page_result(args: RunArgs, page_id: int | None, role: str, page_url: str | None = None) -> RunResult:
    if page_id is None:
        return RunResult()
    url = page_url or build_confluence_page_url(args.wiki_url, page_id)
    return RunResult((ConfluencePageLink(page_id=int(page_id), url=url, role=role),))


def _with_parent_link(result: RunResult, args: RunArgs, parent_id: int, parent_manager, parent_updated: bool) -> RunResult:
    if not parent_updated:
        return result
    parent_url = getattr(parent_manager, 'last_parent_page_url', None)
    parent_link = _single_page_result(args, parent_id, 'parent', parent_url).page_links
    return RunResult(result.page_links + parent_link)


def _log_page_links(result: RunResult) -> None:
    if not result.page_links:
        return
    logger.info('Confluence page links:')
    for page_link in result.page_links:
        logger.info(' - %s page: %s', page_link.role, page_link.url)


def _log_immediate_page_link(result: RunResult, should_log: bool) -> None:
    if not should_log or not result.page_links:
        return
    page_link = result.page_links[0]
    logger.info('Confluence %s page updated: %s', page_link.role, page_link.url)


def _log_parent_page_link(result: RunResult, parent_updated: bool) -> None:
    if not parent_updated:
        return
    parent_link = next((page_link for page_link in result.page_links if page_link.role == 'parent'), None)
    if parent_link is None:
        return
    logger.info('Confluence parent page updated: %s', parent_link.url)


def _create_confluence_manager(args: RunArgs, confluence_manager_class):
    return _create_confluence_manager_for_page(args, args.confluence_page_id, confluence_manager_class)


def _create_confluence_manager_for_page(args: RunArgs, page_id: int | None, confluence_manager_class):
    return confluence_manager_class(
        login=args.confluence_login,
        password=args.confluence_password,
        token=getattr(args, 'confluence_token', None),
        page_id=page_id,
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


def _is_child_mode(args: RunArgs) -> bool:
    return getattr(args, 'confluence_parent_page_id', None) is not None


def _is_child_batch(batch_run_args: list[RunArgs]) -> bool:
    return any(_is_child_mode(run_args) for run_args in batch_run_args)


def _clone_without_child_mode(args: RunArgs):
    child_args = copy.copy(args)
    child_args.batch_run_args = []
    child_args.confluence_parent_page_id = None
    return child_args


def _required_parent_page_id(args: RunArgs) -> int:
    parent_id = getattr(args, 'confluence_parent_page_id', None)
    if parent_id is None:
        raise ValueError('Child page mode requires confluence_parent_page_id.')
    return parent_id


def _common_parent_page_id(batch_run_args: list[RunArgs]) -> int:
    parent_ids = {_required_parent_page_id(run_args) for run_args in batch_run_args}
    if len(parent_ids) != 1:
        raise ValueError('Child page batch mode requires one common parent page id.')
    return next(iter(parent_ids))


def _validate_unique_child_titles(parent_title: str, batch_run_args: list[RunArgs]) -> None:
    seen_titles = set()
    duplicate_titles = []
    for run_args in batch_run_args:
        child_title = build_child_page_title(parent_title, run_args)
        if child_title in seen_titles and child_title not in duplicate_titles:
            duplicate_titles.append(child_title)
        seen_titles.add(child_title)
    if duplicate_titles:
        duplicates = ', '.join(duplicate_titles)
        raise ValueError(f'Child page batch mode requires unique effective child titles. Duplicates: {duplicates}')


def _build_test_folder(args: RunArgs) -> str:
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return os.path.join(args.test_root_folder, build_run_folder_name(args.test_id, current_time))


def _apply_runtime_playwright_options(grafana_configs: List[GrafanaConfigDownloader], args: RunArgs) -> None:
    for grafana_config in grafana_configs:
        _set_optional_attr(grafana_config, 'playwright_browser', getattr(args, 'playwright_browser', None))
        _set_optional_attr(grafana_config, 'playwright_browser_channel', getattr(args, 'playwright_browser_channel', None))
        _set_optional_attr(
            grafana_config,
            'playwright_browser_executable_path',
            getattr(args, 'playwright_browser_executable_path', None),
        )


def _set_optional_attr(target: object, name: str, value: object | None) -> None:
    if value is not None:
        setattr(target, name, value)


def _load_upload_configs(upload_folders: List[str]) -> List[GrafanaConfigUploader]:
    grafana_configs = []
    for folder in upload_folders:
        for file in dashboard_metadata_files(folder):
            with open(file, 'r', encoding='utf-8') as yaml_file:
                config = yaml.safe_load(yaml_file)

            grafana_configs.append(GrafanaConfigUploader(config['name'], config))
    return grafana_configs


def get_yaml_files(directory):
    yaml_files = []

    for entry in sorted(os.listdir(directory)):
        full_path = os.path.join(directory, entry)

        if os.path.isfile(full_path) and entry.endswith('.yaml') and entry != 'manifest.yaml':
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
        grafana_manager = _create_grafana_manager(grafana_manager_class, grafana_config, args)
        _authenticate_and_download(grafana_manager, grafana_config, test_folder, args)

        # Upload to Confluence
        if not args.only_graphs:
            confluence_manager.upload_charts(
                grafana_manager.charts_path,
            )
    except Exception as e:
        logger.error(f'Failed to process dashboard {grafana_config.dash_title}: {e}')
        if not getattr(args, 'confluence_continue_on_error', True):
            raise


def _create_grafana_manager(grafana_manager_class, grafana_config: GrafanaConfigDownloader, args: RunArgs):
    session_record = _session_record(args, grafana_config)
    if session_record is None:
        return grafana_manager_class(config=grafana_config)
    return grafana_manager_class(config=grafana_config, session=session_record.session)


def _authenticate_and_download(grafana_manager, grafana_config: GrafanaConfigDownloader, test_folder: str, args: RunArgs) -> None:
    session_record = _session_record(args, grafana_config)
    if session_record is None:
        _download_with_auth(grafana_manager, test_folder, args)
        return
    with session_record.lock:
        if not session_record.authenticated:
            grafana_manager.authenticate(args.confluence_login, args.confluence_password)
            session_record.authenticated = True
        grafana_manager.download_charts(test_folder=test_folder, timestamps=args.timestamps)


def _download_with_auth(grafana_manager, test_folder: str, args: RunArgs) -> None:
    grafana_manager.authenticate(args.confluence_login, args.confluence_password)
    grafana_manager.download_charts(test_folder=test_folder, timestamps=args.timestamps)


def _session_record(args: RunArgs, grafana_config: GrafanaConfigDownloader):
    pool = getattr(args, '_grafana_session_pool', None)
    if pool is None:
        return None
    return pool.record_for(grafana_config)


def _raise_failed_futures(futures: List[Future]) -> None:
    for future in futures:
        future.result()


__all__ = [
    "ConfluencePageLink",
    "RunArgs",
    "RunResult",
    "_apply_runtime_playwright_options",
    "_build_test_folder",
    "_validate_unique_child_titles",
    "_create_confluence_manager",
    "_load_upload_configs",
    "_raise_failed_futures",
    "get_yaml_files",
    "logger",
    "process_grafana_dashboard",
    "run",
    "run_child_page",
    "run_child_page_batch",
    "run_direct_page",
    "upload_already_downloaded_graphs",
]
