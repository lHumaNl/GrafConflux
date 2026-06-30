"""Compatibility facade for GrafConflux orchestration helpers."""

from __future__ import annotations

from grafconflux._orchestration.runner import (
    RunArgs,
    _build_test_folder,
    _load_upload_configs,
    _raise_failed_futures,
    get_yaml_files,
    logger,
)
from grafconflux._orchestration.runner import _create_confluence_manager as _create_confluence_manager_impl
from grafconflux._orchestration.runner import process_grafana_dashboard as _process_grafana_dashboard_impl
from grafconflux._orchestration.runner import run as _run_impl
from grafconflux._orchestration.runner import upload_already_downloaded_graphs as _upload_already_downloaded_graphs_impl
from grafconflux._orchestration.upload_merge import (
    _UploadMergeState,
    _copy_snapshot_backups,
    _copy_upload_graph_files,
    _merge_upload_config,
    _merge_upload_panel,
    _merge_upload_panel_data,
    _shift_artifact,
    _shift_artifacts,
    _shift_png_file_name,
    _upload_config_matches_folder,
    _upload_match_key,
    _write_merged_upload_config,
    _write_merged_upload_configs,
    transform_grafana_configs,
)
from grafconflux.confluence import ConfluenceManager
from grafconflux.grafana import GrafanaConfigDownloader, GrafanaConfigUploader, GrafanaManager


def run(args: RunArgs) -> None:
    _run_impl(args, ConfluenceManager, GrafanaManager, process_grafana_dashboard)


def upload_already_downloaded_graphs(args: RunArgs):
    return _upload_already_downloaded_graphs_impl(args, ConfluenceManager)


def _create_confluence_manager(args: RunArgs) -> ConfluenceManager:
    return _create_confluence_manager_impl(args, ConfluenceManager)


def process_grafana_dashboard(
    grafana_config: GrafanaConfigDownloader,
    test_folder: str,
    args: RunArgs,
    confluence_manager: ConfluenceManager,
):
    return _process_grafana_dashboard_impl(
        grafana_config,
        test_folder,
        args,
        confluence_manager,
        GrafanaManager,
    )


__all__ = [
    "ConfluenceManager",
    "GrafanaConfigDownloader",
    "GrafanaConfigUploader",
    "GrafanaManager",
    "RunArgs",
    "_UploadMergeState",
    "_build_test_folder",
    "_copy_snapshot_backups",
    "_copy_upload_graph_files",
    "_create_confluence_manager",
    "_load_upload_configs",
    "_merge_upload_config",
    "_merge_upload_panel",
    "_merge_upload_panel_data",
    "_raise_failed_futures",
    "_shift_artifact",
    "_shift_artifacts",
    "_shift_png_file_name",
    "_upload_config_matches_folder",
    "_upload_match_key",
    "_write_merged_upload_config",
    "_write_merged_upload_configs",
    "get_yaml_files",
    "logger",
    "process_grafana_dashboard",
    "run",
    "transform_grafana_configs",
    "upload_already_downloaded_graphs",
]
