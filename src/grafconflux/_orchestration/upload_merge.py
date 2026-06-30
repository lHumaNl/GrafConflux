"""Upload-only merge helpers for GrafConflux orchestration."""

from __future__ import annotations

import datetime
import os
import re
import shutil
from dataclasses import dataclass, field
from typing import List

import yaml

from grafconflux._shared.grafana_models import GrafanaConfigUploader, Panel


@dataclass
class _UploadMergeState:
    timestamps_count: dict[str, int] = field(default_factory=dict)
    config_names: set[str] = field(default_factory=set)
    snapshot_urls: dict[str, list] = field(default_factory=dict)
    full_links: dict[str, list] = field(default_factory=dict)
    backup_dashboard_links: dict[str, list] = field(default_factory=dict)
    timestamps: dict[str, list] = field(default_factory=dict)
    panels: dict[str, list] = field(default_factory=dict)

    def ensure_config(self, grafana_config: GrafanaConfigUploader) -> None:
        if grafana_config.name in self.snapshot_urls:
            return
        self.timestamps_count[grafana_config.name] = 0
        self.snapshot_urls[grafana_config.name] = []
        self.full_links[grafana_config.name] = []
        self.backup_dashboard_links[grafana_config.name] = list(grafana_config.backup_dashboard_links)
        self.timestamps[grafana_config.name] = []
        self.panels[grafana_config.name] = []


def transform_grafana_configs(grafana_configs: List[GrafanaConfigUploader], args):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_folder_graphs = os.path.join(args.test_root_folder, f'{args.test_id}__{current_time}')

    os.makedirs(new_folder_graphs, exist_ok=True)
    merge_state = _UploadMergeState()

    for folder_id, folder in enumerate(args.test_upload_folders):
        for grafana_config in grafana_configs:
            if not _upload_config_matches_folder(grafana_config, folder):
                continue

            timestamp_offset = merge_state.timestamps_count.get(grafana_config.name, 0)
            _merge_upload_config(merge_state, grafana_config, folder_id, timestamp_offset)
            _copy_upload_graph_files(grafana_config, new_folder_graphs, timestamp_offset)
            merge_state.timestamps_count[grafana_config.name] += len(grafana_config.timestamps)

    new_configs = _write_merged_upload_configs(merge_state, new_folder_graphs)
    _copy_snapshot_backups(args.test_upload_folders, new_folder_graphs)

    return new_configs, new_folder_graphs


def _upload_config_matches_folder(grafana_config: GrafanaConfigUploader, folder: str) -> bool:
    config_path = _upload_match_key(grafana_config.charts_path)
    folder_path = _upload_match_key(folder)
    return folder_path in config_path


def _upload_match_key(path: str) -> str:
    return path.replace('\\', '_').replace('/', '_')


def _merge_upload_config(
    merge_state: _UploadMergeState,
    grafana_config: GrafanaConfigUploader,
    folder_id: int,
    timestamp_offset: int,
) -> None:
    merge_state.config_names.add(grafana_config.name)
    merge_state.ensure_config(grafana_config)
    merge_state.snapshot_urls[grafana_config.name].extend(grafana_config.snapshot_urls)
    merge_state.full_links[grafana_config.name].extend(grafana_config.full_links)
    if folder_id == 0:
        merge_state.timestamps[grafana_config.name].extend(grafana_config.timestamps)
        merge_state.panels[grafana_config.name].extend(grafana_config.panels)
        return
    _merge_upload_panel_data(merge_state, grafana_config, timestamp_offset)


def _merge_upload_panel_data(
    merge_state: _UploadMergeState,
    grafana_config: GrafanaConfigUploader,
    timestamp_offset: int,
) -> None:
    for panel in grafana_config.panels:
        for root_panel in merge_state.panels[grafana_config.name]:
            if panel.panel_id == root_panel.panel_id:
                _merge_upload_panel(root_panel, panel, timestamp_offset)
                break
    for timestamp in grafana_config.timestamps:
        timestamp.id_time += timestamp_offset
        merge_state.timestamps[grafana_config.name].append(timestamp)


def _copy_upload_graph_files(
    grafana_config: GrafanaConfigUploader,
    new_folder_graphs: str,
    timestamp_offset: int,
) -> None:
    new_graphs_folder = os.path.join(new_folder_graphs, grafana_config.name)
    os.makedirs(new_graphs_folder, exist_ok=True)
    for entry in os.listdir(grafana_config.charts_path):
        src_path = os.path.join(grafana_config.charts_path, entry)
        dst_path = os.path.join(new_graphs_folder, _shift_png_file_name(entry, timestamp_offset))
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)


def _write_merged_upload_configs(
    merge_state: _UploadMergeState,
    new_folder_graphs: str,
) -> List[GrafanaConfigUploader]:
    return [
        _write_merged_upload_config(merge_state, new_folder_graphs, config_name)
        for config_name in merge_state.config_names
    ]


def _write_merged_upload_config(
    merge_state: _UploadMergeState,
    new_folder_graphs: str,
    config_name: str,
) -> GrafanaConfigUploader:
    config_dict = _merged_upload_config_dict(merge_state, new_folder_graphs, config_name)
    merged_config = GrafanaConfigUploader(config_name, config_dict)
    _write_merged_upload_config_file(new_folder_graphs, config_name, config_dict)
    return merged_config


def _merged_upload_config_dict(
    merge_state: _UploadMergeState,
    new_folder_graphs: str,
    config_name: str,
) -> dict:
    return {
        'snapshot_urls': merge_state.snapshot_urls[config_name],
        'full_links': merge_state.full_links[config_name],
        'backup_dashboard_links': merge_state.backup_dashboard_links[config_name],
        'timestamps': merge_state.timestamps[config_name],
        'panels': merge_state.panels[config_name],
        'charts_path': os.path.join(new_folder_graphs, config_name),
    }


def _write_merged_upload_config_file(new_folder_graphs: str, config_name: str, config_dict: dict) -> None:
    yaml_config = dict(config_dict)
    yaml_config['name'] = config_name
    yaml_config['timestamps'] = _convert_to_dict(yaml_config['timestamps'])
    yaml_config['panels'] = _convert_to_dict(yaml_config['panels'])
    with open(os.path.join(new_folder_graphs, f'{config_name}.yaml'), 'w+', encoding='utf-8') as yaml_file:
        yaml_file.write(yaml.safe_dump(yaml_config, sort_keys=False, allow_unicode=True))


def _copy_snapshot_backups(upload_folders: List[str], new_folder_graphs: str) -> None:
    for upload_folder in upload_folders:
        for entry in os.listdir(upload_folder):
            if not entry.endswith('.json'):
                continue
            src_path = os.path.join(upload_folder, entry)
            dst_path = os.path.join(new_folder_graphs, entry)
            if os.path.isfile(src_path):
                shutil.copy2(src_path, dst_path)


def _merge_upload_panel(root_panel, panel, timestamp_offset: int) -> None:
    root_panel.links.extend(panel.links)
    if getattr(panel, 'artifacts', None):
        root_panel.artifacts.extend(_shift_artifacts(panel.artifacts, timestamp_offset))


def _shift_artifacts(artifacts: List[dict], timestamp_offset: int) -> List[dict]:
    return [_shift_artifact(artifact, timestamp_offset) for artifact in artifacts]


def _shift_artifact(artifact: dict, timestamp_offset: int) -> dict:
    shifted_artifact = dict(artifact)
    if shifted_artifact.get('png_file'):
        shifted_artifact['png_file'] = _shift_png_file_name(shifted_artifact['png_file'], timestamp_offset)
    return shifted_artifact


def _shift_png_file_name(file_name: str, timestamp_offset: int) -> str:
    match = re.search(r'__(\d+)\.png$', file_name)
    if match is None or timestamp_offset == 0:
        return file_name
    shifted_id = int(match.group(1)) + timestamp_offset
    return f'{file_name[:match.start(1)]}{shifted_id}{file_name[match.end(1):]}'


def _convert_to_dict(obj):
    if isinstance(obj, list):
        return [_convert_to_dict(item) for item in obj]
    if isinstance(obj, Panel):
        return _panel_to_dict(obj)
    if hasattr(obj, '__dict__'):
        return {key: _convert_to_dict(value) for key, value in obj.__dict__.items()}
    return obj


def _panel_to_dict(panel: Panel) -> dict:
    data = {key: _convert_to_dict(getattr(panel, key)) for key in _legacy_panel_keys()}
    if panel.is_repeating or panel.artifacts:
        data.update(_repeating_panel_dict(panel))
    return data


def _legacy_panel_keys() -> List[str]:
    return ['panel_id', 'type', 'title', 'row_title', 'from_collapsed_row', 'row_id', 'grid_pos', 'links']


def _repeating_panel_dict(panel: Panel) -> dict:
    return {
        'is_repeating': panel.is_repeating,
        'source_panel_id': panel.source_panel_id,
        'repeat_var': panel.repeat_var,
        'artifacts': panel.artifacts,
    }


__all__ = [
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
