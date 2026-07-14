"""Upload-only merge helpers for GrafConflux orchestration."""

from __future__ import annotations

import datetime
import copy
import os
import re
import shutil
from dataclasses import dataclass, field
from typing import List

import yaml

from grafconflux._shared.grafana_models import GrafanaConfigUploader, Panel
from grafconflux._shared.grafana_models import ConfigurationError
from grafconflux._shared.matrix_layout import DEFAULT_MATRIX_LAYOUT, validated_metadata_layout
from grafconflux._orchestration.manifest import assign_artifact_order, write_run_manifest
from grafconflux._orchestration.paths import build_run_folder_name


@dataclass
class _UploadMergeState:
    timestamps_count: dict[str, int] = field(default_factory=dict)
    config_names: list[str] = field(default_factory=list)
    snapshot_urls: dict[str, list] = field(default_factory=dict)
    full_links: dict[str, list] = field(default_factory=dict)
    matrix_dashboard_links: dict[str, list] = field(default_factory=dict)
    backup_dashboard_links: dict[str, list] = field(default_factory=dict)
    confluence_rendering: dict[str, dict] = field(default_factory=dict)
    render_matrix: dict[str, dict | None] = field(default_factory=dict)
    matrix_schemas: dict[str, frozenset[tuple[tuple[str, str, str, bool, bool], ...]] | None] = field(default_factory=dict)
    vars_presentation: dict[str, dict] = field(default_factory=dict)
    has_vars_presentation_metadata: dict[str, bool] = field(default_factory=dict)
    timestamps: dict[str, list] = field(default_factory=dict)
    panels: dict[str, list] = field(default_factory=dict)

    def ensure_config(self, grafana_config: GrafanaConfigUploader) -> None:
        if grafana_config.name in self.snapshot_urls:
            return
        self.timestamps_count[grafana_config.name] = 0
        self.snapshot_urls[grafana_config.name] = []
        self.full_links[grafana_config.name] = []
        self.matrix_dashboard_links[grafana_config.name] = []
        self.backup_dashboard_links[grafana_config.name] = list(grafana_config.backup_dashboard_links)
        self.confluence_rendering[grafana_config.name] = grafana_config.confluence_rendering.to_metadata()
        self.render_matrix[grafana_config.name] = getattr(grafana_config, 'render_matrix', None)
        self.matrix_schemas[grafana_config.name] = _matrix_schema_fingerprints(grafana_config.panels)
        self.vars_presentation[grafana_config.name] = getattr(grafana_config, 'vars_presentation', {})
        self.has_vars_presentation_metadata[grafana_config.name] = _has_vars_presentation_metadata(grafana_config)
        self.timestamps[grafana_config.name] = []
        self.panels[grafana_config.name] = []


def transform_grafana_configs(grafana_configs: List[GrafanaConfigUploader], args):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_folder_graphs = os.path.join(args.test_root_folder, build_run_folder_name(args.test_id, current_time))

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
    write_run_manifest(new_folder_graphs, new_configs, getattr(args, 'config_file', None))
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
    _append_config_name_once(merge_state, grafana_config.name)
    merge_state.ensure_config(grafana_config)
    _merge_render_matrix(merge_state, grafana_config)
    _merge_vars_presentation(merge_state, grafana_config)
    merge_state.snapshot_urls[grafana_config.name].extend(grafana_config.snapshot_urls)
    merge_state.full_links[grafana_config.name].extend(grafana_config.full_links)
    merge_state.matrix_dashboard_links[grafana_config.name].extend(
        _shift_matrix_dashboard_links(grafana_config.matrix_dashboard_links, timestamp_offset)
    )
    _merge_upload_panel_data(merge_state, grafana_config, timestamp_offset)


def _merge_render_matrix(merge_state: _UploadMergeState, grafana_config: GrafanaConfigUploader) -> None:
    config_name = grafana_config.name
    current = merge_state.render_matrix[config_name]
    incoming = getattr(grafana_config, 'render_matrix', None)
    if current is None:
        merge_state.render_matrix[config_name] = copy.deepcopy(incoming)
        return
    if incoming is None:
        return
    current_layout = _upload_matrix_layout(config_name, current)
    incoming_layout = _upload_matrix_layout(config_name, incoming)
    if current_layout != incoming_layout:
        raise ConfigurationError(
            f"upload merge for dashboard '{config_name}': render_matrix layouts differ across folders "
            f"({current_layout} != {incoming_layout})."
        )
    _validate_matrix_schema_compatibility(
        config_name,
        current_layout,
        merge_state.matrix_schemas[config_name],
        _matrix_schema_fingerprints(grafana_config.panels),
    )


def _upload_matrix_layout(config_name: str, matrix: object) -> str:
    if not isinstance(matrix, dict):
        raise ConfigurationError(
            f"upload merge for dashboard '{config_name}': invalid render_matrix layout metadata."
        )
    return validated_metadata_layout(matrix.get('layout'))


def _validate_matrix_schema_compatibility(
    config_name: str,
    layout: str,
    current_schema: frozenset[tuple[tuple[str, str, str, bool, bool], ...]] | None,
    incoming_schema: frozenset[tuple[tuple[str, str, str, bool, bool], ...]] | None,
) -> None:
    if layout != DEFAULT_MATRIX_LAYOUT:
        return
    if current_schema and incoming_schema and current_schema != incoming_schema:
        raise ConfigurationError(
            f"upload merge for dashboard '{config_name}': matrix dimension schemas differ across folders."
        )


def _matrix_schema_fingerprints(panels: list[Panel] | None) -> frozenset[tuple[tuple[str, str, str, bool, bool], ...]] | None:
    """Fingerprint complete matrix context schemas without recording raw dimension values."""
    artifacts = [
        artifact
        for panel in panels or []
        for artifact in getattr(panel, 'artifacts', []) or []
        if artifact.get('artifact_type') == 'matrix'
    ]
    if not artifacts:
        return None
    schemas = [_context_schema(artifact) for artifact in artifacts]
    return frozenset(schemas) if all(schema is not None for schema in schemas) else None


def _context_schema(artifact: dict) -> tuple[tuple[str, str, str, bool, bool], ...] | None:
    context = (artifact.get('matrix') or {}).get('context_path')
    if not isinstance(context, list) or not context:
        return None
    dimensions = [_context_dimension(item) for item in context]
    return tuple(dimensions) if all(dimension is not None for dimension in dimensions) else None


def _context_dimension(item: object) -> tuple[str, str, str, bool, bool] | None:
    if not isinstance(item, dict):
        return None
    key = item.get('key')
    variable = item.get('grafana_variable')
    label = item.get('label')
    if not all(isinstance(value, str) and value for value in (key, variable, label)):
        return None
    return key, variable, label, item.get('hidden') is True, item.get('hide_explicit') is True


def _append_config_name_once(merge_state: _UploadMergeState, config_name: str) -> None:
    if config_name not in merge_state.config_names:
        merge_state.config_names.append(config_name)


def _merge_vars_presentation(merge_state: _UploadMergeState, grafana_config: GrafanaConfigUploader) -> None:
    config_name = grafana_config.name
    if not _has_vars_presentation_metadata(grafana_config):
        return
    if not merge_state.has_vars_presentation_metadata[config_name]:
        merge_state.vars_presentation[config_name] = grafana_config.vars_presentation
        merge_state.has_vars_presentation_metadata[config_name] = True
        return
    if merge_state.vars_presentation[config_name] != grafana_config.vars_presentation:
        raise ConfigurationError(
            f"upload merge for dashboard '{config_name}': vars_presentation metadata differs across folders."
        )


def _has_vars_presentation_metadata(grafana_config: GrafanaConfigUploader) -> bool:
    return getattr(grafana_config, 'has_vars_presentation_metadata', True)


def _merge_upload_panel_data(
    merge_state: _UploadMergeState,
    grafana_config: GrafanaConfigUploader,
    timestamp_offset: int,
) -> None:
    for panel in grafana_config.panels:
        root_panel = _find_merged_panel(merge_state.panels[grafana_config.name], panel)
        if root_panel is None:
            merge_state.panels[grafana_config.name].append(_shift_panel(panel, timestamp_offset))
            continue
        _merge_upload_panel(root_panel, panel, timestamp_offset)
    for timestamp in grafana_config.timestamps:
        shifted_timestamp = copy.copy(timestamp)
        shifted_timestamp.id_time += timestamp_offset
        merge_state.timestamps[grafana_config.name].append(shifted_timestamp)


def _find_merged_panel(panels: list[Panel], panel: Panel) -> Panel | None:
    return next((root_panel for root_panel in panels if _same_upload_panel(root_panel, panel)), None)


def _same_upload_panel(left: Panel, right: Panel) -> bool:
    return (left.panel_id, left.type, left.title, left.display_title) == (right.panel_id, right.type, right.title, right.display_title)


def _shift_panel(panel: Panel, timestamp_offset: int) -> Panel:
    shifted_panel = copy.copy(panel)
    shifted_panel.links = _shift_links(panel.links, timestamp_offset)
    shifted_panel.artifacts = _shift_artifacts(getattr(panel, 'artifacts', []) or [], timestamp_offset)
    return shifted_panel


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
    assign_artifact_order(merged_config, preserve_existing=False)
    config_dict['panels'] = merged_config.panels
    _write_merged_upload_config_file(new_folder_graphs, config_name, config_dict)
    return merged_config


def _merged_upload_config_dict(
    merge_state: _UploadMergeState,
    new_folder_graphs: str,
    config_name: str,
) -> dict:
    config_dict = {
        'manifest': {'dashboard_order_index': merge_state.config_names.index(config_name)},
        'snapshot_urls': merge_state.snapshot_urls[config_name],
        'full_links': merge_state.full_links[config_name],
        'matrix_dashboard_links': merge_state.matrix_dashboard_links[config_name],
        'backup_dashboard_links': merge_state.backup_dashboard_links[config_name],
        'confluence_rendering': merge_state.confluence_rendering[config_name],
        'render_matrix': merge_state.render_matrix[config_name],
        'timestamps': merge_state.timestamps[config_name],
        'panels': merge_state.panels[config_name],
        'charts_path': os.path.join(new_folder_graphs, config_name),
    }
    if merge_state.has_vars_presentation_metadata[config_name]:
        config_dict['vars_presentation'] = merge_state.vars_presentation[config_name]
    return config_dict


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
    root_panel.links = _merge_shifted_links(root_panel.links, panel.links, timestamp_offset)
    if getattr(panel, 'artifacts', None):
        root_panel.artifacts.extend(_shift_artifacts(panel.artifacts, timestamp_offset))


def _shift_links(links: list, timestamp_offset: int) -> list:
    if timestamp_offset <= 0:
        return list(links)
    return [None] * timestamp_offset + list(links)


def _merge_shifted_links(root_links: list, panel_links: list, timestamp_offset: int) -> list:
    merged_links = list(root_links)
    shifted_links = _shift_links(panel_links, timestamp_offset)
    if len(merged_links) < len(shifted_links):
        merged_links.extend([None] * (len(shifted_links) - len(merged_links)))
    for index, link in enumerate(shifted_links):
        if link is not None:
            merged_links[index] = link
    return merged_links


def _shift_artifacts(artifacts: List[dict], timestamp_offset: int) -> List[dict]:
    return [_shift_artifact(artifact, timestamp_offset) for artifact in artifacts]


def _shift_matrix_dashboard_links(links: List[dict], timestamp_offset: int) -> List[dict]:
    shifted_links = []
    for link in links or []:
        shifted_link = copy.deepcopy(link)
        if shifted_link.get('timestamp_id') is not None:
            shifted_link['timestamp_id'] = int(shifted_link['timestamp_id']) + timestamp_offset
        shifted_links.append(shifted_link)
    return shifted_links


def _shift_artifact(artifact: dict, timestamp_offset: int) -> dict:
    shifted_artifact = copy.deepcopy(artifact)
    shifted_artifact.pop('artifact_id', None)
    shifted_artifact.pop('order_index', None)
    if shifted_artifact.get('timestamp_id') is not None:
        shifted_artifact['timestamp_id'] = int(shifted_artifact['timestamp_id']) + timestamp_offset
    if shifted_artifact.get('source_timestamp_id') is not None:
        shifted_artifact['source_timestamp_id'] = int(shifted_artifact['source_timestamp_id']) + timestamp_offset
    if shifted_artifact.get('png_file'):
        shifted_artifact['png_file'] = _shift_png_file_name(shifted_artifact['png_file'], timestamp_offset)
    _shift_composite_sources(shifted_artifact, timestamp_offset)
    return shifted_artifact


def _shift_composite_sources(artifact: dict, timestamp_offset: int) -> None:
    composite = artifact.get('composite') or {}
    for source in composite.get('sources') or []:
        source.pop('artifact_id', None)
        if source.get('source_timestamp_id') is not None:
            source['source_timestamp_id'] = int(source['source_timestamp_id']) + timestamp_offset
        if source.get('png_file'):
            source['png_file'] = _shift_png_file_name(source['png_file'], timestamp_offset)


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
    return ['panel_id', 'type', 'title', 'display_title', 'row_title', 'from_collapsed_row', 'row_id', 'grid_pos', 'links']


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
    "_shift_matrix_dashboard_links",
    "_shift_png_file_name",
    "_upload_config_matches_folder",
    "_upload_match_key",
    "_write_merged_upload_config",
    "_write_merged_upload_configs",
    "transform_grafana_configs",
]
