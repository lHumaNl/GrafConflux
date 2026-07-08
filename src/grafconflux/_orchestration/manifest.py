"""Run-level manifest helpers for deterministic upload/render ordering."""

from __future__ import annotations

import datetime as _datetime
import os
from typing import Any

import yaml

MANIFEST_FILE = "manifest.yaml"
SCHEMA_VERSION = 2


def assign_artifact_order(config: Any, preserve_existing: bool = True) -> None:
    """Assign stable artifact ids/order indexes in panel list order."""

    dashboard_index = int(getattr(config, "order_index", 0) or 0)
    artifact_index = 0
    for panel_index, panel in enumerate(config.panels or []):
        setattr(panel, "order_index", panel_index)
        for artifact in getattr(panel, "artifacts", []) or []:
            _set_artifact_order(artifact, dashboard_index, panel_index, artifact_index, preserve_existing)
            artifact_index += 1
    _refresh_composite_source_references(config)


def write_run_manifest(test_folder: str, grafana_configs: list[Any], config_file: str | None = None) -> dict[str, Any]:
    manifest = build_run_manifest(test_folder, grafana_configs, config_file)
    os.makedirs(test_folder, exist_ok=True)
    with open(os.path.join(test_folder, MANIFEST_FILE), "w", encoding="utf-8") as manifest_file:
        manifest_file.write(yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True))
    return manifest


def build_run_manifest(test_folder: str, grafana_configs: list[Any], config_file: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": os.path.basename(test_folder),
        "config_file": config_file,
        "created_at": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
        "dashboards": [_dashboard_entry(test_folder, config, index) for index, config in enumerate(grafana_configs)],
        "artifacts": _artifact_entries(grafana_configs),
    }


def load_manifest(folder: str) -> dict[str, Any] | None:
    path = os.path.join(folder, MANIFEST_FILE)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as manifest_file:
        manifest = yaml.safe_load(manifest_file) or {}
    return manifest if isinstance(manifest, dict) else None


def dashboard_metadata_files(folder: str) -> list[str]:
    manifest = load_manifest(folder)
    if manifest:
        files = _manifest_metadata_files(folder, manifest)
        if files:
            return files
    return _legacy_metadata_files(folder)


def dashboard_manifest_metadata(config: Any) -> dict[str, Any]:
    return {
        "dashboard_order_index": getattr(config, "order_index", None),
        "credential_ref": getattr(config, "credential_ref", None),
        "session_mode": getattr(config, "session_mode", None),
        "config_source": getattr(config, "config_source", None),
    }


def _set_artifact_order(artifact: dict[str, Any], dashboard_index: int, panel_index: int,
                        artifact_index: int, preserve_existing: bool) -> None:
    if not preserve_existing:
        artifact.pop("order_index", None)
        artifact.pop("artifact_id", None)
    artifact.setdefault("order_index", dashboard_index * 100000 + artifact_index)
    artifact.setdefault("artifact_id", _artifact_id(dashboard_index, panel_index, artifact_index, artifact))
    artifact.setdefault("artifact_type", _artifact_type(artifact))


def _refresh_composite_source_references(config: Any) -> None:
    artifacts_by_png = _artifacts_by_png_file(config)
    for artifact in _all_artifacts(config):
        composite = artifact.get("composite") or {}
        for source in composite.get("sources") or []:
            _refresh_composite_source(source, artifacts_by_png)


def _artifacts_by_png_file(config: Any) -> dict[str, dict[str, Any]]:
    return {
        artifact["png_file"]: artifact
        for artifact in _all_artifacts(config)
        if artifact.get("png_file")
    }


def _all_artifacts(config: Any) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for panel in getattr(config, "panels", []) or []:
        artifacts.extend(getattr(panel, "artifacts", []) or [])
    return artifacts


def _refresh_composite_source(source: dict[str, Any], artifacts_by_png: dict[str, dict[str, Any]]) -> None:
    png_file = source.get("png_file")
    source_artifact = artifacts_by_png.get(png_file)
    if source_artifact is None:
        return
    source["artifact_id"] = source_artifact.get("artifact_id")


def _artifact_id(dashboard_index: int, panel_index: int, artifact_index: int, artifact: dict[str, Any]) -> str:
    artifact_type = _artifact_type(artifact)
    timestamp_id = artifact.get("timestamp_id", artifact.get("timestamp_tag", "x"))
    return f"dashboard-{dashboard_index:02d}.panel-{panel_index:03d}.{artifact_type}.{artifact_index:04d}.time-{timestamp_id}"


def _artifact_type(artifact: dict[str, Any]) -> str:
    if artifact.get("artifact_type"):
        return artifact["artifact_type"]
    if artifact.get("repeat_value") is not None:
        return "repeat"
    return "normal"


def _dashboard_entry(test_folder: str, config: Any, fallback_index: int = 0) -> dict[str, Any]:
    name = _config_name(config)
    metadata_file = f"{name}.yaml"
    return {
        "order_index": getattr(config, "order_index", None) if getattr(config, "order_index", None) is not None else fallback_index,
        "name": name,
        "metadata_file": metadata_file,
        "charts_path": getattr(config, "charts_path", os.path.join(test_folder, name)),
    }


def _artifact_entries(grafana_configs: list[Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for config in grafana_configs:
        entries.extend(_config_artifact_entries(config))
    return sorted(entries, key=lambda item: item.get("order_index") or 0)


def _config_artifact_entries(config: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for panel in getattr(config, "panels", None) or []:
        entries.extend(_panel_artifact_entries(config, panel))
    return entries


def _panel_artifact_entries(config: Any, panel: Any) -> list[dict[str, Any]]:
    artifacts = getattr(panel, "artifacts", []) or []
    if artifacts:
        return [_artifact_entry(config, panel, artifact) for artifact in artifacts]
    return [_legacy_panel_artifact_entry(config, panel, timestamp_index) for timestamp_index, _ in enumerate(getattr(config, "full_links", None) or [])]


def _artifact_entry(config: Any, panel: Any, artifact: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_index": artifact.get("order_index"), "dashboard": _config_name(config),
        "panel_id": panel.panel_id, "artifact_id": artifact.get("artifact_id"),
        "artifact_type": artifact.get("artifact_type", "normal"), "png_file": artifact.get("png_file"),
    }


def _legacy_panel_artifact_entry(config: Any, panel: Any, timestamp_index: int) -> dict[str, Any]:
    base = int(getattr(config, "order_index", 0) or 0) * 100000
    return {
        "order_index": base + timestamp_index, "dashboard": _config_name(config),
        "panel_id": panel.panel_id, "artifact_id": None, "artifact_type": "normal",
        "png_file": f"{_config_name(config)}__{panel.panel_id}__{timestamp_index}.png",
    }


def _config_name(config: Any) -> str:
    return str(getattr(config, "name", None) or getattr(config, "dash_title", None) or "dashboard")


def _manifest_metadata_files(folder: str, manifest: dict[str, Any]) -> list[str]:
    dashboards = manifest.get("dashboards") or []
    ordered = sorted(dashboards, key=lambda entry: entry.get("order_index") or 0)
    files = [os.path.join(folder, entry.get("metadata_file", "")) for entry in ordered]
    return [file for file in files if os.path.isfile(file)]


def _legacy_metadata_files(folder: str) -> list[str]:
    return [
        os.path.join(folder, entry)
        for entry in sorted(os.listdir(folder))
        if entry.endswith(".yaml") and entry != MANIFEST_FILE and os.path.isfile(os.path.join(folder, entry))
    ]
