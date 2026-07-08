"""Shared Confluence panel row grouping helpers."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any


def group_entries_by_row(panel_entries: list[dict[str, Any]]) -> OrderedDict[str, list[dict[str, Any]]]:
    groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for entry in panel_entries:
        groups.setdefault(row_group_title(entry), []).append(entry)
    return groups


def row_group_title(entry: dict[str, Any]) -> str:
    artifact = entry.get("artifact")
    if isinstance(artifact, dict):
        matrix_group = (artifact.get("matrix") or {}).get("group")
        if matrix_group:
            return str(matrix_group)
    panel = entry["panel"]
    return str(getattr(panel, "row_title", None) or "Panels")
