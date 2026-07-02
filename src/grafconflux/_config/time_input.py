"""YAML time input file parsing for CLI batch runs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass(frozen=True)
class TimeInputFile:
    """Validated time input values loaded from a YAML file."""

    path: str
    page_id: int | None
    parent_page_id: int | None
    test_id: str | None
    title: str | None
    timestamps: list[str]


def load_time_input_files(paths: list[str]) -> list[TimeInputFile]:
    """Load and validate one or more YAML time input files."""
    return [load_time_input_file(path) for path in paths]


def load_time_input_file(path: str) -> TimeInputFile:
    """Load a YAML time input file without exposing file contents in errors."""
    _ensure_time_file_exists(path)
    with open(path, "r", encoding="utf-8") as time_file:
        data = yaml.safe_load(time_file) or {}
    return _time_input_from_data(os.fspath(path), data)


def _ensure_time_file_exists(path: str) -> None:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Time input file {path} not found.")


def _time_input_from_data(path: str, data: Any) -> TimeInputFile:
    if not isinstance(data, dict):
        raise ValueError(f"Time input file {path} must be a mapping.")
    return TimeInputFile(
        path=path,
        page_id=_optional_page_id_from_data(path, data, "page_id"),
        parent_page_id=_optional_page_id_from_data(path, data, "parent_page_id"),
        test_id=_test_id_from_data(path, data),
        title=_title_from_data(path, data),
        timestamps=_timestamps_from_data(path, data),
    )


def _optional_page_id_from_data(path: str, data: dict[str, Any], field_name: str) -> int | None:
    if field_name not in data or data[field_name] is None:
        return None
    page_id = data[field_name]
    if isinstance(page_id, str) and page_id.isdecimal():
        page_id = int(page_id)
    if isinstance(page_id, bool) or not isinstance(page_id, int) or page_id <= 0:
        raise ValueError(f"Time input file {path} field {field_name} must be a positive integer.")
    return page_id


def _test_id_from_data(path: str, data: dict[str, Any]) -> str | None:
    if "test_id" not in data or data["test_id"] is None:
        return None
    if not isinstance(data["test_id"], str):
        raise ValueError(f"Time input file {path} field test_id must be a string.")
    return data["test_id"]


def _title_from_data(path: str, data: dict[str, Any]) -> str | None:
    if "title" not in data or data["title"] is None:
        return None
    if not isinstance(data["title"], str):
        raise ValueError(f"Time input file {path} field title must be a string.")
    return data["title"]


def _timestamps_from_data(path: str, data: dict[str, Any]) -> list[str]:
    times = data.get("times")
    if not isinstance(times, list) or not times:
        raise ValueError(f"Time input file {path} field times must be a non-empty list.")
    return [_timestamp_from_time_entry(path, entry) for entry in times]


def _timestamp_from_time_entry(path: str, entry: Any) -> str:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return _timestamp_from_mapping_entry(path, entry)
    raise ValueError(f"Time input file {path} field times entries must be strings or single-pair mappings.")


def _timestamp_from_mapping_entry(path: str, entry: dict[Any, Any]) -> str:
    if len(entry) != 1:
        raise ValueError(f"Time input file {path} mapping time entries must contain exactly one pair.")
    label, time_range = next(iter(entry.items()))
    if label is None or isinstance(label, (dict, list, set, tuple)):
        raise ValueError(f"Time input file {path} mapping time labels must be scalar values.")
    if not isinstance(time_range, str):
        raise ValueError(f"Time input file {path} mapping time ranges must be strings.")
    return f"{label}__{time_range}"
