"""Filesystem path helpers for GrafConflux orchestration."""

from __future__ import annotations

import re

INVALID_PATH_SEGMENT_CHARS = r'[<>:"/\\|?*\x00-\x1f]+'
DEFAULT_RUN_FOLDER_NAME = 'run'


def sanitize_run_folder_segment(value: str | None) -> str:
    """Sanitize user-controlled folder name segments for local filesystem use."""
    normalized = re.sub(INVALID_PATH_SEGMENT_CHARS, '_', value or '')
    normalized = re.sub(r'\s+', ' ', normalized).strip(' .')
    return normalized or DEFAULT_RUN_FOLDER_NAME


def build_run_folder_name(test_id: str | None, current_time: str) -> str:
    """Build a stable run folder name without accidental nested directories."""
    return f'{sanitize_run_folder_segment(test_id)}__{current_time}'
