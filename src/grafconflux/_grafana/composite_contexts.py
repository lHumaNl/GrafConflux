"""Matrix-context grouping helpers for composite artifacts."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from typing import Any


def context_groups(sources: list[Any]) -> list[tuple[list[dict[str, Any]] | None, list[Any]]]:
    contexts: OrderedDict[tuple[tuple[str, str], ...], list[dict[str, Any]]] = OrderedDict()
    plain_sources: list[Any] = []
    for source in sources:
        context = source_matrix_context(source)
        if not context:
            plain_sources.append(source)
            continue
        contexts.setdefault(_context_key(context), context)
    if not contexts:
        return [(None, sources)]
    return [(context, plain_sources + _sources_for_context(sources, context)) for context in contexts.values()]


def source_matrix_context(source: Any) -> list[dict[str, Any]]:
    matrix = (getattr(source, "artifact", {}) or {}).get("matrix") or {}
    return [dict(item) for item in matrix.get("context_path") or []]


def context_suffix(context: list[dict[str, Any]] | None) -> str:
    if not context:
        return ""
    payload = repr([(item.get("key"), item.get("value")) for item in context])
    return "__matrix-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]


def _sources_for_context(sources: list[Any], context: list[dict[str, Any]]) -> list[Any]:
    key = _context_key(context)
    return [source for source in sources if _context_key(source_matrix_context(source)) == key]


def _context_key(context: list[dict[str, Any]]) -> tuple[tuple[str, str], ...]:
    return tuple((str(item.get("key")), str(item.get("value"))) for item in context or [])
