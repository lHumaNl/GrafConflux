"""Shared render-matrix layout names and metadata migration helpers."""

from __future__ import annotations

from typing import Any


DEFAULT_MATRIX_LAYOUT = "matrix_grouped_panels"
MATRIX_LAYOUTS = frozenset({
    "dashboard_first",
    "matrix_grouped_panels",
    "matrix_values_first",
    "panel_first",
})


def resolved_metadata_matrix(value: Any, panels: list[Any] | None) -> Any:
    """Resolve the layout for replay metadata without validating downloader DSL fields."""
    if isinstance(value, dict):
        resolved = dict(value)
        resolved.setdefault("layout", DEFAULT_MATRIX_LAYOUT)
        resolved["layout"] = validated_metadata_layout(resolved["layout"])
        return resolved
    if value is None and _has_matrix_artifacts(panels):
        return {"layout": DEFAULT_MATRIX_LAYOUT}
    return value


def validated_metadata_layout(value: Any) -> str:
    """Return a supported replay layout without exposing invalid metadata values."""
    if isinstance(value, str) and value in MATRIX_LAYOUTS:
        return value
    from grafconflux._shared.grafana_models import ConfigurationError

    expected = ", ".join(sorted(MATRIX_LAYOUTS))
    raise ConfigurationError(f"render_matrix layout metadata: expected one of [{expected}].")


def _has_matrix_artifacts(panels: list[Any] | None) -> bool:
    return any(
        artifact.get("artifact_type") == "matrix"
        for panel in panels or []
        for artifact in getattr(panel, "artifacts", []) or []
    )
