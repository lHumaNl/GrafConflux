"""Composite PNG artifact generation from rendered Grafana panel images."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from grafconflux._shared.grafana_models import ConfigurationError, Panel
from grafconflux._orchestration.manifest import assign_artifact_order
from grafconflux._grafana.composite_contexts import context_groups, context_suffix, source_matrix_context

COMPOSITES_KEY = "composites"
SUPPORTED_LAYOUTS = {"vertical", "horizontal", "grid", "dashboard_grid"}
MISSING_POLICIES = {"fail", "skip", "placeholder"}
THREE_PANEL_POLICIES = {"preserve", "top_wide", "bottom_half"}
DEFAULT_GAP_PX = 16
DEFAULT_BACKGROUND = "#111217"
PLACEHOLDER_SIZE = (640, 360)


@dataclass(frozen=True)
class CompositeSource:
    panel: Panel | None
    artifact: dict[str, Any]
    path: str | None
    selector: dict[str, Any]
    missing_reason: str | None = None


def validated_composites(dashboard_name: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    value = config.get(COMPOSITES_KEY, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ConfigurationError(f"dashboards.{dashboard_name}.{COMPOSITES_KEY}: expected list of mappings.")
    for index, rule in enumerate(value):
        _validate_composite_rule(dashboard_name, index, rule)
    return list(value)


def generate_composites(config: Any, charts_path: str, timestamps: list[Any]) -> None:
    rules = getattr(config, "composites", [])
    if not rules:
        return
    _ensure_pillow_available()
    for rule_index, rule in enumerate(rules):
        panel = _composite_panel(rule_index, rule, len(timestamps))
        config.panels.append(panel)
        for timestamp in timestamps:
            _generate_composite_for_timestamp(config, charts_path, rule, rule_index, panel, timestamp)


def _validate_composite_rule(dashboard_name: str, index: int, rule: dict[str, Any]) -> None:
    if rule.get("layout", "vertical") not in SUPPORTED_LAYOUTS:
        raise ConfigurationError(_path(dashboard_name, index, "layout") + ": unsupported layout.")
    if rule.get("three_panel_policy", "preserve") not in THREE_PANEL_POLICIES:
        raise ConfigurationError(_path(dashboard_name, index, "three_panel_policy") + ": expected preserve, top_wide, or bottom_half.")
    if rule.get("missing_source", "fail") not in MISSING_POLICIES:
        raise ConfigurationError(_path(dashboard_name, index, "missing_source") + ": expected fail, skip, or placeholder.")
    if not isinstance(rule.get("sources"), list) or not rule["sources"]:
        raise ConfigurationError(_path(dashboard_name, index, "sources") + ": expected non-empty list.")


def _path(dashboard_name: str, index: int, suffix: str) -> str:
    return f"dashboards.{dashboard_name}.{COMPOSITES_KEY}[{index}].{suffix}"


def _ensure_pillow_available() -> None:
    try:
        from PIL import Image  # noqa: F401
    except ImportError as error:
        raise RuntimeError("Composite images require Pillow. Install dependencies from requirements.txt.") from error


def _composite_panel(rule_index: int, rule: dict[str, Any], timestamps_count: int) -> Panel:
    title = str(rule.get("title") or rule.get("name") or f"Composite {rule_index + 1}")
    return Panel(0, "composite", title, timestamps_count, display_title=title)


def _generate_composite_for_timestamp(config: Any, charts_path: str, rule: dict[str, Any], rule_index: int,
                                      panel: Panel, timestamp: Any) -> None:
    sources = _resolve_sources(config, charts_path, rule, timestamp)
    for matrix_context, context_sources in context_groups(sources):
        _generate_composite_artifact(config, charts_path, rule, rule_index, panel, timestamp, context_sources, matrix_context)


def _generate_composite_artifact(config: Any, charts_path: str, rule: dict[str, Any], rule_index: int,
                                 panel: Panel, timestamp: Any, sources: list[CompositeSource], matrix_context) -> None:
    missing_policy = rule.get("missing_source", "fail")
    if _has_missing_sources(sources) and missing_policy == "fail":
        raise FileNotFoundError(f"Composite {rule.get('name', rule_index)} has missing source PNGs.")
    assign_artifact_order(config)
    artifact = _base_composite_artifact(config.name, rule, rule_index, timestamp, sources, matrix_context)
    if _has_missing_sources(sources) and missing_policy == "skip":
        artifact.update({"render_status": "skipped", "skip_reason": "missing_source", "png_file": None})
        panel.artifacts.append(artifact)
        return
    _write_composite_image(charts_path, artifact["png_file"], rule, sources, missing_policy)
    _record_composite_image_metadata(charts_path, artifact, rule)
    panel.artifacts.append(artifact)


def _resolve_sources(config: Any, charts_path: str, rule: dict[str, Any], timestamp: Any) -> list[CompositeSource]:
    sources: list[CompositeSource] = []
    for selector in rule["sources"]:
        matched = _matched_panels(config.panels, selector)
        if not matched:
            sources.append(_missing_selector_source(selector, timestamp))
            continue
        for panel in matched:
            sources.extend(_panel_sources(config.name, charts_path, panel, selector, timestamp, rule))
    return sources


def _matched_panels(panels: list[Panel], selector: dict[str, Any]) -> list[Panel]:
    return [panel for panel in panels if panel.type != "composite" and _panel_matches(panel, selector)]


def _panel_matches(panel: Panel, selector: dict[str, Any]) -> bool:
    if selector.get("type") not in (None, panel.type):
        return False
    if "panel_id" in selector and selector["panel_id"] == panel.panel_id:
        return True
    if "title" in selector and selector["title"] in {panel.title, panel.display_title}:
        return True
    return _matches_regex(panel.title, selector.get("title_regex")) or _matches_regex(panel.display_title, selector.get("title_regex"))


def _matches_regex(value: str, pattern: Any) -> bool:
    if pattern in (None, ""):
        return False
    return re.search(str(pattern), value or "") is not None


def _panel_sources(config_name: str, charts_path: str, panel: Panel, selector: dict[str, Any], timestamp: Any,
                   rule: dict[str, Any]) -> list[CompositeSource]:
    artifacts = _candidate_artifacts(config_name, panel, timestamp)
    sources = [_source_from_artifact(charts_path, panel, artifact, selector) for artifact in artifacts if _artifact_matches(artifact, selector)]
    if not sources:
        return [_missing_panel_source(panel, selector, timestamp)]
    if rule.get("include_sources", True) is False:
        _hide_sources(sources)
    return sources


def _candidate_artifacts(config_name: str, panel: Panel, timestamp: Any) -> list[dict[str, Any]]:
    artifacts = [artifact for artifact in panel.artifacts if _same_timestamp(artifact, timestamp)]
    if artifacts:
        return artifacts
    artifact = _synthetic_normal_artifact(config_name, panel, timestamp)
    panel.artifacts.append(artifact)
    return [artifact]


def _synthetic_normal_artifact(config_name: str, panel: Panel, timestamp: Any) -> dict[str, Any]:
    link = panel.links[timestamp.id_time] if timestamp.id_time < len(panel.links) else None
    return {
        "artifact_type": "normal", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
        "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
        "render_status": "rendered", "png_file": f"{config_name}__{panel.panel_id}__{timestamp.id_time}.png",
        "link": link, "skip_reason": None,
    }


def _same_timestamp(artifact: dict[str, Any], timestamp: Any) -> bool:
    if "timestamp_id" in artifact:
        return artifact.get("timestamp_id") == timestamp.id_time
    return artifact.get("timestamp_tag") in (None, timestamp.time_tag)


def _artifact_matches(artifact: dict[str, Any], selector: dict[str, Any]) -> bool:
    artifact_type = selector.get("artifact_type")
    if artifact_type and artifact.get("artifact_type", "normal") != artifact_type:
        return False
    variant_rule = selector.get("variant_rule")
    if variant_rule:
        return (artifact.get("variant") or {}).get("rule_name") == variant_rule
    return True


def _source_from_artifact(charts_path: str, panel: Panel, artifact: dict[str, Any], selector: dict[str, Any]) -> CompositeSource:
    png_file = artifact.get("png_file")
    path = os.path.join(charts_path, png_file) if png_file else None
    missing_reason = None if path and os.path.isfile(path) else "missing_png_file"
    return CompositeSource(panel, artifact, path if path and os.path.isfile(path) else None, selector=selector, missing_reason=missing_reason)


def _missing_selector_source(selector: dict[str, Any], timestamp: Any) -> CompositeSource:
    return CompositeSource(None, _missing_artifact(timestamp), None, selector=selector, missing_reason="selector_matched_no_panels")


def _missing_panel_source(panel: Panel, selector: dict[str, Any], timestamp: Any) -> CompositeSource:
    artifact = _missing_artifact(timestamp)
    artifact["link"] = panel.links[timestamp.id_time] if timestamp.id_time < len(panel.links) else None
    return CompositeSource(panel, artifact, None, selector=selector, missing_reason="selector_matched_no_artifacts")


def _missing_artifact(timestamp: Any) -> dict[str, Any]:
    return {
        "artifact_type": "missing_source", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
        "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
        "render_status": "missing", "png_file": None, "link": None, "skip_reason": "missing_source",
    }


def _hide_sources(sources: list[CompositeSource]) -> None:
    for source in sources:
        source.artifact["confluence"] = {"visible": False, "hidden_reason": "composite_source"}


def _has_missing_sources(sources: list[CompositeSource]) -> bool:
    return not sources or any(source.path is None for source in sources)


def _base_composite_artifact(config_name: str, rule: dict[str, Any], rule_index: int, timestamp: Any,
                             sources: list[CompositeSource], matrix_context=None) -> dict[str, Any]:
    name = _safe_name(str(rule.get("name") or f"composite-{rule_index + 1}"))
    file_name = f"{config_name}__composite-{name}{context_suffix(matrix_context)}__{timestamp.id_time}.png"
    source_entries = [_source_metadata(source) for source in sources]
    return {
        "artifact_type": "composite", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
        "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
        "render_status": "rendered", "png_file": file_name, "skip_reason": None,
        "link": _composite_link(rule, source_entries),
        "composite": {"name": rule.get("name"), "layout": rule.get("layout", "vertical"),
                       "title": rule.get("title"), "three_panel_policy": _three_panel_policy(rule),
                       "matrix_context": matrix_context, "sources": source_entries},
    }


def _source_metadata(source: CompositeSource) -> dict[str, Any]:
    selector = dict(source.selector or {})
    metadata = {
        "artifact_id": source.artifact.get("artifact_id"), "panel_id": source.panel.panel_id if source.panel else None,
        "png_file": source.artifact.get("png_file"), "link": source.artifact.get("link"),
        "missing": source.path is None,
        "missing_reason": source.missing_reason,
        "placeholder": source.path is None,
        "selector": selector,
        "matrix_context": source_matrix_context(source) or None,
    }
    return metadata


def _composite_link(rule: dict[str, Any], source_entries: list[dict[str, Any]]) -> str | None:
    if rule.get("source_link_policy", "first") == "none":
        return None
    return next((entry.get("link") for entry in source_entries if entry.get("link")), None)


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return safe or "composite"


def _write_composite_image(charts_path: str, file_name: str, rule: dict[str, Any], sources: list[CompositeSource],
                           missing_policy: str) -> None:
    from PIL import Image
    images = _open_source_images(sources, rule, missing_policy)
    composite = _compose_images(images, rule, sources)
    output_path = os.path.join(charts_path, file_name)
    composite.save(output_path, "PNG")
    for image in images:
        image.close()
    composite.close()


def _open_source_images(sources: list[CompositeSource], rule: dict[str, Any], missing_policy: str) -> list[Any]:
    if sources:
        return [_open_source_image(source, rule, missing_policy) for source in sources]
    if missing_policy == "placeholder":
        return [_placeholder_image(rule)]
    return []


def _open_source_image(source: CompositeSource, rule: dict[str, Any], missing_policy: str):
    from PIL import Image
    if source.path is not None:
        return Image.open(source.path).convert("RGBA")
    if missing_policy == "placeholder":
        return _placeholder_image(rule)
    raise FileNotFoundError("Missing composite source PNG.")


def _placeholder_image(rule: dict[str, Any]):
    from PIL import Image, ImageDraw
    image = Image.new("RGBA", PLACEHOLDER_SIZE, _background(rule))
    draw = ImageDraw.Draw(image)
    draw.text((20, 20), "Missing source", fill="#ffffff")
    return image


def _compose_images(images: list[Any], rule: dict[str, Any], sources: list[CompositeSource]):
    if not images:
        return _placeholder_image(rule)
    if not sources:
        return _compose_vertical(images, rule)
    layout = rule.get("layout", "vertical")
    if layout == "horizontal":
        return _compose_horizontal(images, rule)
    if layout == "grid":
        return _compose_grid(images, rule)
    if layout == "dashboard_grid":
        if any(source.panel is None for source in sources):
            return _compose_vertical(images, rule)
        return _compose_dashboard_grid(images, rule, sources)
    return _compose_vertical(images, rule)


def _compose_vertical(images: list[Any], rule: dict[str, Any]):
    gap = _gap(rule); width = max(image.width for image in images)
    height = sum(image.height for image in images) + gap * (len(images) - 1)
    canvas = _canvas(width, height, rule); y = 0
    for image in images:
        canvas.alpha_composite(image, ((width - image.width) // 2, y)); y += image.height + gap
    return canvas


def _compose_horizontal(images: list[Any], rule: dict[str, Any]):
    gap = _gap(rule); height = max(image.height for image in images)
    width = sum(image.width for image in images) + gap * (len(images) - 1)
    canvas = _canvas(width, height, rule); x = 0
    for image in images:
        canvas.alpha_composite(image, (x, (height - image.height) // 2)); x += image.width + gap
    return canvas


def _compose_grid(images: list[Any], rule: dict[str, Any]):
    columns = max(1, int(rule.get("columns", 2))); gap = _gap(rule)
    cell_width = max(image.width for image in images); cell_height = max(image.height for image in images)
    rows = (len(images) + columns - 1) // columns
    canvas = _canvas(columns * cell_width + gap * (columns - 1), rows * cell_height + gap * (rows - 1), rule)
    for index, image in enumerate(images):
        x = (index % columns) * (cell_width + gap); y = (index // columns) * (cell_height + gap)
        canvas.alpha_composite(image, (x, y))
    return canvas


def _compose_dashboard_grid(images: list[Any], rule: dict[str, Any], sources: list[CompositeSource]):
    three_panel = _three_panel_layout(images, rule, sources)
    if three_panel is not None:
        return three_panel
    gap = _gap(rule)
    unit_width, unit_height = _dashboard_grid_units(images, sources)
    x_positions, x_spans, width_units = _compacted_grid_axis([(_grid_x(source), _grid_w(source)) for source in sources])
    y_positions, y_spans, height_units = _compacted_grid_axis([(_grid_y(source), _grid_h(source)) for source in sources])
    width = width_units * unit_width; height = height_units * unit_height
    canvas = _canvas(width, height, rule)
    for image, x, y, w, h in zip(images, x_positions, y_positions, x_spans, y_spans):
        rect = x * unit_width, y * unit_height, w * unit_width, h * unit_height
        _paste_letterboxed(canvas, image, rect, gap)
    return canvas


def _three_panel_layout(images: list[Any], rule: dict[str, Any], sources: list[CompositeSource]):
    policy = _three_panel_policy(rule)
    if policy == "preserve" or len(images) != 3 or len(sources) != 3:
        return None
    if policy == "top_wide":
        return _compose_three_panel_top_wide(images, rule)
    return _compose_three_panel_bottom_half(images, rule)


def _compose_three_panel_top_wide(images: list[Any], rule: dict[str, Any]):
    top, bottom_left, bottom_right = images
    width = max(top.width, bottom_left.width + bottom_right.width)
    half_width = width // 2
    top_height = _fit_size(top, width, top.height)[1]
    bottom_height = max(_fit_size(image, half_width, image.height)[1] for image in (bottom_left, bottom_right))
    canvas = _canvas(width, top_height + bottom_height, rule)
    _paste_letterboxed(canvas, top, (0, 0, width, top_height), _gap(rule))
    _paste_letterboxed(canvas, bottom_left, (0, top_height, half_width, bottom_height), _gap(rule))
    _paste_letterboxed(canvas, bottom_right, (half_width, top_height, width - half_width, bottom_height), _gap(rule))
    return canvas


def _compose_three_panel_bottom_half(images: list[Any], rule: dict[str, Any]):
    top, bottom_left, bottom_right = images
    width = max(1, top.width)
    half_width = max(1, width // 2)
    top_height = top.height
    bottom_height = max(_fit_size(image, half_width, image.height)[1] for image in (bottom_left, bottom_right))
    canvas = _canvas(width, top_height + bottom_height, rule)
    _paste_letterboxed(canvas, top, (0, 0, width, top_height), _gap(rule))
    _paste_letterboxed(canvas, bottom_left, (0, top_height, half_width, bottom_height), _gap(rule))
    _paste_letterboxed(canvas, bottom_right, (half_width, top_height, width - half_width, bottom_height), _gap(rule))
    return canvas


def _dashboard_grid_units(images: list[Any], sources: list[CompositeSource]) -> tuple[int, int]:
    unit_width = max(max(round(image.width / max(1, _grid_w(source))), 1) for image, source in zip(images, sources))
    unit_height = max(max(round(image.height / max(1, _grid_h(source))), 1) for image, source in zip(images, sources))
    return unit_width, unit_height


def _compacted_grid_axis(spans: list[tuple[int, int]]) -> tuple[list[int], list[int], int]:
    intervals = _merged_grid_intervals(spans)
    positions: list[int] = []
    compact_spans: list[int] = []
    for start, span in spans:
        compact_start = _compact_axis_coord(start, intervals)
        compact_end = _compact_axis_coord(start + span, intervals)
        positions.append(compact_start)
        compact_spans.append(compact_end - compact_start)
    return positions, compact_spans, sum(end - start for start, end in intervals)


def _merged_grid_intervals(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    intervals = sorted((start, start + span) for start, span in spans if span > 0)
    if not intervals:
        return [(0, 1)]
    merged: list[list[int]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _compact_axis_coord(coord: int, intervals: list[tuple[int, int]]) -> int:
    offset = 0
    for start, end in intervals:
        if coord <= start:
            return offset
        if coord <= end:
            return offset + coord - start
        offset += end - start
    return offset


def _paste_letterboxed(canvas: Any, image: Any, rect: tuple[int, int, int, int], padding: int) -> None:
    fitted = _fit_image_to_rect(image, max(1, rect[2] - padding * 2), max(1, rect[3] - padding * 2))
    x = rect[0] + (rect[2] - fitted.width) // 2; y = rect[1] + (rect[3] - fitted.height) // 2
    canvas.alpha_composite(fitted, (x, y))
    if fitted is not image:
        fitted.close()


def _fit_image_to_rect(image: Any, width: int, height: int):
    size = _fit_size(image, width, height)
    if size == (image.width, image.height):
        return image
    return image.resize(size, _resize_filter())


def _fit_size(image: Any, width: int, height: int) -> tuple[int, int]:
    scale = min(width / image.width, height / image.height)
    return max(1, round(image.width * scale)), max(1, round(image.height * scale))


def _resize_filter():
    from PIL import Image
    return getattr(getattr(Image, "Resampling", Image), "LANCZOS")


def _grid_pos(source: CompositeSource) -> dict[str, int]:
    return source.panel.grid_pos or {"x": 0, "y": 0, "w": 24, "h": 8}


def _grid_x(source: CompositeSource) -> int:
    return int(_grid_pos(source).get("x", 0))


def _grid_y(source: CompositeSource) -> int:
    return int(_grid_pos(source).get("y", 0))


def _grid_w(source: CompositeSource) -> int:
    return int(_grid_pos(source).get("w", 24))


def _grid_h(source: CompositeSource) -> int:
    return int(_grid_pos(source).get("h", 8))


def _canvas(width: int, height: int, rule: dict[str, Any]):
    from PIL import Image
    return Image.new("RGBA", (max(1, width), max(1, height)), _background(rule))


def _background(rule: dict[str, Any]) -> str:
    return str(rule.get("background") or DEFAULT_BACKGROUND)


def _gap(rule: dict[str, Any]) -> int:
    return max(0, int(rule.get("gap_px", DEFAULT_GAP_PX)))


def _three_panel_policy(rule: dict[str, Any]) -> str:
    return str(rule.get("three_panel_policy") or "preserve")


def _record_composite_image_metadata(charts_path: str, artifact: dict[str, Any], rule: dict[str, Any]) -> None:
    from PIL import Image
    with Image.open(os.path.join(charts_path, artifact["png_file"])) as image:
        artifact["composite"]["image"] = {"width": image.width, "height": image.height,
                                           "gap_px": _gap(rule), "background": _background(rule)}
