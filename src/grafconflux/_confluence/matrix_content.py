"""Confluence storage rendering for render-matrix artifacts."""

from __future__ import annotations

import html
from collections import OrderedDict
from typing import Any

from grafconflux._shared.display import normalize_grafana_display_value
from grafconflux._shared.confluence_settings import (
    ConfluenceRenderingSettings,
    DESCRIPTION_PANELS,
)
from grafconflux._confluence.row_groups import group_entries_by_row, row_group_title


def has_matrix_artifacts(grafana_config: Any) -> bool:
    return any(_matrix_artifacts(panel) for panel in getattr(grafana_config, "panels", []) or [])


def render_matrix_dashboard(grafana_config: Any, graph_width: int,
                            settings: ConfluenceRenderingSettings | None = None) -> str:
    settings = settings or _settings_for_config(grafana_config)
    if _matrix_layout(grafana_config) == "matrix_values_first":
        return _render_matrix_values_first_dashboard(grafana_config, graph_width, settings)
    title = html.escape(str(grafana_config.name))
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += _render_context_sections(grafana_config, graph_width, settings)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_matrix_values_first_dashboard(grafana_config: Any, graph_width: int,
                                          settings: ConfluenceRenderingSettings) -> str:
    title = html.escape(str(grafana_config.name))
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += _render_matrix_values_first_sections(grafana_config, graph_width, settings)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_matrix_values_first_sections(grafana_config: Any, graph_width: int,
                                          settings: ConfluenceRenderingSettings) -> str:
    sections = _context_sections(grafana_config, full_context=True)
    return _render_matrix_tree(grafana_config, sections.values(), graph_width, settings)


def _render_matrix_values_first_section(grafana_config: Any, section: dict[str, Any], graph_width: int,
                                        settings: ConfluenceRenderingSettings) -> str:
    content = ''
    content += _render_leaf_dashboard_links(grafana_config, section, settings)
    if settings.enabled(DESCRIPTION_PANELS):
        content += _render_context_panel_expand(settings.label(DESCRIPTION_PANELS), section["panels"].values(), graph_width)
    else:
        content += _render_panel_entries(list(section["panels"].values()), graph_width)
    return content


def _render_matrix_tree(grafana_config: Any, sections, graph_width: int,
                        settings: ConfluenceRenderingSettings) -> str:
    tree = _matrix_tree(sections)
    return ''.join(_render_matrix_node(grafana_config, node, graph_width, settings) for node in tree.values())


def _render_matrix_node(grafana_config: Any, node: dict[str, Any], graph_width: int,
                        settings: ConfluenceRenderingSettings) -> str:
    title = html.escape(node["title"])
    if _is_leaf_matrix_node(node):
        content = f'<h3>{title}</h3>\n'
        if node["section"] is not None:
            content += _render_matrix_values_first_section(grafana_config, node["section"], graph_width, settings)
        return content
    content = f'<h3>{title}</h3>\n<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n  <ac:rich-text-body>\n'
    if node["section"] is not None:
        content += _render_matrix_values_first_section(grafana_config, node["section"], graph_width, settings)
    content += ''.join(_render_matrix_node(grafana_config, child, graph_width, settings) for child in node["children"].values())
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _is_leaf_matrix_node(node: dict[str, Any]) -> bool:
    return not node["children"]


def _matrix_tree(sections) -> OrderedDict[str, dict[str, Any]]:
    tree: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for section in sections:
        children = tree
        for item in section["context"] or [{"key": "matrix", "label": "Matrix", "value": ""}]:
            key = f'{item.get("key")}={item.get("value")}'
            node = children.setdefault(key, {"title": _context_item_label(item), "children": OrderedDict(), "section": None})
            children = node["children"]
        node["section"] = section
    return tree


def _render_leaf_dashboard_links(grafana_config: Any, section: dict[str, Any],
                                 settings: ConfluenceRenderingSettings) -> str:
    if not settings.dashboard_links_at_leaf():
        return ''
    return _render_section_dashboard_links(grafana_config, section["context"], section["full_context"])


def _render_context_panel_expand(title: str, panel_entries, graph_width: int) -> str:
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{html.escape(title)}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += _render_panel_entries(list(panel_entries), graph_width)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _settings_for_config(grafana_config: Any) -> ConfluenceRenderingSettings:
    return getattr(grafana_config, "confluence_rendering", None) or ConfluenceRenderingSettings()


def _render_context_sections(grafana_config: Any, graph_width: int,
                             settings: ConfluenceRenderingSettings, full_context: bool = False) -> str:
    sections = _context_sections(grafana_config, full_context)
    return ''.join(_render_context_section(grafana_config, section, graph_width, settings) for section in sections.values())


def _context_sections(grafana_config: Any, full_context: bool = False) -> OrderedDict[str, dict[str, Any]]:
    sections: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for panel in _ordered_panels(getattr(grafana_config, "panels", []) or []):
        for artifact in _matrix_artifacts(panel):
            key, title = _section_key_title(grafana_config, artifact, full_context)
            section = sections.setdefault(
                key,
                {"title": title, "panels": OrderedDict(), "context": _context_path(artifact), "full_context": full_context},
            )
            panel_entry = section["panels"].setdefault(id(panel), {"panel": panel, "artifacts": []})
            panel_entry["artifacts"].append(artifact)
    return sections


def _render_context_section(grafana_config: Any, section: dict[str, Any], graph_width: int,
                            settings: ConfluenceRenderingSettings) -> str:
    content = f'<h3>{html.escape(section["title"])}</h3>\n'
    if settings.dashboard_links_at_leaf():
        section_links = _render_section_dashboard_links(grafana_config, section["context"], section["full_context"])
        if section_links:
            content += section_links
    panels = _render_row_groups(section["panels"].values(), graph_width)
    if settings.enabled(DESCRIPTION_PANELS):
        content += _render_expand(settings.label(DESCRIPTION_PANELS), panels)
    else:
        content += panels
    return content


def _render_expand(title: str, body: str) -> str:
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{html.escape(title)}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += body
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_section_dashboard_links(grafana_config: Any, context_path: list[dict[str, str]], full_context: bool) -> str:
    content = ''
    for link in getattr(grafana_config, "matrix_dashboard_links", []) or []:
        if not _same_link_context(context_path, link.get("context_path") or [], full_context):
            continue
        label = html.escape(str(link.get("label") or "Matrix"))
        url = html.escape(str(link.get("url") or ""))
        if url:
            content += f'<p><a href="{url}">{label}</a></p>\n'
    return content


def _render_row_groups(panel_entries, graph_width: int) -> str:
    groups = group_entries_by_row(_grouped_artifact_panel_entries(panel_entries))
    if len(groups) == 1:
        return _render_panel_entries(next(iter(groups.values())), graph_width)
    return ''.join(_render_row_group(title, entries, graph_width) for title, entries in groups.items())


def _grouped_artifact_panel_entries(panel_entries) -> list[dict[str, Any]]:
    entries: OrderedDict[tuple[int, str], dict[str, Any]] = OrderedDict()
    for entry in panel_entries:
        for artifact in _ordered_artifacts(entry["artifacts"]):
            group_title = row_group_title({"panel": entry["panel"], "artifact": artifact})
            grouped = entries.setdefault((id(entry["panel"]), group_title), {
                "panel": entry["panel"],
                "artifacts": [],
                "artifact": artifact,
            })
            grouped["artifacts"].append(artifact)
    return list(entries.values())


def _render_row_group(title: str, panel_entries: list[dict[str, Any]], graph_width: int) -> str:
    escaped = html.escape(title)
    content = f'<ac:structured-macro ac:name="expand">\n  <ac:parameter ac:name="title">{escaped}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += _render_panel_entries(panel_entries, graph_width)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_panel_entries(panel_entries: list[dict[str, Any]], graph_width: int) -> str:
    return ''.join(_render_panel_entry(entry, graph_width) for entry in panel_entries)


def _render_panel_entry(entry: dict[str, Any], graph_width: int) -> str:
    panel = entry["panel"]
    title = html.escape(str(getattr(panel, "display_title", panel.title)))
    content = ''
    content += '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n  <ac:rich-text-body>\n'
    for artifact in _ordered_artifacts(entry["artifacts"]):
        content += _render_artifact(panel, artifact, graph_width)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_artifact(panel: Any, artifact: dict[str, Any], graph_width: int) -> str:
    title = html.escape(_artifact_title(panel, artifact))
    content = ''
    link = html.escape(str(artifact.get("link") or _first_panel_link(panel) or ""))
    content += f'    <p><a href="{link}">{title}</a></p>\n' if link else f'    <p>{title} (Grafana link unavailable)</p>\n'
    file_name = html.escape(str(artifact["png_file"]))
    content += f'    <p><ac:image ac:width="{graph_width}"><ri:attachment ri:filename="{file_name}" /></ac:image></p>\n'
    return content


def _section_key_title(grafana_config: Any, artifact: dict[str, Any], full_context: bool = False) -> tuple[str, str]:
    context = _context_path(artifact)
    if not context:
        return "matrix", f"{grafana_config.name} (Matrix)"
    if full_context:
        return _full_context_key(context), _context_label(context)
    first = context[0]
    label = _context_item_label(first)
    return f'{first.get("key")}={first.get("value")}', f"{grafana_config.name} ({label})"


def _full_context_key(context: list[dict[str, str]]) -> str:
    return "|".join(f'{item.get("key")}={item.get("value")}' for item in context)


def _matrix_layout(grafana_config: Any) -> str:
    matrix = getattr(grafana_config, "render_matrix", None) or {}
    return str(matrix.get("layout", "dashboard_first")) if isinstance(matrix, dict) else "dashboard_first"


def _artifact_title(panel: Any, artifact: dict[str, Any]) -> str:
    if artifact.get("display_title"):
        return str(artifact["display_title"])
    panel_title = str(getattr(panel, "display_title", panel.title))
    label = _context_label(_context_path(artifact))
    return f"{panel_title} ({label})" if label else panel_title


def _context_label(context_path: list[dict[str, str]]) -> str:
    return ", ".join(_context_item_label(item) for item in context_path)


def _context_item_label(item: dict[str, str]) -> str:
    label = str(item.get("label") or _friendly_label(item.get("key") or "Variable"))
    value = normalize_grafana_display_value(item.get("value"))
    return f"{label}: {value}"


def _context_path(artifact: dict[str, Any]) -> list[dict[str, str]]:
    matrix = artifact.get("matrix") or {}
    if matrix.get("context_path"):
        return list(matrix["context_path"])
    variables = matrix.get("variables") or {}
    return [{"key": key, "label": key, "value": value, "grafana_variable": key} for key, value in variables.items()]


def _same_first_context(left: list[dict[str, str]], right: list[dict[str, str]]) -> bool:
    if not left or not right:
        return not left and not right
    return left[0].get("key") == right[0].get("key") and left[0].get("value") == right[0].get("value")


def _same_link_context(left: list[dict[str, str]], right: list[dict[str, str]], full_context: bool) -> bool:
    if not full_context:
        return _same_first_context(left, right)
    return _context_signature(left) == _context_signature(right)


def _context_signature(context: list[dict[str, str]]) -> list[tuple[str | None, str | None]]:
    return [(item.get("key"), item.get("value")) for item in context]


def _matrix_artifacts(panel: Any) -> list[dict[str, Any]]:
    return [artifact for artifact in getattr(panel, "artifacts", []) or [] if _is_visible_matrix_artifact(artifact)]


def _is_visible_matrix_artifact(artifact: dict[str, Any]) -> bool:
    confluence = artifact.get("confluence") or {}
    return (
        artifact.get("artifact_type") == "matrix"
        and artifact.get("render_status", "rendered") == "rendered"
        and confluence.get("visible", True) is not False
        and artifact.get("png_file")
    )


def _ordered_panels(panels):
    return sorted(panels or [], key=lambda panel: getattr(panel, "order_index", 0))


def _ordered_artifacts(artifacts):
    return sorted(artifacts or [], key=lambda artifact: artifact.get("order_index", 0))


def _first_panel_link(panel: Any) -> str:
    return next((link for link in getattr(panel, "links", []) if link), "")


def _friendly_label(value: Any) -> str:
    return str(value).replace("_", " ").replace("-", " ").title()
