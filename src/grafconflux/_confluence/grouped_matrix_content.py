"""Confluence renderer for the grouped-panels matrix layout."""

from __future__ import annotations

import html
from collections import OrderedDict
from typing import Any

from grafconflux._shared.confluence_settings import ConfluenceRenderingSettings, DESCRIPTION_PANELS


def render_grouped_panels_dashboard(
    grafana_config: Any,
    graph_width: int,
    settings: ConfluenceRenderingSettings,
) -> str:
    title = html.escape(str(grafana_config.name))
    body = _render_group_content(grafana_config, _grouped_hierarchy(grafana_config), graph_width, settings)
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n  <ac:rich-text-body>\n'
    content += body
    return content + '  </ac:rich-text-body>\n</ac:structured-macro>\n'


def _grouped_hierarchy(grafana_config: Any) -> dict[str, Any]:
    from grafconflux._confluence.matrix_content import _matrix_artifacts, _ordered_panels

    root = _empty_node()
    for panel in _ordered_panels(getattr(grafana_config, "panels", []) or []):
        for artifact in _matrix_artifacts(panel):
            node = _prefix_node(grafana_config, root, _raw_context(artifact)[:-1])
            entry = node["panels"].setdefault(id(panel), {"panel": panel, "artifacts": []})
            entry["artifacts"].append(artifact)
    return root


def _empty_node(title: str | None = None) -> dict[str, Any]:
    return {"title": title, "children": OrderedDict(), "panels": OrderedDict()}


def _prefix_node(grafana_config: Any, root: dict[str, Any], context: list[dict[str, Any]]) -> dict[str, Any]:
    node = root
    for item in context:
        identity = _context_identity(item)
        children = node["children"]
        title = _prefix_title(grafana_config, item, len(children) + 1)
        node = children.setdefault(identity, _empty_node(title))
    return node


def _render_group_content(
    grafana_config: Any,
    node: dict[str, Any],
    graph_width: int,
    settings: ConfluenceRenderingSettings,
) -> str:
    content = _render_group_links(grafana_config, node["panels"], settings)
    content += _render_group_panels(grafana_config, node["panels"], graph_width, settings)
    content += ''.join(
        _render_group_node(grafana_config, child, graph_width, settings)
        for child in node["children"].values()
    )
    return content


def _render_group_node(
    grafana_config: Any,
    node: dict[str, Any],
    graph_width: int,
    settings: ConfluenceRenderingSettings,
) -> str:
    title = html.escape(str(node["title"]))
    body = _render_group_content(grafana_config, node, graph_width, settings)
    content = f'<h3>{title}</h3>\n<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n  <ac:rich-text-body>\n'
    return content + body + '  </ac:rich-text-body>\n</ac:structured-macro>\n'


def _render_group_links(
    grafana_config: Any,
    panels: OrderedDict,
    settings: ConfluenceRenderingSettings,
) -> str:
    if not panels or not settings.dashboard_links_at_leaf():
        return ""
    artifacts = _unique_group_artifacts(panels)
    return _matching_dashboard_links(grafana_config, artifacts)


def _unique_group_artifacts(panels: OrderedDict) -> list[dict[str, Any]]:
    from grafconflux._confluence.matrix_content import _ordered_artifacts

    return [
        artifact
        for entry in panels.values()
        for artifact in _ordered_artifacts(entry["artifacts"])
    ]


def _matching_dashboard_links(grafana_config: Any, artifacts: list[dict[str, Any]]) -> str:
    links = getattr(grafana_config, "matrix_dashboard_links", []) or []
    seen: set[tuple[Any, ...]] = set()
    content = ""
    for link in links:
        artifact = _matching_artifact(artifacts, link)
        identity = _link_identity(link)
        if artifact is None or identity in seen:
            continue
        rendered = _dashboard_link(link, _leaf_title(grafana_config, artifact))
        if rendered:
            seen.add(identity)
            content += rendered
    return content


def _matching_artifact(artifacts: list[dict[str, Any]], link: dict[str, Any]) -> dict[str, Any] | None:
    from grafconflux._confluence.matrix_content import _same_artifact_link_identity

    return next((artifact for artifact in artifacts if _same_artifact_link_identity(artifact, link)), None)


def _link_identity(link: dict[str, Any]) -> tuple[Any, ...]:
    context = tuple(_context_identity(item) for item in link.get("context_path") or [])
    timestamp = ("timestamp_id" in link, str(link.get("timestamp_id")))
    return context + (timestamp,)


def _dashboard_link(link: dict[str, Any], label: str) -> str:
    url = html.escape(str(link.get("url") or ""))
    return f'<p><a href="{url}">{html.escape(label)}</a></p>\n' if url else ""


def _render_group_panels(
    grafana_config: Any,
    panels: OrderedDict,
    graph_width: int,
    settings: ConfluenceRenderingSettings,
) -> str:
    if not panels:
        return ""
    body = ''.join(
        _render_group_panel(grafana_config, entry, graph_width)
        for entry in panels.values()
    )
    return _panels_container(body, settings)


def _panels_container(body: str, settings: ConfluenceRenderingSettings) -> str:
    from grafconflux._confluence.matrix_content import _render_expand

    if settings.enabled(DESCRIPTION_PANELS):
        return _render_expand(settings.label(DESCRIPTION_PANELS), body)
    return body


def _render_group_panel(grafana_config: Any, entry: dict[str, Any], graph_width: int) -> str:
    from grafconflux._confluence.matrix_content import _render_matrix_panel_artifacts

    panel = entry["panel"]
    title = html.escape(str(getattr(panel, "display_title", panel.title)))
    body = _render_matrix_panel_artifacts(
        panel,
        entry["artifacts"],
        graph_width,
        lambda artifact: _leaf_title(grafana_config, artifact),
    )
    content = '<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n  <ac:rich-text-body>\n'
    return content + body + '  </ac:rich-text-body>\n</ac:structured-macro>\n'


def _prefix_title(grafana_config: Any, item: dict[str, Any], ordinal: int) -> str:
    from grafconflux._confluence.matrix_content import _context_item_label

    if _explicitly_hidden(grafana_config, item):
        return f"Group {ordinal}"
    return _context_item_label(item)


def _leaf_title(grafana_config: Any, artifact: dict[str, Any]) -> str:
    from grafconflux._confluence.matrix_content import _context_item_label

    context = _raw_context(artifact)
    if context and not _explicitly_hidden(grafana_config, context[-1]):
        return _context_item_label(context[-1])
    matrix = artifact.get("matrix") or {}
    return str(matrix.get("neutral_label") or "Variant")


def _explicitly_hidden(grafana_config: Any, item: dict[str, Any]) -> bool:
    if item.get("hidden") is not True:
        return False
    if "hide_explicit" in item:
        return item.get("hide_explicit") is True
    spec = _matrix_variable_spec(grafana_config, str(item.get("key") or ""))
    return spec.get("hide") is True if spec is not None else True


def _matrix_variable_spec(grafana_config: Any, key: str) -> dict[str, Any] | None:
    matrix = getattr(grafana_config, "render_matrix", None)
    variables = matrix.get("variables") if isinstance(matrix, dict) else None
    spec = variables.get(key) if isinstance(variables, dict) else None
    return spec if isinstance(spec, dict) else None


def _raw_context(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    matrix = artifact.get("matrix") or {}
    if "context_path" in matrix:
        return list(matrix.get("context_path") or [])
    variables = matrix.get("variables") or {}
    return [{"key": key, "value": value, "raw_value": value} for key, value in variables.items()]


def _context_identity(item: dict[str, Any]) -> tuple[Any, Any]:
    return item.get("key"), item.get("raw_value", item.get("value"))
