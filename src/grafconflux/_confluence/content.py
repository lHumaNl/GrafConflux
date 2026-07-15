import html
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from typing import List, Optional

from grafconflux._shared.display import normalize_grafana_display_value
from grafconflux._shared.confluence_settings import (
    ConfluenceRenderingSettings,
    DESCRIPTION_BACKUP_DASHBOARD_LINKS,
    DESCRIPTION_PANELS,
    DESCRIPTION_TEST_TIMES,
    effective_time_zone,
    format_timestamp_time,
)
from grafconflux._shared.grafana_models import GrafanaConfigBase
from grafconflux._shared.time import GrafanaTimeBase
from grafconflux._confluence.matrix_content import has_matrix_artifacts, render_matrix_dashboard
from grafconflux._confluence.row_groups import group_entries_by_row

GRAPHS_PLACEHOLDER = '%%%graphs%%%'
DEFAULT_CHILD_TITLE_PREFIX = 'GrafConflux: '
DEFAULT_CHILD_TITLE_FALLBACK = 'GrafConflux child page'
MAX_CHILD_TITLE_LENGTH = 240
TITLE_FORBIDDEN_CHARS = r'[\\/?|<>]+'


@dataclass(frozen=True)
class ChildPageInclude:
    """Confluence child page reference for parent include macros."""

    title: str
    space_key: str
    page_id: int | None = None
    page_url: str | None = None

__all__ = ('GRAPHS_PLACEHOLDER', '_artifact_has_rendered_png', '_artifact_title', '_dashboard_period', '_first_panel_link', '_non_repeating_artifact_title', '_panel_period', '_render_dashboard_links', '_render_backup_dashboard_links', '_render_dashboards_section', '_render_panel_artifacts', '_render_panel_timestamps', '_render_panels', '_render_snapshot_backup_section', '_render_test_times_section', 'apply_graphs_placeholder', 'apply_graphs_placeholder_if_present', 'build_child_page_title', 'build_confluence_storage_content', 'build_parent_include_content', 'sanitize_confluence_page_title', 'ChildPageInclude', 'DEFAULT_CHILD_TITLE_PREFIX')


def apply_graphs_placeholder(body: str, new_content: str) -> str:
    """Replace the graphs placeholder when present, otherwise return generated content."""
    if body.__contains__(GRAPHS_PLACEHOLDER):
        return body.replace(GRAPHS_PLACEHOLDER, new_content)
    return new_content


def apply_graphs_placeholder_if_present(body: str, new_content: str) -> str | None:
    """Replace the graphs placeholder only when present."""
    if GRAPHS_PLACEHOLDER not in body:
        return None
    return body.replace(GRAPHS_PLACEHOLDER, new_content)


def sanitize_confluence_page_title(title: str | None) -> str:
    """Normalize a generated Confluence page title."""
    normalized = _collapse_whitespace(title or '')
    normalized = re.sub(TITLE_FORBIDDEN_CHARS, '-', normalized).strip(' .')
    if not normalized or normalized.strip('- ') == '':
        normalized = DEFAULT_CHILD_TITLE_FALLBACK
    return normalized[:MAX_CHILD_TITLE_LENGTH].rstrip(' .') or DEFAULT_CHILD_TITLE_FALLBACK


def build_child_page_title(parent_title: str, args) -> str:
    """Build a sanitized child page title from run arguments."""
    direct_title = _explicit_child_title(args)
    if direct_title is not None:
        return sanitize_confluence_page_title(direct_title)
    if getattr(args, 'confluence_child_title_from_test_id', False):
        return sanitize_confluence_page_title(_test_id_label(args))
    return sanitize_confluence_page_title(_default_child_page_title(parent_title, args))


def build_parent_include_content(child_pages: list[ChildPageInclude]) -> str:
    """Render expand macros that include child pages in order."""
    return ''.join(_render_parent_include_macro(child_page) for child_page in child_pages)


def _explicit_child_title(args) -> str | None:
    title = getattr(args, 'confluence_child_title', None)
    if title not in (None, ''):
        return str(title)
    return None


def _default_child_page_title(parent_title: str, args) -> str:
    prefix = getattr(args, 'confluence_child_title_prefix', DEFAULT_CHILD_TITLE_PREFIX)
    label = _child_title_label(args)
    return f'{parent_title} — {prefix}{label}'


def _child_title_label(args) -> str:
    test_id = _test_id_label(args)
    if test_id:
        return test_id
    timestamps = getattr(args, 'timestamps', [])
    time_tag = _first_time_tag(timestamps)
    return time_tag or _first_timestamp_label(timestamps) or DEFAULT_CHILD_TITLE_FALLBACK


def _test_id_label(args) -> str | None:
    test_id = getattr(args, 'test_id', None)
    if test_id in (None, '', '-1'):
        return None
    return str(test_id)


def _first_time_tag(timestamps) -> str | None:
    for timestamp in timestamps:
        time_tag = getattr(timestamp, 'time_tag', None)
        if time_tag not in (None, ''):
            return str(time_tag)
    return None


def _first_timestamp_label(timestamps) -> str | None:
    for timestamp in timestamps:
        start_time = getattr(timestamp, 'start_time_human', None)
        end_time = getattr(timestamp, 'end_time_human', None)
        if start_time and end_time:
            return f'{start_time} - {end_time}'
    return None


def _collapse_whitespace(value: str) -> str:
    return ' '.join(str(value).split())


def _render_parent_include_macro(child_page: ChildPageInclude) -> str:
    title = html.escape(child_page.title)
    include_macro = _render_include_page_macro(child_page)
    return (
        '<ac:structured-macro ac:name="expand">\n'
        f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n'
        '  <ac:rich-text-body>\n'
        f'{include_macro}'
        '  </ac:rich-text-body>\n'
        '</ac:structured-macro>\n'
    )


def _render_include_page_macro(child_page: ChildPageInclude) -> str:
    title = html.escape(child_page.title, quote=True)
    space_key = html.escape(child_page.space_key, quote=True)
    return (
        '    <ac:structured-macro ac:name="include">\n'
        '      <ac:parameter ac:name="">'
        f'<ac:link><ri:page ri:content-title="{title}" ri:space-key="{space_key}" /></ac:link>'
        '</ac:parameter>\n'
        '    </ac:structured-macro>\n'
    )


def build_confluence_storage_content(grafana_configs: List[GrafanaConfigBase], timestamps: List[GrafanaTimeBase],
                                      graph_width: int, snapshot_list: Optional[List[str]] = None) -> str:
    """Build Confluence storage-format HTML for downloaded Grafana artifacts."""
    settings = _rendering_settings(grafana_configs)
    new_content = ''
    if snapshot_list:
        new_content += _render_snapshot_backup_section(snapshot_list)
    new_content += _render_test_times_section(timestamps, settings)
    new_content += _render_dashboards_section(grafana_configs, timestamps, graph_width, settings)
    return new_content


def _rendering_settings(grafana_configs: List[GrafanaConfigBase]) -> ConfluenceRenderingSettings:
    for grafana_config in grafana_configs:
        settings = getattr(grafana_config, 'confluence_rendering', None)
        if isinstance(settings, ConfluenceRenderingSettings):
            return settings
    return ConfluenceRenderingSettings()


def _render_snapshot_backup_section(snapshot_list: List[str]) -> str:
    new_content = '<ac:structured-macro ac:name="expand">\n'
    new_content += '  <ac:parameter ac:name="title">Snapshot backups</ac:parameter>\n'
    new_content += '  <ac:rich-text-body>\n'
    for snapshot in snapshot_list:
        snapshot_name = html.escape(snapshot)
        new_content += f'<p><ac:link><ri:attachment ri:filename="{snapshot_name}" />'
        new_content += f'<ac:plain-text-link-body><![CDATA[{snapshot_name}]]></ac:plain-text-link-body>'
        new_content += '</ac:link></p>\n'
    new_content += '  </ac:rich-text-body>\n'
    new_content += '</ac:structured-macro>\n'
    return new_content


def _render_test_times_section(timestamps: List[GrafanaTimeBase],
                               settings: ConfluenceRenderingSettings | None = None) -> str:
    settings = settings or ConfluenceRenderingSettings()
    zone = effective_time_zone(settings)
    title = settings.label(DESCRIPTION_TEST_TIMES)
    new_content = '<ac:structured-macro ac:name="expand">\n'
    new_content += f'  <ac:parameter ac:name="title">{html.escape(title)}</ac:parameter>\n'
    new_content += '  <ac:rich-text-body>\n'
    if settings.timezone_label:
        new_content += f'<p>Timezone: {html.escape(zone.label)}</p>\n'
    new_content += '<table>\n  <tbody>\n    <tr>\n'
    new_content += '      <th>Test tag</th>\n      <th>Start test time</th>\n'
    new_content += '      <th>End test time</th>\n    </tr>\n'
    for timestamp in timestamps:
        new_content += '    <tr>\n'
        new_content += f'      <td>{html.escape(str(timestamp.time_tag))}</td>\n'
        new_content += f'      <td>{html.escape(format_timestamp_time(timestamp, "start", settings))}</td>\n'
        new_content += f'      <td>{html.escape(format_timestamp_time(timestamp, "end", settings))}</td>\n'
        new_content += '    </tr>\n'
    new_content += '  </tbody>\n</table>\n'
    new_content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return new_content


def _render_dashboards_section(grafana_configs: List[GrafanaConfigBase], timestamps: List[GrafanaTimeBase],
                               graph_width: int, settings: ConfluenceRenderingSettings | None = None) -> str:
    settings = settings or ConfluenceRenderingSettings()
    new_content = ''
    for grafana_config in grafana_configs:
        matrix_artifacts_present = has_matrix_artifacts(grafana_config)
        dash_title = html.escape(grafana_config.name)
        new_content += f'<h2>{dash_title}</h2>\n'
        dashboard_links = ''
        if settings.dashboard_links_at_dashboard(matrix_artifacts_present):
            dashboard_links += _render_dashboard_links(grafana_config, timestamps)
        if _should_render_matrix_links_at_dashboard(grafana_config, matrix_artifacts_present, settings):
            dashboard_links += _render_matrix_dashboard_links(grafana_config)
        if dashboard_links:
            new_content += dashboard_links
        if settings.enabled(DESCRIPTION_BACKUP_DASHBOARD_LINKS) and getattr(grafana_config, 'backup_dashboard_links', []):
            label = html.escape(settings.label(DESCRIPTION_BACKUP_DASHBOARD_LINKS))
            new_content += f'<p>{label}</p>\n'
            new_content += _render_backup_dashboard_links(grafana_config, timestamps)
        if matrix_artifacts_present:
            new_content += _render_matrix_dashboard(grafana_config, graph_width, settings)
            continue
        new_content += _render_panel_root(grafana_config, timestamps, graph_width, settings)
    return new_content


def _render_panel_root(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase],
                       graph_width: int, settings: ConfluenceRenderingSettings) -> str:
    panels = _render_panels(grafana_config, timestamps, graph_width)
    if not settings.enabled(DESCRIPTION_PANELS):
        return panels
    title = html.escape(settings.label(DESCRIPTION_PANELS))
    content = f'<ac:structured-macro ac:name="expand">\n'
    content += f'  <ac:parameter ac:name="title">{title}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += panels
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_matrix_dashboard(grafana_config: GrafanaConfigBase, graph_width: int,
                             settings: ConfluenceRenderingSettings) -> str:
    return render_matrix_dashboard(grafana_config, graph_width, settings)


def _should_render_matrix_links_at_dashboard(grafana_config: GrafanaConfigBase, matrix_artifacts_present: bool,
                                              settings: ConfluenceRenderingSettings) -> bool:
    return _has_matrix_dashboard_links(grafana_config) and settings.dashboard_links_at_dashboard(matrix_artifacts_present)


def _render_dashboard_links(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase]) -> str:
    new_content = ''
    snapshot_urls = ''
    for timestamp in timestamps:
        period = _dashboard_period(timestamp, len(timestamps))
        new_content += f'<p><a href="{html.escape(grafana_config.full_links[timestamp.id_time])}">{period}</a></p>\n'
        if grafana_config.snapshot_urls:
            snapshot_url = html.escape(grafana_config.snapshot_urls[timestamp.id_time])
            snapshot_urls += f'<p><a href="{snapshot_url}">{period} (Snapshot)</a></p>\n'
    if snapshot_urls != '':
        new_content += '<p>Snapshots</p>\n'
        new_content += snapshot_urls
    return new_content


def _has_matrix_dashboard_links(grafana_config: GrafanaConfigBase) -> bool:
    matrix_links = getattr(grafana_config, 'matrix_dashboard_links', [])
    return isinstance(matrix_links, list) and bool(matrix_links)


def _render_backup_dashboard_links(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase]) -> str:
    new_content = ''
    for backup_link in getattr(grafana_config, 'backup_dashboard_links', []):
        for timestamp in timestamps:
            period = _dashboard_period(timestamp, len(timestamps))
            url = _with_dashboard_timerange(
                backup_link,
                timestamp.start_time_timestamp,
                timestamp.end_time_timestamp,
            )
            new_content += f'<p><a href="{html.escape(url)}">{period}</a></p>\n'
    return new_content


def _render_matrix_dashboard_links(grafana_config: GrafanaConfigBase) -> str:
    new_content = ''
    matrix_links = getattr(grafana_config, 'matrix_dashboard_links', [])
    if not isinstance(matrix_links, list):
        return new_content
    for link in matrix_links:
        label = html.escape(str(link.get('label') or 'Matrix'))
        url = html.escape(str(link.get('url') or ''))
        if url:
            new_content += f'<p><a href="{url}">{label}</a></p>\n'
    return new_content


def _with_dashboard_timerange(url: str, start_time: int, end_time: int) -> str:
    parts = urlsplit(url)
    query_items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key not in {'from', 'to'}
    ]
    query_items.extend([('from', str(start_time)), ('to', str(end_time))])
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_items, doseq=True), parts.fragment))


def _render_panels(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase], graph_width: int) -> str:
    entries = [{'panel': panel} for panel in _ordered_panels(grafana_config.panels)]
    groups = group_entries_by_row(entries)
    if len(groups) == 1:
        return _render_panel_entries(grafana_config, timestamps, graph_width, next(iter(groups.values())))
    return ''.join(_render_flat_row_group(grafana_config, timestamps, graph_width, title, items) for title, items in groups.items())


def _render_flat_row_group(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase],
                           graph_width: int, title: str, entries: list[dict]) -> str:
    row_title = html.escape(title)
    content = f'<ac:structured-macro ac:name="expand">\n  <ac:parameter ac:name="title">{row_title}</ac:parameter>\n'
    content += '  <ac:rich-text-body>\n'
    content += _render_panel_entries(grafana_config, timestamps, graph_width, entries)
    content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return content


def _render_panel_entries(grafana_config: GrafanaConfigBase, timestamps: List[GrafanaTimeBase],
                          graph_width: int, entries: list[dict]) -> str:
    new_content = ''
    for entry in entries:
        panel = entry['panel']
        row_title = html.escape(getattr(panel, 'display_title', panel.title))
        new_content += f'<ac:structured-macro ac:name="expand">\n'
        new_content += f'  <ac:parameter ac:name="title">{row_title}</ac:parameter>\n'
        new_content += '  <ac:rich-text-body>\n'
        new_content += _render_panel_timestamps(grafana_config, panel, timestamps, row_title, graph_width)
        new_content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return new_content


def _render_panel_timestamps(grafana_config: GrafanaConfigBase, panel, timestamps: List[GrafanaTimeBase],
                             row_title: str, graph_width: int) -> str:
    if getattr(panel, 'artifacts', None):
        return _render_panel_artifacts(panel, row_title, graph_width)
    new_content = ''
    for timestamp in timestamps:
        period = _panel_period(timestamp, len(timestamps), row_title)
        image_name = f'{grafana_config.name}__{panel.panel_id}__{timestamp.id_time}.png'
        link = panel.links[timestamp.id_time] if timestamp.id_time < len(panel.links) else None
        if link:
            new_content += f'    <p><a href="{html.escape(link)}">{period}</a></p>\n'
        else:
            new_content += f'    <p>{period} (Grafana link unavailable)</p>\n'
        new_content += f'    <p><ac:image ac:width="{graph_width}">'
        new_content += f'<ri:attachment ri:filename="{html.escape(image_name)}" /></ac:image></p>\n'
    return new_content


def _render_panel_artifacts(panel, row_title: str, graph_width: int) -> str:
    new_content = ''
    if _has_matrix_artifacts(panel.artifacts):
        new_content += _render_matrix_artifacts(panel, row_title, graph_width)
    artifacts = [
        artifact for artifact in _ordered_artifacts(panel.artifacts)
        if artifact.get('artifact_type') != 'matrix'
        and _artifact_is_visible(artifact)
        and _artifact_has_rendered_png(artifact)
    ]
    repeated_artifacts = [artifact for artifact in artifacts if artifact.get('repeat_value') is not None]
    other_artifacts = [artifact for artifact in artifacts if artifact.get('repeat_value') is None]
    repeat_groups = _repeat_artifact_groups(repeated_artifacts)
    if len(repeat_groups) > 1:
        for repeat_value, grouped_artifacts in repeat_groups.items():
            new_content += '    <ac:structured-macro ac:name="expand">\n'
            new_content += (
                '      <ac:parameter ac:name="title">'
                f'{html.escape(normalize_grafana_display_value(repeat_value))}</ac:parameter>\n'
            )
            new_content += '      <ac:rich-text-body>\n'
            for artifact in grouped_artifacts:
                new_content += _render_artifact_image_block(panel, row_title, artifact, graph_width, '        ')
            new_content += '      </ac:rich-text-body>\n    </ac:structured-macro>\n'
        for artifact in other_artifacts:
            new_content += _render_artifact_image_block(panel, row_title, artifact, graph_width, '    ')
        return new_content
    for artifact in artifacts:
        new_content += _render_artifact_image_block(panel, row_title, artifact, graph_width, '    ')
    return new_content


def _repeat_artifact_groups(artifacts):
    if not artifacts:
        return {}
    grouped = {}
    for artifact in artifacts:
        grouped.setdefault(str(artifact['repeat_value']), []).append(artifact)
    return grouped


def _has_matrix_artifacts(artifacts) -> bool:
    return any(artifact.get('artifact_type') == 'matrix' for artifact in artifacts or [])


def _render_matrix_artifacts(panel, row_title: str, graph_width: int) -> str:
    new_content = ''
    for group_title, artifacts in _grouped_matrix_artifacts(panel.artifacts).items():
        new_content += '    <ac:structured-macro ac:name="expand">\n'
        new_content += f'      <ac:parameter ac:name="title">{html.escape(group_title)}</ac:parameter>\n'
        new_content += '      <ac:rich-text-body>\n'
        for artifact in artifacts:
            new_content += _render_artifact_image_block(panel, row_title, artifact, graph_width, '        ')
        new_content += '      </ac:rich-text-body>\n    </ac:structured-macro>\n'
    return new_content


def _render_artifact_image_block(panel, row_title: str, artifact, graph_width: int, indent: str) -> str:
    title = _artifact_title(panel, row_title, artifact)
    link = artifact.get('link') or _first_panel_link(panel)
    block = ''
    if link:
        block += f'{indent}<p><a href="{html.escape(link)}">{title}</a></p>\n'
    else:
        block += f'{indent}<p>{title} (Grafana link unavailable)</p>\n'
    block += f'{indent}<p><ac:image ac:width="{graph_width}">'
    block += f'<ri:attachment ri:filename="{html.escape(artifact["png_file"])}" /></ac:image></p>\n'
    return block


def _grouped_matrix_artifacts(artifacts):
    grouped = {}
    for artifact in _ordered_artifacts(artifacts):
        if artifact.get('artifact_type') != 'matrix':
            continue
        if not _artifact_is_visible(artifact) or not _artifact_has_rendered_png(artifact):
            continue
        group_title = ((artifact.get('matrix') or {}).get('group') or 'Matrix')
        grouped.setdefault(str(group_title), []).append(artifact)
    return grouped


def _artifact_has_rendered_png(artifact) -> bool:
    return artifact.get('render_status', 'rendered') == 'rendered' and bool(artifact.get('png_file'))


def _artifact_title(panel, row_title: str, artifact) -> str:
    if artifact.get('artifact_type') == 'matrix':
        if artifact.get('display_title'):
            return html.escape(str(artifact['display_title']))
        label = (artifact.get('matrix') or {}).get('label')
        return html.escape(str(label or row_title))
    if artifact.get('artifact_type') == 'variant':
        label = (artifact.get('variant') or {}).get('label')
        return html.escape(str(label or row_title))
    if artifact.get('artifact_type') == 'composite':
        composite = artifact.get('composite') or {}
        return html.escape(str(composite.get('title') or composite.get('name') or row_title))
    if artifact.get('repeat_value') is None:
        return _non_repeating_artifact_title(row_title, artifact)
    repeat_var = html.escape(str(artifact.get('repeat_var') or getattr(panel, 'repeat_var', 'value')))
    repeat_value = html.escape(normalize_grafana_display_value(artifact.get('repeat_value')))
    return f'{row_title} [{repeat_var}={repeat_value}]'


def _non_repeating_artifact_title(row_title: str, artifact) -> str:
    timestamp_tag = artifact.get('timestamp_tag')
    if timestamp_tag:
        return f'{row_title} [{html.escape(str(timestamp_tag))}]'
    return row_title


def _first_panel_link(panel) -> str:
    links = getattr(panel, 'links', [])
    return next((link for link in links if link), '')


def _ordered_panels(panels):
    return sorted(panels or [], key=lambda panel: getattr(panel, 'order_index', 0))


def _ordered_artifacts(artifacts):
    return sorted(
        artifacts or [],
        key=_artifact_order_key,
    )


def _artifact_order_key(artifact):
    if artifact.get('repeat_value') is not None:
        return 0, artifact.get('repeat_index', 0), artifact.get('order_index', 0)
    return 1, artifact.get('order_index', 0), 0


def _artifact_is_visible(artifact) -> bool:
    confluence = artifact.get('confluence') or {}
    return confluence.get('visible', True) is not False


def _dashboard_period(timestamp: GrafanaTimeBase, timestamps_count: int) -> str:
    if timestamps_count > 1:
        return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
    return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else ''


def _panel_period(timestamp: GrafanaTimeBase, timestamps_count: int, row_title: str) -> str:
    if timestamps_count > 1:
        return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
    return f'{row_title}'
