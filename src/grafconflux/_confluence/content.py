import html
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from typing import List, Optional

from grafconflux._shared.display import normalize_grafana_display_value
from grafconflux._shared.grafana_models import GrafanaConfigBase
from grafconflux._shared.time import GrafanaTimeBase

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

__all__ = (
    'GRAPHS_PLACEHOLDER',
    '_artifact_has_rendered_png',
    '_artifact_title',
    '_dashboard_period',
    '_first_panel_link',
    '_non_repeating_artifact_title',
    '_panel_period',
    '_render_dashboard_links',
    '_render_backup_dashboard_links',
    '_render_dashboards_section',
    '_render_panel_artifacts',
    '_render_panel_timestamps',
    '_render_panels',
    '_render_snapshot_backup_section',
    '_render_test_times_section',
    'apply_graphs_placeholder',
    'apply_graphs_placeholder_if_present',
    'build_child_page_title',
    'build_confluence_storage_content',
    'build_parent_include_content',
    'sanitize_confluence_page_title',
    'ChildPageInclude',
    'DEFAULT_CHILD_TITLE_PREFIX',
)


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
    new_content = ''
    if snapshot_list:
        new_content += _render_snapshot_backup_section(snapshot_list)
    new_content += _render_test_times_section(timestamps)
    new_content += _render_dashboards_section(grafana_configs, timestamps, graph_width)
    return new_content


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


def _render_test_times_section(timestamps: List[GrafanaTimeBase]) -> str:
    new_content = '<ac:structured-macro ac:name="expand">\n'
    new_content += '  <ac:parameter ac:name="title">Test times</ac:parameter>\n'
    new_content += '  <ac:rich-text-body>\n'
    new_content += '<table>\n  <tbody>\n    <tr>\n'
    new_content += '      <th>Test tag</th>\n      <th>Start test time</th>\n'
    new_content += '      <th>End test time</th>\n    </tr>\n'
    for timestamp in timestamps:
        new_content += '    <tr>\n'
        new_content += f'      <td>{html.escape(str(timestamp.time_tag))}</td>\n'
        new_content += f'      <td>{html.escape(str(timestamp.start_time_human))}</td>\n'
        new_content += f'      <td>{html.escape(str(timestamp.end_time_human))}</td>\n'
        new_content += '    </tr>\n'
    new_content += '  </tbody>\n</table>\n'
    new_content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return new_content


def _render_dashboards_section(grafana_configs: List[GrafanaConfigBase], timestamps: List[GrafanaTimeBase],
                               graph_width: int) -> str:
    new_content = ''
    for grafana_config in grafana_configs:
        dash_title = html.escape(grafana_config.name)
        new_content += f'<h2>{dash_title}</h2>\n<p>Dashboard links</p>\n'
        new_content += _render_dashboard_links(grafana_config, timestamps)
        if getattr(grafana_config, 'backup_dashboard_links', []):
            new_content += '<p>Backup dashboard links</p>\n'
            new_content += _render_backup_dashboard_links(grafana_config, timestamps)
        new_content += '<p>Panels</p>\n'
        new_content += f'<ac:structured-macro ac:name="expand">\n'
        new_content += f'  <ac:parameter ac:name="title">{dash_title}</ac:parameter>\n'
        new_content += '  <ac:rich-text-body>\n'
        new_content += _render_panels(grafana_config, timestamps, graph_width)
        new_content += '  </ac:rich-text-body>\n</ac:structured-macro>\n'
    return new_content


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
    new_content = ''
    for panel in grafana_config.panels:
        row_title = html.escape(getattr(panel, 'display_title', panel.title))
        new_content += f'<h3>{row_title}</h3>\n'
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
    for artifact in panel.artifacts:
        if not _artifact_has_rendered_png(artifact):
            continue
        title = _artifact_title(panel, row_title, artifact)
        link = html.escape(artifact.get('link') or _first_panel_link(panel))
        new_content += f'    <h4>{title}</h4>\n'
        new_content += f'    <p><a href="{link}">{title}</a></p>\n'
        new_content += f'    <p><ac:image ac:width="{graph_width}">'
        new_content += f'<ri:attachment ri:filename="{html.escape(artifact["png_file"])}" /></ac:image></p>\n'
    return new_content


def _artifact_has_rendered_png(artifact) -> bool:
    return artifact.get('render_status', 'rendered') == 'rendered' and bool(artifact.get('png_file'))


def _artifact_title(panel, row_title: str, artifact) -> str:
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


def _dashboard_period(timestamp: GrafanaTimeBase, timestamps_count: int) -> str:
    if timestamps_count > 1:
        return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
    return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else ''


def _panel_period(timestamp: GrafanaTimeBase, timestamps_count: int, row_title: str) -> str:
    if timestamps_count > 1:
        return f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
    return f'{row_title}'
