import logging
from typing import Dict, List, Optional, Pattern, Tuple

from grafconflux._shared.grafana_models import (
    INCLUDE_ONLY_SELECTED,
    PANEL_FILTERING_KEY,
    Panel,
    PanelDescriptor,
    PanelFilteringConfig,
    _SelectorConfig,
)

logger = logging.getLogger('grafconflux.grafana')


def extract_dashboard_panels(panels: List[Dict], include_collapsed_rows: bool = False,
                             row_title: Optional[str] = None, from_collapsed_row: bool = False,
                             row_id: Optional[int] = None, row_repeat: Optional[str] = None,
                             include_collapsed: Optional[bool] = None) -> List[Dict]:
    """Recursively extract Grafana dashboard panels without network or browser access."""
    if include_collapsed is not None:
        include_collapsed_rows = include_collapsed
    extracted_panels = []
    for panel in panels:
        if _skip_collapsed_panel(panel, include_collapsed_rows):
            continue
        extracted_panels.extend(_extract_panel_or_children(
            panel, include_collapsed_rows, row_title, from_collapsed_row, row_id, row_repeat))
    return extracted_panels


def apply_disabled_graph_type_filter(descriptors: List[PanelDescriptor], disabled_graph_types: List) -> List[PanelDescriptor]:
    return [descriptor for descriptor in descriptors if descriptor.graph_type not in disabled_graph_types]


def filter_runtime_repeat_clones(descriptors: List[PanelDescriptor], enable_repeating_panels: bool,
                                 repeating_panels: List[Dict]) -> List[PanelDescriptor]:
    if not _should_filter_runtime_repeat_clones(descriptors, enable_repeating_panels, repeating_panels):
        return descriptors
    return [descriptor for descriptor in descriptors if descriptor.repeat_panel_id is None]


def filter_panel_descriptors(config_name: str, filtering: PanelFilteringConfig,
                             descriptors: List[PanelDescriptor]) -> List[PanelDescriptor]:
    filtered_descriptors = []
    for descriptor in descriptors:
        if _is_descriptor_excluded(descriptor, filtering):
            continue
        if _requires_include_match(config_name, descriptor, filtering):
            continue
        filtered_descriptors.append(descriptor)
    return filtered_descriptors


def warn_unmatched_filter_selectors(config_name: str, filtering: PanelFilteringConfig,
                                    descriptors: List[PanelDescriptor]) -> None:
    for section_name, selector, scope in _filtering_warning_sections(filtering):
        _warn_unmatched_ids(config_name, descriptors, section_name, selector, scope)
        _warn_unmatched_titles(config_name, descriptors, section_name, selector, scope)
        _warn_unmatched_regexes(config_name, descriptors, section_name, selector, scope)


def panel_from_descriptor(descriptor: PanelDescriptor, timestamps_count: int, display_title: Optional[str] = None) -> Panel:
    return Panel(
        descriptor.panel_id,
        descriptor.graph_type,
        descriptor.title or '',
        timestamps_count,
        display_title=display_title,
        row_title=descriptor.row_title,
        from_collapsed_row=descriptor.from_collapsed_row,
        row_id=descriptor.row_id,
        grid_pos=descriptor.grid_pos,
    )


def _skip_collapsed_panel(panel: Dict, include_collapsed_rows: bool) -> bool:
    return not include_collapsed_rows and panel.get('collapsed') is True


def _extract_panel_or_children(panel: Dict, include_collapsed_rows: bool, row_title: Optional[str],
                               from_collapsed_row: bool, row_id: Optional[int],
                               row_repeat: Optional[str]) -> List[Dict]:
    if 'panels' not in panel:
        return [_panel_with_row_context(panel, row_title, from_collapsed_row, row_id, row_repeat)]
    return extract_dashboard_panels(
        panel['panels'], include_collapsed_rows, _panel_row_title(panel, row_title),
        from_collapsed_row or panel.get('collapsed') is True, _panel_row_id(panel, row_id),
        _panel_row_repeat(panel, row_repeat))


def _panel_row_title(panel: Dict, current_row_title: Optional[str]) -> Optional[str]:
    if panel.get('type') == 'row' or 'title' in panel:
        return panel.get('title', current_row_title)
    return current_row_title


def _panel_row_id(panel: Dict, current_row_id: Optional[int]) -> Optional[int]:
    if panel.get('type') == 'row' or 'title' in panel:
        return panel.get('id', current_row_id)
    return current_row_id


def _panel_row_repeat(panel: Dict, current_row_repeat: Optional[str]) -> Optional[str]:
    if panel.get('type') == 'row' or 'panels' in panel:
        return panel.get('repeat') or current_row_repeat
    return current_row_repeat


def _panel_with_row_context(panel: Dict, row_title: Optional[str], from_collapsed_row: bool,
                            row_id: Optional[int], row_repeat: Optional[str]) -> Dict:
    panel_with_context = dict(panel)
    panel_with_context['row_title'] = row_title
    panel_with_context['row_id'] = row_id
    panel_with_context['from_collapsed_row'] = from_collapsed_row
    panel_with_context['row_repeat'] = row_repeat
    return panel_with_context


def _should_filter_runtime_repeat_clones(descriptors: List[PanelDescriptor], enable_repeating_panels: bool,
                                         repeating_panels: List[Dict]) -> bool:
    return bool(enable_repeating_panels or repeating_panels or any(descriptor.effective_repeat() for descriptor in descriptors))


def _is_descriptor_excluded(descriptor: PanelDescriptor, filtering: PanelFilteringConfig) -> bool:
    reason = _selector_match_reason(descriptor, 'exclude_rows', filtering.exclude_rows, 'row')
    if reason:
        logger.info(f'Panel filtering excluded panel_id={descriptor.panel_id} reason={reason}')
        return True
    reason = _selector_match_reason(descriptor, 'exclude_panels', filtering.exclude_panels, 'panel')
    if reason:
        logger.info(f'Panel filtering excluded panel_id={descriptor.panel_id} reason={reason}')
        return True
    return False


def _requires_include_match(config_name: str, descriptor: PanelDescriptor, filtering: PanelFilteringConfig) -> bool:
    if filtering.mode != INCLUDE_ONLY_SELECTED:
        return False
    reason = _include_match_reason(descriptor, filtering)
    if reason is None:
        return True
    logger.info(f'Panel filtering included panel_id={descriptor.panel_id} row_title={descriptor.row_title} reason={reason}')
    return False


def _include_match_reason(descriptor: PanelDescriptor, filtering: PanelFilteringConfig) -> Optional[str]:
    return (_selector_match_reason(descriptor, 'include_panels', filtering.include_panels, 'panel')
            or _selector_match_reason(descriptor, 'include_rows', filtering.include_rows, 'row'))


def _selector_match_reason(descriptor: PanelDescriptor, section_name: str,
                           selector: _SelectorConfig, scope: str) -> Optional[str]:
    if scope == 'panel':
        return _panel_selector_match_reason(descriptor, section_name, selector)
    return _row_selector_match_reason(descriptor, section_name, selector)


def _panel_selector_match_reason(descriptor: PanelDescriptor, section_name: str,
                                 selector: _SelectorConfig) -> Optional[str]:
    if descriptor.panel_id in selector.ids:
        return f'{section_name}.ids'
    if descriptor.title and descriptor.title in selector.titles:
        return f'{section_name}.titles'
    if descriptor.title and any(title == descriptor.title and panel_type == descriptor.graph_type for title, panel_type in selector.typed_titles):
        return f'{section_name}.titles'
    if _matches_regex_selector(descriptor.title, selector.title_regex):
        return f'{section_name}.title_regex'
    return None


def _row_selector_match_reason(descriptor: PanelDescriptor, section_name: str,
                               selector: _SelectorConfig) -> Optional[str]:
    if descriptor.row_id is not None and descriptor.row_id in selector.ids:
        return f'{section_name}.ids'
    if descriptor.row_title is not None and descriptor.row_title in selector.titles:
        return f'{section_name}.titles'
    if _matches_regex_selector(descriptor.row_title, selector.title_regex):
        return f'{section_name}.title_regex'
    return None


def _matches_regex_selector(value: Optional[str], regexes: List[Pattern[str]]) -> bool:
    if value is None:
        return False
    return any(regex.search(value) for regex in regexes)


def _filtering_warning_sections(filtering: PanelFilteringConfig) -> List[Tuple[str, _SelectorConfig, str]]:
    return [
        ('include_panels', filtering.include_panels, 'panel'),
        ('exclude_panels', filtering.exclude_panels, 'panel'),
        ('include_rows', filtering.include_rows, 'row'),
        ('exclude_rows', filtering.exclude_rows, 'row'),
    ]


def _warn_unmatched_ids(config_name: str, descriptors: List[PanelDescriptor], section_name: str,
                        selector: _SelectorConfig, scope: str) -> None:
    for index, selector_id in enumerate(selector.ids):
        count = _selector_match_count(descriptors, scope, 'ids', selector_id)
        _warn_if_selector_matched_nothing(config_name, descriptors, scope, section_name, f'ids[{index}]', selector_id, count)


def _warn_unmatched_titles(config_name: str, descriptors: List[PanelDescriptor], section_name: str,
                           selector: _SelectorConfig, scope: str) -> None:
    for index, title in enumerate(selector.titles):
        count = _selector_match_count(descriptors, scope, 'titles', title)
        _warn_if_selector_matched_nothing(config_name, descriptors, scope, section_name, f'titles[{index}]', title, count)
        _warn_if_panel_title_matched_multiple(section_name, title, count, scope)
    for index, typed_title in enumerate(selector.typed_titles, start=len(selector.titles)):
        count = _selector_match_count(descriptors, scope, 'typed_titles', typed_title)
        _warn_if_selector_matched_nothing(config_name, descriptors, scope, section_name, f'titles[{index}]', {typed_title[0]: typed_title[1]}, count)


def _warn_unmatched_regexes(config_name: str, descriptors: List[PanelDescriptor], section_name: str,
                            selector: _SelectorConfig, scope: str) -> None:
    for index, regex_value in enumerate(selector.title_regex_values):
        count = _selector_match_count(descriptors, scope, 'title_regex', selector.title_regex[index])
        _warn_if_selector_matched_nothing(
            config_name, descriptors, scope, section_name, f'title_regex[{index}]', regex_value, count)


def _selector_match_count(descriptors: List[PanelDescriptor], scope: str, selector_type: str, value) -> int:
    return sum(1 for descriptor in descriptors if _descriptor_matches_selector_value(descriptor, scope, selector_type, value))


def _descriptor_matches_selector_value(descriptor: PanelDescriptor, scope: str, selector_type: str, value) -> bool:
    if scope == 'panel':
        return _panel_value_matches(descriptor, selector_type, value)
    return _row_value_matches(descriptor, selector_type, value)


def _panel_value_matches(descriptor: PanelDescriptor, selector_type: str, value) -> bool:
    if selector_type == 'ids':
        return descriptor.panel_id == value
    if selector_type == 'titles':
        return bool(descriptor.title) and descriptor.title == value
    if selector_type == 'typed_titles':
        return bool(descriptor.title) and (descriptor.title, descriptor.graph_type) == value
    return bool(descriptor.title) and bool(value.search(descriptor.title))


def _row_value_matches(descriptor: PanelDescriptor, selector_type: str, value) -> bool:
    if selector_type == 'ids':
        return descriptor.row_id == value
    if selector_type == 'titles':
        return descriptor.row_title == value
    return descriptor.row_title is not None and bool(value.search(descriptor.row_title))


def _warn_if_selector_matched_nothing(config_name: str, descriptors: List[PanelDescriptor], scope: str,
                                      section_name: str, selector_path: str, value, count: int) -> None:
    if count > 0:
        return
    logger.warning(
        f'Panel filtering selector matched_nothing path=dashboards.{config_name}.'
        f'{PANEL_FILTERING_KEY}.{section_name}.{selector_path} value="{value}" '
        f'{_available_selector_context(descriptors, scope)}'
    )


def _available_selector_context(descriptors: List[PanelDescriptor], scope: str) -> str:
    if scope == 'panel':
        return f'available_panel_ids={[descriptor.panel_id for descriptor in descriptors]}'
    row_ids = sorted({descriptor.row_id for descriptor in descriptors if descriptor.row_id is not None})
    row_titles = sorted({descriptor.row_title for descriptor in descriptors if descriptor.row_title is not None})
    return f'available_row_ids={row_ids} available_row_titles={row_titles}'


def _warn_if_panel_title_matched_multiple(section_name: str, title: str, count: int, scope: str) -> None:
    if scope != 'panel' or count <= 1:
        return
    logger.warning(f'Panel filtering title selector matched_multiple section={section_name} title="{title}" count={count}')
