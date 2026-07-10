import logging, re
from typing import Any, Dict, List, Optional, Pattern, Tuple
from urllib.parse import quote
from grafconflux._shared.time import GrafanaTimeDownloader
from grafconflux._shared.grafana_models import (
    ALL_REPEAT_SENTINELS,
    COLLECT_NO_DATA_PANELS_KEY,
    PROMETHEUS_DATASOURCE_TYPE,
    REPEAT_VALUE_MODES,
    REPEAT_VALUES_KEY,
    REPEATING_PANELS_KEY,
    ConfigurationError,
    Panel,
    PanelDescriptor,
    PanelRenderTask,
)
from grafconflux._grafana.repeat_tasks import RepeatTaskBuilder
logger = logging.getLogger('grafconflux.grafana')
def is_unresolved_repeating_rule(rule: Optional[Dict[str, Any]]) -> bool: return rule is not None and not rule.get('values')
class RepeatingPlanner:
    def __init__(self, config, session): self.config = config; self.session = session
    def resolve_repeating_rules(self, dashboard: Dict, descriptors: List[PanelDescriptor],
                                timestamps: List[GrafanaTimeDownloader]) -> Dict[int, Dict[str, Any]]:
        resolved_rules = {}
        for index, rule in enumerate(self.config.repeating_panels):
            self._add_repeating_rule(resolved_rules, dashboard, descriptors, timestamps, index, rule)
        self._add_auto_repeating_rules(resolved_rules, dashboard, descriptors, timestamps)
        return resolved_rules
    def _add_repeating_rule(self, resolved_rules: Dict[int, Dict[str, Any]], dashboard: Dict,
                            descriptors: List[PanelDescriptor], timestamps: List[GrafanaTimeDownloader],
                            index: int, rule: Dict) -> None:
        path = f'dashboards.{self.config.name}.{REPEATING_PANELS_KEY}[{index}]'
        descriptor = self._resolve_repeating_rule_source(descriptors, rule, path)
        self._validate_repeating_descriptor(descriptor, rule, path)
        if descriptor.panel_id in resolved_rules:
            self._raise_repeating_error(path, descriptor.panel_id, 'one rule per source panel')
        if self._has_unsupported_multi_repeat(descriptor):
            raise ConfigurationError(self._multi_repeat_message(descriptor, path))
        repeat_var = descriptor.effective_repeat()
        values_by_timestamp = self._resolve_repeat_values_by_timestamp(
            dashboard, repeat_var, rule, path, descriptor.panel_id, timestamps)
        values = self._flatten_timestamp_values(values_by_timestamp)
        self._log_repeating_resolution(descriptor.panel_id, repeat_var, values)
        resolved_rules[descriptor.panel_id] = {
            'repeat_var': repeat_var,
            'values': values,
            'values_by_timestamp': values_by_timestamp,
            COLLECT_NO_DATA_PANELS_KEY: self._repeating_collect_no_data(rule, path),
        }
    def _add_auto_repeating_rules(self, resolved_rules: Dict[int, Dict[str, Any]], dashboard: Dict,
                                  descriptors: List[PanelDescriptor],
                                  timestamps: List[GrafanaTimeDownloader]) -> None:
        for descriptor in descriptors:
            if self._has_unsupported_multi_repeat(descriptor):
                self._skip_multi_repeat_auto_rule(resolved_rules, descriptor)
                continue
            repeat_var = descriptor.effective_repeat()
            if not repeat_var or descriptor.panel_id in resolved_rules:
                continue
            self._add_auto_repeating_rule(resolved_rules, dashboard, descriptor, timestamps, repeat_var)
    def _add_auto_repeating_rule(self, resolved_rules: Dict[int, Dict[str, Any]], dashboard: Dict,
                                 descriptor: PanelDescriptor, timestamps: List[GrafanaTimeDownloader],
                                 repeat_var: str) -> None:
        path = f'dashboards.{self.config.name}.auto_repeating_panels.panel_id={descriptor.panel_id}'
        rule: Dict[str, Any] = {}
        values_by_timestamp = self._resolve_repeat_values_by_timestamp(
            dashboard, repeat_var, rule, path, descriptor.panel_id, timestamps)
        values = self._flatten_timestamp_values(values_by_timestamp)
        self._log_repeating_resolution(descriptor.panel_id, repeat_var, values)
        resolved_rules[descriptor.panel_id] = {
            'repeat_var': repeat_var,
            'values': values,
            'values_by_timestamp': values_by_timestamp,
            COLLECT_NO_DATA_PANELS_KEY: None,
        }
    def _skip_multi_repeat_auto_rule(self, resolved_rules: Dict[int, Dict[str, Any]],
                                     descriptor: PanelDescriptor) -> None:
        logger.warning(self._multi_repeat_message(
            descriptor, f'dashboards.{self.config.name}.auto_repeating_panels.panel_id={descriptor.panel_id}'))
        resolved_rules[descriptor.panel_id] = {
            'repeat_var': descriptor.effective_repeat(),
            'values': [],
            'values_by_timestamp': {},
            COLLECT_NO_DATA_PANELS_KEY: None,
        }
    @staticmethod
    def _has_unsupported_multi_repeat(descriptor: PanelDescriptor) -> bool: return bool(descriptor.repeat and descriptor.row_repeat and descriptor.repeat != descriptor.row_repeat)
    @staticmethod
    def _multi_repeat_message(descriptor: PanelDescriptor, path: str) -> str:
        return (
            f'{path}: panel_id={descriptor.panel_id} reason=multi_variable_repeat_unsupported '
            f'panel_repeat={descriptor.repeat} row_repeat={descriptor.row_repeat}'
        )
    @staticmethod
    def _repeating_collect_no_data(rule: Dict, path: str) -> Optional[bool]:
        value = rule.get(COLLECT_NO_DATA_PANELS_KEY)
        if value is None or isinstance(value, bool):
            return value
        raise ConfigurationError(
            f'{path}.{COLLECT_NO_DATA_PANELS_KEY}: invalid value="{value}", '
            'expected bool, suggested fix: set true or false'
        )
    @staticmethod
    def _log_repeating_resolution(panel_id: int, repeat_var: str, values: List[str]) -> None:
        if values:
            logger.info(f'Resolved repeating panel panel_id={panel_id} repeat_var={repeat_var} values={len(values)}')
            return
        logger.warning(
            f'Skipping repeating panel panel_id={panel_id} repeat_var={repeat_var} '
            'reason=variable_values_unresolved'
        )
    def _resolve_repeating_rule_source(self, descriptors: List[PanelDescriptor], rule: Dict,
                                       path: str) -> PanelDescriptor:
        selector_name, selector_value = self._repeating_rule_selector(rule, path)
        matches = [descriptor for descriptor in descriptors
                   if self._repeating_selector_matches(descriptor, selector_name, selector_value)]
        if len(matches) != 1:
            raise ConfigurationError(
                f'{path}: selector {self._format_repeating_selector(selector_name, selector_value)} '
                f'resolved {len(matches)} panels, expected exactly 1 source panel, '
                f'available source panels: {self._format_source_panels(descriptors)}, '
                'suggested fix: verify selector against dashboard JSON'
            )
        return matches[0]
    @staticmethod
    def _format_repeating_selector(selector_name: str, selector_value: Any) -> str:
        value = selector_value.pattern if hasattr(selector_value, 'pattern') else selector_value
        if isinstance(value, str):
            return f'{selector_name}="{value}"'
        return f'{selector_name}={value}'
    @staticmethod
    def _format_source_panels(descriptors: List[PanelDescriptor]) -> str:
        if not descriptors:
            return '[]'
        return '; '.join(
            f'panel_id={descriptor.panel_id} title="{descriptor.title}"'
            for descriptor in descriptors
        )
    def _repeating_rule_selector(self, rule: Dict, path: str) -> Tuple[str, Any]:
        selectors = [key for key in ('panel_id', 'title', 'title_regex') if key in rule]
        if len(selectors) != 1:
            self._raise_repeating_error(path, selectors, 'exactly one selector panel_id, title, or title_regex')
        return selectors[0], self._validated_repeating_selector_value(rule, selectors[0], path)
    def _validated_repeating_selector_value(self, rule: Dict, selector_name: str, path: str) -> Any:
        value = rule[selector_name]
        if selector_name == 'panel_id' and isinstance(value, int) and not isinstance(value, bool):
            return value
        if selector_name == 'title' and isinstance(value, str) and value:
            return value
        if selector_name == 'title_regex' and isinstance(value, str) and value:
            return self._compile_repeating_regex(value, f'{path}.title_regex')
        self._raise_repeating_error(f'{path}.{selector_name}', value, 'valid selector value')
    @staticmethod
    def _repeating_selector_matches(descriptor: PanelDescriptor, selector_name: str, value: Any) -> bool:
        if selector_name == 'panel_id':
            return descriptor.panel_id == value
        if selector_name == 'title':
            return descriptor.title == value
        return bool(descriptor.title) and bool(value.search(descriptor.title))
    def _validate_repeating_descriptor(self, descriptor: PanelDescriptor, rule: Dict, path: str) -> None:
        repeat_var = descriptor.effective_repeat()
        if not repeat_var:
            raise ConfigurationError(
                f'{path}: panel_id={descriptor.panel_id} is not a repeating panel, expected dashboard panel.repeat '
                'to be non-empty, suggested fix: remove repeating rule or select a panel with repeat'
            )
        if rule.get('repeat_var') not in (None, repeat_var):
            self._raise_repeating_error(f'{path}.repeat_var', rule.get('repeat_var'), f'dashboard panel repeat="{repeat_var}"')
    def _resolve_repeat_values(self, dashboard: Dict, repeat_var: str, rule: Dict, path: str,
                               panel_id: int, timestamps: List[GrafanaTimeDownloader]) -> List[str]:
        values = self._repeat_values_from_rule(dashboard, repeat_var, rule, path, panel_id, timestamps)
        values = self._dedupe_values(values)
        self._validate_max_values(rule.get('max_values'), values, path)
        return values
    def _resolve_repeat_values_by_timestamp(self, dashboard: Dict, repeat_var: str, rule: Dict, path: str,
                                            panel_id: int, timestamps: List[GrafanaTimeDownloader]) -> Dict[int, List[str]]:
        if not timestamps:
            return {}
        if REPEAT_VALUES_KEY in rule and rule.get(REPEAT_VALUES_KEY) is not None:
            values = self._resolve_repeat_values(dashboard, repeat_var, rule, path, panel_id, timestamps)
            return {timestamp.id_time: values for timestamp in timestamps}
        return {
            timestamp.id_time: self._resolve_repeat_values(dashboard, repeat_var, rule, path, panel_id, [timestamp])
            for timestamp in timestamps
        }
    def _flatten_timestamp_values(self, values_by_timestamp: Dict[int, List[str]]) -> List[str]:
        return self._dedupe_values([value for values in values_by_timestamp.values() for value in values])
    def _repeat_values_from_rule(self, dashboard: Dict, repeat_var: str, rule: Dict, path: str,
                                 panel_id: int, timestamps: List[GrafanaTimeDownloader]) -> List[str]:
        if REPEAT_VALUES_KEY not in rule or rule.get(REPEAT_VALUES_KEY) is None:
            return self._fallback_repeat_values(dashboard, repeat_var, self._first_timestamp(timestamps))
        repeat_values = self._validated_repeat_values_mapping(rule[REPEAT_VALUES_KEY], path)
        mode = self._validated_repeat_values_mode(repeat_values, path)
        if mode == 'manual':
            return self._manual_repeat_values(repeat_values, path)
        return self._discovered_repeat_values(dashboard, repeat_var, repeat_values, mode, path, panel_id)
    @staticmethod
    def _first_timestamp(timestamps: List[GrafanaTimeDownloader]) -> Optional[GrafanaTimeDownloader]: return timestamps[0] if timestamps else None
    def _validated_repeat_values_mapping(self, repeat_values: Any, path: str) -> Dict:
        if isinstance(repeat_values, dict):
            return repeat_values
        self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}', repeat_values, 'mapping with mode')
    def _validated_repeat_values_mode(self, repeat_values: Dict, path: str) -> str:
        mode = repeat_values.get('mode')
        if mode in REPEAT_VALUE_MODES:
            return mode
        self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}.mode', mode, f'one of {REPEAT_VALUE_MODES}')
    def _manual_repeat_values(self, repeat_values: Dict, path: str) -> List[str]:
        values = repeat_values.get('values')
        if not isinstance(values, list) or not values:
            self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}.values', values, 'non-empty list[str]')
        return [self._validated_manual_value(value, index, path) for index, value in enumerate(values)]
    def _validated_manual_value(self, value: Any, index: int, path: str) -> str:
        if isinstance(value, str) and value:
            return value
        self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}.values[{index}]', value, 'non-empty string')
    def _discovered_repeat_values(self, dashboard: Dict, repeat_var: str, repeat_values: Dict,
                                  mode: str, path: str, panel_id: int) -> List[str]:
        values = self._templating_option_values(dashboard, repeat_var, panel_id)
        if mode == 'all':
            return values
        patterns = self._compile_repeat_value_patterns(repeat_values.get('regex'), path)
        return [value for value in values if any(pattern.search(value) for pattern in patterns)]
    def _compile_repeat_value_patterns(self, regex_config: Any, path: str) -> List[Pattern[str]]:
        regex_values = self._repeat_regex_values(regex_config, path)
        return [self._compile_repeating_regex(value, f'{path}.{REPEAT_VALUES_KEY}.regex{suffix}')
                for value, suffix in regex_values]
    def _repeat_regex_values(self, regex_config: Any, path: str) -> List[Tuple[str, str]]:
        if isinstance(regex_config, str) and regex_config:
            return [(regex_config, '')]
        if isinstance(regex_config, list) and regex_config:
            return self._validated_regex_list(regex_config, path)
        self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}.regex', regex_config, 'non-empty list[str] or str')
    def _validated_regex_list(self, regex_config: List, path: str) -> List[Tuple[str, str]]:
        values = []
        for index, value in enumerate(regex_config):
            if not isinstance(value, str) or not value:
                self._raise_repeating_error(f'{path}.{REPEAT_VALUES_KEY}.regex[{index}]', value, 'non-empty string')
            values.append((value, f'[{index}]'))
        return values
    def _fallback_repeat_values(self, dashboard: Dict, repeat_var: str,
                                timestamp: Optional[GrafanaTimeDownloader]) -> List[str]:
        if self.config.vars and repeat_var in self.config.vars:
            return self._normalize_auto_values(self.config.vars[repeat_var])
        return self._current_or_default_repeat_values(dashboard, repeat_var, timestamp)
    def _current_or_default_repeat_values(self, dashboard: Dict, repeat_var: str,
                                          timestamp: Optional[GrafanaTimeDownloader]) -> List[str]:
        variable = self._templating_variable(dashboard, repeat_var)
        if not variable:
            return []
        value = variable.get('current', {}).get('value', variable.get('default'))
        values = self._normalize_auto_values(value)
        if values and self._should_discover_query_values(variable, value):
            discovered_values = self._prometheus_query_variable_values(variable, repeat_var, timestamp, dashboard)
            if discovered_values:
                return discovered_values
        if values:
            return values
        if self._has_no_option_values(variable):
            return self._prometheus_query_variable_values(variable, repeat_var, timestamp, dashboard)
        return []
    def _templating_option_values(self, dashboard: Dict, repeat_var: str, panel_id: int) -> List[str]:
        variable = self._templating_variable(dashboard, repeat_var)
        options = variable.get('options', []) if variable else []
        return [value for option in options for value in self._option_values(option, repeat_var, panel_id)]
    def _should_discover_query_values(self, variable: Dict[str, Any], value: Any) -> bool:
        return self._has_no_option_values(variable) and self._is_all_repeat_value(value)
    def _has_no_option_values(self, variable: Dict[str, Any]) -> bool:
        options = variable.get('options', [])
        values = []
        for option in options:
            option_value = option.get('value') if isinstance(option, dict) else None
            values.extend(self._normalize_auto_values(option_value))
        return not [value for value in values if value.lower() not in ALL_REPEAT_SENTINELS]
    def _is_all_repeat_value(self, value: Any) -> bool:
        values = self._normalize_auto_values(value)
        return bool(values) and all(item.lower() in ALL_REPEAT_SENTINELS for item in values)
    def _prometheus_query_variable_values(self, variable: Dict[str, Any], repeat_var: str,
                                          timestamp: Optional[GrafanaTimeDownloader], dashboard: Dict) -> List[str]:
        query = self._prometheus_label_values_query(variable, dashboard)
        if query is None:
            return []
        try:
            response = self.session.get(
                self._prometheus_label_values_url(variable, query[1], dashboard),
                params=self._prometheus_label_values_params(query[0], timestamp),
                timeout=self.config.timeout,
            )
            return self._prometheus_values_from_response(response, repeat_var)
        except Exception as error:
            logger.warning(f'Prometheus repeat values discovery failed repeat_var={repeat_var} error={error}')
            return []
    def _prometheus_label_values_url(self, variable: Dict[str, Any], label: str, dashboard: Dict) -> str:
        datasource_uid = self._resolved_datasource_type_uid(variable.get('datasource'), dashboard)[1]
        uid_path = quote(str(datasource_uid), safe='')
        label_path = quote(label, safe='')
        return f'{self.config.grafana_base_url}/api/datasources/proxy/uid/{uid_path}/api/v1/label/{label_path}/values'
    @staticmethod
    def _prometheus_label_values_params(metric: Optional[str],
                                        timestamp: Optional[GrafanaTimeDownloader]) -> Dict[str, str]:
        params = {'match[]': metric} if metric else {}
        if timestamp is not None:
            params.update({
                'start': str(timestamp.start_time_timestamp),
                'end': str(timestamp.end_time_timestamp),
            })
        return params
    def _prometheus_values_from_response(self, response, repeat_var: str) -> List[str]:
        if response.status_code != 200:
            logger.warning(f'Prometheus repeat values discovery failed repeat_var={repeat_var} status={response.status_code}')
            return []
        data = response.json().get('data', [])
        return [value for value in self._normalize_auto_values(data) if value.lower() not in ALL_REPEAT_SENTINELS]
    def _prometheus_label_values_query(self, variable: Dict[str, Any], dashboard: Dict) -> Optional[Tuple[Optional[str], str]]:
        if not self._is_prometheus_query_variable(variable, dashboard):
            return None
        query = self._variable_query_text(variable.get('query'))
        if not query:
            return None
        return self._parse_prometheus_label_values_query(query)
    def _is_prometheus_query_variable(self, variable: Dict[str, Any], dashboard: Dict) -> bool:
        datasource_type, datasource_uid = self._resolved_datasource_type_uid(variable.get('datasource'), dashboard)
        return (variable.get('type') == 'query' and str(datasource_type).lower() == PROMETHEUS_DATASOURCE_TYPE
                and bool(datasource_uid))

    def _resolved_datasource_type_uid(self, datasource: Any, dashboard: Dict) -> Tuple[Optional[str], Optional[str]]:
        datasource_type, datasource_uid = _datasource_type_uid(datasource)
        ref_name = self._datasource_ref_name(datasource_type, datasource_uid)
        if not ref_name:
            return datasource_type, datasource_uid
        variable = self._templating_variable(dashboard, ref_name)
        resolved_type = self._datasource_variable_type(variable) or self._resolved_config_var(datasource_type)
        resolved_uid = self._resolved_config_var(datasource_uid)
        return resolved_type, resolved_uid

    def _datasource_ref_name(self, datasource_type: Any, datasource_uid: Any) -> Optional[str]:
        datasource_vars = getattr(self.config, 'datasource_vars', {}) or {}
        for value in (datasource_uid, datasource_type):
            ref_name = self._variable_reference_name(value)
            if ref_name in datasource_vars:
                return ref_name
        return None

    def _resolved_config_var(self, value: Any) -> Optional[str]:
        ref_name = self._variable_reference_name(value)
        if ref_name and ref_name in (self.config.vars or {}):
            return str(self.config.vars[ref_name])
        return str(value) if value not in (None, '') else None

    @staticmethod
    def _variable_reference_name(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        match = re.fullmatch(r'\$\{([^}]+)}|\$(\w+)', value)
        return (match.group(1) or match.group(2)) if match else None

    @staticmethod
    def _datasource_variable_type(variable: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(variable, dict) or variable.get('type') != 'datasource':
            return None
        query = variable.get('query')
        if isinstance(query, str) and query:
            return query
        return query.get('type') if isinstance(query, dict) else None
    @staticmethod
    def _variable_query_text(query_config: Any) -> Optional[str]:
        if isinstance(query_config, str):
            return query_config
        if isinstance(query_config, dict) and isinstance(query_config.get('query'), str):
            return query_config['query']
        return None
    def _parse_prometheus_label_values_query(self, query: str) -> Optional[Tuple[Optional[str], str]]:
        if not query.strip().startswith('label_values(') or not query.strip().endswith(')'):
            return None
        inner_query = query.strip()[len('label_values('):-1].strip()
        metric, label = self._split_label_values_args(inner_query)
        if self._is_safe_prometheus_label(label) and self._is_safe_prometheus_match(metric):
            return metric, label
        return None
    @staticmethod
    def _split_label_values_args(inner_query: str) -> Tuple[Optional[str], str]:
        if ',' not in inner_query:
            return None, inner_query.strip()
        metric, label = inner_query.rsplit(',', 1)
        return metric.strip(), label.strip()
    @staticmethod
    def _is_safe_prometheus_label(label: str) -> bool:
        return bool(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', label or ''))
    @staticmethod
    def _is_safe_prometheus_match(metric: Optional[str]) -> bool:
        if metric is None:
            return True
        return 0 < len(metric) <= 300 and not re.search(r'[$()\r\n]', metric)
    def _option_values(self, option: Dict, repeat_var: str, panel_id: int) -> List[str]:
        option_value = option.get('value')
        if self._is_unsupported_option_value(option_value):
            self._warn_unsupported_option_value(panel_id, repeat_var, option_value)
            return []
        values = self._normalize_auto_values(option_value)
        return [value for value in values if value.lower() not in ALL_REPEAT_SENTINELS]
    @staticmethod
    def _is_unsupported_option_value(option_value: Any) -> bool: return isinstance(option_value, (dict, list, tuple, set))
    @staticmethod
    def _warn_unsupported_option_value(panel_id: int, repeat_var: str, option_value: Any) -> None:
        logger.warning(
            f'Skipping unsupported non-scalar repeat option panel_id={panel_id} '
            f'repeat_var={repeat_var} value_type={type(option_value).__name__}'
        )
    @staticmethod
    def _templating_variable(dashboard: Dict, repeat_var: str) -> Optional[Dict]:
        variables = dashboard.get('templating', {}).get('list', [])
        return next((variable for variable in variables if variable.get('name') == repeat_var), None)
    def _normalize_auto_values(self, value: Any) -> List[str]:
        if isinstance(value, list):
            return [item for raw_item in value for item in self._normalize_auto_values(raw_item)]
        if value is None or isinstance(value, (dict, tuple, set)):
            return []
        return [str(value)]
    @staticmethod
    def _dedupe_values(values: List[str]) -> List[str]: return list(dict.fromkeys(values))
    def _validate_max_values(self, max_values: Any, values: List[str], path: str) -> None:
        if max_values is None:
            return
        if not isinstance(max_values, int) or isinstance(max_values, bool) or max_values <= 0:
            self._raise_repeating_error(f'{path}.max_values', max_values, 'positive integer')
        if len(values) > max_values:
            raise ConfigurationError(
                f'{path}: resolved {len(values)} values, expected <= max_values={max_values}, '
                'suggested fix: narrow regex or lower source value set'
            )
    def append_panel_tasks(self, render_tasks: List[PanelRenderTask], panel: Panel, descriptor: PanelDescriptor,
                           timestamps: List[GrafanaTimeDownloader], rule: Optional[Dict[str, Any]]) -> None:
        RepeatTaskBuilder(self.config).append_panel_tasks(render_tasks, panel, descriptor, timestamps, rule)
    def _compile_repeating_regex(self, regex_value: str, path: str) -> Pattern[str]:
        try:
            return re.compile(regex_value)
        except re.error as error:
            raise ConfigurationError(
                f'{path}: invalid value="{regex_value}", expected valid regex pattern, '
                f'suggested fix: correct or remove the broken regex entry ({error})'
            ) from error
    @staticmethod
    def _raise_repeating_error(path: str, value: Any, expected: str) -> None:
        raise ConfigurationError(
            f'{path}: invalid value="{value}", expected {expected}, suggested fix: update repeating panel config'
        )
def _datasource_type_uid(datasource: Any) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(datasource, dict):
        return datasource.get('type'), datasource.get('uid')
    if isinstance(datasource, str):
        return datasource, datasource
    return None, None
