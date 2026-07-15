import hashlib
import re
from typing import Any, Dict, List, Optional

from grafconflux._shared.time import GrafanaTimeDownloader
from grafconflux._shared.grafana_models import (
    COLLECT_NO_DATA_PANELS_KEY,
    Panel,
    PanelDescriptor,
    PanelRenderTask,
)


class RepeatTaskBuilder:
    def __init__(self, config):
        self.config = config

    def append_panel_tasks(self, render_tasks: List[PanelRenderTask], panel: Panel, descriptor: PanelDescriptor,
                           timestamps: List[GrafanaTimeDownloader], rule: Optional[Dict[str, Any]]) -> None:
        if rule is None:
            self._append_normal_panel_tasks(render_tasks, panel, descriptor, timestamps)
            return
        self._append_repeating_panel_tasks(render_tasks, panel, descriptor, timestamps, rule)

    def _append_normal_panel_tasks(self, render_tasks: List[PanelRenderTask], panel: Panel,
                                   descriptor: PanelDescriptor,
                                   timestamps: List[GrafanaTimeDownloader]) -> None:
        for timestamp in timestamps:
            file_name = f'{self.config.name}__{panel.panel_id}__{timestamp.id_time}.png'
            artifact = self._normal_artifact(panel, timestamp, file_name)
            render_tasks.append(PanelRenderTask(
                panel, timestamp, self.config.vars, file_name, artifact,
                raw_panel=descriptor.raw_panel, collect_no_data_panels=self.config.collect_no_data_panels))

    def _normal_artifact(self, panel: Panel, timestamp: GrafanaTimeDownloader,
                         file_name: str) -> Optional[Dict[str, Any]]:
        if self.config.collect_no_data_panels:
            return None
        artifact = self._base_artifact(timestamp, file_name)
        panel.artifacts.append(artifact)
        return artifact

    def _append_repeating_panel_tasks(self, render_tasks: List[PanelRenderTask], panel: Panel,
                                      descriptor: PanelDescriptor, timestamps: List[GrafanaTimeDownloader],
                                      rule: Dict[str, Any]) -> None:
        self._mark_repeating_panel(panel, descriptor, rule['repeat_var'])
        for timestamp in timestamps:
            values = self._timestamp_repeat_values(rule, timestamp)
            slugs = self._repeat_value_slugs(values)
            for repeat_index, (repeat_value, repeat_slug) in enumerate(zip(values, slugs)):
                self._append_repeating_task(
                    render_tasks,
                    panel,
                    descriptor,
                    timestamp,
                    rule,
                    repeat_value,
                    repeat_slug,
                    repeat_index,
                )

    @staticmethod
    def _timestamp_repeat_values(rule: Dict[str, Any], timestamp: GrafanaTimeDownloader) -> List[str]:
        return (rule.get('values_by_timestamp') or {}).get(timestamp.id_time, rule['values'])

    @staticmethod
    def _mark_repeating_panel(panel: Panel, descriptor: PanelDescriptor, repeat_var: str) -> None:
        panel.is_repeating = True
        panel.source_panel_id = descriptor.panel_id
        panel.repeat_var = repeat_var

    def _append_repeating_task(self, render_tasks: List[PanelRenderTask], panel: Panel,
                               descriptor: PanelDescriptor, timestamp: GrafanaTimeDownloader,
                               rule: Dict[str, Any], repeat_value: str, repeat_slug: str,
                               repeat_index: int) -> None:
        repeat_var = rule['repeat_var']
        artifact = self._repeating_artifact(
            timestamp,
            repeat_var,
            repeat_value,
            repeat_slug,
            repeat_index,
            panel.panel_id,
        )
        panel.artifacts.append(artifact)
        variables = self._task_variables(repeat_var, repeat_value)
        task = PanelRenderTask(
            panel, timestamp, variables, artifact['png_file'], artifact, repeat_var, repeat_value,
            descriptor.raw_panel, self._effective_repeating_collect_no_data(rule))
        render_tasks.append(task)

    def _effective_repeating_collect_no_data(self, rule: Dict[str, Any]) -> bool:
        value = rule.get(COLLECT_NO_DATA_PANELS_KEY)
        return self.config.collect_no_data_panels if value is None else value

    @staticmethod
    def _base_artifact(timestamp: GrafanaTimeDownloader, file_name: Optional[str]) -> Dict[str, Any]:
        return {
            'timestamp_tag': timestamp.time_tag,
            'from': str(timestamp.start_time_timestamp),
            'to': str(timestamp.end_time_timestamp),
            'render_status': 'rendered',
            'png_file': file_name,
            'skip_reason': None,
        }

    def _repeating_artifact(self, timestamp: GrafanaTimeDownloader, repeat_var: str,
                             repeat_value: str, repeat_slug: str, repeat_index: int,
                             panel_id: int) -> Dict[str, Any]:
        repeat_hash = self._stable_hash(repeat_value)
        repeat_id = f'{repeat_index:03d}-{repeat_hash}'
        artifact = self._base_artifact(
            timestamp, f'{self.config.name}__{panel_id}__repeat-{repeat_id}__{timestamp.id_time}.png')
        artifact.update({
            'repeat_var': repeat_var,
            'repeat_value': repeat_value,
            'repeat_value_slug': repeat_slug,
            'repeat_index': repeat_index,
            'repeat_hash': repeat_hash,
            'repeat_id': repeat_id,
        })
        return artifact

    def _task_variables(self, repeat_var: str, repeat_value: str) -> Dict[str, Any]:
        variables = dict(self.config.vars or {})
        variables[repeat_var] = repeat_value
        return variables

    def _repeat_value_slugs(self, values: List[str]) -> List[str]:
        used = {}
        return [self._unique_repeat_slug(value, used) for value in values]

    def _unique_repeat_slug(self, value: str, used: Dict[str, str]) -> str:
        slug = self._repeat_value_slug(value)
        if slug not in used or used[slug] == value:
            used[slug] = value
            return slug
        unique_slug = f'{slug}-{self._stable_hash(value)}'
        used[unique_slug] = value
        return unique_slug

    @staticmethod
    def _repeat_value_slug(value: str) -> str:
        slug = re.sub(r'[^A-Za-z0-9._-]+', '-', value).strip('-')
        if slug:
            return slug
        return f'value-{RepeatTaskBuilder._stable_hash(value)}'

    @staticmethod
    def _stable_hash(value: str) -> str:
        return hashlib.sha256(value.encode('utf-8')).hexdigest()[:8]
