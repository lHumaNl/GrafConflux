import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from grafconflux._shared.time import GrafanaTimeDownloader
from grafconflux._shared.grafana_models import (
    DEFAULT_INTERVAL_MS,
    DEFAULT_MAX_DATA_POINTS,
    SKIP_REASON_EMPTY_FRAMES,
    SUPPORTED_PHASE1_DATASOURCES,
    NoDataDatasourceInference,
    NoDataPreflightResult,
    Panel,
    PanelRenderTask,
)

logger = logging.getLogger('grafconflux.grafana')

BuildPanelUrl = Callable[[Panel, GrafanaTimeDownloader, Optional[Dict[str, Any]]], Tuple[str, Dict[str, Any]]]
RecordTaskLink = Callable[[PanelRenderTask, str], None]


class NoDataDetectorRegistry:
    """Internal registry for conservative no-data detector factories."""

    def __init__(self, detectors: Optional[Dict[str, Callable]] = None):
        self._detectors = dict(detectors or {})

    @classmethod
    def default(cls) -> 'NoDataDetectorRegistry':
        return cls({name: _GenericNoDataDetector for name in SUPPORTED_PHASE1_DATASOURCES})

    def get(self, datasource_type: Optional[str]) -> Optional[Callable]:
        return self._detectors.get(datasource_type or '')

    def supports(self, datasource_type: Optional[str]) -> bool:
        return self.get(datasource_type) is not None


class NoDataPreflightRunner:
    def __init__(self, config, session, build_panel_url: BuildPanelUrl, record_task_link: RecordTaskLink):
        self.config = config
        self.session = session
        self._build_panel_url = build_panel_url
        self._record_task_link = record_task_link

    def skip_task(self, task: PanelRenderTask) -> bool:
        result = self.result(task)
        self.record_preflight(task, result)
        if not result.should_skip:
            return False
        self.record_skipped_task_link(task)
        logger.info(
            f'Skipping panel render panel_id={task.panel.panel_id} '
            f'timestamp={task.timestamp.time_tag} reason=no_data_confirmed'
        )
        return True

    def result(self, task: PanelRenderTask) -> NoDataPreflightResult:
        if task.collect_no_data_panels:
            return NoDataPreflightResult(False, 'not_applicable')
        inference = infer_no_data_datasource(task.raw_panel or {}, NoDataDetectorRegistry.default())
        if not inference.applicable:
            return NoDataPreflightResult(False, inference.reason, datasource_uid=inference.datasource_uid)
        logger.info(
            f'Preflight detector inferred panel_id={task.panel.panel_id} '
            f'detector={inference.detector_name} datasource_uid={inference.datasource_uid}'
        )
        detector_factory = NoDataDetectorRegistry.default().get(inference.datasource_type)
        return detector_factory(self, inference).check(task)

    def record_preflight(self, task: PanelRenderTask, result: NoDataPreflightResult) -> None:
        if task.artifact is None or task.collect_no_data_panels:
            return
        if result.should_skip and not self.config.no_data_preflight.store_skip_metadata:
            task.artifact.update({'render_status': 'missing', 'png_file': None, 'skip_reason': None})
            return
        task.artifact.update({
            'has_data_preflight': True,
            'preflight_datasource_uid': result.datasource_uid,
            'preflight_status': result.status,
            'preflight_detector': result.detector_name,
        })
        if result.should_skip:
            task.artifact.update({
                'render_status': 'skipped_no_data',
                'png_file': None,
                'skip_reason': result.reason or SKIP_REASON_EMPTY_FRAMES,
            })

    def record_skipped_task_link(self, task: PanelRenderTask) -> None:
        url, params = self._build_panel_url(task.panel, task.timestamp, task.variables)
        self._record_task_link(task, f"{url}?{urlencode(params, doseq=True)}")

    def ds_query_url(self) -> str:
        prefix = self.config.nginx_prefix if self.config.nginx_prefix else ''
        return f'{self.config.host}{prefix}/api/ds/query'


class _GenericNoDataDetector:
    def __init__(self, runner: Any, inference: NoDataDatasourceInference):
        self.runner = runner
        self.inference = inference

    def check(self, task: PanelRenderTask) -> NoDataPreflightResult:
        try:
            response = self.runner.session.post(
                self._ds_query_url(),
                json=self._payload(task),
                timeout=self.runner.config.no_data_preflight.timeout,
            )
            if response.status_code != 200:
                return self._render_anyway('error')
            result = interpret_no_data_response(response.json(), self.inference.ref_ids)
        except Exception as error:
            logger.warning(
                f'Preflight no-data check error panel_id={task.panel.panel_id} '
                f'action=render_anyway error={error}'
            )
            return self._render_anyway('error')
        return self._result_from_interpretation(result)

    def _ds_query_url(self) -> str:
        if hasattr(self.runner, 'ds_query_url'):
            return self.runner.ds_query_url()
        return self.runner._ds_query_url()

    def _payload(self, task: PanelRenderTask) -> Dict[str, Any]:
        return {
            'from': str(task.timestamp.start_time_timestamp),
            'to': str(task.timestamp.end_time_timestamp),
            'queries': [self._query(target, task.variables) for target in self.inference.targets],
        }

    @staticmethod
    def _query(target: Dict[str, Any], variables: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        query = dict(target)
        query.setdefault('maxDataPoints', DEFAULT_MAX_DATA_POINTS)
        query.setdefault('intervalMs', DEFAULT_INTERVAL_MS)
        if variables:
            query.setdefault('scopedVars', _scoped_vars(variables))
        return query

    def _result_from_interpretation(self, result: NoDataPreflightResult) -> NoDataPreflightResult:
        if result.status == 'confirmed_no_data':
            return NoDataPreflightResult(
                True, result.status, SKIP_REASON_EMPTY_FRAMES,
                self.inference.datasource_uid, self.inference.detector_name)
        return NoDataPreflightResult(
            False, result.status, result.reason,
            self.inference.datasource_uid, self.inference.detector_name)

    def _render_anyway(self, status: str) -> NoDataPreflightResult:
        return NoDataPreflightResult(
            False, status, None, self.inference.datasource_uid, self.inference.detector_name)


def infer_no_data_datasource(panel: Dict[str, Any], registry: NoDataDetectorRegistry) -> NoDataDatasourceInference:
    if _has_no_data_unsupported_panel_features(panel):
        return NoDataDatasourceInference(False, reason='not_applicable')
    targets = _preflight_targets(panel)
    if not targets:
        return NoDataDatasourceInference(False, reason='not_applicable')
    datasource_refs = [_target_datasource_ref(panel, target) for target in targets]
    if any(ref is None for ref in datasource_refs):
        return NoDataDatasourceInference(False, reason='ambiguous_datasource')
    if _has_ambiguous_datasource_refs(datasource_refs):
        return NoDataDatasourceInference(False, reason='mixed_datasource')
    datasource_type, datasource_uid = datasource_refs[0]
    if not registry.supports(datasource_type):
        return NoDataDatasourceInference(False, datasource_type, datasource_uid, reason='unsupported_datasource')
    ref_ids = [target.get('refId') for target in targets]
    if any(not ref_id for ref_id in ref_ids):
        return NoDataDatasourceInference(False, datasource_type, datasource_uid, reason='incomplete_panel')
    return NoDataDatasourceInference(True, datasource_type, datasource_uid, datasource_type, 'applicable', targets, ref_ids)


def interpret_no_data_response(response_json: Dict[str, Any], ref_ids: List[str]) -> NoDataPreflightResult:
    results = response_json.get('results') if isinstance(response_json, dict) else None
    if not isinstance(results, dict):
        return NoDataPreflightResult(False, 'inconclusive')
    states = [_interpret_ref_result(results, ref_id) for ref_id in ref_ids]
    if 'error' in states:
        return NoDataPreflightResult(False, 'error')
    if 'inconclusive' in states:
        return NoDataPreflightResult(False, 'inconclusive')
    if 'confirmed_has_data' in states:
        return NoDataPreflightResult(False, 'confirmed_has_data')
    return NoDataPreflightResult(True, 'confirmed_no_data', SKIP_REASON_EMPTY_FRAMES)


def _interpret_ref_result(results: Dict[str, Any], ref_id: str) -> str:
    result = results.get(ref_id)
    if not isinstance(result, dict):
        return 'inconclusive'
    if result.get('error') or result.get('errorSource'):
        return 'error'
    frames = result.get('frames', [])
    if not isinstance(frames, list):
        return 'inconclusive'
    return _interpret_frames(frames)


def _interpret_frames(frames: List[Any]) -> str:
    for frame in frames:
        frame_state = _interpret_frame(frame)
        if frame_state != 'confirmed_no_data':
            return frame_state
    return 'confirmed_no_data'


def _interpret_frame(frame: Any) -> str:
    if not isinstance(frame, dict):
        return 'inconclusive'
    values = frame.get('data', {}).get('values')
    if values is not None:
        return _interpret_values(values)
    fields = frame.get('schema', {}).get('fields') or frame.get('fields')
    if fields is None:
        return 'confirmed_no_data'
    return _interpret_fields(fields)


def _interpret_fields(fields: Any) -> str:
    if not isinstance(fields, list):
        return 'inconclusive'
    states = [_interpret_values(field.get('values', [])) for field in fields if isinstance(field, dict)]
    if not states:
        return 'confirmed_no_data'
    return 'confirmed_has_data' if 'confirmed_has_data' in states else 'confirmed_no_data'


def _interpret_values(values: Any) -> str:
    if isinstance(values, list):
        return 'confirmed_has_data' if any(_value_has_data(value) for value in values) else 'confirmed_no_data'
    return 'inconclusive' if values is not None else 'confirmed_no_data'


def _value_has_data(value: Any) -> bool:
    if isinstance(value, list):
        return any(item is not None for item in value)
    return value is not None


def _has_no_data_unsupported_panel_features(panel: Dict[str, Any]) -> bool:
    return bool(
        panel.get('transformations') or panel.get('libraryPanel') or panel.get('libraryPanelUid')
        or panel.get('libraryPanelId')
    )


def _preflight_targets(panel: Dict[str, Any]) -> List[Dict[str, Any]]:
    targets = panel.get('targets')
    if not isinstance(targets, list):
        return []
    return [target for target in targets if isinstance(target, dict) and not target.get('hide')]


def _target_datasource_ref(panel: Dict[str, Any], target: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    datasource = target.get('datasource') or panel.get('datasource')
    datasource_type, datasource_uid = _datasource_type_uid(datasource)
    if datasource_type in ('mixed', '__expr__', 'expr'):
        return None
    if not datasource_type or not datasource_uid:
        return None
    return datasource_type, datasource_uid


def _datasource_type_uid(datasource: Any) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(datasource, dict):
        return datasource.get('type'), datasource.get('uid')
    if isinstance(datasource, str):
        return datasource, datasource
    return None, None


def _has_ambiguous_datasource_refs(datasource_refs: List[Tuple[str, str]]) -> bool:
    return len(set(datasource_refs)) != 1


def _scoped_vars(variables: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {key: {'text': value, 'value': value} for key, value in variables.items()}
