import os
from typing import Any, Dict, Optional

from grafconflux._shared.time import GrafanaTimeDownloader


def build_dashboard_url_params(
    timestamp: GrafanaTimeDownloader,
    org_id: int,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build query params for a dashboard link for one timestamp."""
    params = {
        'orgId': org_id,
        'from': timestamp.start_time_timestamp,
        'to': timestamp.end_time_timestamp,
    }
    _append_grafana_variables(params, variables)
    return params


def build_panel_url_params(
    panel_id: int,
    timestamp: GrafanaTimeDownloader,
    org_id: int,
    white_theme: bool,
    tz: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build query params for a Grafana panel view URL."""
    params = {
        'orgId': org_id,
        'panelId': panel_id,
        'viewPanel': panel_id,
        'from': timestamp.start_time_timestamp,
        'to': timestamp.end_time_timestamp,
        'theme': 'light' if white_theme else 'dark',
    }
    if tz:
        params.update({'tz': tz})
    _append_grafana_variables(params, variables)
    return params


def build_render_api_params(panel_params: Dict[str, Any], width: int, height: int, timeout: int) -> Dict[str, Any]:
    """Build Grafana render API params from panel view params."""
    render_params = dict(panel_params)
    render_params.pop('viewPanel', None)
    render_params.update({'width': width, 'height': height, 'timeout': timeout})
    return render_params


def build_render_api_url(grafana_base_url: str, dashboard_url: str) -> str:
    """Build the Grafana render API URL for a dashboard panel."""
    return f'{grafana_base_url}/render/d-solo{dashboard_url[2:]}'


def build_render_file_path(
    charts_path: str,
    dashboard_name: str,
    panel_id: int,
    timestamp: GrafanaTimeDownloader,
    task_file_name: Optional[str] = None,
) -> str:
    """Build the PNG output path for a concrete render task."""
    file_name = task_file_name or f'{dashboard_name}__{panel_id}__{timestamp.id_time}.png'
    return os.path.join(charts_path, file_name)


def _append_grafana_variables(params: Dict[str, Any], variables: Optional[Dict[str, Any]]) -> None:
    if variables is not None:
        for key, value in variables.items():
            params.update({f'var-{key}': value})
