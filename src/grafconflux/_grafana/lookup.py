import logging
from typing import Dict, List

from grafconflux._shared.grafana_models import DashboardLookupRequest, DashboardLookupResult

logger = logging.getLogger('grafconflux.grafana')


def search_params(request: DashboardLookupRequest) -> Dict[str, str]:
    params = {'type': 'dash-db'}
    if request.dashboard_uid:
        params['dashboardUIDs'] = request.dashboard_uid
        return params
    params['query'] = request.dash_title
    if request.folder_uid:
        params['folderUIDs'] = request.folder_uid
    return params


def log_lookup_mode(request: DashboardLookupRequest) -> None:
    if request.dashboard_uid:
        logger.info(f'Dashboard lookup using dashboard_uid={request.dashboard_uid}')
        return
    message = f'Dashboard lookup using title="{request.dash_title}"'
    if request.folder_uid:
        message = f'{message} folder_uid={request.folder_uid}'
    if request.folder:
        message = f'{message} folder="{request.folder}"'
    logger.info(message)


def select_dashboard(request: DashboardLookupRequest, dashboards: List[Dict]) -> DashboardLookupResult:
    if request.dashboard_uid:
        return _select_by_uid(request.dashboard_uid, dashboards)
    return _select_by_title(request, dashboards)


def _select_by_uid(dashboard_uid: str, dashboards: List[Dict]) -> DashboardLookupResult:
    matches = [dash for dash in dashboards if dash.get('uid') == dashboard_uid]
    if len(matches) == 1:
        return DashboardLookupResult.from_search_result(matches[0])
    if not matches:
        raise ValueError(f'dashboard uid "{dashboard_uid}" not found.')
    _raise_ambiguity(f'dashboard_uid="{dashboard_uid}"', matches)


def _select_by_title(request: DashboardLookupRequest, dashboards: List[Dict]) -> DashboardLookupResult:
    title_matches = [dash for dash in dashboards if dash.get('title') == request.dash_title]
    matches = _filter_dashboard_folder_matches(request, title_matches)
    if len(matches) == 1:
        return DashboardLookupResult.from_search_result(matches[0])
    if not matches:
        _raise_title_not_found(request, title_matches)
    _raise_ambiguity(f'title="{request.dash_title}"', matches)


def _filter_dashboard_folder_matches(request: DashboardLookupRequest, dashboards: List[Dict]) -> List[Dict]:
    matches = dashboards
    if request.folder_uid:
        matches = [dash for dash in matches if dash.get('folderUid') == request.folder_uid]
    if request.folder:
        matches = [dash for dash in matches if dash.get('folderTitle') == request.folder]
    return matches


def _raise_title_not_found(request: DashboardLookupRequest, title_matches: List[Dict]) -> None:
    if title_matches and (request.folder or request.folder_uid):
        folder_identity = _requested_folder_identity(request)
        logger.warning(
            f'Dashboard lookup folder mismatch title="{request.dash_title}" '
            f'expected={folder_identity} candidates={len(title_matches)}'
        )
        raise ValueError(
            f'Dashboard title "{request.dash_title}" found but not in {folder_identity}. '
            f'available folders for exact title: {_format_folder_candidates(title_matches)}. '
            'matching is case-sensitive.'
        )
    raise ValueError(f'Dashboard with title "{request.dash_title}" not found.')


def _requested_folder_identity(request: DashboardLookupRequest) -> str:
    if request.folder:
        return f'folder "{request.folder}"'
    return f'folder_uid "{request.folder_uid}"'


def _format_folder_candidates(dashboards: List[Dict]) -> str:
    return '; '.join(f'{dash.get("folderTitle")} (uid={dash.get("folderUid")})' for dash in dashboards)


def _raise_ambiguity(identity: str, dashboards: List[Dict]) -> None:
    logger.warning(f'Dashboard lookup ambiguity {identity} candidates={len(dashboards)}')
    raise ValueError(f'Dashboard {identity} is ambiguous. Candidates: {_format_candidates(dashboards)}')


def _format_candidates(dashboards: List[Dict]) -> str:
    return '; '.join(
        f'uid={dash.get("uid")} folder={dash.get("folderTitle")} folder_uid={dash.get("folderUid")}'
        for dash in dashboards
    )
