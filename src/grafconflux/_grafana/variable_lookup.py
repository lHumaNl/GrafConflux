"""Resolve user-facing variable lookup identifiers against one dashboard."""

from __future__ import annotations

import logging
from typing import Any

from grafconflux._grafana.matrix_context import dashboard_variable_current_raw_value
from grafconflux._shared.grafana_models import ConfigurationError
from grafconflux._shared.presentation import display_value

logger = logging.getLogger(__name__)

LOOKUP_METADATA_FIELDS = ("name", "label", "description")


def resolve_dashboard_variable_lookups(config: Any, dashboard: dict[str, Any]) -> None:
    """Replace opted-in static config keys with matched Grafana technical names."""
    lookups = getattr(config, "var_lookups", {}) or {}
    if not lookups or getattr(config, "_var_lookups_resolved", False):
        return
    variables = dict(getattr(config, "vars", None) or {})
    datasource_vars = dict(getattr(config, "datasource_vars", {}) or {})
    datasource_names = dict(getattr(config, "datasource_names", {}) or {})
    presentation = dict(getattr(config, "vars_presentation", {}) or {})
    for config_name, lookup_spec in lookups.items():
        _apply_static_lookup(
            config.name, config_name, lookup_spec, dashboard,
            variables, datasource_vars, presentation, datasource_names,
        )
    config.vars = variables
    config.datasource_vars = datasource_vars
    config.datasource_names = datasource_names
    config.vars_presentation = presentation
    config._var_lookups_resolved = True


def _apply_static_lookup(
    dashboard_name: str, config_name: str, lookup_spec: dict[str, Any],
    dashboard: dict[str, Any], variables: dict[str, Any],
    datasource_vars: dict[str, Any], presentation: dict[str, dict[str, Any]],
    datasource_names: dict[str, str],
) -> None:
    variable = _matched_variable(dashboard_name, config_name, lookup_spec, dashboard, "vars")
    technical_name = str(variable["name"])
    value = _resolved_static_value(dashboard_name, config_name, lookup_spec, variable, variables)
    _replace_key(variables, config_name, technical_name, value, dashboard_name)
    _replace_presentation(presentation, config_name, technical_name, value)
    datasource_vars.pop(config_name, None)
    if lookup_spec["is_datasource"]:
        datasource_vars[technical_name] = value
    _replace_datasource_name(datasource_names, config_name, technical_name)
    _log_resolution(dashboard_name, config_name, variable)


def resolve_matrix_variable_lookups(
    dashboard_name: str,
    matrix: dict[str, Any],
    dashboard: dict[str, Any],
) -> dict[str, Any]:
    """Return a matrix copy whose lookup specs contain raw Grafana names."""
    resolved_variables = {
        key: _resolved_matrix_spec(dashboard_name, key, spec, dashboard)
        for key, spec in matrix["variables"].items()
    }
    return {**matrix, "variables": resolved_variables}


def _resolved_matrix_spec(
    dashboard_name: str, key: str, spec: dict[str, Any], dashboard: dict[str, Any],
) -> dict[str, Any]:
    resolved = dict(spec)
    lookup = spec.get("lookup")
    if not lookup:
        return resolved
    lookup_spec = {"lookup": lookup, "is_datasource": False}
    variable = _matched_variable(dashboard_name, key, lookup_spec, dashboard, "render_matrix.variables")
    resolved["grafana_variable"] = str(variable["name"])
    _log_resolution(dashboard_name, key, variable)
    return resolved


def _log_resolution(dashboard_name: str, config_name: str, variable: dict[str, Any]) -> None:
    logger.info(
        "Grafana variable lookup resolved dashboard=%s config_variable=%s source=dashboard_metadata type=%s",
        dashboard_name, config_name, variable.get("type", "unknown"),
    )


def _matched_variable(
    dashboard_name: str,
    config_name: str,
    lookup_spec: dict[str, Any],
    dashboard: dict[str, Any],
    config_section: str,
) -> dict[str, Any]:
    lookup = lookup_spec["lookup"]
    matches = [
        variable
        for variable in _dashboard_variables(dashboard)
        if _matches_lookup(variable, lookup, lookup_spec["is_datasource"])
    ]
    if len(matches) != 1:
        path = f"dashboards.{dashboard_name}.{config_section}.{config_name}.lookup"
        raise ConfigurationError(f"{path}: matched {len(matches)} dashboard variables; expected exactly one.")
    return matches[0]


def _dashboard_variables(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    templating = dashboard.get("templating", {}) if isinstance(dashboard, dict) else {}
    variables = templating.get("list", []) if isinstance(templating, dict) else []
    return [item for item in variables if isinstance(item, dict) and item.get("name")]


def _matches_lookup(variable: dict[str, Any], lookup: str, datasource_only: bool) -> bool:
    if datasource_only and variable.get("type") != "datasource":
        return False
    return any(variable.get(field) == lookup for field in LOOKUP_METADATA_FIELDS)


def _resolved_static_value(
    dashboard_name: str,
    config_name: str,
    lookup_spec: dict[str, Any],
    variable: dict[str, Any],
    configured: dict[str, Any],
) -> Any:
    if not lookup_spec["use_current"]:
        return configured[config_name]
    value = dashboard_variable_current_raw_value(variable)
    if value is None:
        path = f"dashboards.{dashboard_name}.vars.{config_name}.value"
        raise ConfigurationError(f"{path}: dashboard datasource variable has no usable current raw value.")
    return value


def _replace_key(
    variables: dict[str, Any],
    config_name: str,
    technical_name: str,
    value: Any,
    dashboard_name: str,
) -> None:
    if technical_name != config_name and technical_name in variables:
        path = f"dashboards.{dashboard_name}.vars.{config_name}.lookup"
        raise ConfigurationError(f"{path}: resolved technical name collides with another configured variable.")
    variables.pop(config_name, None)
    variables[technical_name] = value


def _replace_presentation(
    presentation: dict[str, dict[str, Any]],
    config_name: str,
    technical_name: str,
    value: Any,
) -> None:
    metadata = presentation.pop(config_name, None)
    if metadata is not None:
        metadata = dict(metadata)
        metadata["raw_value"] = value
        metadata["display_value"] = display_value(value, metadata.get("value_aliases", {}))
        presentation[technical_name] = metadata


def _replace_datasource_name(
    datasource_names: dict[str, str], config_name: str, technical_name: str,
) -> None:
    configured_name = datasource_names.pop(config_name, None)
    if configured_name is not None:
        datasource_names[technical_name] = configured_name


def resolve_configured_datasource_names(config: Any, session: Any, datasource_url: str) -> None:
    """Resolve explicit datasource names to UIDs using one authenticated list request."""
    datasource_names = getattr(config, "datasource_names", {}) or {}
    if not datasource_names or getattr(config, "_datasource_names_resolved", False):
        return
    response = session.get(datasource_url, timeout=config.timeout)
    if response.status_code != 200:
        raise ConfigurationError(f"dashboards.{config.name}.vars: datasource name resolution failed.")
    records = response.json()
    if not isinstance(records, list):
        raise ConfigurationError(f"dashboards.{config.name}.vars: datasource name resolution failed.")
    resolved = _datasource_name_uids(config.name, datasource_names, records)
    _apply_datasource_uids(config, resolved)
    config._datasource_names_resolved = True


def _datasource_name_uids(
    dashboard_name: str, datasource_names: dict[str, str], records: list[Any],
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for variable, datasource_name in datasource_names.items():
        matches = [record for record in records if _datasource_name_matches(record, datasource_name)]
        if len(matches) != 1 or not _valid_datasource_uid(matches[0]):
            raise ConfigurationError(
                f"dashboards.{dashboard_name}.vars.{variable}.name: expected exactly one available datasource.")
        resolved[variable] = matches[0]["uid"]
    return resolved


def _datasource_name_matches(record: Any, datasource_name: str) -> bool:
    return isinstance(record, dict) and record.get("name") == datasource_name


def _valid_datasource_uid(record: dict[str, Any]) -> bool:
    return isinstance(record.get("uid"), str) and bool(record["uid"])


def _apply_datasource_uids(config: Any, resolved: dict[str, str]) -> None:
    variables = dict(getattr(config, "vars", {}) or {})
    datasource_vars = dict(getattr(config, "datasource_vars", {}) or {})
    presentation = dict(getattr(config, "vars_presentation", {}) or {})
    for variable, uid in resolved.items():
        variables[variable] = uid
        datasource_vars[variable] = uid
        _replace_presentation_value(presentation, variable, uid)
    config.vars = variables
    config.datasource_vars = datasource_vars
    config.vars_presentation = presentation


def _replace_presentation_value(presentation: dict[str, dict[str, Any]], variable: str, uid: str) -> None:
    metadata = presentation.get(variable)
    if metadata is not None:
        metadata = dict(metadata)
        metadata["raw_value"] = uid
        metadata["display_value"] = display_value(uid, metadata.get("value_aliases", {}))
        presentation[variable] = metadata
