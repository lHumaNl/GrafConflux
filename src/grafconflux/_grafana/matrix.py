"""Dashboard-level render matrix planning for Grafana render tasks."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from grafconflux._grafana.matrix_browser_planning import BrowserMatrixFallback
from grafconflux._grafana.matrix_config import (
    DEFAULT_MAX_MATRIX_ROWS,
    RENDER_MATRIX_KEY,
    max_values,
    regex_value,
    validated_render_matrix,
)
from grafconflux._grafana.matrix_dependencies import configured_dependencies, ordered_matrix_variables
from grafconflux._grafana.matrix_discovery import MatrixValueResolver, safe_discovery_variable
from grafconflux._shared.grafana_models import ConfigurationError, Panel, PanelDescriptor, PanelRenderTask

logger = logging.getLogger(__name__)


def append_matrix_tasks(
    config: Any,
    dashboard: dict[str, Any],
    descriptors: list[PanelDescriptor],
    panels: list[Panel],
    render_tasks: list[PanelRenderTask],
    timestamps: list[Any],
    session: Any = None,
    dashboard_url: str = "",
) -> list[PanelRenderTask]:
    matrix = getattr(config, "render_matrix", None)
    if not matrix:
        return render_tasks
    planning_matrix = _planning_matrix(config.name, matrix, dashboard)
    browser_fallback = (
        BrowserMatrixFallback(config, session, dashboard_url, dashboard=dashboard)
        if dashboard_url else None
    )
    dynamic_names = {_grafana_variable(key, spec) for key, spec in matrix["variables"].items()}
    resolver = MatrixValueResolver(dashboard, session, config, browser_fallback, dynamic_names)
    try:
        rows_by_time = _rows_by_timestamp(
            config, planning_matrix, dashboard, timestamps, getattr(config, "vars", None) or {}, resolver,
        )
    finally:
        if browser_fallback is not None:
            _close_browser_fallback(browser_fallback)
    setattr(config, "render_matrix_rows_by_timestamp", rows_by_time)
    _remove_source_artifacts(panels, render_tasks)
    return _matrix_tasks(config, render_tasks, rows_by_time)


def build_matrix_dashboard_links(config: Any, timestamps: list[Any], dashboard_url: str, params_builder) -> list[dict[str, Any]]:
    rows_by_time = getattr(config, "render_matrix_rows_by_timestamp", {}) or {}
    links: list[dict[str, Any]] = []
    for timestamp in timestamps:
        for row in rows_by_time.get(timestamp.id_time, []):
            params = params_builder(timestamp, config.orgId, _dashboard_link_variables(config, row))
            links.append({
                "timestamp_id": timestamp.id_time,
                "label": row["label"],
                "url": _url(dashboard_url, params),
                "variables": row["variables"],
                "grafana_variables": row["url_variables"],
                "context_path": row["context_path"],
            })
    return links


def _dashboard_link_variables(config: Any, row: dict[str, Any]) -> dict[str, Any]:
    variables = dict(getattr(config, "vars", None) or {})
    variables.update(row["url_variables"])
    return variables


def _planning_matrix(dashboard_name: str, matrix: dict[str, Any], dashboard: dict[str, Any]) -> dict[str, Any]:
    ordered, dependencies = ordered_matrix_variables(dashboard_name, matrix, dashboard)
    variables = {
        key: {**matrix["variables"][key], "__resolved_dependencies__": dependencies[key]}
        for key in ordered
    }
    if matrix.get("combination_mode", "product") == "zip" and any(dependencies.values()):
        raise ConfigurationError(_path(dashboard_name, "combination_mode") + ": zip does not support dependent variables.")
    return {**matrix, "variables": variables}


def _rows_by_timestamp(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                       timestamps: list[Any], static_vars: dict[str, Any], resolver: MatrixValueResolver) -> dict[int, list[dict[str, Any]]]:
    return {
        timestamp.id_time: _rows_for_timestamp(config, matrix, dashboard, timestamp, static_vars, resolver)
        for timestamp in timestamps
    }


def _rows_for_timestamp(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                         timestamp: Any, static_vars: dict[str, Any], resolver: MatrixValueResolver) -> list[dict[str, Any]]:
    dashboard_name = config.name
    if matrix.get("combination_mode", "product") == "product":
        rows = _product_rows(config, matrix, dashboard, timestamp, static_vars, resolver)
        if not rows:
            raise ConfigurationError(
                _path(dashboard_name, "variables")
                + ": no rows resolved. Check values_from discovery warnings; implicit values_from requires "
                + "a supported Grafana variable query or explicit values/values_by."
            )
        _validate_row_limit(dashboard_name, matrix, len(rows))
        return [_row_record(dashboard_name, matrix, index, row) for index, row in enumerate(rows)]
    values, provenance = _variable_values(config, matrix, dashboard, timestamp, static_vars, resolver)
    rows = _zip_rows(values, dashboard_name)
    _validate_row_limit(dashboard_name, matrix, len(rows))
    rows = [{**row, "__discovery__": provenance} for row in rows]
    return [_row_record(dashboard_name, matrix, index, row) for index, row in enumerate(rows)]


def _product_rows(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                   timestamp: Any, static_vars: dict[str, Any], resolver: MatrixValueResolver) -> list[dict[str, Any]]:
    rows: list[dict[str, str]] = [{}]
    for key, spec in matrix["variables"].items():
        rows = _extend_rows(config, matrix, key, spec, dashboard, timestamp, static_vars, rows, resolver)
    return rows


def _extend_rows(config: Any, matrix: dict[str, Any], key: str, spec: dict[str, Any],
                 dashboard: dict[str, Any], timestamp: Any, static_vars: dict[str, Any],
                  rows: list[dict[str, Any]], resolver: MatrixValueResolver) -> list[dict[str, Any]]:
    extended: list[dict[str, str]] = []
    for row in rows:
        values, provenance = _values_for_spec(config, key, spec, dashboard, timestamp, row, static_vars, resolver)
        if not values:
            logger.warning(
                "Render matrix branch skipped dashboard=%s variable=%s timestamp_id=%s context_vars=%s "
                "reason=authoritative_empty source=%s method=%s",
                config.name,
                safe_discovery_variable(key),
                timestamp.id_time,
                sorted(str(name) for name in row if not str(name).startswith("__")),
                (provenance or {}).get("source"),
                (provenance or {}).get("method"),
            )
            continue
        for value in values:
            extended.append(_extended_row(row, key, value, provenance))
            _validate_row_limit(config.name, matrix, len(extended))
    return extended


def _variable_values(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                     timestamp: Any, static_vars: dict[str, Any], resolver: MatrixValueResolver) -> tuple[dict[str, list[str]], dict[str, Any]]:
    values: dict[str, list[str]] = {}
    provenance: dict[str, Any] = {}
    for key, spec in matrix["variables"].items():
        values[key], variable_provenance = _values_for_spec(config, key, spec, dashboard, timestamp, {}, static_vars, resolver)
        if variable_provenance:
            provenance[key] = variable_provenance
    return values, provenance


def _values_for_spec(config: Any, key: str, spec: dict[str, Any], dashboard: dict[str, Any], timestamp: Any,
                     context: dict[str, Any], static_vars: dict[str, Any], resolver: MatrixValueResolver) -> tuple[list[str], dict[str, Any] | None]:
    provenance = None
    if "values" in spec:
        values = _scalar_list(spec.get("values"))
    elif "values_by" in spec:
        values = _values_by_context(config.name, key, spec, context)
    else:
        result = resolver.resolve(
            key, spec, timestamp, _discovery_context(config.render_matrix, context), static_vars,
        )
        if not result.authoritative:
            raise ConfigurationError(_discovery_failure(config.name, key, result.provenance))
        values, provenance = result.values, result.provenance
    values = _filtered_values(spec, values)
    return values, provenance


def _discovery_failure(dashboard_name: str, key: str, provenance: dict[str, Any]) -> str:
    details = " ".join(
        f"{name}={provenance.get(name)}"
        for name in ("status", "source", "method", "timestamp_id", "from", "to", "context_vars")
    )
    return _path(dashboard_name, f"variables.{key}") + f": dynamic discovery did not resolve ({details})."


def _extended_row(row: dict[str, Any], key: str, value: str, provenance: dict[str, Any] | None) -> dict[str, Any]:
    extended = {**row, key: value}
    if provenance:
        extended["__discovery__"] = {**row.get("__discovery__", {}), key: provenance}
    return extended


def _discovery_context(matrix: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    public_context = {key: value for key, value in context.items() if not str(key).startswith("__")}
    return {_grafana_variable(key, matrix["variables"][key]): value for key, value in public_context.items()}


def _close_browser_fallback(browser_fallback: BrowserMatrixFallback) -> None:
    try:
        browser_fallback.close()
    except Exception as error:
        logger.warning("Matrix planning browser cleanup failed error_type=%s", type(error).__name__)


def _values_by_context(dashboard_name: str, key: str, spec: dict[str, Any], context: dict[str, str]) -> list[str]:
    mapping = spec.get("values_by")
    if not isinstance(mapping, dict):
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.values_by") + ": expected mapping.")
    deps = spec.get("__resolved_dependencies__", configured_dependencies(spec))
    missing = [dependency for dependency in deps if dependency not in context]
    if missing:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.depends_on") + f": unresolved dependencies {missing}.")
    context_key = "|".join(context[dep] for dep in deps)
    if context_key not in mapping:
        raise ConfigurationError(
            _path(dashboard_name, f"variables.{key}.values_by")
            + f": missing values for dependency context {context_key}."
        )
    return _dedupe(_scalar_list(mapping.get(context_key, [])))


def _filtered_values(spec: dict[str, Any], values: list[str]) -> list[str]:
    regex = regex_value(spec)
    if regex:
        pattern = re.compile(str(regex))
        values = [value for value in values if pattern.search(value)]
    return _dedupe(values)[:max_values(spec)]


def _zip_rows(values: dict[str, list[str]], dashboard_name: str) -> list[dict[str, str]]:
    _validate_zip_lengths(values, dashboard_name)
    names = list(values)
    return [dict(zip(names, row)) for row in zip(*(values[name] for name in names))]


def _validate_zip_lengths(values: dict[str, list[str]], dashboard_name: str) -> None:
    if len({len(items) for items in values.values()}) > 1:
        raise ConfigurationError(_path(dashboard_name, "combination_mode") + ": zip variable lists must have equal length.")


def _row_record(dashboard_name: str, matrix: dict[str, Any], index: int, row: dict[str, str]) -> dict[str, Any]:
    aliases = _alias_variables(matrix, row)
    url_variables = _url_variables(matrix, row)
    row_hash = _stable_hash(dashboard_name, index, aliases)
    context_path = _context_path(matrix, row)
    return {"index": index, "hash": row_hash, "variables": aliases, "url_variables": url_variables,
            "label": _row_label(matrix, aliases), "group": _row_group(matrix, aliases),
            "context_path": context_path, "discovery": row.get("__discovery__", {})}


def _context_path(matrix: dict[str, Any], row: dict[str, str]) -> list[dict[str, str]]:
    return [
        {"key": key, "label": _alias(key, spec), "value": row[key], "grafana_variable": _grafana_variable(key, spec)}
        for key, spec in matrix["variables"].items()
    ]


def _alias_variables(matrix: dict[str, Any], row: dict[str, str]) -> dict[str, str]:
    return {_alias(key, spec): row[key] for key, spec in matrix["variables"].items()}


def _url_variables(matrix: dict[str, Any], row: dict[str, str]) -> dict[str, str]:
    return {_grafana_variable(key, spec): row[key] for key, spec in matrix["variables"].items()}


def _row_label(matrix: dict[str, Any], aliases: dict[str, str]) -> str:
    template = matrix.get("label_template")
    if isinstance(template, str) and template:
        return template.format(**_label_template_values(matrix, aliases))
    return ", ".join(f"{key}: {value}" for key, value in aliases.items())


def _label_template_values(matrix: dict[str, Any], aliases: dict[str, str]) -> dict[str, str]:
    values = dict(aliases)
    for key, spec in matrix["variables"].items():
        values[key] = aliases[_alias(key, spec)]
    return values


def _row_group(matrix: dict[str, Any], aliases: dict[str, str]) -> str | None:
    group_by = matrix.get("row_grouping", matrix.get("group_by", [])) or []
    labels = [_alias(key, matrix["variables"][key]) for key in group_by]
    return ", ".join(f"{label}: {aliases[label]}" for label in labels) if labels else None


def _validate_row_limit(dashboard_name: str, matrix: dict[str, Any], row_count: int) -> None:
    max_rows = matrix.get("max_rows", DEFAULT_MAX_MATRIX_ROWS)
    if row_count <= max_rows:
        return
    raise ConfigurationError(_path(dashboard_name, "max_rows") + f": expansion produced {row_count} rows, limit is {max_rows}.")


def _matrix_tasks(config: Any, render_tasks: list[PanelRenderTask], rows_by_time: dict[int, list[dict[str, Any]]]) -> list[PanelRenderTask]:
    tasks: list[PanelRenderTask] = []
    for task in render_tasks:
        for row in rows_by_time.get(task.timestamp.id_time, []):
            tasks.append(_matrix_task(config, task, row))
    return tasks


def _matrix_task(config: Any, task: PanelRenderTask, row: dict[str, Any]) -> PanelRenderTask:
    variables = dict(task.variables or {})
    variables.update(row["url_variables"])
    artifact = _matrix_artifact(config.name, task.panel, task.timestamp, row, task.artifact)
    task.panel.artifacts.append(artifact)
    return PanelRenderTask(task.panel, task.timestamp, variables, artifact["png_file"], artifact,
                           task.repeat_var, task.repeat_value, task.raw_panel, task.collect_no_data_panels)


def _matrix_artifact(dashboard_name: str, panel: Panel, timestamp: Any, row: dict[str, Any], source: dict[str, Any] | None) -> dict[str, Any]:
    file_name = f"{dashboard_name}__{panel.panel_id}__matrix-{row['index']:03d}-{row['hash']}__{timestamp.id_time}.png"
    return {"artifact_type": "matrix", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
            "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
            "render_status": "rendered", "png_file": file_name, "skip_reason": None,
            "source_panel_id": panel.panel_id, "source_panel_type": panel.type,
            "source_panel_title": panel.title, "source_panel_display_title": panel.display_title,
            "source_timestamp_id": timestamp.id_time, "display_title": _matrix_panel_title(panel.display_title, row),
            "repeat_var": source.get("repeat_var") if source else None,
            "matrix": {"index": row["index"], "hash": row["hash"], "variables": row["variables"],
                        "grafana_variables": row["url_variables"], "label": row["label"], "group": row.get("group"),
                        "context_path": row["context_path"], "discovery": row.get("discovery", {})}}


def _matrix_panel_title(panel_title: str, row: dict[str, Any]) -> str:
    label = row.get("label")
    return f"{panel_title} ({label})" if label else panel_title


def _remove_source_artifacts(panels: list[Panel], render_tasks: list[PanelRenderTask]) -> None:
    source_ids = {id(task.artifact) for task in render_tasks if task.artifact is not None}
    for panel in panels:
        panel.artifacts = [artifact for artifact in panel.artifacts if id(artifact) not in source_ids]


def _scalar_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in (None, "") and not isinstance(item, (dict, list))]


def _alias(key: str, spec: dict[str, Any]) -> str:
    return str(spec.get("alias", key))


def _grafana_variable(key: str, spec: dict[str, Any]) -> str:
    return str(spec.get("grafana_variable") or key)


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _stable_hash(dashboard_name: str, index: int, variables: dict[str, str]) -> str:
    payload = repr((dashboard_name, index, sorted(variables.items())))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]


def _url(base_url: str, params: dict[str, Any]) -> str:
    from urllib.parse import urlencode

    return f"{base_url}?{urlencode(params, doseq=True)}"


def _path(dashboard_name: str, suffix: str) -> str:
    return f"dashboards.{dashboard_name}.{RENDER_MATRIX_KEY}.{suffix}"
