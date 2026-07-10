"""Dashboard-level render matrix planning for Grafana render tasks."""

from __future__ import annotations

import hashlib
import logging
import re
from string import Formatter
from typing import Any

from grafconflux._grafana.matrix_discovery import resolve_values_from
from grafconflux._shared.grafana_models import ConfigurationError, Panel, PanelDescriptor, PanelRenderTask

RENDER_MATRIX_KEY = "render_matrix"
DEFAULT_MAX_MATRIX_VALUES = 50
DEFAULT_MAX_MATRIX_ROWS = 500
MATRIX_MODES = {"product", "zip"}
MATRIX_LAYOUTS = {"dashboard_first", "matrix_values_first"}
MATRIX_NESTED_OPTION_KEYS = {
    "enabled", "row_grouping", "group_by", "combination_mode", "label_template", "max_rows", "layout",
}
MATRIX_FLAT_OPTION_KEYS = MATRIX_NESTED_OPTION_KEYS - {"layout"}
logger = logging.getLogger(__name__)


def validated_render_matrix(dashboard_name: str, config: dict[str, Any]) -> dict[str, Any] | None:
    value = config.get(RENDER_MATRIX_KEY)
    if value in (None, False):
        return None
    if not isinstance(value, dict):
        raise ConfigurationError(f"dashboards.{dashboard_name}.{RENDER_MATRIX_KEY}: expected mapping.")
    matrix = _normalized_matrix(dashboard_name, value)
    if matrix.get("enabled", True) is False:
        return None
    _validate_matrix(dashboard_name, matrix)
    return matrix


def append_matrix_tasks(
    config: Any,
    dashboard: dict[str, Any],
    descriptors: list[PanelDescriptor],
    panels: list[Panel],
    render_tasks: list[PanelRenderTask],
    timestamps: list[Any],
    session: Any = None,
) -> list[PanelRenderTask]:
    matrix = getattr(config, "render_matrix", None)
    if not matrix:
        return render_tasks
    rows_by_time = _rows_by_timestamp(config, matrix, dashboard, timestamps, getattr(config, "vars", None) or {}, session)
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


def _normalized_matrix(dashboard_name: str, value: dict[str, Any]) -> dict[str, Any]:
    matrix = dict(value)
    _reject_flat_layout(dashboard_name, matrix)
    nested_options = matrix.pop("options", None)
    variables = matrix.pop("variables", None)
    normalized = _flat_options(matrix)
    normalized.update(_nested_options(dashboard_name, nested_options))
    normalized_variables = variables if variables is not None else _legacy_variables(matrix)
    if normalized_variables:
        normalized["variables"] = normalized_variables
    return normalized


def _flat_options(matrix: dict[str, Any]) -> dict[str, Any]:
    return {key: matrix.pop(key) for key in list(matrix) if key in MATRIX_FLAT_OPTION_KEYS}


def _reject_flat_layout(dashboard_name: str, matrix: dict[str, Any]) -> None:
    if "layout" not in matrix:
        return
    raise ConfigurationError(_path(dashboard_name, "layout") + ": use render_matrix.options.layout.")


def _nested_options(dashboard_name: str, options: Any) -> dict[str, Any]:
    if options in (None, ""):
        return {}
    if not isinstance(options, dict):
        raise ConfigurationError(_path(dashboard_name, "options") + ": expected mapping.")
    unknown = sorted(str(key) for key in options if key not in MATRIX_NESTED_OPTION_KEYS)
    if unknown:
        raise ConfigurationError(_path(dashboard_name, "options") + f": unknown option(s) {unknown}.")
    return dict(options)


def _legacy_variables(matrix: dict[str, Any]) -> dict[str, Any]:
    return {key: matrix.pop(key) for key in list(matrix)}


def _dashboard_link_variables(config: Any, row: dict[str, Any]) -> dict[str, Any]:
    variables = dict(getattr(config, "vars", None) or {})
    variables.update(row["url_variables"])
    return variables


def _validate_matrix(dashboard_name: str, matrix: dict[str, Any]) -> None:
    variables = matrix.get("variables")
    if not isinstance(variables, dict) or not variables:
        raise ConfigurationError(_path(dashboard_name, "variables") + ": expected non-empty mapping.")
    if matrix.get("combination_mode", "product") not in MATRIX_MODES:
        raise ConfigurationError(_path(dashboard_name, "combination_mode") + ": expected product or zip.")
    _validate_layout(dashboard_name, matrix)
    _validate_max_rows(dashboard_name, matrix)
    _validate_group_by(dashboard_name, matrix, variables)
    _validate_variable_specs(dashboard_name, variables)
    _validate_label_template(dashboard_name, matrix, variables)


def _validate_layout(dashboard_name: str, matrix: dict[str, Any]) -> None:
    layout = matrix.get("layout", "dashboard_first")
    if layout in MATRIX_LAYOUTS:
        return
    expected = ", ".join(sorted(MATRIX_LAYOUTS))
    raise ConfigurationError(_path(dashboard_name, "layout") + f": expected one of [{expected}].")


def _validate_group_by(dashboard_name: str, matrix: dict[str, Any], variables: dict[str, Any]) -> None:
    group_by = matrix.get("row_grouping", matrix.get("group_by", []))
    if group_by in (None, ""):
        return
    if not isinstance(group_by, list) or not all(isinstance(item, str) for item in group_by):
        raise ConfigurationError(_path(dashboard_name, "row_grouping") + ": expected list[str].")
    unknown = [item for item in group_by if item not in variables]
    if unknown:
        raise ConfigurationError(_path(dashboard_name, "row_grouping") + f": unknown variables {unknown}.")


def _validate_variable_specs(dashboard_name: str, variables: dict[str, Any]) -> None:
    aliases: set[str] = set()
    previous: set[str] = set()
    for key, spec in variables.items():
        if not isinstance(key, str) or not key:
            raise ConfigurationError(_path(dashboard_name, "variables") + ": variable keys must be non-empty strings.")
        if not isinstance(spec, dict):
            raise ConfigurationError(_path(dashboard_name, f"variables.{key}") + ": expected mapping.")
        _validate_grafana_variable(dashboard_name, key, spec)
        _validate_alias(dashboard_name, key, spec, aliases)
        _validate_value_source(dashboard_name, key, spec)
        _validate_dependencies(dashboard_name, key, spec, previous)
        previous.add(key)


def _validate_max_rows(dashboard_name: str, matrix: dict[str, Any]) -> None:
    max_rows = matrix.get("max_rows", DEFAULT_MAX_MATRIX_ROWS)
    if isinstance(max_rows, int) and not isinstance(max_rows, bool) and max_rows > 0:
        return
    raise ConfigurationError(_path(dashboard_name, "max_rows") + ": expected positive integer.")


def _validate_grafana_variable(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    grafana_variable = spec.get("grafana_variable")
    if grafana_variable in (None, ""):
        return
    if isinstance(grafana_variable, str):
        return
    raise ConfigurationError(_path(dashboard_name, f"variables.{key}.grafana_variable") + ": expected non-empty string.")


def _validate_dependencies(dashboard_name: str, key: str, spec: dict[str, Any], previous: set[str]) -> None:
    dependencies = _configured_dependencies(spec)
    if "values_by" in spec and not dependencies:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.depends_on") + ": required for values_by.")
    unknown = [dependency for dependency in dependencies if dependency not in previous]
    if unknown:
        raise ConfigurationError(
            _path(dashboard_name, f"variables.{key}.depends_on")
            + f": unknown or later dependencies {unknown}."
        )


def _configured_dependencies(spec: dict[str, Any]) -> list[str]:
    depends_on = spec.get("depends_on")
    if depends_on in (None, ""):
        return []
    if isinstance(depends_on, str):
        return [depends_on]
    if isinstance(depends_on, list) and all(isinstance(item, str) and item for item in depends_on):
        return list(depends_on)
    return [""]


def _validate_alias(dashboard_name: str, key: str, spec: dict[str, Any], aliases: set[str]) -> None:
    alias = spec.get("alias", key)
    if not isinstance(alias, str) or not alias:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.alias") + ": expected non-empty string.")
    if alias in aliases:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.alias") + ": duplicate metadata alias.")
    aliases.add(alias)


def _validate_value_source(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    sources = [name for name in ("values", "values_by", "values_from") if name in spec]
    if len(sources) != 1:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}") + ": expected exactly one value source.")
    _validate_source_shape(dashboard_name, key, spec, sources[0])
    _validate_regex(dashboard_name, key, spec)
    _validate_max_values(dashboard_name, key, spec)


def _validate_source_shape(dashboard_name: str, key: str, spec: dict[str, Any], source: str) -> None:
    value = spec.get(source)
    if source == "values" and isinstance(value, list) and value:
        return
    if source == "values_by" and _valid_values_by(value):
        return
    if source == "values_from" and _valid_values_from(value, key, spec):
        return
    raise ConfigurationError(_path(dashboard_name, f"variables.{key}.{source}") + ": invalid value source.")


def _valid_values_from(value: Any, key: str, spec: dict[str, Any]) -> bool:
    if isinstance(value, str) and value:
        return value == _grafana_variable(key, spec)
    if not isinstance(value, dict):
        return False
    return not set(value) - {"regex", "max_values"}


def _valid_values_by(value: Any) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    return all(isinstance(items, list) and bool(items) for items in value.values())


def _validate_regex(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    regex = _regex_value(spec)
    if regex in (None, ""):
        return
    try:
        re.compile(str(regex))
    except re.error as error:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.regex") + f": invalid regex ({error}).") from error


def _validate_max_values(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    max_values = _max_values(spec)
    if max_values is None:
        return
    if not isinstance(max_values, int) or isinstance(max_values, bool) or max_values <= 0:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.max_values") + ": expected positive integer.")


def _rows_by_timestamp(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                       timestamps: list[Any], static_vars: dict[str, Any], session: Any) -> dict[int, list[dict[str, Any]]]:
    return {
        timestamp.id_time: _rows_for_timestamp(config, matrix, dashboard, timestamp, static_vars, session)
        for timestamp in timestamps
    }


def _rows_for_timestamp(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                         timestamp: Any, static_vars: dict[str, Any], session: Any) -> list[dict[str, Any]]:
    dashboard_name = config.name
    if matrix.get("combination_mode", "product") == "product":
        rows = _product_rows(config, matrix, dashboard, timestamp, static_vars, session)
        if not rows:
            raise ConfigurationError(_path(dashboard_name, "variables") + ": no rows resolved.")
        _validate_row_limit(dashboard_name, matrix, len(rows))
        return [_row_record(dashboard_name, matrix, index, row) for index, row in enumerate(rows)]
    values, provenance = _variable_values(config, matrix, dashboard, timestamp, static_vars, session)
    rows = _zip_rows(values, dashboard_name)
    _validate_row_limit(dashboard_name, matrix, len(rows))
    rows = [{**row, "__discovery__": provenance} for row in rows]
    return [_row_record(dashboard_name, matrix, index, row) for index, row in enumerate(rows)]


def _product_rows(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                  timestamp: Any, static_vars: dict[str, Any], session: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, str]] = [{}]
    for key, spec in matrix["variables"].items():
        rows = _extend_rows(config, matrix, key, spec, dashboard, timestamp, static_vars, rows, session)
    return rows


def _extend_rows(config: Any, matrix: dict[str, Any], key: str, spec: dict[str, Any],
                 dashboard: dict[str, Any], timestamp: Any, static_vars: dict[str, Any],
                  rows: list[dict[str, Any]], session: Any) -> list[dict[str, Any]]:
    extended: list[dict[str, str]] = []
    for row in rows:
        values, provenance = _values_for_spec(config, key, spec, dashboard, timestamp, row, static_vars, session)
        if not values:
            logger.warning(
                "Render matrix branch skipped dashboard=%s variable=%s timestamp_id=%s context=%s reason=no_values_resolved",
                config.name,
                key,
                timestamp.id_time,
                {name: value for name, value in row.items() if not str(name).startswith("__")},
            )
            continue
        for value in values:
            extended.append(_extended_row(row, key, value, provenance))
            _validate_row_limit(config.name, matrix, len(extended))
    return extended


def _variable_values(config: Any, matrix: dict[str, Any], dashboard: dict[str, Any],
                     timestamp: Any, static_vars: dict[str, Any], session: Any) -> tuple[dict[str, list[str]], dict[str, Any]]:
    values: dict[str, list[str]] = {}
    provenance: dict[str, Any] = {}
    for key, spec in matrix["variables"].items():
        values[key], variable_provenance = _values_for_spec(config, key, spec, dashboard, timestamp, {}, static_vars, session)
        if variable_provenance:
            provenance[key] = variable_provenance
    return values, provenance


def _values_for_spec(config: Any, key: str, spec: dict[str, Any], dashboard: dict[str, Any], timestamp: Any,
                     context: dict[str, Any], static_vars: dict[str, Any], session: Any) -> tuple[list[str], dict[str, Any] | None]:
    provenance = None
    if "values" in spec:
        values = _scalar_list(spec.get("values"))
    elif "values_by" in spec:
        values = _values_by_context(config.name, key, spec, context)
    else:
        result = resolve_values_from(key, spec, dashboard, timestamp, _discovery_context(config.render_matrix, context), static_vars, session, config)
        values, provenance = result.values, result.provenance
    values = _filtered_values(spec, values)
    return values, provenance


def _validate_label_template(dashboard_name: str, matrix: dict[str, Any], variables: dict[str, Any]) -> None:
    template = matrix.get("label_template")
    if template in (None, ""):
        return
    if not isinstance(template, str):
        raise ConfigurationError(_path(dashboard_name, "label_template") + ": expected string.")
    allowed = set(variables) | {_alias(key, spec) for key, spec in variables.items()}
    unknown = sorted({field for _, field, _, _ in Formatter().parse(template) if field and field not in allowed})
    if unknown:
        raise ConfigurationError(_path(dashboard_name, "label_template") + f": unknown placeholders {unknown}.")


def _extended_row(row: dict[str, Any], key: str, value: str, provenance: dict[str, Any] | None) -> dict[str, Any]:
    extended = {**row, key: value}
    if provenance:
        extended["__discovery__"] = {**row.get("__discovery__", {}), key: provenance}
    return extended


def _discovery_context(matrix: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    public_context = {key: value for key, value in context.items() if not str(key).startswith("__")}
    grafana_context = {_grafana_variable(key, matrix["variables"][key]): value for key, value in public_context.items()}
    return {**public_context, **grafana_context}


def _values_by_context(dashboard_name: str, key: str, spec: dict[str, Any], context: dict[str, str]) -> list[str]:
    mapping = spec.get("values_by")
    if not isinstance(mapping, dict):
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.values_by") + ": expected mapping.")
    deps = _configured_dependencies(spec)
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
    regex = _regex_value(spec)
    if regex:
        pattern = re.compile(str(regex))
        values = [value for value in values if pattern.search(value)]
    return _dedupe(values)[:_max_values(spec)]


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


def _regex_value(spec: dict[str, Any]) -> Any:
    source = spec.get("values_from")
    return source.get("regex") if isinstance(source, dict) and "regex" in source else spec.get("regex")


def _max_values(spec: dict[str, Any]) -> int:
    source = spec.get("values_from")
    value = source.get("max_values") if isinstance(source, dict) and "max_values" in source else spec.get("max_values")
    return value if value is not None else DEFAULT_MAX_MATRIX_VALUES


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
