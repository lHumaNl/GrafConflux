"""Panel variant planning for Grafana render tasks."""

from __future__ import annotations

import hashlib
import itertools
import re
from dataclasses import replace
from string import Formatter
from typing import Any

from grafconflux._shared.grafana_models import ConfigurationError, Panel, PanelDescriptor, PanelRenderTask

DEFAULT_MAX_VARIANT_VALUES = 20
PANEL_VARIANTS_KEY = "panel_variants"


def validated_panel_variants(dashboard_name: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    value = config.get(PANEL_VARIANTS_KEY, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ConfigurationError(f"dashboards.{dashboard_name}.{PANEL_VARIANTS_KEY}: expected list of mappings.")
    for index, rule in enumerate(value):
        _validate_rule(dashboard_name, index, rule)
    return list(value)


def append_variant_tasks(
    config: Any,
    dashboard: dict[str, Any],
    descriptors: list[PanelDescriptor],
    panels: list[Panel],
    render_tasks: list[PanelRenderTask],
    timestamps: list[Any],
) -> list[PanelRenderTask]:
    if not getattr(config, "panel_variants", []):
        return render_tasks
    if _has_matrix_tasks(render_tasks):
        return _append_matrix_variant_tasks(config, dashboard, descriptors, panels, render_tasks, timestamps)
    panel_by_id = {panel.panel_id: panel for panel in panels}
    tasks = list(render_tasks)
    for rule_index, rule in enumerate(config.panel_variants):
        matched = _matched_descriptors(config.name, rule_index, rule, descriptors)
        tasks = _ensure_source_tasks(config, tasks, panel_by_id, matched, rule)
        _append_rule_tasks(config, dashboard, tasks, panel_by_id, matched, rule, rule_index, timestamps)
    return tasks


def _has_matrix_tasks(render_tasks: list[PanelRenderTask]) -> bool:
    return any((task.artifact or {}).get("artifact_type") == "matrix" for task in render_tasks)


def _append_matrix_variant_tasks(
    config: Any,
    dashboard: dict[str, Any],
    descriptors: list[PanelDescriptor],
    panels: list[Panel],
    render_tasks: list[PanelRenderTask],
    timestamps: list[Any],
) -> list[PanelRenderTask]:
    tasks = list(render_tasks)
    panel_by_id = {panel.panel_id: panel for panel in panels}
    descriptor_by_id = {descriptor.panel_id: descriptor for descriptor in descriptors}
    for rule_index, rule in enumerate(config.panel_variants):
        matched = _matched_descriptors(config.name, rule_index, rule, descriptors)
        variants = _expanded_variants(config.name, dashboard, rule, rule_index)
        source_tasks = _matched_matrix_source_tasks(tasks, matched)
        if rule.get("include_source_panel", True) is False:
            _hide_source_matrix_artifacts(source_tasks)
        for source_task in source_tasks:
            panel = panel_by_id[source_task.panel.panel_id]
            descriptor = descriptor_by_id[source_task.panel.panel_id]
            _append_matrix_task_variants(config, tasks, panel, descriptor, source_task, rule, rule_index, variants)
    return tasks


def _matched_matrix_source_tasks(tasks: list[PanelRenderTask], descriptors: list[PanelDescriptor]) -> list[PanelRenderTask]:
    source_ids = {descriptor.panel_id for descriptor in descriptors}
    return [task for task in tasks if task.panel.panel_id in source_ids and (task.artifact or {}).get("artifact_type") == "matrix"]


def _hide_source_matrix_artifacts(tasks: list[PanelRenderTask]) -> None:
    for task in tasks:
        task.artifact.setdefault("confluence", {})["visible"] = False
        task.artifact["confluence"]["hidden_reason"] = "variant_source"


def _append_matrix_task_variants(config: Any, tasks: list[PanelRenderTask], panel: Panel, descriptor: PanelDescriptor,
                                 source_task: PanelRenderTask, rule: dict[str, Any], rule_index: int,
                                 variants: list[dict[str, Any]]) -> None:
    for variant in variants:
        artifact = _matrix_variant_artifact(config.name, panel, source_task, rule, rule_index, variant)
        panel.artifacts.append(artifact)
        tasks.append(PanelRenderTask(panel, source_task.timestamp, _variant_variables(config, variant, source_task),
                                     artifact["png_file"], artifact, raw_panel=descriptor.raw_panel,
                                     collect_no_data_panels=config.collect_no_data_panels))


def _validate_rule(dashboard_name: str, index: int, rule: dict[str, Any]) -> None:
    selectors = rule.get("selectors")
    if not isinstance(selectors, dict):
        raise ConfigurationError(_path(dashboard_name, index, "selectors") + ": expected mapping.")
    if not any(key in selectors for key in ("panel_id", "title", "title_regex")):
        raise ConfigurationError(_path(dashboard_name, index, "selectors") + ": expected panel_id, title, or title_regex.")
    variables = rule.get("variables")
    if not isinstance(variables, dict) or not variables:
        raise ConfigurationError(_path(dashboard_name, index, "variables") + ": expected non-empty mapping.")
    _validate_combination_mode(dashboard_name, index, rule)
    _validate_label_template(dashboard_name, index, rule, variables)


def _validate_combination_mode(dashboard_name: str, index: int, rule: dict[str, Any]) -> None:
    mode = rule.get("combination_mode", "product")
    if mode not in {"product", "zip"}:
        raise ConfigurationError(_path(dashboard_name, index, "combination_mode") + ": expected product or zip.")


def _path(dashboard_name: str, index: int, suffix: str) -> str:
    return f"dashboards.{dashboard_name}.{PANEL_VARIANTS_KEY}[{index}].{suffix}"


def _matched_descriptors(dashboard_name: str, rule_index: int, rule: dict[str, Any], descriptors: list[PanelDescriptor]) -> list[PanelDescriptor]:
    selectors = rule["selectors"]
    matched = [descriptor for descriptor in descriptors if _descriptor_matches(descriptor, selectors)]
    if not matched:
        raise ConfigurationError(_path(dashboard_name, rule_index, "selectors") + ": matched no panels.")
    if len(matched) > 1 and not selectors.get("allow_multiple", False):
        raise ConfigurationError(_path(dashboard_name, rule_index, "selectors") + ": matched multiple panels; set allow_multiple: true.")
    return matched


def _descriptor_matches(descriptor: PanelDescriptor, selectors: dict[str, Any]) -> bool:
    if selectors.get("type") not in (None, descriptor.graph_type):
        return False
    if "panel_id" in selectors and selectors["panel_id"] == descriptor.panel_id:
        return True
    if "title" in selectors and selectors["title"] == (descriptor.title or ""):
        return True
    return _matches_title_regex(descriptor.title or "", selectors.get("title_regex"))


def _matches_title_regex(title: str, pattern: Any) -> bool:
    if pattern in (None, ""):
        return False
    return re.search(str(pattern), title) is not None


def _ensure_source_tasks(
    config: Any,
    tasks: list[PanelRenderTask],
    panel_by_id: dict[int, Panel],
    descriptors: list[PanelDescriptor],
    rule: dict[str, Any],
) -> list[PanelRenderTask]:
    if rule.get("include_source_panel", True) is False:
        return tasks
    source_ids = {descriptor.panel_id for descriptor in descriptors}
    return [_with_normal_artifact(config, task) if task.panel.panel_id in source_ids else task for task in tasks]


def _with_normal_artifact(config: Any, task: PanelRenderTask) -> PanelRenderTask:
    if task.artifact is not None:
        task.artifact.setdefault("artifact_type", "normal")
        return task
    artifact = _normal_artifact(task, config.name)
    task.panel.artifacts.append(artifact)
    return replace(task, artifact=artifact, file_name=artifact["png_file"])


def _normal_artifact(task: PanelRenderTask, dashboard_name: str) -> dict[str, Any]:
    timestamp = task.timestamp
    return {
        "artifact_type": "normal",
        "timestamp_id": timestamp.id_time,
        "timestamp_tag": timestamp.time_tag,
        "from": str(timestamp.start_time_timestamp),
        "to": str(timestamp.end_time_timestamp),
        "render_status": "rendered",
        "png_file": f"{dashboard_name}__{task.panel.panel_id}__{timestamp.id_time}.png",
        "skip_reason": None,
    }


def _append_rule_tasks(
    config: Any,
    dashboard: dict[str, Any],
    tasks: list[PanelRenderTask],
    panel_by_id: dict[int, Panel],
    descriptors: list[PanelDescriptor],
    rule: dict[str, Any],
    rule_index: int,
    timestamps: list[Any],
) -> None:
    variants = _expanded_variants(config.name, dashboard, rule, rule_index)
    for descriptor in descriptors:
        panel = panel_by_id[descriptor.panel_id]
        for timestamp in timestamps:
            _append_timestamp_variants(config, tasks, panel, descriptor, timestamp, rule, rule_index, variants)


def _append_timestamp_variants(config: Any, tasks: list[PanelRenderTask], panel: Panel, descriptor: PanelDescriptor,
                               timestamp: Any, rule: dict[str, Any], rule_index: int, variants: list[dict[str, Any]]) -> None:
    for variant in variants:
        artifact = _variant_artifact(config.name, panel, timestamp, rule, rule_index, variant)
        panel.artifacts.append(artifact)
        tasks.append(PanelRenderTask(panel, timestamp, _variant_variables(config, variant), artifact["png_file"], artifact,
                                     raw_panel=descriptor.raw_panel, collect_no_data_panels=config.collect_no_data_panels))


def _expanded_variants(dashboard_name: str, dashboard: dict[str, Any], rule: dict[str, Any], rule_index: int) -> list[dict[str, Any]]:
    variable_values = {name: _variable_values(dashboard_name, dashboard, rule_index, name, spec) for name, spec in rule["variables"].items()}
    mappings = _combined_variable_maps(variable_values, rule.get("combination_mode", "product"), dashboard_name, rule_index)
    return [_variant_record(rule, rule_index, index, mapping, dashboard_name) for index, mapping in enumerate(mappings)]


def _variable_values(dashboard_name: str, dashboard: dict[str, Any], rule_index: int, name: str, spec: Any) -> list[str]:
    if not isinstance(spec, dict):
        raise ConfigurationError(_path(dashboard_name, rule_index, f"variables.{name}") + ": expected mapping.")
    values = spec.get("values") if "values" in spec else _matched_variable_values(dashboard, name, spec.get("match_values"))
    values = [str(value) for value in (values or [])]
    max_values = spec.get("max_values", spec.get("match_values", {}).get("max_values", DEFAULT_MAX_VARIANT_VALUES) if isinstance(spec.get("match_values"), dict) else DEFAULT_MAX_VARIANT_VALUES)
    if len(values) > max_values:
        values = values[:max_values]
    if not values:
        raise ConfigurationError(_path(dashboard_name, rule_index, f"variables.{name}") + ": no values resolved.")
    return values


def _matched_variable_values(dashboard: dict[str, Any], name: str, match_values: Any) -> list[str]:
    if not isinstance(match_values, dict):
        return []
    pattern = re.compile(str(match_values.get("regex", ".*")))
    return [value for value in _dashboard_variable_options(dashboard, name) if pattern.search(value)]


def _dashboard_variable_options(dashboard: dict[str, Any], name: str) -> list[str]:
    variables = dashboard.get("templating", {}).get("list", [])
    variable = next((item for item in variables if item.get("name") == name), None)
    options = variable.get("options", []) if isinstance(variable, dict) else []
    return [_option_value(option) for option in options if _option_value(option) not in (None, "")]


def _option_value(option: Any) -> str | None:
    if not isinstance(option, dict):
        return str(option)
    value = option.get("value", option.get("text"))
    return None if value is None else str(value)


def _combined_variable_maps(values: dict[str, list[str]], mode: str, dashboard_name: str, rule_index: int) -> list[dict[str, str]]:
    names = list(values)
    if mode == "zip":
        _validate_zip_lengths(values, dashboard_name, rule_index)
        return [dict(zip(names, row)) for row in zip(*(values[name] for name in names))]
    return [dict(zip(names, row)) for row in itertools.product(*(values[name] for name in names))]


def _validate_zip_lengths(values: dict[str, list[str]], dashboard_name: str, rule_index: int) -> None:
    lengths = {len(items) for items in values.values()}
    if len(lengths) > 1:
        raise ConfigurationError(_path(dashboard_name, rule_index, "combination_mode") + ": zip variable lists must have equal length.")


def _variant_record(rule: dict[str, Any], rule_index: int, variant_index: int, variables: dict[str, str], dashboard_name: str) -> dict[str, Any]:
    variant_hash = _stable_hash(dashboard_name, rule_index, variant_index, variables)
    return {"index": variant_index, "hash": variant_hash, "variables": variables, "label": _variant_label(rule, variant_index, variables)}


def _variant_label(rule: dict[str, Any], variant_index: int, variables: dict[str, str]) -> str:
    template = rule.get("label_template")
    if isinstance(template, str) and template:
        return _safe_format(template, variables)
    values = ", ".join(f"{key}={value}" for key, value in variables.items())
    return f"Variant {variant_index + 1}: {values}" if values else f"Variant {variant_index + 1}"


def _safe_format(template: str, variables: dict[str, str]) -> str:
    allowed = {field for _, field, _, _ in Formatter().parse(template) if field}
    return template.format(**{name: variables.get(name, "") for name in allowed})


def _variant_artifact(dashboard_name: str, panel: Panel, timestamp: Any, rule: dict[str, Any], rule_index: int,
                      variant: dict[str, Any]) -> dict[str, Any]:
    file_name = f"{dashboard_name}__{panel.panel_id}__variant-{rule_index:02d}-{variant['index']:03d}-{variant['hash']}__{timestamp.id_time}.png"
    return {
        "artifact_type": "variant", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
        "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
        "render_status": "rendered", "png_file": file_name, "skip_reason": None,
        "source_panel_id": panel.panel_id, "source_panel_type": panel.type,
        "source_panel_title": panel.title, "source_panel_display_title": panel.display_title,
        "source_timestamp_id": timestamp.id_time, "source_timestamp_tag": timestamp.time_tag,
        "variant": {"rule_name": rule.get("name"), "rule_index": rule_index, "variant_index": variant["index"],
                     "hash": variant["hash"], "variables": variant["variables"], "label": variant["label"]},
    }


def _matrix_variant_artifact(dashboard_name: str, panel: Panel, source_task: PanelRenderTask, rule: dict[str, Any],
                             rule_index: int, variant: dict[str, Any]) -> dict[str, Any]:
    source = source_task.artifact or {}
    matrix = dict(source.get("matrix") or {})
    matrix["variant"] = {"rule_name": rule.get("name"), "rule_index": rule_index, "variant_index": variant["index"],
                          "hash": variant["hash"], "variables": variant["variables"], "label": variant["label"]}
    matrix = _effective_matrix_metadata(matrix, variant)
    timestamp = source_task.timestamp
    file_name = _matrix_variant_file_name(dashboard_name, panel, source, rule_index, variant, timestamp)
    return {"artifact_type": "matrix", "timestamp_id": timestamp.id_time, "timestamp_tag": timestamp.time_tag,
            "from": str(timestamp.start_time_timestamp), "to": str(timestamp.end_time_timestamp),
            "render_status": "rendered", "png_file": file_name, "skip_reason": None,
            "source_panel_id": panel.panel_id, "source_panel_type": panel.type,
            "source_panel_title": panel.title, "source_panel_display_title": panel.display_title,
            "source_timestamp_id": timestamp.id_time,
            "display_title": _matrix_variant_display_title(panel.display_title, matrix, variant),
            "source_artifact": {"artifact_type": source.get("artifact_type"), "png_file": source.get("png_file"),
                                 "matrix_hash": (source.get("matrix") or {}).get("hash")},
            "variant": matrix["variant"], "matrix": matrix}


def _effective_grafana_variables(matrix: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    variables = dict(matrix.get("grafana_variables") or {})
    variables.update(variant["variables"])
    return variables


def _effective_matrix_metadata(matrix: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    effective = dict(matrix)
    effective["grafana_variables"] = _effective_grafana_variables(matrix, variant)
    effective["context_path"] = _effective_context_path(matrix, effective["grafana_variables"])
    effective["variables"] = _effective_alias_variables(effective["context_path"], matrix)
    effective["group"] = _context_group(matrix.get("group"), effective["context_path"])
    effective["label"] = _context_label(effective["context_path"])
    return effective


def _effective_context_path(matrix: dict[str, Any], variables: dict[str, Any]) -> list[dict[str, Any]]:
    context_path = []
    for item in matrix.get("context_path") or []:
        updated = dict(item)
        grafana_variable = str(updated.get("grafana_variable") or updated.get("key") or "")
        key = str(updated.get("key") or "")
        if grafana_variable in variables:
            updated["value"] = variables[grafana_variable]
        elif key in variables:
            updated["value"] = variables[key]
        context_path.append(updated)
    return context_path


def _effective_alias_variables(context_path: list[dict[str, Any]], matrix: dict[str, Any]) -> dict[str, Any]:
    if context_path:
        return {str(item.get("label") or item.get("key") or "Variable"): item.get("value") for item in context_path}
    return dict(matrix.get("variables") or {})


def _context_group(current_group: Any, context_path: list[dict[str, Any]]) -> str | None:
    if not current_group or not context_path:
        return current_group
    group_parts = [part.strip() for part in str(current_group).split(",") if part.strip()]
    by_label = {str(item.get("label") or item.get("key") or "Variable"): item.get("value") for item in context_path}
    updated_parts = []
    for part in group_parts:
        label, _, _ = part.partition(":")
        clean_label = label.strip()
        updated_parts.append(f"{clean_label}: {by_label[clean_label]}" if clean_label in by_label else part)
    return ", ".join(updated_parts)


def _context_label(context_path: list[dict[str, Any]]) -> str:
    return ", ".join(f"{item.get('label') or item.get('key')}: {item.get('value')}" for item in context_path)


def _matrix_variant_display_title(panel_title: str, matrix: dict[str, Any], variant: dict[str, Any]) -> str:
    matrix_label = matrix.get("label")
    if not matrix_label:
        return _combined_matrix_variant_label(panel_title, variant["label"])
    if _variant_overrides_matrix_context(matrix, variant):
        return f"{panel_title} ({matrix_label})"
    return f"{panel_title} ({_combined_matrix_variant_label(matrix_label, variant['label'])})"


def _variant_overrides_matrix_context(matrix: dict[str, Any], variant: dict[str, Any]) -> bool:
    context_keys = {
        str(item.get("key") or "") for item in matrix.get("context_path") or []
    } | {
        str(item.get("grafana_variable") or "") for item in matrix.get("context_path") or []
    }
    return any(name in context_keys for name in variant["variables"])


def _validate_label_template(dashboard_name: str, index: int, rule: dict[str, Any], variables: dict[str, Any]) -> None:
    template = rule.get("label_template")
    if template in (None, ""):
        return
    if not isinstance(template, str):
        raise ConfigurationError(_path(dashboard_name, index, "label_template") + ": expected string.")
    allowed = set(variables)
    unknown = sorted({field for _, field, _, _ in Formatter().parse(template) if field and field not in allowed})
    if unknown:
        raise ConfigurationError(_path(dashboard_name, index, "label_template") + f": unknown placeholders {unknown}.")


def _combined_matrix_variant_label(matrix_label: Any, variant_label: str) -> str:
    return f"{matrix_label} / {variant_label}" if matrix_label else variant_label


def _matrix_variant_file_name(dashboard_name: str, panel: Panel, source: dict[str, Any], rule_index: int,
                              variant: dict[str, Any], timestamp: Any) -> str:
    matrix_hash = (source.get("matrix") or {}).get("hash", "matrix")
    return f"{dashboard_name}__{panel.panel_id}__matrix-{matrix_hash}-variant-{rule_index:02d}-{variant['index']:03d}-{variant['hash']}__{timestamp.id_time}.png"


def _variant_variables(config: Any, variant: dict[str, Any], source_task: PanelRenderTask | None = None) -> dict[str, Any]:
    variables = dict((source_task.variables if source_task else None) or config.vars or {})
    variables.update(variant["variables"])
    return variables


def _stable_hash(dashboard_name: str, rule_index: int, variant_index: int, variables: dict[str, str]) -> str:
    payload = repr((dashboard_name, rule_index, variant_index, sorted(variables.items())))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]
