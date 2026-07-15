"""Validation and normalization for dashboard render-matrix configuration."""

from __future__ import annotations

import copy
import re
from string import Formatter
from typing import Any

from grafconflux._grafana.matrix_dependencies import configured_dependencies
from grafconflux._shared.grafana_models import ConfigurationError
from grafconflux._shared.matrix_layout import DEFAULT_MATRIX_LAYOUT, MATRIX_LAYOUTS
from grafconflux._shared.presentation import display_name, resolved_hidden

RENDER_MATRIX_KEY = "render_matrix"
DEFAULT_MAX_MATRIX_VALUES = 50
DEFAULT_MAX_MATRIX_ROWS = 500
MATRIX_MODES = {"product", "zip"}
MATRIX_NESTED_OPTION_KEYS = {
    "enabled", "row_grouping", "group_by", "combination_mode", "label_template", "max_rows", "layout",
}
MATRIX_FLAT_OPTION_KEYS = MATRIX_NESTED_OPTION_KEYS - {"layout"}
DYNAMIC_SOURCE_KEYS = {"regex", "max_values", "filters_by_parent", "grouping"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
GROUPING_LAYOUTS = {"matrix_grouped_panels", "matrix_values_first"}


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


def _normalized_matrix(dashboard_name: str, value: dict[str, Any]) -> dict[str, Any]:
    matrix = copy.deepcopy(value)
    _reject_flat_layout(dashboard_name, matrix)
    nested_options = matrix.pop("options", None)
    variables = matrix.pop("variables", None)
    normalized = _flat_options(matrix)
    normalized.update(_nested_options(dashboard_name, nested_options))
    normalized.setdefault("layout", DEFAULT_MATRIX_LAYOUT)
    normalized_variables = variables if variables is not None else _legacy_variables(matrix)
    if normalized_variables:
        normalized["variables"] = _normalized_variables(normalized_variables)
    return normalized


def _normalized_variables(variables: Any) -> Any:
    if not isinstance(variables, dict):
        return variables
    return {key: _normalized_variable(key, spec) for key, spec in variables.items()}


def _normalized_variable(key: Any, spec: Any) -> Any:
    spec = _variable_with_automatic_source(spec)
    if not isinstance(spec, dict):
        return spec
    normalized = dict(spec)
    source = normalized.get("values_from")
    if isinstance(source, dict):
        source = dict(source)
        if "filters_by_parent" in source and isinstance(source["filters_by_parent"], list):
            source["filters_by_parent"] = [
                _normalized_parent_filter(item) for item in source["filters_by_parent"]
            ]
        if "grouping" in source and isinstance(source["grouping"], dict):
            dimension = source["grouping"].get("dimension")
            normalized["__group_hide_explicit__"] = isinstance(dimension, dict) and "hide" in dimension
            source["grouping"] = _normalized_grouping(str(key), normalized, source["grouping"])
        normalized["values_from"] = source
    return normalized


def _normalized_parent_filter(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    normalized = dict(item)
    if isinstance(normalized.get("when"), dict):
        normalized["when"] = _normalized_when(normalized["when"])
    normalized.setdefault("mode", "and")
    return normalized


def _normalized_when(when: dict[Any, Any]) -> dict[Any, Any]:
    return {
        key: [_normalized_scalar(item) for item in value]
        if isinstance(value, list) else [_normalized_scalar(value)]
        for key, value in when.items()
    }


def _normalized_scalar(value: Any) -> Any:
    return value if value is None or isinstance(value, (dict, list)) else str(value)


def _normalized_grouping(key: str, spec: dict[str, Any], grouping: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(grouping)
    dimension = normalized.get("dimension", {})
    if isinstance(dimension, dict):
        dimension = dict(dimension)
        dimension.setdefault("key", f"{key}_group")
        dimension.setdefault("display_name", f"{display_name(key, spec)} Group")
        dimension.setdefault("hide", False)
        normalized["dimension"] = dimension
    rules = normalized.get("rules", [])
    if isinstance(rules, list):
        normalized["rules"] = [_normalized_group_rule(rule) for rule in rules]
    unmatched = normalized.get("unmatched", {})
    if isinstance(unmatched, dict):
        unmatched = dict(unmatched)
        unmatched.setdefault("enabled", False)
        unmatched.setdefault("name", "ungrouped")
        unmatched.setdefault("label", "Ungrouped")
        normalized["unmatched"] = unmatched
    capture = normalized.get("capture")
    if isinstance(capture, dict):
        capture = dict(capture)
        if isinstance(capture.get("when"), dict):
            capture["when"] = _normalized_when(capture["when"])
        normalized["capture"] = capture
    return normalized


def _normalized_group_rule(rule: Any) -> Any:
    if not isinstance(rule, dict):
        return rule
    normalized = dict(rule)
    if isinstance(normalized.get("name"), str):
        normalized.setdefault("label", normalized["name"])
    if isinstance(normalized.get("when"), dict):
        normalized["when"] = _normalized_when(normalized["when"])
    return normalized


def _variable_with_automatic_source(spec: Any) -> Any:
    if not isinstance(spec, dict) or _has_value_source(spec):
        return spec
    return {**spec, "values_from": {}}


def _has_value_source(spec: dict[str, Any]) -> bool:
    return any(name in spec for name in ("values", "values_by", "values_from"))


def _flat_options(matrix: dict[str, Any]) -> dict[str, Any]:
    return {key: matrix.pop(key) for key in list(matrix) if key in MATRIX_FLAT_OPTION_KEYS}


def _reject_flat_layout(dashboard_name: str, matrix: dict[str, Any]) -> None:
    if "layout" in matrix:
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
    _validate_dynamic_matrix_constraints(dashboard_name, matrix, variables)
    _validate_label_template(dashboard_name, matrix, variables)


def _validate_layout(dashboard_name: str, matrix: dict[str, Any]) -> None:
    layout = matrix.get("layout", DEFAULT_MATRIX_LAYOUT)
    if layout not in MATRIX_LAYOUTS:
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
    raw_keys = set(variables)
    for key, spec in variables.items():
        if not isinstance(key, str) or not key:
            raise ConfigurationError(_path(dashboard_name, "variables") + ": variable keys must be non-empty strings.")
        if not isinstance(spec, dict):
            raise ConfigurationError(_path(dashboard_name, f"variables.{key}") + ": expected mapping.")
        _validate_grafana_variable(dashboard_name, key, spec)
        _validate_presentation(dashboard_name, key, spec)
        _validate_alias(dashboard_name, key, spec, aliases, raw_keys)
        _validate_value_source(dashboard_name, key, spec)
        _validate_dependencies(dashboard_name, key, spec, variables)


def _validate_max_rows(dashboard_name: str, matrix: dict[str, Any]) -> None:
    max_rows = matrix.get("max_rows", DEFAULT_MAX_MATRIX_ROWS)
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows <= 0:
        raise ConfigurationError(_path(dashboard_name, "max_rows") + ": expected positive integer.")


def _validate_grafana_variable(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    value = spec.get("grafana_variable")
    if value not in (None, "") and not isinstance(value, str):
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.grafana_variable") + ": expected non-empty string.")
    lookup = spec.get("lookup")
    if lookup is not None and (not isinstance(lookup, str) or not lookup):
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.lookup") + ": expected non-empty string.")
    if lookup and value:
        raise ConfigurationError(
            _path(dashboard_name, f"variables.{key}.lookup")
            + ": lookup and grafana_variable are mutually exclusive."
        )


def _validate_dependencies(
    dashboard_name: str,
    key: str,
    spec: dict[str, Any],
    variables: dict[str, Any],
) -> None:
    dependencies = configured_dependencies(spec)
    if "values_by" in spec and not dependencies:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.depends_on") + ": required for values_by.")
    unknown = [dependency for dependency in dependencies if dependency not in variables]
    if unknown:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.depends_on") + f": unknown dependencies {unknown}.")


def _validate_alias(
    dashboard_name: str,
    key: str,
    spec: dict[str, Any],
    aliases: set[str],
    raw_keys: set[str],
) -> None:
    if "alias" in spec and "display_name" in spec and spec["alias"] != spec["display_name"]:
        raise ConfigurationError(
            _path(dashboard_name, f"variables.{key}.alias")
            + ": alias and display_name cannot have different values."
        )
    alias = spec.get("display_name", spec.get("alias", key))
    if not isinstance(alias, str) or not alias:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.alias") + ": expected non-empty string.")
    if alias in raw_keys and alias != key:
        raise ConfigurationError(
            _path(dashboard_name, f"variables.{key}.alias")
            + f": display name '{alias}' collides with raw matrix variable '{alias}'."
        )
    if alias in aliases:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.alias") + ": duplicate metadata alias.")
    aliases.add(alias)


def _validate_presentation(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    path = _path(dashboard_name, f"variables.{key}")
    if "hide" in spec and not isinstance(spec["hide"], bool):
        raise ConfigurationError(path + ".hide: expected boolean.")
    if "display_name" in spec and (not isinstance(spec["display_name"], str) or not spec["display_name"]):
        raise ConfigurationError(path + ".display_name: expected non-empty string.")
    aliases = spec.get("value_aliases", {})
    if not isinstance(aliases, dict) or not all(
        isinstance(raw, str) and raw and isinstance(display, str) and display
        for raw, display in aliases.items()
    ):
        raise ConfigurationError(path + ".value_aliases: expected mapping of non-empty strings.")


def _validate_value_source(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    _reject_misplaced_dynamic_fields(dashboard_name, key, spec)
    sources = [name for name in ("values", "values_by", "values_from") if name in spec]
    if len(sources) != 1:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}") + ": expected exactly one value source.")
    _validate_source_shape(dashboard_name, key, spec, sources[0])
    source = spec.get("values_from")
    has_dynamic_fields = isinstance(source, dict) and any(
        name in source for name in ("filters_by_parent", "grouping")
    )
    has_regex_list = isinstance(source, dict) and isinstance(source.get("regex"), list)
    if not has_dynamic_fields and not has_regex_list:
        _validate_regex(dashboard_name, key, spec)
    _validate_max_values(dashboard_name, key, spec)
    if has_dynamic_fields or has_regex_list:
        spec["__dynamic_planner__"] = _compiled_dynamic_planner(dashboard_name, key, spec)


def _validate_source_shape(dashboard_name: str, key: str, spec: dict[str, Any], source: str) -> None:
    value = spec.get(source)
    valid = (
        source == "values" and isinstance(value, list) and bool(value)
        or source == "values_by" and _valid_values_by(value)
        or source == "values_from" and _valid_values_from(value, key, spec)
    )
    if not valid:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.{source}") + ": invalid value source.")


def _valid_values_from(value: Any, key: str, spec: dict[str, Any]) -> bool:
    if isinstance(value, str) and value:
        if spec.get("lookup"):
            return False
        return value == str(spec.get("grafana_variable") or key)
    return isinstance(value, dict) and not set(value) - DYNAMIC_SOURCE_KEYS


def _valid_values_by(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(
        isinstance(items, list) and bool(items) for items in value.values()
    )


def _validate_regex(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    regex = regex_value(spec)
    if regex in (None, ""):
        return
    try:
        re.compile(str(regex))
    except re.error as error:
        source = spec.get("values_from")
        suffix = (
            f"variables.{key}.values_from.regex"
            if isinstance(source, dict) and "regex" in source else f"variables.{key}.regex"
        )
        raise ConfigurationError(_path(dashboard_name, suffix) + f": invalid regex ({error}).") from error


def _reject_misplaced_dynamic_fields(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    for field in ("filters_by_parent", "grouping"):
        if field in spec:
            raise ConfigurationError(_path(dashboard_name, f"variables.{key}.{field}") + ": allowed only inside mapping values_from.")
    if not isinstance(spec.get("values_from"), dict):
        return
    _validate_dynamic_source(dashboard_name, key, spec["values_from"])


def _validate_dynamic_source(dashboard_name: str, key: str, source: dict[str, Any]) -> None:
    path = _path(dashboard_name, f"variables.{key}.values_from")
    unknown = sorted(str(name) for name in source if name not in DYNAMIC_SOURCE_KEYS)
    if unknown:
        raise ConfigurationError(path + f": unknown field(s) {unknown}.")
    if "regex" in source and (
        isinstance(source["regex"], list)
        or any(name in source for name in ("filters_by_parent", "grouping"))
    ):
        _validate_regex_set(path + ".regex", source["regex"])
    if "filters_by_parent" in source:
        _validate_parent_filters(path, source["filters_by_parent"])
    if "grouping" in source:
        _validate_grouping(path, source["grouping"])


def _validate_parent_filters(path: str, filters: Any) -> None:
    if not isinstance(filters, list):
        raise ConfigurationError(path + ".filters_by_parent: expected list.")
    for index, item in enumerate(filters):
        item_path = f"{path}.filters_by_parent[{index}]"
        if not isinstance(item, dict):
            raise ConfigurationError(item_path + ": expected mapping.")
        _reject_unknown(item_path, item, {"when", "regex", "mode"})
        _validate_when(item_path + ".when", item.get("when"))
        _validate_regex_set(item_path + ".regex", item.get("regex"))
        if item.get("mode", "and") not in {"and", "override_global"}:
            raise ConfigurationError(item_path + ".mode: expected and or override_global.")


def _validate_grouping(path: str, grouping: Any) -> None:
    group_path = path + ".grouping"
    if not isinstance(grouping, dict):
        raise ConfigurationError(group_path + ": expected mapping.")
    _reject_unknown(group_path, grouping, {"dimension", "rules", "capture", "unmatched"})
    _validate_group_dimension(group_path, grouping.get("dimension"))
    rules = grouping.get("rules", [])
    _validate_group_rules(group_path, rules)
    capture = grouping.get("capture")
    if capture is not None:
        _validate_capture(group_path, capture)
    unmatched = grouping.get("unmatched", {})
    _validate_unmatched(group_path, unmatched)
    if not rules and capture is None and not unmatched.get("enabled", False):
        raise ConfigurationError(group_path + ": expected rules, capture, or enabled unmatched.")


def _validate_group_dimension(path: str, dimension: Any) -> None:
    dimension_path = path + ".dimension"
    if not isinstance(dimension, dict):
        raise ConfigurationError(dimension_path + ": expected mapping.")
    _reject_unknown(dimension_path, dimension, {"key", "display_name", "hide"})
    _validate_identifier(dimension_path + ".key", dimension.get("key"))
    _validate_non_empty_string(dimension_path + ".display_name", dimension.get("display_name"))
    if not isinstance(dimension.get("hide"), bool):
        raise ConfigurationError(dimension_path + ".hide: expected boolean.")


def _validate_group_rules(path: str, rules: Any) -> None:
    rules_path = path + ".rules"
    if not isinstance(rules, list):
        raise ConfigurationError(rules_path + ": expected list.")
    names: set[str] = set()
    for index, rule in enumerate(rules):
        rule_path = f"{rules_path}[{index}]"
        if not isinstance(rule, dict):
            raise ConfigurationError(rule_path + ": expected mapping.")
        _reject_unknown(rule_path, rule, {"name", "label", "regex", "when"})
        name = rule.get("name")
        _validate_identifier(rule_path + ".name", name)
        if name in names:
            raise ConfigurationError(rule_path + ".name: duplicate rule name.")
        names.add(name)
        _validate_non_empty_string(rule_path + ".label", rule.get("label"))
        _validate_regex_set(rule_path + ".regex", rule.get("regex"))
        if "when" in rule:
            _validate_when(rule_path + ".when", rule["when"])


def _validate_capture(path: str, capture: Any) -> None:
    capture_path = path + ".capture"
    if not isinstance(capture, dict):
        raise ConfigurationError(capture_path + ": expected mapping.")
    _reject_unknown(capture_path, capture, {"regex", "group", "when", "value_aliases"})
    _validate_non_empty_regex(capture_path + ".regex", capture.get("regex"))
    group = capture.get("group")
    if not isinstance(group, (str, int)) or isinstance(group, bool) or isinstance(group, int) and group < 0:
        raise ConfigurationError(capture_path + ".group: expected capture group name or non-negative index.")
    if "when" in capture:
        _validate_when(capture_path + ".when", capture["when"])
    aliases = capture.get("value_aliases", {})
    if not isinstance(aliases, dict) or not all(
        isinstance(key, str) and key and isinstance(value, str) and value for key, value in aliases.items()
    ):
        raise ConfigurationError(capture_path + ".value_aliases: expected mapping of non-empty strings.")


def _validate_unmatched(path: str, unmatched: Any) -> None:
    unmatched_path = path + ".unmatched"
    if not isinstance(unmatched, dict):
        raise ConfigurationError(unmatched_path + ": expected mapping.")
    _reject_unknown(unmatched_path, unmatched, {"enabled", "name", "label"})
    if not isinstance(unmatched.get("enabled"), bool):
        raise ConfigurationError(unmatched_path + ".enabled: expected boolean.")
    _validate_identifier(unmatched_path + ".name", unmatched.get("name"))
    _validate_non_empty_string(unmatched_path + ".label", unmatched.get("label"))


def _validate_when(path: str, when: Any) -> None:
    if not isinstance(when, dict) or not when:
        raise ConfigurationError(path + ": expected non-empty mapping.")
    for key, values in when.items():
        if not isinstance(key, str) or not key:
            raise ConfigurationError(path + ": keys must be non-empty strings.")
        if not isinstance(values, list) or not values or any(
            value is None or isinstance(value, (dict, list)) for value in values
        ):
            raise ConfigurationError(path + f".{key}: expected scalar or non-empty scalar list.")


def _validate_non_empty_regex(path: str, value: Any) -> None:
    _validate_non_empty_string(path, value)


def _validate_regex_set(path: str, value: Any) -> None:
    if isinstance(value, str):
        _validate_non_empty_string(path, value)
        return
    if not isinstance(value, list) or not value:
        raise ConfigurationError(path + ": expected non-empty string or non-empty list of strings.")
    for index, pattern in enumerate(value):
        _validate_non_empty_string(f"{path}[{index}]", pattern)


def _validate_non_empty_string(path: str, value: Any) -> None:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(path + ": expected non-empty string.")


def _validate_identifier(path: str, value: Any) -> None:
    if not isinstance(value, str) or IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ConfigurationError(path + ": expected identifier [A-Za-z_][A-Za-z0-9_.-]*.")


def _reject_unknown(path: str, value: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(str(key) for key in value if key not in allowed)
    if unknown:
        raise ConfigurationError(path + f": unknown field(s) {unknown}.")


def _compiled_dynamic_planner(dashboard_name: str, key: str, spec: dict[str, Any]):
    from grafconflux._grafana.matrix_dynamic import DynamicValuePlanner

    source = spec["values_from"]
    effective_source = dict(source)
    effective_source["max_values"] = max_values(spec)
    selected_regex = regex_value(spec)
    compiled_regex = (
        selected_regex
        if "regex" in source or selected_regex in (None, "")
        else str(selected_regex)
    )
    if compiled_regex not in (None, ""):
        effective_source["regex"] = compiled_regex
    compiled: dict[str, Any] = {
        "global": _compile_regex_set_at(
            _path(
                dashboard_name,
                f"variables.{key}.values_from.regex" if "regex" in source else f"variables.{key}.regex",
            ),
            compiled_regex,
            optional=True,
        ),
        "parents": [
            _compile_regex_set_at(
                _path(dashboard_name, f"variables.{key}.values_from.filters_by_parent[{index}].regex"),
                item["regex"],
            )
            for index, item in enumerate(source.get("filters_by_parent") or [])
        ],
    }
    grouping = source.get("grouping") or {}
    compiled["named"] = [
        _compile_regex_set_at(
            _path(dashboard_name, f"variables.{key}.values_from.grouping.rules[{index}].regex"),
            rule["regex"],
        )
        for index, rule in enumerate(grouping.get("rules") or [])
    ]
    capture = grouping.get("capture")
    if isinstance(capture, dict):
        capture_path = _path(dashboard_name, f"variables.{key}.values_from.grouping.capture")
        capture_pattern = _compile_at(capture_path + ".regex", capture["regex"])
        _validate_compiled_capture_group(capture_path + ".group", capture_pattern, capture["group"])
        compiled["capture"] = capture_pattern
    return DynamicValuePlanner.from_source(effective_source, compiled)


def _compile_at(path: str, value: Any, optional: bool = False) -> re.Pattern[str] | None:
    if optional and value in (None, ""):
        return None
    try:
        return re.compile(value)
    except re.error as error:
        raise ConfigurationError(path + f": invalid regex ({type(error).__name__}).") from error


def _compile_regex_set_at(path: str, value: Any, optional: bool = False) -> tuple[re.Pattern[str], ...]:
    if optional and value in (None, ""):
        return ()
    values = value if isinstance(value, list) else [value]
    return tuple(
        _compile_at(f"{path}[{index}]" if isinstance(value, list) else path, pattern)
        for index, pattern in enumerate(values)
    )


def _validate_compiled_capture_group(path: str, pattern: re.Pattern[str], group: str | int) -> None:
    exists = group in pattern.groupindex if isinstance(group, str) else group <= pattern.groups
    if not exists:
        raise ConfigurationError(path + ": capture group does not exist.")


def _validate_dynamic_matrix_constraints(
    dashboard_name: str,
    matrix: dict[str, Any],
    variables: dict[str, Any],
) -> None:
    dynamic = [
        (key, spec, spec["values_from"])
        for key, spec in variables.items()
        if isinstance(spec.get("values_from"), dict)
        and any(name in spec["values_from"] for name in ("filters_by_parent", "grouping"))
    ]
    if dynamic and matrix.get("combination_mode", "product") != "product":
        raise ConfigurationError(
            _path(dashboard_name, "combination_mode")
            + ": dynamic parent filtering/grouping requires product."
        )
    raw_keys = set(variables)
    display_names = {display_name(key, spec) for key, spec in variables.items()}
    synthetic_keys: set[str] = set()
    synthetic_names: set[str] = set()
    for key, _, source in dynamic:
        grouping = source.get("grouping")
        if not isinstance(grouping, dict):
            continue
        group_path = _path(dashboard_name, f"variables.{key}.values_from.grouping")
        if matrix.get("layout", DEFAULT_MATRIX_LAYOUT) not in GROUPING_LAYOUTS:
            allowed = ", ".join(sorted(GROUPING_LAYOUTS))
            raise ConfigurationError(group_path + f": grouping requires layout in [{allowed}].")
        dimension = grouping["dimension"]
        dimension_key = dimension["key"]
        dimension_name = dimension["display_name"]
        if dimension_key in raw_keys | display_names | synthetic_keys | synthetic_names:
            raise ConfigurationError(group_path + ".dimension.key: collides with matrix dimension.")
        if dimension_name in raw_keys | display_names | synthetic_keys | synthetic_names:
            raise ConfigurationError(group_path + ".dimension.display_name: collides with matrix dimension.")
        synthetic_keys.add(dimension_key)
        synthetic_names.add(dimension_name)


def _validate_max_values(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    value = max_values(spec)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        source = spec.get("values_from")
        suffix = (
            f"variables.{key}.values_from.max_values"
            if isinstance(source, dict) and "max_values" in source else f"variables.{key}.max_values"
        )
        raise ConfigurationError(_path(dashboard_name, suffix) + ": expected positive integer.")


def _validate_label_template(dashboard_name: str, matrix: dict[str, Any], variables: dict[str, Any]) -> None:
    template = matrix.get("label_template")
    if template in (None, ""):
        return
    if not isinstance(template, str):
        raise ConfigurationError(_path(dashboard_name, "label_template") + ": expected string.")
    fields = {field for _, field, _, _ in Formatter().parse(template) if field}
    allowed = set(variables) | {display_name(key, spec) for key, spec in variables.items()}
    unknown = sorted(fields - allowed)
    if unknown:
        raise ConfigurationError(_path(dashboard_name, "label_template") + f": unknown placeholders {unknown}.")
    hidden = _known_hidden_template_names(variables)
    hidden_fields = sorted(fields & hidden)
    if hidden_fields:
        raise ConfigurationError(
            _path(dashboard_name, "label_template") + f": placeholders reference hidden variables {hidden_fields}."
        )


def _known_hidden_template_names(variables: dict[str, Any]) -> set[str]:
    hidden: set[str] = set()
    for key, spec in variables.items():
        raw_values = spec.get("values") if "values" in spec else []
        known = spec.get("hide") is True or (
            "hide" not in spec and (bool(spec.get("value_aliases")) or "values" in spec and resolved_hidden(spec, raw_values))
        )
        if known:
            hidden.update({key, display_name(key, spec)})
    return hidden


def regex_value(spec: dict[str, Any]) -> Any:
    source = spec.get("values_from")
    return source.get("regex") if isinstance(source, dict) and "regex" in source else spec.get("regex")


def max_values(spec: dict[str, Any]) -> int:
    source = spec.get("values_from")
    value = source.get("max_values") if isinstance(source, dict) and "max_values" in source else spec.get("max_values")
    return value if value is not None else DEFAULT_MAX_MATRIX_VALUES


def serializable_render_matrix(matrix: dict[str, Any]) -> dict[str, Any]:
    """Return metadata-safe matrix configuration without planning-only state."""
    return _without_private_fields(matrix)


def _without_private_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_private_fields(item)
            for key, item in value.items()
            if not str(key).startswith("__")
        }
    if isinstance(value, list):
        return [_without_private_fields(item) for item in value]
    return value


def _path(dashboard_name: str, suffix: str) -> str:
    return f"dashboards.{dashboard_name}.{RENDER_MATRIX_KEY}.{suffix}"
