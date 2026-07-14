"""Validation and normalization for dashboard render-matrix configuration."""

from __future__ import annotations

import re
from string import Formatter
from typing import Any

from grafconflux._grafana.matrix_dependencies import configured_dependencies
from grafconflux._shared.grafana_models import ConfigurationError
from grafconflux._shared.presentation import display_name, resolved_hidden

RENDER_MATRIX_KEY = "render_matrix"
DEFAULT_MAX_MATRIX_VALUES = 50
DEFAULT_MAX_MATRIX_ROWS = 500
MATRIX_MODES = {"product", "zip"}
DEFAULT_MATRIX_LAYOUT = "panel_first"
MATRIX_LAYOUTS = {"dashboard_first", "matrix_values_first", "panel_first"}
MATRIX_NESTED_OPTION_KEYS = {
    "enabled", "row_grouping", "group_by", "combination_mode", "label_template", "max_rows", "layout",
}
MATRIX_FLAT_OPTION_KEYS = MATRIX_NESTED_OPTION_KEYS - {"layout"}


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
    matrix = dict(value)
    _reject_flat_layout(dashboard_name, matrix)
    nested_options = matrix.pop("options", None)
    variables = matrix.pop("variables", None)
    normalized = _flat_options(matrix)
    normalized.update(_nested_options(dashboard_name, nested_options))
    normalized_variables = variables if variables is not None else _legacy_variables(matrix)
    if normalized_variables:
        normalized["variables"] = _variables_with_automatic_sources(normalized_variables)
    return normalized


def _variables_with_automatic_sources(variables: Any) -> Any:
    if not isinstance(variables, dict):
        return variables
    return {key: _variable_with_automatic_source(spec) for key, spec in variables.items()}


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
    sources = [name for name in ("values", "values_by", "values_from") if name in spec]
    if len(sources) != 1:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}") + ": expected exactly one value source.")
    _validate_source_shape(dashboard_name, key, spec, sources[0])
    _validate_regex(dashboard_name, key, spec)
    _validate_max_values(dashboard_name, key, spec)


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
    return isinstance(value, dict) and not set(value) - {"regex", "max_values"}


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
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.regex") + f": invalid regex ({error}).") from error


def _validate_max_values(dashboard_name: str, key: str, spec: dict[str, Any]) -> None:
    value = max_values(spec)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(_path(dashboard_name, f"variables.{key}.max_values") + ": expected positive integer.")


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


def _path(dashboard_name: str, suffix: str) -> str:
    return f"dashboards.{dashboard_name}.{RENDER_MATRIX_KEY}.{suffix}"
