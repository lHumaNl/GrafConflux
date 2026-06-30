"""Compatibility facade for Grafana no-data preflight helpers."""

from grafconflux._grafana.no_data import *  # noqa: F401,F403
from grafconflux._grafana.no_data import (  # noqa: F401
    _GenericNoDataDetector,
    _datasource_type_uid,
    _has_ambiguous_datasource_refs,
    _has_no_data_unsupported_panel_features,
    _interpret_fields,
    _interpret_frame,
    _interpret_frames,
    _interpret_ref_result,
    _interpret_values,
    _preflight_targets,
    _scoped_vars,
    _target_datasource_ref,
    _value_has_data,
)
