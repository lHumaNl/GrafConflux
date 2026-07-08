"""Named Grafana credential resolution and per-run session pooling."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Any

import requests

from grafconflux._shared.grafana_models import ConfigurationError

logger = logging.getLogger("grafconflux.grafana")

GRAFANA_CREDENTIALS_KEY = "grafana_credentials"
DEFAULT_GRAFANA_CREDENTIALS_KEY = "default_grafana_credentials"
DEFAULT_GRAFANA_CREDENTIAL_REF = "__default__"
SESSION_MODE_SHARED = "shared"
SESSION_MODE_ISOLATED = "isolated"
SESSION_MODES = {SESSION_MODE_SHARED, SESSION_MODE_ISOLATED}
AUTH_IDENTITY_KEYS = {"grafana_url", "auth_url", "login", "password", "token", "domain"}
SECRET_KEYS = {"login", "password", "token"}


@dataclass
class GrafanaSessionRecord:
    """Mutable shared session state protected by a per-session lock."""

    session: requests.Session
    lock: threading.Lock
    authenticated: bool = False


class GrafanaSessionPool:
    """Per-run-item session registry for dashboards using shared credentials."""

    def __init__(self) -> None:
        self._records: dict[str, GrafanaSessionRecord] = {}
        self._lock = threading.Lock()

    def record_for(self, config: Any) -> GrafanaSessionRecord | None:
        session_key = getattr(config, "session_key", None)
        if getattr(config, "session_mode", SESSION_MODE_ISOLATED) != SESSION_MODE_SHARED:
            return None
        if not session_key:
            return None
        with self._lock:
            if session_key not in self._records:
                self._records[session_key] = GrafanaSessionRecord(requests.Session(), threading.Lock())
            return self._records[session_key]


def resolve_dashboard_configs(config: Any) -> dict[str, dict[str, Any]]:
    """Return effective dashboard configs with named credentials merged in order."""

    dashboards = _validate_top_level_config(config)
    credentials = _validated_credentials(config.get(GRAFANA_CREDENTIALS_KEY, {}))
    default_credentials = _validated_default_credentials(config.get(DEFAULT_GRAFANA_CREDENTIALS_KEY))
    return {
        name: _resolved_dashboard_config(name, dashboard_config, credentials, default_credentials)
        for name, dashboard_config in dashboards.items()
    }


def _validate_top_level_config(config: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(config, dict):
        raise ConfigurationError("YAML config must be a mapping with top-level 'dashboards' and optional 'settings'.")
    allowed = {"settings", "dashboards", GRAFANA_CREDENTIALS_KEY, DEFAULT_GRAFANA_CREDENTIALS_KEY}
    if "dashboards" not in config and any(key not in allowed and isinstance(value, dict) for key, value in config.items()):
        raise ConfigurationError(
            "Legacy top-level dashboard YAML format is not supported; "
            "move dashboard entries under top-level 'dashboards'."
        )
    if "settings" in config and not isinstance(config["settings"], dict):
        raise ConfigurationError("YAML top-level 'settings' must be a mapping.")
    dashboards = config.get("dashboards")
    if not isinstance(dashboards, dict) or not dashboards:
        raise ConfigurationError("YAML config must contain a non-empty top-level 'dashboards' mapping.")
    return dashboards


def _validated_credentials(raw_credentials: Any) -> dict[str, dict[str, Any]]:
    if raw_credentials in (None, {}):
        return {}
    if not isinstance(raw_credentials, dict):
        raise ConfigurationError("YAML top-level 'grafana_credentials' must be a mapping.")
    return {
        name: _resolved_env_values(f"{GRAFANA_CREDENTIALS_KEY}.{name}", _credential_mapping(name, value))
        for name, value in raw_credentials.items()
    }


def _validated_default_credentials(raw_credentials: Any) -> dict[str, Any]:
    if raw_credentials is None:
        return {}
    if not isinstance(raw_credentials, dict):
        raise ConfigurationError("YAML top-level 'default_grafana_credentials' must be a mapping.")
    return _resolved_env_values(DEFAULT_GRAFANA_CREDENTIALS_KEY, dict(raw_credentials))


def _credential_mapping(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(name, str) or not name:
        raise ConfigurationError("grafana_credentials: credential names must be non-empty strings.")
    if not isinstance(value, dict):
        raise ConfigurationError(f"grafana_credentials.{name}: invalid value, expected mapping.")
    return dict(value)


def _resolved_dashboard_config(
    dashboard_name: str,
    dashboard_config: Any,
    credentials: dict[str, dict[str, Any]],
    default_credentials: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(dashboard_config, dict):
        raise ConfigurationError(f"dashboards.{dashboard_name}: invalid value, expected mapping.")
    ref = _credential_ref(dashboard_name, dashboard_config)
    if ref is None:
        if _uses_default_credentials(dashboard_config, default_credentials):
            return _merge_credential_config(
                dashboard_name=dashboard_name,
                credential_ref=DEFAULT_GRAFANA_CREDENTIAL_REF,
                dashboard_config=dashboard_config,
                credential_config=default_credentials,
                config_source="default_credentials",
            )
        resolved = _resolved_env_values(f"dashboards.{dashboard_name}", dict(dashboard_config))
        _set_inline_session_metadata(resolved)
        return resolved
    if ref not in credentials:
        raise ConfigurationError(f"dashboards.{dashboard_name}.credentials: unknown Grafana credentials reference '{ref}'.")
    return _merge_named_credentials(dashboard_name, ref, dashboard_config, credentials[ref])


def _uses_default_credentials(dashboard_config: dict[str, Any], default_credentials: dict[str, Any]) -> bool:
    if not default_credentials:
        return False
    return not AUTH_IDENTITY_KEYS.intersection(dashboard_config)


def _credential_ref(dashboard_name: str, dashboard_config: dict[str, Any]) -> str | None:
    has_short = "credentials" in dashboard_config
    has_long = GRAFANA_CREDENTIALS_KEY in dashboard_config
    if has_short and has_long:
        raise ConfigurationError(f"dashboards.{dashboard_name}: use either credentials or grafana_credentials, not both.")
    ref = dashboard_config.get("credentials") if has_short else dashboard_config.get(GRAFANA_CREDENTIALS_KEY)
    if ref in (None, ""):
        return None
    if not isinstance(ref, str):
        raise ConfigurationError(f"dashboards.{dashboard_name}.credentials: invalid value, expected string.")
    return ref


def _merge_named_credentials(
    dashboard_name: str,
    credential_ref: str,
    dashboard_config: dict[str, Any],
    credential_config: dict[str, Any],
) -> dict[str, Any]:
    return _merge_credential_config(
        dashboard_name=dashboard_name,
        credential_ref=credential_ref,
        dashboard_config=dashboard_config,
        credential_config=credential_config,
        config_source="named_credentials",
    )


def _merge_credential_config(
    dashboard_name: str,
    credential_ref: str,
    dashboard_config: dict[str, Any],
    credential_config: dict[str, Any],
    config_source: str,
) -> dict[str, Any]:
    mode = _effective_session_mode(dashboard_name, credential_ref, credential_config, dashboard_config)
    _validate_identity_overrides(dashboard_name, dashboard_config, mode)
    dashboard_values = _resolved_env_values(f"dashboards.{dashboard_name}", dict(dashboard_config))
    resolved = dict(credential_config)
    resolved.update(dashboard_values)
    resolved.pop("credentials", None)
    resolved.pop(GRAFANA_CREDENTIALS_KEY, None)
    resolved["credential_ref"] = credential_ref
    resolved["config_source"] = config_source
    resolved["session_mode"] = mode
    resolved["session_key"] = _session_key(credential_ref, resolved, mode)
    return resolved


def _set_inline_session_metadata(resolved: dict[str, Any]) -> None:
    resolved.setdefault("config_source", "inline")
    resolved.setdefault("session_mode", SESSION_MODE_ISOLATED)
    resolved.setdefault("session_key", None)


def _effective_session_mode(
    dashboard_name: str,
    credential_ref: str,
    credential_config: dict[str, Any],
    dashboard_config: dict[str, Any],
) -> str:
    source = dashboard_config if _has_session_mode(dashboard_config) else credential_config
    default = SESSION_MODE_SHARED
    mode = _session_mode_from_mapping(f"dashboards.{dashboard_name}", source, default)
    if _conflicting_reuse(source):
        logger.warning("session_mode overrides conflicting reuse_session for Grafana credentials %s", credential_ref)
    return mode


def _has_session_mode(config: dict[str, Any]) -> bool:
    return "session_mode" in config or "reuse_session" in config


def _session_mode_from_mapping(path: str, config: dict[str, Any], default: str) -> str:
    if "session_mode" in config:
        value = config["session_mode"]
        if value not in SESSION_MODES:
            raise ConfigurationError(f"{path}.session_mode: invalid value='{value}', expected shared or isolated.")
        return value
    if "reuse_session" in config:
        value = config["reuse_session"]
        if not isinstance(value, bool):
            raise ConfigurationError(f"{path}.reuse_session: invalid value='{value}', expected bool.")
        return SESSION_MODE_SHARED if value else SESSION_MODE_ISOLATED
    return default


def _conflicting_reuse(config: dict[str, Any]) -> bool:
    if "session_mode" not in config or "reuse_session" not in config:
        return False
    reuse_mode = SESSION_MODE_SHARED if config.get("reuse_session") is True else SESSION_MODE_ISOLATED
    return config.get("session_mode") != reuse_mode


def _validate_identity_overrides(dashboard_name: str, dashboard_config: dict[str, Any], mode: str) -> None:
    if mode == SESSION_MODE_ISOLATED:
        return
    overrides = sorted(AUTH_IDENTITY_KEYS.intersection(dashboard_config))
    if overrides:
        keys = ", ".join(overrides)
        raise ConfigurationError(
            f"dashboards.{dashboard_name}: shared named credentials cannot override Grafana identity fields: {keys}; "
            "set session_mode: isolated or move overrides to grafana_credentials."
        )


def _resolved_env_values(path: str, config: dict[str, Any]) -> dict[str, Any]:
    resolved = dict(config)
    for key in SECRET_KEYS:
        if key in resolved:
            resolved[key] = _env_value(path, key, resolved[key])
    return resolved


def _env_value(path: str, key: str, value: Any) -> Any:
    if not isinstance(value, str) or not value.startswith("env:"):
        return value
    env_name = value[4:]
    if not env_name:
        raise ConfigurationError(f"{path}.{key}: invalid env reference, expected env:VARIABLE_NAME.")
    env_value = os.getenv(env_name)
    if env_value is None:
        raise ConfigurationError(f"{path}.{key}: environment variable '{env_name}' is not set.")
    return env_value


def _session_key(credential_ref: str, resolved: dict[str, Any], mode: str) -> str | None:
    if mode != SESSION_MODE_SHARED:
        return None
    identity = [credential_ref, resolved.get("grafana_url"), resolved.get("auth_url"), str(resolved.get("domain", False))]
    return "|".join(str(value) for value in identity)
