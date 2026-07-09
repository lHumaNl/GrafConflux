"""Confluence rendering settings shared by config, metadata, and renderers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from grafconflux._shared.time import HUMAN_TIME_FORMAT

DESCRIPTION_DASHBOARD_LINKS = "dashboard_links"
DESCRIPTION_BACKUP_DASHBOARD_LINKS = "backup_dashboard_links"
DESCRIPTION_PANELS = "panels"
DESCRIPTION_TEST_TIMES = "test_times"
DESCRIPTION_IDS = frozenset({
    DESCRIPTION_DASHBOARD_LINKS,
    DESCRIPTION_BACKUP_DASHBOARD_LINKS,
    DESCRIPTION_PANELS,
    DESCRIPTION_TEST_TIMES,
})
DEFAULT_DESCRIPTION_LABELS = {
    DESCRIPTION_DASHBOARD_LINKS: "Dashboard links",
    DESCRIPTION_BACKUP_DASHBOARD_LINKS: "Backup dashboard links",
    DESCRIPTION_PANELS: "Panels",
    DESCRIPTION_TEST_TIMES: "Test times",
}
DASHBOARD_LINKS_LEAF = "leaf"
DASHBOARD_LINKS_DASHBOARD = "dashboard"
DASHBOARD_LINKS_NONE = "none"
DASHBOARD_LINKS_LOCATIONS = frozenset({
    DASHBOARD_LINKS_LEAF,
    DASHBOARD_LINKS_DASHBOARD,
    DASHBOARD_LINKS_NONE,
})
FIXED_OFFSET_PATTERN = re.compile(r"^([+-])(\d{2}):(\d{2})$")
MILLISECONDS_PER_SECOND = 1000


@dataclass(frozen=True)
class EffectiveTimeZone:
    """Timezone used for Confluence time display."""

    zone: tzinfo | None
    label: str


@dataclass(frozen=True)
class ConfluenceRenderingSettings:
    """Validated user-facing Confluence rendering preferences."""

    description_rename: dict[str, str] = field(default_factory=dict)
    description_switch: dict[str, bool] = field(default_factory=dict)
    time_zone: str | None = None
    dashboard_links_location: str = DASHBOARD_LINKS_LEAF

    def label(self, description_id: str) -> str:
        return self.description_rename.get(description_id, DEFAULT_DESCRIPTION_LABELS[description_id])

    def enabled(self, description_id: str) -> bool:
        if description_id == DESCRIPTION_TEST_TIMES:
            return True
        return self.description_switch.get(description_id, True)

    def dashboard_links_at_dashboard(self, has_matrix: bool) -> bool:
        if not self.enabled(DESCRIPTION_DASHBOARD_LINKS):
            return False
        if self.dashboard_links_location == DASHBOARD_LINKS_NONE:
            return False
        return not has_matrix or self.dashboard_links_location == DASHBOARD_LINKS_DASHBOARD

    def dashboard_links_at_leaf(self) -> bool:
        return self.enabled(DESCRIPTION_DASHBOARD_LINKS) and self.dashboard_links_location == DASHBOARD_LINKS_LEAF

    def to_metadata(self) -> dict[str, Any]:
        return {
            "description_rename": dict(self.description_rename),
            "description_switch": dict(self.description_switch),
            "time_zone": self.time_zone,
            "dashboard_links_location": self.dashboard_links_location,
        }


def confluence_rendering_settings_from_config(config_data: dict[str, Any]) -> ConfluenceRenderingSettings:
    settings_data = config_data.get("settings", {}) if isinstance(config_data, dict) else {}
    return confluence_rendering_settings_from_mapping(settings_data)


def confluence_rendering_settings_from_metadata(metadata: dict[str, Any]) -> ConfluenceRenderingSettings:
    settings_data = metadata.get("confluence_rendering") or metadata.get("settings") or {}
    return confluence_rendering_settings_from_mapping(settings_data)


def confluence_rendering_settings_from_mapping(settings_data: Any) -> ConfluenceRenderingSettings:
    if settings_data in (None, ""):
        settings_data = {}
    if not isinstance(settings_data, dict):
        raise ValueError("YAML top-level 'settings' must be a mapping.")
    return ConfluenceRenderingSettings(
        description_rename=_validated_renames(settings_data),
        description_switch=_validated_switches(settings_data),
        time_zone=_validated_time_zone(settings_data),
        dashboard_links_location=_validated_dashboard_links_location(settings_data),
    )


def effective_time_zone(settings: ConfluenceRenderingSettings) -> EffectiveTimeZone:
    if settings.time_zone in (None, ""):
        return EffectiveTimeZone(None, "host timezone")
    return EffectiveTimeZone(_timezone_from_string(settings.time_zone), settings.time_zone)


def format_timestamp_time(timestamp: Any, field_prefix: str, settings: ConfluenceRenderingSettings) -> str:
    epoch_value = getattr(timestamp, f"{field_prefix}_time_timestamp", None)
    if epoch_value in (None, ""):
        return str(getattr(timestamp, f"{field_prefix}_time_human", ""))
    try:
        return _format_epoch_in_effective_zone(epoch_value, effective_time_zone(settings)).strftime(HUMAN_TIME_FORMAT)
    except (TypeError, ValueError):
        return str(getattr(timestamp, f"{field_prefix}_time_human", ""))


def _validated_renames(settings_data: dict[str, Any]) -> dict[str, str]:
    renames = _validated_description_mapping(settings_data, "description_rename")
    for key, value in renames.items():
        if not isinstance(value, str):
            raise ValueError(f"YAML settings.description_rename.{key}: expected string display label.")
    return dict(renames)


def _validated_switches(settings_data: dict[str, Any]) -> dict[str, bool]:
    switches = _validated_description_mapping(settings_data, "description_switch")
    for key, value in switches.items():
        if not isinstance(value, bool):
            raise ValueError(f"YAML settings.description_switch.{key}: expected bool.")
    if switches.get(DESCRIPTION_TEST_TIMES) is False:
        raise ValueError("YAML settings.description_switch.test_times cannot be disabled.")
    return dict(switches)


def _validated_description_mapping(settings_data: dict[str, Any], key: str) -> dict[str, Any]:
    value = settings_data.get(key, {})
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML settings.{key}: expected mapping keyed by stable description ids.")
    _reject_unknown_description_ids(key, value)
    return value


def _reject_unknown_description_ids(setting_name: str, value: dict[str, Any]) -> None:
    unknown = sorted(str(key) for key in value if key not in DESCRIPTION_IDS)
    if unknown:
        raise ValueError(f"YAML settings.{setting_name}: unknown description id(s): {', '.join(unknown)}.")


def _validated_time_zone(settings_data: dict[str, Any]) -> str | None:
    value = settings_data.get("time_zone")
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ValueError("YAML settings.time_zone: expected IANA name or fixed UTC offset string.")
    _timezone_from_string(value)
    return value


def _validated_dashboard_links_location(settings_data: dict[str, Any]) -> str:
    value = settings_data.get("dashboard_links_location", DASHBOARD_LINKS_LEAF)
    if value in DASHBOARD_LINKS_LOCATIONS:
        return value
    expected = ", ".join(sorted(DASHBOARD_LINKS_LOCATIONS))
    raise ValueError(f"YAML settings.dashboard_links_location: invalid value '{value}', expected one of: {expected}.")


def _timezone_from_string(value: str) -> tzinfo:
    fixed_offset = _fixed_offset_timezone(value)
    if fixed_offset is not None:
        return fixed_offset
    try:
        return ZoneInfo(value)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Invalid or unavailable timezone: {value}") from error


def _fixed_offset_timezone(value: str) -> timezone | None:
    match = FIXED_OFFSET_PATTERN.match(value)
    if match is None:
        return None
    sign, hours, minutes = match.groups()
    minute_value = int(minutes)
    if minute_value >= 60:
        raise ValueError(f"Invalid or unavailable timezone: {value}")
    delta = timedelta(hours=int(hours), minutes=minute_value)
    if delta >= timedelta(hours=24):
        raise ValueError(f"Invalid or unavailable timezone: {value}")
    return timezone(delta if sign == "+" else -delta, name=f"UTC{value}")


def _datetime_from_epoch(epoch_value: Any) -> datetime:
    timestamp = int(epoch_value)
    seconds = timestamp / MILLISECONDS_PER_SECOND if len(str(abs(timestamp))) > 10 else timestamp
    return datetime.fromtimestamp(seconds, timezone.utc)


def _format_epoch_in_effective_zone(epoch_value: Any, effective_zone: EffectiveTimeZone) -> datetime:
    timestamp = _datetime_from_epoch(epoch_value)
    if effective_zone.zone is None:
        return timestamp.astimezone()
    return timestamp.astimezone(effective_zone.zone)


def _offset_label(zone: tzinfo) -> str:
    offset = datetime.now(zone).utcoffset() or timedelta()
    sign = "+" if offset >= timedelta() else "-"
    total_minutes = abs(int(offset.total_seconds() // 60))
    return f"UTC{sign}{total_minutes // 60:02d}:{total_minutes % 60:02d}"
