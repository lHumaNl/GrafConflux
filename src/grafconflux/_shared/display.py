from typing import Any

from grafconflux._shared.grafana_models import ALL_REPEAT_SENTINELS


DISPLAY_ALL_VALUE = 'All'


def normalize_grafana_display_value(value: Any) -> str:
    """Normalize Grafana sentinel values for user-facing titles only."""
    text = str(value)
    if text.lower() in ALL_REPEAT_SENTINELS:
        return DISPLAY_ALL_VALUE
    return text
