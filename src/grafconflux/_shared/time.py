import re
from abc import ABC
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


HUMAN_TIME_FORMAT = "%Y/%m/%d %H:%M:%S"
MILLISECONDS_PER_SECOND = 1000
TIMESTAMP_SECONDS_LENGTH = 10
ISO_TIME_RANGE_PATTERN = re.compile(r'&from=([\d\-T:.Z]+).*?&to=([\d\-T:.Z]+)')
START_EPOCH_PATTERN = re.compile(r'&from=(\d+)')
END_EPOCH_PATTERN = re.compile(r'&to=(\d+)')
FIXED_OFFSET_PATTERN = re.compile(r'^([+-])(\d{2}):(\d{2})$')


class GrafanaTimeBase(ABC):
    def __init__(self):
        self.time_tag: Optional[str] = None
        self.id_time: int = 0
        self.start_time_timestamp: int = 0
        self.end_time_timestamp: int = 0
        self.start_time_human: str = ''
        self.end_time_human: str = ''


class GrafanaTimeUploader(GrafanaTimeBase):
    def __init__(self, timestamp: Dict):
        super().__init__()
        self.time_tag = timestamp['time_tag']
        self.id_time = timestamp['id_time']
        self.start_time_timestamp = timestamp['start_time_timestamp']
        self.end_time_timestamp = timestamp['end_time_timestamp']
        self.start_time_human = timestamp['start_time_human']
        self.end_time_human = timestamp['end_time_human']


class GrafanaTimeDownloader(GrafanaTimeBase):
    def __init__(self, timestamp_str: str, id_time: int, tz: str):
        super().__init__()
        self.time_tag = self._extract_time_tag(timestamp_str)
        self.id_time: int = id_time
        tz_zone = _load_timezone(tz)
        iso_match = ISO_TIME_RANGE_PATTERN.findall(timestamp_str)
        if iso_match and 'T' in iso_match[0][0]:
            self._set_iso_range(iso_match[0], tz_zone)
            return

        self._set_epoch_range(timestamp_str, tz_zone)

    @staticmethod
    def _extract_time_tag(timestamp_str: str) -> Optional[str]:
        time_tag = timestamp_str.split('__')[0]
        return None if time_tag == timestamp_str else time_tag

    def _set_iso_range(self, iso_range: tuple[str, str], tz_zone: tzinfo) -> None:
        start_time_dt = _parse_iso_datetime(iso_range[0])
        end_time_dt = _parse_iso_datetime(iso_range[1])
        self.start_time_timestamp = int(start_time_dt.timestamp()) * MILLISECONDS_PER_SECOND
        self.end_time_timestamp = int(end_time_dt.timestamp()) * MILLISECONDS_PER_SECOND
        self.start_time_human = _human_time(start_time_dt, tz_zone)
        self.end_time_human = _human_time(end_time_dt, tz_zone)

    def _set_epoch_range(self, timestamp_str: str, tz_zone: tzinfo) -> None:
        start_timestamp = int(START_EPOCH_PATTERN.findall(timestamp_str)[0])
        end_timestamp = int(END_EPOCH_PATTERN.findall(timestamp_str)[0])
        self.start_time_timestamp, start_seconds = _normalize_epoch_timestamp(start_timestamp)
        self.end_time_timestamp, end_seconds = _normalize_epoch_timestamp(end_timestamp)
        self.start_time_human = _human_time(_utc_from_timestamp(start_seconds), tz_zone)
        self.end_time_human = _human_time(_utc_from_timestamp(end_seconds), tz_zone)


def _load_timezone(tz: str) -> tzinfo:
    if tz == "UTC":
        return timezone.utc
    fixed_offset = _fixed_offset_timezone(tz)
    if fixed_offset is not None:
        return fixed_offset
    try:
        return ZoneInfo(tz)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"Invalid or unavailable timezone: {tz}") from error


def _fixed_offset_timezone(tz: str) -> timezone | None:
    match = FIXED_OFFSET_PATTERN.match(tz)
    if match is None:
        return None
    sign, hours, minutes = match.groups()
    hour_value = int(hours)
    minute_value = int(minutes)
    if minute_value >= 60:
        raise ValueError(f"Invalid or unavailable timezone: {tz}")
    offset = timedelta(hours=hour_value, minutes=minute_value)
    if offset >= timedelta(hours=24):
        raise ValueError(f"Invalid or unavailable timezone: {tz}")
    return timezone(offset if sign == '+' else -offset, name=f'UTC{tz}')


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def _normalize_epoch_timestamp(timestamp: int) -> tuple[int, float]:
    if len(str(timestamp)) > TIMESTAMP_SECONDS_LENGTH:
        return timestamp, timestamp / MILLISECONDS_PER_SECOND
    return timestamp * MILLISECONDS_PER_SECOND, float(timestamp)


def _utc_from_timestamp(timestamp_seconds: float) -> datetime:
    return datetime.fromtimestamp(timestamp_seconds, timezone.utc)


def _human_time(value: datetime, tz_zone: tzinfo) -> str:
    return value.astimezone(tz_zone).strftime(HUMAN_TIME_FORMAT)
