import threading
import time
from email.utils import parsedate_to_datetime
from typing import Optional


RETRY_AFTER_HEADER = 'Retry-After'
RETRYABLE_STATUS_CODES = {408, 409, 425, 429}

__all__ = (
    'RETRY_AFTER_HEADER',
    'RETRYABLE_STATUS_CODES',
    '_ConfluenceUploadRateLimiter',
    '_coerce_status_code',
    '_direct_header_value',
    '_effective_upload_interval',
    '_extract_status_code',
    '_header_value',
    '_is_retryable_upload_error',
    '_iterated_header_value',
    '_parse_retry_after',
    '_parse_retry_after_date',
    '_retry_after_header_value',
    '_retry_after_seconds',
    '_status_code_from_source',
)


def _effective_upload_interval(delay_seconds: float, rate_per_second: Optional[float]) -> float:
    if rate_per_second is None:
        return delay_seconds
    return max(delay_seconds, 1 / rate_per_second)


def _extract_status_code(error: Exception) -> Optional[int]:
    for source in (error, getattr(error, 'response', None)):
        status_code = _status_code_from_source(source)
        if status_code is not None:
            return status_code
    return None


def _status_code_from_source(source) -> Optional[int]:
    if source is None:
        return None
    for attr_name in ('status_code', 'status'):
        status_code = _coerce_status_code(getattr(source, attr_name, None))
        if status_code is not None:
            return status_code
    return None


def _coerce_status_code(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        status_code = int(value)
    except (TypeError, ValueError):
        return None
    return status_code


def _is_retryable_upload_error(error: Exception) -> bool:
    status_code = _extract_status_code(error)
    if status_code is None:
        return True
    if status_code in RETRYABLE_STATUS_CODES or status_code >= 500:
        return True
    return not 400 <= status_code < 500


def _retry_after_seconds(error: Exception) -> Optional[float]:
    value = _retry_after_header_value(error)
    if value is None:
        return None
    return _parse_retry_after(str(value))


def _retry_after_header_value(error: Exception) -> Optional[str]:
    for source in (error, getattr(error, 'response', None)):
        value = _header_value(getattr(source, 'headers', None), RETRY_AFTER_HEADER)
        if value is not None:
            return value
    return None


def _header_value(headers, header_name: str) -> Optional[str]:
    if headers is None:
        return None
    value = _direct_header_value(headers, header_name)
    if value is not None:
        return value
    return _iterated_header_value(headers, header_name)


def _direct_header_value(headers, header_name: str) -> Optional[str]:
    get = getattr(headers, 'get', None)
    if get is None:
        return None
    return get(header_name) or get(header_name.lower())


def _iterated_header_value(headers, header_name: str) -> Optional[str]:
    items = getattr(headers, 'items', lambda: [])()
    for key, value in items:
        if str(key).lower() == header_name.lower():
            return value
    return None


def _parse_retry_after(value: str) -> Optional[float]:
    stripped_value = value.strip()
    if stripped_value.isdigit():
        return float(int(stripped_value))
    return _parse_retry_after_date(stripped_value)


def _parse_retry_after_date(value: str) -> Optional[float]:
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, retry_at.timestamp() - time.time())


class _ConfluenceUploadRateLimiter:
    """Process-wide limiter for Confluence upload attempt start times."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_start_time: Optional[float] = None

    def reset(self) -> None:
        with self._lock:
            self._last_start_time = None

    def acquire(self, delay_seconds: float) -> None:
        with self._lock:
            self._sleep_until_slot(delay_seconds)
            self._last_start_time = time.monotonic()

    def _sleep_until_slot(self, delay_seconds: float) -> None:
        if self._last_start_time is None or delay_seconds <= 0:
            return
        wait_seconds = delay_seconds - (time.monotonic() - self._last_start_time)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
