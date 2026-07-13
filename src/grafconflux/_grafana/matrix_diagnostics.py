"""Credential-safe diagnostic summaries for matrix variable discovery."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Mapping
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

PROMETHEUS_DATASOURCE_TYPE = "prometheus"
SUPPORTED_VARIABLE_TYPES = {
    "query", "datasource", "custom", "constant", "interval", "textbox", "adhoc", "groupby",
}
CORRELATION_FINGERPRINT_LENGTH = 12
_CORRELATION_FINGERPRINT_KEY = secrets.token_bytes(32)
DIAGNOSTIC_PREVIEW_LIMIT = 300
DIAGNOSTIC_FIELD_LIMIT = 2_000
_SENSITIVE_NAME_FRAGMENT = re.compile(
    r"(?:authorization|cookie|pass(?:word|wd)?|secret|token|api[_-]?key)", re.I,
)
_SENSITIVE_ASSIGNMENT = re.compile(
    r"[\"']?([A-Za-z0-9_.-]*(?:authorization|cookie|pass(?:word|wd)?|secret|token|"
    r"api[_-]?key)[A-Za-z0-9_.-]*)[\"']?\s*[=:]\s*(?:\"[^\"]*\"|'[^']*'|[^\s,;&}]+)",
    re.I,
)
_AUTH_HEADER = re.compile(r"\b(?:authorization|cookie|set-cookie)\s*:\s*[^\r\n]*", re.I)
_AUTH_SCHEME = re.compile(r"\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", re.I)
_JWT = re.compile(r"\b[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
_URL = re.compile(r"https?://[^\s<>]+", re.I)
_URL_USERINFO = re.compile(r"https?://[^/\s@]+@", re.I)


def sanitize_diagnostic_url(url: Any) -> str:
    """Expose a URL route/query while removing userinfo and secret query values."""
    text = str(url or "")
    try:
        parsed = urlsplit(text)
        netloc = parsed.netloc.rsplit("@", 1)[-1]
        query = urlencode([
            (name, _safe_query_value(name, value))
            for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        ], doseq=True)
        return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))
    except (TypeError, ValueError):
        return _URL_USERINFO.sub(lambda match: match.group(0).split("://", 1)[0] + "://", text)


def sanitize_diagnostic_text(value: Any, limit: int = DIAGNOSTIC_FIELD_LIMIT) -> str:
    """Return one bounded line with obvious credential material removed."""
    text = str(value if value is not None else "none")
    text = _URL.sub(lambda match: sanitize_diagnostic_url(match.group(0)), text)
    text = _AUTH_HEADER.sub("<redacted:auth-header>", text)
    text = _AUTH_SCHEME.sub("<redacted:auth-token>", text)
    text = _SENSITIVE_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = _JWT.sub("<redacted:token>", text)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:max(0, limit - 3)] + "..."


def bounded_response_preview(response: Any) -> str:
    """Return a short response preview only when it has no obvious secret markers."""
    value = getattr(response, "text", None)
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if not isinstance(value, str):
        return "unavailable"
    candidate = value[:DIAGNOSTIC_PREVIEW_LIMIT + 1]
    if _contains_obvious_secret(candidate):
        return "<redacted:sensitive-content>"
    return sanitize_diagnostic_text(candidate, DIAGNOSTIC_PREVIEW_LIMIT)


def response_content_type(response: Any) -> str:
    """Return only the bounded Content-Type value, never the header collection."""
    headers = getattr(response, "headers", {}) or {}
    if not isinstance(headers, Mapping):
        return "unavailable"
    value = headers.get("Content-Type", headers.get("content-type", "unavailable"))
    return sanitize_diagnostic_text(value, 160)


def diagnostic_block(title: str, fields: Any) -> str:
    """Format a diagnostic event as a clearly delimited multiline block."""
    lines = [f"--- BEGIN {title} ---"]
    lines.extend(f"  {name}={sanitize_diagnostic_text(value)}" for name, value in fields)
    lines.append(f"--- END {title} ---")
    return "\n".join(lines)


def render_diagnostic_url(url: str, params: dict[str, Any] | None = None) -> str:
    """Render request parameters into a sanitized URL for diagnostics only."""
    if not params:
        return sanitize_diagnostic_url(url)
    separator = "&" if urlsplit(url).query else "?"
    return sanitize_diagnostic_url(f"{url}{separator}{urlencode(params, doseq=True)}")


def diagnostic_path_query(url: Any) -> str:
    """Return the sanitized path and query portion of a URL."""
    parsed = urlsplit(sanitize_diagnostic_url(url))
    return f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path


def _safe_query_value(name: str, value: str) -> str:
    if _SENSITIVE_NAME_FRAGMENT.search(name):
        return "<redacted>"
    return sanitize_diagnostic_text(value)


def primary_adapter_attempt_block(
    *, url: str, params: dict[str, Any], method: str, route_family: str,
    datasource_uid: str, datasource_uid_source: str, target_label: str,
    variable: str, duration_ms: int, response: Any, error: Exception | None,
    classification: str, prometheus_status: str, data_count: str | int, outcome: str,
) -> str:
    """Build the authorized, credential-safe primary adapter diagnostic block."""
    rendered_url = render_diagnostic_url(url, params)
    fields = (
        ("variable", variable), ("adapter", "prometheus"), ("route_family", route_family),
        ("method", method), ("request_url", rendered_url),
        ("request_path_query", diagnostic_path_query(rendered_url)),
        ("request_params", urlsplit(rendered_url).query),
        ("http_status", getattr(response, "status_code", None)),
        ("response_content_type", response_content_type(response)),
        ("response_preview", bounded_response_preview(response)),
        ("exception_class", type(error).__name__ if error else "none"),
        ("exception_message", str(error) if error else "none"),
        ("datasource_uid_source", datasource_uid_source), ("datasource_uid", datasource_uid),
        ("normalized_selector", params.get("match[]", "none")), ("target_label", target_label),
        ("start", params.get("start", "none")), ("end", params.get("end", "none")),
        ("duration_ms", duration_ms), ("response_classification", classification),
        ("prometheus_status", prometheus_status), ("data_count", data_count), ("outcome", outcome),
    )
    return diagnostic_block("MATRIX DISCOVERY PRIMARY ADAPTER ATTEMPT", fields)


def _contains_obvious_secret(text: str) -> bool:
    return any(pattern.search(text) for pattern in (
        _AUTH_HEADER, _AUTH_SCHEME, _SENSITIVE_ASSIGNMENT, _JWT, _URL_USERINFO,
    ))


def correlation_fingerprint(domain: str, value: Any) -> str:
    """Return a process-local value-free identifier for correlation diagnostics."""
    if value in (None, ""):
        return "unavailable"
    message = f"{domain}\0{value}".encode("utf-8", errors="replace")
    digest = hmac.new(_CORRELATION_FINGERPRINT_KEY, message, hashlib.sha256)
    return digest.hexdigest()[:CORRELATION_FINGERPRINT_LENGTH]


def variable_diagnostic(
    variable: dict[str, Any] | None,
    context: dict[str, Any],
    references: Callable[[Any], list[str]],
) -> dict[str, Any]:
    """Describe variable metadata without exposing its saved values or query."""
    if not isinstance(variable, dict):
        return _missing_variable_diagnostic()
    names = sorted(set(references((
        variable.get("datasource"), variable.get("query"), variable.get("definition"),
    ))))
    return {
        "found": "found", "type": _variable_type(variable.get("type")),
        "current": _saved_value_kind(variable.get("current"), "current"),
        "default": _saved_value_kind(variable, "default"),
        "references": names,
        "missing_references": [name for name in names if name not in context and not name.startswith("__")],
    }


def datasource_diagnostic(variable: dict[str, Any] | None, context: dict[str, Any]) -> dict[str, Any]:
    """Describe datasource resolution inputs without emitting its UID."""
    datasource = variable.get("datasource") if isinstance(variable, dict) else None
    datasource_type, datasource_uid, shape = _datasource_parts(datasource)
    reference = _reference_name(datasource_uid) or _reference_name(datasource_type)
    uid_value = context.get(reference) if reference else datasource_uid
    return {
        "shape": shape,
        "type": "prometheus" if str(datasource_type).lower() == PROMETHEUS_DATASOURCE_TYPE else _value_kind(datasource_type),
        "reference": "variable" if reference else "direct",
        "uid_present": uid_value not in (None, ""),
    }


def _missing_variable_diagnostic() -> dict[str, Any]:
    return {
        "found": "not_found", "type": "missing", "current": "missing", "default": "missing",
        "references": [], "missing_references": [],
    }


def _saved_value_kind(container: Any, field: str) -> str:
    if not isinstance(container, dict) or field not in container:
        return "missing"
    value = container[field]
    if field == "current" and isinstance(value, dict):
        value = value.get("value", value.get("text", _MISSING))
    return "missing" if value is _MISSING else _value_kind(value)


def _variable_type(value: Any) -> str:
    return str(value) if value in SUPPORTED_VARIABLE_TYPES else _value_kind(value)


def _datasource_parts(datasource: Any) -> tuple[Any, Any, str]:
    if isinstance(datasource, dict):
        return datasource.get("type"), datasource.get("uid"), "mapping"
    if isinstance(datasource, str):
        return datasource, datasource, "string"
    return None, None, "missing" if datasource is None else "unsupported"


def _reference_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\$\{([^}:]+)(?::[^}]+)?}|\$(\w+)", value)
    return (match.group(1) or match.group(2)) if match else None


def _value_kind(value: Any) -> str:
    if value is None:
        return "null"
    if value == "":
        return "empty_string"
    if isinstance(value, (str, int, float, bool)):
        return "nonempty_scalar"
    return "unsupported"


_MISSING = object()
