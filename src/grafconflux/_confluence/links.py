"""Confluence page URL helpers."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


def build_confluence_page_url(wiki_url: str, page_id: int | str, page: dict | None = None) -> str:
    """Build a safe Confluence page URL, preferring response metadata links."""
    metadata_url = _metadata_page_url(wiki_url, page)
    if metadata_url:
        return _strip_url_userinfo(metadata_url)
    base_url = _strip_url_userinfo(wiki_url).rstrip('/')
    return f'{base_url}/pages/viewpage.action?pageId={page_id}'


def _metadata_page_url(wiki_url: str, page: dict | None) -> str | None:
    links = (page or {}).get('_links') or {}
    page_path = links.get('webui') or links.get('tinyui')
    if not page_path:
        return None
    if _is_absolute_url(page_path):
        return page_path
    base_url = links.get('base') or wiki_url
    return _join_base_and_path(base_url, page_path)


def _is_absolute_url(value: str) -> bool:
    return value.startswith(('http://', 'https://'))


def _join_base_and_path(base_url: str, page_path: str) -> str:
    return f'{base_url.rstrip("/")}/{page_path.lstrip("/")}'


def _strip_url_userinfo(url: str) -> str:
    parts = urlsplit(url)
    if '@' not in parts.netloc:
        return url
    host = parts.netloc.rsplit('@', 1)[1]
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))
