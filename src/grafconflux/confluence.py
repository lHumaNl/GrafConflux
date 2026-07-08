import logging
import os
import random
import time
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple
from atlassian import Confluence

from grafconflux._shared.time import GrafanaTimeBase
from grafconflux._confluence.content import (
    ChildPageInclude,
    GRAPHS_PLACEHOLDER,
    _artifact_has_rendered_png,
    _artifact_title,
    _dashboard_period,
    _first_panel_link,
    _non_repeating_artifact_title,
    _panel_period,
    _render_dashboard_links,
    _render_dashboards_section,
    _render_panel_artifacts,
    _render_panel_timestamps,
    _render_panels,
    _render_snapshot_backup_section,
    _render_test_times_section,
    apply_graphs_placeholder,
    apply_graphs_placeholder_if_present,
    build_child_page_title,
    build_confluence_storage_content,
    build_parent_include_content,
)
from grafconflux._confluence.links import build_confluence_page_url
# Keep helper imports from grafconflux.confluence stable after extraction.
from grafconflux._confluence.uploads import (
    RETRY_AFTER_HEADER,
    RETRYABLE_STATUS_CODES,
    _ConfluenceUploadRateLimiter,
    _coerce_status_code,
    _direct_header_value,
    _effective_upload_interval,
    _extract_status_code,
    _header_value,
    _is_retryable_upload_error,
    _iterated_header_value,
    _parse_retry_after,
    _parse_retry_after_date,
    _retry_after_seconds,
    _retry_after_header_value,
    _status_code_from_source,
)
from grafconflux._shared.grafana_models import GrafanaConfigBase

logger = logging.getLogger(__name__)

DEFAULT_CONTENT_TYPE = 'image/png'
CHILD_PAGE_LOOKUP_LIMIT = 100


class ConfluenceManager:
    """
    Manages interactions with Confluence.
    """

    _upload_rate_limiter = _ConfluenceUploadRateLimiter()

    def __init__(self, login: Optional[str], password: Optional[str], page_id: int, upload_threads: int,
                  wiki_url: str, verify_ssl: bool,
                  upload_delay: float = 0, upload_rate_per_second: Optional[float] = None,
                  retry_enabled: bool = True, retry_count: int = 3, retry_delay: float = 5,
                  retry_backoff_multiplier: float = 1.0, retry_max_delay: Optional[float] = None,
                  retry_jitter: float = 0, continue_on_error: bool = False,
                  token: Optional[str] = None) -> None:
        self.login = login
        self.password = password
        self.token = token
        self.page_id = page_id
        self.upload_threads = max(1, int(upload_threads))
        self.wiki_url = wiki_url
        self.verify_ssl = verify_ssl
        self.upload_delay = upload_delay
        self.upload_rate_per_second = upload_rate_per_second
        self.retry_enabled = retry_enabled
        self.retry_count = max(0, int(retry_count))
        self.retry_delay = retry_delay
        self.retry_backoff_multiplier = retry_backoff_multiplier
        self.retry_max_delay = retry_max_delay
        self.retry_jitter = retry_jitter
        self.continue_on_error = continue_on_error
        self.last_parent_page_url = None
        self._validate_retry_and_rate_options()
        self.upload_interval = _effective_upload_interval(upload_delay, upload_rate_per_second)
        self._upload_clients = threading.local()
        self.confluence = Confluence(**self._client_kwargs())
        self._upload_clients.client = self.confluence

    def _validate_retry_and_rate_options(self) -> None:
        if self.upload_rate_per_second is not None and self.upload_rate_per_second <= 0:
            raise ValueError('confluence_upload_rate_per_second must be greater than 0 when set')
        if self.retry_backoff_multiplier < 1:
            raise ValueError('confluence_retry_backoff_multiplier must be greater than or equal to 1')
        if self.retry_max_delay is not None and self.retry_max_delay < 0:
            raise ValueError('confluence_retry_max_delay must be greater than or equal to 0')
        if self.retry_jitter < 0:
            raise ValueError('confluence_retry_jitter must be greater than or equal to 0')

    @classmethod
    def reset_upload_rate_limiter(cls) -> None:
        cls._upload_rate_limiter.reset()

    def _create_confluence_client(self) -> Confluence:
        return Confluence(**self._client_kwargs())

    def _client_kwargs(self) -> dict:
        kwargs = {"url": self.wiki_url, "verify_ssl": self.verify_ssl}
        if self.token not in (None, ''):
            kwargs["token"] = self.token
            return kwargs
        kwargs["username"] = self.login
        kwargs["password"] = self.password
        return kwargs

    def _get_upload_client(self) -> Confluence:
        """Use one Confluence client per thread to avoid sharing sessions."""
        client = getattr(self._upload_clients, 'client', None)
        if client is None:
            client = self._create_confluence_client()
            self._upload_clients.client = client
        return client

    @staticmethod
    def get_files_from_folder(test_folder: str, file_format: str) -> List[str]:
        files_list = []
        for file in sorted(os.listdir(test_folder)):
            if file.__contains__(file_format):
                files_list.append(file)

        return files_list

    def upload_charts(self, files_path: str, files_format: Optional[List[List]] = None) -> List[Exception]:
        """
        Upload charts to Confluence attachments.
        """
        upload_items = self._get_upload_items(files_path, files_format)
        if self.upload_threads == 1:
            return self._upload_charts_serial(upload_items)
        return self._upload_charts_parallel(upload_items)

    def _get_upload_items(self, files_path: str, files_format: Optional[List[List]]) -> List[Tuple[str, str, str]]:
        upload_items = []
        for file in sorted(os.listdir(files_path)):
            file_content_type = self._get_content_type(file, files_format)
            if file_content_type is not None:
                upload_items.append((os.path.join(files_path, file), file, file_content_type))
        return upload_items

    def _get_content_type(self, file: str, files_format: Optional[List[List]]) -> Optional[str]:
        if files_format is None:
            return DEFAULT_CONTENT_TYPE
        for file_format, file_content_type in files_format:
            if file.__contains__(file_format):
                return file_content_type
        return None

    def _upload_charts_serial(self, upload_items: List[Tuple[str, str, str]]) -> List[Exception]:
        failures = []
        for upload_item in upload_items:
            failures.extend(self._upload_item_with_policy(upload_item))
        return failures

    def _upload_charts_parallel(self, upload_items: List[Tuple[str, str, str]]) -> List[Exception]:
        failures = []
        with ThreadPoolExecutor(max_workers=self.upload_threads) as executor:
            futures = {executor.submit(self.__upload_attachment, *item): item[1] for item in upload_items}
            for future in as_completed(futures):
                failures.extend(self._handle_upload_future(future, futures[future]))
        return failures

    def _upload_item_with_policy(self, upload_item: Tuple[str, str, str]) -> List[Exception]:
        try:
            self.__upload_attachment(*upload_item)
            return []
        except Exception as error:
            return self._handle_final_upload_error(upload_item[1], error)

    def _handle_upload_future(self, future: Future, file: str) -> List[Exception]:
        try:
            future.result()
            return []
        except Exception as error:
            return self._handle_final_upload_error(file, error)

    def _handle_final_upload_error(self, file: str, error: Exception) -> List[Exception]:
        if not self.continue_on_error:
            raise error
        logger.error(f'Continuing after failed upload {file}: {error}')
        return [error]

    def __upload_attachment(self, file_path: str, file: str, file_content_type: str = DEFAULT_CONTENT_TYPE) -> None:
        """
        Upload a single attachment to Confluence with global rate limiting and retry logic.
        """
        max_attempts = 1 + self.retry_count if self.retry_enabled else 1
        for attempt in range(max_attempts):
            try:
                self._upload_attachment_once(file_path, file, file_content_type)
                return
            except Exception as error:
                if self._is_final_upload_attempt(attempt, max_attempts, error):
                    self._log_final_upload_failure(file, max_attempts, error)
                    raise
                self._sleep_before_retry(file, attempt, max_attempts, error)

    def _is_final_upload_attempt(self, attempt: int, max_attempts: int, error: Exception) -> bool:
        if attempt == max_attempts - 1:
            return True
        return not _is_retryable_upload_error(error)

    def _log_final_upload_failure(self, file: str, max_attempts: int, error: Exception) -> None:
        if _is_retryable_upload_error(error):
            logger.error(f'Failed to upload {file} after {max_attempts} attempts: {error}')
        else:
            logger.error(f'Failed to upload {file}; non-retryable response: {error}')

    def _upload_attachment_once(self, file_path: str, file: str, file_content_type: str) -> None:
        ConfluenceManager._upload_rate_limiter.acquire(self.upload_interval)
        self._get_upload_client().attach_file(
            filename=file_path,
            name=file,
            content_type=file_content_type,
            page_id=str(self.page_id),
        )

    def _sleep_before_retry(self, file: str, attempt: int, max_attempts: int, error: Exception) -> None:
        retry_delay = self._retry_delay_for_attempt(attempt, error)
        logger.warning(
            f'Failed to upload {file} (attempt {attempt + 1}/{max_attempts}), '
            f'retrying in {retry_delay}s: {error}'
        )
        if retry_delay > 0:
            time.sleep(retry_delay)

    def _retry_delay_for_attempt(self, attempt: int, error: Exception) -> float:
        configured_delay = self._configured_retry_delay(attempt)
        retry_after_delay = _retry_after_seconds(error)
        if retry_after_delay is None:
            return configured_delay
        return max(configured_delay, retry_after_delay)

    def _configured_retry_delay(self, attempt: int) -> float:
        retry_delay = self.retry_delay * (self.retry_backoff_multiplier ** attempt)
        if self.retry_jitter > 0:
            retry_delay += random.uniform(0, self.retry_jitter)
        if self.retry_max_delay is not None:
            retry_delay = min(retry_delay, self.retry_max_delay)
        return retry_delay

    def update_page_content(self, grafana_configs: List[GrafanaConfigBase], timestamps: List[GrafanaTimeBase],
                            graph_width: int, test_folder: str):
        """
        Update the Confluence page with the new content.
        """
        page = self.confluence.get_page_by_id(self.page_id, expand='body.storage')

        snapshot_list = self.get_files_from_folder(test_folder, '.json')

        new_content = build_confluence_storage_content(grafana_configs, timestamps, graph_width, snapshot_list)

        body: str = page['body']['storage']['value']
        new_content = apply_graphs_placeholder(body, new_content)

        self.confluence.update_page(
            page_id=self.page_id,
            title=page['title'],
            body=new_content
        )

        logger.info('Confluence page content updated.')
        return self.page_url(self.page_id, page)

    def page_url(self, page_id: int, page: dict | None = None) -> str:
        """Return a stable URL for a Confluence page."""
        return build_confluence_page_url(self.wiki_url, page_id, page)

    def get_parent_page(self, parent_page_id: int) -> dict:
        """Return a parent page with storage body and space metadata."""
        return self.confluence.get_page_by_id(parent_page_id, expand='body.storage,space')

    def create_or_get_child_page(self, parent_page_id: int, args) -> ChildPageInclude:
        """Create a child page when missing and return its include reference."""
        parent_page = self.get_parent_page(parent_page_id)
        space_key = self._space_key(parent_page)
        title = build_child_page_title(parent_page['title'], args)
        child_page = self.find_child_page(parent_page_id, title)
        if child_page is None:
            child_page = self._create_child_page(parent_page_id, space_key, title)
        page_id = int(child_page['id'])
        args.confluence_page_id = page_id
        return ChildPageInclude(
            title=title,
            space_key=space_key,
            page_id=page_id,
            page_url=self.page_url(page_id, child_page),
        )

    def find_child_page(self, parent_page_id: int, title: str) -> dict | None:
        """Find a direct child page by title."""
        for child_page in self._child_pages(parent_page_id):
            if child_page.get('title') == title:
                return child_page
        return None

    def update_parent_include_block(self, parent_page_id: int, child_pages: list[ChildPageInclude]) -> bool:
        """Replace the parent marker with child include macros when present."""
        parent_page = self.get_parent_page(parent_page_id)
        body = parent_page['body']['storage']['value']
        content = build_parent_include_content(child_pages)
        new_body = apply_graphs_placeholder_if_present(body, content)
        if new_body is None:
            self.last_parent_page_url = None
            logger.info('Parent page marker not found; parent page was not updated.')
            return False
        self.confluence.update_page(page_id=parent_page_id, title=parent_page['title'], body=new_body)
        self.last_parent_page_url = self.page_url(parent_page_id, parent_page)
        logger.info('Parent Confluence page include block updated.')
        return True

    def _child_pages(self, parent_page_id: int) -> list[dict]:
        start = 0
        child_pages = []
        while True:
            children = self._child_page_batch(parent_page_id, start)
            child_pages.extend(children)
            if len(children) < CHILD_PAGE_LOOKUP_LIMIT:
                return child_pages
            start += CHILD_PAGE_LOOKUP_LIMIT

    def _child_page_batch(self, parent_page_id: int, start: int) -> list[dict]:
        children = self.confluence.get_page_child_by_type(
            parent_page_id, type='page', start=start, limit=CHILD_PAGE_LOOKUP_LIMIT, expand='version'
        )
        return children.get('results', children) if isinstance(children, dict) else children

    def _create_child_page(self, parent_page_id: int, space_key: str, title: str) -> dict:
        return self.confluence.create_page(
            space=space_key,
            title=title,
            body=GRAPHS_PLACEHOLDER,
            parent_id=parent_page_id,
            representation='storage',
        )

    @staticmethod
    def _space_key(page: dict) -> str:
        space = page.get('space') or {}
        if not space.get('key'):
            raise ValueError('Confluence parent page response does not include a space key.')
        return space['key']
