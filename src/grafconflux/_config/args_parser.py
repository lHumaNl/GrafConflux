import argparse
import os
from typing import List, Optional

import yaml
from grafconflux._config.yaml_settings import (
    DEFAULT_CONFLUENCE_UPLOAD_THREADS,
    DEFAULT_CONTINUE_ON_ERROR,
    DEFAULT_GRAPH_WIDTH,
    DEFAULT_RETRY,
    DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETRY_JITTER,
    DEFAULT_RETRY_MAX_DELAY,
    DEFAULT_THREADS,
    DEFAULT_UPLOAD_DELAY,
    DEFAULT_UPLOAD_RATE_PER_SECOND,
    YamlSettings,
    _yaml_default_setting_values,
    ignore_verify_ssl_or_current,
    wiki_url_or_current,
    yaml_settings_from_config,
)
from grafconflux._shared.time import GrafanaTimeDownloader


class ArgsParser:
    @staticmethod
    def _load_yaml_settings(config_file: str) -> YamlSettings:
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f) or {}

        return yaml_settings_from_config(config_data)

    @staticmethod
    def _build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description='Grafana to Confluence Uploader')
        parser.add_argument('-w', '--wiki_url', type=str, required=True, help='URL to wiki')
        parser.add_argument('-c', '--config', type=str, default='config.yaml',
                            help='Path to YAML configuration file')
        parser.add_argument('-s', '--confluence_ignore_verify_ssl', action='store_true',
                            help='Ignoring SSL verify in Confluence of stored')
        parser.add_argument('-l', '--confluence_login', type=str,
                            default=os.getenv('CONFLUENCE_LOGIN', None),
                            help='Confluence login')
        parser.add_argument('-p', '--confluence_password', type=str,
                            default=os.getenv('CONFLUENCE_PASSWORD', None),
                            help='Confluence password')
        parser.add_argument('-i', '--confluence_page_id', type=int, required=True,
                            help='Confluence page ID to upload data')
        parser.add_argument('-f', '--test_root_folder', type=str, default='graphs', help='Folder for graphs')
        parser.add_argument('-u', '--test_upload_folders', nargs='+', help='Folders with already downloaded graphs')
        parser.add_argument('-W', '--graph_width', type=int, default=DEFAULT_GRAPH_WIDTH, help='Width of graphs in Confluence')
        parser.add_argument('-I', '--test_id', type=str, default='-1', help='Test ID')
        parser.add_argument('-T', '--threads', type=int, default=DEFAULT_THREADS, help='Threads for parsing Grafana dashboards')
        parser.add_argument('--confluence_upload_threads', type=int, default=DEFAULT_CONFLUENCE_UPLOAD_THREADS,
                            help='Threads for Confluence attachment uploads (default: 1)')
        parser.add_argument('-z', '--tz', type=str, default='UTC', help='TZ for --timestamps')
        parser.add_argument('-t', '--timestamps', nargs='+', help='Time periods in format &from=...&to=...')
        parser.add_argument('-g', '--only_graphs', action='store_true', help='Download only graphs')
        parser.add_argument('-d', '--confluence_upload_delay', type=float, default=DEFAULT_UPLOAD_DELAY,
                            help='Delay between Confluence uploads in seconds (global rate limiter)')
        parser.add_argument('--confluence_upload_rate_per_second', type=float, default=DEFAULT_UPLOAD_RATE_PER_SECOND,
                            help='Maximum Confluence upload starts per second (disabled by default)')
        parser.add_argument('--confluence_retry', action=argparse.BooleanOptionalAction, default=DEFAULT_RETRY,
                            help='Enable retry on upload failure')
        parser.add_argument('--confluence_retry_count', type=int, default=DEFAULT_RETRY_COUNT,
                            help='Number of retry attempts after the first failed upload attempt (default: 3)')
        parser.add_argument('--confluence_retry_delay', type=float, default=DEFAULT_RETRY_DELAY,
                            help='Delay between retry attempts in seconds (default: 5)')
        parser.add_argument('--confluence_retry_backoff_multiplier', type=float, default=DEFAULT_RETRY_BACKOFF_MULTIPLIER,
                            help='Retry delay backoff multiplier, must be >= 1 (default: 1.0)')
        parser.add_argument('--confluence_retry_max_delay', type=float, default=DEFAULT_RETRY_MAX_DELAY,
                            help='Optional maximum retry delay in seconds')
        parser.add_argument('--confluence_retry_jitter', type=float, default=DEFAULT_RETRY_JITTER,
                            help='Maximum random retry jitter in seconds (default: 0)')
        parser.add_argument('--confluence_continue_on_error', action='store_true', default=DEFAULT_CONTINUE_ON_ERROR,
                            help='Continue uploading next file on failure (default: False - stop utility)')

        return parser

    @staticmethod
    def _apply_yaml_settings(args: argparse.Namespace, yaml_settings: YamlSettings) -> argparse.Namespace:
        args.wiki_url = wiki_url_or_current(yaml_settings, args.wiki_url)
        args.confluence_ignore_verify_ssl = ignore_verify_ssl_or_current(
            yaml_settings,
            args.confluence_ignore_verify_ssl,
        )
        for setting_name, value in _yaml_default_setting_values(yaml_settings, vars(args)).items():
            setattr(args, setting_name, value)
        return args

    def __init__(self, argv: Optional[List[str]] = None):
        parser = self._build_parser()
        args = parser.parse_args(argv)

        yaml_settings = self._load_yaml_settings(args.config)
        args = self._apply_yaml_settings(args, yaml_settings)

        self.wiki_url: str = args.wiki_url
        self.config_file: str = args.config
        self.confluence_verify_ssl: bool = not args.confluence_ignore_verify_ssl
        self.confluence_login: str = args.confluence_login
        self.confluence_password: str = args.confluence_password
        self.confluence_page_id: int = args.confluence_page_id
        self.test_root_folder: str = args.test_root_folder
        self.test_upload_folders: List[str] = args.test_upload_folders
        self.graph_width: int = args.graph_width
        self.test_id: str = args.test_id
        self.threads: int = args.threads
        self.confluence_upload_threads: int = args.confluence_upload_threads
        self.only_graphs: bool = args.only_graphs
        self.tz: str = args.tz
        self.confluence_upload_delay: float = args.confluence_upload_delay
        self.confluence_upload_rate_per_second: Optional[float] = args.confluence_upload_rate_per_second
        self.confluence_retry: bool = args.confluence_retry
        self.confluence_retry_count: int = args.confluence_retry_count
        self.confluence_retry_delay: float = args.confluence_retry_delay
        self.confluence_retry_backoff_multiplier: float = args.confluence_retry_backoff_multiplier
        self.confluence_retry_max_delay: Optional[float] = args.confluence_retry_max_delay
        self.confluence_retry_jitter: float = args.confluence_retry_jitter
        self.confluence_continue_on_error: bool = args.confluence_continue_on_error

        self.timestamps: List[GrafanaTimeDownloader] = []
        if args.timestamps:
            for id_time, timestamp_str in enumerate(args.timestamps):
                self.timestamps.append(GrafanaTimeDownloader(timestamp_str, id_time, self.tz))

        self.__validate_cli_args()

    def __validate_cli_args(self):
        """
        Validate command-line arguments.
        """
        if self.confluence_login is None or self.confluence_login == '':
            raise ValueError('CLI arg "--confluence_login" is NULL')

        if self.confluence_password is None or self.confluence_password == '':
            raise ValueError('CLI arg "--confluence_password" is NULL')

        if not os.path.isfile(self.config_file):
            raise FileNotFoundError(f'Configuration file {self.config_file} not found.')

        if self.confluence_upload_threads < 1:
            raise ValueError('CLI arg "--confluence_upload_threads" must be greater than 0')

        if self.confluence_upload_rate_per_second is not None and self.confluence_upload_rate_per_second <= 0:
            raise ValueError('CLI arg "--confluence_upload_rate_per_second" must be greater than 0 when set')

        if self.confluence_retry_backoff_multiplier < 1:
            raise ValueError('CLI arg "--confluence_retry_backoff_multiplier" must be greater than or equal to 1')

        if self.confluence_retry_max_delay is not None and self.confluence_retry_max_delay < 0:
            raise ValueError('CLI arg "--confluence_retry_max_delay" must be greater than or equal to 0')

        if self.confluence_retry_jitter < 0:
            raise ValueError('CLI arg "--confluence_retry_jitter" must be greater than or equal to 0')

        if not self.timestamps and not self.test_upload_folders:
            raise ValueError('At least one timestamp must be provided.')
