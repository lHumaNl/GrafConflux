import argparse
import copy
import os
from typing import List, Optional

import yaml
from grafconflux._config.time_input import TimeInputFile, load_time_input_files
from grafconflux._config.yaml_settings import (
    DEFAULT_CONFLUENCE_UPLOAD_THREADS,
    DEFAULT_CONTINUE_ON_ERROR,
    DEFAULT_CHILD_TITLE_PREFIX,
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
    wiki_url_or_current,
    yaml_settings_from_config,
)
from grafconflux._shared.time import GrafanaTimeDownloader

SUPPORTED_PLAYWRIGHT_BROWSERS = ('chromium', 'firefox', 'webkit')


class ArgsParser:
    _TIME_FILE_OVERRIDE_OPTIONS = (
        ('confluence_page_id', '--confluence_page_id'),
        ('confluence_child_title', '--confluence_child_title'),
        ('test_id', '--test_id'),
        ('timestamps', '--timestamps'),
    )

    @staticmethod
    def _parse_bool_option(value: str) -> bool:
        normalized_value = value.strip().lower()
        bool_values = {
            'true': True,
            '1': True,
            'yes': True,
            'y': True,
            'on': True,
            'false': False,
            '0': False,
            'no': False,
            'n': False,
            'off': False,
        }
        if normalized_value not in bool_values:
            raise argparse.ArgumentTypeError(
                'Expected a boolean value for --confluence_verify_ssl: '
                'true/false, yes/no, on/off, or 1/0.'
            )
        return bool_values[normalized_value]

    @staticmethod
    def _has_page_target(args: argparse.Namespace) -> bool:
        return args.confluence_page_id is not None or args.confluence_parent_page_id is not None

    @staticmethod
    def _load_yaml_settings(config_file: str) -> YamlSettings:
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f) or {}

        return yaml_settings_from_config(config_data)

    @staticmethod
    def _build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description='Grafana to Confluence Uploader')
        parser.add_argument('-w', '--wiki_url', type=str, help='URL to wiki')
        parser.add_argument('-c', '--config', type=str, default='config.yaml',
                            help='Path to YAML configuration file')
        parser.add_argument('--time_files', '--times_files', '--timestamps_files', nargs='+', default=None,
                            help='YAML time input file(s) with page_id or parent_page_id, title/test_id, and times')
        parser.add_argument('--confluence_verify_ssl', type=ArgsParser._parse_bool_option, default=None,
                            help='Enable or disable SSL verification for Confluence (true/false)')
        parser.add_argument('-l', '--confluence_login', type=str,
                            default=None,
                            help='Confluence login')
        parser.add_argument('-p', '--confluence_password', type=str,
                            default=None,
                            help='Confluence password')
        parser.add_argument('--confluence_token', type=str, default=None,
                            help='Confluence personal access token')
        parser.add_argument('-i', '--confluence_page_id', type=int, default=None,
                            help='Confluence page ID to upload data')
        parser.add_argument('--confluence_parent_page_id', type=int, default=None,
                            help='Parent Confluence page ID for child-page publish mode')
        parser.add_argument('--confluence_child_title', type=str, default=None,
                            help='Explicit child page title for child-page publish mode')
        parser.add_argument('--confluence_child_title_prefix', type=str, default=DEFAULT_CHILD_TITLE_PREFIX,
                            help='Prefix used in generated child page titles')
        parser.add_argument('--confluence_child_title_from_test_id', action='store_true', default=False,
                            help='Use test_id directly as the child page title')
        parser.add_argument('-f', '--test_root_folder', type=str, default='graphs', help='Folder for graphs')
        parser.add_argument('-u', '--test_upload_folders', nargs='+', help='Folders with already downloaded graphs')
        parser.add_argument('-W', '--graph_width', type=int, default=DEFAULT_GRAPH_WIDTH, help='Width of graphs in Confluence')
        parser.add_argument('-I', '--test_id', type=str, default=None, help='Test ID')
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
        parser.add_argument('--playwright_browser', choices=SUPPORTED_PLAYWRIGHT_BROWSERS, default=None,
                            help='Playwright browser type for render:false screenshots')
        parser.add_argument('--playwright_browser_channel', type=str, default=None,
                            help='Installed browser channel for Playwright, e.g. chrome or msedge')
        parser.add_argument('--playwright_browser_executable_path', type=str, default=None,
                            help='Path to a custom browser executable for Playwright')

        return parser

    @staticmethod
    def _apply_yaml_settings(args: argparse.Namespace, yaml_settings: YamlSettings) -> argparse.Namespace:
        args.wiki_url = wiki_url_or_current(yaml_settings, args.wiki_url)
        ArgsParser._apply_yaml_credentials(args, yaml_settings)
        if args.confluence_verify_ssl is None:
            args.confluence_verify_ssl = ArgsParser._yaml_confluence_verify_ssl(yaml_settings)
        for setting_name, value in _yaml_default_setting_values(yaml_settings, vars(args)).items():
            setattr(args, setting_name, value)
        ArgsParser._apply_env_credentials(args)
        return args

    @staticmethod
    def _apply_yaml_credentials(args: argparse.Namespace, yaml_settings: YamlSettings) -> None:
        for setting_name in ("confluence_login", "confluence_password", "confluence_token"):
            if getattr(args, setting_name) in (None, ""):
                setting_value = getattr(yaml_settings, setting_name)
                if setting_value is not None:
                    setattr(args, setting_name, setting_value)

    @staticmethod
    def _apply_env_credentials(args: argparse.Namespace) -> None:
        env_names = {
            "confluence_login": "CONFLUENCE_LOGIN",
            "confluence_password": "CONFLUENCE_PASSWORD",
            "confluence_token": "CONFLUENCE_TOKEN",
        }
        for setting_name, env_name in env_names.items():
            if getattr(args, setting_name) in (None, ""):
                setattr(args, setting_name, os.getenv(env_name, None))

    @staticmethod
    def _yaml_confluence_verify_ssl(yaml_settings: YamlSettings) -> bool:
        if yaml_settings.confluence_verify_ssl is not None:
            return yaml_settings.confluence_verify_ssl
        if yaml_settings.confluence_ignore_verify_ssl is not None:
            return not yaml_settings.confluence_ignore_verify_ssl
        return True

    def __init__(self, argv: Optional[List[str]] = None):
        parser = self._build_parser()
        args = parser.parse_args(argv)
        cli_overrides = self._time_file_cli_overrides(args)

        if args.time_files is None and not ArgsParser._has_page_target(args):
            parser.error('the following arguments are required: -i/--confluence_page_id or --confluence_parent_page_id')
        if args.confluence_page_id is not None and args.confluence_parent_page_id is not None:
            parser.error('--confluence_page_id and --confluence_parent_page_id are mutually exclusive')

        yaml_settings = self._load_yaml_settings(args.config)
        args = self._apply_yaml_settings(args, yaml_settings)
        time_inputs = self._load_and_apply_time_inputs(args, cli_overrides)
        if args.test_id is None:
            args.test_id = '-1'

        self.wiki_url: str = args.wiki_url
        self.config_file: str = args.config
        self.time_files: Optional[List[str]] = args.time_files
        self.confluence_verify_ssl: bool = args.confluence_verify_ssl
        self.confluence_login: Optional[str] = args.confluence_login
        self.confluence_password: Optional[str] = args.confluence_password
        self.confluence_token: Optional[str] = args.confluence_token
        self.confluence_page_id: int = args.confluence_page_id
        self.confluence_parent_page_id: int = args.confluence_parent_page_id
        self.confluence_child_title: Optional[str] = args.confluence_child_title
        self.confluence_child_title_prefix: str = args.confluence_child_title_prefix
        self.confluence_child_title_from_test_id: bool = args.confluence_child_title_from_test_id
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
        self.playwright_browser: Optional[str] = args.playwright_browser
        self.playwright_browser_channel: Optional[str] = args.playwright_browser_channel
        self.playwright_browser_executable_path: Optional[str] = args.playwright_browser_executable_path
        self.batch_run_args: List[ArgsParser] = []

        self.timestamps: List[GrafanaTimeDownloader] = []
        if args.timestamps:
            for id_time, timestamp_str in enumerate(args.timestamps):
                self.timestamps.append(GrafanaTimeDownloader(timestamp_str, id_time, self.tz))

        self.__validate_cli_args()
        if len(time_inputs) > 1:
            self.batch_run_args = self._build_batch_run_args(time_inputs)
            for batch_args in self.batch_run_args:
                batch_args.__validate_cli_args()

    @classmethod
    def _time_file_cli_overrides(cls, args: argparse.Namespace) -> set[str]:
        overrides = set()
        if args.confluence_page_id is not None:
            overrides.add('confluence_page_id')
        if args.confluence_parent_page_id is not None:
            overrides.add('confluence_parent_page_id')
        if args.confluence_child_title is not None:
            overrides.add('confluence_child_title')
        if args.test_id is not None:
            overrides.add('test_id')
        if args.timestamps is not None:
            overrides.add('timestamps')
        return overrides

    @classmethod
    def _load_and_apply_time_inputs(
        cls,
        args: argparse.Namespace,
        cli_overrides: set[str],
    ) -> List[TimeInputFile]:
        if args.time_files is None:
            return []
        if args.test_upload_folders:
            raise ValueError('--time_files cannot be combined with --test_upload_folders')
        time_inputs = load_time_input_files(args.time_files)
        cls._validate_time_file_override_conflicts(time_inputs, cli_overrides)
        cls._validate_time_input_targets(time_inputs, cli_overrides)
        cls._apply_time_input(args, time_inputs[0], cli_overrides)
        return time_inputs

    @classmethod
    def _validate_time_file_override_conflicts(
        cls,
        time_inputs: List[TimeInputFile],
        cli_overrides: set[str],
    ) -> None:
        if len(time_inputs) <= 1 or not cli_overrides:
            return
        forbidden_options = [
            option_name
            for field_name, option_name in cls._TIME_FILE_OVERRIDE_OPTIONS
            if field_name in cli_overrides
        ]
        if not forbidden_options:
            return
        raise ValueError(
            'Multiple --time_files cannot be combined with CLI overrides for '
            f'{", ".join(forbidden_options)}.'
        )

    @staticmethod
    def _validate_time_input_targets(
        time_inputs: List[TimeInputFile],
        cli_overrides: set[str],
    ) -> None:
        if 'confluence_parent_page_id' in cli_overrides:
            if any(time_input.page_id is not None for time_input in time_inputs):
                raise ValueError('--confluence_parent_page_id cannot override time files with page_id.')
            return
        for time_input in time_inputs:
            if (time_input.page_id is None) == (time_input.parent_page_id is None):
                raise ValueError(f'Time input file {time_input.path} must contain page_id or parent_page_id.')
        if len(time_inputs) > 1:
            modes = {time_input.parent_page_id is not None for time_input in time_inputs}
            if len(modes) > 1:
                raise ValueError('Multiple --time_files cannot mix page_id and parent_page_id.')
            parent_ids = {time_input.parent_page_id for time_input in time_inputs if time_input.parent_page_id is not None}
            if len(parent_ids) > 1:
                raise ValueError('Multiple child-mode --time_files require one common parent_page_id.')

    @staticmethod
    def _apply_time_input(
        args: argparse.Namespace,
        time_input: TimeInputFile,
        cli_overrides: set[str],
    ) -> None:
        if 'confluence_page_id' not in cli_overrides:
            args.confluence_page_id = time_input.page_id
        if 'confluence_parent_page_id' not in cli_overrides:
            args.confluence_parent_page_id = time_input.parent_page_id
        if 'confluence_child_title' not in cli_overrides and time_input.title is not None:
            args.confluence_child_title = time_input.title
        if 'test_id' not in cli_overrides and time_input.test_id is not None:
            args.test_id = time_input.test_id
        if 'timestamps' not in cli_overrides:
            args.timestamps = time_input.timestamps

    def _build_batch_run_args(self, time_inputs: List[TimeInputFile]) -> List['ArgsParser']:
        return [self._clone_for_time_input(time_input) for time_input in time_inputs]

    def _clone_for_time_input(self, time_input: TimeInputFile) -> 'ArgsParser':
        batch_args = copy.copy(self)
        batch_args.batch_run_args = []
        batch_args.confluence_page_id = time_input.page_id
        batch_args.confluence_parent_page_id = self._batch_parent_page_id(time_input)
        batch_args.confluence_child_title = time_input.title
        batch_args.test_id = time_input.test_id if time_input.test_id is not None else '-1'
        batch_args.timestamps = [
            GrafanaTimeDownloader(timestamp_str, id_time, self.tz)
            for id_time, timestamp_str in enumerate(time_input.timestamps)
        ]
        return batch_args

    def _batch_parent_page_id(self, time_input: TimeInputFile) -> int | None:
        if self.confluence_parent_page_id is not None:
            return self.confluence_parent_page_id
        return time_input.parent_page_id

    def __validate_cli_args(self):
        """
        Validate command-line arguments.
        """
        if not self._has_confluence_auth():
            raise ValueError(
                'Confluence auth requires either "--confluence_token" or both '
                '"--confluence_login" and "--confluence_password"'
            )

        if self.wiki_url is None or self.wiki_url == '':
            raise ValueError('CLI arg "--wiki_url" is NULL')

        if not os.path.isfile(self.config_file):
            raise FileNotFoundError(f'Configuration file {self.config_file} not found.')

        if self.confluence_page_id is None and self.confluence_parent_page_id is None:
            raise ValueError('CLI arg "--confluence_page_id" or "--confluence_parent_page_id" is required')

        if self.confluence_page_id is not None and self.confluence_parent_page_id is not None:
            raise ValueError('CLI args "--confluence_page_id" and "--confluence_parent_page_id" are mutually exclusive')

        if self.confluence_parent_page_id is not None and self.test_upload_folders:
            raise ValueError('--test_upload_folders cannot be used with child page mode')

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

        if self.playwright_browser is not None and self.playwright_browser not in SUPPORTED_PLAYWRIGHT_BROWSERS:
            raise ValueError('CLI arg "--playwright_browser" must be one of: chromium, firefox, webkit')

        if not self.timestamps and not self.test_upload_folders:
            raise ValueError('At least one timestamp must be provided.')

    def _has_confluence_auth(self) -> bool:
        has_token = self.confluence_token not in (None, '')
        has_login = self.confluence_login not in (None, '')
        has_password = self.confluence_password not in (None, '')
        return has_token or (has_login and has_password)
