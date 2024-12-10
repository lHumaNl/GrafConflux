import argparse
import os
import re
from abc import ABC
from datetime import datetime
from typing import List, Optional, Dict

import pytz


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
        self.time_tag: Optional[str] = timestamp_str.split('__')[0]
        if self.time_tag == timestamp_str:
            self.time_tag = None

        self.id_time: int = id_time

        human_time_format = "%Y/%m/%d %H:%M:%S"
        tz_zone = pytz.timezone(tz)

        self.start_time_timestamp: int = int(re.findall(r'&from=(\d+)', timestamp_str)[0])
        self.end_time_timestamp: int = int(re.findall(r'&to=(\d+)', timestamp_str)[0])

        if len(str(self.start_time_timestamp)) > 10:
            final_time_start = self.start_time_timestamp / 1000
        else:
            final_time_start = self.start_time_timestamp

        if len(str(self.end_time_timestamp)) > 10:
            final_time_end = self.end_time_timestamp / 1000
        else:
            final_time_end = self.end_time_timestamp

        self.start_time_human: str = (datetime
                                      .fromtimestamp(final_time_start)
                                      .astimezone(tz_zone)
                                      .strftime(human_time_format))
        self.end_time_human: str = (datetime
                                    .fromtimestamp(final_time_end)
                                    .astimezone(tz_zone)
                                    .strftime(human_time_format))


class ArgsParser:
    def __init__(self):
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
        parser.add_argument('-W', '--graph_width', type=int, default=1500, help='Width of graphs in Confluence')
        parser.add_argument('-I', '--test_id', type=str, default='-1', help='Test ID')
        parser.add_argument('-T', '--threads', type=int, default=4, help='Threads for parsing Grafana dashboards')
        parser.add_argument('-z', '--tz', type=str, default='UTC', help='TZ for --timestamps')
        parser.add_argument('-t', '--timestamps', nargs='+', help='Time periods in format &from=...&to=...')
        parser.add_argument('-g', '--only_graphs', action='store_true', help='Download only graphs')

        args = parser.parse_args()

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
        self.only_graphs: bool = args.only_graphs
        self.tz: str = args.tz

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

        if not self.timestamps and not self.test_upload_folders:
            raise ValueError('At least one timestamp must be provided.')
