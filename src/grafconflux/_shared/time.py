import re
from abc import ABC
from datetime import datetime
from typing import Dict, Optional

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

        # Try to extract ISO 8601 format (new Grafana format)
        iso_match = re.findall(r'&from=([\d\-T:.Z]+).*?&to=([\d\-T:.Z]+)', timestamp_str)

        if iso_match and 'T' in iso_match[0][0]:
            # New format: ISO 8601 (e.g., 2025-11-16T14:24:49.073Z)
            start_time_str = iso_match[0][0].replace('Z', '+00:00')
            end_time_str = iso_match[0][1].replace('Z', '+00:00')

            start_time_dt = datetime.fromisoformat(start_time_str)
            end_time_dt = datetime.fromisoformat(end_time_str)

            self.start_time_timestamp: int = int(start_time_dt.timestamp()) * 1000
            self.end_time_timestamp: int = int(end_time_dt.timestamp()) * 1000

            # Convert to human readable format in target timezone
            self.start_time_human: str = start_time_dt.astimezone(tz_zone).strftime(human_time_format)
            self.end_time_human: str = end_time_dt.astimezone(tz_zone).strftime(human_time_format)
        else:
            # Old format: timestamp in milliseconds or seconds
            self.start_time_timestamp: int = int(re.findall(r'&from=(\d+)', timestamp_str)[0])
            self.end_time_timestamp: int = int(re.findall(r'&to=(\d+)', timestamp_str)[0])

            # Convert milliseconds to seconds if needed
            if len(str(self.start_time_timestamp)) > 10:
                final_time_start = self.start_time_timestamp / 1000
            else:
                final_time_start = self.start_time_timestamp
                self.start_time_timestamp = self.start_time_timestamp * 1000

            if len(str(self.end_time_timestamp)) > 10:
                final_time_end = self.end_time_timestamp / 1000
            else:
                final_time_end = self.end_time_timestamp
                self.end_time_timestamp = self.end_time_timestamp * 1000

            self.start_time_human: str = (datetime
                                          .fromtimestamp(final_time_start)
                                          .astimezone(tz_zone)
                                          .strftime(human_time_format))
            self.end_time_human: str = (datetime
                                        .fromtimestamp(final_time_end)
                                        .astimezone(tz_zone)
                                        .strftime(human_time_format))
