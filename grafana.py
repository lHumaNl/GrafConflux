import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlencode, urlparse

import requests
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
from lxml import html
import demjson3

from args_parser import GrafanaTime

logger = logging.getLogger(__name__)
logging.getLogger('seleniumwire').setLevel(logging.ERROR)


@dataclass
class Panel:
    panel_id: int
    type: str
    title: str


@dataclass
class GrafanaConfig:
    """
    Class representing Grafana configuration.
    """
    name: str
    dash_title: str
    host: str
    panels: Optional[List[Panel]] = None
    width: int = 1920
    height: int = 1080
    render: bool = True
    chrome_driver_preload_time: float = 2.5
    timeout: int = 30
    tz: Optional[str] = None
    threads: int = 4
    vars: Optional[Dict[str, str]] = None
    white_theme: bool = False
    orgId: int = 1
    login: str = None
    password: str = None
    token: str = None
    auth: bool = True
    domain: bool = False
    verify_ssl: bool = True
    folder: Optional[str] = None


class GrafanaManager:
    """
    Manages interactions with a Grafana instance.
    """

    def __init__(self, config: GrafanaConfig):
        self.browser: Optional[webdriver.Chrome] = None
        self.dashboard_url = ''
        self.config = config
        self.session = requests.Session()
        self.charts_path = ''
        self.dashboard_uid = ''
        self.panels: List[Panel] = []

    def authenticate(self, confluence_login: str, confluence_password: str):
        """
        Authenticate with Grafana using the specified method.
        """
        if self.config.auth is False:
            logger.info('Authentication disabled for this Grafana instance.')
            return

        if self.config.domain:
            login = confluence_login.split('@')[0]
            password = confluence_password
        elif self.config.login and self.config.password:
            login = self.config.login
            password = self.config.password
        elif self.config.token:
            self.session.headers.update({'Authorization': f'Bearer {self.config.token}'})
            return
        else:
            raise ValueError('No valid authentication method provided.')

        # Authenticate
        payload = {
            'user': login,
            'password': password
        }

        response = self.session.post(f'{self.config.host}/login', headers={'Content-type': 'application/json'},
                                     data=json.dumps(payload), verify=self.config.verify_ssl)

        if response.status_code != 200:
            raise ConnectionError('Failed to authenticate with Grafana.')
        logger.info('Successfully authenticated with Grafana.')

    def download_charts(self, test_folder: str, timestamps: List[GrafanaTime]):
        """
        Download charts from Grafana.
        """
        self.charts_path = os.path.join(test_folder, self.config.name)
        os.makedirs(self.charts_path, exist_ok=True)
        logger.info(f'Downloading charts to {self.charts_path}')

        # Get dashboard UID,URL
        self.dashboard_uid, self.dashboard_url = self.get_dashboard_uid()

        # Get panels
        self.panels = self.get_panels()
        self.config.panels = self.panels

        # try:
        #     if not self.config.render:
        #         self.init_browser()
        #     #
        #     #     # Download panels in threads
        #     #     # panel_threads = []
        #     for panel in self.panels:
        #         #         # thread = threading.Thread(target=self.download_panel_charts, args=(panel, timestamps))
        #         self.download_panel_charts(panel, timestamps)
        # #         # panel_threads.append(thread)
        # #         # thread.start()
        # finally:
        #     if self.browser:
        #         self.browser.close()

        # for thread in panel_threads:
        #    thread.join()

    def get_dashboard_uid(self):
        """
        Retrieve the UID of the dashboard by its title.
        """
        response = self.session.get(f'{self.config.host}/api/search',
                                    params={'query': self.config.dash_title},
                                    verify=self.config.verify_ssl)
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard list.')

        dashboards = response.json()
        for dash in dashboards:
            if dash['title'] == self.config.dash_title:
                logger.debug(f'Found dashboard UID: {dash["uid"]}')
                return dash['uid'], dash['url']
        raise ValueError(f'Dashboard with title "{self.config.dash_title}" not found.')

    def get_panels(self):
        """
        Retrieve panel information from the dashboard.
        """
        response = self.session.get(f'{self.config.host}/api/dashboards/uid/{self.dashboard_uid}',
                                    verify=self.config.verify_ssl)
        if response.status_code != 200:
            raise ConnectionError('Failed to retrieve dashboard details.')

        dashboard = response.json()['dashboard']
        raw_panels = self.extract_panels(dashboard['panels'])

        panels = []
        for raw_panel in raw_panels:
            panels.append(Panel(raw_panel['id'], raw_panel['type'], raw_panel.get('title', 'Row')))

        return panels

    def extract_panels(self, panels):
        """
        Recursively extract panels from dashboard panels.
        """
        extracted_panels = []
        for panel in panels:
            if 'panels' in panel:
                extracted_panels.extend(self.extract_panels(panel['panels']))
            else:
                extracted_panels.append(panel)

        return extracted_panels

    def download_panel_charts(self, panel: Panel, timestamps: List[GrafanaTime]):
        """
        Download charts for a specific panel and time periods.
        """
        # threads = []
        for timestamp in timestamps:
            self.download_chart(panel, timestamp)
            # thread = threading.Thread(target=self.download_chart, args=(panel, timestamp, idx))
            # threads.append(thread)
            # thread.start()

        # for thread in threads:
        #    thread.join()

    def download_chart(self, panel: Panel, timestamp: GrafanaTime):
        """
        Download or render a single chart.
        """
        file_name = f'{self.config.name}__{panel.panel_id}__{timestamp.id_time}.png'

        file_path = os.path.join(self.charts_path, file_name)
        url, params = self.build_panel_url(panel, timestamp)

        if self.config.render:
            # Use Grafana rendering API
            render_url = f'{self.config.host}/render/d-solo/{self.dashboard_uid}/{self.dashboard_url}'

            response = self.session.get(render_url, params=params, verify=self.config.verify_ssl)

            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f'Downloaded chart to {file_path}')
            else:
                logger.error(f'Failed to download chart for panel {panel.panel_id}')
        else:
            # Use headless browser
            self.take_screenshot(url, params, file_path)

    def build_panel_url(self, panel: Panel, timestamp: GrafanaTime):
        """
        Build the URL for a panel in view mode.
        """
        url = f'{self.config.host}{self.dashboard_url}'
        params = {
            'orgId': self.config.orgId,
            'panelId': panel.panel_id,
            'viewPanel': panel.panel_id,
            'from': timestamp.start_time,
            'to': timestamp.end_time,
            'theme': 'light' if self.config.white_theme else 'dark',
        }

        if self.config.tz:
            params.update({'tz': self.config.tz})

        if self.config.vars is not None:
            for key, value in self.config.vars.items():
                params.update({f'var-{key}': value})

        return url, params

    def init_browser(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument(f'--window-size={self.config.width},{self.config.height}')
        if not self.config.verify_ssl:
            options.add_argument('--ignore-certificate-errors')

        parsed_url = urlparse(self.config.host)
        grafana_host = parsed_url.hostname

        cookies = {
            name: cookie.__dict__
            for value in self.session.cookies._cookies[grafana_host].values()
            for name, cookie in value.items()
        }
        browser = webdriver.Chrome(options=options)
        try:
            browser.get(self.config.host)
            browser.set_window_size(self.config.width, self.config.height)

            for name, cookie in cookies.items():
                browser.add_cookie(cookie)

            browser.set_page_load_timeout(self.config.timeout)

            self.browser = browser
        except Exception as e:
            logger.error(f'Failed to configure browser: {e}')

    def take_screenshot(self, url, params, file_path):
        """
        Use a headless browser to take a screenshot of the panel.
        """
        final_url = f"{url}?{urlencode(params)}"
        panel_data_sources = self.get_panel_data_sources(final_url)

        try:
            self.browser.get(f"{final_url}&fullscreen")
            is_200 = any(
                request.url == f"{final_url}&fullscreen" and request.response.status_code == 200
                for request in self.browser.requests
            )

            if not is_200:
                raise Exception

            self.wait_for_network_request(panel_data_sources, self.config.timeout)
            self.browser.save_screenshot(file_path)
            logger.info(f'Screenshot saved to {file_path}')
        except Exception:
            try:
                self.browser.get(final_url)
                is_200 = any(
                    request.url == final_url and request.response.status_code == 200
                    for request in self.browser.requests
                )

                if not is_200:
                    raise Exception(f'Request to {final_url} does not return 200 OK!')

                self.wait_for_network_request(panel_data_sources, self.config.timeout)
                self.browser.save_screenshot(file_path)
                logger.info(f'Screenshot saved to {file_path}')
            except Exception as e:
                logger.error(f'Failed to take screenshot: {e}')

    def wait_for_network_request(self, url_part: List[str], timeout):
        """
        Wait until a network request containing `url_part` has completed.
        """
        if url_part:
            time.sleep(self.config.chrome_driver_preload_time)
            start_time = time.time()

            while True:
                responses = [
                    request
                    for url in url_part
                    for request in self.browser.requests
                    if url in request.url
                ]
                is_all_download = all(
                    url in response.url
                    for response in responses
                    for url in url_part
                )
                is_all_200_ok = all(response.response.status_code == 200 for response in responses)

                if is_all_200_ok and is_all_download:
                    return

                if time.time() - start_time > timeout - self.config.chrome_driver_preload_time:
                    return

                time.sleep(0.1)
        else:
            time.sleep(self.config.timeout)

    def get_panel_data_sources(self, final_url):
        response = self.session.get(final_url, verify=self.config.verify_ssl).text
        panel_data_sources = []
        tree = html.fromstring(response)

        script_content = tree.xpath('//script[contains(text(), "window.grafanaBootData")]/text()')

        if script_content:
            data_script = script_content[0]
            match = re.search(r'window\.grafanaBootData\s*=\s*({.*?})\s*;', data_script, re.DOTALL)
            if match:
                data_object = match.group(1)
                data_object_json = demjson3.decode(data_object)
                panel_data_sources = [
                    datasource['url']
                    for datasource in data_object_json['settings']['datasources'].values()
                    if 'url' in datasource
                ]

        return panel_data_sources
