import json
import logging
import os
import re
import threading
import time
from abc import ABC
from typing import Dict, List, Optional
from urllib.parse import urlencode, urlparse
from concurrent.futures import ThreadPoolExecutor, wait

import requests
from seleniumwire import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from lxml import html
import demjson3
import yaml

from services.args_parser import GrafanaTimeDownloader, GrafanaTimeUploader

logger = logging.getLogger(__name__)
logging.getLogger('seleniumwire').setLevel(logging.ERROR)


class Panel:
    def __init__(self, panel_id: int, graph_type: str, title: str, timestamps_count: int):
        self.panel_id: int = panel_id
        self.type: str = graph_type
        self.title: str = title
        self.links: List[Optional[str]] = []

        for i in range(timestamps_count):
            self.links.append(None)


class GrafanaConfigBase(ABC):
    def __init__(self, name: str):
        self.name: str = name
        self.panels: Optional[List[Panel]] = None
        self.full_links: Optional[List[str]] = None
        self.snapshot_urls: Optional[List[str]] = None


class GrafanaConfigUploader(GrafanaConfigBase):
    def __init__(self, name: str, config: Dict):
        super().__init__(name)

        self.panels: Optional[List[Panel]] = []
        if isinstance(config['panels'][0], Panel):
            self.panels = config['panels']
        else:
            for panel in config['panels']:
                self.panels.append(Panel(panel['panel_id'], panel['type'], panel['title'], len(config['timestamps'])))

        self.full_links: Optional[List[str]] = config['full_links']
        self.snapshot_urls: Optional[List[str]] = config['snapshot_urls']
        self.charts_path: str = config['charts_path']

        self.timestamps: List[GrafanaTimeUploader] = []
        if isinstance(config['timestamps'][0], GrafanaTimeUploader):
            self.timestamps = config['timestamps']
        else:
            for timestamp in config['timestamps']:
                self.timestamps.append(GrafanaTimeUploader(timestamp))


class GrafanaConfigDownloader(GrafanaConfigBase):
    """
    Class representing Grafana configuration.
    """

    def __init__(self, name: str, config: Dict):
        super().__init__(name)
        self.dash_title: str = config['dash_title']
        self.host: str = config['host']
        self.width: int = config.get('width', 1920)
        self.height: int = config.get('height', 1080)
        self.render: bool = config.get('render', True)
        self.snapshot: bool = config.get('snapshot', False)
        self.snapshot_timeout: int = config.get('snapshot_timeout', 30)
        self.firefox_driver_preload_time: float = config.get('firefox_driver_preload_time', 2.5)
        self.timeout: int = config.get('timeout', 30)
        self.tz: Optional[str] = config.get('tz', None)
        self.threads: int = config.get('threads', 4)
        self.vars: Optional[Dict[str, str]] = config.get('vars', None)
        self.white_theme: bool = config.get('white_theme', False)
        self.orgId: int = config.get('orgId', 1)
        self.login: Optional[str] = config.get('login', None)
        self.password: Optional[str] = config.get('password', None)
        self.token: Optional[str] = config.get('token', None)
        self.auth: bool = config.get('auth', True)
        self.domain: bool = config.get('domain', False)
        self.verify_ssl: bool = config.get('verify_ssl', True)
        self.folder: Optional[str] = config.get('folder', None)


class GrafanaManager:
    """
    Manages interactions with a Grafana instance.
    """

    def __init__(self, config: GrafanaConfigDownloader):
        self.thread_local = threading.local()
        self.browser_list: Optional[List[webdriver.Firefox]] = []
        self.dashboard_url = ''
        self.config = config
        self.session = requests.Session()
        self.charts_path = ''
        self.dashboard_uid = ''

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

    def download_charts(self, test_folder: str, timestamps: List[GrafanaTimeDownloader]):
        """
        Download charts from Grafana.
        """
        self.charts_path = os.path.join(test_folder, self.config.name)
        os.makedirs(self.charts_path, exist_ok=True)
        logger.info(f'Downloading charts to {self.charts_path}')

        # Get dashboard UID,URL
        self.dashboard_uid, self.dashboard_url = self.get_dashboard_uid()

        # Get panels
        self.config.panels = self.get_panels(timestamps)

        self.config.full_links = self.__get_full_links(timestamps)

        futures = []
        executor = ThreadPoolExecutor(max_workers=self.config.threads)
        try:
            for panel in self.config.panels:
                for timestamp in timestamps:
                    futures.append(
                        executor.submit(
                            self.__download_chart, panel, timestamp
                        )
                    )

            wait(futures)
        finally:
            if self.browser_list:
                for browser in self.browser_list:
                    browser.quit()
                self.browser_list = []

            executor.shutdown()

        if self.config.snapshot:
            self.take_snapshot(timestamps, test_folder)

        self.__save_params_to_file(timestamps, test_folder)

    def take_snapshot(self, timestamps: List[GrafanaTimeDownloader], test_folder: str):
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        firefox_options.add_argument('--disable-gpu')
        firefox_options.add_argument(f'--width={self.config.width}')
        firefox_options.add_argument(f'--height={self.config.height}')

        if not self.config.verify_ssl:
            firefox_options.accept_insecure_certs = True

        selenium_wire_options = {
            'network.stricttransportsecurity.preloadlist': False,
            'network.stricttransportsecurity.enabled': False,
        }

        parsed_url = urlparse(self.config.host)
        grafana_host = parsed_url.hostname

        cookies = {
            name: cookie.__dict__
            for value in self.session.cookies._cookies[grafana_host].values()
            for name, cookie in value.items()
        }

        browser = webdriver.Firefox(options=firefox_options, seleniumwire_options=selenium_wire_options)

        browser.get(self.config.host)

        for name, cookie in cookies.items():
            browser.add_cookie(cookie)

        browser.set_page_load_timeout(self.config.timeout)

        for timestamp in timestamps:
            try:
                browser.get(self.config.full_links[timestamp.id_time])
                time.sleep(5)

                scrollable_div = browser.find_element(By.CSS_SELECTOR, '.scrollbar-view')
                prev_scroll_position = browser.execute_script("return arguments[0].scrollTop", scrollable_div)
                while True:
                    scrollable_div.send_keys(Keys.PAGE_DOWN)
                    current_scroll_position = browser.execute_script("return arguments[0].scrollTop", scrollable_div)

                    if current_scroll_position == prev_scroll_position:
                        break

                    prev_scroll_position = current_scroll_position

                dashboard_menu_button = browser.find_element(By.CSS_SELECTOR, 'button[aria-label="Share dashboard"]')
                dashboard_menu_button.click()
                time.sleep(1)

                snapshot_tab = browser.find_element(By.CSS_SELECTOR, 'a[aria-label="Tab Snapshot"]')
                snapshot_tab.click()
                time.sleep(1)

                snapshot_name_input = browser.find_element(By.CSS_SELECTOR, 'input[id="snapshot-name-input"]')
                snapshot_name = f'{self.config.name}__{timestamp.time_tag}'
                snapshot_name_input.clear()
                snapshot_name_input.send_keys(snapshot_name)

                snapshot_timeout_input = browser.find_element(By.CSS_SELECTOR, 'input[id="timeout-input"]')
                snapshot_timeout_input.clear()
                snapshot_timeout_input.send_keys(f"{self.config.snapshot_timeout}")
                snapshot_timeout_input.send_keys(Keys.HOME)
                snapshot_timeout_input.send_keys(Keys.DELETE)

                save_snapshot_button = browser.find_element("xpath", "//button[.//span[text()='Local Snapshot']]")
                save_snapshot_button.click()
                time.sleep(self.config.snapshot_timeout + 2)

                snapshot_link_element = browser.find_element(By.CSS_SELECTOR, 'input[id="snapshot-url-input"]')
                snapshot_link = snapshot_link_element.get_attribute('value')

                snapshot_key = snapshot_link.split('/')[-1]
                snapshot_json_url = f'{self.config.host}/api/snapshots/{snapshot_key}'
                snapshot_url = f'{self.config.host}/dashboard/snapshot/{snapshot_key}'

                if self.config.snapshot_urls is None:
                    self.config.snapshot_urls = []
                self.config.snapshot_urls.append(snapshot_url)

                logger.info(f'Link to snapshot {self.config.name}: {snapshot_url}')

                browser.quit()

                response = self.session.get(snapshot_json_url)
                if response.status_code != 200:
                    logger.error(f'Failed on {snapshot_json_url}')
                    return

                snapshot_json = response.json()

                output_file = os.path.join(test_folder, f'{self.config.name}__{timestamp.time_tag}.json')
                with open(output_file, 'w') as f:
                    f.write(json.dumps(snapshot_json, ensure_ascii=False, sort_keys=False))

                logger.info(f'Snapshot backup for {self.config.name} saved in {output_file}')
            except Exception as e:
                logger.error(f'Failed on dashboard {self.config.name}: {e}', exc_info=True)
            finally:
                if browser:
                    browser.quit()

    @classmethod
    def convert_to_dict(cls, obj):
        if isinstance(obj, list):
            return [cls.convert_to_dict(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return {key: cls.convert_to_dict(value) for key, value in obj.__dict__.items()}
        else:
            return obj

    def __save_params_to_file(self, timestamps: List[GrafanaTimeDownloader], test_folder: str):
        save_data = {
            'name': self.config.name,
            'charts_path': self.charts_path,
            'full_links': self.config.full_links,
            'timestamps': self.convert_to_dict(timestamps),
            'panels': self.convert_to_dict(self.config.panels)
        }

        if self.config.snapshot_urls:
            save_data.update({'snapshot_urls': self.config.snapshot_urls})

        with open(os.path.join(test_folder, f'{self.config.name}.yaml'), 'w+', encoding='utf-8') as yaml_file:
            yaml_file.write(yaml.safe_dump(save_data, sort_keys=False, allow_unicode=True))

    def __get_full_links(self, timestamps: List[GrafanaTimeDownloader]):
        url = f'{self.config.host}{self.dashboard_url}'
        links = []

        for timestamp in timestamps:
            params = {
                'orgId': self.config.orgId,
                'from': timestamp.start_time_timestamp,
                'to': timestamp.end_time_timestamp,
            }

            if self.config.vars is not None:
                for key, value in self.config.vars.items():
                    params.update({f'var-{key}': value})

            links.append(f"{url}?{urlencode(params, doseq=True)}")

        return links

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

    def get_panels(self, timestamps: List[GrafanaTimeDownloader]):
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
            panels.append(Panel(raw_panel['id'], raw_panel['type'], raw_panel.get('title', 'Row'), len(timestamps)))

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

    def __download_chart(self, panel: Panel, timestamp: GrafanaTimeDownloader):
        """
        Download or render a single chart.
        """
        if not self.config.render:
            browser = getattr(self.thread_local, 'browser', None)

            if browser is None:
                browser = self.__init_browser()
                if browser:
                    self.thread_local.browser = browser
                    self.browser_list.append(browser)
                else:
                    logger.error('Failed to initialize browser')
                    return
        else:
            browser = None

        file_name = f'{self.config.name}__{panel.panel_id}__{timestamp.id_time}.png'

        file_path = os.path.join(self.charts_path, file_name)
        url, params = self.__build_panel_url(panel, timestamp)
        final_url = f"{url}?{urlencode(params, doseq=True)}"

        if self.config.render:
            # Use Grafana rendering API
            del params['viewPanel']
            params['width'] = self.config.width
            params['height'] = self.config.height
            params['timeout'] = self.config.timeout

            render_url = f'{self.config.host}/render/d-solo{self.dashboard_url[2:]}'
            response = self.session.get(render_url, params=params, verify=self.config.verify_ssl,
                                        timeout=self.config.timeout)

            try:
                self.session.get(f"{final_url}&fullscreen", verify=self.config.verify_ssl, timeout=self.config.timeout)
                panel.links[timestamp.id_time] = f"{final_url}&fullscreen"
            except Exception:
                self.session.get(final_url, verify=self.config.verify_ssl, timeout=self.config.timeout)
                panel.links[timestamp.id_time] = final_url

            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)

                logger.info(f'Downloaded chart to {file_path}')
            else:
                logger.error(f'Failed to download chart for panel {panel.panel_id}')
        else:
            # Use headless browser
            self.__take_screenshot(browser, panel, timestamp.id_time, final_url, file_path)

    def __build_panel_url(self, panel: Panel, timestamp: GrafanaTimeDownloader):
        """
        Build the URL for a panel in view mode.
        """
        url = f'{self.config.host}{self.dashboard_url}'
        params = {
            'orgId': self.config.orgId,
            'panelId': panel.panel_id,
            'viewPanel': panel.panel_id,
            'from': timestamp.start_time_timestamp,
            'to': timestamp.end_time_timestamp,
            'theme': 'light' if self.config.white_theme else 'dark',
        }

        if self.config.tz:
            params.update({'tz': self.config.tz})

        if self.config.vars is not None:
            for key, value in self.config.vars.items():
                params.update({f'var-{key}': value})

        return url, params

    def __init_browser(self):
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        firefox_options.add_argument('--disable-gpu')
        firefox_options.add_argument(f'--width={self.config.width}')
        firefox_options.add_argument(f'--height={self.config.height}')

        if not self.config.verify_ssl:
            firefox_options.accept_insecure_certs = True

        selenium_wire_options = {
            'network.stricttransportsecurity.preloadlist': False,
            'network.stricttransportsecurity.enabled': False,
        }

        parsed_url = urlparse(self.config.host)
        grafana_host = parsed_url.hostname

        cookies = {
            name: cookie.__dict__
            for value in self.session.cookies._cookies[grafana_host].values()
            for name, cookie in value.items()
        }

        browser = webdriver.Firefox(options=firefox_options, seleniumwire_options=selenium_wire_options)

        try:
            browser.get(self.config.host)

            for name, cookie in cookies.items():
                browser.add_cookie(cookie)

            browser.set_page_load_timeout(self.config.timeout)

            return browser
        except Exception as e:
            logger.error(f'Failed to configure browser: {e}')

            return None

    def __take_screenshot(self, browser: webdriver.Firefox, panel: Panel, time_id: int, final_url, file_path):
        """
        Use a headless browser to take a screenshot of the panel.
        """
        panel_data_sources = self.__get_panel_data_sources(final_url)

        try:
            browser.get(f"{final_url}&fullscreen")
            is_200 = any(
                request.url == f"{final_url}&fullscreen" and request.response.status_code == 200
                for request in browser.requests
            )

            if not is_200:
                raise Exception

            panel.links[time_id] = f"{final_url}&fullscreen"
            self.__wait_for_network_request(browser, panel_data_sources, self.config.timeout)
            browser.save_screenshot(file_path)

            logger.info(f'Screenshot saved to {file_path}')
        except Exception:
            try:
                browser.get(final_url)
                is_200 = any(
                    request.url == final_url and request.response.status_code == 200
                    for request in browser.requests
                )

                if not is_200:
                    raise Exception(f'Request to {final_url} does not return 200 OK!')

                panel.links[time_id] = final_url
                self.__wait_for_network_request(browser, panel_data_sources, self.config.timeout)
                browser.save_screenshot(file_path)

                logger.info(f'Screenshot saved to {file_path}')
            except Exception as e:
                logger.error(f'Failed to take screenshot: {e}')

    def __wait_for_network_request(self, browser: webdriver.Firefox, url_part: List[str], timeout):
        """
        Wait until a network request containing `url_part` has completed.
        """
        if url_part:
            time.sleep(self.config.firefox_driver_preload_time)
            start_time = time.time()

            while True:
                responses = [
                    request
                    for url in url_part
                    for request in browser.requests
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

                if time.time() - start_time > timeout - self.config.firefox_driver_preload_time:
                    return

                time.sleep(0.1)
        else:
            time.sleep(self.config.timeout)

    def __get_panel_data_sources(self, final_url):
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

    @staticmethod
    def load_grafana_config(path: str) -> List[GrafanaConfigDownloader]:
        """
        Load YAML configuration file.
        """
        with open(path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        grafana_configs = []
        for config_name, config_data in config.items():
            grafana_configs.append(GrafanaConfigDownloader(config_name, config_data))

        return grafana_configs
