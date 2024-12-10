import logging
import os
import html
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Optional
from atlassian import Confluence
from demjson3 import content_type

from services.args_parser import GrafanaTimeBase
from services.grafana import GrafanaConfigBase

logger = logging.getLogger(__name__)


class ConfluenceManager:
    """
    Manages interactions with Confluence.
    """

    def __init__(self, login: str, password: str, page_id: int, upload_threads: int, wiki_url: str, verify_ssl: bool):
        self.login = login
        self.password = password
        self.page_id = page_id
        self.upload_threads = upload_threads
        self.confluence = Confluence(
            url=wiki_url,
            username=login,
            password=password,
            verify_ssl=verify_ssl
        )

    @staticmethod
    def get_files_from_folder(test_folder: str, file_format: str):
        files_list = []
        for file in os.listdir(test_folder):
            if file.__contains__(file_format):
                files_list.append(file)

        return files_list

    def upload_charts(self, files_path: str, files_format: Optional[List[List]] = None):
        """
        Upload charts to Confluence attachments.
        """
        # executor = ThreadPoolExecutor(max_workers=self.upload_threads)
        # futures = []

        for file in os.listdir(files_path):
            if files_format:
                if any(file.__contains__(file_format[0]) for file_format in files_format):
                    file_path = os.path.join(files_path, file)
                    file_content_type = [
                        file_format[1]
                        for file_format in files_format
                        if file.__contains__(file_format[0])
                    ][0]
                    self.__upload_attachment(file_path, file, file_content_type)
            else:
                file_path = os.path.join(files_path, file)
                self.__upload_attachment(file_path, file)
            # futures.append(
            #    executor.submit(self.__upload_attachment, file_path)
            # )

        # wait(futures)
        # executor.shutdown()

    def __upload_attachment(self, file_path: str, file: str, file_content_type: str = 'image/png'):
        """
        Upload a single attachment to Confluence.
        """
        self.confluence.attach_file(
            filename=file_path,
            name=file,
            content_type=file_content_type,
            page_id=str(self.page_id)
        )

    def update_page_content(self, grafana_configs: List[GrafanaConfigBase], timestamps: List[GrafanaTimeBase],
                            graph_width: int, test_folder: str):
        """
        Update the Confluence page with the new content.
        """
        page = self.confluence.get_page_by_id(self.page_id, expand='body.storage')

        snapshot_list = self.get_files_from_folder(test_folder, '.json')

        new_content = ''

        if snapshot_list:
            new_content += '<ac:structured-macro ac:name="expand">\n'
            new_content += '  <ac:parameter ac:name="title">Snapshot backups</ac:parameter>\n'
            new_content += '  <ac:rich-text-body>\n'

            for snapshot in snapshot_list:
                snapshot_name = html.escape(snapshot)
                new_content += f'<p><ac:link><ri:attachment ri:filename="{snapshot_name}" />'
                new_content += f'<ac:plain-text-link-body><![CDATA[{snapshot_name}]]></ac:plain-text-link-body>'
                new_content += '</ac:link></p>\n'

            new_content += '  </ac:rich-text-body>\n'
            new_content += '</ac:structured-macro>\n'

        new_content += '<ac:structured-macro ac:name="expand">\n'
        new_content += '  <ac:parameter ac:name="title">Test times</ac:parameter>\n'
        new_content += '  <ac:rich-text-body>\n'

        new_content += '<table>\n'
        new_content += '  <tbody>\n'
        new_content += '    <tr>\n'
        new_content += '      <th>Test tag</th>\n'
        new_content += '      <th>Start test time</th>\n'
        new_content += '      <th>End test time</th>\n'
        new_content += '    </tr>\n'

        for timestamp in timestamps:
            new_content += '    <tr>\n'
            new_content += f'      <td>{timestamp.time_tag}</td>\n'
            new_content += f'      <td>{timestamp.start_time_human}</td>\n'
            new_content += f'      <td>{timestamp.end_time_human}</td>\n'
            new_content += '    </tr>\n'

        new_content += '  </tbody>\n'
        new_content += '</table>\n'

        new_content += '  </ac:rich-text-body>\n'
        new_content += '</ac:structured-macro>\n'

        for grafana_config in grafana_configs:
            dash_title = html.escape(grafana_config.name)
            new_content += f'<h2>{dash_title}</h2>\n'

            new_content += '<p>Dashboard links</p>\n'
            snapshot_urls = ''

            for timestamp in timestamps:
                if len(timestamps) > 1:
                    period = f'{html.escape(timestamp.time_tag)}' \
                        if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
                else:
                    period = f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else ''

                new_content += (f'<p><a href="{html.escape(grafana_config.full_links[timestamp.id_time])}">'
                                f'{period}</a></p>\n')
                if grafana_config.snapshot_urls:
                    snapshot_urls += (f'<p><a href="{html.escape(grafana_config.snapshot_urls[timestamp.id_time])}">'
                                      f'{period} (Snapshot)</a></p>\n')

            if snapshot_urls != '':
                new_content += '<p>Snapshots</p>\n'
                new_content += snapshot_urls

            new_content += '<p>Panels</p>\n'
            new_content += f'<ac:structured-macro ac:name="expand">\n'
            new_content += f'  <ac:parameter ac:name="title">{dash_title}</ac:parameter>\n'
            new_content += '  <ac:rich-text-body>\n'

            for panel in grafana_config.panels:
                row_title = html.escape(panel.title)
                new_content += f'<h3>{row_title}</h3>\n'
                new_content += f'<ac:structured-macro ac:name="expand">\n'
                new_content += f'  <ac:parameter ac:name="title">{row_title}</ac:parameter>\n'
                new_content += '  <ac:rich-text-body>\n'

                for timestamp in timestamps:
                    if len(timestamps) > 1:
                        period = f'{html.escape(timestamp.time_tag)}' if timestamp.time_tag else f'Test {timestamp.id_time + 1}'
                    else:
                        period = f'{row_title}'

                    image_name = f'{grafana_config.name}__{panel.panel_id}__{timestamp.id_time}.png'
                    new_content += f'    <p><a href="{html.escape(panel.links[timestamp.id_time])}">{period}</a></p>\n'
                    new_content += (f'    <p><ac:image ac:width="{graph_width}">'
                                    f'<ri:attachment ri:filename="{html.escape(image_name)}" /></ac:image></p>\n')

                new_content += '  </ac:rich-text-body>\n'
                new_content += '</ac:structured-macro>\n'

            new_content += '  </ac:rich-text-body>\n'
            new_content += '</ac:structured-macro>\n'

        body: str = page['body']['storage']['value']
        if body.__contains__('%%%graphs%%%'):
            new_content = body.replace('%%%graphs%%%', new_content)

        self.confluence.update_page(
            page_id=self.page_id,
            title=page['title'],
            body=new_content
        )

        logger.info('Confluence page content updated.')
