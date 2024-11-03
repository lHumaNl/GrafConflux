import logging
import os
import html
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List
from atlassian import Confluence
from services.args_parser import GrafanaTime
from services.grafana import GrafanaConfig

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

    def upload_charts(self, charts_path: str):
        """
        Upload charts to Confluence attachments.
        """
        executor = ThreadPoolExecutor(max_workers=self.upload_threads)
        futures = []

        for root, _, files in os.walk(charts_path):
            for file in files:
                file_path = os.path.join(root, file)
                futures.append(
                    executor.submit(self.__upload_attachment, file_path)
                )

        wait(futures)
        executor.shutdown()

    def __upload_attachment(self, file_path: str):
        """
        Upload a single attachment to Confluence.
        """
        file_name = os.path.basename(file_path)
        self.confluence.attach_file(
            filename=file_path,
            name=file_name,
            content_type='image/png',
            page_id=str(self.page_id)
        )

    def update_page_content(self, grafana_configs: List[GrafanaConfig], timestamps: List[GrafanaTime],
                            graph_width: int):
        """
        Update the Confluence page with the new content.
        """
        page = self.confluence.get_page_by_id(self.page_id, expand='body.storage')

        new_content = ''
        for grafana_config in grafana_configs:
            dash_title = html.escape(grafana_config.name)
            new_content += f'<h2>{dash_title}</h2>\n'

            for timestamp in timestamps:

                if len(timestamps) > 1:
                    period = f' {html.escape(timestamp.time_tag)} ' \
                        if timestamp.time_tag else f' Test {timestamp.id_time + 1} '
                else:
                    period = f' {html.escape(timestamp.time_tag)} ' \
                        if timestamp.time_tag else ' '

                new_content += (f'<p><a href="{html.escape(grafana_config.full_links[timestamp.id_time])}">'
                                f'{dash_title}{period}{timestamp.start_time_human} - {timestamp.end_time_human}</a></p>\n')

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
                        period = f'{html.escape(timestamp.time_tag)} ' if timestamp.time_tag else f'Test {timestamp.id_time + 1} '
                        time_str = f'{timestamp.start_time_human} - {timestamp.end_time_human}'
                    else:
                        period = f'{row_title}'
                        time_str = f''

                    image_name = f'{grafana_config.name}__{panel.panel_id}__{timestamp.id_time}.png'
                    new_content += f'    <p><a href="{html.escape(panel.links[timestamp.id_time])}">{period}{time_str}</a></p>\n'
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
