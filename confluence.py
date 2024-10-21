import logging
import os
from typing import List

from atlassian import Confluence

from args_parser import GrafanaTime
from grafana import GrafanaConfig

logger = logging.getLogger(__name__)


class ConfluenceManager:
    """
    Manages interactions with Confluence.
    """

    def __init__(self, login: str, password: str, page_id: int, wiki_url: str, verify_ssl: bool):
        self.login = login
        self.password = password
        self.page_id = page_id
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
        for root, _, files in os.walk(charts_path):
            for file in files:
                file_path = os.path.join(root, file)
                self.upload_attachment(file_path)

    def upload_attachment(self, file_path: str):
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
        logger.debug(f'Uploaded {file_name} to Confluence.')

    def update_page_content(self, grafana_configs:List[GrafanaConfig], timestamps:List[GrafanaTime]):
        """
        Update the Confluence page with the new content.
        """
        page = self.confluence.get_page_by_id(self.page_id, expand='body.storage')

        # Build new content
        new_content = ''
        for grafana_config in grafana_configs:
            new_content += f'<h2>{grafana_config.dash_title}</h2>\n'

            # Assuming rows are defined in panels (simplified)
            for panel in grafana_config.panels:
                row_title = panel.title
                new_content += f'<h3>{row_title}</h3>\n'
                new_content += f'<ac:structured-macro ac:name="expand">\n'
                new_content += f'  <ac:parameter ac:name="title">{row_title}</ac:parameter>\n'
                new_content += '  <ac:rich-text-body>\n'

                for timestamp in timestamps:
                    if timestamp.time_tag:
                        period = timestamp.time_tag
                    else:
                        period = f'Test {timestamp.id_time + 1}'

                    image_name = f'{grafana_config.name}__{panel.panel_id}__{timestamp.id_time}.png'
                    image_link = f'/download/attachments/{self.page_id}/{image_name}'
                    new_content += f'    <p><a href="{image_link}">{panel.title} - {period}</a></p>\n'
                    new_content += f'    <p><ac:image><ri:attachment ri:filename="{image_name}" /></ac:image></p>\n'

                new_content += '  </ac:rich-text-body>\n'
                new_content += '</ac:structured-macro>\n'

        # Update page content
        self.confluence.update_page(
            page_id=self.page_id,
            title=page['title'],
            body=new_content
        )
        logger.info('Confluence page content updated.')
