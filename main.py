import datetime
import logging
import os
import sys
import threading

import urllib3

from args_parser import ArgsParser
from grafana import GrafanaManager, GrafanaConfig
from confluence import ConfluenceManager
from utils import load_grafana_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
urllib3.disable_warnings()


def main():
    try:
        args = ArgsParser()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        test_folder = os.path.join(args.test_folder, f'{args.test_id}__')

        # Load Grafana configurations
        grafana_configs = load_grafana_config(args.config_file)

        # Initialize Confluence manager
        confluence_manager = ConfluenceManager(
            login=args.confluence_login,
            password=args.confluence_password,
            page_id=args.confluence_page_id,
            wiki_url=args.wiki_url,
            verify_ssl=args.confluence_verify_ssl
        )

        # Process each Grafana config
        # threads = []
        for grafana_config in grafana_configs:
            process_grafana_dashboard(grafana_config, test_folder, args, confluence_manager)
            # thread = threading.Thread(target=process_grafana_dashboard, args=(
            #     grafana_config,
            #     args,
            #     confluence_manager
            # ))
            # threads.append(thread)
            # thread.start()

        # for thread in threads:
        #    thread.join()

        # Update Confluence page content
        confluence_manager.update_page_content(grafana_configs, args.timestamps, args.graph_height)

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        sys.exit(1)


def process_grafana_dashboard(grafana_config: GrafanaConfig, test_folder: str, args: ArgsParser,
                              confluence_manager: ConfluenceManager):
    """
    Process a single Grafana dashboard: authenticate, download charts, and upload to Confluence.
    """
    try:
        grafana_manager = GrafanaManager(config=grafana_config)
        grafana_manager.authenticate(args.confluence_login, args.confluence_password)

        # Download charts
        grafana_manager.download_charts(
            test_folder=test_folder,
            timestamps=args.timestamps
        )

        # Upload to Confluence
        # confluence_manager.upload_charts(
        #     grafana_manager.charts_path
        # )

    except Exception as e:
        logger.error(f'Failed to process dashboard {grafana_config.dash_title}: {e}')


if __name__ == '__main__':
    main()
