import datetime
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, wait

import urllib3

from services.args_parser import ArgsParser
from services.grafana import GrafanaManager, GrafanaConfig
from services.confluence import ConfluenceManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
urllib3.disable_warnings()


def main():
    try:
        args = ArgsParser()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        test_folder = os.path.join(args.test_folder, f'{args.test_id}__{current_time}')

        # Load Grafana configurations
        grafana_configs = GrafanaManager.load_grafana_config(args.config_file)

        # Initialize Confluence manager
        confluence_manager = ConfluenceManager(
            login=args.confluence_login,
            password=args.confluence_password,
            page_id=args.confluence_page_id,
            upload_threads=args.threads,
            wiki_url=args.wiki_url,
            verify_ssl=args.confluence_verify_ssl
        )

        # Process each Grafana config
        executor = ThreadPoolExecutor(max_workers=args.threads)
        futures = []

        for grafana_config in grafana_configs:
            futures.append(
                executor.submit(process_grafana_dashboard, grafana_config, test_folder, args, confluence_manager)
            )

        wait(futures)
        executor.shutdown()

        # Update Confluence page content
        if not args.only_graphs:
            confluence_manager.update_page_content(grafana_configs, args.timestamps, args.graph_width)
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
        if not args.only_graphs:
            confluence_manager.upload_charts(
                grafana_manager.charts_path
            )
    except Exception as e:
        logger.error(f'Failed to process dashboard {grafana_config.dash_title}: {e}')


if __name__ == '__main__':
    main()
