import datetime
import logging
import os
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List

import urllib3
import yaml

from services.args_parser import ArgsParser
from services.grafana import GrafanaManager, GrafanaConfigDownloader, GrafanaConfigUploader
from services.confluence import ConfluenceManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
urllib3.disable_warnings()


def main():
    try:
        args = ArgsParser()

        if args.test_upload_folders:
            upload_already_downloaded_graphs(args)
        else:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            test_folder = os.path.join(args.test_root_folder, f'{args.test_id}__{current_time}')

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
                confluence_manager.upload_charts(test_folder, [['.json', 'application/json']])
                confluence_manager.update_page_content(grafana_configs, args.timestamps, args.graph_width, test_folder)
    except Exception as e:
        logger.error(f'An error occurred: {e}', exc_info=True)
        sys.exit(1)


def upload_already_downloaded_graphs(args: ArgsParser):
    confluence_manager = ConfluenceManager(
        login=args.confluence_login,
        password=args.confluence_password,
        page_id=args.confluence_page_id,
        upload_threads=args.threads,
        wiki_url=args.wiki_url,
        verify_ssl=args.confluence_verify_ssl
    )

    grafana_configs = []
    for folder in args.test_upload_folders:
        folder_yaml_files = get_yaml_files(folder)

        for file in folder_yaml_files:
            with open(file, 'r', encoding='utf-8') as yaml_file:
                config = yaml.safe_load(yaml_file)

            grafana_configs.append(GrafanaConfigUploader(config['name'], config))

    if len(args.test_upload_folders) > 1:
        grafana_configs, folder_graphs = transform_grafana_configs(grafana_configs, args)
    else:
        folder_graphs = args.test_upload_folders

    for grafana_config in grafana_configs:
        confluence_manager.upload_charts(grafana_config.charts_path)

    confluence_manager.update_page_content(grafana_configs, grafana_configs[0].timestamps, args.graph_width,
                                           folder_graphs)


def transform_grafana_configs(grafana_configs: List[GrafanaConfigUploader], args: ArgsParser):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_folder_graphs = os.path.join(args.test_root_folder, f'{args.test_id}__{current_time}')

    os.makedirs(new_folder_graphs, exist_ok=True)
    timestamps_count = {}

    set_names = set()
    snapshot_urls = {}
    full_links = {}
    timestamps = {}
    panels = {}

    for folder_id, folder in enumerate(args.test_upload_folders):
        for grafana_config in grafana_configs:
            if not (
                    grafana_config.charts_path
                            .replace('\\', '_')
                            .replace('/', '_')
                            .__contains__(folder
                                                  .replace('\\', '_')
                                                  .replace('/', '_')
                                          )
            ):
                continue

            set_names.add(grafana_config.name)

            if grafana_config.name not in snapshot_urls:
                timestamps_count[grafana_config.name] = 0
                snapshot_urls[grafana_config.name] = []
                full_links[grafana_config.name] = []
                timestamps[grafana_config.name] = []
                panels[grafana_config.name] = []

            snapshot_urls[grafana_config.name].extend(grafana_config.snapshot_urls)
            full_links[grafana_config.name].extend(grafana_config.full_links)

            if folder_id == 0:
                timestamps[grafana_config.name].extend(grafana_config.timestamps)
                panels[grafana_config.name].extend(grafana_config.panels)
            else:
                for panel in grafana_config.panels:
                    for root_panel in panels[grafana_config.name]:
                        if panel.panel_id == root_panel.panel_id:
                            root_panel.links.extend(panel.links)
                            break

                for timestamp in grafana_config.timestamps:
                    timestamp.id_time += timestamps_count[grafana_config.name]
                    timestamps[grafana_config.name].append(timestamp)

            new_graphs_folder = os.path.join(new_folder_graphs, grafana_config.name)
            os.makedirs(new_graphs_folder, exist_ok=True)

            for entry in os.listdir(grafana_config.charts_path):
                src_path = os.path.join(grafana_config.charts_path, entry)

                if folder_id == 0:
                    file_entry = entry
                else:
                    match = re.search(r'__(\d+)\.png$', entry)

                    last_number = int(match.group(1))
                    file_entry = entry.replace(f'__{last_number}.png',
                                               f'__{last_number + timestamps_count[grafana_config.name]}.png')

                dst_path = os.path.join(new_graphs_folder, file_entry)

                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)

            timestamps_count[grafana_config.name] += len(grafana_config.timestamps)

    new_configs = []
    for config_name in set_names:
        config_dict = {
            'snapshot_urls': snapshot_urls[config_name],
            'full_links': full_links[config_name],
            'timestamps': timestamps[config_name],
            'panels': panels[config_name],
            'charts_path': os.path.join(new_folder_graphs, config_name)
        }

        new_configs.append(GrafanaConfigUploader(config_name, config_dict))

        config_dict['name'] = config_name
        config_dict['timestamps'] = GrafanaManager.convert_to_dict(config_dict['timestamps'])
        config_dict['panels'] = GrafanaManager.convert_to_dict(config_dict['panels'])

        with open(os.path.join(new_folder_graphs, f'{config_name}.yaml'), 'w+', encoding='utf-8') as yaml_file:
            yaml_file.write(yaml.safe_dump(config_dict, sort_keys=False, allow_unicode=True))

    for upload_folder in args.test_upload_folders:
        for entry in os.listdir(upload_folder):
            if entry.endswith('.json'):
                src_path = os.path.join(upload_folder, entry)
                dst_path = os.path.join(new_folder_graphs, entry)

                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)

    return new_configs, new_folder_graphs


def get_yaml_files(directory):
    yaml_files = []

    for entry in os.listdir(directory):
        full_path = os.path.join(directory, entry)

        if os.path.isfile(full_path) and entry.endswith('.yaml'):
            yaml_files.append(full_path)

    return yaml_files


def process_grafana_dashboard(grafana_config: GrafanaConfigDownloader, test_folder: str, args: ArgsParser,
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
