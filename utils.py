import logging
import os
from typing import List

import yaml

from grafana import GrafanaConfig

logger = logging.getLogger(__name__)


def load_grafana_config(path: str) -> List[GrafanaConfig]:
    """
    Load YAML configuration file.
    """
    with open(path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    grafana_configs = []
    for config_name, config_data in config.items():
        grafana_configs.append(GrafanaConfig(name=config_name, **config_data))

    return grafana_configs
