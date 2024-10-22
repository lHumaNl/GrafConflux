# GrafConflux

**GrafConflux** is a Python utility that automates the process of downloading charts from multiple Grafana instances and
uploading them to a Confluence page. It supports both downloading charts via the Grafana render plugin and capturing
screenshots using Selenium when the render plugin is unavailable. The configuration is defined in a YAML file.

## Features

- Downloads charts from multiple Grafana dashboards.
- Supports two methods for chart acquisition:
    - **Grafana Render Plugin**: Downloads charts directly via Grafana's rendering API.
    - **Selenium Screenshots**: Takes screenshots when the render plugin is not available.
- Uploads the downloaded charts to a Confluence page.
- Configurable via a YAML file.
- Supports authentication for both Grafana and Confluence.
- Handles multiple timestamps and dashboard panel selections.

## Requirements

To run GrafConflux, you need the following dependencies, which are listed in the `requirements.txt`:

- Python 3.x
- Requests
- PyYAML
- Blinker
- Selenium
- Selenium Wire
- Python Dotenv
- Atlassian Python API
- Urllib3
- LXML
- Demjson3

To install the dependencies, run:

```bash
pip install -r requirements.txt
```

## Configuration

The utility uses a YAML configuration file to define the Grafana dashboards, panels, and other parameters for
downloading charts. The configuration file (`config.yaml` by default) is structured as follows:

```yaml
<grafana_instance_name>:
  dash_title: <title>
  host: <host>
  <param>: <value>
```

### YAML Configuration Parameters:

| Parameter                    | Type    | Default | Description                                                    |
|------------------------------|---------|---------|----------------------------------------------------------------|
| `dash_title`                 | `str`   | —       | Title of the dashboard in Grafana.                             |
| `host`                       | `str`   | —       | URL of the Grafana instance.                                   |
| `width`                      | `int`   | `1920`  | Width of the graphs.                                           |
| `height`                     | `int`   | `1080`  | Height of the graphs.                                          |
| `render`                     | `bool`  | `True`  | Whether to use the Grafana render plugin.                      |
| `chrome_driver_preload_time` | `float` | `2.5`   | Preload time for the Chrome driver.                            |
| `timeout`                    | `int`   | `30`    | Timeout for requests.                                          |
| `tz`                         | `str`   | `None`  | Time zone to be used in Grafana.                               |
| `threads`                    | `int`   | `4`     | Number of threads for processing.                              |
| `vars`                       | `dict`  | `None`  | Variables to pass to the Grafana dashboard.                    |
| `white_theme`                | `bool`  | `False` | Whether to use the white theme for rendering.                  |
| `orgId`                      | `int`   | `1`     | Organization ID in Grafana.                                    |
| `login`                      | `str`   | `None`  | Grafana login (if authentication is required).                 |
| `password`                   | `str`   | `None`  | Grafana password (if authentication is required).              |
| `token`                      | `str`   | `None`  | Grafana API token (if used instead of login/password).         |
| `auth`                       | `bool`  | `True`  | Whether authentication is enabled for Grafana.                 |
| `domain`                     | `bool`  | `False` | Whether to use domain-based login (use Confluence login\pass). |
| `verify_ssl`                 | `bool`  | `True`  | Whether to verify SSL certificates in Grafana.                 |
| `folder`                     | `str`   | `None`  | Folder to save graphs locally.                                 |

Each Grafana instance should have its own entry in the configuration file. The `render` parameter specifies whether the
Grafana render plugin should be used (`true`) or if screenshots should be taken using Selenium (`false`).

### Sample `config.yaml`

```yaml
OS Statistic:
  dash_title: OS Statistic Graphs
  host: https://172.0.0.2:3000
  login: admin
  password: admin
  render: false
  threads: 1
Business Statistic:
  dash_title: OS Statistic Graphs
  host: https://192.168.0.2:3000
  token: ==gfsjngfskfdslkfdnsfds
  threads: 4
  verify_ssl: false
Production Statistic:
  dash_title: Production Statistic Graphs
  host: https://10.10.0.2:3000
  domain: true
  threads: 8
DB Statistic:
  dash_title: DB Statistic Graphs
  host: https://10.10.0.2:3003
  auth: false
  threads: 2
```

## Usage

### Command Line Arguments:

| Argument                               | Type   | Default                                               | Description                                                  |
|----------------------------------------|--------|-------------------------------------------------------|--------------------------------------------------------------|
| `-w`, `--wiki_url`                     | `str`  | —                                                     | URL to your Confluence page.                                 |
| `-c`, `--config`                       | `str`  | `config.yaml`                                         | Path to the YAML configuration file.                         |
| `-s`, `--confluence_ignore_verify_ssl` | `flag` | `False`                                               | Ignore SSL certificate verification.                         |
| `-l`, `--confluence_login`             | `str`  | Value from environment variable `CONFLUENCE_LOGIN`    | Confluence login.                                            |
| `-p`, `--confluence_password`          | `str`  | Value from environment variable `CONFLUENCE_PASSWORD` | Confluence password.                                         |
| `-i`, `--confluence_page_id`           | `int`  | —                                                     | ID of the Confluence page for uploading data.                |
| `-f`, `--test_folder`                  | `str`  | `graphs`                                              | Folder for saving the graphs.                                |
| `-H`, `--graph_height`                 | `int`  | `1500`                                                | Height of the graphs in Confluence.                          |
| `-I`, `--test_id`                      | `int`  | `-1`                                                  | Test ID.                                                     |
| `-t`, `--timestamps`                   | `list` | —                                                     | List of time ranges in the format `<tag>__&from=...&to=...`. |

You can run the GrafConflux utility by executing the following command:

```bash
python main.py --wiki_url <CONFLUENCE_URL> --confluence_page_id <PAGE_ID> --confluence_login <LOGIN>
--confluence_password <PASSWORD> --timestamps "<TAG>__&from=<start_time>&to=<end_time>"
```

Replace the placeholders with the appropriate values:

- `<CONFLUENCE_URL>`: The URL of your Confluence instance.
- `<PAGE_ID>`: The ID of the Confluence page where you want to upload the charts.
- `<LOGIN>`: Your Confluence login.
- `<PASSWORD>`: Your Confluence password.
- `--timestamps`: List of time ranges for the charts in the format `<TAG>__&from=<start_time>&to=<end_time>`.

For example:

```bash
python main.py --wiki_url "https://confluence.example.com" --confluence_page_id 12345 --confluence_login "user" --confluence_password "pass" --timestamps "Stability__&from=1609459200000&to=1609545600000"
```