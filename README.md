# GrafConflux

**GrafConflux** is a Python utility that automates the process of downloading charts from multiple Grafana instances and
uploading them to a Confluence page. It supports both downloading charts via the Grafana render plugin and capturing
screenshots using Selenium when the render plugin is unavailable. The configuration is defined in a YAML file.

## Features

- Downloads charts from multiple Grafana dashboards
- Supports two methods for chart acquisition:
    - **Grafana Render Plugin**: Downloads charts directly via Grafana's rendering API.
    - **Selenium Screenshots**: Takes screenshots when the render plugin is not available (using Firefox driver).
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
- PyTZ

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

| Parameter                     | Type    | Default | Description                                                                         |
|-------------------------------|---------|---------|-------------------------------------------------------------------------------------|
| `dash_title`                  | `str`   | —       | Title of the dashboard in Grafana.                                                  |
| `host`                        | `str`   | —       | URL of the Grafana instance.                                                        |
| `width`                       | `int`   | `1920`  | Width of the graphs.                                                                |
| `height`                      | `int`   | `1080`  | Height of the graphs.                                                               |
| `render`                      | `bool`  | `True`  | Whether to use the Grafana render plugin.                                           |
| `firefox_driver_preload_time` | `float` | `2.5`   | Preload time for the Firefox driver.                                                |
| `timeout`                     | `int`   | `30`    | Timeout for requests.                                                               |
| `tz`                          | `str`   | `None`  | Time zone to be used in Grafana.                                                    |
| `threads`                     | `int`   | `4`     | Number of threads for processing (Currently concurrency is disabled due to issues). |
| `vars`                        | `dict`  | `None`  | Variables to pass to the Grafana dashboard.                                         |
| `white_theme`                 | `bool`  | `False` | Whether to use the white theme for rendering.                                       |
| `orgId`                       | `int`   | `1`     | Organization ID in Grafana.                                                         |
| `login`                       | `str`   | `None`  | Grafana login (if authentication is required).                                      |
| `password`                    | `str`   | `None`  | Grafana password (if authentication is required).                                   |
| `token`                       | `str`   | `None`  | Grafana API token (if used instead of login/password).                              |
| `auth`                        | `bool`  | `True`  | Whether authentication is enabled for Grafana.                                      |
| `domain`                      | `bool`  | `False` | Whether to use domain-based login (use Confluence loginpass).                       |
| `verify_ssl`                  | `bool`  | `True`  | Whether to verify SSL certificates in Grafana.                                      |
| `folder`                      | `str`   | `None`  | Folder in Grafana with dashboard (Need if many dashboards with identical names).    |

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

### Command Line Arguments (from `args_parser.py`):

| Argument                               | Type   | Default                         | Description                                                  |
|----------------------------------------|--------|---------------------------------|--------------------------------------------------------------|
| `-w`, `--wiki_url`                     | `str`  | —                               | URL to your Confluence page.                                 |
| `-c`, `--config`                       | `str`  | `config.yaml`                   | Path to the YAML configuration file.                         |
| `-s`, `--confluence_ignore_verify_ssl` | `flag` | `False`                         | Ignore SSL certificate verification in Confluence.           |
| `-l`, `--confluence_login`             | `str`  | From ENV: `CONFLUENCE_LOGIN`    | Confluence login.                                            |
| `-p`, `--confluence_password`          | `str`  | From ENV: `CONFLUENCE_PASSWORD` | Confluence password.                                         |
| `-i`, `--confluence_page_id`           | `int`  | —                               | ID of the Confluence page for uploading data.                |
| `-f`, `--test_root_folder`             | `str`  | `graphs`                        | Folder for saving the graphs.                                |
| `-u`, `--test_upload_folders`          | `list` | `None`                          | List of folders with already downloaded graphs.              |
| `-W`, `--graph_width`                  | `int`  | `1500`                          | Width of the graphs in Confluence.                           |
| `-I`, `--test_id`                      | `str`  | `-1`                            | Test ID.                                                     |
| `-T`, `--threads`                      | `int`  | `4`                             | Threads for parsing Grafana dashboards (Currently disabled). |
| `-z`, `--tz`                           | `str`  | `UTC`                           | TZ for `--timestamps`.                                       |
| `-t`, `--timestamps`                   | `list` | `None`                          | List of time ranges in the format `<tag>__&from=...&to=...`. |
| `-g`, `--only_graphs`                  | `flag` | `False`                         | Download only graphs (skip page update).                     |

Please note:

* The `--test_upload_folders` argument allows uploading previously downloaded graphs without new fetching.
* The `--only_graphs` argument skips updating the Confluence page content and uploads only the charts.
* The `--tz` parameter defines the timezone for the given timestamps.
* Currently, multithreading is disabled due to reported issues. All parallel execution code has been commented out.

You can run the GrafConflux utility by executing the following command:

```bash
python main.py --wiki_url <CONFLUENCE_URL> --confluence_page_id <PAGE_ID> --confluence_login <LOGIN>
--confluence_password <PASSWORD> --timestamps "<TAG>__&from=<start_time>&to=<end_time>"
```

Replace the placeholders with the appropriate values:

- <CONFLUENCE_URL>: The URL of your Confluence instance.
- <PAGE_ID>: The ID of the Confluence page where you want to upload the charts.
- <LOGIN>: Your Confluence login.
- <PASSWORD>: Your Confluence password.
- `--timestamps`: List of time ranges for the charts in the format `<TAG>__&from=<start_time>&to=<end_time>`.

For example:

```bash
python main.py --wiki_url "https://confluence.example.com" --confluence_page_id 12345 --confluence_login "user" --confluence_password "pass" --timestamps "Stability__&from=1609459200000&to=1609545600000"
```