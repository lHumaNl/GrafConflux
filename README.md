# GrafConflux

Download Grafana panels for one or more time ranges and publish them to a Confluence page.

## Features

- Downloads panels from multiple Grafana dashboards in one run.
- Supports Grafana render API (`render: true`) or Selenium Firefox screenshots (`render: false`).
- Uploads rendered PNGs to Confluence and updates the target page.
- Supports new and legacy YAML config formats.
- Filters panels by id, title, typed title, regex, and row selectors.
- Renames panel titles inline or with reusable `rename_panels` rules.
- Preserves optional dashboard links, backup links, snapshots, and metadata artifacts.

## Quick start on Windows

```powershell
python -m venv .venv
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt
```

Create a minimal `config.yaml` in the new format:

```yaml
settings:
  wiki_url: https://confluence.example.com

dashboards:
  Operations:
    dash_title: Operations Overview
    host: https://grafana.example.com
    token: <GRAFANA_TOKEN>
```

Set Confluence credentials in the environment:

```powershell
$env:CONFLUENCE_LOGIN = "user@example.com"
$env:CONFLUENCE_PASSWORD = "your-confluence-secret"
```

Run a normal download + upload:

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_page_id 12345 `
  --timestamps "Release__&from=1719792000000&to=1719878400000"
```

## Minimal config

The current new format is detected only when the YAML file has a top-level `settings` key.

```yaml
settings:
  wiki_url: https://confluence.example.com
  graph_width: 1500
  threads: 4

dashboards:
  Operations:
    dash_title: Operations Overview
    host: https://grafana.example.com
    token: <GRAFANA_TOKEN>
    render: true
```

Useful dashboard-level keys:

| Key                            | Purpose                                                |
|--------------------------------|--------------------------------------------------------|
| `dash_title`                   | Dashboard title to find in Grafana.                    |
| `host`                         | Grafana base URL.                                      |
| `render`                       | `true` = render API, `false` = Selenium screenshots.   |
| `threads`                      | Worker threads for that dashboard.                     |
| `vars`                         | Grafana variables to apply.                            |
| `token` / `login` + `password` | Grafana auth.                                          |
| `domain: true`                 | Reuse Confluence credentials for Grafana login.        |
| `verify_ssl`                   | Enable or disable Grafana SSL verification.            |
| `nginx_prefix`                 | Reverse-proxy path prefix such as `/grafana`.          |
| `login_url`                    | Optional Basic-auth login endpoint for proxied setups. |
| `panel_filtering`              | Include or exclude specific panels or rows.            |
| `rename_panels`                | Rename matched panels in the Confluence output.        |
| `backup_dashboard_links`       | Extra dashboard links shown beside main links.         |
| `download_collapsed_rows`      | Include panels inside collapsed Grafana rows.          |
| `download_collapse_panels`     | Legacy alias for collapsed-row downloads.              |
| `snapshot`                     | Create Grafana snapshots and include links.            |

## Run modes

### 1. Download and publish

Use `--timestamps` to download panels and publish them to Confluence.

```powershell
& ".venv\Scripts\python.exe" "main.py" --wiki_url "https://confluence.example.com" --confluence_page_id 12345 --timestamps "Release__&from=1719792000000&to=1719878400000"
```

### 2. Download only

Use `--only_graphs` to download artifacts without any Confluence attachment upload or page update.

```powershell
& ".venv\Scripts\python.exe" "main.py" --wiki_url "https://confluence.example.com" --confluence_page_id 12345 --timestamps "Release__&from=1719792000000&to=1719878400000" --only_graphs
```

### 3. Upload previously downloaded folders

Use `--test_upload_folders` to upload existing output folders.

```powershell
& ".venv\Scripts\python.exe" "main.py" --wiki_url "https://confluence.example.com" --confluence_page_id 12345 --test_upload_folders "graphs\run_a" "graphs\run_b"
```

## Common recipes

### Panel filtering

```yaml
dashboards:
  Operations:
    dash_title: Operations Overview
    host: https://grafana.example.com
    token: <GRAFANA_TOKEN>
    panel_filtering:
      mode: include_only_selected
      include_panels:
        ids: [ 12 ]
        titles:
          - CPU usage
          - { Packet Drops: timeseries }
      exclude_panels:
        title_regex:
          - ".*debug.*"
```

String titles match by title. Typed title selectors disambiguate same-title panels by graph type.

### Filter by row title or regex

```yaml
panel_filtering:
  mode: include_only_selected
  include_rows:
    titles: [ Production ]
    title_regex:
      - "^Critical.*"
```

### Inline rename in a selector

```yaml
panel_filtering:
  mode: include_only_selected
  include_panels:
    titles:
      - { Total drops: { rename: Total packet drops } }
      - { Packet Drops: { type: timeseries, rename: Packet drops series } }
```

### Reusable `rename_panels`

```yaml
vars:
  iface: [ xe0, xe1 ]
rename_panels:
  - title: Packet Drops
    rename: Packet drops
  - title: Packet Drops
    type: timeseries
    rename: Packet drops series
  - id: 20
    rename: Traffic $iface
```

Rename variables in titles use this lookup order: config `vars`, then dashboard templating `current.value`, then
`default`, then `current.text`.

### Backup dashboard links

```yaml
backup_dashboard_links:
  - https://grafana-backup.example.com/d/abc123/operations
```

GrafConflux adds the current `from` and `to` range to each backup link when rendering Confluence content.

### Collapsed rows

```yaml
download_collapsed_rows: true
```

Collapsed-row panels are skipped by default.

### Auth and reverse proxy

```yaml
dashboards:
  Operations:
    dash_title: Operations Overview
    host: https://portal.example.com
    login: grafana-user
    password: your-grafana-secret
    nginx_prefix: /grafana
    login_url: https://portal.example.com/grafana/login
```

Supported Grafana auth modes are login/password, bearer token, `domain: true`, or `login_url` with Basic auth.

## CLI reference

| Option                                         | Meaning                                                                                                              |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `--wiki_url`                                   | Required on the command line, even if `settings.wiki_url` exists.                                                    |
| `--confluence_page_id`                         | Target Confluence page ID.                                                                                           |
| `--config`                                     | Config file path. Default: `config.yaml`.                                                                            |
| `--timestamps`                                 | One or more `<tag>__&from=...&to=...` ranges. Accepts epoch seconds, epoch milliseconds, or Grafana ISO-8601 ranges. |
| `--test_upload_folders`                        | Upload previously downloaded folders instead of downloading new panels.                                              |
| `--only_graphs`                                | Download only; skip all Confluence uploads and page updates.                                                         |
| `--confluence_login` / `--confluence_password` | Confluence credentials. Defaults from `CONFLUENCE_LOGIN` and `CONFLUENCE_PASSWORD`.                                  |
| `--graph_width`                                | Attachment width in Confluence output.                                                                               |
| `--threads`                                    | Top-level dashboard worker threads.                                                                                  |
| `--tz`                                         | Time zone used for timestamp parsing. Default: `UTC`.                                                                |
| `--confluence_upload_threads`                  | Attachment upload workers.                                                                                           |
| `--confluence_upload_delay`                    | Global delay between Confluence upload starts.                                                                       |
| `--confluence_upload_rate_per_second`          | Global upload rate limit.                                                                                            |
| `--confluence_retry*`                          | Retry and backoff controls for attachment uploads.                                                                   |
| `--confluence_continue_on_error`               | Continue after upload failures instead of stopping.                                                                  |

## Important behavior and warnings

- A run must provide at least one of `--timestamps` or `--test_upload_folders`.
- `--wiki_url` is still mandatory in CLI parsing, even when `settings.wiki_url` is present.
- After parsing, `settings.wiki_url` overrides the CLI `--wiki_url` value.
- New-format config detection depends only on top-level `settings`; a file with only `dashboards` is treated as legacy
  format.
- `render: true` uses the Grafana render API. `render: false` uses Selenium Firefox screenshots.
- If the Confluence page body contains `%%%graphs%%%`, generated content replaces that marker. Otherwise GrafConflux
  replaces the whole page body.
- Dashboard and panel downloads use worker threads. Confluence uploads are effectively rate-limited globally.

## Output artifacts

Each fresh run writes to `graphs\<test_id>__<timestamp>\`.

Typical artifacts include:

- Rendered panel PNG files.
- Per-dashboard YAML metadata used for later uploads.
- Optional snapshot JSON backups when snapshot backup storage is enabled.

## Library API

GrafConflux can also be called from Python.

```python
from grafconflux import options_from_config_file, run

options = options_from_config_file(
    "config.yaml",
    wiki_url="https://confluence.example.com",
    confluence_page_id=12345,
    timestamps=["Release__&from=1719792000000&to=1719878400000"],
)
run(options)
```

One-call helper:

```python
from grafconflux import run_from_config_file

run_from_config_file(
    "config.yaml",
    wiki_url="https://confluence.example.com",
    confluence_page_id=12345,
    timestamps=["Release__&from=1719792000000&to=1719878400000"],
)
```

Public API exports include `GrafConfluxRunOptions`, `parse_timestamps`, `options_from_config_file`, `run`, and
`run_from_config_file`.
