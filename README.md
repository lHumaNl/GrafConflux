# GrafConflux

Download Grafana panels for one or more time ranges and publish them to Confluence.

## What it does

- Downloads panels from one or more Grafana dashboards.
- Renders images with either the Grafana render API or Playwright screenshots.
- Uploads PNG attachments to Confluence and updates page content.
- Supports direct page updates and child-page publishing under a parent page.
- Can run from CLI, YAML time input files, or Python.

## Requirements

- Python 3.10-3.12
- A browser binary for Playwright screenshot mode (`render: false`)

Timezone handling uses Python `zoneinfo`. On systems without bundled timezone data, install dependencies from
`requirements.txt` so `tzdata` is available.

## Install

### Windows quick start

```powershell
python -m venv .venv
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt
& ".venv\Scripts\python.exe" -m playwright install chromium
```

Notes:

- Selenium is no longer used.
- Install a Playwright browser only if you use `render: false`.

## Minimal config

The config file must contain a non-empty top-level `dashboards` mapping. Top-level `settings` is optional.

```yaml
settings:
  wiki_url: https://confluence.example.com
  confluence_verify_ssl: true
  confluence_login: env:CONFLUENCE_LOGIN
  confluence_token: env:CONFLUENCE_TOKEN

dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example.com
    token: env:GRAFANA_TOKEN
    render: true
```

Dashboards-only config is also valid when Confluence settings come from CLI or environment variables:

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example.com
    token: env:GRAFANA_TOKEN
```

`grafana_url` is the public Grafana base URL. Include any reverse-proxy app path directly in this value, for example
`https://grafana.example.com/grafana` or `https://grafana.example.com/monitoring`. Use optional `auth_url` only for an
external authentication/bootstrap endpoint; GrafConflux still builds Grafana API, render, and browser URLs from
`grafana_url`.

## Confluence credentials and SSL

Supported sources, in practical order:

1. CLI options
2. YAML `settings`
3. Environment variables

Supported credential fields:

- `confluence_login`
- `confluence_password`
- `confluence_token`

Environment fallback names:

- `CONFLUENCE_LOGIN`
- `CONFLUENCE_PASSWORD`
- `CONFLUENCE_TOKEN`

YAML settings may reference environment variables with `env:VARIABLE_NAME`:

```yaml
settings:
  confluence_login: env:CONFLUENCE_LOGIN
  confluence_token: env:CONFLUENCE_TOKEN
```

Confluence SSL verification:

- CLI: `--confluence_verify_ssl true|false`
- YAML: `settings.confluence_verify_ssl`

## Grafana rendering modes

### `render: true`

Uses the Grafana render API.

### `render: false`

Uses Playwright screenshots. This requires a real browser binary.

Supported browser settings:

- `playwright_browser`: `chromium`, `firefox`, or `webkit`
- `playwright_browser_channel`: installed channel such as `chrome` or `msedge`
- `playwright_browser_executable_path`: custom browser executable path

You can set these globally in `settings`, per dashboard, or with CLI overrides.

```yaml
settings:
  playwright_browser: chromium
  playwright_browser_channel: chrome

dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example.com
    token: env:GRAFANA_TOKEN
    render: false
    playwright_browser_executable_path: C:/Browsers/chrome.exe
```

Screenshot mode waits for the panel to appear, related Grafana requests to go idle,
loading indicators to clear, and a short settle period before capture. Optional
per-dashboard tuning:

```yaml
dashboards:
  operations:
    render: false
    screenshot_readiness:
      network_idle_ms: 750
      no_network_grace_ms: 1000
      min_settle_ms: 200
      poll_interval_ms: 100
      strict_datasource_fragments: false
```

Repeating panel and row titles normalize display-only `$__all` values to `All`.

CLI overrides:

- `--playwright_browser`
- `--playwright_browser_channel`
- `--playwright_browser_executable_path`

## Run modes

### 1. Direct mode: update one Confluence page

Use `--confluence_page_id`.

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_page_id 12345 `
  --timestamps "Release__&from=1719792000000&to=1719878400000"
```

If the target page body contains `%%%graphs%%%`, GrafConflux replaces that marker. Otherwise it replaces the whole page
body.

### 2. Child-page mode: publish under a parent page

Use `--confluence_parent_page_id` instead of `--confluence_page_id`.

GrafConflux:

- creates or reuses a child page,
- uploads attachments to that child page,
- updates the child page body,
- optionally updates the parent page if the parent contains `%%%graphs%%%`.

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_parent_page_id 12345 `
  --confluence_child_title "Release graphs" `
  --timestamps "Release__&from=1719792000000&to=1719878400000"
```

Child-title options:

- `--confluence_child_title` - explicit title
- `--confluence_child_title_prefix` - prefix for generated titles (default: `GrafConflux: `)
- `--confluence_child_title_from_test_id` - use `test_id` as the child title

Important behavior:

- Direct mode and child-page mode are mutually exclusive.
- If the parent page does not contain `%%%graphs%%%`, the parent page is left unchanged.
- `--test_upload_folders` is not supported in child-page mode.

### 3. Download only

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_page_id 12345 `
  --timestamps "Release__&from=1719792000000&to=1719878400000" `
  --only_graphs
```

### 4. Upload previously downloaded folders

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_page_id 12345 `
  --test_upload_folders "graphs\run_a" "graphs\run_b"
```

## Time input files (`--time_files`)

Aliases:

- `--time_files`
- `--times_files`
- `--timestamps_files`

Use a YAML file when you want page targeting and time ranges in one place.

### Direct-mode time file

```yaml
page_id: 12345
test_id: Release comparison
times:
  - v1.0: "&from=1719792000000&to=1719878400000"
  - "&from=1719878400000&to=1719964800000"
```

### Child-page time file

```yaml
parent_page_id: 12345
title: Release graphs
test_id: Release comparison
times:
  - v1.0: "&from=1719792000000&to=1719878400000"
```

Schema summary:

- `page_id` or `parent_page_id`
- optional `title`
- optional `test_id`
- required non-empty `times`

`times` entries may be:

- a scalar string: `"&from=...&to=..."`
- a tagged scalar string: `"Release__&from=...&to=..."`
- a single-pair mapping: `- Release: "&from=...&to=..."`

### Single-file behavior

One `--time_files` file may be combined with CLI overrides. Useful overrides include:

- `--confluence_page_id`
- `--confluence_parent_page_id`
- `--confluence_child_title`
- `--test_id`
- `--timestamps`

Example:

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --time_files "times\release.yaml" `
  --test_id "release-from-cli"
```

### Multi-file batch behavior

Passing multiple time files runs them sequentially.

Rules:

- Do not mix direct-mode and child-page files in the same batch.
- In multi-file mode, do not combine `--time_files` with `--confluence_page_id`, `--confluence_child_title`,
  `--test_id`, or `--timestamps`.
- A common CLI `--confluence_parent_page_id` is allowed for multi-file child-page mode.

Example:

```powershell
& ".venv\Scripts\python.exe" "main.py" `
  --wiki_url "https://confluence.example.com" `
  --confluence_parent_page_id 12345 `
  --time_files "times\release-a.yaml" "times\release-b.yaml"
```

## Common CLI options

| Option                                         | Meaning                                                                                         |
|------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `--wiki_url`                                   | Confluence base URL. Optional when `settings.wiki_url` is present.                              |
| `--confluence_page_id`, `-i`                   | Direct-mode target page.                                                                        |
| `--confluence_parent_page_id`                  | Parent page for child-page mode.                                                                |
| `--confluence_child_title`                     | Explicit child page title.                                                                      |
| `--confluence_child_title_prefix`              | Prefix for generated child titles.                                                              |
| `--confluence_child_title_from_test_id`        | Use `test_id` as the child title.                                                               |
| `--timestamps`, `-t`                           | One or more time ranges. Accepts epoch seconds, epoch milliseconds, or Grafana ISO-8601 ranges. |
| `--time_files`                                 | One or more YAML time input files.                                                              |
| `--test_upload_folders`                        | Upload previously downloaded output folders.                                                    |
| `--only_graphs`                                | Download only; skip Confluence updates.                                                         |
| `--confluence_login` / `--confluence_password` | Confluence username/password auth.                                                              |
| `--confluence_token`                           | Confluence token auth.                                                                          |
| `--confluence_verify_ssl true/false`           | Enable or disable Confluence SSL verification.                                                  |
| `--playwright_browser`                         | Browser type for screenshot mode.                                                               |
| `--playwright_browser_channel`                 | Installed browser channel for screenshot mode.                                                  |
| `--playwright_browser_executable_path`         | Custom browser executable for screenshot mode.                                                  |
| `--tz`                                         | Time zone used when parsing timestamps. Default: `UTC`.                                         |

## Testing

Run the full pytest suite:

```powershell
& ".venv\Scripts\python.exe" -m pytest
```

Run a smaller targeted check:

```powershell
& ".venv\Scripts\python.exe" -m pytest "tests\test_time_input_files.py" "tests\test_args_parser.py"
```

CLI help:

```powershell
& ".venv\Scripts\python.exe" "main.py" --help
```

## Output

Fresh downloads are written under:

```text
graphs\<test_id>__<timestamp>\
```

Artifacts may include PNGs, metadata YAML, and optional snapshot JSON backups.

## Python API

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
