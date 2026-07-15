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

## Shared Grafana credentials

You can define reusable Grafana connection blocks at top level and reference them from dashboards.

```yaml
default_grafana_credentials:
  grafana_url: https://grafana.example.com/grafana
  token: env:GRAFANA_DEFAULT_TOKEN
  render: false

grafana_credentials:
  prod:
    grafana_url: https://grafana.example.com/grafana
    token: env:GRAFANA_TOKEN
    render: false
    session_mode: shared

dashboards:
  overview:
    credentials: prod
    dash_title: Operations Overview

  details:
    credentials: prod
    dash_title: Detailed Metrics
    render: true
    session_mode: isolated
```

Notes:

- Named credentials are optional; inline per-dashboard Grafana settings still work.
- `default_grafana_credentials` is optional sugar for dashboards that do not declare `credentials` and do not set inline Grafana identity fields such as `grafana_url`, `auth_url`, `login`, `password`, `token`, or `domain`.
- `render` is supported in both `default_grafana_credentials` and named `grafana_credentials`, and is merged into dashboards unless that dashboard sets its own `render` value.
- Precedence is: dashboard `credentials` reference, then inline Grafana identity fields, then `default_grafana_credentials`, then built-in defaults.
- Shared sessions are reused only within one direct run or one child-page item.
- Batch `--time_files` items are isolated from each other.
- If a dashboard references named credentials in shared mode, do not override Grafana identity fields on that dashboard.

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

Confluence report layout can also be controlled with stable description ids:

```yaml
settings:
  description_rename:
    dashboard_links: Dashboard links
    backup_dashboard_links: Backup links
    panels: Charts
    test_times: Test times
  description_switch:
    dashboard_links: true
    backup_dashboard_links: false
    panels: true
    test_times: true
  time_zone: Europe/Moscow
  time_format: "%d/%m/%Y %H:%M:%S"
  timezone_label: true
  dashboard_links_location: leaf
```

- Supported description ids: `dashboard_links`, `backup_dashboard_links`, `panels`, `test_times`.
- `test_times` is mandatory and cannot be disabled.
- `time_zone` accepts an IANA name or a fixed offset such as `+03:00`; if omitted, the Confluence `Test times` section uses the host timezone and displays a concrete offset/name label.
- `time_format` is a Python `strftime` format for visible test times; default is `%d/%m/%Y %H:%M:%S`.
- `timezone_label: false` hides the visible timezone line while keeping time conversion active.
- `dashboard_links_location` supports `leaf` (default), `dashboard`, and `none`.
- Dashboard links render as links without a separate `Dashboard links` text label.
- When enabled, `panels` is used as an expand macro title/container, not as a standalone paragraph label; `description_switch.panels: false` hides only that container and never suppresses downloaded images.

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

The legacy Grafana fullscreen route uses the bare `fullscreen` query flag without `=`. If a
navigation or datasource request returns 401/403 or lands on the configured same-origin
Grafana `/login` page, GrafConflux re-authenticates the requests session, rebuilds the affected
Playwright context with fresh cookies/headers, and retries that route once. Concurrent workers
share one bounded authentication generation instead of performing duplicate logins.
Screenshot opening/response logs retain the exact constructed panel URL so it can be copied for
manual reproduction; credentials and authorization headers must not be placed in that URL.

Repeating panel and row titles normalize display-only `$__all` values to `All`.

## Dashboard config DSL

### Dashboard lookup

Choose exactly one primary dashboard selector:

- `dashboard_uid`: stable UID lookup
- `dash_title`: title lookup

Optional folder narrowing works with title lookup:

- `folder`: exact folder title
- `folder_uid`: exact folder UID

```yaml
dashboards:
  ops_by_uid:
    dashboard_uid: ops-main
    grafana_url: https://grafana.example/grafana

  ops_by_title:
    dash_title: Operations Overview
    folder_uid: prod
    grafana_url: https://grafana.example/grafana
```

### Dashboard variables and panel titles

Use `vars` to pass Grafana template variables to dashboard links, panel links, render requests, title substitution,
panel variants, and render-matrix rows.

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    vars:
      region: us
      iface: $__all
```

Configured `vars` override the dashboard's current variable values when GrafConflux renders panel display titles.

### Panel filtering and renaming

`panel_filtering` controls which panels or rows are kept before rendering.

- Default mode: `include_all_except_excluded`
- Strict mode: `include_only_selected`
- Selectors support `ids`, `titles`, and `title_regex`
- Row selectors support `ids`, `titles`, and `title_regex`
- `disable_graph_types` still applies before panel filtering

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    disable_graph_types: [stat]
    panel_filtering:
      mode: include_only_selected
      include_rows:
        titles: [Production]
      include_panels:
        ids: [17]
        titles:
          - Speed
          - Packet Drops:
              type: timeseries
              rename: Packet loss
      exclude_panels:
        title_regex: [".*temporary.*"]
```

Notes:

- `titles` can be plain strings or typed selectors like `{Packet Drops: timeseries}`.
- Inline renames are supported only in `include_panels.titles`.
- Top-level `rename_panels` applies even without filtering and supports selectors by `id`, `title`, and optional `type`.

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    rename_panels:
      - id: 20
        rename: "Traffic $iface"
      - title: Packet Drops
        type: timeseries
        rename: Packet loss
```

### Repeating panels and collapsed rows

- `download_collapsed_rows: true` expands collapsed Grafana rows before extraction.
- Legacy alias `download_collapse_panels` is still accepted.
- `download_hidden_panels` is not supported.
- Grafana repeating panels are detected from `panel.repeat` or inherited `row.repeat`; a `$` in a title alone does not make a panel repeating.
- `enable_repeating_panels: true` remains accepted for compatibility.
- `repeating_panels` explicitly selects a repeat source and optionally filters its concrete values.

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    download_collapsed_rows: true
    enable_repeating_panels: true
    panel_filtering:
      mode: include_only_selected
      include_panels:
        titles:
          - Uptime
          - "$jvm_memory_pool_heap"
          - "$jvm_memory_pool_nonheap"

    repeating_panels:
      # Bare source title: all saved Grafana option values except All.
      - "$jvm_memory_pool_heap"

      # Exact one-value shorthand.
      - "$jvm_memory_pool_nonheap": Metaspace

      # Include/regex are a union; exclude is the final veto.
      - "$other_pool":
          include: [Pool A]
          exclude: [Pool B]
          regex:
            - '^G1 .*'
            - '^Other .*'
```

When present, Confluence groups repeating artifacts under the source panel title and labels each repeat like
`CPU by host [host=prod-1]`.

`panel_filtering` runs against source dashboard panels before repeat materialization. Select the raw source title
such as `$jvm_memory_pool_heap`, not runtime clone titles such as `G1 Eden Space`. Repeat status is taken from
Grafana dashboard metadata.

Explicit rules support both shorthand and canonical mappings:

```yaml
repeating_panels:
  # repeat_values is optional; omitted means mode: all.
  - title: "$jvm_memory_pool_heap"

  - panel_id: 17
    repeat_values:
      mode: filter
      include: [G1 Eden Space]
      exclude: [G1 Survivor Space]
      regex:
        - '^G1 .*'

  - panel_id: 21
    repeat_values:
      mode: manual
      values: [exact-value]
```

For `mode: filter`, exact `include` values and regex matches are combined with OR, then `exclude` removes
matches. String and list forms are accepted for `include`, `exclude`, and `regex`; regex lists use OR and
`re.search`. Grafana option order is preserved, duplicate values are removed, and All sentinels are excluded.
Selectors currently match Grafana `option.value`, not `option.text` or a runtime clone title.

`mode: auto` preserves the previous explicit-rule fallback to configured `vars`, dashboard current/default,
and supported datasource-backed discovery. Automatic repeating panels without an explicit rule continue to use
that auto-resolution behavior.

### No-data preflight

By default, GrafConflux still renders panels even if they may be empty.

- `collect_no_data_panels: true` keeps normal rendering behavior
- `collect_no_data_panels: false` enables conservative datasource preflight for supported panels and skips confirmed
  empty results

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    collect_no_data_panels: false
    no_data_preflight:
      timeout: 10
      store_skip_metadata: true
```

Current preflight behavior is intentionally conservative:

- mode is fixed to `conservative`
- `on_error` remains `render_anyway`
- `min_non_empty_frames` remains `1`

### Snapshots and backup links

Set `snapshot: true` to publish Grafana snapshot links and optionally upload downloaded dashboard JSON backups.

```yaml
dashboards:
  operations:
    dash_title: Operations Overview
    grafana_url: https://grafana.example/grafana
    snapshot: true
    snapshot_store_dashboard_json: true
    backup_dashboard_links:
      - https://backup-grafana.example/d/ops-main?orgId=1
```

Notes:

- Snapshot creation always uses the UI flow now.
- `snapshot_mode`, `snapshot_fallback_to_ui`, and `snapshot_expires` are deprecated compatibility keys.
- `backup_dashboard_links` are rendered in Confluence with the active run time range substituted into each URL.

## Panel variants and composites

### Panel variants

Use `panel_variants` to render selected panels multiple times with explicit values or values discovered from Grafana
variables.

```yaml
dashboards:
  operations:
    grafana_url: https://grafana.example.com
    dash_title: Operations Overview
    panel_variants:
      - name: by_service
        selectors:
          panel_id: 17
        variables:
          service:
            values: [api, worker]
        label_template: "Service: {service}"

      - name: top_hosts
        selectors:
          title_regex: "^CPU .*"
          allow_multiple: true
        variables:
          host:
            match_values:
              regex: "^(app-1|app-2)$"
```

- Selectors support `panel_id`, exact `title`, or `title_regex`, plus optional `type`.
- Variant filenames use stable hashes instead of raw variable values; full values stay in metadata.
- Upload-only replay preserves variant order from metadata and `manifest.yaml` when present.

### Render matrix

Use dashboard-level `render_matrix` to render every selected panel for multiple Grafana variable combinations.

```yaml
dashboards:
  operations:
    grafana_url: https://grafana.example.com/grafana
    dash_title: Operations Overview
    vars:
      region:
        value: us
        display_name: Region
      metrics_source:
        lookup: Metrics source
        is_datasource: true
        name: ICAPMock
    render_matrix:
      options:
        row_grouping: [environment]
        label_template: "{Environment} / {Service}"
        layout: matrix_grouped_panels
      variables:
        environment:
          display_name: Environment
          hide: false
          lookup: Environment selector
          values: [prod, stage]
        service:
          display_name: Service
          hide: false
          value_aliases: {api: Public API}
          depends_on: environment
          values_by:
            prod: [api, worker]
            stage: [worker]
```

- `values`: explicit list.
- `values_by`: map dependent values by previously resolved matrix variables. It requires `depends_on`; with multiple dependencies, keys are joined as `value1|value2`.
- `values_from`: pull options from the Grafana variable named by `grafana_variable` or by the matrix key. Use an object with optional `regex`, `max_values`, `filters_by_parent`, and `grouping`. A dependent variable with `depends_on` and no explicit value source is treated as `values_from: {}`.
- `display_name`: user-facing variable name. Matrix `alias` remains supported as a legacy synonym; configuring both with different values is an error.
- `lookup`: explicit, dashboard-scoped lookup identifier for a Grafana variable. It matches exactly and case-sensitively against the variable's technical `name`, `label`, or `description`. Exactly one variable must match; zero or multiple matches are configuration errors. `lookup` and `grafana_variable` are mutually exclusive.
- `value_aliases`: exact raw-to-display mappings. Unknown values fall back to their raw string, and list values are mapped element by element.
- `hide`: presentation-only exclusion from automatically generated matrix/Confluence labels, headings, and suffixes. It never removes a variable from Grafana requests, discovery, dependencies, metadata, filenames, or technical identity.
- `grafana_variable`: actual Grafana URL variable name. Default is the matrix key. Use this for an explicit technical override, or use `lookup` when configuration should not contain the raw URI variable name.
- `label_template`: optional row label built from variable keys or display names, for example `{environment} / {Service}`. Templates cannot reference hidden variables.
- `combination_mode`: `product` (default) or `zip`.
- `options.layout`: optional Confluence matrix layout. The default is `matrix_grouped_panels`: dashboard -> every prefix dimension except the last -> unique final-context dashboard links -> `Panels` -> panel -> final-dimension panel link/image leaves. Explicit `matrix_values_first` (A) renders every dimension as a sequential value branch; each complete context then contains its matching dashboard links followed by `Panels` and the panel link/image content. Explicit `panel_first` and `dashboard_first` retain their existing structures.
- `max_rows`: optional hard limit for resolved matrix rows. Default is 500.
- Static dashboard `vars` are kept and merged with matrix variables in panel and dashboard links. Scalar/list shorthand remains valid. Object form accepts `value`, `lookup`, `hide`, `display_name`, and `value_aliases`, plus optional `is_datasource: true`. Datasource objects also accept `name`.
- A static `lookup` resolves the configuration entry to the matched variable's raw technical name. `value` remains required for generic variables. For `lookup` plus `is_datasource: true`, `value` may be omitted; GrafConflux then uses the matched datasource variable's saved raw current value (normally its datasource UID). If no usable current value exists, configuration fails safely.
- For `is_datasource: true`, `name` explicitly resolves an exact, case-sensitive Grafana datasource name to its UID after authentication. `name` and `value` are mutually exclusive; no UID-or-name inference is performed. A missing or ambiguous datasource is a safe configuration error.
- `is_datasource: true` remains an explicit technical datasource priority hint and restricts lookup matching to datasource variables. It is independent of `hide`; hiding a variable does not change datasource resolution.
- `display_name` never participates in lookup. Lookup identifiers, display names, and configuration keys are never sent to Grafana after lookup resolution; URLs and discovery use only matched technical names and raw values.
- Presentation names and aliases never alter Grafana URLs. Matrix filenames and hashes use raw technical variable identity, not presentation fields or raw values in filenames.

`values_from` example:

```yaml
dashboards:
  operations:
    render_matrix:
      variables:
        service:
          grafana_variable: service
          values_from:
            regex: "^(api|worker|db)$"
            max_values: 2
```

For dynamic matrix filtering, `values_from.regex`, each `filters_by_parent[].regex`, and each
named `grouping.rules[].regex` accept either one regex string or a non-empty list of regex
strings. A list uses OR semantics: a value passes that field when any pattern matches via
`re.search`. Separate matching parent filters still compose with AND. Multiple patterns from
one named grouping rule produce only one membership for that rule. A matching
`override_global` parent filter still disables the complete global regex set for that parent
context without disabling other matching parent filters.

```yaml
pod:
  depends_on: namespace
  values_from:
    regex:
      - '^calculator-covenant-api-.+'
      - '^matrix-calculator-rate-.+'
      - '^matrix-offer-generator-.*'
    filters_by_parent:
      - when: {namespace: production}
        regex:
          - '^calculator-covenant-api-.+'
          - '^matrix-calculator-rate-.+'
    grouping:
      rules:
        - name: calculators
          label: Calculator services
          regex:
            - '^calculator-covenant-api-.+'
            - '^matrix-calculator-rate-.+'
```

Regex lists are intentionally not supported by legacy variable-level `regex` or by
`grouping.capture.regex`; capture grouping continues to require one regex string.

Behavior notes:

- Default `combination_mode` is `product`; `zip` is also supported and requires equal-length value lists.
- Explicit `hide` always wins. When omitted, a variable is visible only when its effective raw value set has exactly one value and `value_aliases` is empty. Multiple values or any non-empty alias mapping default to hidden.
- For `values_from` and context-dependent `values_by`, the omitted-`hide` default is resolved after each effective value set is discovered. This can differ by timestamp or dependency branch and is deterministic for the resolved ordered values.
- If every matrix context variable is hidden, generated output uses deterministic neutral labels (`Variant 1`, `Variant 2`, and so on).
- In default `matrix_grouped_panels` (B), one dimension produces dashboard -> unique dashboard links -> `Panels` -> panel -> value leaves. Two dimensions produce dashboard -> first-dimension groups -> unique final-dimension dashboard links -> `Panels` -> panel -> second-dimension leaves. Additional dimensions add prefix grouping layers in declared/topological order. Prefix groups use raw context identity and never mix, even when display aliases are equal.
- B renders each dashboard link once per final raw context and timestamp before `Panels`; panel leaves contain only the final variable display name/value, panel link, and image. The panel title appears only on its parent panel expand. In explicit `matrix_values_first` (A), all dimensions are value branches and each final branch owns its context-matched dashboard links and `Panels` section.
- B treats omitted `hide` differently from the legacy layouts: automatically hidden values remain visible as structural prefix/leaf labels. Explicit `hide: true` remains private and uses deterministic neutral `Group N`/`Variant N` labels. A, `panel_first`, and `dashboard_first` retain their existing hide behavior. Raw values still determine grouping, URL, dependency, and artifact identity in every layout.
- Grafana `All` options (`$__all`, `__all`, `all`) are excluded from `values_from` resolution.
- Use `render_matrix.options` for renderer options and `render_matrix.variables` for matrix variables.
- Top-level `render_matrix.layout` is not supported; put layout under `render_matrix.options.layout`. Existing flat variable keys without top-level layout remain accepted for older configs.
- `row_grouping` (alias: `group_by`) groups matrix artifacts in Confluence expand sections using the grouped variable aliases, for example `Environment: prod`.
- In child-page mode, grouped matrix sections are rendered on the child page; the parent page only gets include/expand content when its `%%%graphs%%%` marker exists.
- Matrix diagnostics use two concise records. `matrix_discovery` shows the variable, time tag (or concrete time range), parent context such as namespace, discovered count, and raw values. `matrix_filtered` uses the same shape for values remaining after filtering/dedupe/`max_values`. Regex bodies, request payloads, headers, cookies, and configured credentials are not emitted by these matrix diagnostics.
- Upload-only replay preserves raw context plus its display-name, display-value, hidden-state snapshot, grouping, and order from saved metadata and `manifest.yaml` when present. Metadata without a layout migrates to B, including old upload-only metadata; newly written metadata stores the resolved `matrix_grouped_panels` layout. Merging upload folders rejects different resolved layouts instead of silently choosing one.

### Composite images

Use `composites` to stitch already rendered PNGs into one attachment.

```yaml
dashboards:
  operations:
    composites:
      - name: service-overview
        title: Service overview
        layout: grid
        columns: 2
        include_sources: false
        sources:
          - panel_id: 17
          - title: Memory
```

- Supported layouts: `vertical`, `horizontal`, `grid`, `dashboard_grid`.
  `dashboard_grid` preserves selected panels' relative Grafana grid proportions while compacting empty grid bands left by unselected panels.
- `dashboard_grid` also supports optional `three_panel_policy` for exactly three rendered sources: `preserve` (default), `top_wide` (one top row + two half-width panels below), and `bottom_half` (keep the first panel at its natural width with two half-width panels below). All policies preserve source aspect ratio with letterboxing.
- `include_sources: false` hides source artifacts in Confluence while still using them for the composite.
- Composite images require Pillow, which is already included in `requirements.txt`.

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

When present, upload-only mode uses `manifest.yaml` to preserve dashboard and artifact order. Without a manifest, it
falls back to legacy metadata ordering.

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

Each run folder also contains `manifest.yaml` with stable dashboard and artifact ordering metadata used by upload-only
replay.

At the end of a publishing run, GrafConflux logs final Confluence page links:

- direct mode logs the target page,
- direct batch mode logs each target page in input order,
- child-page mode logs child pages and logs the parent page only when the parent `%%%graphs%%%` marker was replaced.

When Confluence response metadata contains page links, those links are used. Otherwise GrafConflux falls back to
`<wiki_url>/pages/viewpage.action?pageId=<page_id>`, preserving any subpath in `wiki_url`.

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
