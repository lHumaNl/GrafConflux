"""Compatibility shim for the legacy top-level GrafConFlux entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_local_src_on_path() -> None:
    src_path = Path(__file__).resolve().parent / "src"
    if src_path.is_dir():
        sys.path.insert(0, str(src_path))


_ensure_local_src_on_path()

from grafconflux.runner import logger, main, run_cli  # noqa: E402
from grafconflux.orchestration import (  # noqa: E402
    ConfluenceManager,
    GrafanaConfigDownloader,
    GrafanaConfigUploader,
    GrafanaManager,
    RunArgs,
    get_yaml_files,
    process_grafana_dashboard,
    run,
    transform_grafana_configs,
    upload_already_downloaded_graphs,
)
from grafconflux.args_parser import ArgsParser  # noqa: E402

__all__ = [
    "ArgsParser",
    "ConfluenceManager",
    "GrafanaConfigDownloader",
    "GrafanaConfigUploader",
    "GrafanaManager",
    "RunArgs",
    "get_yaml_files",
    "logger",
    "main",
    "process_grafana_dashboard",
    "run",
    "run_cli",
    "transform_grafana_configs",
    "upload_already_downloaded_graphs",
]


if __name__ == "__main__":
    main()
