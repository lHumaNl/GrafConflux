"""Public library API for GrafConflux."""

from __future__ import annotations

from grafconflux.config import options_from_config_file, parse_timestamps, run_from_config_file
from grafconflux.orchestration import RunResult
from grafconflux.options import GrafConfluxRunOptions


def run(options: GrafConfluxRunOptions) -> RunResult:
    """Run GrafConflux with library options and propagate failures."""
    from grafconflux.orchestration import run as orchestration_run

    return orchestration_run(options)
