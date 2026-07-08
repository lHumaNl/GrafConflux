"""Public GrafConflux package API."""

from importlib import import_module

from grafconflux.api import options_from_config_file, parse_timestamps, run, run_from_config_file
from grafconflux.options import GrafConfluxRunOptions


def __getattr__(name: str):
    if name == "_grafana":
        return import_module("grafconflux._grafana")
    raise AttributeError(f"module 'grafconflux' has no attribute {name!r}")

__all__ = [
    "GrafConfluxRunOptions",
    "options_from_config_file",
    "parse_timestamps",
    "run",
    "run_from_config_file",
]
