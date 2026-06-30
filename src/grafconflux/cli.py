"""Console script entrypoint for GrafConflux."""

from __future__ import annotations

from collections.abc import Sequence

from grafconflux.runner import main as runner_main


def main(argv: Sequence[str] | None = None) -> None:
    """Delegate CLI execution through the runner boundary."""
    runner_main(argv)
