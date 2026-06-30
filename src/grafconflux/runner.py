"""Runner boundary for GrafConflux CLI execution."""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence

from grafconflux.args_parser import ArgsParser
from grafconflux.orchestration import run

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and run GrafConflux orchestration."""
    try:
        args = ArgsParser(list(argv) if argv is not None else None)
        run(args)
    except Exception as e:
        logger.error(f'An error occurred: {e}', exc_info=True)
        sys.exit(1)


def run_cli(argv: Sequence[str] | None = None) -> None:
    """Console-script compatible wrapper for CLI execution."""
    main(argv)
