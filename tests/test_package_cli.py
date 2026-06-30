import importlib
import os
import sys
import unittest
from unittest.mock import patch


class TestPackageCli(unittest.TestCase):
    def test_cli_entrypoint_delegates_to_runner(self) -> None:
        src_path = os.path.join(os.path.dirname(__file__), "..", "src")
        original_path = list(sys.path)
        sys.path.insert(0, os.path.abspath(src_path))

        try:
            cli = importlib.import_module("grafconflux.cli")
            with patch("grafconflux.cli.runner_main") as runner_main:
                cli.main(["--help"])
        finally:
            sys.path[:] = original_path
            sys.modules.pop("grafconflux.runner", None)
            sys.modules.pop("grafconflux.cli", None)
            sys.modules.pop("grafconflux", None)

        runner_main.assert_called_once_with(["--help"])

    def test_runner_constructs_args_and_calls_orchestration(self) -> None:
        src_path = os.path.join(os.path.dirname(__file__), "..", "src")
        original_path = list(sys.path)
        sys.path.insert(0, os.path.abspath(src_path))
        args = object()

        try:
            runner = importlib.import_module("grafconflux.runner")
            with patch("grafconflux.runner.ArgsParser", return_value=args) as args_parser:
                with patch("grafconflux.runner.run") as run:
                    runner.main(["--help"])
        finally:
            sys.path[:] = original_path
            sys.modules.pop("grafconflux.runner", None)
            sys.modules.pop("grafconflux", None)

        args_parser.assert_called_once_with(["--help"])
        run.assert_called_once_with(args)

    def test_run_cli_delegates_to_runner_main(self) -> None:
        src_path = os.path.join(os.path.dirname(__file__), "..", "src")
        original_path = list(sys.path)
        sys.path.insert(0, os.path.abspath(src_path))

        try:
            runner = importlib.import_module("grafconflux.runner")
            with patch("grafconflux.runner.main") as runner_main:
                runner.run_cli(["--help"])
        finally:
            sys.path[:] = original_path
            sys.modules.pop("grafconflux.runner", None)
            sys.modules.pop("grafconflux", None)

        runner_main.assert_called_once_with(["--help"])
