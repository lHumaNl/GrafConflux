import ast
import os
import tempfile
import unittest
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from grafconflux.args_parser import ArgsParser, GrafanaTimeDownloader
from grafconflux.config import parse_timestamps


MILLISECONDS_PER_SECOND = 1000


def _iso_timestamp_milliseconds(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp()) * MILLISECONDS_PER_SECOND


class TestGrafanaTimeParsing(unittest.TestCase):
    def test_parse_timestamps_defaults_to_utc(self):
        timestamp = parse_timestamps(["default__&from=1700000000&to=1700003600"])[0]

        self.assertEqual(timestamp.start_time_timestamp, 1700000000000)
        self.assertEqual(timestamp.end_time_timestamp, 1700003600000)
        self.assertEqual(timestamp.start_time_human, "2023/11/14 22:13:20")
        self.assertEqual(timestamp.end_time_human, "2023/11/14 23:13:20")

    def test_epoch_milliseconds_are_preserved(self):
        timestamp = GrafanaTimeDownloader(
            "milliseconds__&from=1700000000123&to=1700003600456",
            0,
            "UTC",
        )

        self.assertEqual(timestamp.start_time_timestamp, 1700000000123)
        self.assertEqual(timestamp.end_time_timestamp, 1700003600456)
        self.assertEqual(timestamp.start_time_human, "2023/11/14 22:13:20")

    def test_iso_z_ranges_preserve_existing_timestamp_precision(self):
        timestamp = GrafanaTimeDownloader(
            "iso__&from=2025-11-16T14:24:49.073Z&to=2025-11-16T14:30:00.000Z",
            0,
            "UTC",
        )

        self.assertEqual(
            timestamp.start_time_timestamp,
            _iso_timestamp_milliseconds("2025-11-16T14:24:49.073+00:00"),
        )
        self.assertEqual(
            timestamp.end_time_timestamp,
            _iso_timestamp_milliseconds("2025-11-16T14:30:00.000+00:00"),
        )
        self.assertEqual(timestamp.start_time_human, "2025/11/16 14:24:49")

    def test_non_utc_timezone_converts_human_times(self):
        timestamp = GrafanaTimeDownloader(
            "moscow__&from=1700000000&to=1700003600",
            0,
            "Europe/Moscow",
        )

        self.assertEqual(timestamp.start_time_human, "2023/11/15 01:13:20")
        self.assertEqual(timestamp.end_time_human, "2023/11/15 02:13:20")

    def test_args_parser_applies_cli_timezone(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self._create_config(temp_dir)
            args = self._parse_args(config_path)

        self.assertEqual(args.timestamps[0].start_time_human, "2023/11/15 01:13:20")
        self.assertEqual(args.tz, "Europe/Moscow")

    def test_invalid_timezone_raises_clear_error(self):
        with self.assertRaisesRegex(ValueError, "Invalid or unavailable timezone"):
            GrafanaTimeDownloader(
                "invalid__&from=1700000000&to=1700003600",
                0,
                "Invalid/Zone",
            )

    @staticmethod
    def _create_config(temp_dir: str) -> str:
        config_path = os.path.join(temp_dir, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(
                "dashboards:\n"
                "  demo:\n"
                "    dash_title: Demo\n"
                "    grafana_url: https://grafana.example\n"
            )
        return config_path

    def _parse_args(self, config_path: str) -> ArgsParser:
        env = {"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"}
        argv = self._time_args(config_path)
        with patch.dict(os.environ, env, clear=False):
            return ArgsParser(argv)

    @staticmethod
    def _time_args(config_path: str) -> list[str]:
        return [
            "--config",
            config_path,
            "--wiki_url",
            "https://cli.example",
            "--confluence_page_id",
            "1",
            "--tz",
            "Europe/Moscow",
            "--timestamps",
            "tag__&from=1700000000&to=1700003600",
        ]


class TestRemovedTimezoneDependency(unittest.TestCase):
    def test_source_files_do_not_import_removed_modules(self):
        forbidden_modules = {"py" + "tz", "pkg" + "_" + "resources"}
        violations = self._source_import_violations(forbidden_modules)

        self.assertEqual([], violations)

    def test_dependency_metadata_uses_zoneinfo_data_package(self):
        project_root = Path(__file__).resolve().parents[1]
        dependency_text = self._dependency_text(project_root)

        self.assertNotIn("py" + "tz", dependency_text.lower())
        self.assertNotIn("pkg" + "_" + "resources", dependency_text.lower())
        self.assertIn("tzdata", dependency_text)

    def _source_import_violations(self, forbidden_modules: set[str]) -> list[str]:
        project_root = Path(__file__).resolve().parents[1]
        violations = []
        for source_file in self._source_files(project_root):
            imported_modules = self._imported_modules(source_file)
            violations.extend(self._violations(source_file, imported_modules, forbidden_modules))
        return violations

    @staticmethod
    def _source_files(project_root: Path) -> list[Path]:
        return [*project_root.joinpath("src").rglob("*.py"), project_root / "main.py"]

    @staticmethod
    def _imported_modules(source_file: Path) -> set[str]:
        source = source_file.read_text(encoding="utf-8-sig")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            tree = ast.parse(source, filename=str(source_file))
        return {
            module_name
            for node in ast.walk(tree)
            for module_name in TestRemovedTimezoneDependency._node_imports(node)
        }

    @staticmethod
    def _node_imports(node: ast.AST) -> list[str]:
        if isinstance(node, ast.Import):
            return [alias.name for alias in node.names]
        if isinstance(node, ast.ImportFrom) and node.module:
            return [node.module]
        return []

    @staticmethod
    def _violations(source_file: Path, imports: set[str], forbidden_modules: set[str]) -> list[str]:
        return [
            f"{source_file}: forbidden import {module_name}"
            for module_name in imports & forbidden_modules
        ]

    @staticmethod
    def _dependency_text(project_root: Path) -> str:
        return "\n".join(
            project_root.joinpath(file_name).read_text(encoding="utf-8")
            for file_name in ("requirements.txt", "pyproject.toml")
        )
