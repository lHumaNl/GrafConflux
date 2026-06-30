import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from grafconflux.args_parser import ArgsParser


class TestArgsParser(unittest.TestCase):
    def create_config(self, content: str = "settings: {}\n"):
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(content)
        self.addCleanup(temp_dir.cleanup)
        return config_path

    def parse_args(self, argv, env=None):
        env = env or {}
        with patch.object(sys, "argv", argv), patch.dict(os.environ, env, clear=False):
            return ArgsParser()

    def test_requires_wiki_url(self):
        config_path = self.create_config()

        with patch.object(sys, "argv", ["prog", "--config", config_path, "--confluence_page_id", "1"]):
            with self.assertRaises(SystemExit):
                ArgsParser()

    def test_requires_wiki_url_even_when_yaml_settings_provide_it(self):
        config_path = self.create_config("settings:\n  wiki_url: https://yaml.example\n")

        with patch.object(sys, "argv", ["prog", "--config", config_path, "--confluence_page_id", "1"]):
            with self.assertRaises(SystemExit):
                ArgsParser()

    def test_requires_confluence_page_id(self):
        config_path = self.create_config()

        with patch.object(sys, "argv", ["prog", "--config", config_path, "--wiki_url", "https://cli.example"]):
            with self.assertRaises(SystemExit):
                ArgsParser()

    def test_validation_fails_without_timestamps_or_upload_folders(self):
        config_path = self.create_config()

        with self.assertRaisesRegex(ValueError, "At least one timestamp must be provided"):
            self.parse_args(
                [
                    "prog",
                    "--config",
                    config_path,
                    "--wiki_url",
                    "https://cli.example",
                    "--confluence_page_id",
                    "1",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

    def test_accepts_confluence_credentials_from_environment(self):
        config_path = self.create_config()

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "env-user", "CONFLUENCE_PASSWORD": "env-pass"},
        )

        self.assertEqual(args.confluence_login, "env-user")
        self.assertEqual(args.confluence_password, "env-pass")
        self.assertEqual(args.confluence_upload_threads, 1)
        self.assertIsNone(args.confluence_upload_rate_per_second)
        self.assertEqual(args.confluence_retry_backoff_multiplier, 1.0)
        self.assertIsNone(args.confluence_retry_max_delay)
        self.assertEqual(args.confluence_retry_jitter, 0)

    def test_yaml_settings_confluence_upload_threads_overwrites_default(self):
        config_path = self.create_config("settings:\n  confluence_upload_threads: 3\n")

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.confluence_upload_threads, 3)

    def test_yaml_settings_apply_new_confluence_retry_and_rate_options(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_upload_rate_per_second: 2.5\n"
            "  confluence_retry_backoff_multiplier: 1.5\n"
            "  confluence_retry_max_delay: 8\n"
            "  confluence_retry_jitter: 0.25\n"
        )

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.confluence_upload_rate_per_second, 2.5)
        self.assertEqual(args.confluence_retry_backoff_multiplier, 1.5)
        self.assertEqual(args.confluence_retry_max_delay, 8)
        self.assertEqual(args.confluence_retry_jitter, 0.25)

    def test_yaml_settings_apply_all_cli_defaults(self):
        config_path = self.create_config(
            "settings:\n"
            "  graph_width: 1800\n"
            "  threads: 8\n"
            "  confluence_upload_threads: 3\n"
            "  confluence_upload_delay: 1.5\n"
            "  confluence_retry: false\n"
            "  confluence_retry_count: 9\n"
            "  confluence_retry_delay: 2.5\n"
            "  confluence_upload_rate_per_second: 3.5\n"
            "  confluence_retry_backoff_multiplier: 2\n"
            "  confluence_retry_max_delay: 7\n"
            "  confluence_retry_jitter: 0.4\n"
            "  confluence_continue_on_error: true\n"
            "  confluence_ignore_verify_ssl: true\n"
        )

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.graph_width, 1800)
        self.assertEqual(args.threads, 8)
        self.assertEqual(args.confluence_upload_threads, 3)
        self.assertEqual(args.confluence_upload_delay, 1.5)
        self.assertFalse(args.confluence_retry)
        self.assertEqual(args.confluence_retry_count, 9)
        self.assertEqual(args.confluence_retry_delay, 2.5)
        self.assertEqual(args.confluence_upload_rate_per_second, 3.5)
        self.assertEqual(args.confluence_retry_backoff_multiplier, 2)
        self.assertEqual(args.confluence_retry_max_delay, 7)
        self.assertEqual(args.confluence_retry_jitter, 0.4)
        self.assertTrue(args.confluence_continue_on_error)
        self.assertFalse(args.confluence_verify_ssl)

    def test_explicit_non_default_cli_args_are_not_overwritten_by_yaml_settings(self):
        config_path = self.create_config(
            "settings:\n"
            "  graph_width: 1800\n"
            "  threads: 8\n"
            "  confluence_upload_threads: 3\n"
            "  confluence_upload_delay: 1.5\n"
            "  confluence_retry: true\n"
            "  confluence_retry_count: 9\n"
            "  confluence_retry_delay: 2.5\n"
            "  confluence_upload_rate_per_second: 3.5\n"
            "  confluence_retry_backoff_multiplier: 2\n"
            "  confluence_retry_max_delay: 7\n"
            "  confluence_retry_jitter: 0.4\n"
            "  confluence_continue_on_error: false\n"
            "  confluence_ignore_verify_ssl: false\n"
        )

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
                "--graph_width",
                "1200",
                "--threads",
                "2",
                "--confluence_upload_threads",
                "5",
                "--confluence_upload_delay",
                "0.25",
                "--no-confluence_retry",
                "--confluence_retry_count",
                "4",
                "--confluence_retry_delay",
                "1.25",
                "--confluence_upload_rate_per_second",
                "5",
                "--confluence_retry_backoff_multiplier",
                "3",
                "--confluence_retry_max_delay",
                "9",
                "--confluence_retry_jitter",
                "0.75",
                "--confluence_continue_on_error",
                "--confluence_ignore_verify_ssl",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.graph_width, 1200)
        self.assertEqual(args.threads, 2)
        self.assertEqual(args.confluence_upload_threads, 5)
        self.assertEqual(args.confluence_upload_delay, 0.25)
        self.assertFalse(args.confluence_retry)
        self.assertEqual(args.confluence_retry_count, 4)
        self.assertEqual(args.confluence_retry_delay, 1.25)
        self.assertEqual(args.confluence_upload_rate_per_second, 5)
        self.assertEqual(args.confluence_retry_backoff_multiplier, 3)
        self.assertEqual(args.confluence_retry_max_delay, 9)
        self.assertEqual(args.confluence_retry_jitter, 0.75)
        self.assertTrue(args.confluence_continue_on_error)
        self.assertFalse(args.confluence_verify_ssl)

    def test_cli_overrides_new_confluence_retry_and_rate_options(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_upload_rate_per_second: 2.5\n"
            "  confluence_retry_backoff_multiplier: 1.5\n"
            "  confluence_retry_max_delay: 8\n"
            "  confluence_retry_jitter: 0.25\n"
        )

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
                "--confluence_upload_rate_per_second",
                "4",
                "--confluence_retry_backoff_multiplier",
                "3",
                "--confluence_retry_max_delay",
                "9",
                "--confluence_retry_jitter",
                "0.75",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.confluence_upload_rate_per_second, 4)
        self.assertEqual(args.confluence_retry_backoff_multiplier, 3)
        self.assertEqual(args.confluence_retry_max_delay, 9)
        self.assertEqual(args.confluence_retry_jitter, 0.75)

    def test_invalid_new_confluence_options_raise_clear_errors(self):
        cases = [
            ("confluence_upload_rate_per_second: 0", "confluence_upload_rate_per_second"),
            ("confluence_upload_rate_per_second: -1", "confluence_upload_rate_per_second"),
            ("confluence_retry_backoff_multiplier: 0.5", "confluence_retry_backoff_multiplier"),
            ("confluence_retry_max_delay: -1", "confluence_retry_max_delay"),
            ("confluence_retry_jitter: -0.1", "confluence_retry_jitter"),
        ]

        for yaml_line, expected_error in cases:
            with self.subTest(yaml_line=yaml_line):
                config_path = self.create_config(f"settings:\n  {yaml_line}\n")
                with self.assertRaisesRegex(ValueError, expected_error):
                    self.parse_args(
                        [
                            "prog",
                            "--config",
                            config_path,
                            "--wiki_url",
                            "https://cli.example",
                            "--confluence_page_id",
                            "1",
                            "--timestamps",
                            "tag__&from=1700000000&to=1700003600",
                        ],
                        env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
                    )

    def test_cli_rejects_zero_upload_rate(self):
        config_path = self.create_config()

        with self.assertRaisesRegex(ValueError, "confluence_upload_rate_per_second"):
            self.parse_args(
                [
                    "prog",
                    "--config",
                    config_path,
                    "--wiki_url",
                    "https://cli.example",
                    "--confluence_page_id",
                    "1",
                    "--timestamps",
                    "tag__&from=1700000000&to=1700003600",
                    "--confluence_upload_rate_per_second",
                    "0",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

    def test_cli_can_disable_confluence_retry(self):
        config_path = self.create_config("settings:\n  confluence_retry: true\n")

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
                "--no-confluence_retry",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertFalse(args.confluence_retry)

    def test_explicit_argv_does_not_read_sys_argv(self):
        config_path = self.create_config()

        with patch.object(sys, "argv", ["prog", "--unexpected"]):
            with patch.dict(os.environ, {"CONFLUENCE_LOGIN": "env-user", "CONFLUENCE_PASSWORD": "env-pass"}):
                args = ArgsParser([
                    "--config",
                    config_path,
                    "--wiki_url",
                    "https://cli.example",
                    "--confluence_page_id",
                    "1",
                    "--timestamps",
                    "tag__&from=1700000000&to=1700003600",
                ])

        self.assertEqual(args.wiki_url, "https://cli.example")
        self.assertEqual(args.confluence_page_id, 1)

    def test_yaml_settings_wiki_url_overwrites_cli_value(self):
        config_path = self.create_config("settings:\n  wiki_url: https://yaml.example\n")

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.wiki_url, "https://yaml.example")

    def test_timestamp_parsing_supports_seconds_milliseconds_and_iso_ranges(self):
        config_path = self.create_config()
        timestamps = [
            "seconds__&from=1700000000&to=1700003600",
            "milliseconds__&from=1700000000123&to=1700003600456",
            "iso__&from=2025-11-16T14:24:49.073Z&to=2025-11-16T14:30:00.000Z",
        ]

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_page_id",
                "1",
                "--tz",
                "UTC",
                "--timestamps",
                *timestamps,
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        seconds_timestamp, milliseconds_timestamp, iso_timestamp = args.timestamps

        self.assertEqual(seconds_timestamp.start_time_timestamp, 1700000000 * 1000)
        self.assertEqual(seconds_timestamp.end_time_timestamp, 1700003600 * 1000)
        self.assertEqual(milliseconds_timestamp.start_time_timestamp, 1700000000123)
        self.assertEqual(milliseconds_timestamp.end_time_timestamp, 1700003600456)

        expected_iso_start = int(datetime.fromisoformat("2025-11-16T14:24:49.073+00:00").timestamp()) * 1000
        expected_iso_end = int(datetime.fromisoformat("2025-11-16T14:30:00.000+00:00").timestamp()) * 1000
        self.assertEqual(iso_timestamp.start_time_timestamp, expected_iso_start)
        self.assertEqual(iso_timestamp.end_time_timestamp, expected_iso_end)
        self.assertEqual(iso_timestamp.time_tag, "iso")
