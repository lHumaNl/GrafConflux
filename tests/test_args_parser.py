import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from grafconflux.args_parser import ArgsParser


class TestArgsParser(unittest.TestCase):
    def create_config(
        self,
        content: str = "dashboards:\n  demo:\n    dash_title: Demo\n    grafana_url: https://grafana.example\n",
        *,
        raw: bool = False,
    ):
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        config_text = content
        if not raw and "dashboards:" not in content and not content.lstrip().startswith("dashboard:"):
            config_text = (
                f"{content.rstrip()}\n"
                "dashboards:\n"
                "  demo:\n"
                "    dash_title: Demo\n"
                "    grafana_url: https://grafana.example\n"
            )
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(config_text)
        self.addCleanup(temp_dir.cleanup)
        return config_path

    def parse_args(self, argv, env=None):
        env = env or {}
        with patch.object(sys, "argv", argv), patch.dict(os.environ, env, clear=False):
            return ArgsParser()

    def test_requires_wiki_url(self):
        config_path = self.create_config()

        with self.assertRaisesRegex(ValueError, "wiki_url"):
            self.parse_args(
                [
                    "prog",
                    "--config",
                    config_path,
                    "--confluence_page_id",
                    "1",
                    "--timestamps",
                    "tag__&from=1700000000&to=1700003600",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

    def test_yaml_settings_wiki_url_satisfies_cli_validation(self):
        config_path = self.create_config("settings:\n  wiki_url: https://yaml.example\n")

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--confluence_page_id",
                "1",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.wiki_url, "https://yaml.example")

    def test_requires_confluence_page_id(self):
        config_path = self.create_config()

        with patch.object(sys, "argv", ["prog", "--config", config_path, "--wiki_url", "https://cli.example"]):
            with self.assertRaises(SystemExit):
                ArgsParser()

    def test_accepts_child_page_mode_parent_id(self):
        config_path = self.create_config()

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_parent_page_id",
                "2",
                "--confluence_child_title",
                "Child title",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.confluence_parent_page_id, 2)
        self.assertEqual(args.confluence_child_title, "Child title")
        self.assertIsNone(args.confluence_page_id)

    def test_accepts_child_title_prefix_option(self):
        config_path = self.create_config()

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_parent_page_id",
                "2",
                "--confluence_child_title_prefix",
                "Release: ",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.confluence_child_title_prefix, "Release: ")

    def test_accepts_child_title_from_test_id_option(self):
        config_path = self.create_config()

        args = self.parse_args(
            [
                "prog",
                "--config",
                config_path,
                "--wiki_url",
                "https://cli.example",
                "--confluence_parent_page_id",
                "2",
                "--confluence_child_title_from_test_id",
                "--timestamps",
                "tag__&from=1700000000&to=1700003600",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertTrue(args.confluence_child_title_from_test_id)

    def test_rejects_direct_and_child_page_modes_together(self):
        config_path = self.create_config()

        with patch.object(sys, "argv", [
            "prog",
            "--config",
            config_path,
            "--wiki_url",
            "https://cli.example",
            "--confluence_page_id",
            "1",
            "--confluence_parent_page_id",
            "2",
            "--timestamps",
            "tag__&from=1700000000&to=1700003600",
        ]):
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

    def test_rejects_test_upload_folders_in_child_page_mode(self):
        config_path = self.create_config()

        with self.assertRaisesRegex(ValueError, "--test_upload_folders cannot be used with child page mode"):
            self.parse_args(
                [
                    "prog",
                    "--config",
                    config_path,
                    "--wiki_url",
                    "https://cli.example",
                    "--confluence_parent_page_id",
                    "2",
                    "--test_upload_folders",
                    "graphs\\run-a",
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

    def test_yaml_confluence_credentials_override_environment(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_login: yaml-user\n"
            "  confluence_password: yaml-pass\n"
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
            env={"CONFLUENCE_LOGIN": "env-user", "CONFLUENCE_PASSWORD": "env-pass"},
        )

        self.assertEqual(args.confluence_login, "yaml-user")
        self.assertEqual(args.confluence_password, "yaml-pass")

    def test_cli_confluence_credentials_override_yaml(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_login: yaml-user\n"
            "  confluence_password: yaml-pass\n"
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
                "--confluence_login",
                "cli-user",
                "--confluence_password",
                "cli-pass",
            ],
        )

        self.assertEqual(args.confluence_login, "cli-user")
        self.assertEqual(args.confluence_password, "cli-pass")

    def test_yaml_confluence_credentials_support_env_references(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_login: env:TEST_CONF_LOGIN\n"
            "  confluence_password: env:TEST_CONF_PASSWORD\n"
            "  confluence_token: env:TEST_CONF_TOKEN\n"
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
            env={
                "TEST_CONF_LOGIN": "env-yaml-user",
                "TEST_CONF_PASSWORD": "env-yaml-pass",
                "TEST_CONF_TOKEN": "env-yaml-token",
            },
        )

        self.assertEqual(args.confluence_login, "env-yaml-user")
        self.assertEqual(args.confluence_password, "env-yaml-pass")
        self.assertEqual(args.confluence_token, "env-yaml-token")

    def test_missing_env_reference_raises_clear_error_without_secret(self):
        config_path = self.create_config(
            "settings:\n  confluence_token: env:GRAFCONFLUX_TEST_MISSING_CONF_TOKEN\n"
        )

        with self.assertRaisesRegex(ValueError, "GRAFCONFLUX_TEST_MISSING_CONF_TOKEN") as context:
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
            )

        self.assertNotIn("secret", str(context.exception).lower())

    def test_confluence_token_satisfies_auth_requirement(self):
        config_path = self.create_config("settings:\n  confluence_token: yaml-token\n")

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
        )

        self.assertEqual(args.confluence_token, "yaml-token")

    def test_accepts_dashboards_without_settings_when_cli_or_env_provide_required_fields(self):
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
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.wiki_url, "https://cli.example")

    def test_rejects_missing_dashboards_when_only_settings_are_present(self):
        config_path = self.create_config("settings:\n  wiki_url: https://yaml.example\n", raw=True)

        with self.assertRaisesRegex(ValueError, "non-empty top-level 'dashboards' mapping"):
            self.parse_args(
                [
                    "prog",
                    "--config",
                    config_path,
                    "--confluence_page_id",
                    "1",
                    "--timestamps",
                    "tag__&from=1700000000&to=1700003600",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

    def test_rejects_legacy_yaml_without_settings(self):
        config_path = self.create_config("dashboard:\n  grafana_url: https://grafana.example\n")

        with self.assertRaisesRegex(ValueError, "Legacy top-level dashboard YAML format"):
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

    def test_yaml_settings_apply_playwright_browser_options(self):
        config_path = self.create_config(
            "settings:\n"
            "  playwright_browser: chromium\n"
            "  playwright_browser_channel: chrome\n"
            "  playwright_browser_executable_path: C:/Browsers/chrome.exe\n"
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

        self.assertEqual(args.playwright_browser, "chromium")
        self.assertEqual(args.playwright_browser_channel, "chrome")
        self.assertEqual(args.playwright_browser_executable_path, "C:/Browsers/chrome.exe")

    def test_cli_playwright_browser_options_override_yaml_settings(self):
        config_path = self.create_config(
            "settings:\n"
            "  playwright_browser: chromium\n"
            "  playwright_browser_channel: chrome\n"
            "  playwright_browser_executable_path: C:/Browsers/chrome.exe\n"
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
                "--playwright_browser",
                "firefox",
                "--playwright_browser_channel",
                "firefox",
                "--playwright_browser_executable_path",
                "C:/Browsers/firefox.exe",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertEqual(args.playwright_browser, "firefox")
        self.assertEqual(args.playwright_browser_channel, "firefox")
        self.assertEqual(args.playwright_browser_executable_path, "C:/Browsers/firefox.exe")

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

    def test_cli_confluence_verify_ssl_false_disables_ssl_verification(self):
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
                "--confluence_verify_ssl",
                "false",
            ],
            env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
        )

        self.assertFalse(args.confluence_verify_ssl)

    def test_legacy_confluence_ignore_verify_ssl_cli_option_is_rejected(self):
        config_path = self.create_config()

        with self.assertRaises(SystemExit):
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
                    "--confluence_ignore_verify_ssl",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

    def test_yaml_confluence_verify_ssl_false_disables_ssl_verification(self):
        config_path = self.create_config("settings:\n  confluence_verify_ssl: false\n")

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

        self.assertFalse(args.confluence_verify_ssl)

    def test_yaml_confluence_verify_ssl_takes_precedence_over_yaml_ignore_alias(self):
        config_path = self.create_config(
            "settings:\n"
            "  confluence_verify_ssl: true\n"
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

        self.assertTrue(args.confluence_verify_ssl)

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
            "  confluence_verify_ssl: true\n"
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
                "--confluence_verify_ssl",
                "false",
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

    def test_invalid_confluence_verify_ssl_value_raises_clear_error(self):
        config_path = self.create_config()

        with self.assertRaises(SystemExit):
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
                    "--confluence_verify_ssl",
                    "maybe",
                ],
                env={"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"},
            )

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
