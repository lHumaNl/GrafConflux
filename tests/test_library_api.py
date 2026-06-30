import importlib
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import main


class TestLibraryApi(unittest.TestCase):
    def setUp(self) -> None:
        self.src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
        self.original_path = list(sys.path)
        sys.path.insert(0, self.src_path)

    def tearDown(self) -> None:
        sys.path[:] = self.original_path
        for module_name in list(sys.modules):
            if (
                module_name == "grafconflux"
                or module_name.startswith("grafconflux.")
            ):
                sys.modules.pop(module_name, None)

    def create_config(self, content: str = "settings: {}\n") -> str:
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(content)
        self.addCleanup(temp_dir.cleanup)
        return config_path

    def required_options(self) -> dict[str, object]:
        return {
            "wiki_url": "https://library.example",
            "confluence_page_id": 1,
            "confluence_login": "user",
            "confluence_password": "secret",
            "timestamps": ["tag__&from=1700000000&to=1700003600"],
        }

    def test_public_exports_are_available(self) -> None:
        package = importlib.import_module("grafconflux")

        self.assertTrue(hasattr(package, "GrafConfluxRunOptions"))
        self.assertTrue(hasattr(package, "options_from_config_file"))
        self.assertTrue(hasattr(package, "parse_timestamps"))
        self.assertTrue(hasattr(package, "run"))
        self.assertTrue(hasattr(package, "run_from_config_file"))

    def test_config_module_exports_config_helpers(self) -> None:
        config = importlib.import_module("grafconflux.config")

        self.assertTrue(hasattr(config, "options_from_config_file"))
        self.assertTrue(hasattr(config, "parse_timestamps"))
        self.assertTrue(hasattr(config, "run_from_config_file"))

    def test_parse_timestamps_uses_existing_downloader_semantics(self) -> None:
        api = importlib.import_module("grafconflux.api")
        timestamps = api.parse_timestamps([
            "first__&from=1700000000&to=1700003600",
            "second__&from=1700003600000&to=1700007200000",
        ])

        self.assertEqual([timestamp.id_time for timestamp in timestamps], [0, 1])
        self.assertEqual(timestamps[0].time_tag, "first")
        self.assertEqual(timestamps[0].start_time_timestamp, 1700000000000)
        self.assertEqual(timestamps[1].end_time_timestamp, 1700007200000)

    def test_api_run_delegates_to_orchestration_run(self) -> None:
        api = importlib.import_module("grafconflux.api")
        options = api.GrafConfluxRunOptions(wiki_url="https://wiki.example", confluence_page_id=1)

        with patch("grafconflux.orchestration.run") as orchestration_run:
            api.run(options)

        orchestration_run.assert_called_once_with(options)

    def test_options_from_config_file_does_not_touch_sys_argv(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config()

        with patch.object(sys, "argv", ["prog", "--unexpected"]):
            with patch("grafconflux.args_parser.ArgsParser.__init__", side_effect=AssertionError):
                options = api.options_from_config_file(config_path, **self.required_options())

        self.assertEqual(options.config_file, config_path)
        self.assertEqual(options.wiki_url, "https://library.example")
        self.assertEqual(options.timestamps[0].time_tag, "tag")
        self.assertEqual(options.confluence_upload_threads, 1)
        self.assertIsNone(options.confluence_upload_rate_per_second)
        self.assertEqual(options.confluence_retry_backoff_multiplier, 1.0)
        self.assertIsNone(options.confluence_retry_max_delay)
        self.assertEqual(options.confluence_retry_jitter, 0)

    def test_yaml_settings_wiki_url_overwrites_library_value(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config("settings:\n  wiki_url: https://yaml.example\n")

        options = api.options_from_config_file(config_path, **self.required_options())

        self.assertEqual(options.wiki_url, "https://yaml.example")

    def test_yaml_settings_apply_when_defaults_are_used(self) -> None:
        api = importlib.import_module("grafconflux.api")
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

        options = api.options_from_config_file(config_path, **self.required_options())

        self.assertEqual(options.graph_width, 1800)
        self.assertEqual(options.threads, 8)
        self.assertEqual(options.confluence_upload_threads, 3)
        self.assertEqual(options.confluence_upload_delay, 1.5)
        self.assertFalse(options.confluence_retry)
        self.assertEqual(options.confluence_retry_count, 9)
        self.assertEqual(options.confluence_retry_delay, 2.5)
        self.assertEqual(options.confluence_upload_rate_per_second, 3.5)
        self.assertEqual(options.confluence_retry_backoff_multiplier, 2)
        self.assertEqual(options.confluence_retry_max_delay, 7)
        self.assertEqual(options.confluence_retry_jitter, 0.4)
        self.assertTrue(options.confluence_continue_on_error)
        self.assertFalse(options.confluence_verify_ssl)

    def test_explicit_non_default_args_are_not_overwritten_by_yaml_settings(self) -> None:
        api = importlib.import_module("grafconflux.api")
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
        options_kwargs = {
            **self.required_options(),
            "graph_width": 1200,
            "threads": 2,
            "confluence_upload_threads": 5,
            "confluence_upload_delay": 0.25,
            "confluence_retry": False,
            "confluence_retry_count": 4,
            "confluence_retry_delay": 1.25,
            "confluence_upload_rate_per_second": 5,
            "confluence_retry_backoff_multiplier": 3,
            "confluence_retry_max_delay": 9,
            "confluence_retry_jitter": 0.75,
            "confluence_continue_on_error": True,
            "confluence_verify_ssl": False,
        }

        options = api.options_from_config_file(config_path, **options_kwargs)

        self.assertEqual(options.graph_width, 1200)
        self.assertEqual(options.threads, 2)
        self.assertEqual(options.confluence_upload_threads, 5)
        self.assertEqual(options.confluence_upload_delay, 0.25)
        self.assertFalse(options.confluence_retry)
        self.assertEqual(options.confluence_retry_count, 4)
        self.assertEqual(options.confluence_retry_delay, 1.25)
        self.assertEqual(options.confluence_upload_rate_per_second, 5)
        self.assertEqual(options.confluence_retry_backoff_multiplier, 3)
        self.assertEqual(options.confluence_retry_max_delay, 9)
        self.assertEqual(options.confluence_retry_jitter, 0.75)
        self.assertTrue(options.confluence_continue_on_error)
        self.assertFalse(options.confluence_verify_ssl)

    def test_invalid_new_library_options_raise_clear_errors(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config()
        cases = [
            ({"confluence_upload_rate_per_second": 0}, "confluence_upload_rate_per_second"),
            ({"confluence_upload_rate_per_second": -1}, "confluence_upload_rate_per_second"),
            ({"confluence_retry_backoff_multiplier": 0.5}, "confluence_retry_backoff_multiplier"),
            ({"confluence_retry_max_delay": -1}, "confluence_retry_max_delay"),
            ({"confluence_retry_jitter": -0.1}, "confluence_retry_jitter"),
        ]

        for kwargs, expected_error in cases:
            with self.subTest(kwargs=kwargs):
                options_kwargs = {**self.required_options(), **kwargs}
                with self.assertRaisesRegex(ValueError, expected_error):
                    api.options_from_config_file(config_path, **options_kwargs)

    def test_yaml_config_rejects_zero_upload_rate(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config("settings:\n  confluence_upload_rate_per_second: 0\n")

        with self.assertRaisesRegex(ValueError, "confluence_upload_rate_per_second"):
            api.options_from_config_file(config_path, **self.required_options())

    def test_env_credentials_fallback_works(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config()
        kwargs = self.required_options()
        kwargs.pop("confluence_login")
        kwargs.pop("confluence_password")

        with patch.dict(os.environ, {"CONFLUENCE_LOGIN": "env-user", "CONFLUENCE_PASSWORD": "env-pass"}):
            options = api.options_from_config_file(config_path, **kwargs)

        self.assertEqual(options.confluence_login, "env-user")
        self.assertEqual(options.confluence_password, "env-pass")

    def test_run_from_config_file_delegates_to_orchestration_with_built_options(self) -> None:
        api = importlib.import_module("grafconflux.api")
        config_path = self.create_config()

        with patch("grafconflux.orchestration.run") as orchestration_run:
            api.run_from_config_file(config_path, **self.required_options())

        orchestration_run.assert_called_once()
        options = orchestration_run.call_args.args[0]
        self.assertEqual(options.config_file, config_path)
        self.assertEqual(options.wiki_url, "https://library.example")
        self.assertEqual(options.confluence_page_id, 1)

    def test_main_wrapper_constructs_args_and_calls_run(self) -> None:
        args = Mock()
        args_parser = Mock(return_value=args)
        run = Mock()

        with patch.dict(main.main.__globals__, {"ArgsParser": args_parser, "run": run}):
            main.main(["--help"])

        args_parser.assert_called_once_with(["--help"])
        run.assert_called_once_with(args)

    def test_main_wrapper_exits_with_one_on_failure(self) -> None:
        args_parser = Mock(side_effect=ValueError("bad args"))

        with patch.dict(main.main.__globals__, {"ArgsParser": args_parser}):
            with patch.object(main.main.__globals__["sys"], "exit") as sys_exit:
                with self.assertLogs(main.logger, level="ERROR") as logs:
                    main.main(["--bad"])

        sys_exit.assert_called_once_with(1)
        self.assertTrue(any("An error occurred: bad args" in message for message in logs.output))

    def test_main_compatibility_shim_reexports_run(self) -> None:
        self.assertTrue(callable(main.run))
        self.assertEqual(main.run.__name__, "run")


if __name__ == "__main__":
    unittest.main()
