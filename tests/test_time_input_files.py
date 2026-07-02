import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from grafconflux._orchestration.runner import run as run_impl
from grafconflux.args_parser import ArgsParser


class TestTimeInputFiles(unittest.TestCase):
    def create_config(self, temp_dir: str) -> str:
        config_path = os.path.join(temp_dir, "config.yaml")
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write(
                "dashboards:\n"
                "  demo:\n"
                "    dash_title: Demo\n"
                "    host: https://grafana.example\n"
            )
        return config_path

    def create_time_file(self, temp_dir: str, name: str, content: str) -> str:
        time_file_path = os.path.join(temp_dir, name)
        with open(time_file_path, "w", encoding="utf-8") as time_file:
            time_file.write(content)
        return time_file_path

    def parse_args(self, argv: list[str]) -> ArgsParser:
        env = {"CONFLUENCE_LOGIN": "user", "CONFLUENCE_PASSWORD": "secret"}
        with patch.dict(os.environ, env, clear=False):
            return ArgsParser(argv)

    def test_single_time_file_supplies_page_test_id_and_timestamps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            time_path = self.create_time_file(
                temp_dir,
                "times.yaml",
                "page_id: \"683084932\"\n"
                "test_id: file-test\n"
                "times:\n"
                "  - 13.0.10058: \"&from=1700000000&to=1700003600\"\n"
                "  - \"&from=1700003600&to=1700007200\"\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--time_files",
                time_path,
            ])

        self.assertEqual(args.confluence_page_id, 683084932)
        self.assertEqual(args.test_id, "file-test")
        self.assertEqual([timestamp.time_tag for timestamp in args.timestamps], ["13.0.10058", None])
        self.assertEqual(args.timestamps[1].start_time_timestamp, 1700003600000)

    def test_time_file_aliases_supply_inputs(self):
        for option_name in ("--time_files", "--times_files", "--timestamps_files"):
            with self.subTest(option_name=option_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    config_path = self.create_config(temp_dir)
                    time_path = self.create_time_file(
                        temp_dir,
                        "times.yaml",
                        "page_id: 7\ntimes:\n  - alias__&from=1700000000&to=1700003600\n",
                    )

                    args = self.parse_args([
                        "--config",
                        config_path,
                        "--wiki_url",
                        "https://wiki.example",
                        option_name,
                        time_path,
                    ])

                self.assertEqual(args.confluence_page_id, 7)
                self.assertEqual(args.timestamps[0].time_tag, "alias")

    def test_single_time_file_allows_cli_overrides(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            time_path = self.create_time_file(
                temp_dir,
                "times.yaml",
                "page_id: 1\n"
                "test_id: file-test\n"
                "times:\n"
                "  - file__&from=1700000000&to=1700003600\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--time_files",
                time_path,
                "--confluence_page_id",
                "2",
                "--test_id",
                "cli-test",
                "--timestamps",
                "cli__&from=1700003600&to=1700007200",
            ])

        self.assertEqual(args.confluence_page_id, 2)
        self.assertEqual(args.test_id, "cli-test")
        self.assertEqual([timestamp.time_tag for timestamp in args.timestamps], ["cli"])

    def test_single_time_file_supplies_parent_page_and_title(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            time_path = self.create_time_file(
                temp_dir,
                "times.yaml",
                "parent_page_id: 42\n"
                "title: Child title\n"
                "test_id: file-test\n"
                "times:\n"
                "  - file__&from=1700000000&to=1700003600\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--time_files",
                time_path,
            ])

        self.assertIsNone(args.confluence_page_id)
        self.assertEqual(args.confluence_parent_page_id, 42)
        self.assertEqual(args.confluence_child_title, "Child title")

    def test_single_time_file_allows_cli_child_mode_overrides(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            time_path = self.create_time_file(
                temp_dir,
                "times.yaml",
                "parent_page_id: 1\n"
                "title: File title\n"
                "times:\n"
                "  - file__&from=1700000000&to=1700003600\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--time_files",
                time_path,
                "--confluence_parent_page_id",
                "2",
                "--confluence_child_title",
                "CLI title",
            ])

        self.assertEqual(args.confluence_parent_page_id, 2)
        self.assertEqual(args.confluence_child_title, "CLI title")

    def test_multiple_time_files_create_sequential_batch_args(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            first_path = self.create_time_file(
                temp_dir,
                "first.yaml",
                "page_id: 1\ntest_id: first\ntimes:\n  - first__&from=1700000000&to=1700003600\n",
            )
            second_path = self.create_time_file(
                temp_dir,
                "second.yaml",
                "page_id: 2\ntest_id: second\ntimes:\n  - second__&from=1700003600&to=1700007200\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--time_files",
                first_path,
                second_path,
            ])

        self.assertEqual([batch.confluence_page_id for batch in args.batch_run_args], [1, 2])
        self.assertEqual([batch.test_id for batch in args.batch_run_args], ["first", "second"])
        self.assertEqual([batch.timestamps[0].time_tag for batch in args.batch_run_args], ["first", "second"])

    def test_multiple_time_files_create_child_batch_with_common_cli_parent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            first_path = self.create_time_file(
                temp_dir,
                "first.yaml",
                "title: First child\ntest_id: first\ntimes:\n  - first__&from=1700000000&to=1700003600\n",
            )
            second_path = self.create_time_file(
                temp_dir,
                "second.yaml",
                "title: Second child\ntest_id: second\ntimes:\n  - second__&from=1700003600&to=1700007200\n",
            )

            args = self.parse_args([
                "--config",
                config_path,
                "--wiki_url",
                "https://wiki.example",
                "--confluence_parent_page_id",
                "9",
                "--time_files",
                first_path,
                second_path,
            ])

        self.assertEqual([batch.confluence_parent_page_id for batch in args.batch_run_args], [9, 9])
        self.assertEqual([batch.confluence_child_title for batch in args.batch_run_args], ["First child", "Second child"])

    def test_multiple_time_files_reject_mixed_direct_and_child_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config(temp_dir)
            first_path = self.create_time_file(
                temp_dir,
                "first.yaml",
                "page_id: 1\ntimes:\n  - first__&from=1700000000&to=1700003600\n",
            )
            second_path = self.create_time_file(
                temp_dir,
                "second.yaml",
                "parent_page_id: 2\ntimes:\n  - second__&from=1700003600&to=1700007200\n",
            )

            with self.assertRaisesRegex(ValueError, "cannot mix"):
                self.parse_args([
                    "--config",
                    config_path,
                    "--wiki_url",
                    "https://wiki.example",
                    "--time_files",
                    first_path,
                    second_path,
                ])

    def test_multiple_time_files_reject_page_test_and_timestamp_overrides(self):
        override_cases = [
            ["--confluence_page_id", "3"],
            ["--test_id", "cli-test"],
            ["--timestamps", "cli__&from=1700000000&to=1700003600"],
        ]
        for override_args in override_cases:
            with self.subTest(override_args=override_args):
                with tempfile.TemporaryDirectory() as temp_dir:
                    config_path = self.create_config(temp_dir)
                    first_path = self.create_time_file(
                        temp_dir,
                        "first.yaml",
                        "page_id: 1\ntimes:\n  - first__&from=1700000000&to=1700003600\n",
                    )
                    second_path = self.create_time_file(
                        temp_dir,
                        "second.yaml",
                        "page_id: 2\ntimes:\n  - second__&from=1700003600&to=1700007200\n",
                    )

                    with self.assertRaisesRegex(ValueError, "Multiple --time_files"):
                        self.parse_args([
                            "--config",
                            config_path,
                            "--wiki_url",
                            "https://wiki.example",
                            "--time_files",
                            first_path,
                            second_path,
                            *override_args,
                        ])

    def test_time_file_validation_rejects_invalid_shapes(self):
        cases = [
            ("times:\n  - a__&from=1&to=2\n", "page_id"),
            ("page_id: 1\ntimes: []\n", "non-empty list"),
            ("page_id: 1\ntimes:\n  - {a: '&from=1&to=2', b: '&from=3&to=4'}\n", "exactly one pair"),
            ("page_id: true\ntimes:\n  - a__&from=1&to=2\n", "positive integer"),
        ]
        for content, expected_error in cases:
            with self.subTest(content=content):
                with tempfile.TemporaryDirectory() as temp_dir:
                    config_path = self.create_config(temp_dir)
                    time_path = self.create_time_file(temp_dir, "times.yaml", content)

                    with self.assertRaisesRegex(ValueError, expected_error):
                        self.parse_args([
                            "--config",
                            config_path,
                            "--wiki_url",
                            "https://wiki.example",
                            "--time_files",
                            time_path,
                        ])


class TestTimeFileBatchRunner(unittest.TestCase):
    def make_run_args(self, page_id: int, test_id: str) -> SimpleNamespace:
        return SimpleNamespace(
            batch_run_args=[],
            config_file=f"config-{page_id}.yaml",
            confluence_continue_on_error=False,
            confluence_login="user",
            confluence_child_title=None,
            confluence_child_title_from_test_id=False,
            confluence_child_title_prefix="GrafConflux: ",
            confluence_page_id=page_id,
            confluence_parent_page_id=None,
            confluence_password="secret",
            confluence_retry=True,
            confluence_retry_backoff_multiplier=1.0,
            confluence_retry_count=3,
            confluence_retry_delay=1.0,
            confluence_retry_jitter=0,
            confluence_retry_max_delay=None,
            confluence_token=None,
            confluence_upload_delay=0,
            confluence_upload_rate_per_second=None,
            confluence_upload_threads=1,
            confluence_verify_ssl=True,
            graph_width=1000,
            only_graphs=False,
            playwright_browser=None,
            playwright_browser_channel=None,
            playwright_browser_executable_path=None,
            test_id=test_id,
            test_root_folder="graphs",
            test_upload_folders=None,
            threads=1,
            timestamps=[test_id],
            wiki_url="https://wiki.example",
        )

    def test_batch_runs_complete_each_file_before_starting_next(self):
        calls = []
        batch_args = [self.make_run_args(1, "first"), self.make_run_args(2, "second")]
        args = SimpleNamespace(batch_run_args=batch_args)

        class FakeGrafanaManager:
            @staticmethod
            def load_grafana_config(config_file):
                calls.append(f"load:{config_file}")
                return [SimpleNamespace(dash_title=config_file)]

        class FakeConfluenceManager:
            def __init__(self, **kwargs):
                self.page_id = kwargs["page_id"]

            def upload_charts(self, *args):
                calls.append(f"upload:{self.page_id}")

            def update_page_content(self, *args):
                calls.append(f"update:{self.page_id}")

        def process_dashboard(grafana_config, test_folder, run_args, confluence_manager):
            calls.append(f"process:{run_args.confluence_page_id}:{grafana_config.dash_title}")

        run_impl(args, FakeConfluenceManager, FakeGrafanaManager, process_dashboard)

        self.assertEqual(
            calls,
            [
                "load:config-1.yaml",
                "process:1:config-1.yaml",
                "upload:1",
                "update:1",
                "load:config-2.yaml",
                "process:2:config-2.yaml",
                "upload:2",
                "update:2",
            ],
        )

    def test_child_batch_updates_parent_once_after_children_succeed(self):
        calls = []
        batch_args = [self.make_run_args(None, "first"), self.make_run_args(None, "second")]
        for run_args in batch_args:
            run_args.confluence_parent_page_id = 9
        args = self.make_run_args(None, "root")
        args.batch_run_args = batch_args
        args.confluence_parent_page_id = 9

        class FakeGrafanaManager:
            @staticmethod
            def load_grafana_config(config_file):
                calls.append(f"load:{config_file}")
                return [SimpleNamespace(dash_title=config_file)]

        class FakeConfluenceManager:
            def __init__(self, **kwargs):
                self.page_id = kwargs["page_id"]

            def get_parent_page(self, parent_id):
                return {"title": "Parent", "space": {"key": "OPS"}, "body": {"storage": {"value": "%%%graphs%%%"}}}

            def create_or_get_child_page(self, parent_id, run_args):
                calls.append(f"child:{parent_id}:{run_args.test_id}")
                run_args.confluence_page_id = 100 + len(calls)
                return SimpleNamespace(title=run_args.test_id, space_key="OPS")

            def upload_charts(self, *args):
                calls.append(f"upload:{self.page_id}")

            def update_page_content(self, *args):
                calls.append(f"update:{self.page_id}")

            def update_parent_include_block(self, parent_id, child_pages):
                calls.append(f"parent:{parent_id}:{len(child_pages)}")

        def process_dashboard(grafana_config, test_folder, run_args, confluence_manager):
            calls.append(f"process:{run_args.confluence_page_id}:{grafana_config.dash_title}")

        run_impl(args, FakeConfluenceManager, FakeGrafanaManager, process_dashboard)

        self.assertEqual(calls[-1], "parent:9:2")
        self.assertEqual(calls.count("parent:9:2"), 1)
        self.assertLess(calls.index("update:101"), calls.index("child:9:second"))

    def test_child_batch_rejects_duplicate_effective_titles_before_processing(self):
        calls = []
        first = self.make_run_args(None, "same")
        second = self.make_run_args(None, "same")
        for run_args in (first, second):
            run_args.confluence_parent_page_id = 9
        args = self.make_run_args(None, "root")
        args.batch_run_args = [first, second]
        args.confluence_parent_page_id = 9

        class FakeGrafanaManager:
            @staticmethod
            def load_grafana_config(config_file):
                calls.append(f"load:{config_file}")
                return [SimpleNamespace(dash_title=config_file)]

        class FakeConfluenceManager:
            def __init__(self, **kwargs):
                self.page_id = kwargs["page_id"]

            def get_parent_page(self, parent_id):
                calls.append(f"parent-page:{parent_id}")
                return {"title": "Parent", "space": {"key": "OPS"}, "body": {"storage": {"value": "%%%graphs%%%"}}}

            def create_or_get_child_page(self, parent_id, run_args):
                calls.append(f"child:{parent_id}:{run_args.test_id}")
                return SimpleNamespace(title=run_args.test_id, space_key="OPS")

        with self.assertRaisesRegex(ValueError, "unique effective child titles"):
            run_impl(args, FakeConfluenceManager, FakeGrafanaManager, lambda *run_args: None)

        self.assertEqual(calls, ["parent-page:9"])


if __name__ == "__main__":
    unittest.main()
