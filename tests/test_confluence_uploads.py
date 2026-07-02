import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import Mock, patch

from grafconflux import confluence, confluence_uploads
from grafconflux.confluence import ConfluenceManager


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class TestConfluenceUploads(unittest.TestCase):
    def setUp(self):
        self.reset_rate_limiter()
        self.confluence_class = Mock()
        self.confluence = self.confluence_class.return_value
        self.confluence_patcher = patch.dict(
            ConfluenceManager.__init__.__globals__, {"Confluence": self.confluence_class}
        )
        self.confluence_patcher.start()

    def tearDown(self):
        self.confluence_patcher.stop()
        self.reset_rate_limiter()

    def reset_rate_limiter(self):
        reset = getattr(ConfluenceManager, "reset_upload_rate_limiter", None)
        if reset is not None:
            reset()

    def create_files(self, temp_dir, names):
        for name in names:
            with open(os.path.join(temp_dir, name), "wb") as file:
                file.write(b"data")

    def create_manager(self, upload_threads=1, **kwargs):
        return ConfluenceManager(
            login="user",
            password="secret",
            page_id=123,
            upload_threads=upload_threads,
            wiki_url="https://wiki.example",
            verify_ssl=True,
            **kwargs,
        )

    def test_threaded_upload_uploads_all_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["b.png", "a.png", "c.png"])
            manager = self.create_manager(upload_threads=3)

            manager.upload_charts(temp_dir)

        uploaded_names = [call.kwargs["name"] for call in self.confluence.attach_file.call_args_list]
        self.assertCountEqual(uploaded_names, ["a.png", "b.png", "c.png"])

    def test_token_auth_uses_confluence_token_constructor_arg(self):
        self.create_manager(token="pat-token")

        self.confluence_class.assert_called_with(
            url="https://wiki.example",
            verify_ssl=True,
            token="pat-token",
        )

    def test_upload_files_are_submitted_in_deterministic_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["z.png", "a.png", "m.png"])
            manager = self.create_manager(upload_threads=1)

            manager.upload_charts(temp_dir)

        uploaded_names = [call.kwargs["name"] for call in self.confluence.attach_file.call_args_list]
        self.assertEqual(uploaded_names, ["a.png", "m.png", "z.png"])

    def test_shared_limiter_spaces_upload_attempts_across_managers(self):
        clock = FakeClock()
        starts = []

        def attach_file(**kwargs):
            starts.append((kwargs["name"], clock.now))

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            self.create_files(first_dir, ["first.png"])
            self.create_files(second_dir, ["second.json"])
            first_manager = self.create_manager(upload_delay=2)
            second_manager = self.create_manager(upload_delay=2)

            with patch("grafconflux.confluence.time.monotonic", clock.monotonic):
                with patch("grafconflux.confluence.time.sleep", clock.sleep):
                    first_manager.upload_charts(first_dir)
                    second_manager.upload_charts(second_dir, [[".json", "application/json"]])

        self.assertEqual(starts, [("first.png", 0.0), ("second.json", 2.0)])
        self.assertEqual(clock.sleeps, [2.0])

    def test_rate_per_second_controls_effective_upload_interval(self):
        clock = FakeClock()
        starts = []

        def attach_file(**kwargs):
            starts.append((kwargs["name"], clock.now))

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["first.png", "second.png"])
            manager = self.create_manager(upload_delay=0.1, upload_rate_per_second=2)

            with patch("grafconflux.confluence.time.monotonic", clock.monotonic):
                with patch("grafconflux.confluence.time.sleep", clock.sleep):
                    manager.upload_charts(temp_dir)

        self.assertEqual(starts, [("first.png", 0.0), ("second.png", 0.5)])
        self.assertEqual(clock.sleeps, [0.5])

    def test_upload_delay_wins_when_larger_than_rate_interval(self):
        clock = FakeClock()
        starts = []

        self.confluence.attach_file.side_effect = lambda **kwargs: starts.append(clock.now)

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["first.png", "second.png"])
            manager = self.create_manager(upload_delay=0.75, upload_rate_per_second=4)

            with patch("grafconflux.confluence.time.monotonic", clock.monotonic):
                with patch("grafconflux.confluence.time.sleep", clock.sleep):
                    manager.upload_charts(temp_dir)

        self.assertEqual(starts, [0.0, 0.75])
        self.assertEqual(clock.sleeps, [0.75])

    def test_upload_rate_per_second_rejects_zero(self):
        with self.assertRaisesRegex(ValueError, "confluence_upload_rate_per_second"):
            self.create_manager(upload_rate_per_second=0)

    def test_upload_helpers_remain_available_from_confluence_module(self):
        self.assertIs(confluence._effective_upload_interval, confluence_uploads._effective_upload_interval)
        self.assertIs(confluence._extract_status_code, confluence_uploads._extract_status_code)
        self.assertIs(confluence._retry_after_seconds, confluence_uploads._retry_after_seconds)
        self.assertIs(confluence._ConfluenceUploadRateLimiter, confluence_uploads._ConfluenceUploadRateLimiter)

    def test_retry_waits_before_reacquiring_limiter_slot(self):
        clock = FakeClock()
        starts = []

        def attach_file(**kwargs):
            starts.append(clock.now)
            if len(starts) == 1:
                raise RuntimeError("temporary failure")

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(upload_delay=1, retry_count=2, retry_delay=0.25)

            with patch("grafconflux.confluence.time.monotonic", clock.monotonic):
                with patch("grafconflux.confluence.time.sleep", clock.sleep):
                    manager.upload_charts(temp_dir)

        self.assertEqual(starts, [0.0, 1.0])
        self.assertEqual(clock.sleeps, [0.25, 0.75])

    def test_retry_uses_backoff_jitter_and_caps_final_delay(self):
        clock = FakeClock()
        attempts = []

        def attach_file(**kwargs):
            attempts.append(clock.now)
            if len(attempts) < 4:
                raise RuntimeError("temporary failure")

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(
                retry_count=3,
                retry_delay=1,
                retry_backoff_multiplier=2,
                retry_max_delay=2.5,
                retry_jitter=0.5,
            )

            with patch("grafconflux.confluence.time.sleep", clock.sleep):
                with patch("grafconflux.confluence.random.uniform", return_value=0.25):
                    manager.upload_charts(temp_dir)

        self.assertEqual(attempts, [0.0, 1.25, 3.5, 6.0])
        self.assertEqual(clock.sleeps, [1.25, 2.25, 2.5])

    def test_retry_after_integer_overrides_configured_delay(self):
        clock = FakeClock()
        attempts = []
        response = Mock(status_code=429, headers={"Retry-After": "3"})
        error = RuntimeError("rate limited")
        error.response = response

        def attach_file(**kwargs):
            attempts.append(clock.now)
            if len(attempts) == 1:
                raise error

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=1, retry_delay=0.5)

            with patch("grafconflux.confluence.time.sleep", clock.sleep):
                manager.upload_charts(temp_dir)

        self.assertEqual(attempts, [0.0, 3.0])
        self.assertEqual(clock.sleeps, [3.0])

    def test_configured_delay_overrides_shorter_retry_after(self):
        clock = FakeClock()
        attempts = []
        response = Mock(status_code=429, headers={"Retry-After": "1"})
        error = RuntimeError("rate limited")
        error.response = response

        def attach_file(**kwargs):
            attempts.append(clock.now)
            if len(attempts) == 1:
                raise error

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=1, retry_delay=2)

            with patch("grafconflux.confluence.time.sleep", clock.sleep):
                manager.upload_charts(temp_dir)

        self.assertEqual(attempts, [0.0, 2.0])
        self.assertEqual(clock.sleeps, [2.0])

    def test_retry_after_http_date_overrides_configured_delay(self):
        clock = FakeClock()
        attempts = []
        retry_date = datetime.fromtimestamp(10, timezone.utc) + timedelta(seconds=4)
        response = Mock(status_code=429, headers={"Retry-After": format_datetime(retry_date)})
        error = RuntimeError("rate limited")
        error.response = response

        def attach_file(**kwargs):
            attempts.append(clock.now)
            if len(attempts) == 1:
                raise error

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=1, retry_delay=0.5)

            with patch("grafconflux.confluence.time.sleep", clock.sleep):
                with patch("grafconflux.confluence.time.time", return_value=10):
                    manager.upload_charts(temp_dir)

        self.assertEqual(attempts, [0.0, 4.0])
        self.assertEqual(clock.sleeps, [4.0])

    def test_permanent_401_error_is_not_retried(self):
        error = RuntimeError("unauthorized")
        error.response = Mock(status_code=401, headers={})
        self.confluence.attach_file.side_effect = error

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=3, retry_delay=0)

            with self.assertRaisesRegex(RuntimeError, "unauthorized"):
                manager.upload_charts(temp_dir)

        self.assertEqual(self.confluence.attach_file.call_count, 1)

    def test_rate_limited_429_error_is_retried_with_retry_after(self):
        clock = FakeClock()
        attempts = []
        error = RuntimeError("rate limited")
        error.status_code = 429
        error.headers = {"Retry-After": "2"}

        def attach_file(**kwargs):
            attempts.append(clock.now)
            if len(attempts) == 1:
                raise error

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=1, retry_delay=0)

            with patch("grafconflux.confluence.time.sleep", clock.sleep):
                manager.upload_charts(temp_dir)

        self.assertEqual(attempts, [0.0, 2.0])
        self.assertEqual(clock.sleeps, [2.0])

    def test_retryable_status_codes_are_retried(self):
        for status_code in [408, 409, 425, 503]:
            with self.subTest(status_code=status_code):
                error = RuntimeError("temporary failure")
                error.response = Mock(status_code=status_code, headers={})
                self.confluence.attach_file.reset_mock(side_effect=True)
                self.confluence.attach_file.side_effect = [error, None]

                with tempfile.TemporaryDirectory() as temp_dir:
                    self.create_files(temp_dir, ["chart.png"])
                    manager = self.create_manager(retry_count=1, retry_delay=0)

                    manager.upload_charts(temp_dir)

                self.assertEqual(self.confluence.attach_file.call_count, 2)

    def test_continue_on_error_false_propagates_after_retries(self):
        self.confluence.attach_file.side_effect = RuntimeError("upload failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_count=2, retry_delay=0)

            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                manager.upload_charts(temp_dir)

        self.assertEqual(self.confluence.attach_file.call_count, 3)

    def test_retry_disabled_attempts_upload_once(self):
        self.confluence.attach_file.side_effect = RuntimeError("upload failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["chart.png"])
            manager = self.create_manager(retry_enabled=False, retry_count=3)

            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                manager.upload_charts(temp_dir)

        self.assertEqual(self.confluence.attach_file.call_count, 1)

    def test_json_snapshot_uploads_are_retried(self):
        attempts = []

        def attach_file(**kwargs):
            attempts.append((kwargs["name"], kwargs["content_type"]))
            if len(attempts) == 1:
                raise RuntimeError("temporary failure")

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["snapshot.json"])
            manager = self.create_manager(retry_count=1, retry_delay=0)

            manager.upload_charts(temp_dir, [[".json", "application/json"]])

        self.assertEqual(
            attempts,
            [("snapshot.json", "application/json"), ("snapshot.json", "application/json")],
        )

    def test_continue_on_error_true_continues_after_final_failure(self):
        attempted = []

        def attach_file(**kwargs):
            attempted.append(kwargs["name"])
            if kwargs["name"] == "a-fails.png":
                raise RuntimeError("upload failed")

        self.confluence.attach_file.side_effect = attach_file

        with tempfile.TemporaryDirectory() as temp_dir:
            self.create_files(temp_dir, ["a-fails.png", "b-ok.png"])
            manager = self.create_manager(retry_count=1, retry_delay=0, continue_on_error=True)

            manager.upload_charts(temp_dir)

        self.assertEqual(attempted, ["a-fails.png", "a-fails.png", "b-ok.png"])


if __name__ == "__main__":
    unittest.main()
