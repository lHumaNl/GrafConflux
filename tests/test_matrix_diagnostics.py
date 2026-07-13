import unittest
from types import SimpleNamespace

from grafconflux._grafana.matrix_diagnostics import (
    bounded_response_preview,
    diagnostic_block,
    sanitize_diagnostic_text,
    sanitize_diagnostic_url,
)


class TestMatrixDiagnosticRedaction(unittest.TestCase):
    def test_url_keeps_route_and_safe_query_but_redacts_userinfo_and_secrets(self) -> None:
        url = (
            "https://alice:password@grafana.example/grafana/api/v1/series"
            "?match%5B%5D=up%7Bjob%3D%22api%22%7D&start=10&token=hidden"
            "&api-key=also-hidden&client_secret=third-hidden"
        )

        sanitized = sanitize_diagnostic_url(url)

        self.assertIn("https://grafana.example/grafana/api/v1/series", sanitized)
        self.assertIn("match%5B%5D=up%7Bjob%3D%22api%22%7D", sanitized)
        self.assertIn("start=10", sanitized)
        self.assertNotIn("alice", sanitized)
        self.assertNotIn("password", sanitized)
        self.assertNotIn("hidden", sanitized)
        self.assertIn("token=%3Credacted%3E", sanitized)
        self.assertIn("client_secret=%3Credacted%3E", sanitized)

    def test_text_redacts_auth_cookie_token_and_password_material(self) -> None:
        text = (
            "Authorization: Bearer bearer-secret Cookie: sid=cookie-secret; "
            "Set-Cookie: auth=set-cookie-secret password=plain token=plain-token"
        )

        sanitized = sanitize_diagnostic_text(text)

        for secret in ("bearer-secret", "cookie-secret", "set-cookie-secret", "plain-token"):
            self.assertNotIn(secret, sanitized)
        self.assertNotIn("password=plain", sanitized)

    def test_url_redacts_secret_assignments_nested_in_safe_query_values(self) -> None:
        url = (
            "https://grafana.example/grafana/api/v1/series?"
            "match%5B%5D=up%7Bclient_secret%3D%22selector-secret%22%7D"
        )

        sanitized = sanitize_diagnostic_url(url)

        self.assertNotIn("selector-secret", sanitized)
        self.assertIn("client_secret%3D%3Credacted%3E", sanitized)

    def test_preview_is_bounded_and_suppressed_when_auth_pattern_is_present(self) -> None:
        long_response = SimpleNamespace(text="x" * 500)
        sensitive_response = SimpleNamespace(text="result Authorization: Bearer private-token")
        userinfo_response = SimpleNamespace(text="failed https://alice:password@example.test/api")
        json_secret_response = SimpleNamespace(text='{"access_token":"private-token"}')

        preview = bounded_response_preview(long_response)

        self.assertLessEqual(len(preview), 300)
        self.assertTrue(preview.endswith("..."))
        self.assertEqual(bounded_response_preview(sensitive_response), "<redacted:sensitive-content>")
        self.assertEqual(bounded_response_preview(userinfo_response), "<redacted:sensitive-content>")
        self.assertEqual(bounded_response_preview(json_secret_response), "<redacted:sensitive-content>")

    def test_block_is_clearly_delimited_and_multiline(self) -> None:
        block = diagnostic_block("MATRIX TEST", (("route", "proxy_uid"), ("status", 503)))

        self.assertEqual(block.splitlines()[0], "--- BEGIN MATRIX TEST ---")
        self.assertIn("  route=proxy_uid", block.splitlines())
        self.assertEqual(block.splitlines()[-1], "--- END MATRIX TEST ---")
        self.assertGreaterEqual(block.count("\n"), 3)


if __name__ == "__main__":
    unittest.main()
