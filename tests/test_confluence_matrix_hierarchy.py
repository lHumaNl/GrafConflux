import unittest
from types import SimpleNamespace

from grafconflux._confluence.matrix_content import render_matrix_dashboard
from grafconflux._shared.confluence_settings import ConfluenceRenderingSettings
from grafconflux._shared.grafana_models import Panel


class TestConfluenceMatrixHierarchy(unittest.TestCase):
    def test_one_dimension_places_link_and_panels_inside_final_value(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel(
            self.artifact("service-api.png", context, 0),
        )
        config = self.config(panel)
        config.matrix_dashboard_links = [self.dashboard_link("service-dashboard", context)]

        content = render_matrix_dashboard(config, 600)

        panels = 'ac:parameter ac:name="title">Panels</ac:parameter>'
        requests = 'ac:parameter ac:name="title">Requests</ac:parameter>'
        leaf = "Service: api"
        fragments = (leaf, "service-dashboard", panels, requests, "service-api.png")
        positions = [content.index(fragment) for fragment in fragments]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("<h3>Service: api</h3>", content)
        self.assertNotIn('ac:parameter ac:name="title">Service: api</ac:parameter>', content)
        self.assertNotIn(">Requests (Service: api)</a>", content)

    def test_two_dimensions_render_each_final_value_as_a_separate_branch(self) -> None:
        namespace = self.context("namespace", "Namespace", "team-a")
        pod_a = [namespace, self.context("pod", "Pod", "pod-a")]
        pod_b = [namespace, self.context("pod", "Pod", "pod-b")]
        panel = self.panel(
            self.artifact("team-a-pod-a.png", pod_a, 0),
            self.artifact("team-a-pod-b.png", pod_b, 1),
        )
        config = self.config(panel)
        config.matrix_dashboard_links = [
            self.dashboard_link("pod-a-dashboard", pod_a),
            self.dashboard_link("pod-b-dashboard", pod_b),
        ]

        content = render_matrix_dashboard(config, 600)

        pod_a_start = content.index("<h3>Pod: pod-a</h3>")
        pod_b_start = content.index("<h3>Pod: pod-b</h3>")
        pod_a_branch = content[pod_a_start:pod_b_start]
        pod_b_branch = content[pod_b_start:]
        self.assertLess(pod_a_branch.index("pod-a-dashboard"), pod_a_branch.index(">Panels</"))
        self.assertIn("team-a-pod-a.png", pod_a_branch)
        self.assertNotIn("pod-b-dashboard", pod_a_branch)
        self.assertLess(pod_b_branch.index("pod-b-dashboard"), pod_b_branch.index(">Panels</"))
        self.assertIn("team-a-pod-b.png", pod_b_branch)
        self.assertEqual(content.count('ac:parameter ac:name="title">Panels</ac:parameter>'), 2)

    def test_n_dimensions_render_all_layers_before_leaf_link_and_panels(self) -> None:
        context = [
            self.context("region", "Region", "east"),
            self.context("namespace", "Namespace", "team-a"),
            self.context("pod", "Pod", "api-1"),
        ]
        panel = self.panel(self.artifact("api-1.png", context, 0))
        config = self.config(panel)
        config.matrix_dashboard_links = [self.dashboard_link("api-dashboard", context)]

        content = render_matrix_dashboard(config, 600)

        fragments = (
            "Region: east",
            "Namespace: team-a",
            "Pod: api-1",
            "api-dashboard",
            'ac:parameter ac:name="title">Panels</ac:parameter>',
            'ac:parameter ac:name="title">Requests</ac:parameter>',
            "api-1.png",
        )
        positions = [content.index(fragment) for fragment in fragments]
        self.assertEqual(positions, sorted(positions))

    def test_implicit_hidden_prefix_values_are_visible(self) -> None:
        panel = self.panel(
            self.artifact(
                "hidden-one.png",
                [self.context("namespace", "Namespace", "private-a", hidden=True), self.context("pod", "Pod", "one")],
                0,
            ),
            self.artifact(
                "hidden-two.png",
                [self.context("namespace", "Namespace", "private-b", hidden=True), self.context("pod", "Pod", "two")],
                1,
            ),
        )

        content = render_matrix_dashboard(self.config(panel), 600)

        self.assertIn("Namespace: private-a", content)
        self.assertIn("Namespace: private-b", content)
        self.assertNotIn("Group 1", content)
        self.assertNotIn("Group 2", content)
        self.assertEqual(content.count('ac:parameter ac:name="title">Panels</ac:parameter>'), 2)

    def test_explicit_hide_false_displays_each_dimension(self) -> None:
        context = [
            self.context("namespace", "Namespace", "apps", hide_explicit=True),
            self.context("service", "Service", "api", hide_explicit=True),
        ]
        panel = self.panel(self.artifact("visible.png", context, 0))

        content = render_matrix_dashboard(self.config(panel), 600)

        self.assertIn("<h3>Namespace: apps</h3>", content)
        self.assertIn("<h3>Service: api</h3>", content)

    def test_explicit_hide_true_keeps_dimension_values_private(self) -> None:
        context = [
            self.context("namespace", "Namespace", "secret", hidden=True, hide_explicit=True),
            self.context("service", "Service", "private", hidden=True, hide_explicit=True),
        ]
        panel = self.panel(self.artifact("hidden.png", context, 0))

        content = render_matrix_dashboard(self.config(panel), 600)

        self.assertNotIn("secret", content)
        self.assertNotIn("private", content)
        self.assertIn("Group 1", content)
        self.assertIn("Variant 1", content)

    def test_aliases_and_html_are_escaped_without_changing_raw_identity(self) -> None:
        context = [self.context("service", "Service & Tier", "api")]
        context[0]["display_value"] = "<Public & API>"
        panel = self.panel(self.artifact("escaped.png", context, 0))
        config = self.config(panel)
        config.matrix_dashboard_links = [{
            "url": "https://grafana.example/d?x=1&y=2",
            "label": "<Dashboard & context>",
            "context_path": context,
        }]

        content = render_matrix_dashboard(config, 600)

        self.assertIn("Service &amp; Tier: &lt;Public &amp; API&gt;", content)
        self.assertIn('href="https://grafana.example/d?x=1&amp;y=2"', content)
        self.assertIn("&lt;Dashboard &amp; context&gt;", content)

    def test_empty_context_keeps_dashboard_link_before_panels(self) -> None:
        panel = self.panel(self.artifact("neutral.png", [], 0))
        config = self.config(panel)
        config.matrix_dashboard_links = [self.dashboard_link("matrix-dashboard", [])]

        content = render_matrix_dashboard(config, 600)

        panels = content.index('ac:parameter ac:name="title">Panels</ac:parameter>')
        self.assertLess(content.index("matrix-dashboard"), panels)

    def test_links_match_timestamp_and_raw_context(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel(
            self.artifact("first.png", context, 0, timestamp_id=1),
            self.artifact("second.png", context, 0, timestamp_id=2),
        )
        config = self.config(panel)
        config.matrix_dashboard_links = [
            self.dashboard_link("first-dashboard", context, timestamp_id=1),
            self.dashboard_link("second-dashboard", context, timestamp_id=2),
        ]

        content = render_matrix_dashboard(config, 600)

        panels = content.index('ac:parameter ac:name="title">Panels</ac:parameter>')
        self.assertLess(content.index("first-dashboard"), panels)
        self.assertLess(content.index("second-dashboard"), panels)
        self.assertEqual(content.count("first-dashboard"), 2)
        self.assertEqual(content.count("second-dashboard"), 2)

    def test_modern_artifact_does_not_use_legacy_or_other_timestamp_link(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel(self.artifact("modern.png", context, 0, timestamp_id=2))
        config = self.config(panel)
        config.matrix_dashboard_links = [
            self.dashboard_link("legacy-dashboard", context),
            self.dashboard_link("other-dashboard", context, timestamp_id=1),
        ]

        content = render_matrix_dashboard(config, 600)

        self.assertNotIn("legacy-dashboard", content)
        self.assertNotIn("other-dashboard", content)

    def test_legacy_artifact_does_not_use_modern_timestamp_link(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel(self.artifact("legacy.png", context, 0))
        config = self.config(panel)
        config.matrix_dashboard_links = [
            self.dashboard_link("modern-dashboard", context, timestamp_id=1),
        ]

        content = render_matrix_dashboard(config, 600)

        self.assertNotIn("modern-dashboard", content)

    def test_link_context_uses_raw_value_not_display_value(self) -> None:
        artifact_context = [self.context("service", "Service", "api")]
        link_context = [self.context("service", "Service", "worker")]
        artifact_context[0]["display_value"] = "Shared alias"
        link_context[0]["display_value"] = "Shared alias"
        panel = self.panel(self.artifact("raw-context.png", artifact_context, 0))
        config = self.config(panel)
        config.matrix_dashboard_links = [self.dashboard_link("wrong-context", link_context)]

        content = render_matrix_dashboard(config, 600)

        self.assertNotIn("wrong-context", content)

    @staticmethod
    def config(panel: Panel) -> SimpleNamespace:
        return SimpleNamespace(
            name="Demo",
            panels=[panel],
            matrix_dashboard_links=[],
            render_matrix={"layout": "matrix_values_first"},
            confluence_rendering=ConfluenceRenderingSettings(),
        )

    @staticmethod
    def panel(*artifacts: dict) -> Panel:
        panel = Panel(17, "timeseries", "Requests", 1, ["https://grafana.example/panel/17"])
        panel.artifacts = list(artifacts)
        return panel

    @staticmethod
    def artifact(file_name: str, context: list[dict], index: int,
                 timestamp_id: int | None = None) -> dict:
        artifact = {
            "artifact_type": "matrix",
            "render_status": "rendered",
            "png_file": file_name,
            "matrix": {
                "index": index,
                "neutral_label": f"Variant {index + 1}",
                "context_path": context,
            },
        }
        if timestamp_id is not None:
            artifact["timestamp_id"] = timestamp_id
        return artifact

    @staticmethod
    def dashboard_link(url: str, context: list[dict], timestamp_id: int | None = None) -> dict:
        link = {"url": url, "label": url, "context_path": context}
        if timestamp_id is not None:
            link["timestamp_id"] = timestamp_id
        return link

    @staticmethod
    def context(key: str, label: str, value: str, hidden: bool = False,
                hide_explicit: bool = False) -> dict:
        return {
            "key": key,
            "label": label,
            "value": value,
            "raw_value": value,
            "display_value": value,
            "hidden": hidden,
            "hide_explicit": hide_explicit,
        }


if __name__ == "__main__":
    unittest.main()
