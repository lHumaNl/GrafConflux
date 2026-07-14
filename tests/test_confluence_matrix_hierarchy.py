import unittest
from types import SimpleNamespace

from grafconflux._confluence.matrix_content import render_matrix_dashboard
from grafconflux._shared.confluence_settings import ConfluenceRenderingSettings
from grafconflux._shared.grafana_models import Panel


class TestConfluenceMatrixHierarchy(unittest.TestCase):
    def test_one_dimension_places_final_value_inside_panel(self) -> None:
        panel = self.panel(
            self.artifact("service-api.png", [self.context("service", "Service", "api")], 0),
        )

        content = render_matrix_dashboard(self.config(panel), 600)

        panels = 'ac:parameter ac:name="title">Panels</ac:parameter>'
        requests = 'ac:parameter ac:name="title">Requests</ac:parameter>'
        leaf = "Service: api"
        self.assertLess(content.index(panels), content.index(requests))
        self.assertLess(content.index(requests), content.index(leaf))
        self.assertNotIn("<h3>Service: api</h3>", content)

    def test_two_dimensions_keep_final_values_inside_their_parent_group(self) -> None:
        panel = self.panel(
            self.artifact(
                "team-a-pod-a.png",
                [self.context("namespace", "Namespace", "team-a"), self.context("pod", "Pod", "pod-a")],
                0,
            ),
            self.artifact(
                "team-b-pod-b.png",
                [self.context("namespace", "Namespace", "team-b"), self.context("pod", "Pod", "pod-b")],
                1,
            ),
        )

        content = render_matrix_dashboard(self.config(panel), 600)

        team_a = content.index("Namespace: team-a")
        team_b = content.index("Namespace: team-b")
        self.assertLess(team_a, content.index("Pod: pod-a", team_a, team_b))
        self.assertNotIn("Pod: pod-b", content[team_a:team_b])
        self.assertIn("Pod: pod-b", content[team_b:])
        self.assertEqual(content.count('ac:parameter ac:name="title">Panels</ac:parameter>'), 2)

    def test_n_dimensions_render_all_prefix_layers_before_panels(self) -> None:
        context = [
            self.context("region", "Region", "east"),
            self.context("namespace", "Namespace", "team-a"),
            self.context("pod", "Pod", "api-1"),
        ]
        panel = self.panel(self.artifact("api-1.png", context, 0))

        content = render_matrix_dashboard(self.config(panel), 600)

        fragments = (
            "Region: east",
            "Namespace: team-a",
            'ac:parameter ac:name="title">Panels</ac:parameter>',
            'ac:parameter ac:name="title">Requests</ac:parameter>',
            "Pod: api-1",
            "api-1.png",
        )
        positions = [content.index(fragment) for fragment in fragments]
        self.assertEqual(positions, sorted(positions))

    def test_hidden_prefix_values_keep_separate_neutral_groups(self) -> None:
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

        self.assertIn("Group 1", content)
        self.assertIn("Group 2", content)
        self.assertNotIn("private-a", content)
        self.assertNotIn("private-b", content)
        self.assertEqual(content.count('ac:parameter ac:name="title">Panels</ac:parameter>'), 2)

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

        first_section = content[content.index("first-dashboard"):content.index("first.png")]
        second_section = content[content.index("second-dashboard"):content.index("second.png")]
        self.assertNotIn("second-dashboard", first_section)
        self.assertNotIn("first-dashboard", second_section)

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
    def context(key: str, label: str, value: str, hidden: bool = False) -> dict:
        return {
            "key": key,
            "label": label,
            "value": value,
            "raw_value": value,
            "display_value": value,
            "hidden": hidden,
        }


if __name__ == "__main__":
    unittest.main()
