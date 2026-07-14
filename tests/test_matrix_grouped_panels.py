import os
import tempfile
import unittest
from types import SimpleNamespace

import yaml

from grafconflux._confluence.matrix_content import render_matrix_dashboard
from grafconflux._grafana.matrix import _zip_rows
from grafconflux._grafana.matrix_config import validated_render_matrix
from grafconflux._orchestration.upload_merge import transform_grafana_configs
from grafconflux._shared.confluence_settings import ConfluenceRenderingSettings
from grafconflux._shared.grafana_models import ConfigurationError, GrafanaConfigUploader, Panel


GROUPED_LAYOUT = "matrix_grouped_panels"


class TestMatrixGroupedPanelsConfig(unittest.TestCase):
    def test_omitted_layout_resolves_to_grouped_panels(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {"variables": {"service": {"values": ["api"]}}},
        })

        self.assertEqual(matrix["layout"], GROUPED_LAYOUT)

    def test_all_explicit_layouts_remain_supported(self) -> None:
        for layout in (GROUPED_LAYOUT, "matrix_values_first", "panel_first", "dashboard_first"):
            with self.subTest(layout=layout):
                matrix = validated_render_matrix("Demo", {
                    "render_matrix": {
                        "options": {"layout": layout},
                        "variables": {"service": {"values": ["api"]}},
                    },
                })
                self.assertEqual(matrix["layout"], layout)


class TestMatrixGroupedPanelsRendering(unittest.TestCase):
    def test_one_dimension_places_unique_dashboard_links_before_panels(self) -> None:
        contexts = [self.context("service", "Service", "api")]
        panel = self.panel("Requests", self.artifact("api.png", contexts, "panel-api"))
        config = self.config([panel], layout=None)
        config.matrix_dashboard_links = [
            self.dashboard_link("dashboard-api", contexts),
            self.dashboard_link("dashboard-api-duplicate", contexts),
        ]

        content = render_matrix_dashboard(config, 600)

        self.assertLess(content.index("dashboard-api"), content.index(self.panels_title()))
        self.assertLess(content.index(self.panels_title()), content.index(self.panel_title("Requests")))
        self.assertLess(content.index(self.panel_title("Requests")), content.index("panel-api"))
        self.assertEqual(content.count("dashboard-api"), 1)
        self.assertIn(">Service: api</a>", content)
        self.assertNotIn("Requests (Service: api)", content)

    def test_product_prefix_groups_do_not_mix_dependent_namespace_pods(self) -> None:
        team_a = [
            self.context("namespace", "Namespace", "team-a"),
            self.context("pod", "Pod", "api-a"),
        ]
        team_b = [
            self.context("namespace", "Namespace", "team-b"),
            self.context("pod", "Pod", "api-b"),
        ]
        panel = self.panel(
            "CPU",
            self.artifact("team-a.png", team_a, "panel-team-a"),
            self.artifact("team-b.png", team_b, "panel-team-b"),
        )
        config = self.config([panel])
        config.matrix_dashboard_links = [
            self.dashboard_link("dashboard-team-a", team_a),
            self.dashboard_link("dashboard-team-b", team_b),
        ]

        content = render_matrix_dashboard(config, 600)

        first_start = content.index("Namespace: team-a")
        second_start = content.index("Namespace: team-b")
        first_group = content[first_start:second_start]
        self.assertIn("dashboard-team-a", first_group)
        self.assertIn("panel-team-a", first_group)
        self.assertNotIn("team-b.png", first_group)
        self.assertIn("dashboard-team-b", content[second_start:])
        self.assertEqual(content.count(self.panels_title()), 2)

    def test_n_dimensions_put_all_prefixes_before_links_and_panels(self) -> None:
        context = [
            self.context("region", "Region", "east"),
            self.context("namespace", "Namespace", "apps"),
            self.context("pod", "Pod", "api-1"),
        ]
        panel = self.panel("Latency", self.artifact("api-1.png", context, "panel-api-1"))
        config = self.config([panel])
        config.matrix_dashboard_links = [self.dashboard_link("dashboard-api-1", context)]

        content = render_matrix_dashboard(config, 600)

        fragments = (
            "Region: east",
            "Namespace: apps",
            "dashboard-api-1",
            self.panels_title(),
            self.panel_title("Latency"),
            "panel-api-1",
            "api-1.png",
        )
        positions = [content.index(fragment) for fragment in fragments]
        self.assertEqual(positions, sorted(positions))

    def test_automatic_hide_is_visible_but_explicit_hide_is_neutral(self) -> None:
        automatic = [
            self.context("namespace", "Namespace", "apps", hidden=True, hide_explicit=False),
            self.context("pod", "Pod", "api", hidden=True, hide_explicit=False),
        ]
        explicit = [
            self.context("namespace", "Namespace", "secret", hidden=True, hide_explicit=True),
            self.context("pod", "Pod", "private", hidden=True, hide_explicit=True),
        ]
        panel = self.panel(
            "CPU",
            self.artifact("automatic.png", automatic, "automatic-panel", neutral_label="Variant 1"),
            self.artifact("explicit.png", explicit, "explicit-panel", neutral_label="Variant 2"),
        )

        config = self.config([panel])
        config.matrix_dashboard_links = [
            self.dashboard_link("automatic-dashboard", automatic),
            self.dashboard_link("explicit-dashboard", explicit),
        ]
        content = render_matrix_dashboard(config, 600)

        self.assertIn("Namespace: apps", content)
        self.assertIn("Pod: api", content)
        self.assertIn("Group 2", content)
        self.assertIn(">Variant 2</a>", content)
        self.assertEqual(content.count(">Pod: api</a>"), 2)
        self.assertEqual(content.count(">Variant 2</a>"), 2)
        self.assertNotIn("secret", content)
        self.assertNotIn("private", content)

    def test_duplicate_display_aliases_keep_raw_context_and_timestamp_links(self) -> None:
        first = [self.context("service", "Service", "api", display_value="Shared")]
        second = [self.context("service", "Service", "worker", display_value="Shared")]
        panel = self.panel(
            "Errors",
            self.artifact("api-t1.png", first, "panel-api-t1", timestamp_id=1),
            self.artifact("api-t2.png", first, "panel-api-t2", timestamp_id=2),
            self.artifact("worker-t1.png", second, "panel-worker-t1", timestamp_id=1),
        )
        config = self.config([panel])
        config.matrix_dashboard_links = [
            self.dashboard_link("dashboard-api-t1", first, timestamp_id=1),
            self.dashboard_link("stale-api", first, timestamp_id=3),
            self.dashboard_link("dashboard-api-t2", first, timestamp_id=2),
            self.dashboard_link("dashboard-worker-t1", second, timestamp_id=1),
        ]

        content = render_matrix_dashboard(config, 600)

        panels_position = content.index(self.panels_title())
        for link in ("dashboard-api-t1", "dashboard-api-t2", "dashboard-worker-t1"):
            self.assertLess(content.index(link), panels_position)
            self.assertEqual(content.count(link), 1)
        self.assertNotIn("stale-api", content)
        self.assertEqual(content.count(">Service: Shared</a>"), 6)

    def test_explicit_matrix_values_first_golden_contract_is_unchanged(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel("Requests", self.artifact("api.png", context, "panel-api"))
        config = self.config([panel], layout="matrix_values_first")
        config.matrix_dashboard_links = [self.dashboard_link("dashboard-api", context)]

        content = render_matrix_dashboard(config, 600)

        expected_order = (
            self.panels_title(),
            self.panel_title("Requests"),
            "dashboard-api",
            "panel-api",
            "api.png",
        )
        positions = [content.index(fragment) for fragment in expected_order]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(content.count("dashboard-api"), 1)

    def test_explicit_legacy_layout_contracts_are_unchanged(self) -> None:
        context = [self.context("service", "Service", "api")]
        panel = self.panel("Requests", self.artifact("api.png", context, "panel-api"))

        panel_first = render_matrix_dashboard(self.config([panel], layout="panel_first"), 600)
        dashboard_first = render_matrix_dashboard(self.config([panel], layout="dashboard_first"), 600)

        self.assertNotIn(self.panels_title(), panel_first)
        self.assertIn("Requests (Service: api)", panel_first)
        self.assertIn("Demo (Service: api)", dashboard_first)
        self.assertLess(dashboard_first.index("Demo (Service: api)"), dashboard_first.index("Requests (Service: api)"))

    def test_zip_rows_preserve_pairing_without_cross_product(self) -> None:
        rows = _zip_rows({"namespace": ["team-a", "team-b"], "pod": ["api-a", "api-b"]}, "Demo")

        self.assertEqual(rows, [
            {"namespace": "team-a", "pod": "api-a"},
            {"namespace": "team-b", "pod": "api-b"},
        ])

    @staticmethod
    def config(panels: list[Panel], layout: str | None = GROUPED_LAYOUT) -> SimpleNamespace:
        render_matrix = {} if layout is None else {"layout": layout}
        return SimpleNamespace(
            name="Demo",
            panels=panels,
            matrix_dashboard_links=[],
            render_matrix=render_matrix,
            confluence_rendering=ConfluenceRenderingSettings(),
        )

    @staticmethod
    def panel(title: str, *artifacts: dict) -> Panel:
        panel = Panel(17, "timeseries", title, 1, ["https://grafana.example/panel/17"])
        panel.artifacts = list(artifacts)
        return panel

    @staticmethod
    def artifact(file_name: str, context: list[dict], link: str, timestamp_id: int | None = None,
                 neutral_label: str = "Variant 1") -> dict:
        artifact = {
            "artifact_type": "matrix",
            "render_status": "rendered",
            "png_file": file_name,
            "link": link,
            "matrix": {"context_path": context, "neutral_label": neutral_label},
        }
        if timestamp_id is not None:
            artifact["timestamp_id"] = timestamp_id
        return artifact

    @staticmethod
    def context(key: str, label: str, raw_value: str, display_value: str | None = None,
                hidden: bool = False, hide_explicit: bool = False) -> dict:
        return {
            "key": key,
            "label": label,
            "value": raw_value,
            "raw_value": raw_value,
            "display_value": display_value or raw_value,
            "hidden": hidden,
            "hide_explicit": hide_explicit,
        }

    @staticmethod
    def dashboard_link(url: str, context: list[dict], timestamp_id: int | None = None) -> dict:
        link = {"url": url, "label": "Dashboard", "context_path": context}
        if timestamp_id is not None:
            link["timestamp_id"] = timestamp_id
        return link

    @staticmethod
    def panels_title() -> str:
        return 'ac:parameter ac:name="title">Panels</ac:parameter>'

    @staticmethod
    def panel_title(title: str) -> str:
        return f'ac:parameter ac:name="title">{title}</ac:parameter>'


class TestMatrixGroupedPanelsUploadMigration(unittest.TestCase):
    def test_old_matrix_metadata_without_layout_resolves_to_grouped_panels(self) -> None:
        uploader = self.uploader("unused", layout=None)

        self.assertEqual(uploader.render_matrix["layout"], GROUPED_LAYOUT)

    def test_old_render_matrix_mapping_without_layout_resolves_to_grouped_panels(self) -> None:
        uploader = self.uploader("unused", layout=None, include_render_matrix=True)

        self.assertEqual(uploader.render_matrix["layout"], GROUPED_LAYOUT)

    def test_merge_persists_default_with_explicit_grouped_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = self.folder(temp_dir, "first")
            second = self.folder(temp_dir, "second")
            configs = [self.uploader(first, layout=None), self.uploader(second, layout=GROUPED_LAYOUT)]
            args = SimpleNamespace(
                test_root_folder=temp_dir,
                test_id="merged",
                test_upload_folders=[first, second],
                config_file=None,
            )

            merged, _ = transform_grafana_configs(configs, args)

        self.assertEqual(merged[0].render_matrix["layout"], GROUPED_LAYOUT)

    def test_metadata_roundtrip_preserves_resolved_layout_and_context_presentation(self) -> None:
        layouts = (None, "matrix_values_first", "panel_first", "dashboard_first")
        for layout in layouts:
            with self.subTest(layout=layout):
                with tempfile.TemporaryDirectory() as temp_dir:
                    folder = self.folder(temp_dir, "source")
                    config = self.uploader(
                        folder,
                        layout=layout,
                        schema=(("region", "var-region"), ("service", "var-service")),
                        hide_explicit=True,
                    )
                    args = self.merge_args(temp_dir, [folder])

                    _, output = transform_grafana_configs([config], args)
                    with open(os.path.join(output, "Demo.yaml"), encoding="utf-8") as metadata_file:
                        reloaded = GrafanaConfigUploader("Demo", yaml.safe_load(metadata_file))

                expected_layout = layout or GROUPED_LAYOUT
                context = reloaded.panels[0].artifacts[0]["matrix"]["context_path"]
                self.assertEqual(reloaded.render_matrix["layout"], expected_layout)
                self.assertEqual([item["key"] for item in context], ["region", "service"])
                self.assertTrue(all(item["hide_explicit"] for item in context))

    def test_merge_rejects_default_and_explicit_incompatible_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = self.folder(temp_dir, "first")
            second = self.folder(temp_dir, "second")
            configs = [self.uploader(first, layout=None), self.uploader(second, layout="matrix_values_first")]
            args = SimpleNamespace(
                test_root_folder=temp_dir,
                test_id="merged",
                test_upload_folders=[first, second],
                config_file=None,
            )

            with self.assertRaisesRegex(ConfigurationError, "render_matrix layouts differ"):
                transform_grafana_configs(configs, args)

    def test_merge_preserves_matching_explicit_matrix_values_first(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = self.folder(temp_dir, "first")
            second = self.folder(temp_dir, "second")
            configs = [
                self.uploader(first, layout="matrix_values_first"),
                self.uploader(second, layout="matrix_values_first"),
            ]
            args = SimpleNamespace(
                test_root_folder=temp_dir,
                test_id="merged",
                test_upload_folders=[first, second],
                config_file=None,
            )

            merged, _ = transform_grafana_configs(configs, args)

        self.assertEqual(merged[0].render_matrix["layout"], "matrix_values_first")

    def test_unknown_replay_layout_is_rejected(self) -> None:
        from grafconflux._shared.grafana_models import ConfigurationError as CurrentConfigurationError

        with self.assertRaisesRegex(CurrentConfigurationError, "render_matrix layout metadata"):
            self.uploader("unused", layout="unknown-layout")

    def test_merge_rejects_incompatible_grouped_dimension_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = self.folder(temp_dir, "first")
            second = self.folder(temp_dir, "second")
            configs = [
                self.uploader(first, GROUPED_LAYOUT, schema=(("service", "var-service"),)),
                self.uploader(second, GROUPED_LAYOUT, schema=(("region", "var-region"),)),
            ]

            with self.assertRaisesRegex(ConfigurationError, "matrix dimension schemas differ"):
                transform_grafana_configs(configs, self.merge_args(temp_dir, [first, second]))

    @staticmethod
    def folder(root: str, name: str) -> str:
        folder = os.path.join(root, name)
        os.makedirs(os.path.join(folder, "Demo"))
        return folder

    @staticmethod
    def uploader(
        folder: str,
        layout: str | None,
        include_render_matrix: bool = False,
        schema: tuple[tuple[str, str], ...] | None = None,
        hide_explicit: bool = False,
    ) -> GrafanaConfigUploader:
        context = [
            {
                "key": key,
                "label": key.title(),
                "value": "value",
                "raw_value": "value",
                "display_value": "value",
                "hidden": hide_explicit,
                "hide_explicit": hide_explicit,
                "grafana_variable": variable,
            }
            for key, variable in schema or (("service", "service"),)
        ]
        config = {
            "charts_path": os.path.join(folder, "Demo"),
            "full_links": [],
            "timestamps": [{
                "time_tag": "smoke",
                "id_time": 0,
                "start_time_timestamp": 1,
                "end_time_timestamp": 2,
                "start_time_human": "start",
                "end_time_human": "end",
            }],
            "panels": [{
                "panel_id": 17,
                "type": "timeseries",
                "title": "Requests",
                "links": [],
                "artifacts": [{
                    "artifact_type": "matrix",
                    "render_status": "rendered",
                    "png_file": "matrix__0.png",
                    "matrix": {"context_path": context},
                }],
            }],
        }
        if layout is not None:
            config["render_matrix"] = {"layout": layout}
        elif include_render_matrix:
            config["render_matrix"] = {"variables": {"service": {"values": ["api"]}}}
        return GrafanaConfigUploader("Demo", config)

    @staticmethod
    def merge_args(root: str, folders: list[str]) -> SimpleNamespace:
        return SimpleNamespace(
            test_root_folder=root,
            test_id="merged",
            test_upload_folders=folders,
            config_file=None,
        )


if __name__ == "__main__":
    unittest.main()
