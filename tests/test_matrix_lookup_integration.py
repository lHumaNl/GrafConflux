import unittest
from types import SimpleNamespace

from grafconflux._grafana.matrix import append_matrix_tasks
from grafconflux._shared.grafana_models import Panel, PanelRenderTask


class TestMatrixLookupIntegration(unittest.TestCase):
    def test_lookup_names_drive_inferred_topological_reordering(self) -> None:
        config = SimpleNamespace(
            name="Demo",
            render_matrix={
                "variables": {
                    "pod": {"lookup": "Pod selector", "values": ["api-1"]},
                    "namespace": {"lookup": "Namespace selector", "values": ["team-a"]},
                },
            },
            vars={},
        )
        dashboard = {"templating": {"list": [
            {
                "name": "pod_uri",
                "label": "Pod selector",
                "query": 'label_values(up{namespace="$namespace_uri"}, pod)',
            },
            {"name": "namespace_uri", "label": "Namespace selector", "query": "constant"},
        ]}}
        timestamp = SimpleNamespace(
            id_time=0,
            time_tag="period",
            start_time_timestamp=1,
            end_time_timestamp=2,
        )
        panel = Panel(17, "timeseries", "Requests", 1)
        source_artifact = {"artifact_type": "normal", "png_file": "source.png"}
        source_task = PanelRenderTask(panel, timestamp, {}, "source.png", source_artifact)

        tasks = append_matrix_tasks(
            config, dashboard, [], [panel], [source_task], [timestamp],
        )

        self.assertEqual(tasks[0].variables, {"namespace_uri": "team-a", "pod_uri": "api-1"})
        context_path = tasks[0].artifact["matrix"]["context_path"]
        self.assertEqual([item["key"] for item in context_path], ["namespace", "pod"])


if __name__ == "__main__":
    unittest.main()
