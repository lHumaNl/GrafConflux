import unittest
from types import SimpleNamespace

from grafconflux._orchestration.runner import run as run_impl


BASE_RUN_ARGS = {
    "batch_run_args": [],
    "config_file": "config.yaml",
    "confluence_continue_on_error": False,
    "confluence_login": None,
    "confluence_child_title": None,
    "confluence_child_title_from_test_id": False,
    "confluence_child_title_prefix": "GrafConflux: ",
    "confluence_parent_page_id": None,
    "confluence_password": None,
    "confluence_retry": True,
    "confluence_retry_backoff_multiplier": 1.0,
    "confluence_retry_count": 3,
    "confluence_retry_delay": 1.0,
    "confluence_retry_jitter": 0,
    "confluence_retry_max_delay": None,
    "confluence_token": None,
    "confluence_upload_delay": 0,
    "confluence_upload_rate_per_second": None,
    "confluence_upload_threads": 1,
    "confluence_verify_ssl": True,
    "graph_width": 1000,
    "only_graphs": False,
    "playwright_browser": None,
    "playwright_browser_channel": None,
    "playwright_browser_executable_path": None,
    "test_id": "run",
    "test_root_folder": "graphs",
    "test_upload_folders": None,
    "threads": 1,
    "timestamps": [],
}


class TestOrchestrationPageLinks(unittest.TestCase):
    def make_run_args(self, page_id=1, wiki_url="https://wiki.example"):
        values = BASE_RUN_ARGS | {"confluence_page_id": page_id, "wiki_url": wiki_url}
        values["batch_run_args"] = []
        values["timestamps"] = []
        return SimpleNamespace(**values)

    def test_direct_mode_outputs_target_page_link(self):
        result = self.run_with_fakes(self.make_run_args(7))

        self.assertEqual([link.role for link in result.page_links], ["target"])
        self.assertEqual(result.page_links[0].url, "https://wiki.example/pages/viewpage.action?pageId=7")

    def test_direct_batch_outputs_target_page_links_in_order(self):
        first = self.make_run_args(1, "https://wiki.example/confluence/")
        second = self.make_run_args(2, "https://wiki.example/confluence/")
        result = self.run_with_fakes(SimpleNamespace(batch_run_args=[first, second]))

        self.assertEqual([link.page_id for link in result.page_links], [1, 2])
        self.assertEqual(result.page_links[0].url, "https://wiki.example/confluence/pages/viewpage.action?pageId=1")

    def test_child_mode_outputs_child_and_updated_parent_links(self):
        args = self.make_run_args(None)
        args.confluence_parent_page_id = 9
        result = self.run_with_fakes(args, parent_updated=True)

        self.assertEqual([link.role for link in result.page_links], ["child", "parent"])
        self.assertEqual(result.page_links[0].url, "https://wiki.example/pages/viewpage.action?pageId=101")
        self.assertEqual(result.page_links[1].url, "https://wiki.example/pages/viewpage.action?pageId=9")

    def test_child_mode_omits_parent_link_when_marker_is_missing(self):
        args = self.make_run_args(None)
        args.confluence_parent_page_id = 9
        result = self.run_with_fakes(args, parent_updated=False)

        self.assertEqual([link.role for link in result.page_links], ["child"])

    def test_direct_batch_logs_page_link_after_each_page_update_and_final_summary(self):
        first = self.make_run_args(1)
        second = self.make_run_args(2)

        with self.assertLogs("grafconflux.orchestration", level="INFO") as captured:
            self.run_with_fakes(SimpleNamespace(batch_run_args=[first, second]))

        self.assertEqual(
            [
                message for message in captured.output
                if "Confluence target page updated:" in message
            ],
            [
                "INFO:grafconflux.orchestration:Confluence target page updated: https://wiki.example/pages/viewpage.action?pageId=1",
                "INFO:grafconflux.orchestration:Confluence target page updated: https://wiki.example/pages/viewpage.action?pageId=2",
            ],
        )
        self.assertIn("INFO:grafconflux.orchestration:Confluence page links:", captured.output)

    def test_child_mode_logs_child_and_parent_links_immediately(self):
        args = self.make_run_args(None)
        args.confluence_parent_page_id = 9

        with self.assertLogs("grafconflux.orchestration", level="INFO") as captured:
            self.run_with_fakes(args, parent_updated=True)

        self.assertIn(
            "INFO:grafconflux.orchestration:Confluence child page updated: https://wiki.example/pages/viewpage.action?pageId=101",
            captured.output,
        )
        self.assertIn(
            "INFO:grafconflux.orchestration:Confluence parent page updated: https://wiki.example/pages/viewpage.action?pageId=9",
            captured.output,
        )

    def run_with_fakes(self, args, parent_updated=True):
        fake_confluence = self.fake_confluence_class(parent_updated)
        return run_impl(args, fake_confluence, FakeGrafanaManager, process_dashboard)

    def fake_confluence_class(self, parent_updated):
        class FakeConfluenceManager:
            def __init__(self, **kwargs):
                self.page_id = kwargs["page_id"]
                self.wiki_url = kwargs["wiki_url"]
                self.last_parent_page_url = None

            def get_parent_page(self, parent_id):
                return {"title": "Parent", "space": {"key": "OPS"}}

            def create_or_get_child_page(self, parent_id, run_args):
                run_args.confluence_page_id = 101
                return SimpleNamespace(title="Child", space_key="OPS", page_id=101)

            def upload_charts(self, *args):
                return []

            def update_page_content(self, *args):
                return f"{self.wiki_url.rstrip('/')}/pages/viewpage.action?pageId={self.page_id}"

            def update_parent_include_block(self, parent_id, child_pages):
                if parent_updated:
                    self.last_parent_page_url = f"{self.wiki_url.rstrip('/')}/pages/viewpage.action?pageId={parent_id}"
                return parent_updated

        return FakeConfluenceManager


class FakeGrafanaManager:
    @staticmethod
    def load_grafana_config(config_file):
        return [SimpleNamespace(dash_title=config_file)]


def process_dashboard(grafana_config, test_folder, run_args, confluence_manager):
    return None
