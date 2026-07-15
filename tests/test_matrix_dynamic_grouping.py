import os
import re
import statistics
import tempfile
import textwrap
import time
import unittest
from unittest.mock import Mock, patch

import yaml

from grafconflux._confluence.matrix_content import render_matrix_dashboard
from grafconflux._grafana.matrix import _planning_matrix
from grafconflux._grafana.matrix_config import serializable_render_matrix, validated_render_matrix
from grafconflux._grafana.matrix_dynamic import DynamicValuePlanner
from grafconflux._shared.grafana_models import ConfigurationError
from grafconflux.args_parser import GrafanaTimeDownloader
from grafconflux.grafana import GrafanaConfigUploader, GrafanaManager


class TestDynamicMatrixConfig(unittest.TestCase):
    def test_normalizes_parent_filters_and_grouping(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {
                "variables": {
                    "namespace": {"values": ["payments"]},
                    "pod": {
                        "depends_on": "namespace",
                        "values_from": {
                            "regex": "^app-",
                            "filters_by_parent": [{
                                "when": {"namespace": "payments"},
                                "regex": "-api-",
                            }],
                            "grouping": {
                                "rules": [{"name": "api", "regex": "-api-"}],
                            },
                        },
                    },
                },
            },
        })

        source = matrix["variables"]["pod"]["values_from"]
        self.assertEqual(source["filters_by_parent"][0]["when"], {"namespace": ["payments"]})
        self.assertEqual(source["filters_by_parent"][0]["mode"], "and")
        self.assertEqual(source["grouping"]["dimension"], {
            "key": "pod_group", "display_name": "pod Group", "hide": False,
        })
        self.assertEqual(source["grouping"]["rules"][0]["label"], "api")
        self.assertEqual(source["grouping"]["unmatched"], {
            "enabled": False, "name": "ungrouped", "label": "Ungrouped",
        })

    def test_rejects_new_fields_outside_mapping_values_from(self) -> None:
        for source in (
            {"values": ["pod-a"], "grouping": {"rules": [{"name": "a", "regex": "a"}]}},
            {"values_by": {"payments": ["pod-a"]}, "depends_on": "namespace", "filters_by_parent": []},
            {"values_from": "pod", "grouping": {"rules": [{"name": "a", "regex": "a"}]}},
        ):
            with self.subTest(source=source), self.assertRaisesRegex(
                ConfigurationError, r"render_matrix\.variables\.pod\.(filters_by_parent|grouping)"
            ):
                validated_render_matrix("Demo", {
                    "render_matrix": {"variables": {"namespace": {"values": ["payments"]}, "pod": source}},
                })

    def test_rejects_invalid_nested_schema_before_discovery(self) -> None:
        invalid_sources = (
            ({"filters_by_parent": [{"when": {}, "regex": "x"}]}, "filters_by_parent[0].when"),
            ({"filters_by_parent": [{"when": {"namespace": "payments"}, "regex": "("}]}, "filters_by_parent[0].regex"),
            ({"grouping": {"unknown": True}}, "grouping"),
            ({"grouping": {"rules": [{"name": "bad name", "regex": "x"}]}}, "grouping.rules[0].name"),
            ({"grouping": {"capture": {"regex": "(?P<service>x)", "group": "missing"}}}, "grouping.capture.group"),
        )
        for extra, path in invalid_sources:
            with self.subTest(path=path), self.assertRaisesRegex(ConfigurationError, path.replace("[", r"\[").replace("]", r"\]")):
                validated_render_matrix("Demo", {
                    "render_matrix": {"variables": {
                        "namespace": {"values": ["payments"]},
                        "pod": {"depends_on": "namespace", "values_from": extra},
                    }},
                })

    def test_nested_regex_and_max_values_errors_report_exact_paths(self) -> None:
        for source, suffix in (
            ({"regex": "("}, "values_from.regex"),
            ({"max_values": 0}, "values_from.max_values"),
        ):
            with self.subTest(source=source), self.assertRaisesRegex(ConfigurationError, suffix):
                validated_render_matrix("Demo", {
                    "render_matrix": {"variables": {"pod": {"values_from": source}}},
                })

    def test_new_invalid_regex_error_does_not_echo_pattern(self) -> None:
        pattern = "(?P<SECRET_REGEX_CANARY>"
        with self.assertRaises(ConfigurationError) as captured:
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {
                    "namespace": {"values": ["payments"]},
                    "pod": {"depends_on": "namespace", "values_from": {
                        "filters_by_parent": [{
                            "when": {"namespace": "payments"}, "regex": pattern,
                        }],
                    }},
                }},
            })
        self.assertNotIn(pattern, str(captured.exception))
        self.assertNotIn("SECRET_REGEX_CANARY", str(captured.exception))

    def test_dynamic_validation_matrix_rejects_invalid_shapes(self) -> None:
        cases = (
            ({"filters_by_parent": {}}, "filters_by_parent"),
            ({"filters_by_parent": [{"when": {"namespace": "x"}, "regex": "x", "extra": 1}]}, "unknown field"),
            ({"filters_by_parent": [{"when": {"namespace": "x"}, "regex": "x", "mode": "or"}]}, "mode"),
            ({"grouping": []}, "grouping"),
            ({"grouping": {"dimension": {"hide": "yes"}, "rules": [{"name": "x", "regex": "x"}]}}, "dimension.hide"),
            ({"grouping": {"rules": [{"name": "x", "regex": "x"}, {"name": "x", "regex": "y"}]}}, "duplicate rule"),
            ({"grouping": {"capture": {"regex": "(x)", "group": 2}}}, "capture.group"),
            ({"grouping": {"capture": {"regex": "(x)", "group": 1, "value_aliases": {"x": ""}}}}, "value_aliases"),
            ({"grouping": {"unmatched": {"enabled": "yes"}}}, "unmatched.enabled"),
            ({"grouping": {"rules": []}}, "expected rules, capture, or enabled unmatched"),
        )
        for source, expected in cases:
            with self.subTest(source=source), self.assertRaisesRegex(ConfigurationError, expected):
                validated_render_matrix("Demo", {
                    "render_matrix": {"variables": {
                        "namespace": {"values": ["x"]},
                        "pod": {"depends_on": "namespace", "values_from": source},
                    }},
                })

    def test_rejects_grouping_for_zip_and_unsupported_layout(self) -> None:
        for options, expected in (
            ({"combination_mode": "zip"}, "combination_mode"),
            ({"layout": "panel_first"}, "values_from.grouping"),
            ({"layout": "dashboard_first"}, "values_from.grouping"),
        ):
            with self.subTest(options=options), self.assertRaisesRegex(ConfigurationError, expected):
                validated_render_matrix("Demo", {
                    "render_matrix": {
                        "options": options,
                        "variables": {"pod": {"values_from": {
                            "grouping": {"rules": [{"name": "api", "regex": "api"}]},
                        }}},
                    },
                })

    def test_filtering_without_grouping_accepts_every_existing_layout(self) -> None:
        for layout in ("matrix_grouped_panels", "matrix_values_first", "panel_first", "dashboard_first"):
            with self.subTest(layout=layout):
                matrix = validated_render_matrix("Demo", {
                    "render_matrix": {
                        "options": {"layout": layout},
                        "variables": {
                            "namespace": {"values": ["payments"]},
                            "pod": {"depends_on": "namespace", "values_from": {
                                "filters_by_parent": [{
                                    "when": {"namespace": "payments"}, "regex": "api",
                                }],
                            }},
                        },
                    },
                })
                self.assertEqual(_planning_matrix("Demo", matrix, {})["layout"], layout)

    def test_dependency_and_dimension_collisions_fail_after_dependency_resolution(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {"variables": {
                "namespace": {"values": ["payments"]},
                "pod": {"depends_on": "namespace", "values_from": {
                    "filters_by_parent": [{"when": {"missing": "x"}, "regex": "x"}],
                }},
            }},
        })
        with self.assertRaisesRegex(ConfigurationError, "filters_by_parent.*missing.*resolved dependencies"):
            _planning_matrix("Demo", matrix, {})

        with self.assertRaisesRegex(ConfigurationError, "grouping.dimension.key"):
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {
                    "pod_group": {"values": ["x"]},
                    "pod": {"depends_on": "pod_group", "values_from": {
                        "grouping": {"rules": [{"name": "api", "regex": "api"}]},
                    }},
                }},
            })

    def test_validation_does_not_invoke_discovery(self) -> None:
        discovery = Mock(side_effect=AssertionError("discovery called"))
        with self.assertRaises(ConfigurationError):
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {"pod": {"values_from": {
                    "grouping": {"capture": {"regex": "(", "group": 1}},
                }}}},
            })
        discovery.assert_not_called()

    def test_each_dynamic_regex_is_compiled_once(self) -> None:
        original_compile = re.compile
        with patch("re.compile", wraps=original_compile) as compiled:
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {
                    "namespace": {"values": ["payments"]},
                    "pod": {"depends_on": "namespace", "values_from": {
                        "regex": "global-once",
                        "filters_by_parent": [{
                            "when": {"namespace": "payments"}, "regex": "parent-once",
                        }],
                        "grouping": {
                            "rules": [{"name": "api", "regex": "named-once"}],
                            "capture": {"regex": "(?P<service>capture-once)", "group": "service"},
                        },
                    }},
                }},
            })

        patterns = [call.args[0] for call in compiled.call_args_list]
        for pattern in ("global-once", "parent-once", "named-once", "(?P<service>capture-once)"):
            self.assertEqual(patterns.count(pattern), 1)

    def test_accepts_or_regex_lists_in_supported_dynamic_fields(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {"variables": {
                "namespace": {"values": ["payments"]},
                "pod": {"depends_on": "namespace", "values_from": {
                    "regex": ["^calculator-", "^matrix-"],
                    "filters_by_parent": [{
                        "when": {"namespace": "payments"},
                        "regex": ["-rate-", "-offer-"],
                    }],
                    "grouping": {"rules": [{
                        "name": "calculators",
                        "regex": ["calculator", "offer-generator"],
                    }]},
                }},
            }},
        })

        source = matrix["variables"]["pod"]["values_from"]
        self.assertEqual(source["regex"], ["^calculator-", "^matrix-"])
        self.assertEqual(source["filters_by_parent"][0]["regex"], ["-rate-", "-offer-"])
        self.assertEqual(
            source["grouping"]["rules"][0]["regex"],
            ["calculator", "offer-generator"],
        )
        self.assertEqual(
            serializable_render_matrix(matrix)["variables"]["pod"]["values_from"]["regex"],
            ["^calculator-", "^matrix-"],
        )

    def test_list_only_nested_global_regex_uses_or_planner(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {"variables": {
                "pod": {"values_from": {
                    "regex": ["^calculator-covenant-api-", "^matrix-calculator-rate-"],
                }},
            }},
        })

        planner = matrix["variables"]["pod"]["__dynamic_planner__"]
        self.assertEqual(
            planner.plan([
                "other-a",
                "calculator-covenant-api-a",
                "matrix-calculator-rate-b",
            ], {}).values,
            ["calculator-covenant-api-a", "matrix-calculator-rate-b"],
        )

    def test_rejects_invalid_regex_lists_with_indexed_paths(self) -> None:
        cases = (
            ({"regex": []}, r"values_from\.regex"),
            ({"regex": ["valid", ""]}, r"values_from\.regex\[1\]"),
            ({
                "filters_by_parent": [{
                    "when": {"namespace": "payments"}, "regex": ["valid", 42],
                }],
            }, r"filters_by_parent\[0\]\.regex\[1\]"),
            ({
                "grouping": {"rules": [{"name": "api", "regex": []}]},
            }, r"grouping\.rules\[0\]\.regex"),
        )
        for source, expected_path in cases:
            with self.subTest(source=source), self.assertRaisesRegex(ConfigurationError, expected_path):
                validated_render_matrix("Demo", {
                    "render_matrix": {"variables": {
                        "namespace": {"values": ["payments"]},
                        "pod": {"depends_on": "namespace", "values_from": source},
                    }},
                })

    def test_capture_regex_remains_single_string(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, r"grouping\.capture\.regex"):
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {"pod": {"values_from": {
                    "grouping": {"capture": {
                        "regex": [r"^(?P<service>api)-", r"^(?P<service>worker)-"],
                        "group": "service",
                    }},
                }}}},
            })

    def test_invalid_regex_list_item_reports_index_without_pattern(self) -> None:
        secret_pattern = "(?P<SECRET_REGEX_LIST_CANARY>"
        with self.assertRaises(ConfigurationError) as captured:
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {"pod": {"values_from": {
                    "regex": ["valid", secret_pattern],
                }}}},
            })

        message = str(captured.exception)
        self.assertIn("values_from.regex[1]", message)
        self.assertNotIn(secret_pattern, message)
        self.assertNotIn("SECRET_REGEX_LIST_CANARY", message)

    def test_each_regex_list_item_is_compiled_once(self) -> None:
        original_compile = re.compile
        expected = ("global-a", "global-b", "parent-a", "parent-b", "named-a", "named-b")
        with patch("re.compile", wraps=original_compile) as compiled:
            validated_render_matrix("Demo", {
                "render_matrix": {"variables": {
                    "namespace": {"values": ["payments"]},
                    "pod": {"depends_on": "namespace", "values_from": {
                        "regex": ["global-a", "global-b"],
                        "filters_by_parent": [{
                            "when": {"namespace": "payments"},
                            "regex": ["parent-a", "parent-b"],
                        }],
                        "grouping": {"rules": [{
                            "name": "api", "regex": ["named-a", "named-b"],
                        }]},
                    }},
                }},
            })

        patterns = [call.args[0] for call in compiled.call_args_list]
        for pattern in expected:
            self.assertEqual(patterns.count(pattern), 1)

    def test_dynamic_nested_regex_overrides_legacy_and_input_is_not_mutated(self) -> None:
        config = {
            "render_matrix": {"variables": {"pod": {
                "regex": "^legacy-",
                "values_from": {
                    "regex": "^nested-",
                    "grouping": {"unmatched": {"enabled": True}},
                },
            }}},
        }
        original = {
            "render_matrix": {"variables": {"pod": {
                "regex": "^legacy-",
                "values_from": {
                    "regex": "^nested-",
                    "grouping": {"unmatched": {"enabled": True}},
                },
            }}},
        }

        matrix = validated_render_matrix("Demo", config)
        planner = matrix["variables"]["pod"]["__dynamic_planner__"]

        self.assertEqual(planner.plan(["legacy-a", "nested-a"], {}).values, ["nested-a"])
        self.assertEqual(config, original)
        metadata = serializable_render_matrix(matrix)
        self.assertNotIn("__dynamic_planner__", metadata["variables"]["pod"])
        self.assertEqual(metadata["variables"]["pod"]["values_from"]["regex"], "^nested-")

    def test_scalar_legacy_regex_is_coerced_when_combined_with_new_dynamic_fields(self) -> None:
        matrix = validated_render_matrix("Demo", {
            "render_matrix": {"variables": {
                "pod": {
                    "regex": 123,
                    "values_from": {"grouping": {"unmatched": {"enabled": True}}},
                },
            }},
        })

        planner = matrix["variables"]["pod"]["__dynamic_planner__"]
        result = planner.plan(["x123y", "other"], {})

        self.assertEqual(result.values, ["x123y"])
        self.assertEqual(result.occurrences[0].value, "x123y")


class TestDynamicValuePlanner(unittest.TestCase):
    def test_parent_filters_compose_with_global_and_override(self) -> None:
        and_planner = DynamicValuePlanner.from_source({
            "regex": "^payments-",
            "filters_by_parent": [
                {"when": {"namespace": ["payments"]}, "regex": "-api-", "mode": "and"},
                {"when": {"namespace": ["payments"]}, "regex": "-v2-", "mode": "and"},
            ],
        })
        self.assertEqual(
            and_planner.plan(
                ["payments-api-v1-a", "payments-api-v2-a", "payments-worker-v2-a"],
                {"namespace": "payments"},
            ).values,
            ["payments-api-v2-a"],
        )

        override_planner = DynamicValuePlanner.from_source({
            "regex": "^app-",
            "filters_by_parent": [
                {"when": {"namespace": ["system"]}, "regex": "^coredns-", "mode": "override_global"},
                {"when": {"namespace": ["system"]}, "regex": "-ready$", "mode": "and"},
            ],
        })
        result = override_planner.plan(
            ["app-ready", "coredns-old", "coredns-ready"], {"namespace": "system"}
        )
        self.assertEqual(result.values, ["coredns-ready"])
        self.assertTrue(result.provenance["global_overridden"])
        self.assertEqual(result.provenance["matched_parent_filters"], 2)

    def test_regex_lists_are_or_within_each_filter_and_rules_emit_once(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "regex": ["^calculator-", "^matrix-"],
            "filters_by_parent": [
                {
                    "when": {"namespace": ["payments"]},
                    "regex": ["-rate-", "-offer-"],
                    "mode": "and",
                },
                {
                    "when": {"namespace": ["payments"]},
                    "regex": ["generator", "v2"],
                    "mode": "and",
                },
            ],
            "grouping": {
                "rules": [{
                    "name": "calculators",
                    "label": "Calculators",
                    "regex": ["calculator", "rate-v2"],
                }],
                "unmatched": {"enabled": False},
            },
        })

        result = planner.plan([
            "calculator-covenant-api-v2",
            "matrix-calculator-rate-v2",
            "matrix-offer-generator-v1",
            "other-offer-generator",
        ], {"namespace": "payments"})

        self.assertEqual(result.values, [
            "matrix-calculator-rate-v2",
            "matrix-offer-generator-v1",
        ])
        self.assertEqual(
            [(item.value, item.membership.identity) for item in result.occurrences],
            [("matrix-calculator-rate-v2", "named:calculators")],
        )

        override_planner = DynamicValuePlanner.from_source({
            "regex": ["^app-", "^web-"],
            "filters_by_parent": [{
                "when": {"namespace": ["system"]},
                "regex": ["^coredns-", "^metrics-server-"],
                "mode": "override_global",
            }],
        })
        self.assertEqual(
            override_planner.plan(
                ["app-api", "coredns-ready", "metrics-server-ready", "other"],
                {"namespace": "system"},
            ).values,
            ["coredns-ready", "metrics-server-ready"],
        )

    def test_when_uses_raw_parent_context_and_no_match_keeps_global(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "regex": "^app-",
            "filters_by_parent": [{
                "when": {"namespace": ["payments"]}, "regex": "-api-", "mode": "and",
            }],
        })
        self.assertEqual(
            planner.plan(["app-api-a", "app-worker-a"], {"namespace": "Payments"}).values,
            ["app-api-a", "app-worker-a"],
        )

    def test_named_capture_overlap_and_unmatched_have_stable_group_major_order(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "max_values": 3,
            "grouping": {
                "rules": [
                    {"name": "api", "label": "API", "regex": "api"},
                    {"name": "critical", "label": "Critical", "regex": "api|login"},
                ],
                "capture": {
                    "regex": "^(?P<service>[^-]+)-", "group": "service",
                    "value_aliases": {"checkout": "API"},
                },
                "unmatched": {"enabled": True, "name": "other", "label": "Other"},
            },
        })

        result = planner.plan(
            ["checkout-api", "auth-login", "checkout-api", "plain"], {}
        )

        self.assertEqual(result.values, ["checkout-api", "auth-login", "plain"])
        self.assertEqual(
            [(item.value, item.membership.identity) for item in result.occurrences],
            [
                ("checkout-api", "named:api"),
                ("checkout-api", "named:critical"),
                ("auth-login", "named:critical"),
                ("checkout-api", "capture:checkout"),
                ("auth-login", "capture:auth"),
                ("plain", "unmatched:other"),
            ],
        )
        self.assertEqual(result.occurrences[0].membership.display_value, "API")
        self.assertEqual(result.occurrences[3].membership.display_value, "API")
        self.assertNotEqual(
            result.occurrences[0].membership.identity,
            result.occurrences[3].membership.identity,
        )
        self.assertEqual(result.provenance["unique_capped"], 3)
        self.assertEqual(result.provenance["memberships"], 6)

    def test_default_unmatched_skips_capture_failures(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "grouping": {
                "rules": [],
                "capture": {"regex": r"^(?P<service>\w+)-pod$", "group": "service"},
                "unmatched": {"enabled": False, "name": "ungrouped", "label": "Ungrouped"},
            },
        })
        result = planner.plan(["other", "api-pod"], {})
        self.assertEqual([(item.value, item.membership.identity) for item in result.occurrences], [
            ("api-pod", "capture:api"),
        ])
        self.assertEqual(result.provenance["unmatched"], 1)

    def test_named_and_capture_when_match_only_raw_parent_values(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "grouping": {
                "rules": [{
                    "name": "api", "label": "API", "regex": "api",
                    "when": {"namespace": ["payments"]},
                }],
                "capture": {
                    "regex": "^(?P<service>api)$", "group": "service",
                    "when": {"namespace": ["payments"]},
                },
                "unmatched": {"enabled": False, "name": "ungrouped", "label": "Ungrouped"},
            },
        })

        self.assertEqual(planner.plan(["api"], {"namespace": "Payments"}).occurrences, [])
        self.assertEqual(
            [item.membership.identity for item in planner.plan(["api"], {"namespace": "payments"}).occurrences],
            ["named:api", "capture:api"],
        )

    def test_planning_benchmark_contract(self) -> None:
        planner = DynamicValuePlanner.from_source({
            "filters_by_parent": [
                {"when": {"namespace": ["apps"]}, "regex": ".*", "mode": "and"}
                for _ in range(10)
            ],
            "grouping": {
                "rules": [
                    {"name": f"group_{index}", "label": str(index), "regex": f"value-{index % 5}"}
                    for index in range(20)
                ],
                "capture": {"regex": "^(?P<group>value)-", "group": "group"},
                "unmatched": {"enabled": True, "name": "other", "label": "Other"},
            },
        })
        values = [f"value-{index}" for index in range(50)]
        planner.plan(values, {"namespace": "apps"})
        durations = []
        for _ in range(21):
            started = time.perf_counter()
            planner.plan(values, {"namespace": "apps"})
            durations.append((time.perf_counter() - started) * 1000)
        self.assertLess(statistics.median(durations), 100)

    def test_safe_empty_reasons_distinguish_pipeline_stages(self) -> None:
        global_empty = DynamicValuePlanner.from_source({"regex": "^allowed$"}).plan(["other"], {})
        parent_empty = DynamicValuePlanner.from_source({
            "filters_by_parent": [{
                "when": {"namespace": ["apps"]}, "regex": "^allowed$", "mode": "and",
            }],
        }).plan(["other"], {"namespace": "apps"})
        unmatched_empty = DynamicValuePlanner.from_source({
            "grouping": {
                "rules": [{"name": "allowed", "regex": "^allowed$"}],
                "unmatched": {"enabled": False, "name": "ungrouped", "label": "Ungrouped"},
            },
        }).plan(["other"], {})

        self.assertEqual(global_empty.provenance["reason"], "global_filter_empty")
        self.assertEqual(parent_empty.provenance["reason"], "parent_filter_empty")
        self.assertEqual(unmatched_empty.provenance["reason"], "grouping_unmatched_empty")


class TestDynamicGroupingIntegration(unittest.TestCase):
    def test_legacy_static_matrix_hash_filename_and_html_contract(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    service:
                      values: [api, api, worker]
                      regex: "^api$"
                      max_values: 1
        """, [])

        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        artifact = panels[0].artifacts[0]
        artifact["link"] = "panel-api"
        content = render_matrix_dashboard(grafana.config, 600)

        self.assertEqual(len(grafana.render_tasks), 1)
        self.assertEqual(artifact["matrix"]["hash"], "691c3e24")
        self.assertEqual(artifact["png_file"], "Demo__17__matrix-000-691c3e24__0.png")
        self.assertNotIn("groups", artifact["matrix"])
        self.assertIn(">service: api</a>", content)

    def test_legacy_values_by_regex_dedupe_order_and_cap(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    environment: {values: [prod]}
                    service:
                      depends_on: environment
                      values_by:
                        prod: [worker, api, worker, db]
                      regex: "^(api|worker)$"
                      max_values: 2
        """, [])
        grafana.session.get = Mock(return_value=Mock(
            status_code=200,
            json=Mock(return_value={"dashboard": {
                "panels": [{"id": 17, "type": "timeseries", "title": "Requests"}],
            }}),
        ))

        grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["service"] for task in grafana.render_tasks], ["worker", "api"])

    def test_legacy_zip_with_global_regex_is_unchanged(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {combination_mode: zip}
                  variables:
                    namespace: {values: [payments, auth]}
                    pod: {values: [api, worker], regex: "^(api|worker)$"}
        """, [])
        grafana.session.get = Mock(return_value=Mock(
            status_code=200,
            json=Mock(return_value={"dashboard": {
                "panels": [{"id": 17, "type": "timeseries", "title": "Requests"}],
            }}),
        ))

        grafana.get_panels([self.timestamp()])

        self.assertEqual(
            [(task.variables["namespace"], task.variables["pod"]) for task in grafana.render_tasks],
            [("payments", "api"), ("auth", "worker")],
        )

    def test_overlap_creates_distinct_raw_only_tasks_and_synthetic_metadata(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {layout: matrix_grouped_panels}
                  variables:
                    namespace: {values: [payments], hide: false, display_name: Namespace}
                    pod:
                      depends_on: namespace
                      hide: false
                      display_name: Pod
                      value_aliases: {payments-api-abc: "Shared Alias"}
                      values_from:
                        filters_by_parent:
                          - when: {namespace: payments}
                            regex: "^payments-"
                        grouping:
                          dimension: {key: service, display_name: Service}
                          rules:
                            - {name: api, label: API, regex: api}
                            - {name: critical, label: Critical, regex: api}
        """, ["payments-api-abc"])

        panels = grafana.get_panels([self.timestamp()])
        tasks = grafana.render_tasks

        self.assertEqual(len(tasks), 2)
        self.assertEqual([task.variables for task in tasks], [
            {"namespace": "payments", "pod": "payments-api-abc"},
            {"namespace": "payments", "pod": "payments-api-abc"},
        ])
        self.assertEqual(len({task.file_name for task in tasks}), 2)
        matrices = [artifact["matrix"] for artifact in panels[0].artifacts]
        self.assertEqual(len({matrix["hash"] for matrix in matrices}), 2)
        self.assertEqual([matrix["groups"][0]["id"] for matrix in matrices], ["named:api", "named:critical"])
        self.assertEqual([item["key"] for item in matrices[0]["context_path"]], ["namespace", "service", "pod"])
        self.assertEqual(matrices[0]["context_path"][-1]["display_value"], "Shared Alias")
        synthetic = matrices[0]["context_path"][1]
        self.assertIsNone(synthetic["grafana_variable"])
        self.assertTrue(synthetic["synthetic"])
        self.assertEqual(synthetic["source_variable"], "pod")
        self.assertEqual(matrices[0]["variables"], {
            "Namespace": "payments", "Pod": "Shared Alias",
        })
        self.assertEqual(set(matrices[0]["raw_variables"]), {"namespace", "pod"})
        self.assertEqual(set(matrices[0]["grafana_variables"]), {"namespace", "pod"})
        self.assertIn("Service: API", matrices[0]["label"])
        self.assertNotIn("service", matrices[0]["grafana_variables"])
        self.assertEqual(grafana.session.get.call_count, 2)

    def test_filtering_and_capture_preserve_group_major_order_and_safe_counts(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        regex: "^payments-"
                        filters_by_parent:
                          - when: {namespace: payments}
                            regex: "-(api|worker)-"
                        grouping:
                          capture:
                            regex: "^payments-(?P<service>api|worker)-"
                            group: service
                            value_aliases: {api: API}
        """, ["db", "payments-api-a", "payments-worker-a", "payments-api-b"])

        panels = grafana.get_panels([self.timestamp()])

        matrices = [artifact["matrix"] for artifact in panels[0].artifacts]
        self.assertEqual(
            [matrix["raw_variables"]["pod"] for matrix in matrices],
            ["payments-api-a", "payments-api-b", "payments-worker-a"],
        )
        self.assertEqual([matrix["groups"][0]["id"] for matrix in matrices], [
            "capture:api", "capture:api", "capture:worker",
        ])
        counts = matrices[0]["discovery"]["pod"]
        self.assertEqual((counts["discovered"], counts["after_global"], counts["after_parent"]), (4, 3, 3))

    def test_capture_consolidates_distinct_raw_values_into_one_real_group_branch(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments], hide: false}
                    pod:
                      depends_on: namespace
                      hide: false
                      values_from:
                        grouping:
                          dimension: {key: service, display_name: Service}
                          capture:
                            regex: "^(?P<service>checkout)-"
                            group: service
        """, ["checkout-replica-a", "checkout-replica-b"])

        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        for artifact in panels[0].artifacts:
            artifact["link"] = "panel-" + artifact["matrix"]["raw_variables"]["pod"]
        content = render_matrix_dashboard(grafana.config, 600)

        matrices = [artifact["matrix"] for artifact in panels[0].artifacts]
        self.assertEqual([matrix["groups"][0]["id"] for matrix in matrices], [
            "capture:checkout", "capture:checkout",
        ])
        self.assertEqual(content.count("<h3>Service: checkout</h3>"), 1)
        self.assertIn("pod: checkout-replica-a", content)
        self.assertIn("pod: checkout-replica-b", content)

    def test_raw_values_are_logged_but_config_secrets_stay_absent(self) -> None:
        canaries = (
            "RAW-POD-CANARY", "CAPTURE-VALUE-CANARY", "REGEX-BODY-CANARY",
            "CREDENTIAL-CANARY", "TOKEN-CANARY", "QUERY-CANARY",
        )
        dashboard = self.dashboard()
        dashboard["templating"]["list"][0]["query"] = (
            'label_values(up{credential="CREDENTIAL-CANARY", token="TOKEN-CANARY", '
            'query="QUERY-CANARY"}, pod)'
        )
        config = """
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {max_rows: 1}
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        regex: "(?#REGEX-BODY-CANARY)^CAPTURE-VALUE-CANARY-"
                        grouping:
                          rules: [{name: raw, regex: RAW-POD-CANARY}]
                          capture:
                            regex: "^(?P<service>CAPTURE-VALUE-CANARY)-"
                            group: service
        """
        grafana = self.manager(config, ["CAPTURE-VALUE-CANARY-RAW-POD-CANARY"])
        response = Mock(status_code=200, json=Mock(return_value={
            "status": "success", "data": [{"pod": "CAPTURE-VALUE-CANARY-RAW-POD-CANARY"}],
        }))
        grafana.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})), response,
        ])

        with self.assertLogs("grafconflux._grafana", level="INFO") as logs:
            with self.assertRaises(ConfigurationError) as captured:
                grafana.get_panels([self.timestamp()])

        public = "\n".join(logs.output) + str(captured.exception)
        self.assertIn("RAW-POD-CANARY", public)
        self.assertIn("CAPTURE-VALUE-CANARY", public)
        for canary in ("REGEX-BODY-CANARY", "CREDENTIAL-CANARY", "TOKEN-CANARY", "QUERY-CANARY"):
            self.assertNotIn(canary, public)

        success = self.manager(config.replace("max_rows: 1", "max_rows: 2"), [
            "CAPTURE-VALUE-CANARY-RAW-POD-CANARY",
        ])
        success.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})), response,
        ])
        with self.assertLogs("grafconflux._grafana.matrix_discovery", level="INFO") as success_logs:
            panels = success.get_panels([self.timestamp()])
        provenance = repr(panels[0].artifacts[0]["matrix"]["discovery"])
        success_output = "\n".join(success_logs.output)
        self.assertIn("RAW-POD-CANARY", success_output)
        self.assertIn("CAPTURE-VALUE-CANARY", success_output)
        for canary in ("REGEX-BODY-CANARY", "CREDENTIAL-CANARY", "TOKEN-CANARY", "QUERY-CANARY"):
            self.assertNotIn(canary, success_output)
        for canary in canaries:
            self.assertNotIn(canary, provenance)

    def test_same_raw_value_in_different_parents_keeps_distinct_technical_identity(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments, auth]}
                    pod:
                        depends_on: namespace
                        values_from:
                          grouping:
                            rules:
                              - name: api
                                regex: [api, "^unused$"]
        """, ["api"])
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard()}))
        values_response = Mock(status_code=200, json=Mock(return_value={
            "status": "success", "data": [{"pod": "api"}],
        }))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response, values_response])

        panels = grafana.get_panels([self.timestamp()])

        matrices = [artifact["matrix"] for artifact in panels[0].artifacts]
        self.assertEqual([matrix["raw_variables"]["pod"] for matrix in matrices], ["api", "api"])
        self.assertEqual(
            [matrix["raw_variables"]["namespace"] for matrix in matrices], ["payments", "auth"],
        )
        self.assertEqual(len({matrix["hash"] for matrix in matrices}), 2)

    def test_group_overlap_enforces_max_rows_before_render(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {max_rows: 1}
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules:
                            - {name: one, regex: api}
                            - {name: two, regex: api}
        """, ["api"])

        with self.assertRaisesRegex(ConfigurationError, "expansion produced 2 rows"):
            grafana.get_panels([self.timestamp()])
        self.assertEqual(grafana.session.get.call_count, 2)

    def test_failure_after_discovery_closes_fallback_before_matrix_task_creation(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {max_rows: 1}
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules:
                            - {name: one, regex: api}
                            - {name: two, regex: api}
        """, ["api"])
        fallback = Mock()
        append_matrix = GrafanaManager._build_panels_and_tasks.__globals__["append_matrix_tasks"]
        fallback_factory = Mock(return_value=fallback)

        with patch.dict(append_matrix.__globals__, {"BrowserMatrixFallback": fallback_factory}):
            with self.assertRaisesRegex(ConfigurationError, "expansion produced 2 rows"):
                grafana.get_panels([self.timestamp()])

        fallback_factory.assert_called_once()
        fallback.close.assert_called_once_with()
        self.assertFalse(any(
            (task.artifact or {}).get("artifact_type") == "matrix"
            for task in grafana.render_tasks
        ))

    def test_max_values_applies_before_overlap_duplication(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  options: {max_rows: 2}
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        max_values: 1
                        grouping:
                          rules:
                            - {name: one, regex: api}
                            - {name: two, regex: api}
        """, ["api", "other"])

        grafana.get_panels([self.timestamp()])

        self.assertEqual(len(grafana.render_tasks), 2)
        self.assertTrue(all(task.variables["pod"] == "api" for task in grafana.render_tasks))

    def test_all_filtered_rows_fail_with_safe_reason(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [RAW-NAMESPACE-CANARY]}
                    pod:
                      depends_on: namespace
                      values_from:
                        filters_by_parent:
                          - when: {namespace: RAW-NAMESPACE-CANARY}
                            regex: "^allowed$"
        """, ["sensitive-raw-canary"])

        with self.assertLogs("grafconflux._grafana", level="INFO") as logs:
            with self.assertRaisesRegex(ConfigurationError, "reason=filtered_or_unmatched_empty") as captured:
                grafana.get_panels([self.timestamp()])
        output = "\n".join(logs.output) + str(captured.exception)
        self.assertIn("sensitive-raw-canary", output)
        self.assertIn("RAW-NAMESPACE-CANARY", output)
        self.assertIn("parent_filter_empty", output)

    def test_successful_dynamic_planning_logs_values_after_filtering(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        filters_by_parent:
                          - when: {namespace: payments}
                            regex: ["^api-", "^worker-"]
        """, ["api-one", "worker-one", "other-one"])

        with self.assertLogs("grafconflux._grafana", level="INFO") as logs:
            grafana.get_panels([self.timestamp()])

        output = "\n".join(logs.output)
        self.assertIn(
            "matrix_discovery variable=pod time=smoke context={'namespace': 'payments'} count=3 "
            "values=['api-one', 'worker-one', 'other-one']",
            output,
        )
        self.assertIn(
            "matrix_filtered variable=pod time=smoke context={'namespace': 'payments'} "
            "count=2 values=['api-one', 'worker-one']",
            output,
        )
        self.assertNotIn("matrix_planning", output)

    def test_dynamic_planning_log_reports_nonzero_unmatched_count(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules: [{name: api, regex: "^api-"}]
        """, ["api-one", "worker-one"])

        with self.assertLogs("grafconflux._grafana.matrix", level="INFO") as logs:
            grafana.get_panels([self.timestamp()])

        self.assertIn(
            "matrix_filtered variable=pod time=smoke context={'namespace': 'payments'} "
            "count=2 values=['api-one', 'worker-one']",
            "\n".join(logs.output),
        )

    def test_panel_variant_cannot_override_grouped_variable(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules: [{name: api, regex: api}]
                panel_variants:
                  - selectors: {panel_id: 17}
                    variables:
                      pod: {values: [other]}
        """, ["api"])

        with self.assertRaisesRegex(ConfigurationError, r"panel_variants\[0\].variables.pod"):
            grafana.get_panels([self.timestamp()])
        self.assertEqual(grafana.session.get.call_count, 1)

    def test_panel_variant_for_other_variable_preserves_group_snapshot(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules: [{name: api, regex: api}]
                panel_variants:
                  - selectors: {panel_id: 17}
                    variables:
                      region: {values: [west]}
        """, ["api"])

        panels = grafana.get_panels([self.timestamp()])

        variant_matrix = panels[0].artifacts[1]["matrix"]
        self.assertEqual(variant_matrix["groups"][0]["id"], "named:api")
        self.assertEqual(set(variant_matrix["raw_variables"]), {"namespace", "pod"})
        self.assertNotIn("pod_group", variant_matrix["grafana_variables"])
        self.assertEqual(variant_matrix["grafana_variables"]["region"], "west")

    def test_inferred_dependency_supports_parent_filter(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      values_from:
                        filters_by_parent:
                          - when: {namespace: payments}
                            regex: "^payments-"
        """, ["payments-api", "other"])

        grafana.get_panels([self.timestamp()])

        self.assertEqual([task.variables["pod"] for task in grafana.render_tasks], ["payments-api"])

    def test_dynamic_filter_skips_only_the_empty_parent_branch(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments, auth]}
                    pod:
                      depends_on: namespace
                      values_from:
                        filters_by_parent:
                          - when: {namespace: payments}
                            regex: "^payments-"
                          - when: {namespace: auth}
                            regex: "^auth-"
        """, ["payments-api"])
        dashboard_response = Mock(status_code=200, json=Mock(return_value={"dashboard": self.dashboard()}))
        values_response = Mock(status_code=200, json=Mock(return_value={
            "status": "success", "data": [{"pod": "payments-api"}],
        }))
        grafana.session.get = Mock(side_effect=[dashboard_response, values_response, values_response])

        with self.assertLogs("grafconflux._grafana.matrix", level="WARNING"):
            grafana.get_panels([self.timestamp()])

        self.assertEqual(
            [(task.variables["namespace"], task.variables["pod"]) for task in grafana.render_tasks],
            [("payments", "payments-api")],
        )

    def test_custom_unmatched_group_survives_metadata_and_display(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments], hide: false}
                    pod:
                      depends_on: namespace
                      hide: false
                      values_from:
                        grouping:
                          rules: [{name: api, regex: "^api$"}]
                          unmatched: {enabled: true, name: other, label: Other workloads}
        """, ["worker"])

        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        artifact = panels[0].artifacts[0]
        artifact["link"] = "panel-worker"
        content = render_matrix_dashboard(grafana.config, 600)

        self.assertEqual(artifact["matrix"]["groups"][0]["id"], "unmatched:other")
        self.assertEqual(artifact["matrix"]["groups"][0]["display_value"], "Other workloads")
        self.assertIn("pod Group: Other workloads", content)

    def test_default_enabled_unmatched_group_is_emitted_once(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules: [{name: api, regex: "^api$"}]
                          unmatched: {enabled: true}
        """, ["worker"])

        panels = grafana.get_panels([self.timestamp()])
        groups = panels[0].artifacts[0]["matrix"]["groups"]

        self.assertEqual(len(grafana.render_tasks), 1)
        self.assertEqual(groups[0]["id"], "unmatched:ungrouped")
        self.assertEqual(groups[0]["display_value"], "Ungrouped")

    def test_default_unmatched_skip_matches_byte_fixture_and_excludes_canary(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments], hide: false}
                    pod:
                      depends_on: namespace
                      hide: false
                      values_from:
                        grouping:
                          rules: [{name: api, label: API, regex: "^api$"}]
        """, ["api", "UNMATCHED-CANARY"])
        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        panels[0].artifacts[0]["link"] = "panel-api"

        content = render_matrix_dashboard(grafana.config, 600)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "confluence",
            "matrix_dynamic_grouping_unmatched_disabled.xml",
        )
        with open(fixture_path, encoding="utf-8") as fixture:
            expected = fixture.read()

        self.assertEqual(content, expected)
        self.assertNotIn("UNMATCHED-CANARY", content)

    def test_grouping_does_not_add_discovery_calls(self) -> None:
        grouped = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules: [{name: api, regex: api}]
        """, ["api"])
        ungrouped = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod: {depends_on: namespace, values_from: {}}
        """, ["api"])

        grouped.get_panels([self.timestamp()])
        ungrouped.get_panels([self.timestamp()])

        self.assertEqual(grouped.session.get.call_count, ungrouped.session.get.call_count)

    def test_render_api_and_playwright_modes_plan_identical_raw_variables(self) -> None:
        managers = [
            self.manager(f"""
                dashboards:
                  Demo:
                    grafana_url: https://grafana.example
                    dash_title: Demo
                    render: {str(render).lower()}
                    render_matrix:
                      variables:
                        namespace: {{values: [payments]}}
                        pod:
                          depends_on: namespace
                          values_from:
                            grouping:
                              rules: [{{name: api, regex: api}}]
            """, ["api"])
            for render in (True, False)
        ]

        for manager in managers:
            manager.get_panels([self.timestamp()])

        self.assertEqual(managers[0].render_tasks[0].variables, managers[1].render_tasks[0].variables)

    def test_download_metadata_save_reload_replays_a_and_b_without_discovery(self) -> None:
        for layout in ("matrix_grouped_panels", "matrix_values_first"):
            with self.subTest(layout=layout), tempfile.TemporaryDirectory() as metadata_dir:
                grafana = self.manager(f"""
                    dashboards:
                      Demo:
                        grafana_url: https://grafana.example
                        dash_title: Demo
                        render_matrix:
                          options: {{layout: {layout}}}
                          variables:
                            namespace: {{values: [payments], hide: false}}
                            pod:
                              depends_on: namespace
                              hide: false
                              values_from:
                                regex: ["^checkout-", "^unused-"]
                                grouping:
                                  capture:
                                    regex: "^(?P<service>checkout)-"
                                    group: service
                """, ["checkout-a", "checkout-b"])
                timestamp = self.timestamp()
                panels = grafana.get_panels([timestamp])
                grafana.config.panels = panels
                grafana.config.full_links = []
                grafana.config.matrix_dashboard_links = []
                for artifact in panels[0].artifacts:
                    artifact["link"] = "panel-" + artifact["matrix"]["raw_variables"]["pod"]
                before = render_matrix_dashboard(grafana.config, 600)
                grafana.charts_path = os.path.join(metadata_dir, "Demo")
                grafana._GrafanaManager__save_params_to_file([timestamp], metadata_dir)

                with open(os.path.join(metadata_dir, "Demo.yaml"), encoding="utf-8") as metadata_file:
                    saved = yaml.safe_load(metadata_file)
                with patch(
                    "grafconflux._grafana.matrix_discovery.MatrixValueResolver.resolve",
                    side_effect=AssertionError("replay attempted discovery"),
                ) as replay_discovery:
                    uploader = GrafanaConfigUploader("Demo", saved)
                    after = render_matrix_dashboard(uploader, 600)

                replay_discovery.assert_not_called()
                self.assertEqual(after, before)
                matrix = uploader.panels[0].artifacts[0]["matrix"]
                self.assertEqual(matrix["groups"][0]["id"], "capture:checkout")
                self.assertTrue(matrix["context_path"][1]["synthetic"])
                self.assertEqual(set(matrix["variables"]), {"namespace", "pod"})
                self.assertEqual(set(matrix["raw_variables"]), {"namespace", "pod"})
                self.assertEqual(set(matrix["grafana_variables"]), {"namespace", "pod"})

    def test_hidden_group_and_raw_value_use_neutral_confluence_labels(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments], hide: false}
                    pod:
                      depends_on: namespace
                      hide: true
                      values_from:
                        grouping:
                          dimension: {key: service, display_name: Service, hide: true}
                          rules:
                            - {name: private, label: "<Private & Core>", regex: private}
        """, ["sensitive-private-pod"])
        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        artifact = panels[0].artifacts[0]
        artifact["link"] = "panel"
        grafana.config.matrix_dashboard_links = [{
            "url": "dashboard",
            "label": artifact["matrix"]["neutral_label"],
            "context_path": artifact["matrix"]["context_path"],
            "timestamp_id": 0,
        }]

        content = render_matrix_dashboard(grafana.config, 600)

        self.assertIn("Group 1", content)
        self.assertIn("Variant 1", content)
        self.assertNotIn("sensitive-private-pod", content)
        self.assertNotIn("Private", content)

    def test_visible_group_label_is_html_escaped(self) -> None:
        grafana = self.manager("""
            dashboards:
              Demo:
                grafana_url: https://grafana.example
                dash_title: Demo
                render_matrix:
                  variables:
                    namespace: {values: [payments]}
                    pod:
                      depends_on: namespace
                      values_from:
                        grouping:
                          rules:
                            - {name: api, label: "<API & Core>", regex: api}
        """, ["api"])
        panels = grafana.get_panels([self.timestamp()])
        grafana.config.panels = panels
        panels[0].artifacts[0]["link"] = "panel"

        content = render_matrix_dashboard(grafana.config, 600)

        self.assertIn("&lt;API &amp; Core&gt;", content)
        self.assertNotIn("<API & Core>", content)

    def manager(self, content: str, discovered: list[str]) -> GrafanaManager:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = os.path.join(temp_dir.name, "config.yaml")
        with open(path, "w", encoding="utf-8") as config_file:
            config_file.write(textwrap.dedent(content))
        config = GrafanaManager.load_grafana_config(path)[0]
        manager = GrafanaManager(config)
        manager.dashboard_uid = "uid"
        manager.dashboard_url = "/d/uid/demo"
        dashboard = self.dashboard()
        manager.session.get = Mock(side_effect=[
            Mock(status_code=200, json=Mock(return_value={"dashboard": dashboard})),
            Mock(status_code=200, json=Mock(return_value={
                "status": "success", "data": [{"pod": value} for value in discovered],
            })),
        ])
        return manager

    @staticmethod
    def timestamp() -> GrafanaTimeDownloader:
        return GrafanaTimeDownloader("smoke__&from=1700000000&to=1700003600", 0, "UTC")

    @staticmethod
    def dashboard() -> dict:
        return {
            "panels": [{"id": 17, "type": "timeseries", "title": "Requests"}],
            "templating": {"list": [{
                "name": "pod",
                "type": "query",
                "datasource": {"type": "prometheus", "uid": "prom"},
                "query": 'label_values(kube_pod_info{namespace="$namespace"}, pod)',
            }]},
        }


if __name__ == "__main__":
    unittest.main()
