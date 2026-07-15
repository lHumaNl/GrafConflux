"""Pure filtering and membership planning for dynamic matrix values."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


PatternSet = tuple[re.Pattern[str], ...]


@dataclass(frozen=True)
class GroupMembership:
    """One presentation-only membership for a raw dynamic value."""

    identity: str
    display_value: str
    origin: str
    name: str


@dataclass(frozen=True)
class DynamicOccurrence:
    """One render occurrence produced from a raw value and membership."""

    value: str
    membership: GroupMembership


@dataclass(frozen=True)
class DynamicPlanResult:
    """Filtered raw values, ordered occurrences, and safe counters."""

    values: list[str]
    occurrences: list[DynamicOccurrence]
    provenance: dict[str, Any]


@dataclass(frozen=True)
class _ParentFilter:
    when: dict[str, tuple[str, ...]]
    patterns: PatternSet
    override_global: bool


@dataclass(frozen=True)
class _NamedRule:
    name: str
    label: str
    patterns: PatternSet
    when: dict[str, tuple[str, ...]] | None


@dataclass(frozen=True)
class _CaptureRule:
    pattern: re.Pattern[str]
    group: str | int
    when: dict[str, tuple[str, ...]] | None
    aliases: dict[str, str]


class DynamicValuePlanner:
    """Compiled, reusable planner for one normalized ``values_from`` mapping."""

    def __init__(
        self,
        global_patterns: PatternSet,
        max_values: int,
        parent_filters: tuple[_ParentFilter, ...],
        named_rules: tuple[_NamedRule, ...],
        capture: _CaptureRule | None,
        unmatched: GroupMembership | None,
        grouping: bool,
    ) -> None:
        self.global_patterns = global_patterns
        self.max_values = max_values
        self.parent_filters = parent_filters
        self.named_rules = named_rules
        self.capture = capture
        self.unmatched = unmatched
        self.grouping = grouping

    @classmethod
    def from_source(
        cls,
        source: dict[str, Any],
        compiled: dict[str, Any] | None = None,
    ) -> "DynamicValuePlanner":
        compiled = compiled or {}
        grouping = source.get("grouping")
        grouping_config = grouping if isinstance(grouping, dict) else {}
        unmatched_config = grouping_config.get("unmatched") or {}
        unmatched = None
        if unmatched_config.get("enabled") is True:
            unmatched = GroupMembership(
                f"unmatched:{unmatched_config.get('name', 'ungrouped')}",
                str(unmatched_config.get("label", "Ungrouped")),
                "unmatched",
                str(unmatched_config.get("name", "ungrouped")),
            )
        return cls(
            _pattern_set(compiled["global"])
            if "global" in compiled else _compile_pattern_set(source.get("regex")),
            int(source.get("max_values", 50)),
            tuple(
                _compiled_parent_filter(item, _pattern_set_at(compiled.get("parents"), index))
                for index, item in enumerate(source.get("filters_by_parent") or [])
            ),
            tuple(
                _compiled_named_rule(item, _pattern_set_at(compiled.get("named"), index))
                for index, item in enumerate(grouping_config.get("rules") or [])
            ),
            _compiled_capture(grouping_config.get("capture"), compiled.get("capture")),
            unmatched,
            isinstance(grouping, dict),
        )

    def plan(self, discovered_values: list[str], parent_context: dict[str, Any]) -> DynamicPlanResult:
        """Apply the normative filter/dedupe/cap/membership pipeline."""
        values = [str(value) for value in discovered_values]
        matching_filters = [item for item in self.parent_filters if _condition_matches(item.when, parent_context)]
        overridden = any(item.override_global for item in matching_filters)
        after_global = (
            values
            if overridden or not self.global_patterns
            else [value for value in values if _matches_any(self.global_patterns, value)]
        )
        after_parent = [
            value
            for value in after_global
            if all(_matches_any(item.patterns, value) for item in matching_filters)
        ]
        unique_capped = list(dict.fromkeys(after_parent))[:self.max_values]
        occurrences, unmatched_count = self._occurrences(unique_capped, parent_context)
        provenance = {
            "discovered": len(values),
            "after_global": len(after_global),
            "after_parent": len(after_parent),
            "unique_capped": len(unique_capped),
            "memberships": len(occurrences),
            "unmatched": unmatched_count,
            "emitted_rows": len(occurrences) if self.grouping else len(unique_capped),
            "matched_parent_filters": len(matching_filters),
            "global_overridden": overridden,
            "reason": _empty_reason(values, after_global, after_parent, unique_capped, occurrences, self.grouping),
        }
        return DynamicPlanResult(unique_capped, occurrences, provenance)

    def _occurrences(
        self,
        values: list[str],
        parent_context: dict[str, Any],
    ) -> tuple[list[DynamicOccurrence], int]:
        if not self.grouping:
            return [], 0
        named_buckets: list[list[DynamicOccurrence]] = [[] for _ in self.named_rules]
        capture_buckets: dict[str, list[DynamicOccurrence]] = {}
        unmatched_occurrences: list[DynamicOccurrence] = []
        unmatched_count = 0
        for value in values:
            membership_found = False
            for index, rule in enumerate(self.named_rules):
                if _optional_condition_matches(rule.when, parent_context) and _matches_any(rule.patterns, value):
                    membership_found = True
                    membership = GroupMembership(
                        f"named:{rule.name}", rule.label, "named_rule", rule.name,
                    )
                    named_buckets[index].append(DynamicOccurrence(value, membership))
            capture = self._capture_membership(value, parent_context)
            if capture is not None:
                membership_found = True
                capture_buckets.setdefault(capture.identity, []).append(DynamicOccurrence(value, capture))
            if membership_found:
                continue
            unmatched_count += 1
            if self.unmatched is not None:
                unmatched_occurrences.append(DynamicOccurrence(value, self.unmatched))
        occurrences = [item for bucket in named_buckets for item in bucket]
        occurrences.extend(item for bucket in capture_buckets.values() for item in bucket)
        occurrences.extend(unmatched_occurrences)
        return occurrences, unmatched_count

    def _capture_membership(
        self,
        value: str,
        parent_context: dict[str, Any],
    ) -> GroupMembership | None:
        capture = self.capture
        if capture is None or not _optional_condition_matches(capture.when, parent_context):
            return None
        match = capture.pattern.search(value)
        if match is None:
            return None
        extracted = match.group(capture.group)
        if extracted is None or not extracted.strip():
            return None
        return GroupMembership(
            f"capture:{extracted}", capture.aliases.get(extracted, extracted), "capture", extracted,
        )


def _compiled_parent_filter(item: dict[str, Any], patterns: PatternSet = ()) -> _ParentFilter:
    return _ParentFilter(
        _condition(item["when"]),
        patterns or _compile_pattern_set(item["regex"]),
        item.get("mode", "and") == "override_global",
    )


def _compiled_named_rule(item: dict[str, Any], patterns: PatternSet = ()) -> _NamedRule:
    return _NamedRule(
        str(item["name"]),
        str(item.get("label", item["name"])),
        patterns or _compile_pattern_set(item["regex"]),
        _condition(item["when"]) if "when" in item else None,
    )


def _compiled_capture(item: Any, pattern: re.Pattern[str] | None = None) -> _CaptureRule | None:
    if not isinstance(item, dict):
        return None
    return _CaptureRule(
        pattern or re.compile(item["regex"]),
        item["group"],
        _condition(item["when"]) if "when" in item else None,
        dict(item.get("value_aliases") or {}),
    )


def _compile_pattern_set(value: Any) -> PatternSet:
    if value in (None, ""):
        return ()
    items = value if isinstance(value, list) else [value]
    return tuple(re.compile(item) for item in items)


def _pattern_set(value: Any) -> PatternSet:
    if value is None:
        return ()
    if isinstance(value, re.Pattern):
        return (value,)
    return tuple(value)


def _pattern_set_at(value: Any, index: int) -> PatternSet:
    return _pattern_set(value[index]) if isinstance(value, list) and index < len(value) else ()


def _matches_any(patterns: PatternSet, value: str) -> bool:
    return any(pattern.search(value) for pattern in patterns)


def _condition(value: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    return {key: tuple(str(item) for item in items) for key, items in value.items()}


def _condition_matches(condition: dict[str, tuple[str, ...]], context: dict[str, Any]) -> bool:
    return all(str(context.get(key)) in allowed for key, allowed in condition.items())


def _optional_condition_matches(
    condition: dict[str, tuple[str, ...]] | None,
    context: dict[str, Any],
) -> bool:
    return condition is None or _condition_matches(condition, context)


def _empty_reason(
    discovered: list[str],
    after_global: list[str],
    after_parent: list[str],
    unique_capped: list[str],
    occurrences: list[DynamicOccurrence],
    grouping: bool,
) -> str | None:
    if not discovered:
        return "discovery_empty"
    if not after_global:
        return "global_filter_empty"
    if not after_parent:
        return "parent_filter_empty"
    if grouping and unique_capped and not occurrences:
        return "grouping_unmatched_empty"
    return None
