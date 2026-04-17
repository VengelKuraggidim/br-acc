"""Catalog-integrity tests for bracc.models.pattern and friends.

These tests guard against typos / missing entries in the pattern metadata
catalog that would silently degrade UX (fallback to showing a raw
pattern_id rather than the localized name/description).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from bracc.models.entity import SourceAttribution
from bracc.models.pattern import PATTERN_METADATA, PatternResponse, PatternResult
from bracc.services.intelligence_provider import (
    COMMUNITY_PATTERN_IDS,
    COMMUNITY_PATTERN_QUERIES,
)

REQUIRED_KEYS = {"name_pt", "name_en", "desc_pt", "desc_en"}
SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*[a-z0-9]$")


class TestPatternMetadataCatalog:
    def test_not_empty(self) -> None:
        assert len(PATTERN_METADATA) >= 1

    @pytest.mark.parametrize("pattern_id", sorted(PATTERN_METADATA))
    def test_entry_has_all_required_keys(self, pattern_id: str) -> None:
        entry = PATTERN_METADATA[pattern_id]
        missing = REQUIRED_KEYS - entry.keys()
        assert not missing, (
            f"{pattern_id} missing keys: {sorted(missing)}"
        )

    @pytest.mark.parametrize("pattern_id", sorted(PATTERN_METADATA))
    def test_entry_values_non_empty(self, pattern_id: str) -> None:
        entry = PATTERN_METADATA[pattern_id]
        for key in REQUIRED_KEYS:
            value = entry.get(key, "")
            assert value.strip(), f"{pattern_id}.{key} is empty/whitespace"

    @pytest.mark.parametrize("pattern_id", sorted(PATTERN_METADATA))
    def test_pattern_ids_are_snake_case(self, pattern_id: str) -> None:
        assert SNAKE_CASE_RE.match(pattern_id), (
            f"pattern_id must be snake_case, got: {pattern_id!r}"
        )

    def test_no_duplicate_translations(self) -> None:
        # Catch copy-paste bugs where someone duplicates an adjacent pattern's
        # strings into a new entry. Each localized name should be unique.
        pt_names = [entry["name_pt"] for entry in PATTERN_METADATA.values()]
        en_names = [entry["name_en"] for entry in PATTERN_METADATA.values()]
        assert len(set(pt_names)) == len(pt_names), "Duplicate name_pt detected"
        assert len(set(en_names)) == len(en_names), "Duplicate name_en detected"


class TestCommunityPatternRegistry:
    def test_all_community_ids_have_metadata(self) -> None:
        missing = set(COMMUNITY_PATTERN_IDS) - set(PATTERN_METADATA.keys())
        assert not missing, (
            f"COMMUNITY_PATTERN_IDS reference missing metadata entries: "
            f"{sorted(missing)}"
        )

    def test_all_community_ids_have_query_mapping(self) -> None:
        missing = set(COMMUNITY_PATTERN_IDS) - set(COMMUNITY_PATTERN_QUERIES.keys())
        assert not missing, (
            f"COMMUNITY_PATTERN_IDS missing from COMMUNITY_PATTERN_QUERIES: "
            f"{sorted(missing)}"
        )

    def test_query_mapping_has_no_extra_entries(self) -> None:
        extra = set(COMMUNITY_PATTERN_QUERIES.keys()) - set(COMMUNITY_PATTERN_IDS)
        assert not extra, (
            f"COMMUNITY_PATTERN_QUERIES has IDs not in COMMUNITY_PATTERN_IDS: "
            f"{sorted(extra)}"
        )

    def test_query_files_exist(self) -> None:
        # Every value in COMMUNITY_PATTERN_QUERIES must correspond to a
        # shipping .cypher file under api/src/bracc/queries/.
        queries_dir = Path(__file__).resolve().parents[2] / "src" / "bracc" / "queries"
        assert queries_dir.is_dir(), f"queries dir missing: {queries_dir}"
        missing: list[str] = []
        for pattern_id, query_name in COMMUNITY_PATTERN_QUERIES.items():
            if not (queries_dir / f"{query_name}.cypher").exists():
                missing.append(f"{pattern_id} -> {query_name}.cypher")
        assert not missing, f"Missing Cypher files: {missing}"


class TestPatternResult:
    def test_minimal_construction(self) -> None:
        pr = PatternResult(
            pattern_id="sanctioned_still_receiving",
            pattern_name="Co-occurrence: sanction and contract",
            description="Contract within sanction window",
            data={"contract_value": 100000.0},
            entity_ids=["cnpj:12345678000195"],
            sources=[],
        )
        assert pr.exposure_tier == "public_safe"
        assert pr.intelligence_tier == "community"
        assert pr.entity_ids == ["cnpj:12345678000195"]

    def test_tier_overrides_apply(self) -> None:
        pr = PatternResult(
            pattern_id="x",
            pattern_name="X",
            description="x",
            data={},
            entity_ids=[],
            sources=[],
            exposure_tier="restricted",
            intelligence_tier="institutional",
        )
        assert pr.exposure_tier == "restricted"
        assert pr.intelligence_tier == "institutional"

    def test_data_accepts_mixed_value_types(self) -> None:
        pr = PatternResult(
            pattern_id="x",
            pattern_name="X",
            description="x",
            data={
                "str_field": "value",
                "int_field": 42,
                "float_field": 3.14,
                "bool_field": True,
                "none_field": None,
                "list_field": ["a", "b", "c"],
            },
            entity_ids=[],
            sources=[],
        )
        assert pr.data["int_field"] == 42
        assert pr.data["list_field"] == ["a", "b", "c"]

    def test_sources_accept_source_attribution(self) -> None:
        pr = PatternResult(
            pattern_id="x",
            pattern_name="X",
            description="x",
            data={},
            entity_ids=[],
            sources=[
                SourceAttribution(database="cnpj", record_id="12345"),
            ],
        )
        assert len(pr.sources) == 1
        assert pr.sources[0].database == "cnpj"
        assert pr.sources[0].record_id == "12345"


class TestPatternResponse:
    def test_empty_response(self) -> None:
        resp = PatternResponse(entity_id=None, patterns=[], total=0)
        assert resp.patterns == []
        assert resp.total == 0

    def test_roundtrip_with_entity(self) -> None:
        inner = PatternResult(
            pattern_id="sanctioned_still_receiving",
            pattern_name="n",
            description="d",
            data={},
            entity_ids=["cnpj:1"],
            sources=[],
        )
        resp = PatternResponse(entity_id="cnpj:1", patterns=[inner], total=1)
        dumped = resp.model_dump()
        restored = PatternResponse.model_validate(dumped)
        assert restored.total == 1
        assert restored.patterns[0].pattern_id == "sanctioned_still_receiving"
