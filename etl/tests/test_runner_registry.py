"""Invariants for the PIPELINES registry in bracc_etl.runner.

These tests catch typos where a pipeline's class-level `name` or
`source_id` drifts from the key under which it was registered — the
kind of bug that only surfaces at runtime when a data load starts
tagging records with an unexpected source_id.
"""

from __future__ import annotations

import pytest

from bracc_etl.base import Pipeline
from bracc_etl.runner import PIPELINES


class TestPipelinesRegistry:
    def test_registry_not_empty(self) -> None:
        assert len(PIPELINES) > 0

    @pytest.mark.parametrize("key", sorted(PIPELINES))
    def test_all_entries_subclass_pipeline(self, key: str) -> None:
        cls = PIPELINES[key]
        assert issubclass(cls, Pipeline), (
            f"PIPELINES[{key!r}] = {cls!r} must inherit from Pipeline"
        )

    @pytest.mark.parametrize("key", sorted(PIPELINES))
    def test_registry_key_matches_pipeline_name(self, key: str) -> None:
        cls = PIPELINES[key]
        name = getattr(cls, "name", None)
        assert name == key, (
            f"PIPELINES[{key!r}].name is {name!r}, expected {key!r}"
        )

    @pytest.mark.parametrize("key", sorted(PIPELINES))
    def test_pipeline_has_source_id(self, key: str) -> None:
        cls = PIPELINES[key]
        assert hasattr(cls, "source_id"), (
            f"PIPELINES[{key!r}].source_id attribute missing"
        )
        assert cls.source_id, f"PIPELINES[{key!r}].source_id is empty"

    def test_no_duplicate_pipeline_classes(self) -> None:
        # Two keys pointing at the same class would indicate an alias
        # that breaks the "name == key" invariant above.
        classes = list(PIPELINES.values())
        assert len(set(classes)) == len(classes), "Duplicate pipeline classes in PIPELINES"

    def test_keys_are_snake_case(self) -> None:
        # Pipeline keys flow into the source registry CSV and CLI args;
        # enforce consistent snake_case identifiers.
        import re

        snake_re = re.compile(r"^[a-z][a-z0-9_]*[a-z0-9]$")
        bad = [key for key in PIPELINES if not snake_re.match(key)]
        assert not bad, f"Non-snake_case pipeline keys: {bad}"
