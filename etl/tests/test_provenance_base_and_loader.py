"""Tests for Pipeline.attach_provenance and the Neo4jBatchLoader
contract enforcement.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from bracc_etl import provenance as provenance_mod
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.provenance import (
    enforce_provenance,
    missing_provenance_fields,
    provenance_mode,
)


class _DummyPipeline(Pipeline):
    name = "dummy"
    source_id = "folha_go"  # exists in docs/source_registry_br_v1.csv

    def extract(self) -> None:  # pragma: no cover
        pass

    def transform(self) -> None:  # pragma: no cover
        pass

    def load(self) -> None:  # pragma: no cover
        pass


class _PipelineWithoutRegistryEntry(Pipeline):
    name = "no_such_source"
    source_id = "no_such_source_xyz"

    def extract(self) -> None:  # pragma: no cover
        pass

    def transform(self) -> None:  # pragma: no cover
        pass

    def load(self) -> None:  # pragma: no cover
        pass


@pytest.fixture(autouse=True)
def _reset_registry_cache() -> None:
    provenance_mod._reset_cache_for_tests()


def _make_pipeline() -> _DummyPipeline:
    return _DummyPipeline(driver=MagicMock())


class TestAttachProvenance:
    def test_adds_five_fields(self) -> None:
        pipe = _make_pipeline()
        stamped = pipe.attach_provenance(
            {"name": "Fulano"},
            record_id="12345",
            record_url="https://example.com/record/12345",
        )
        assert stamped["source_id"] == "folha_go"
        assert stamped["source_record_id"] == "12345"
        assert stamped["source_url"] == "https://example.com/record/12345"
        assert stamped["ingested_at"].startswith("20")
        assert stamped["run_id"].startswith("folha_go_")
        assert stamped["name"] == "Fulano"

    def test_falls_back_to_primary_url_from_registry(self) -> None:
        pipe = _make_pipeline()
        stamped = pipe.attach_provenance({"x": 1}, record_id="42")
        # folha_go has a primary_url in docs/source_registry_br_v1.csv
        assert stamped["source_url"].startswith("http")

    def test_record_id_coerced_to_string(self) -> None:
        pipe = _make_pipeline()
        stamped = pipe.attach_provenance(
            {}, record_id=12345, record_url="https://x.test",
        )
        assert stamped["source_record_id"] == "12345"

    def test_empty_record_id_stays_empty_string(self) -> None:
        pipe = _make_pipeline()
        for rid in (None, ""):
            stamped = pipe.attach_provenance(
                {}, record_id=rid, record_url="https://x.test",
            )
            assert stamped["source_record_id"] == ""

    def test_raises_when_no_valid_url(self) -> None:
        pipe = _PipelineWithoutRegistryEntry(driver=MagicMock())
        with pytest.raises(ValueError, match="no valid source_url"):
            pipe.attach_provenance({}, record_id="1")

    def test_raises_when_record_url_non_http_and_no_fallback(self) -> None:
        pipe = _PipelineWithoutRegistryEntry(driver=MagicMock())
        with pytest.raises(ValueError):
            pipe.attach_provenance({}, record_id="1", record_url="ftp://nope")

    def test_cache_is_per_instance(self) -> None:
        pipe = _make_pipeline()
        url_first = pipe._get_primary_url()
        url_second = pipe._get_primary_url()
        assert url_first == url_second
        assert pipe._primary_url_cache == url_first


class TestMissingProvenanceFields:
    def _valid(self) -> dict[str, Any]:
        return {
            "source_id": "folha_go",
            "source_record_id": "42",
            "source_url": "https://example.com",
            "ingested_at": "2026-04-17T00:00:00+00:00",
            "run_id": "folha_go_20260417",
        }

    def test_valid_row_has_no_missing(self) -> None:
        assert missing_provenance_fields(self._valid()) == []

    def test_empty_source_record_id_is_ok(self) -> None:
        row = self._valid()
        row["source_record_id"] = ""
        assert missing_provenance_fields(row) == []

    @pytest.mark.parametrize(
        "field", ["source_id", "source_url", "ingested_at", "run_id"]
    )
    def test_missing_required_field_flagged(self, field: str) -> None:
        row = self._valid()
        row[field] = ""
        assert field in missing_provenance_fields(row)

    def test_non_http_url_flagged(self) -> None:
        row = self._valid()
        row["source_url"] = "ftp://bad"
        assert "source_url" in missing_provenance_fields(row)


class TestEnforceProvenanceMode:
    def test_off_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "off")
        enforce_provenance([{}], context="t")

    def test_warn_logs_but_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "warn")
        with caplog.at_level(logging.WARNING, logger="bracc_etl.provenance"):
            enforce_provenance([{}], context="test-ctx")
        assert any("test-ctx" in rec.message for rec in caplog.records)

    def test_strict_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "strict")
        with pytest.raises(ValueError, match="violate contract"):
            enforce_provenance([{}], context="t")

    def test_unknown_defaults_to_warn(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "bananas")
        assert provenance_mode() == "warn"


class TestLoaderIntegration:
    def _make_loader(self) -> tuple[Neo4jBatchLoader, MagicMock]:
        driver = MagicMock()
        session = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)
        return Neo4jBatchLoader(driver), session

    def _stamped_row(self, **extra: Any) -> dict[str, Any]:
        return {
            "source_id": "folha_go",
            "source_record_id": "abc",
            "source_url": "https://example.com",
            "ingested_at": "2026-04-17T00:00:00+00:00",
            "run_id": "folha_go_1",
            **extra,
        }

    def test_load_nodes_accepts_stamped_rows(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "strict")
        loader, session = self._make_loader()
        rows = [self._stamped_row(cnpj="1", name="A")]
        count = loader.load_nodes("Company", rows, "cnpj")
        assert count == 1
        session.run.assert_called_once()

    def test_load_nodes_strict_rejects_unstamped(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "strict")
        loader, _ = self._make_loader()
        with pytest.raises(ValueError, match="nodes:Company"):
            loader.load_nodes(
                "Company", [{"cnpj": "1", "name": "A"}], "cnpj",
            )

    def test_load_nodes_warn_allows_unstamped(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "warn")
        loader, session = self._make_loader()
        with caplog.at_level(logging.WARNING, logger="bracc_etl.provenance"):
            count = loader.load_nodes(
                "Company", [{"cnpj": "1", "name": "A"}], "cnpj",
            )
        assert count == 1
        session.run.assert_called_once()
        assert any("nodes:Company" in rec.message for rec in caplog.records)

    def test_load_relationships_auto_propagates_provenance_props(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "strict")
        loader, session = self._make_loader()
        rows = [
            self._stamped_row(source_key="1", target_key="2", value=100),
        ]
        count = loader.load_relationships(
            "REL", rows, "A", "id", "B", "id", properties=["value"],
        )
        assert count == 1
        query = session.run.call_args[0][0]
        for field in ("source_id", "source_url", "ingested_at", "run_id"):
            assert f"r.{field} = row.{field}" in query

    def test_load_relationships_without_explicit_properties(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_PROVENANCE_MODE", "strict")
        loader, session = self._make_loader()
        rows = [self._stamped_row(source_key="1", target_key="2")]
        loader.load_relationships("REL", rows, "A", "id", "B", "id")
        query = session.run.call_args[0][0]
        assert "r.source_id = row.source_id" in query
