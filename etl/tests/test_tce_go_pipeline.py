"""Tests for the TCE Goias scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.tce_go import TceGoPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TceGoPipeline:
    return TceGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert TceGoPipeline.name == "tce_go"

    def test_source_id(self) -> None:
        assert TceGoPipeline.source_id == "tce_go"


class TestExtract:
    def test_extract_all_three_domains(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._raw_decisions) == 2
        assert len(pipeline._raw_irregular) == 1
        assert len(pipeline._raw_audits) == 2


class TestTransform:
    def test_decisions_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.decisions) == 2

    def test_irregular_accounts_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.irregular_accounts) == 1

    def test_irregular_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {r["cnpj"] for r in pipeline.irregular_accounts}
        assert "55.667.788/0001-99" in cnpjs

    def test_audits_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.audits) == 2

    def test_uf_always_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.decisions + pipeline.irregular_accounts + pipeline.audits:
            assert r["uf"] == "GO"

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.decisions + pipeline.irregular_accounts + pipeline.audits:
            assert r["source"] == "tce_go"


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
